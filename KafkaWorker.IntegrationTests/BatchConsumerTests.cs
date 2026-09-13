using System.Collections.Concurrent;
using KafkaWorker.Worker;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Xunit.Abstractions;

namespace KafkaWorker.IntegrationTests;

/// <summary>
/// End-to-end coverage of batch consumption against a real broker: messages arrive in batches,
/// offsets are committed at the batch boundary, and a failed batch degrades to per-message
/// processing so only the genuinely bad message is dead lettered.
/// </summary>
public class BatchConsumerTests(ITestOutputHelper testOutputHelper)
{
    /// <summary>Records every batch the library hands the handler.</summary>
    private sealed class RecordingBatchState
    {
        public ConcurrentQueue<int> BatchSizes { get; } = new();
        public ConcurrentQueue<int> OrderIds { get; } = new();

        /// <summary>Order id that should make the batch throw, or null for "never fail".</summary>
        public int? FailOnOrderId { get; init; }
    }

    private sealed class RecordingBatchHandler(
        RecordingBatchState state,
        ILogger<RecordingBatchHandler> logger) : IBatchMessageHandler<OrderMessage>
    {
        public Task HandleBatchAsync(IReadOnlyList<OrderMessage> messages, CancellationToken stoppingToken)
        {
            if (state.FailOnOrderId is { } badId && messages.Any(m => m.OrderId == badId))
            {
                logger.LogWarning("Simulating batch failure because order {OrderId} is present", badId);
                throw new InvalidOperationException($"Batch containing order {badId} failed");
            }

            state.BatchSizes.Enqueue(messages.Count);
            foreach (var message in messages)
            {
                state.OrderIds.Enqueue(message.OrderId);
            }

            logger.LogInformation("Successfully processed batch of {Count} orders", messages.Count);
            return Task.CompletedTask;
        }
    }

    private static OrderMessage Order(int orderId) => new()
    {
        OrderId = orderId,
        SellerId = "DesignSpace",
        OrderDate = DateTime.UtcNow,
        Total = 42.00m
    };

    private static string Json(OrderMessage message) => System.Text.Json.JsonSerializer.Serialize(message);

    [Fact]
    public async Task ProcessesMultipleMessagesInASingleBatch()
    {
        string topic = $"{Guid.NewGuid():N}";
        var configurationOverrides = new Dictionary<string, string?>
        {
            ["KafkaWorker:Consumer:Topic"] = topic,
            ["KafkaWorker:Consumer:GroupId"] = $"group-id-{topic}",
            ["KafkaWorker:Consumer:MaxBatchSize"] = "50",
            ["KafkaWorker:Consumer:BatchLingerMs"] = "1000"
        };

        var state = new RecordingBatchState();
        await KafkaHelper.InitializeTopicAsync(topic, Json(Order(1)));

        using var cts = new CancellationTokenSource(TestLoggerProvider.WaitTime);
        var (host, logProvider) = HostBuilderHelper.CreateHost(testOutputHelper, configurationOverrides, (context, services) =>
        {
            services.AddSingleton(state);
            services.AddKafkaWorkerBatch<OrderMessage, RecordingBatchHandler>(context.Configuration);
        });

        var hostTask = Task.Run(async () => await host.RunAsync(cts.Token));

        await logProvider.WaitForLogAsync("Subscribed to kafka", hostTask);
        await Task.Delay(TimeSpan.FromSeconds(3));

        // Publish a burst so the drain has several messages buffered locally when it runs.
        for (var orderId = 100; orderId < 110; orderId++)
        {
            await KafkaHelper.PublishMessageAsync(topic, $"key-{orderId}", Json(Order(orderId)));
        }

        await logProvider.WaitForLogAsync("Successfully processed batch", hostTask);
        await Task.Delay(TimeSpan.FromSeconds(3));

        await cts.CancelAsync();
        await hostTask;

        Assert.NotEmpty(state.BatchSizes);
        // The burst must not have been delivered one message per handler call.
        Assert.Contains(state.BatchSizes, size => size > 1);
        Assert.Equal(10, state.OrderIds.Count(id => id is >= 100 and < 110));
    }

    [Fact]
    public async Task CommitsOffsetsAtTheBatchBoundarySoMessagesAreNotRedelivered()
    {
        string topic = $"{Guid.NewGuid():N}";
        var groupId = $"group-id-{topic}";
        var configurationOverrides = new Dictionary<string, string?>
        {
            ["KafkaWorker:Consumer:Topic"] = topic,
            ["KafkaWorker:Consumer:GroupId"] = groupId,
            ["KafkaWorker:Consumer:MaxBatchSize"] = "50",
            ["KafkaWorker:Consumer:BatchLingerMs"] = "500"
        };

        await KafkaHelper.InitializeTopicAsync(topic, Json(Order(1)));

        // First host: consume a burst, then shut down.
        var firstState = new RecordingBatchState();
        using (var cts = new CancellationTokenSource(TestLoggerProvider.WaitTime))
        {
            var (host, logProvider) = HostBuilderHelper.CreateHost(testOutputHelper, configurationOverrides, (context, services) =>
            {
                services.AddSingleton(firstState);
                services.AddKafkaWorkerBatch<OrderMessage, RecordingBatchHandler>(context.Configuration);
            });

            var hostTask = Task.Run(async () => await host.RunAsync(cts.Token));
            await logProvider.WaitForLogAsync("Subscribed to kafka", hostTask);
            await Task.Delay(TimeSpan.FromSeconds(3));

            for (var orderId = 200; orderId < 205; orderId++)
            {
                await KafkaHelper.PublishMessageAsync(topic, $"key-{orderId}", Json(Order(orderId)));
            }

            await logProvider.WaitForLogAsync("Successfully processed batch", hostTask);
            await Task.Delay(TimeSpan.FromSeconds(3));
            await cts.CancelAsync();
            await hostTask;
        }

        Assert.Equal(5, firstState.OrderIds.Count(id => id is >= 200 and < 205));

        // Second host, same group: the committed offsets must keep the burst from coming back.
        var secondState = new RecordingBatchState();
        using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(25)))
        {
            var (host, logProvider) = HostBuilderHelper.CreateHost(testOutputHelper, configurationOverrides, (context, services) =>
            {
                services.AddSingleton(secondState);
                services.AddKafkaWorkerBatch<OrderMessage, RecordingBatchHandler>(context.Configuration);
            });

            var hostTask = Task.Run(async () => await host.RunAsync(cts.Token));
            await logProvider.WaitForLogAsync("Subscribed to kafka", hostTask);
            await Task.Delay(TimeSpan.FromSeconds(8));
            await cts.CancelAsync();
            await hostTask;
        }

        Assert.Empty(secondState.OrderIds);
    }

    [Fact]
    public async Task FailedBatchDeadLettersOnlyTheOffendingMessage()
    {
        string topic = $"{Guid.NewGuid():N}";
        var configurationOverrides = new Dictionary<string, string?>
        {
            ["KafkaWorker:Consumer:Topic"] = topic,
            ["KafkaWorker:Consumer:GroupId"] = $"group-id-{topic}",
            ["KafkaWorker:Consumer:DeadLetterTopic"] = $"dlq-{topic}",
            ["KafkaWorker:Consumer:MaxRetries"] = "0",
            ["KafkaWorker:Consumer:MaxBatchSize"] = "50",
            ["KafkaWorker:Consumer:BatchLingerMs"] = "1000"
        };

        // Order 999 poisons any batch it lands in; every other order is fine.
        var state = new RecordingBatchState { FailOnOrderId = 999 };
        await KafkaHelper.InitializeTopicAsync(topic, Json(Order(1)));
        await KafkaHelper.InitializeEmptyTopicAsync($"dlq-{topic}");

        using var cts = new CancellationTokenSource(TestLoggerProvider.WaitTime);
        var (host, logProvider) = HostBuilderHelper.CreateHost(testOutputHelper, configurationOverrides, (context, services) =>
        {
            services.AddSingleton(state);
            services.AddKafkaWorkerBatch<OrderMessage, RecordingBatchHandler>(context.Configuration);
        });

        var hostTask = Task.Run(async () => await host.RunAsync(cts.Token));

        await logProvider.WaitForLogAsync("Subscribed to kafka", hostTask);
        await Task.Delay(TimeSpan.FromSeconds(3));

        await KafkaHelper.PublishMessageAsync(topic, "key-300", Json(Order(300)));
        await KafkaHelper.PublishMessageAsync(topic, "key-999", Json(Order(999)));
        await KafkaHelper.PublishMessageAsync(topic, "key-301", Json(Order(301)));

        await logProvider.WaitForLogAsync("re-processing them one at a time", hostTask);
        await logProvider.WaitForLogAsync("Message sent to dead letter topic", hostTask);
        await Task.Delay(TimeSpan.FromSeconds(3));

        await cts.CancelAsync();
        await hostTask;

        // The good messages were processed individually during the fallback; only 999 was dead lettered.
        Assert.Contains(300, state.OrderIds);
        Assert.Contains(301, state.OrderIds);
        Assert.DoesNotContain(999, state.OrderIds);
    }
}
