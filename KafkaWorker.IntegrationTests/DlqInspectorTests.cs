using System.Diagnostics;
using KafkaWorker.Worker;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Time.Testing;
using Xunit.Abstractions;

namespace KafkaWorker.IntegrationTests;

/// <summary>
/// Exercises <see cref="IDlqInspector{TMessage}"/> against a real broker.
/// </summary>
/// <remarks>
/// The unit tests substitute the Kafka consumer, so everything that makes the inspector safe is
/// unproven there: that it can read the DLQ consumer group's committed offsets without joining the
/// group, that manual assignment plus partition EOF ends a read promptly instead of idling out its
/// timeout, that partitions come back from broker metadata, and — most importantly — that reading
/// the dead letter topic leaves the sweep's position exactly where it was.
/// </remarks>
public class DlqInspectorTests(ITestOutputHelper testOutputHelper)
{
    private const int DlqPartitions = 3;
    private const int MessageCount = 9;

    private static OrderMessage Order(int orderId) => new()
    {
        OrderId = orderId,
        SellerId = "DesignSpace",
        OrderDate = DateTime.UtcNow,
        Total = 25.00m
    };

    private static string Json(OrderMessage message) => System.Text.Json.JsonSerializer.Serialize(message);

    [Fact]
    public async Task Inspector_ReportsTheWholeDeadLetterTopic_AndLeavesTheSweepUndisturbed()
    {
        string topic = $"{Guid.NewGuid():N}";
        string dlqTopic = $"dlq-{topic}";
        var configurationOverrides = new Dictionary<string, string?>
        {
            ["KafkaWorker:Consumer:Topic"] = topic,
            ["KafkaWorker:Consumer:GroupId"] = $"group-id-{topic}",
            ["KafkaWorker:Consumer:DeadLetterTopic"] = dlqTopic,
            ["KafkaWorker:Consumer:MaxRetries"] = "0",
            ["KafkaWorker:Consumer:DeadLetterProcessingIntervalMinutes"] = "1",
            ["KafkaWorker:Consumer:DeadLetterMaxReprocessAttempts"] = "3"
        };

        await KafkaHelper.InitializeTopicAsync(topic, Json(Order(1)));
        await KafkaHelper.InitializeEmptyTopicAsync(dlqTopic, DlqPartitions);

        // Every message fails on the main consumer and succeeds when the DLQ consumer reprocesses it.
        var failureState = new TransientFailureState { FailCount = MessageCount };

        var fakeTime = new FakeTimeProvider();
        using var cts = new CancellationTokenSource(TestLoggerProvider.WaitTime);
        var (host, logProvider) = HostBuilderHelper.CreateHost(testOutputHelper, configurationOverrides, (context, services) =>
        {
            services.AddSingleton(failureState);
            services.AddSingleton<TimeProvider>(fakeTime);
            services.AddKafkaWorker<OrderMessage, TransientFailureHandlerJson>(context.Configuration);
            services.AddKafkaWorkerDeadLetter<OrderMessage>(context.Configuration);
        });

        var hostTask = Task.Run(async () => await host.RunAsync(cts.Token));

        await logProvider.WaitForLogAsync("Subscribed to kafka", hostTask);
        await Task.Delay(TimeSpan.FromSeconds(3));

        // Distinct keys so the dead letters spread across all three partitions.
        for (var orderId = 600; orderId < 600 + MessageCount; orderId++)
        {
            await KafkaHelper.PublishMessageAsync(topic, $"key-{orderId}", Json(Order(orderId)));
        }

        await logProvider.WaitForLogCountAsync("Message sent to dead letter topic", MessageCount, hostTask);
        await Task.Delay(TimeSpan.FromSeconds(2));

        // Registered by AddKafkaWorkerDeadLetter, with no registration of its own.
        var inspector = host.Services.GetRequiredService<IDlqInspector<OrderMessage>>();

        // --- Depth, before any sweep has run ------------------------------------------------
        var depth = await inspector.GetDepthAsync(cts.Token);

        Assert.Equal(dlqTopic, depth.Topic);
        Assert.Equal(DlqPartitions, depth.Partitions.Count);
        Assert.Equal(MessageCount, depth.Pending);
        Assert.False(depth.IsEmpty);

        // Depth covers the whole topic from broker metadata, so it sees partitions regardless of
        // how the DLQ consumer group is balanced.
        Assert.True(
            depth.Partitions.Count(p => p.Pending > 0) >= 2,
            $"expected the dead letters to span partitions, got [{string.Join(", ", depth.Partitions.Select(p => p.Pending))}]");

        // --- Peek ---------------------------------------------------------------------------
        // A generous timeout the read must not spend: it ends on partition EOF, not on the clock.
        var stopwatch = Stopwatch.StartNew();
        var entries = await inspector.PeekAsync(
            new DlqPeekOptions { MaxMessages = 500, Timeout = TimeSpan.FromMinutes(2) }, cts.Token);
        stopwatch.Stop();

        Assert.True(stopwatch.Elapsed < TimeSpan.FromSeconds(30),
            $"peek should end at the end of the log, not at its timeout; took {stopwatch.Elapsed}");

        Assert.Equal(MessageCount, entries.Count);
        Assert.All(entries, entry =>
        {
            Assert.Equal(DlqEntryState.Retryable, entry.State);
            Assert.Equal(topic, entry.SourceTopic);
            Assert.NotNull(entry.Message);
            Assert.False(string.IsNullOrWhiteSpace(entry.Error));
            Assert.Equal(0, entry.ReprocessAttempts);
            Assert.Equal(3, entry.RemainingAttempts);
        });

        // Ordered by partition then offset, and covering every order that failed.
        Assert.Equal(
            entries.Select(e => (e.Partition, e.Offset)).OrderBy(x => x.Partition).ThenBy(x => x.Offset),
            entries.Select(e => (e.Partition, e.Offset)));
        Assert.Equal(
            Enumerable.Range(600, MessageCount),
            entries.Select(e => e.Message!.OrderId).OrderBy(id => id));

        // Peeking twice must be repeatable: the first read committed nothing, so the second sees
        // exactly the same thing.
        var secondRead = await inspector.PeekAsync(cancellationToken: cts.Token);
        Assert.Equal(entries.Count, secondRead.Count);

        // --- The sweep still has everything to do -------------------------------------------
        await Task.Delay(200);
        fakeTime.Advance(TimeSpan.FromMinutes(1));

        await logProvider.WaitForLogCountAsync(
            "Successfully reprocessed dead letter message in place", MessageCount, hostTask);
        await Task.Delay(TimeSpan.FromSeconds(3));

        // Nothing was skipped or stranded by the reads that happened first.
        Assert.Equal(MessageCount, logProvider.CountLogged("Successfully reprocessed dead letter message in place"));
        Assert.False(logProvider.HasLogged("Re-enqueued message to dead letter topic"));

        // --- Depth after the sweep ----------------------------------------------------------
        var drained = await inspector.GetDepthAsync(cts.Token);
        Assert.True(drained.IsEmpty, $"expected an empty DLQ, {drained.Pending} still pending");

        var afterDrain = await inspector.PeekAsync(cancellationToken: cts.Token);
        Assert.Empty(afterDrain);

        await cts.CancelAsync();
        await hostTask;
    }

    [Fact]
    public async Task Inspector_ReportsAnEmptyDeadLetterTopicWithoutBlocking()
    {
        string topic = $"{Guid.NewGuid():N}";
        string dlqTopic = $"dlq-{topic}";
        var configurationOverrides = new Dictionary<string, string?>
        {
            ["KafkaWorker:Consumer:Topic"] = topic,
            ["KafkaWorker:Consumer:GroupId"] = $"group-id-{topic}",
            ["KafkaWorker:Consumer:DeadLetterTopic"] = dlqTopic,
            ["KafkaWorker:Consumer:DeadLetterProcessingIntervalMinutes"] = "60"
        };

        await KafkaHelper.InitializeTopicAsync(topic, Json(Order(1)));
        await KafkaHelper.InitializeEmptyTopicAsync(dlqTopic, DlqPartitions);

        using var cts = new CancellationTokenSource(TestLoggerProvider.WaitTime);
        var (host, logProvider) = HostBuilderHelper.CreateHost(testOutputHelper, configurationOverrides, (context, services) =>
        {
            services.AddSingleton(new TransientFailureState { FailCount = 0 });
            services.AddKafkaWorker<OrderMessage, TransientFailureHandlerJson>(context.Configuration);
            services.AddKafkaWorkerDeadLetter<OrderMessage>(context.Configuration);
        });

        var hostTask = Task.Run(async () => await host.RunAsync(cts.Token));
        await logProvider.WaitForLogAsync("Subscribed to kafka", hostTask);

        var inspector = host.Services.GetRequiredService<IDlqInspector<OrderMessage>>();

        var stopwatch = Stopwatch.StartNew();
        var depth = await inspector.GetDepthAsync(cts.Token);
        var entries = await inspector.PeekAsync(
            new DlqPeekOptions { Timeout = TimeSpan.FromMinutes(2) }, cts.Token);
        stopwatch.Stop();

        Assert.True(depth.IsEmpty);
        Assert.Equal(DlqPartitions, depth.Partitions.Count);
        Assert.Empty(entries);

        // An empty DLQ is the common case and must not cost a timeout to discover.
        Assert.True(stopwatch.Elapsed < TimeSpan.FromSeconds(30),
            $"inspecting an empty DLQ should return promptly; took {stopwatch.Elapsed}");

        await cts.CancelAsync();
        await hostTask;
    }
}
