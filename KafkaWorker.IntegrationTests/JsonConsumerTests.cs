using KafkaWorker;
using KafkaWorker.Worker;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Time.Testing;
using Xunit.Abstractions;

namespace KafkaWorker.IntegrationTests;

public class JsonConsumerTests(ITestOutputHelper testOutputHelper)
{
    [Fact]
    public async Task ProcessesSuccessfully()
    {
        // Arrange
        string topic = $"{Guid.NewGuid():N}";
        var configurationOverrides = new Dictionary<string, string?>
        {
            ["KafkaWorker:Consumer:Topic"] = topic,
            ["KafkaWorker:Consumer:GroupId"] = $"group-id-{topic}",
            ["KafkaWorker:Consumer:DeadLetterTopic"] = $"dlq-{topic}",
            ["KafkaWorker:Consumer:DeadLetterProcessingIntervalMinutes"] = "1"
        };

        var message = new OrderMessage
        {
            OrderId = 12345,
            SellerId = "DesignSpace",
            OrderDate = DateTime.UtcNow,
            Total = 99.99m
        };

        var messageJson = System.Text.Json.JsonSerializer.Serialize(message);

        await KafkaHelper.InitializeTopicAsync(topic, messageJson);

        using var cts = new CancellationTokenSource(TestLoggerProvider.WaitTime);

        var (host, logProvider) = HostBuilderHelper.CreateHost(testOutputHelper, configurationOverrides, (context, services) =>
        {
            services.AddKafkaWorker<OrderMessage, OrderMessageProcessorJson>(context.Configuration);
            services.AddKafkaWorkerDeadLetter<OrderMessage>(context.Configuration);
        });

        // Act
        var hostTask = Task.Run(async () => await host.RunAsync(cts.Token));

        await logProvider.WaitForLogAsync("Subscribed to kafka", hostTask);
        await Task.Delay(TimeSpan.FromSeconds(3));
        await KafkaHelper.PublishMessageAsync(topic, "key", messageJson);
        await logProvider.WaitForLogAsync("Successfully processed message", hostTask);

        // Stop the host now that we've observed the expected behavior
        await cts.CancelAsync();
        await hostTask;

        // Assert
        Assert.True(logProvider.HasLogged("Subscribed to kafka"));
        Assert.True(logProvider.HasLogged("Successfully processed message"));
        Assert.False(logProvider.HasLogged("Invalid message detected"));
        Assert.False(logProvider.HasLogged("Message sent to dead letter topic"));
    }

    [Fact]
    public async Task InvalidMessage_SentToDlq()
    {
        // Arrange
        string topic = $"{Guid.NewGuid():N}";
        string dlqTopic = $"dlq-{topic}";
        var configurationOverrides = new Dictionary<string, string?>
        {
            ["KafkaWorker:Consumer:Topic"] = topic,
            ["KafkaWorker:Consumer:GroupId"] = $"group-id-{topic}",
            ["KafkaWorker:Consumer:DeadLetterTopic"] = dlqTopic,
            ["KafkaWorker:Consumer:DeadLetterProcessingIntervalMinutes"] = "1"
        };

        var invalidMessage = new OrderMessage
        {
            OrderId = 99999,
            SellerId = null,
            OrderDate = DateTime.UtcNow,
            Total = 50.00m
        };
        var messageJson = System.Text.Json.JsonSerializer.Serialize(invalidMessage);

        await KafkaHelper.InitializeTopicAsync(topic, messageJson);

        using var cts = new CancellationTokenSource(TestLoggerProvider.WaitTime);
        var (host, logProvider) = HostBuilderHelper.CreateHost(testOutputHelper, configurationOverrides, (context, services) =>
        {
            services.AddKafkaWorker<OrderMessage, OrderMessageProcessorJson>(context.Configuration);
            services.AddKafkaWorkerDeadLetter<OrderMessage>(context.Configuration);
        });

        // Act
        var hostTask = Task.Run(async () => await host.RunAsync(cts.Token));

        await logProvider.WaitForLogAsync("Subscribed to kafka", hostTask);
        await Task.Delay(TimeSpan.FromSeconds(3));
        await KafkaHelper.PublishMessageAsync(topic, "key", messageJson);
        await logProvider.WaitForLogAsync("Message sent to dead letter topic", hostTask);

        // Stop the host now that we've observed the expected behavior
        await cts.CancelAsync();
        await hostTask;

        // Assert — invalid detected, bypassed retry, published to DLQ, never processed successfully
        Assert.True(logProvider.HasLogged("Invalid message detected"));
        Assert.True(logProvider.HasLogged("Message sent to dead letter topic"));
        Assert.False(logProvider.HasLogged("Successfully processed message"));
    }

    [Fact]
    public async Task InvalidMessage_DlqConsumerSkipsMessage()
    {
        // Arrange
        string topic = $"{Guid.NewGuid():N}";
        string dlqTopic = $"dlq-{topic}";
        var configurationOverrides = new Dictionary<string, string?>
        {
            ["KafkaWorker:Consumer:Topic"] = topic,
            ["KafkaWorker:Consumer:GroupId"] = $"group-id-{topic}",
            ["KafkaWorker:Consumer:DeadLetterTopic"] = dlqTopic,
            ["KafkaWorker:Consumer:DeadLetterProcessingIntervalMinutes"] = "1"
        };

        var invalidMessage = new OrderMessage
        {
            OrderId = 88888,
            SellerId = null,
            OrderDate = DateTime.UtcNow,
            Total = 25.00m
        };
        var messageJson = System.Text.Json.JsonSerializer.Serialize(invalidMessage);

        await KafkaHelper.InitializeTopicAsync(topic, messageJson);
        await KafkaHelper.InitializeEmptyTopicAsync(dlqTopic);

        var fakeTime = new FakeTimeProvider();
        using var cts = new CancellationTokenSource(TestLoggerProvider.WaitTime);
        var (host, logProvider) = HostBuilderHelper.CreateHost(testOutputHelper, configurationOverrides, (context, services) =>
        {
            services.AddSingleton<TimeProvider>(fakeTime);
            services.AddKafkaWorker<OrderMessage, OrderMessageProcessorJson>(context.Configuration);
            services.AddKafkaWorkerDeadLetter<OrderMessage>(context.Configuration);
        });

        // Act
        var hostTask = Task.Run(async () => await host.RunAsync(cts.Token));

        // 1) Wait for the main consumer to subscribe and start polling
        await logProvider.WaitForLogAsync("Subscribed to kafka", hostTask);
        await Task.Delay(TimeSpan.FromSeconds(3));

        // 2) Publish the invalid message — main consumer routes it to DLQ
        await KafkaHelper.PublishMessageAsync(topic, "key", messageJson);
        await logProvider.WaitForLogAsync("Message sent to dead letter topic", hostTask);

        // 3) Advance fake time to trigger the DLQ consumer periodic tick
        await Task.Delay(200);
        fakeTime.Advance(TimeSpan.FromMinutes(1));

        // 4) Wait for the DLQ consumer to pick up and skip the invalid message
        await logProvider.WaitForLogAsync("Skipping invalid message", hostTask);

        // Stop the host now that we've observed the expected behavior
        await cts.CancelAsync();
        await hostTask;

        // Assert — full cycle: main consumer → DLQ → DLQ consumer skips invalid
        Assert.True(logProvider.HasLogged("Invalid message detected"));
        Assert.True(logProvider.HasLogged("Message sent to dead letter topic"));
        Assert.True(logProvider.HasLogged("Subscribed to dead letter topic"));
        Assert.True(logProvider.HasLogged("Skipping invalid message"));
        Assert.True(logProvider.HasLogged("Finished processing dead letter queue batch"));
    }

    [Fact]
    public async Task TransientFailure_DlqReprocessesAndConsumerSucceeds()
    {
        // Arrange
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

        var message = new OrderMessage
        {
            OrderId = 42,
            SellerId = "DesignSpace",
            OrderDate = DateTime.UtcNow,
            Total = 100.00m
        };
        var messageJson = System.Text.Json.JsonSerializer.Serialize(message);

        await KafkaHelper.InitializeTopicAsync(topic, messageJson);
        await KafkaHelper.InitializeEmptyTopicAsync(dlqTopic);

        // Fail the first call, succeed on the second (after DLQ round-trip)
        var failureState = new TransientFailureState { FailCount = 1 };

        var fakeTime = new FakeTimeProvider();
        using var cts = new CancellationTokenSource(TestLoggerProvider.WaitTime);
        var (host, logProvider) = HostBuilderHelper.CreateHost(testOutputHelper, configurationOverrides, (context, services) =>
        {
            services.AddSingleton(failureState);
            services.AddSingleton<TimeProvider>(fakeTime);
            services.AddKafkaWorker<OrderMessage, TransientFailureProcessorJson>(context.Configuration);
            services.AddKafkaWorkerDeadLetter<OrderMessage>(context.Configuration);
        });

        // Act
        var hostTask = Task.Run(async () => await host.RunAsync(cts.Token));

        await logProvider.WaitForLogAsync("Subscribed to kafka", hostTask);
        await Task.Delay(TimeSpan.FromSeconds(3));
        await KafkaHelper.PublishMessageAsync(topic, "key", messageJson);

        // 1) Main consumer fails → message sent to DLQ
        await logProvider.WaitForLogAsync("Message sent to dead letter topic", hostTask);

        // 2) Advance fake time to trigger DLQ consumer tick
        await Task.Delay(200);
        fakeTime.Advance(TimeSpan.FromMinutes(1));

        // 3) DLQ consumer sends message back to original topic
        await logProvider.WaitForLogAsync("Successfully reprocessed message", hostTask);

        // 4) Main consumer processes the redelivered message successfully
        await logProvider.WaitForLogAsync("Successfully processed message", hostTask);

        await cts.CancelAsync();
        await hostTask;

        // Assert — full round-trip: fail → DLQ → DLQ sends back → success
        Assert.True(logProvider.HasLogged("Failed to process message"));
        Assert.True(logProvider.HasLogged("Message sent to dead letter topic"));
        Assert.True(logProvider.HasLogged("Successfully reprocessed message"));
        Assert.True(logProvider.HasLogged("Successfully processed message"));
        Assert.False(logProvider.HasLogged("Invalid message detected"));
    }

    [Fact]
    public async Task TransientFailure_ExceedsMaxReprocessAttempts_DlqSkips()
    {
        // Arrange
        string topic = $"{Guid.NewGuid():N}";
        string dlqTopic = $"dlq-{topic}";
        var configurationOverrides = new Dictionary<string, string?>
        {
            ["KafkaWorker:Consumer:Topic"] = topic,
            ["KafkaWorker:Consumer:GroupId"] = $"group-id-{topic}",
            ["KafkaWorker:Consumer:DeadLetterTopic"] = dlqTopic,
            ["KafkaWorker:Consumer:MaxRetries"] = "0",
            ["KafkaWorker:Consumer:DeadLetterProcessingIntervalMinutes"] = "1",
            ["KafkaWorker:Consumer:DeadLetterMaxReprocessAttempts"] = "1",
            ["KafkaWorker:Consumer:DeadLetterStartFrom"] = DateTime.UtcNow.AddDays(-1).ToString("o") // ensure DLQ consumer picks up the message immediately
        };

        var message = new OrderMessage
        {
            OrderId = 42,
            SellerId = "DesignSpace",
            OrderDate = DateTime.UtcNow,
            Total = 100.00m
        };
        var messageJson = System.Text.Json.JsonSerializer.Serialize(message);

        await KafkaHelper.InitializeTopicAsync(topic, messageJson);
        await KafkaHelper.InitializeEmptyTopicAsync(dlqTopic);

        // Always fail — message bounces between consumer and DLQ until max attempts exceeded
        var failureState = new TransientFailureState { FailCount = int.MaxValue };

        var fakeTime = new FakeTimeProvider();
        // Requires 2 DLQ ticks: tick 1 sends back (attempt 1), tick 2 sees attempt >= max (1) and skips
        using var cts = new CancellationTokenSource(TestLoggerProvider.WaitTime);
        var (host, logProvider) = HostBuilderHelper.CreateHost(testOutputHelper, configurationOverrides, (context, services) =>
        {
            services.AddSingleton(failureState);
            services.AddSingleton<TimeProvider>(fakeTime);
            services.AddKafkaWorker<OrderMessage, TransientFailureProcessorJson>(context.Configuration);
            services.AddKafkaWorkerDeadLetter<OrderMessage>(context.Configuration);
        });

        // Act
        var hostTask = Task.Run(async () => await host.RunAsync(cts.Token));

        await logProvider.WaitForLogAsync("Subscribed to kafka", hostTask);
        await Task.Delay(TimeSpan.FromSeconds(3));
        await KafkaHelper.PublishMessageAsync(topic, "key", messageJson);

        // 1) Main consumer fails → sent to DLQ
        await logProvider.WaitForLogAsync("Message sent to dead letter topic", hostTask);

        // 2) DLQ tick 1: advance fake time → sends message back (attempt 1)
        await Task.Delay(200);
        fakeTime.Advance(TimeSpan.FromMinutes(1));
        await logProvider.WaitForLogAsync("Successfully reprocessed message", hostTask);

        // 3) Wait for consumer to fail again and send back to DLQ (second occurrence)
        await logProvider.WaitForLogCountAsync("Message sent to dead letter topic", 2, hostTask);

        // 4) DLQ tick 2: advance fake time → sees attempt >= max, skips
        await Task.Delay(200);
        fakeTime.Advance(TimeSpan.FromMinutes(1));
        await logProvider.WaitForLogAsync("exceeded max reprocess attempts", hostTask);

        await cts.CancelAsync();
        await hostTask;

        // Assert — full cycle: fail → DLQ → send back → fail again → DLQ → max attempts → skip
        Assert.True(logProvider.HasLogged("Failed to process message"));
        Assert.True(logProvider.HasLogged("Message sent to dead letter topic"));
        Assert.True(logProvider.HasLogged("Successfully reprocessed message"));
        Assert.True(logProvider.HasLogged("exceeded max reprocess attempts"));
        Assert.False(logProvider.HasLogged("Successfully processed message"));
        Assert.False(logProvider.HasLogged("Invalid message detected"));
    }
}
