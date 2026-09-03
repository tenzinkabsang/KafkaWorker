using KafkaWorker;
using KafkaWorker.Worker;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Time.Testing;
using Xunit.Abstractions;

namespace KafkaWorker.IntegrationTests;

public class JsonConsumerTests(ITestOutputHelper testOutputHelper)
{
    /// <summary>Records terminal failures so tests can assert the sink was invoked.</summary>
    private sealed class RecordingTerminalFailureSink : ITerminalFailureSink<OrderMessage>
    {
        public List<TerminalFailure<OrderMessage>> Failures { get; } = [];

        public Task HandleAsync(TerminalFailure<OrderMessage> failure, CancellationToken cancellationToken)
        {
            lock (Failures)
            {
                Failures.Add(failure);
            }
            return Task.CompletedTask;
        }
    }

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
            services.AddKafkaWorker<OrderMessage, OrderMessageHandlerJson>(context.Configuration);
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
            services.AddKafkaWorker<OrderMessage, OrderMessageHandlerJson>(context.Configuration);
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
            services.AddKafkaWorker<OrderMessage, OrderMessageHandlerJson>(context.Configuration);
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
    public async Task TransientFailure_DlqReprocessesInPlaceWithoutReturningToOriginalTopic()
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
            OrderId = 7,
            SellerId = "DesignSpace",
            OrderDate = DateTime.UtcNow,
            Total = 100.00m
        };
        var messageJson = System.Text.Json.JsonSerializer.Serialize(message);

        await KafkaHelper.InitializeTopicAsync(topic, messageJson);
        await KafkaHelper.InitializeEmptyTopicAsync(dlqTopic);

        // Call 1 (main consumer) fails → DLQ. Call 2 (in-place from DLQ consumer) succeeds.
        var failureState = new TransientFailureState { FailCount = 1 };

        var fakeTime = new FakeTimeProvider();
        using var cts = new CancellationTokenSource(TestLoggerProvider.WaitTime);
        var (host, logProvider) = HostBuilderHelper.CreateHost(testOutputHelper, configurationOverrides, (context, services) =>
        {
            services.AddSingleton(failureState);
            services.AddSingleton<TimeProvider>(fakeTime);
            services.AddKafkaWorker<OrderMessage, TransientFailureHandlerJson>(context.Configuration);
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

        // 3) DLQ consumer reprocesses the message in place (handler invoked directly)
        await logProvider.WaitForLogAsync("Successfully reprocessed dead letter message in place", hostTask);

        await cts.CancelAsync();
        await hostTask;

        // Assert — in-place reprocessing: fail → DLQ → handled in place, never republished
        Assert.True(logProvider.HasLogged("Failed to process message"));
        Assert.True(logProvider.HasLogged("Message sent to dead letter topic"));
        Assert.True(logProvider.HasLogged("Successfully reprocessed dead letter message in place"));
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

        // Always fail — message is re-enqueued to the DLQ until max attempts exceeded
        var failureState = new TransientFailureState { FailCount = int.MaxValue };
        var terminalSink = new RecordingTerminalFailureSink();

        var fakeTime = new FakeTimeProvider();
        // Requires 2 DLQ ticks: tick 1 re-enqueues (attempt 1), tick 2 sees attempt >= max (1) and skips
        using var cts = new CancellationTokenSource(TestLoggerProvider.WaitTime);
        var (host, logProvider) = HostBuilderHelper.CreateHost(testOutputHelper, configurationOverrides, (context, services) =>
        {
            services.AddSingleton(failureState);
            services.AddSingleton<ITerminalFailureSink<OrderMessage>>(terminalSink);
            services.AddSingleton<TimeProvider>(fakeTime);
            services.AddKafkaWorker<OrderMessage, TransientFailureHandlerJson>(context.Configuration);
            services.AddKafkaWorkerDeadLetter<OrderMessage>(context.Configuration);
        });

        // Act
        var hostTask = Task.Run(async () => await host.RunAsync(cts.Token));

        await logProvider.WaitForLogAsync("Subscribed to kafka", hostTask);
        await Task.Delay(TimeSpan.FromSeconds(3));
        await KafkaHelper.PublishMessageAsync(topic, "key", messageJson);

        // 1) Main consumer fails → sent to DLQ
        await logProvider.WaitForLogAsync("Message sent to dead letter topic", hostTask);

        // 2) DLQ tick 1: advance fake time → in-place handler fails → re-enqueued (attempt 1)
        await Task.Delay(200);
        fakeTime.Advance(TimeSpan.FromMinutes(1));
        await logProvider.WaitForLogAsync("Re-enqueued message to dead letter topic", hostTask);

        // 3) DLQ tick 2: advance fake time → sees attempt >= max, skips
        await Task.Delay(200);
        fakeTime.Advance(TimeSpan.FromMinutes(1));
        await logProvider.WaitForLogAsync("exceeded max reprocess attempts", hostTask);

        await cts.CancelAsync();
        await hostTask;

        // Assert — full cycle: fail → DLQ → in-place fail → re-enqueue → max attempts → skip
        Assert.True(logProvider.HasLogged("Failed to process message"));
        Assert.True(logProvider.HasLogged("Message sent to dead letter topic"));
        Assert.True(logProvider.HasLogged("Re-enqueued message to dead letter topic"));
        Assert.True(logProvider.HasLogged("exceeded max reprocess attempts"));
        Assert.False(logProvider.HasLogged("Successfully processed message"));
        Assert.False(logProvider.HasLogged("Invalid message detected"));

        // The terminal failure sink saw the message exactly once, with full context
        var failure = Assert.Single(terminalSink.Failures);
        Assert.Equal(TerminalFailureReason.MaxReprocessAttemptsExceeded, failure.Reason);
        Assert.Equal(42, failure.Message.OrderId);
        Assert.Equal(topic, failure.SourceTopic);
        Assert.Equal(1, failure.ReprocessAttempts);
    }

    [Fact]
    public async Task PoisonMessage_RawBytesCapturedToDlq()
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

        const string poisonPayload = "this is not valid json {{{";

        await KafkaHelper.InitializeTopicAsync(topic, "{}");
        await KafkaHelper.InitializeEmptyTopicAsync(dlqTopic);

        using var cts = new CancellationTokenSource(TestLoggerProvider.WaitTime);
        var (host, logProvider) = HostBuilderHelper.CreateHost(testOutputHelper, configurationOverrides, (context, services) =>
        {
            services.AddKafkaWorker<OrderMessage, OrderMessageHandlerJson>(context.Configuration);
        });

        // Act
        var hostTask = Task.Run(async () => await host.RunAsync(cts.Token));

        await logProvider.WaitForLogAsync("Subscribed to kafka", hostTask);
        await Task.Delay(TimeSpan.FromSeconds(3));
        await KafkaHelper.PublishMessageAsync(topic, "poison-key", poisonPayload);
        await logProvider.WaitForLogAsync("captured to dead letter topic", hostTask);

        await cts.CancelAsync();
        await hostTask;

        // Assert — the handler never saw it, and the DLQ holds the raw bytes verbatim
        Assert.False(logProvider.HasLogged("Successfully processed message"));

        using var rawConsumer = new Confluent.Kafka.ConsumerBuilder<byte[], byte[]>(new Confluent.Kafka.ConsumerConfig
        {
            BootstrapServers = "localhost:9092",
            GroupId = $"verify-{dlqTopic}",
            AutoOffsetReset = Confluent.Kafka.AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        }).Build();
        rawConsumer.Subscribe(dlqTopic);
        var captured = rawConsumer.Consume(TimeSpan.FromSeconds(15));
        rawConsumer.Close();

        Assert.NotNull(captured);
        Assert.Equal(poisonPayload, System.Text.Encoding.UTF8.GetString(captured.Message.Value));
        Assert.Equal("poison-key", System.Text.Encoding.UTF8.GetString(captured.Message.Key));
        Assert.Equal("true", System.Text.Encoding.UTF8.GetString(captured.Message.Headers.GetLastBytes("deserialization-failed")));
        Assert.Equal(topic, System.Text.Encoding.UTF8.GetString(captured.Message.Headers.GetLastBytes("original-topic")));
    }
}
