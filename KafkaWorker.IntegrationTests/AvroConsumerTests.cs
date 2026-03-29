using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry.Serdes;
using KafkaWorker;
using KafkaWorker.Avro;
using KafkaWorker.Worker;
using KafkaWorker.Sample;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Time.Testing;
using Xunit.Abstractions;

namespace KafkaWorker.IntegrationTests;

public class AvroConsumerTests(ITestOutputHelper testOutputHelper)
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

        var message = new AvroOrderMessage
        {
            OrderId = 1,
            SellerId = "DesignSpace",
            OrderDate = DateTime.UtcNow,
            Total = 99.99m
        };

        await KafkaHelper.InitializeTopicAsync(topic, message, s => new AvroSerializer<AvroOrderMessage>(s).AsSyncOverAsync());

        using var cts = new CancellationTokenSource(TestLoggerProvider.WaitTime);
        var (host, logProvider) = HostBuilderHelper.CreateHost(testOutputHelper, configurationOverrides, (context, services) =>
        {
            services.AddKafkaWorkerAvro<AvroOrderMessage, OrderMessageProcessorAvro>(context.Configuration);
            services.AddKafkaWorkerDeadLetter<AvroOrderMessage>(context.Configuration);
        });

        // Act
        var hostTask = Task.Run(async () => await host.RunAsync(cts.Token));

        await logProvider.WaitForLogAsync("Subscribed to kafka", hostTask);
        await Task.Delay(TimeSpan.FromSeconds(3));
        await KafkaHelper.PublishMessageAsync(topic, "key", message, s => new AvroSerializer<AvroOrderMessage>(s).AsSyncOverAsync());
        await logProvider.WaitForLogAsync("Successfully processed message", hostTask);

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

        var invalidMessage = new AvroOrderMessage
        {
            OrderId = 99999,
            SellerId = "",
            OrderDate = DateTime.UtcNow,
            Total = 50.00m
        };

        await KafkaHelper.InitializeTopicAsync(topic, invalidMessage, s => new AvroSerializer<AvroOrderMessage>(s).AsSyncOverAsync());

        using var cts = new CancellationTokenSource(TestLoggerProvider.WaitTime);
        var (host, logProvider) = HostBuilderHelper.CreateHost(testOutputHelper, configurationOverrides, (context, services) =>
        {
            services.AddKafkaWorkerAvro<AvroOrderMessage, OrderMessageProcessorAvro>(context.Configuration);
            services.AddKafkaWorkerDeadLetter<AvroOrderMessage>(context.Configuration);
        });

        // Act
        var hostTask = Task.Run(async () => await host.RunAsync(cts.Token));

        await logProvider.WaitForLogAsync("Subscribed to kafka", hostTask);
        await Task.Delay(TimeSpan.FromSeconds(3));
        await KafkaHelper.PublishMessageAsync(topic, "key", invalidMessage, s => new AvroSerializer<AvroOrderMessage>(s).AsSyncOverAsync());
        await logProvider.WaitForLogAsync("Message sent to dead letter topic", hostTask);

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

        var invalidMessage = new AvroOrderMessage
        {
            OrderId = 88888,
            SellerId = "",
            OrderDate = DateTime.UtcNow,
            Total = 25.00m
        };

        await KafkaHelper.InitializeTopicAsync(topic, invalidMessage, s => new AvroSerializer<AvroOrderMessage>(s).AsSyncOverAsync());
        await KafkaHelper.InitializeEmptyTopicAsync(dlqTopic);

        var fakeTime = new FakeTimeProvider();
        using var cts = new CancellationTokenSource(TestLoggerProvider.WaitTime);
        var (host, logProvider) = HostBuilderHelper.CreateHost(testOutputHelper, configurationOverrides, (context, services) =>
        {
            services.AddSingleton<TimeProvider>(fakeTime);
            services.AddKafkaWorkerAvro<AvroOrderMessage, OrderMessageProcessorAvro>(context.Configuration);
            services.AddKafkaWorkerDeadLetter<AvroOrderMessage>(context.Configuration);
        });

        // Act
        var hostTask = Task.Run(async () => await host.RunAsync(cts.Token));

        // 1) Wait for the main consumer to subscribe and start polling
        await logProvider.WaitForLogAsync("Subscribed to kafka", hostTask);
        await Task.Delay(TimeSpan.FromSeconds(3));

        // 2) Publish the invalid message — main consumer routes it to DLQ
        await KafkaHelper.PublishMessageAsync(topic, "key", invalidMessage, s => new AvroSerializer<AvroOrderMessage>(s).AsSyncOverAsync());
        await logProvider.WaitForLogAsync("Message sent to dead letter topic", hostTask);

        // 3) Advance fake time to trigger the DLQ consumer periodic tick
        await Task.Delay(200);
        fakeTime.Advance(TimeSpan.FromMinutes(1));

        // 4) Wait for the DLQ consumer to pick up and skip the invalid message
        await logProvider.WaitForLogAsync("Skipping invalid message", hostTask);

        await cts.CancelAsync();
        await hostTask;

        // Assert — full cycle: main consumer → DLQ → DLQ consumer skips invalid
        Assert.True(logProvider.HasLogged("Invalid message detected"));
        Assert.True(logProvider.HasLogged("Message sent to dead letter topic"));
        Assert.True(logProvider.HasLogged("Subscribed to dead letter topic"));
        Assert.True(logProvider.HasLogged("Skipping invalid message"));
        Assert.True(logProvider.HasLogged("Finished processing dead letter queue batch"));
    }
}
