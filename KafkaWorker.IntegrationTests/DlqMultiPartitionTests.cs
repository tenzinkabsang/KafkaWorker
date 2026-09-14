using KafkaWorker.Worker;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Time.Testing;
using Xunit.Abstractions;

namespace KafkaWorker.IntegrationTests;

/// <summary>
/// Exercises a dead letter topic with more than one partition against a real broker.
/// </summary>
/// <remarks>
/// Before the sweep bounded itself with a per-partition finish line, reaching the end of any one
/// partition ended the whole sweep, stranding unprocessed messages on the others until a later tick.
/// A single-partition DLQ topic cannot catch that, so this fixture uses three.
/// </remarks>
public class DlqMultiPartitionTests(ITestOutputHelper testOutputHelper)
{
    private const int DlqPartitions = 3;

    private static OrderMessage Order(int orderId) => new()
    {
        OrderId = orderId,
        SellerId = "DesignSpace",
        OrderDate = DateTime.UtcNow,
        Total = 25.00m
    };

    private static string Json(OrderMessage message) => System.Text.Json.JsonSerializer.Serialize(message);

    [Fact]
    public async Task DlqSweep_DrainsEveryPartitionOfAMultiPartitionDeadLetterTopic()
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

        // Every message fails on the main consumer (call 1..N) and succeeds when the DLQ consumer
        // reprocesses it in place, so each one makes exactly one round trip through the DLQ.
        const int messageCount = 9;
        var failureState = new TransientFailureState { FailCount = messageCount };

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

        // Distinct keys so the producer's partitioner spreads the dead letters across all three
        // partitions rather than piling them onto one.
        for (var orderId = 500; orderId < 500 + messageCount; orderId++)
        {
            await KafkaHelper.PublishMessageAsync(topic, $"key-{orderId}", Json(Order(orderId)));
        }

        // Every message fails on the main consumer and lands in the DLQ.
        await logProvider.WaitForLogCountAsync("Message sent to dead letter topic", messageCount, hostTask);
        Assert.Equal(messageCount, logProvider.CountLogged("Message sent to dead letter topic"));

        // One tick — a single sweep must drain all three partitions, not just the first to finish.
        await Task.Delay(200);
        fakeTime.Advance(TimeSpan.FromMinutes(1));

        await logProvider.WaitForLogCountAsync(
            "Successfully reprocessed dead letter message in place", messageCount, hostTask);
        await Task.Delay(TimeSpan.FromSeconds(3));

        await cts.CancelAsync();
        await hostTask;

        var reprocessed = logProvider.CountLogged("Successfully reprocessed dead letter message in place");
        Assert.Equal(messageCount, reprocessed);

        // Nothing should have been re-enqueued for a later tick: the sweep handled every partition.
        Assert.False(logProvider.HasLogged("Re-enqueued message to dead letter topic"));

        // Guard against the test quietly becoming single-partition: if the partitioner had put every
        // dead letter on one partition, draining "all partitions" would prove nothing.
        var perPartition = KafkaHelper.GetPartitionMessageCounts(dlqTopic, DlqPartitions);
        Assert.True(
            perPartition.Count(count => count > 0) >= 2,
            $"expected the dead letters to span partitions, got [{string.Join(", ", perPartition)}]");
    }
}
