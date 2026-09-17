using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace KafkaWorker;

/// <summary>
/// Builds inspector clients from the same connection and deserializer settings the DLQ consumer
/// uses, so what the inspector renders is exactly what the sweep would read.
/// </summary>
internal sealed class DlqInspectorClientFactory<TKey, TMessage>(
    IServiceProvider serviceProvider,
    IDeserializer<TMessage> deserializer,
    ILogger<DlqInspector<TKey, TMessage>> logger) : IDlqInspectorClientFactory<TKey, TMessage> where TMessage : class
{
    private readonly ConsumerConfig _dlqConsumerConfig = serviceProvider
        .GetRequiredKeyedService<ConsumerConfig>(ServiceCollectionExtensions.GetDlqConfigKey<TMessage>());

    public IConsumer<TKey, TMessage> CreateConsumer()
    {
        // A copy, never the DLQ consumer's own config instance: that one is a singleton the sweep
        // builds every one of its consumers from, and mutating it here would change their behaviour.
        var config = new ConsumerConfig(_dlqConsumerConfig.ToDictionary(entry => entry.Key, entry => entry.Value))
        {
            // The group id is kept so Committed() can report where the sweep will resume. The
            // inspector never joins the group - it assigns partitions manually, which bypasses the
            // rebalance protocol entirely - so a sweep in flight never sees it.
            EnableAutoCommit = false,
            EnableAutoOffsetStore = false,

            // Unlike the sweep, the inspector *wants* to know it has reached the end of the log:
            // that is what lets a peek return immediately instead of idling out its timeout.
            EnablePartitionEof = true,
            ClientId = $"{_dlqConsumerConfig.ClientId}-dlq-inspector"
        };

        return new ConsumerBuilder<TKey, TMessage>(config)
            .SetValueDeserializer(deserializer)
            .SetLogHandler((_, logMessage) => KafkaClientLogging.LogClientMessage(logger, logMessage.Name, logMessage.Message))
            .SetErrorHandler((_, error) => KafkaClientLogging.LogClientError(logger, error.Reason, error.IsFatal))
            .Build();
    }

    public IReadOnlyList<int> GetTopicPartitions(IConsumer<TKey, TMessage> consumer, string topic, TimeSpan timeout)
    {
        // Dependent on the consumer's handle so this shares its connection and security settings
        // rather than opening a second client with a duplicated config.
        using var adminClient = new DependentAdminClientBuilder(consumer.Handle).Build();

        var topicMetadata = adminClient.GetMetadata(topic, timeout)
            .Topics
            .FirstOrDefault(t => t.Topic == topic);

        if (topicMetadata is null || topicMetadata.Error.IsError)
        {
            return [];
        }

        return topicMetadata.Partitions
            .Select(partition => partition.PartitionId)
            .OrderBy(id => id)
            .ToList();
    }
}
