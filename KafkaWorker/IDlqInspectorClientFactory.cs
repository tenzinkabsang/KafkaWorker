using Confluent.Kafka;

namespace KafkaWorker;

/// <summary>
/// Creates the short-lived Kafka clients <see cref="DlqInspector{TKey, TMessage}"/> uses, and looks
/// up the dead letter topic's partitions.
/// </summary>
/// <remarks>
/// Kept separate from <see cref="IDlqConsumerFactory{TKey, TMessage}"/> because the two build very
/// different consumers: the sweep's joins the consumer group and commits, the inspector's never
/// does either.
/// </remarks>
internal interface IDlqInspectorClientFactory<TKey, TMessage> where TMessage : class
{
    /// <summary>
    /// Builds a consumer for reading without consuming: auto-commit and auto-offset-store off,
    /// partition EOF on so a read can stop as soon as it reaches the end of the log.
    /// </summary>
    IConsumer<TKey, TMessage> CreateConsumer();

    /// <summary>
    /// Lists the topic's partition ids from broker metadata, reusing <paramref name="consumer"/>'s
    /// connection. Returns empty when the topic does not exist.
    /// </summary>
    IReadOnlyList<int> GetTopicPartitions(IConsumer<TKey, TMessage> consumer, string topic, TimeSpan timeout);
}
