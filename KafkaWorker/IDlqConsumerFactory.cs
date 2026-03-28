using Confluent.Kafka;

namespace KafkaWorker;

/// <summary>
/// Factory for creating Kafka consumers for dead letter queue batch processing.
/// Each batch creates and destroys its own consumer to avoid broker health-check timeouts.
/// </summary>
internal interface IDlqConsumerFactory<TKey, TMessage> where TMessage : class
{
    IConsumer<TKey, TMessage> Create();
}
