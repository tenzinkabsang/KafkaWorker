using Confluent.Kafka;

namespace KafkaWorker;

/// <summary>
/// Lazily-resolved raw-bytes producer used to capture records that fail deserialization.
/// </summary>
/// <remarks>
/// Generic over <typeparamref name="TMessage"/> purely so each consumer registration gets its own
/// instance: <c>IProducer&lt;byte[], byte[]&gt;</c> is the same service type for every consumer in
/// the host, so an unkeyed registration would share one producer (and one registration's
/// <c>configureProducer</c>) across all of them, and would collide with any byte-array producer the
/// application registers for its own purposes. The underlying producer is keyed by message type and
/// owned by the container; this wrapper only defers its creation.
/// </remarks>
/// <typeparam name="TMessage">The message type whose consumer owns this producer.</typeparam>
internal sealed class RawDeadLetterProducer<TMessage>(Func<IProducer<byte[], byte[]>> producerFactory)
    where TMessage : class
{
    private readonly Lazy<IProducer<byte[], byte[]>> _producer = new(producerFactory);

    /// <summary>
    /// The producer, created on first access. Nothing is created (and no broker connection opened)
    /// unless a message actually fails deserialization.
    /// </summary>
    public IProducer<byte[], byte[]> Value => _producer.Value;
}
