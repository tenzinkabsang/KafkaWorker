using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace KafkaWorker;

/// <summary>
/// Default implementation that creates a real Kafka consumer using ConsumerBuilder.
/// When <see cref="KafkaWorkerConfig.DeadLetterStartFrom"/> is configured, the consumer
/// uses <c>OffsetsForTimes</c> to seek to that timestamp on first startup (no committed offsets).
/// </summary>
internal sealed class DlqConsumerFactory<TKey, TMessage>(
    IServiceProvider serviceProvider,
    IDeserializer<TMessage> deserializer,
    IOptionsMonitor<KafkaWorkerConfig> kafkaConfigMonitor,
    ILogger<DlqConsumer<TKey, TMessage>> logger) : IDlqConsumerFactory<TKey, TMessage> where TMessage : class
{
    private readonly ConsumerConfig _consumerConfig = serviceProvider
        .GetRequiredKeyedService<ConsumerConfig>(ServiceCollectionExtensions.GetDlqConfigKey<TMessage>());
    private readonly KafkaWorkerConfig _kafkaConfig = kafkaConfigMonitor.Get(typeof(TMessage).FullName);

    private static readonly TimeSpan _brokerRequestTimeout = TimeSpan.FromSeconds(10);

    public IConsumer<TKey, TMessage> Create()
    {
        var builder = new ConsumerBuilder<TKey, TMessage>(_consumerConfig)
            .SetValueDeserializer(deserializer)
            .SetLogHandler((_, logMessage) => KafkaClientLogging.LogClientMessage(logger, logMessage.Name, logMessage.Message))
            .SetErrorHandler((_, error) => KafkaClientLogging.LogClientError(logger, error.Reason, error.IsFatal));

        var startFrom = _kafkaConfig.DeadLetterStartFrom;
        if (startFrom.HasValue)
        {
            builder.SetPartitionsAssignedHandler((consumer, partitions) =>
                ResolveStartOffsets(consumer, partitions, startFrom.Value, _brokerRequestTimeout));
        }

        return builder.Build();
    }

    /// <summary>
    /// Resolves the start offset for each assigned partition individually: partitions with a
    /// committed offset resume where they left off, and only partitions without one seek to the
    /// first offset at or after <paramref name="startFrom"/>. A partition whose newest message is
    /// older than <paramref name="startFrom"/> resolves to <see cref="Offset.End"/> (new messages only).
    /// </summary>
    /// <remarks>
    /// The decision is per partition — returning <see cref="Offset.Unset"/> for an uncommitted
    /// partition would fall back to <c>AutoOffsetReset.Earliest</c> and reprocess the entire
    /// backlog, which is exactly what <see cref="KafkaWorkerConfig.DeadLetterStartFrom"/> exists to avoid.
    /// </remarks>
    internal static IEnumerable<TopicPartitionOffset> ResolveStartOffsets(
        IConsumer<TKey, TMessage> consumer,
        List<TopicPartition> partitions,
        DateTimeOffset startFrom,
        TimeSpan brokerRequestTimeout)
    {
        var committed = consumer.Committed(partitions, brokerRequestTimeout);

        var uncommitted = committed.Where(c => c.Offset == Offset.Unset)
                                   .Select(c => c.TopicPartition)
                                   .ToList();
        if (uncommitted.Count == 0)
            return committed;

        var byTimestamp = consumer
            .OffsetsForTimes(
                uncommitted.Select(tp => new TopicPartitionTimestamp(tp, new Timestamp(startFrom))),
                brokerRequestTimeout)
            .ToDictionary(t => t.TopicPartition);

        return committed.Select(c => c.Offset == Offset.Unset ? byTimestamp[c.TopicPartition] : c);
    }
}
