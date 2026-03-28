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
    private readonly KafkaWorkerConfig _kafkaConfig = kafkaConfigMonitor.Get(typeof(TMessage).Name);

    private static readonly TimeSpan _brokerRequestTimeout = TimeSpan.FromSeconds(10);

    public IConsumer<TKey, TMessage> Create()
    {
        var builder = new ConsumerBuilder<TKey, TMessage>(_consumerConfig)
            .SetValueDeserializer(deserializer)
            .SetLogHandler((_, logMessage) => logger.LogDebug(logMessage.Message))
            .SetErrorHandler((_, error) => logger.LogError(error.Reason));

        var startFrom = _kafkaConfig.DeadLetterStartFrom;
        if (startFrom.HasValue)
        {
            builder.SetPartitionsAssignedHandler((consumer, partitions) =>
            {
                var committed = consumer.Committed(partitions, _brokerRequestTimeout);
                if (committed.Any(c => c.Offset != Offset.Unset))
                    return committed;

                var timestamps = partitions.Select(tp => new TopicPartitionTimestamp(tp, new Timestamp(startFrom.Value)));
                return consumer.OffsetsForTimes(timestamps, _brokerRequestTimeout);
            });
        }

        return builder.Build();
    }
}
