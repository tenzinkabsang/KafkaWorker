using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace KafkaWorker;

/// <summary>
/// Read-only view over the dead letter topic. Never stores an offset, never commits, and never
/// joins the DLQ consumer group, so inspecting cannot disturb a sweep in flight.
/// </summary>
/// <remarks>
/// <para>
/// Partitions are assigned manually rather than subscribed. Manual assignment bypasses the group's
/// rebalance protocol, which is what keeps this invisible to the live DLQ consumer — subscribing
/// under the same group id would trigger a rebalance and pull partitions out from under a running
/// sweep. The group id is still carried so <c>Committed</c> can report where the sweep will resume.
/// </para>
/// <para>
/// Partitions come from broker metadata rather than from any assignment, so both methods cover the
/// whole topic no matter which replica asks or how the group is currently balanced.
/// </para>
/// </remarks>
internal sealed partial class DlqInspector<TKey, TMessage>(
    IDlqInspectorClientFactory<TKey, TMessage> clientFactory,
    IOptionsMonitor<KafkaWorkerConfig> kafkaConfigMonitor,
    ILogger<DlqInspector<TKey, TMessage>> logger) : IDlqInspector<TMessage> where TMessage : class
{
    private readonly KafkaWorkerConfig _kafkaConfig = kafkaConfigMonitor.Get(typeof(TMessage).FullName);

    private int MaxReprocessAttempts => _kafkaConfig.DeadLetterMaxReprocessAttempts;

    /// <summary>How long any single broker round trip may block.</summary>
    private static readonly TimeSpan BrokerRequestTimeout = TimeSpan.FromSeconds(10);

    /// <summary>
    /// How long one poll waits during a peek. Short because the read normally ends on partition EOF;
    /// this only bounds how often the loop re-checks the overall deadline and the cancellation token.
    /// </summary>
    private static readonly TimeSpan PeekPollTimeout = TimeSpan.FromMilliseconds(500);

    private string DeadLetterTopic => _kafkaConfig.DeadLetterTopic
        ?? throw new InvalidOperationException(
            $"No DeadLetterTopic is configured for {typeof(TMessage).Name}. IDlqInspector is only " +
            $"usable when AddKafkaWorkerDeadLetter has been called with a dead letter topic set.");

    // Confluent's client API is synchronous and blocking. The async signatures exist because the
    // callers are async (health checks, endpoints); offloading keeps a blocking fetch off theirs.
    public Task<DlqDepth> GetDepthAsync(CancellationToken cancellationToken = default)
        => Task.Run(() => GetDepth(cancellationToken), cancellationToken);

    public Task<IReadOnlyList<DlqEntry<TMessage>>> PeekAsync(
        DlqPeekOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        var peekOptions = options ?? new DlqPeekOptions();
        if (peekOptions.FromOffset.HasValue && !peekOptions.Partition.HasValue)
        {
            throw new ArgumentException(
                $"{nameof(DlqPeekOptions.FromOffset)} requires {nameof(DlqPeekOptions.Partition)}: an " +
                $"offset identifies a record only within a partition.", nameof(options));
        }

        return Task.Run(() => Peek(peekOptions, cancellationToken), cancellationToken);
    }

    private DlqDepth GetDepth(CancellationToken cancellationToken)
    {
        var topic = DeadLetterTopic;
        using var consumer = clientFactory.CreateConsumer();

        try
        {
            var partitions = GetPartitions(consumer, topic, partition: null);
            var resumesAt = ResolveSweepStartOffsets(consumer, partitions);

            var depths = new List<DlqPartitionDepth>(partitions.Count);
            foreach (var topicPartition in partitions)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var watermarks = consumer.QueryWatermarkOffsets(topicPartition, BrokerRequestTimeout);
                depths.Add(new DlqPartitionDepth
                {
                    Partition = topicPartition.Partition.Value,
                    Low = watermarks.Low.Value,
                    High = watermarks.High.Value,
                    // An uncommitted partition resumes at the low watermark, matching the DLQ
                    // consumer's AutoOffsetReset.Earliest.
                    ResumesAt = Resolve(resumesAt, topicPartition, watermarks.Low)
                });
            }

            return new DlqDepth { Topic = topic, Partitions = depths };
        }
        finally
        {
            consumer.Close();
        }
    }

    private IReadOnlyList<DlqEntry<TMessage>> Peek(DlqPeekOptions options, CancellationToken cancellationToken)
    {
        var topic = DeadLetterTopic;
        using var consumer = clientFactory.CreateConsumer();

        try
        {
            var partitions = GetPartitions(consumer, topic, options.Partition);
            if (partitions.Count == 0)
            {
                return [];
            }

            consumer.Assign(BuildAssignment(consumer, partitions, options));

            var entries = new List<DlqEntry<TMessage>>();
            var finished = new HashSet<TopicPartition>();
            var deadline = DateTime.UtcNow + options.Timeout;

            while (entries.Count < options.MaxMessages && finished.Count < partitions.Count)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var remaining = deadline - DateTime.UtcNow;
                if (remaining <= TimeSpan.Zero)
                {
                    break;
                }

                ConsumeResult<TKey, TMessage> consumeResult;
                try
                {
                    consumeResult = consumer.Consume(remaining < PeekPollTimeout ? remaining : PeekPollTimeout);
                }
                catch (ConsumeException ex) when (!ex.Error.IsFatal && ex.ConsumerRecord is not null)
                {
                    // Reporting what cannot be deserialized is the point of the inspector, so a
                    // poison record becomes an entry rather than an exception. The consumer's
                    // position has already moved past it, so the loop makes progress.
                    entries.Add(DescribePoison(ex));
                    continue;
                }

                if (consumeResult is null)
                {
                    continue;
                }

                if (consumeResult.IsPartitionEOF)
                {
                    // The whole reason the inspector enables partition EOF: stop as soon as every
                    // partition is exhausted instead of waiting out the timeout on a quiet topic.
                    finished.Add(consumeResult.TopicPartition);
                    continue;
                }

                entries.Add(Describe(consumeResult));
            }

            LogPeeked(logger, topic, entries.Count, partitions.Count);

            return entries
                .OrderBy(entry => entry.Partition)
                .ThenBy(entry => entry.Offset)
                .ToList();
        }
        finally
        {
            consumer.Close();
        }
    }

    /// <summary>
    /// Lists the dead letter topic's partitions from broker metadata, optionally narrowed to one.
    /// </summary>
    private List<TopicPartition> GetPartitions(IConsumer<TKey, TMessage> consumer, string topic, int? partition)
    {
        var partitionIds = clientFactory.GetTopicPartitions(consumer, topic, BrokerRequestTimeout);
        if (partitionIds.Count == 0)
        {
            LogTopicNotFound(logger, topic);
            return [];
        }

        if (partition.HasValue && !partitionIds.Contains(partition.Value))
        {
            throw new ArgumentOutOfRangeException(
                nameof(partition), partition.Value,
                $"Dead letter topic '{topic}' has no partition {partition.Value}. It has {partitionIds.Count}.");
        }

        return partitionIds
            .Where(id => !partition.HasValue || id == partition.Value)
            .Select(id => new TopicPartition(topic, new Partition(id)))
            .ToList();
    }

    /// <summary>
    /// Where the next sweep would resume for each partition: the group's committed offset, or the
    /// offset <see cref="KafkaWorkerConfig.DeadLetterStartFrom"/> resolves to when it never committed.
    /// </summary>
    /// <remarks>
    /// Reuses the DLQ consumer factory's own resolution so the inspector's idea of "pending" is the
    /// sweep's, not an approximation of it.
    /// </remarks>
    private Dictionary<TopicPartition, Offset> ResolveSweepStartOffsets(
        IConsumer<TKey, TMessage> consumer,
        List<TopicPartition> partitions)
    {
        if (partitions.Count == 0)
        {
            return [];
        }

        var startFrom = _kafkaConfig.DeadLetterStartFrom;
        var resolved = startFrom.HasValue
            ? DlqConsumerFactory<TKey, TMessage>.ResolveStartOffsets(consumer, partitions, startFrom.Value, BrokerRequestTimeout)
            : consumer.Committed(partitions, BrokerRequestTimeout);

        return resolved.ToDictionary(offset => offset.TopicPartition, offset => offset.Offset);
    }

    /// <summary>
    /// Assigns each partition at the offset the read should start from: an explicit
    /// <see cref="DlqPeekOptions.FromOffset"/> when paging, otherwise where the sweep would resume.
    /// </summary>
    private List<TopicPartitionOffset> BuildAssignment(
        IConsumer<TKey, TMessage> consumer,
        List<TopicPartition> partitions,
        DlqPeekOptions options)
    {
        if (options.FromOffset.HasValue)
        {
            // Validated as single-partition before we got here.
            return [new TopicPartitionOffset(partitions[0], new Offset(options.FromOffset.Value))];
        }

        var resumesAt = ResolveSweepStartOffsets(consumer, partitions);

        return partitions
            .Select(topicPartition => new TopicPartitionOffset(
                topicPartition,
                new Offset(Resolve(resumesAt, topicPartition, Offset.Beginning))))
            .ToList();
    }

    /// <summary>
    /// Reads a resolved start offset, falling back when the group has no committed offset for the
    /// partition (<see cref="Offset.Unset"/>) or the partition is missing from the lookup entirely.
    /// </summary>
    private static long Resolve(Dictionary<TopicPartition, Offset> resumesAt, TopicPartition topicPartition, Offset fallback)
        => resumesAt.TryGetValue(topicPartition, out var offset) && offset != Offset.Unset
            ? offset.Value
            : fallback.Value;

    private DlqEntry<TMessage> Describe(ConsumeResult<TKey, TMessage> consumeResult)
    {
        var headers = consumeResult.Message.Headers;
        var attempts = headers.GetReprocessAttemptCount();
        var state = DetermineState(consumeResult, headers, attempts);

        return new DlqEntry<TMessage>
        {
            Partition = consumeResult.Partition.Value,
            Offset = consumeResult.Offset.Value,
            Timestamp = consumeResult.Message.Timestamp.UtcDateTime,
            MessageKey = consumeResult.Message.Key,
            Message = consumeResult.Message.Value,
            SourceTopic = headers.GetValue(KafkaHeaders.OriginalTopic),
            Error = headers.GetValue(KafkaHeaders.ErrorMessage),
            ReprocessAttempts = attempts,
            RemainingAttempts = state == DlqEntryState.Retryable ? Math.Max(0, MaxReprocessAttempts - attempts) : 0,
            State = state,
            Headers = headers
        };
    }

    /// <summary>
    /// Mirrors the order the sweep itself applies: a tombstone is committed past before anything
    /// else is looked at, a captured poison record is never handed to a handler, and invalid beats
    /// attempt-counting because it is permanent regardless of how many attempts remain.
    /// </summary>
    private DlqEntryState DetermineState(ConsumeResult<TKey, TMessage> consumeResult, Headers? headers, int attempts)
    {
        if (consumeResult.Message.Value is null)
        {
            return DlqEntryState.Tombstone;
        }

        if (string.Equals(headers.GetValue(KafkaHeaders.DeserializationFailed), "true", StringComparison.OrdinalIgnoreCase))
        {
            return DlqEntryState.Undeserializable;
        }

        if (headers.IsInvalidMessage())
        {
            return DlqEntryState.Invalid;
        }

        return attempts >= MaxReprocessAttempts ? DlqEntryState.AttemptsExhausted : DlqEntryState.Retryable;
    }

    /// <summary>
    /// Describes a record that could not be deserialized. The typed key and value are unavailable by
    /// definition, so the entry carries the raw record's position, headers and the read error.
    /// </summary>
    private static DlqEntry<TMessage> DescribePoison(ConsumeException ex)
    {
        var record = ex.ConsumerRecord!;
        var headers = record.Message?.Headers;

        return new DlqEntry<TMessage>
        {
            Partition = record.Partition.Value,
            Offset = record.Offset.Value,
            Timestamp = record.Message?.Timestamp.UtcDateTime ?? default,
            MessageKey = null,
            Message = null,
            SourceTopic = headers.GetValue(KafkaHeaders.OriginalTopic),
            Error = ex.Error.Reason,
            ReprocessAttempts = headers.GetReprocessAttemptCount(),
            RemainingAttempts = 0,
            State = DlqEntryState.Undeserializable,
            Headers = headers
        };
    }

    [LoggerMessage(EventId = 230, Level = LogLevel.Debug, Message = "Peeked {EntryCount} entries from {Partitions} partition(s) of dead letter topic {DeadLetterTopic}")]
    private static partial void LogPeeked(ILogger logger, string? deadLetterTopic, int entryCount, int partitions);

    [LoggerMessage(EventId = 231, Level = LogLevel.Warning, Message = "Dead letter topic {DeadLetterTopic} was not found in broker metadata; reporting it as empty")]
    private static partial void LogTopicNotFound(ILogger logger, string? deadLetterTopic);
}
