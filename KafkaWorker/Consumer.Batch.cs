using System.Diagnostics;
using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace KafkaWorker;

/// <summary>
/// Batch consumption for <see cref="Consumer{TKey, TMessage}"/>: drains a bounded batch from the
/// client's local pre-fetch queue, hands it to an <see cref="IBatchMessageHandler{TMessage}"/>, and
/// commits the batch's offsets synchronously.
/// </summary>
/// <remarks>
/// <para>
/// Batching is a downstream optimization, not a Kafka one — draining is cheap because the client
/// already holds pre-fetched messages in memory, so no additional broker round trips are involved.
/// </para>
/// <para>
/// Failure handling is deliberately unchanged from single-message mode: a batch that throws tells
/// us <i>something</i> failed but never <i>what</i>, so every message in it is re-processed one at a
/// time through <c>ProcessMessageWithRetryAsync</c>, which retries, dead letters, notifies the
/// terminal failure sink and stores offsets exactly as it always has. Batching therefore never
/// widens the blast radius of a single bad message.
/// </para>
/// <para>
/// Unlike the single-message loop, offsets are committed synchronously at each batch boundary
/// (batch consumers run with the client's background auto-commit disabled). One round trip per
/// batch is roughly a hundredth of the per-message cost that made background auto-commit
/// worthwhile, and it bounds redelivery after a hard crash to a single batch rather than to
/// however much throughput fit inside <c>AutoCommitIntervalMs</c>.
/// </para>
/// </remarks>
internal sealed partial class Consumer<TKey, TMessage>
{
    /// <summary>
    /// How long a drain with nothing accumulated blocks before re-checking the stopping token.
    /// <c>Consume(TimeSpan)</c> does not observe a cancellation token, so an idle consumer needs a
    /// bounded poll to stay responsive to shutdown.
    /// </summary>
    private static readonly TimeSpan IdlePollTimeout = TimeSpan.FromSeconds(1);

    private int MaxBatchSize => _kafkaConfig.MaxBatchSize;

    private int BatchLingerMs => _kafkaConfig.BatchLingerMs;

    /// <summary>
    /// Drains, processes and commits batches until the host shuts down.
    /// </summary>
    private async Task RunBatchLoopAsync(CancellationToken stoppingToken)
    {
        var batch = new List<ConsumeResult<TKey, TMessage>>(MaxBatchSize);

        while (!stoppingToken.IsCancellationRequested)
        {
            batch.Clear();
            var poison = DrainBatch(batch, stoppingToken);

            if (batch.Count > 0)
            {
                await ProcessBatchAsync(batch, stoppingToken);
                CommitStoredOffsets();
            }

            // Ordering matters: a record that failed to deserialize is skipped by storing its
            // offset + 1, which would commit past any message ahead of it on the same partition.
            // The accumulated batch must therefore be processed and committed first.
            if (poison is not null)
            {
                await SkipPoisonMessageAsync(poison, stoppingToken);
                CommitStoredOffsets();
            }
        }
    }

    /// <summary>
    /// Fills <paramref name="batch"/> until it reaches <see cref="MaxBatchSize"/>, the linger window
    /// closes, or the local queue runs dry.
    /// </summary>
    /// <returns>
    /// The <see cref="ConsumeException"/> that ended the drain when a record could not be
    /// deserialized; otherwise <c>null</c>.
    /// </returns>
    private ConsumeException? DrainBatch(List<ConsumeResult<TKey, TMessage>> batch, CancellationToken stoppingToken)
    {
        var lingerStart = 0L;

        while (batch.Count < MaxBatchSize && !stoppingToken.IsCancellationRequested)
        {
            TimeSpan timeout;
            if (batch.Count == 0)
            {
                timeout = IdlePollTimeout;
            }
            else
            {
                // The linger window opens when the first message of the batch arrives. Subsequent
                // reads sweep up whatever the client already holds locally, which costs about a
                // microsecond each. Once the window closes the drain keeps going at zero timeout —
                // taking only what is already buffered — and ends when the queue runs dry, so
                // BatchLingerMs caps how long the batch waits, never how much it may collect.
                var remaining = TimeSpan.FromMilliseconds(BatchLingerMs) - Stopwatch.GetElapsedTime(lingerStart);
                timeout = remaining > TimeSpan.Zero ? remaining : TimeSpan.Zero;
            }

            ConsumeResult<TKey, TMessage> consumeResult;
            try
            {
                consumeResult = consumer.Consume(timeout);
            }
            catch (ConsumeException ex) when (!ex.Error.IsFatal)
            {
                return ex;
            }

            if (consumeResult == null)
            {
                // Nothing available: keep waiting while the batch is still empty, otherwise the
                // linger window has produced everything it is going to.
                if (batch.Count == 0)
                {
                    continue;
                }

                break;
            }

            if (consumeResult.IsPartitionEOF)
            {
                continue;
            }

            if (batch.Count == 0)
            {
                lingerStart = Stopwatch.GetTimestamp();
            }

            if (consumeResult.Message?.Value == null)
            {
                // Tombstone (null value): kept in the batch so its offset advances with the batch's
                // commit, but filtered out before the handler sees it. Storing its offset here
                // instead would commit past an earlier message in the same batch that has not run.
                LogTombstoneSkipped(logger, Topic, consumeResult.Partition.Value, consumeResult.Offset.Value);
            }

            batch.Add(consumeResult);
        }

        return null;
    }

    /// <summary>
    /// Hands the batch to the registered <see cref="IBatchMessageHandler{TMessage}"/> and stores the
    /// batch's offsets on success. On failure every message is re-processed individually through the
    /// single-message path, which stores offsets one at a time as it goes.
    /// </summary>
    private async Task ProcessBatchAsync(List<ConsumeResult<TKey, TMessage>> batch, CancellationToken stoppingToken)
    {
        var payload = new List<TMessage>(batch.Count);
        foreach (var consumeResult in batch)
        {
            if (consumeResult.Message?.Value is not null)
            {
                payload.Add(consumeResult.Message.Value);
            }
        }

        if (payload.Count == 0)
        {
            // Tombstones only — nothing for the handler to do, but the offsets still advance.
            StoreBatchOffsets(batch);
            return;
        }

        var startTimestamp = Stopwatch.GetTimestamp();
        try
        {
            LogProcessingBatch(logger, payload.Count, Topic);

            using var scope = serviceScopeFactory.CreateScope();
            var handler = scope.ServiceProvider.GetRequiredService<IBatchMessageHandler<TMessage>>();
            await handler.HandleBatchAsync(payload, stoppingToken);

            LogSuccessfullyProcessedBatch(logger, payload.Count, Topic);
            metrics.MessagesProcessed.Add(payload.Count,
                new KeyValuePair<string, object?>("topic", Topic),
                new KeyValuePair<string, object?>("status", "success"));
            metrics.BatchSize.Record(payload.Count, new KeyValuePair<string, object?>("topic", Topic));

            StoreBatchOffsets(batch);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // Shutdown mid-batch: store nothing and commit nothing, so the whole batch is
            // redelivered on restart rather than silently committed past.
            throw;
        }
        catch (Exception ex)
        {
            // A failed batch identifies no particular message, so fall back to the per-message path:
            // each message is retried, dead lettered and offset-stored on its own merits, exactly as
            // in single-message mode. Messages that already succeeded inside the batch run again —
            // which is why a batch handler must be idempotent.
            LogBatchFellBack(logger, ex, batch.Count, Topic);
            metrics.BatchFallbacks.Add(1, new KeyValuePair<string, object?>("topic", Topic));

            foreach (var consumeResult in batch)
            {
                if (consumeResult.Message?.Value is not null)
                {
                    await ProcessMessageWithRetryAsync(consumeResult, stoppingToken);
                }

                consumer.StoreOffset(consumeResult);
            }
        }
        finally
        {
            var elapsed = Stopwatch.GetElapsedTime(startTimestamp);
            metrics.BatchProcessingDuration.Record(elapsed.TotalMilliseconds, new KeyValuePair<string, object?>("topic", Topic));
        }
    }

    /// <summary>
    /// Stores the highest offset reached on each partition represented in the batch.
    /// </summary>
    /// <remarks>
    /// A batch drained from the local queue can span partitions, so there is no single "last offset"
    /// to store. Note that <c>StoreOffset(TopicPartitionOffset)</c> stores the given offset verbatim
    /// — hence the + 1 — unlike the <c>ConsumeResult</c> overload, which stores the next offset for
    /// you. The offset store holds the <i>next</i> offset to commit for each partition.
    /// </remarks>
    private void StoreBatchOffsets(List<ConsumeResult<TKey, TMessage>> batch)
    {
        var highestByPartition = new Dictionary<TopicPartition, Offset>();
        foreach (var consumeResult in batch)
        {
            if (!highestByPartition.TryGetValue(consumeResult.TopicPartition, out var highest)
                || consumeResult.Offset > highest)
            {
                highestByPartition[consumeResult.TopicPartition] = consumeResult.Offset;
            }
        }

        foreach (var (topicPartition, offset) in highestByPartition)
        {
            consumer.StoreOffset(new TopicPartitionOffset(topicPartition, offset + 1));
        }
    }

    /// <summary>
    /// Commits the offsets stored so far. Best-effort: a commit failure is logged and the loop
    /// continues, since the uncommitted messages are simply redelivered.
    /// </summary>
    private void CommitStoredOffsets()
    {
        try
        {
            consumer.Commit();
        }
        catch (KafkaException ex) when (ex.Error.Code == ErrorCode.Local_NoOffset)
        {
            // Nothing was stored (e.g. offsets already committed) — not an error.
        }
        catch (KafkaException ex)
        {
            // Mirrors the single-message loop's posture towards commit failures (e.g. after a group
            // eviction): surface it, but never crash the host over one. The batch is reprocessed
            // after the rebalance.
            LogBatchCommitFailed(logger, ex, Topic);
        }
    }

    [LoggerMessage(EventId = 116, Level = LogLevel.Debug, Message = "Processing batch of {BatchSize} messages from topic: {Topic}")]
    private static partial void LogProcessingBatch(ILogger logger, int batchSize, string topic);

    [LoggerMessage(EventId = 117, Level = LogLevel.Debug, Message = "Successfully processed batch of {BatchSize} messages from topic: {Topic}")]
    private static partial void LogSuccessfullyProcessedBatch(ILogger logger, int batchSize, string topic);

    [LoggerMessage(EventId = 118, Level = LogLevel.Warning, Message = "Batch of {BatchSize} messages failed; re-processing them one at a time so each is retried and dead lettered individually. Topic: {Topic}")]
    private static partial void LogBatchFellBack(ILogger logger, Exception ex, int batchSize, string topic);

    [LoggerMessage(EventId = 119, Level = LogLevel.Error, Message = "Failed to commit offsets after a batch on topic: {Topic}. The batch will be redelivered.")]
    private static partial void LogBatchCommitFailed(ILogger logger, Exception ex, string topic);
}
