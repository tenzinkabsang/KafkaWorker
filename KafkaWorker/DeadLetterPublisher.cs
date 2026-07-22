using Confluent.Kafka;
using Polly;

namespace KafkaWorker;

/// <summary>
/// Shared mechanism for writing a message to a Kafka topic with dead-letter headers.
/// Used by both the main consumer (publishing failed messages to the DLQ) and the DLQ
/// consumer (re-enqueuing messages that fail in-place reprocessing).
/// </summary>
/// <remarks>
/// This helper is policy-free: it only builds the merged header set and produces with the
/// supplied resilience pipeline. Failure-handling semantics (best-effort vs. stop-the-batch)
/// remain the responsibility of each caller.
/// </remarks>
internal static class DeadLetterPublisher
{
    /// <summary>
    /// Produces <paramref name="value"/> to <paramref name="topic"/> with a merged header set.
    /// </summary>
    /// <remarks>
    /// Header precedence: <paramref name="overrideHeaders"/> win. Any header from
    /// <paramref name="sourceHeaders"/> whose key is also present in <paramref name="overrideHeaders"/>
    /// is dropped, guaranteeing exactly one header per overridden key with the new value.
    /// All other source headers are copied through unchanged.
    /// </remarks>
    /// <param name="producer">The Kafka producer used to publish the message.</param>
    /// <param name="topic">The destination topic.</param>
    /// <param name="key">The message key to preserve.</param>
    /// <param name="value">The message value to preserve.</param>
    /// <param name="overrideHeaders">Headers that take precedence over any matching source headers.</param>
    /// <param name="sourceHeaders">The original message headers to copy through (minus overridden keys).</param>
    /// <param name="resiliencePipeline">The resilience pipeline wrapping the produce operation.</param>
    /// <param name="cancellationToken">A token to cancel the produce operation.</param>
    public static async Task PublishAsync<TKey, TMessage>(
        IProducer<TKey, TMessage> producer,
        string topic,
        TKey key,
        TMessage value,
        Headers overrideHeaders,
        Headers? sourceHeaders,
        ResiliencePipeline resiliencePipeline,
        CancellationToken cancellationToken)
    {
        var headers = new Headers();
        var overriddenKeys = new HashSet<string>(StringComparer.Ordinal);

        foreach (var header in overrideHeaders)
        {
            headers.Add(header.Key, header.GetValueBytes());
            overriddenKeys.Add(header.Key);
        }

        // Copy remaining source headers, skipping any key we just overrode so we never
        // emit duplicate headers (e.g. batch-id, reprocessed-attempt).
        if (sourceHeaders is not null)
        {
            foreach (var header in sourceHeaders)
            {
                if (overriddenKeys.Contains(header.Key))
                {
                    continue;
                }

                headers.Add(header.Key, header.GetValueBytes());
            }
        }

        var message = new Message<TKey, TMessage>
        {
            Key = key,
            Value = value,
            Headers = headers
        };

        await resiliencePipeline.ExecuteAsync(
            async token => await producer.ProduceAsync(topic, message, token),
            cancellationToken);
    }
}
