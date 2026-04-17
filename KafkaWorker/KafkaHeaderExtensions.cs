using System.Text;
using Confluent.Kafka;

namespace KafkaWorker;

/// <summary>
/// Extension methods for reading and writing Kafka message headers with UTF-8 encoding.
/// Centralizes the encoding/decoding logic used by Kafka consumers for header operations.
/// </summary>
internal static class KafkaHeaderExtensions
{
    public static string? GetValue(this Headers? headers, string key)
    {
        var header = headers?.FirstOrDefault(h => h.Key == key);
        return header != null ? Encoding.UTF8.GetString(header.GetValueBytes()) : null;
    }

    public static void AddUtf8(this Headers headers, string key, string value)
        => headers.Add(key, Encoding.UTF8.GetBytes(value));

    public static string GetOriginalTopic(this Headers headers)
        => headers.GetValue(KafkaHeaders.OriginalTopic) ?? string.Empty;

    public static string GetBatchId(this Headers headers)
        => headers.GetValue(KafkaHeaders.BatchId) ?? string.Empty;

    public static int GetReprocessAttemptCount(this Headers headers)
    {
        var headerValue = headers.GetValue(KafkaHeaders.ReprocessedAttempt);
        return int.TryParse(headerValue, out var count) ? count : 0;
    }

    public static bool IsInvalidMessage(this Headers headers)
        => string.Equals(headers.GetValue(KafkaHeaders.InvalidMessage), "true", StringComparison.OrdinalIgnoreCase);

    public static string? GetFailedConsumerGroupId(this Headers headers)
        => headers.GetValue(KafkaHeaders.FailedConsumerGroupId);
}
