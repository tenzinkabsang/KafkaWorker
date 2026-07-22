using Microsoft.Extensions.Logging;

namespace KafkaWorker;

/// <summary>
/// Shared LoggerMessage definitions for librdkafka client log and error callbacks
/// (<c>SetLogHandler</c>/<c>SetErrorHandler</c>), so client-emitted text is never
/// used as a log message template.
/// </summary>
internal static partial class KafkaClientLogging
{
    [LoggerMessage(EventId = 300, Level = LogLevel.Debug, Message = "Kafka client {ClientName}: {ClientLog}")]
    public static partial void LogClientMessage(ILogger logger, string clientName, string clientLog);

    [LoggerMessage(EventId = 301, Level = LogLevel.Error, Message = "Kafka client error: {Reason} (IsFatal: {IsFatal})")]
    public static partial void LogClientError(ILogger logger, string reason, bool isFatal);
}
