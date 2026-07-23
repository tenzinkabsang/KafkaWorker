using KafkaWorker;

namespace OrderProcessor;

/// <summary>
/// The only class you need to write. The sample producer sends messages that
/// exercise each failure path — watch the logs to see how the library reacts.
/// </summary>
public class OrderMessageHandler(ILogger<OrderMessageHandler> logger) : IMessageHandler<OrderMessage>
{
    public Task HandleMessageAsync(OrderMessage message, CancellationToken stoppingToken)
    {
        // Permanent failure: no retries — goes straight to the DLQ with
        // the invalid-message header, and is never reprocessed.
        if (string.IsNullOrWhiteSpace(message.OrderId))
            throw new InvalidMessageException("OrderId is required", message);

        // Simulated downstream outage: retried with exponential backoff, then
        // dead-lettered. The DLQ consumer retries it in place every minute until
        // DeadLetterMaxReprocessAttempts is reached (it always fails in this demo,
        // so you can watch the full lifecycle end in a terminal skip).
        if (message.CustomerId == "FLAKY")
            throw new InvalidOperationException($"Downstream service unavailable for order {message.OrderId} (simulated)");

        logger.LogInformation("Processed order {OrderId} for {CustomerId}: ${Total}",
            message.OrderId, message.CustomerId, message.Total);
        return Task.CompletedTask;
    }
}
