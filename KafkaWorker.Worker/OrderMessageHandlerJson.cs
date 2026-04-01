namespace KafkaWorker.Worker;

public sealed class OrderMessageHandlerJson(ILogger<OrderMessageHandlerJson> logger) : IMessageHandler<OrderMessage>
{
    // In-memory storage for processed orders (for demonstration purposes)
    private readonly IList<OrderMessage> _orders = [];

    public async Task HandleMessageAsync(OrderMessage message, CancellationToken stoppingToken)
    {
        // Simulate invalid message detection
        if (string.IsNullOrEmpty(message.SellerId))
            throw new InvalidMessageException("SellerId cannot be null");

        logger.LogInformation("Processing order {OrderId} for seller {SellerId} with total {Total}", message.OrderId, message.SellerId, message.Total);

        _orders.Add(message);

        logger.LogInformation($"{nameof(OrderMessageHandlerJson)} successfully processed {message.OrderId}");
    }
}

