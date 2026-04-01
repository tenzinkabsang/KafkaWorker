using KafkaWorker.Sample;

namespace KafkaWorker.Worker;

public sealed class OrderMessageHandlerAvro(ILogger<OrderMessageHandlerAvro> logger) : IMessageHandler<AvroOrderMessage>
{
    // In-memory storage for processed orders (for demonstration purposes)
    private readonly IList<AvroOrderMessage> _orders = [];

    public async Task HandleMessageAsync(AvroOrderMessage message, CancellationToken stoppingToken)
    {
        // Simulate invalid message detection
        if (string.IsNullOrEmpty(message.SellerId))
            throw new InvalidMessageException("SellerId cannot be null");

        logger.LogInformation("Processing order {OrderId} for seller {SellerId} with total {Total}", message.OrderId, message.SellerId, message.Total);

        _orders.Add(message);

        logger.LogInformation($"{nameof(OrderMessageHandlerAvro)} successfully processed {message.OrderId}");
    }
}

