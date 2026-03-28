using KafkaWorker.Proto;

namespace KafkaWorker.Worker;

public sealed class OrderMessageProcessorProto(ILogger<OrderMessageProcessorProto> logger) : IMessageHandler<ProtobufOrderMessage>
{
    // In-memory storage for processed orders (for demonstration purposes)
    private readonly IList<ProtobufOrderMessage> _orders = [];

    public async Task HandleMessageAsync(ProtobufOrderMessage message, CancellationToken stoppingToken)
    {
        // Simulate invalid message detection
        if (string.IsNullOrEmpty(message.SellerId))
            throw new InvalidMessageException("SellerId cannot be null");

        logger.LogInformation("Processing order {OrderId} for seller {SellerId} with total {Total}", message.OrderId, message.SellerId, message.Total);

        _orders.Add(message);

        logger.LogInformation($"{nameof(OrderMessageProcessorProto)} successfully processed {message.OrderId}");
    }
}

