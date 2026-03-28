namespace KafkaWorker.Worker;

public record OrderMessage
{
    public int OrderId { get; set; }
    public string? SellerId { get; set; }
    public DateTime OrderDate { get; set; }
    public decimal Total { get; set; }
}
