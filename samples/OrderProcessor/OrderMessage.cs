namespace OrderProcessor;

public record OrderMessage
{
    public required string OrderId { get; init; }
    public required string CustomerId { get; init; }
    public required decimal Total { get; init; }
}
