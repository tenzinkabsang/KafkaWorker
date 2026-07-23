using System.Text.Json;
using Confluent.Kafka;

// Publishes sample messages that exercise every failure path in the OrderProcessor.
// Run the OrderProcessor first, then run this and watch its logs.

const string Topic = "sample.orders";

using var producer = new ProducerBuilder<string, string>(
    new ProducerConfig { BootstrapServers = "localhost:9092" }).Build();

// 1) Three valid orders — processed successfully.
for (var i = 1; i <= 3; i++)
{
    var order = new { OrderId = $"ORD-{i:000}", CustomerId = $"CUST-{i}", Total = 25.50m * i };
    await SendAsync(order.OrderId, JsonSerializer.Serialize(order));
}

// 2) A simulated transient failure — retried with backoff, then dead-lettered.
//    The DLQ consumer reprocesses it in place every minute until max attempts.
await SendAsync("ORD-FLAKY", JsonSerializer.Serialize(
    new { OrderId = "ORD-FLAKY", CustomerId = "FLAKY", Total = 99.99m }));

// 3) An invalid message — InvalidMessageException: no retries, straight to the
//    DLQ with the invalid-message header, permanently skipped by the reprocessor.
await SendAsync("ORD-INVALID", JsonSerializer.Serialize(
    new { OrderId = "", CustomerId = "CUST-X", Total = 10m }));

// 4) A poison message — not valid JSON. The consumer logs it at Critical,
//    commits past it, and keeps running (no crash loop, no wedged consumer).
await SendAsync("ORD-POISON", "this is not json {");

producer.Flush(TimeSpan.FromSeconds(10));
Console.WriteLine();
Console.WriteLine("All sample messages sent — watch the OrderProcessor logs.");
Console.WriteLine("Browse the topics at http://localhost:888 (kafka-ui).");

async Task SendAsync(string key, string value)
{
    await producer.ProduceAsync(Topic, new Message<string, string> { Key = key, Value = value });
    Console.WriteLine($"sent {key}");
}
