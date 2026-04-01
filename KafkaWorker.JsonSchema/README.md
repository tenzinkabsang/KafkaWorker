# KafkaWorker.JsonSchema

JSON Schema deserialization add-on for [KafkaWorker](https://www.nuget.org/packages/KafkaWorker/). Adds `AddKafkaWorkerRegistryJson` which wires up Confluent Schema Registry and a JSON deserializer alongside the standard KafkaWorker consume loop, retry, and DLQ pipeline.

Use this package when your messages are serialized as JSON with schema validation via Confluent Schema Registry. For plain JSON without Schema Registry, use `AddKafkaWorker` from the core [KafkaWorker](https://www.nuget.org/packages/KafkaWorker/) package instead.

## Installation

```bash
dotnet add package KafkaWorker
dotnet add package KafkaWorker.JsonSchema
```

## Usage

```csharp
// Program.cs
builder.Services.AddKafkaWorkerRegistryJson<OrderMessage, OrderMessageHandler>(builder.Configuration);
```

```csharp
public class OrderMessageHandler(ILogger<OrderMessageHandler> logger)
    : IMessageHandler<OrderMessage>
{
    public Task HandleMessageAsync(OrderMessage message, CancellationToken stoppingToken)
    {
        logger.LogInformation("Received order {OrderId}", message.OrderId);
        return Task.CompletedTask;
    }
}
```

## Configuration

```json
{
  "KafkaWorker": {
    "Consumer": {
      "BootstrapServers": "localhost:9092",
      "Topic": "orders",
      "GroupId": "orders-consumer",
      "SchemaRegistryUrl": "http://localhost:8081"
    }
  }
}
```

`SchemaRegistryUrl` is required. All other KafkaWorker options (retry, DLQ, etc.) work the same as the core package.

## Custom Key Type

```csharp
builder.Services.AddKafkaWorkerRegistryJson<long, OrderMessage, OrderMessageHandler>(builder.Configuration);
```

## Documentation

Full documentation, configuration reference, and DLQ setup at [github.com/tenzinkabsang/KafkaWorker](https://github.com/tenzinkabsang/KafkaWorker).
