# KafkaWorker.Avro

Avro deserialization add-on for [KafkaWorker](https://www.nuget.org/packages/KafkaWorker/). Adds `AddKafkaWorkerAvro` which wires up Confluent Schema Registry and an Avro deserializer alongside the standard KafkaWorker consume loop, retry, and DLQ pipeline.

## Installation

```bash
dotnet add package KafkaWorker
dotnet add package KafkaWorker.Avro
```

## Usage

```csharp
// Program.cs
builder.Services.AddKafkaWorkerAvro<OrderMessage, OrderMessageProcessor>(builder.Configuration);
```

```csharp
public class OrderMessageProcessor(ILogger<OrderMessageProcessor> logger)
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
builder.Services.AddKafkaWorkerAvro<long, OrderMessage, OrderMessageProcessor>(builder.Configuration);
```

## Documentation

Full documentation, configuration reference, and DLQ setup at [github.com/tenzinkabsang/KafkaWorker](https://github.com/tenzinkabsang/KafkaWorker).
