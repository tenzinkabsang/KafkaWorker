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
builder.Services.AddKafkaWorkerAvro<OrderMessage, OrderMessageHandler>(builder.Configuration);
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
    "Connection": {
      "BootstrapServers": "localhost:9092",
      "SchemaRegistryUrls": "http://localhost:8081"
    },
    "Consumer": {
      "GroupId": "orders-consumer",
      "Topic": "orders"
    }
  }
}
```

`SchemaRegistryUrls` (under `KafkaWorker:Connection`) is required. For registries that need basic auth (e.g. Confluent Cloud), also set `SchemaRegistryUsername` and `SchemaRegistryPassword`. All other KafkaWorker options (retry, DLQ, etc.) work the same as the core package.

## Serializer Options

An optional `configureSerializer` callback customizes the `AvroSerializerConfig` used when publishing failed messages to the dead letter topic. By default the first DLQ publish auto-registers a `{DeadLetterTopic}-value` subject in Schema Registry; if your registry denies client-side registration, pre-register that subject or disable auto-registration:

```csharp
builder.Services.AddKafkaWorkerAvro<OrderMessage, OrderMessageHandler>(
    builder.Configuration,
    configureSerializer: config =>
    {
        config.AutoRegisterSchemas = false;
        config.UseLatestVersion = true;
    });
```

## Custom Key Type

```csharp
builder.Services.AddKafkaWorkerAvro<long, OrderMessage, OrderMessageHandler>(builder.Configuration);
```

## Documentation

Full documentation, configuration reference, and DLQ setup at [tenzinkabsang.github.io/KafkaWorker](https://tenzinkabsang.github.io/KafkaWorker/).
