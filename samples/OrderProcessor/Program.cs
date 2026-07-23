using Confluent.Kafka;
using KafkaWorker;
using OrderProcessor;

var builder = Host.CreateApplicationBuilder(args);

// One registration wires up the consume loop, offset management, retry,
// dead letter queue, and a scoped OrderMessageHandler per message.
builder.Services.AddKafkaWorker<OrderMessage, OrderMessageHandler>(
    builder.Configuration,
    // Earliest so the demo works no matter which order you start the apps in.
    configureConsumer: config => config.AutoOffsetReset = AutoOffsetReset.Earliest);

// Periodically reprocesses failed messages from the DLQ in place (see appsettings.json).
builder.Services.AddKafkaWorkerDeadLetter<OrderMessage>(builder.Configuration);

builder.Build().Run();
