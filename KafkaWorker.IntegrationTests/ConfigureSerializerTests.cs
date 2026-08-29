using Confluent.Kafka;
using KafkaWorker.Avro;
using KafkaWorker.JsonSchema;
using KafkaWorker.Protobuf;
using KafkaWorker.Proto;
using KafkaWorker.Sample;
using KafkaWorker.Worker;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace KafkaWorker.IntegrationTests;

/// <summary>
/// Verifies that the configureSerializer callback on the Schema Registry add-ons is applied to the
/// serializer used for dead letter publishing, and only when the producer is actually created.
/// These tests exercise DI wiring only — no broker connection is made.
/// </summary>
public class ConfigureSerializerTests
{
    private static IConfiguration CreateConfiguration() => new ConfigurationBuilder()
        .AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["KafkaWorker:Connection:BootstrapServers"] = "localhost:9092",
            ["KafkaWorker:Connection:SchemaRegistryUrls"] = "localhost:8082",
            ["KafkaWorker:AvroConsumer:Topic"] = "avro-topic",
            ["KafkaWorker:AvroConsumer:GroupId"] = "avro-group",
            ["KafkaWorker:ProtoConsumer:Topic"] = "proto-topic",
            ["KafkaWorker:ProtoConsumer:GroupId"] = "proto-group",
            ["KafkaWorker:JsonConsumer:Topic"] = "json-topic",
            ["KafkaWorker:JsonConsumer:GroupId"] = "json-group",
        })
        .Build();

    [Fact]
    public void AddKafkaWorkerAvro_ConfigureSerializerCallback_InvokedWhenProducerIsCreated()
    {
        var services = new ServiceCollection();
        var invoked = false;

        services.AddKafkaWorkerAvro<AvroOrderMessage, OrderMessageHandlerAvro>(
            CreateConfiguration(),
            configSection: "KafkaWorker:AvroConsumer",
            configureSerializer: config =>
            {
                invoked = true;
                config.AutoRegisterSchemas = false;
                config.UseLatestVersion = true;
            });

        using var provider = services.BuildServiceProvider();
        Assert.False(invoked);

        _ = provider.GetRequiredService<IProducer<string, AvroOrderMessage>>();
        Assert.True(invoked);
    }

    [Fact]
    public void AddKafkaWorkerProtobuf_ConfigureSerializerCallback_InvokedWhenProducerIsCreated()
    {
        var services = new ServiceCollection();
        var invoked = false;

        services.AddKafkaWorkerProtobuf<ProtobufOrderMessage, OrderMessageHandlerProto>(
            CreateConfiguration(),
            configSection: "KafkaWorker:ProtoConsumer",
            configureSerializer: config =>
            {
                invoked = true;
                config.AutoRegisterSchemas = false;
            });

        using var provider = services.BuildServiceProvider();
        Assert.False(invoked);

        _ = provider.GetRequiredService<IProducer<string, ProtobufOrderMessage>>();
        Assert.True(invoked);
    }

    [Fact]
    public void AddKafkaWorkerRegistryJson_ConfigureSerializerCallback_InvokedWhenProducerIsCreated()
    {
        var services = new ServiceCollection();
        var invoked = false;

        services.AddKafkaWorkerRegistryJson<OrderMessage, OrderMessageHandlerRegistryJson>(
            CreateConfiguration(),
            configSection: "KafkaWorker:JsonConsumer",
            configureSerializer: config =>
            {
                invoked = true;
                config.AutoRegisterSchemas = false;
            });

        using var provider = services.BuildServiceProvider();
        Assert.False(invoked);

        _ = provider.GetRequiredService<IProducer<string, OrderMessage>>();
        Assert.True(invoked);
    }

    [Fact]
    public void ConfigureSerializerOmitted_ProducerStillResolves()
    {
        var services = new ServiceCollection();
        services.AddKafkaWorkerAvro<AvroOrderMessage, OrderMessageHandlerAvro>(
            CreateConfiguration(), configSection: "KafkaWorker:AvroConsumer");

        using var provider = services.BuildServiceProvider();

        Assert.NotNull(provider.GetRequiredService<IProducer<string, AvroOrderMessage>>());
    }
}
