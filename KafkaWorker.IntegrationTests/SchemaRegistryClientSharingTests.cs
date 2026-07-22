using Confluent.Kafka;
using Confluent.SchemaRegistry;
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
/// Verifies that registering multiple Schema Registry-based formats in one host shares a single
/// ISchemaRegistryClient. These tests exercise DI wiring only — no broker connection is made.
/// </summary>
public class SchemaRegistryClientSharingTests
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

    private static IServiceCollection RegisterAllThreeFormats(IServiceCollection services, IConfiguration config)
    {
        services.AddKafkaWorkerAvro<AvroOrderMessage, OrderMessageHandlerAvro>(config, configSection: "KafkaWorker:AvroConsumer");
        services.AddKafkaWorkerProtobuf<ProtobufOrderMessage, OrderMessageHandlerProto>(config, configSection: "KafkaWorker:ProtoConsumer");
        services.AddKafkaWorkerRegistryJson<OrderMessage, OrderMessageHandlerRegistryJson>(config, configSection: "KafkaWorker:JsonConsumer");
        return services;
    }

    [Fact]
    public void RegisteringMultipleFormats_DoesNotThrow()
    {
        var services = new ServiceCollection();

        var exception = Record.Exception(() => RegisterAllThreeFormats(services, CreateConfiguration()));

        Assert.Null(exception);
    }

    [Fact]
    public void RegisteringMultipleFormats_AddsExactlyOneSchemaRegistryClientDescriptor()
    {
        var services = RegisterAllThreeFormats(new ServiceCollection(), CreateConfiguration());

        Assert.Equal(1, services.Count(sd => sd.ServiceType == typeof(ISchemaRegistryClient)));
    }

    [Fact]
    public void RegisteringMultipleFormats_AllResolveTheSameClientInstance()
    {
        var services = RegisterAllThreeFormats(new ServiceCollection(), CreateConfiguration());
        using var provider = services.BuildServiceProvider();

        var first = provider.GetRequiredService<ISchemaRegistryClient>();
        var second = provider.GetRequiredService<ISchemaRegistryClient>();

        Assert.Same(first, second);

        // Every format's deserializer resolves against the shared client without throwing
        Assert.NotNull(provider.GetRequiredService<IDeserializer<AvroOrderMessage>>());
        Assert.NotNull(provider.GetRequiredService<IDeserializer<ProtobufOrderMessage>>());
        Assert.NotNull(provider.GetRequiredService<IDeserializer<OrderMessage>>());
    }

    [Fact]
    public void UserRegisteredSchemaRegistryClient_IsHonoredByAllFormats()
    {
        var services = new ServiceCollection();
        ISchemaRegistryClient? userClient = null;

        // Factory-style registration — the old ImplementationInstance check could not see these
        services.AddSingleton<ISchemaRegistryClient>(sp =>
        {
            userClient = new CachedSchemaRegistryClient(new SchemaRegistryConfig { Url = "localhost:9999" });
            return userClient;
        });
        RegisterAllThreeFormats(services, CreateConfiguration());

        using var provider = services.BuildServiceProvider();
        var resolved = provider.GetRequiredService<ISchemaRegistryClient>();

        Assert.Same(userClient, resolved);
    }
}
