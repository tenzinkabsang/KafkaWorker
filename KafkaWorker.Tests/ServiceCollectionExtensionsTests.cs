using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;

namespace KafkaWorker.Tests;

public class ServiceCollectionExtensionsTests
{
    private static IConfiguration CreateConfiguration(string topic = "test-topic", string groupId = "test-group")
    {
        return new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["KafkaWorker:Connection:BootstrapServers"] = "localhost:9092",
                ["KafkaWorker:Consumer:Topic"] = topic,
                ["KafkaWorker:Consumer:GroupId"] = groupId,
            })
            .Build();
    }

    public class MessageA
    {
        public string Data { get; set; } = string.Empty;
    }

    public class MessageB
    {
        public string Data { get; set; } = string.Empty;
    }

    public class HandlerA : IMessageHandler<MessageA>
    {
        public Task HandleMessageAsync(MessageA message, CancellationToken stoppingToken) => Task.CompletedTask;
    }

    public class HandlerB : IMessageHandler<MessageB>
    {
        public Task HandleMessageAsync(MessageB message, CancellationToken stoppingToken) => Task.CompletedTask;
    }

    public class AltHandlerA : IMessageHandler<MessageA>
    {
        public Task HandleMessageAsync(MessageA message, CancellationToken stoppingToken) => Task.CompletedTask;
    }

    [Fact]
    public void AddKafkaWorker_DuplicateMessageType_ThrowsInvalidOperationException()
    {
        var services = new ServiceCollection();
        var config = CreateConfiguration();

        services.AddKafkaWorker<MessageA, HandlerA>(config);

        var ex = Assert.Throws<InvalidOperationException>(
            () => services.AddKafkaWorker<MessageA, AltHandlerA>(config));

        Assert.Contains("MessageA", ex.Message);
        Assert.Contains("already registered", ex.Message);
    }

    [Fact]
    public void AddKafkaWorker_DifferentMessageTypes_Succeeds()
    {
        var services = new ServiceCollection();
        var config = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["KafkaWorker:Connection:BootstrapServers"] = "localhost:9092",
                ["KafkaWorker:ConsumerA:Topic"] = "topic-a",
                ["KafkaWorker:ConsumerA:GroupId"] = "group-a",
                ["KafkaWorker:ConsumerB:Topic"] = "topic-b",
                ["KafkaWorker:ConsumerB:GroupId"] = "group-b",
            })
            .Build();

        services.AddKafkaWorker<MessageA, HandlerA>(config, configSection: "KafkaWorker:ConsumerA");

        var exception = Record.Exception(
            () => { services.AddKafkaWorker<MessageB, HandlerB>(config, configSection: "KafkaWorker:ConsumerB"); });

        Assert.Null(exception);
    }

    [Fact]
    public void ApplySecurityConfig_SecuredCluster_UsesConfiguredSaslMechanism()
    {
        var connection = new KafkaConnectionConfig
        {
            BootstrapServers = "localhost:9092",
            IsSecuredCluster = true,
            Username = "user",
            Password = "pass",
            SaslMechanism = SaslMechanism.Plain
        };
        var clientConfig = new ConsumerConfig();

        ServiceCollectionExtensions.ApplySecurityConfig(clientConfig, connection);

        Assert.Equal(SecurityProtocol.SaslSsl, clientConfig.SecurityProtocol);
        Assert.Equal(SaslMechanism.Plain, clientConfig.SaslMechanism);
        Assert.Equal("user", clientConfig.SaslUsername);
        Assert.Equal("pass", clientConfig.SaslPassword);
    }

    [Fact]
    public void ApplySecurityConfig_SecuredCluster_DefaultsToScramSha512()
    {
        var connection = new KafkaConnectionConfig
        {
            BootstrapServers = "localhost:9092",
            IsSecuredCluster = true,
            Username = "user",
            Password = "pass"
        };
        var clientConfig = new ConsumerConfig();

        ServiceCollectionExtensions.ApplySecurityConfig(clientConfig, connection);

        Assert.Equal(SaslMechanism.ScramSha512, clientConfig.SaslMechanism);
    }

    [Fact]
    public void ApplySecurityConfig_UnsecuredCluster_DoesNotSetSecuritySettings()
    {
        var connection = new KafkaConnectionConfig { BootstrapServers = "localhost:9092" };
        var clientConfig = new ConsumerConfig();

        ServiceCollectionExtensions.ApplySecurityConfig(clientConfig, connection);

        Assert.Null(clientConfig.SecurityProtocol);
        Assert.Null(clientConfig.SaslMechanism);
    }

    [Fact]
    public void AddKafkaWorker_ConfigureProducer_IsAppliedWhenProducerIsBuilt()
    {
        var services = new ServiceCollection();
        var config = CreateConfiguration();
        var callbackInvoked = false;

        services.AddKafkaWorker<MessageA, HandlerA>(config, configureProducer: producerConfig =>
        {
            callbackInvoked = true;
            producerConfig.MessageTimeoutMs = 1234;
        });

        using var provider = services.BuildServiceProvider();
        var lazyProducer = provider.GetRequiredService<Lazy<IProducer<string, MessageA>>>();

        Assert.False(callbackInvoked);
        _ = lazyProducer.Value;
        Assert.True(callbackInvoked);
    }

    [Fact]
    public void AddKafkaWorkerDeadLetter_RegistersReprocessTrigger()
    {
        var services = new ServiceCollection();
        var config = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["KafkaWorker:Connection:BootstrapServers"] = "localhost:9092",
                ["KafkaWorker:Consumer:Topic"] = "test-topic",
                ["KafkaWorker:Consumer:GroupId"] = "test-group",
                ["KafkaWorker:Consumer:DeadLetterTopic"] = "test-topic-dlq",
            })
            .Build();

        services.AddKafkaWorker<MessageA, HandlerA>(config);
        services.AddKafkaWorkerDeadLetter<MessageA>(config);

        using var provider = services.BuildServiceProvider();

        Assert.NotNull(provider.GetService<IDlqReprocessTrigger<MessageA>>());
    }

    [Fact]
    public void AddKafkaWorker_WithoutDeadLetter_DoesNotRegisterReprocessTrigger()
    {
        var services = new ServiceCollection();
        services.AddKafkaWorker<MessageA, HandlerA>(CreateConfiguration());

        using var provider = services.BuildServiceProvider();

        Assert.Null(provider.GetService<IDlqReprocessTrigger<MessageA>>());
    }

    [Fact]
    public void AddKafkaWorker_ProducerNotCreated_UntilLazyValueAccessed()
    {
        var services = new ServiceCollection();
        var config = CreateConfiguration();
        var producerCreated = false;

        // Pre-register a flagging producer factory; AddKafkaWorker's TryAdd defers to it.
        services.AddSingleton<IProducer<string, MessageA>>(sp =>
        {
            producerCreated = true;
            return Substitute.For<IProducer<string, MessageA>>();
        });
        services.AddKafkaWorker<MessageA, HandlerA>(config);

        using var provider = services.BuildServiceProvider();
        var lazyProducer = provider.GetRequiredService<Lazy<IProducer<string, MessageA>>>();

        Assert.False(producerCreated);
        _ = lazyProducer.Value;
        Assert.True(producerCreated);
    }
}
