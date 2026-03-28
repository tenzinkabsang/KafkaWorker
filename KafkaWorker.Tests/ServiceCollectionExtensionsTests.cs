using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

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

    public class ProcessorA : IMessageHandler<MessageA>
    {
        public Task HandleMessageAsync(MessageA message, CancellationToken stoppingToken) => Task.CompletedTask;
    }

    public class ProcessorB : IMessageHandler<MessageB>
    {
        public Task HandleMessageAsync(MessageB message, CancellationToken stoppingToken) => Task.CompletedTask;
    }

    public class AltProcessorA : IMessageHandler<MessageA>
    {
        public Task HandleMessageAsync(MessageA message, CancellationToken stoppingToken) => Task.CompletedTask;
    }

    [Fact]
    public void AddKafkaWorker_DuplicateMessageType_ThrowsInvalidOperationException()
    {
        var services = new ServiceCollection();
        var config = CreateConfiguration();

        services.AddKafkaWorker<MessageA, ProcessorA>(config);

        var ex = Assert.Throws<InvalidOperationException>(
            () => services.AddKafkaWorker<MessageA, AltProcessorA>(config));

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

        services.AddKafkaWorker<MessageA, ProcessorA>(config, configSection: "KafkaWorker:ConsumerA");

        var exception = Record.Exception(
            () => { services.AddKafkaWorker<MessageB, ProcessorB>(config, configSection: "KafkaWorker:ConsumerB"); });

        Assert.Null(exception);
    }
}
