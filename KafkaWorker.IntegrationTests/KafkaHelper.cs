using System.Diagnostics;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Confluent.SchemaRegistry;

namespace KafkaWorker.IntegrationTests;

public static class KafkaHelper
{
    private const string BootstrapServers = "localhost:9092";
    private const string SchemaRegistryUrl = "localhost:8082";
    private static readonly TimeSpan ProduceTimeout = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Deletes and recreates a topic, publishes a seed message with a schema-registry serializer
    /// to register the schema, then clears the topic so the consumer starts clean.
    /// </summary>
    public static async Task InitializeTopicAsync<TMessage>(
        string topic,
        TMessage seedMessage,
        Func<CachedSchemaRegistryClient, ISerializer<TMessage>> serializer) where TMessage : class
    {
        await DeleteAndRecreateTopicAsync(topic);
        await PublishMessageAsync(topic, "1", seedMessage, serializer);
        await ClearMessagesAsync(topic);
    }

    /// <summary>
    /// Deletes and recreates a topic, publishes a seed message as a plain JSON string,
    /// then clears the topic so the consumer starts clean.
    /// </summary>
    public static async Task InitializeTopicAsync(string topic, string seedMessage)
    {
        await DeleteAndRecreateTopicAsync(topic);
        await PublishMessageAsync(topic, "1", seedMessage);
        await ClearMessagesAsync(topic);
    }

    /// <summary>
    /// Deletes and recreates a topic with no messages (useful for DLQ topics).
    /// </summary>
    public static async Task InitializeEmptyTopicAsync(string topic, int partitions = 1)
    {
        await DeleteAndRecreateTopicAsync(topic, partitions);
    }

    /// <summary>
    /// Returns how many records each partition of a topic holds, so a test can prove its messages
    /// actually spread across partitions rather than piling onto one.
    /// </summary>
    public static IReadOnlyList<long> GetPartitionMessageCounts(string topic, int partitions)
    {
        using var consumer = new ConsumerBuilder<string, string>(new ConsumerConfig
        {
            BootstrapServers = BootstrapServers,
            GroupId = $"watermark-probe-{Guid.NewGuid():N}"
        }).Build();

        var counts = new List<long>();
        for (var partition = 0; partition < partitions; partition++)
        {
            var watermarks = consumer.QueryWatermarkOffsets(
                new TopicPartition(topic, new Partition(partition)), TimeSpan.FromSeconds(10));
            counts.Add(watermarks.High.Value - watermarks.Low.Value);
        }

        consumer.Close();
        return counts;
    }

    /// <summary>
    /// Publishes a message using a schema-registry-aware serializer (Avro, Protobuf, etc.).
    /// </summary>
    public static async Task PublishMessageAsync<TMessage>(
        string topic,
        string key,
        TMessage message,
        Func<CachedSchemaRegistryClient, ISerializer<TMessage>> serializer) where TMessage : class
    {
        using var schemaRegistry = new CachedSchemaRegistryClient(new SchemaRegistryConfig { Url = SchemaRegistryUrl });
        var config = new ProducerConfig { BootstrapServers = BootstrapServers };

        using var producer = new ProducerBuilder<string, TMessage>(config)
            .SetValueSerializer(serializer(schemaRegistry))
            .Build();

        using var cts = new CancellationTokenSource(ProduceTimeout);
        await producer.ProduceAsync(topic, new Message<string, TMessage> { Key = key, Value = message }, cts.Token);
        producer.Flush();
    }

    /// <summary>
    /// Publishes a plain JSON string message (no schema registry).
    /// </summary>
    public static async Task PublishMessageAsync(string topic, string key, string message)
    {
        var config = new ProducerConfig { BootstrapServers = BootstrapServers };

        using var producer = new ProducerBuilder<string, string>(config).Build();

        using var cts = new CancellationTokenSource(ProduceTimeout);
        await producer.ProduceAsync(topic, new Message<string, string> { Key = key, Value = message }, cts.Token);
        producer.Flush();
    }

    /// <summary>
    /// Deletes and recreates a topic, retrying creation until Kafka finishes the deletion.
    /// </summary>
    private static async Task DeleteAndRecreateTopicAsync(string topic, int partitions = 1)
    {
        using var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = BootstrapServers }).Build();

        try
        {
            await adminClient.DeleteTopicsAsync([topic]);
        }
        catch (DeleteTopicsException ex)
        {
            Debug.WriteLine($"DeleteTopicsException: {ex.Message}");
        }

        // Kafka only marks the topic as deleted — it won't accept a new topic
        // of the same name until the deletion is fully complete.
        bool created = false;
        while (!created)
        {
            try
            {
                await adminClient.CreateTopicsAsync([new TopicSpecification { Name = topic, NumPartitions = partitions, ReplicationFactor = 1 }]);
                created = true;
            }
            catch (CreateTopicsException ex)
            {
                Debug.WriteLine($"CreateTopicsException: {ex.Message}");
                await Task.Delay(50);
            }
        }
    }

    private static async Task ClearMessagesAsync(string topic)
    {
        using var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = BootstrapServers }).Build();
        await adminClient.DeleteRecordsAsync([new TopicPartitionOffset(new TopicPartition(topic, 0), Offset.End)]);
    }
}
