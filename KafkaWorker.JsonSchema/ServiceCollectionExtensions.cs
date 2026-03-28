using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace KafkaWorker.JsonSchema;

/// <summary>
/// Extension methods for registering a JSON Schema Registry-based Kafka consumer with dependency injection.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers a hosted Kafka consumer that deserializes messages using JSON format with Schema Registry.
    /// </summary>
    /// <typeparam name="TMessage">The message type to consume.</typeparam>
    /// <typeparam name="TProcessor">The message processor implementation type. Registered as a scoped service.</typeparam>
    /// <param name="services">The service collection to add services to.</param>
    /// <param name="configuration">The application configuration containing Kafka settings.</param>
    /// <param name="configSection">The configuration section path for consumer settings. Defaults to <c>KafkaWorker:Consumer</c>.</param>
    /// <param name="configureConsumer">Optional callback to configure the underlying Confluent <see cref="ConsumerConfig"/>.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddKafkaWorkerRegistryJson<TMessage, TProcessor>(
        this IServiceCollection services, IConfiguration configuration,
        string configSection = KafkaWorkerConfig.Section,
        Action<ConsumerConfig>? configureConsumer = null)
        where TMessage : class
        where TProcessor : class, IMessageHandler<TMessage>
        => AddKafkaWorkerRegistryJson<string, TMessage, TProcessor>(services, configuration, configSection, configureConsumer);

    /// <inheritdoc cref="AddKafkaWorkerRegistryJson{TMessage, TProcessor}"/>
    /// <typeparam name="TKey">The message key type.</typeparam>
    /// <typeparam name="TMessage">The message type to consume.</typeparam>
    /// <typeparam name="TProcessor">The message processor implementation type.</typeparam>
    public static IServiceCollection AddKafkaWorkerRegistryJson<TKey, TMessage, TProcessor>(
        this IServiceCollection services, IConfiguration configuration,
        string configSection = KafkaWorkerConfig.Section,
        Action<ConsumerConfig>? configureConsumer = null)
        where TMessage : class
        where TProcessor : class, IMessageHandler<TMessage>
    {
        var schemaRegistry = GetSchemaRegistry(services, configuration);

        services.TryAddSingleton<IDeserializer<TMessage>>(sp =>
            new JsonDeserializer<TMessage>().AsSyncOverAsync());

        KafkaWorker.ServiceCollectionExtensions.RegisterProducer<TKey, TMessage>(services, configuration, b =>
        {
            b.SetValueSerializer(new JsonSerializer<TMessage>(schemaRegistry).AsSyncOverAsync());
        });

        return KafkaWorker.ServiceCollectionExtensions.RegisterHostedConsumer<TKey, TMessage, TProcessor>(
            services, configuration, configSection, configureConsumer, b =>
            {
                b.SetValueDeserializer(new JsonDeserializer<TMessage>().AsSyncOverAsync());
            });
    }

    private static CachedSchemaRegistryClient GetSchemaRegistry(IServiceCollection services, IConfiguration configuration)
    {
        var existing = services.FirstOrDefault(s => s.ServiceType == typeof(ISchemaRegistryClient));
        if (existing?.ImplementationInstance is CachedSchemaRegistryClient cached)
            return cached;

        var kafkaConnection = KafkaWorker.ServiceCollectionExtensions.GetKafkaConnectionConfig(configuration);
        var schemaRegistry = new CachedSchemaRegistryClient(new SchemaRegistryConfig { Url = kafkaConnection.SchemaRegistryUrls });
        services.TryAddSingleton<ISchemaRegistryClient>(schemaRegistry);
        return schemaRegistry;
    }
}
