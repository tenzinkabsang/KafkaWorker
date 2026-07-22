using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace KafkaWorker.Avro;

/// <summary>
/// Extension methods for registering an Avro-based Kafka consumer with dependency injection.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers a hosted Kafka consumer that deserializes messages using Avro format with Schema Registry.
    /// </summary>
    /// <typeparam name="TMessage">The Avro-generated message type to consume.</typeparam>
    /// <typeparam name="THandler">The message handler implementation type. Registered as a scoped service.</typeparam>
    /// <param name="services">The service collection to add services to.</param>
    /// <param name="configuration">The application configuration containing Kafka settings.</param>
    /// <param name="configSection">The configuration section path for consumer settings. Defaults to <c>KafkaWorker:Consumer</c>.</param>
    /// <param name="configureConsumer">Optional callback to configure the underlying Confluent <see cref="ConsumerConfig"/>.</param>
    /// <param name="configureProducer">Optional callback to configure the underlying Confluent <see cref="ProducerConfig"/>
    /// used for dead letter publishing.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddKafkaWorkerAvro<TMessage, THandler>(
        this IServiceCollection services, IConfiguration configuration,
        string configSection = KafkaWorkerConfig.Section,
        Action<ConsumerConfig>? configureConsumer = null,
        Action<ProducerConfig>? configureProducer = null)
        where TMessage : class
        where THandler : class, IMessageHandler<TMessage>
        => AddKafkaWorkerAvro<string, TMessage, THandler>(services, configuration, configSection, configureConsumer, configureProducer);

    /// <inheritdoc cref="AddKafkaWorkerAvro{TMessage, THandler}"/>
    /// <typeparam name="TKey">The message key type.</typeparam>
    /// <typeparam name="TMessage">The Avro-generated message type to consume.</typeparam>
    /// <typeparam name="THandler">The message handler implementation type.</typeparam>
    public static IServiceCollection AddKafkaWorkerAvro<TKey, TMessage, THandler>(
        this IServiceCollection services, IConfiguration configuration,
        string configSection = KafkaWorkerConfig.Section,
        Action<ConsumerConfig>? configureConsumer = null,
        Action<ProducerConfig>? configureProducer = null)
        where TMessage : class
        where THandler : class, IMessageHandler<TMessage>
    {
        RegisterSchemaRegistryClient(services, configuration);

        services.TryAddSingleton<IDeserializer<TMessage>>(sp =>
            new AvroDeserializer<TMessage>(sp.GetRequiredService<ISchemaRegistryClient>()).AsSyncOverAsync());

        KafkaWorker.ServiceCollectionExtensions.RegisterProducer<TKey, TMessage>(services, configuration, (sp, b) =>
        {
            b.SetValueSerializer(new AvroSerializer<TMessage>(sp.GetRequiredService<ISchemaRegistryClient>()).AsSyncOverAsync());
        }, configureProducer);

        return KafkaWorker.ServiceCollectionExtensions.RegisterHostedConsumer<TKey, TMessage, THandler>(
            services, configuration, configSection, configureConsumer, (sp, b) =>
            {
                b.SetValueDeserializer(sp.GetRequiredService<IDeserializer<TMessage>>());
            });
    }

    /// <summary>
    /// Registers a shared <see cref="ISchemaRegistryClient"/> unless the application has already
    /// registered one (any registration style is honored). Applies basic-auth credentials from
    /// <see cref="KafkaConnectionConfig"/> when configured.
    /// </summary>
    private static void RegisterSchemaRegistryClient(IServiceCollection services, IConfiguration configuration)
    {
        services.TryAddSingleton<ISchemaRegistryClient>(sp =>
        {
            var kafkaConnection = KafkaWorker.ServiceCollectionExtensions.GetKafkaConnectionConfig(configuration);
            var schemaRegistryConfig = new SchemaRegistryConfig { Url = kafkaConnection.SchemaRegistryUrls };
            if (!string.IsNullOrWhiteSpace(kafkaConnection.SchemaRegistryUsername))
            {
                schemaRegistryConfig.BasicAuthCredentialsSource = AuthCredentialsSource.UserInfo;
                schemaRegistryConfig.BasicAuthUserInfo = $"{kafkaConnection.SchemaRegistryUsername}:{kafkaConnection.SchemaRegistryPassword}";
            }
            return new CachedSchemaRegistryClient(schemaRegistryConfig);
        });
    }
}
