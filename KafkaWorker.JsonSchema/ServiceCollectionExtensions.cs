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
    /// <typeparam name="THandler">The message handler implementation type. Registered as a scoped service.</typeparam>
    /// <param name="services">The service collection to add services to.</param>
    /// <param name="configuration">The application configuration containing Kafka settings.</param>
    /// <param name="configSection">The configuration section path for consumer settings. Defaults to <c>KafkaWorker:Consumer</c>.</param>
    /// <param name="configureConsumer">Optional callback to configure the underlying Confluent <see cref="ConsumerConfig"/>.</param>
    /// <param name="configureProducer">Optional callback to configure the underlying Confluent <see cref="ProducerConfig"/>
    /// used for dead letter publishing.</param>
    /// <param name="configureSerializer">Optional callback to configure the Confluent <see cref="JsonSerializerConfig"/>
    /// used when publishing to the dead letter topic (e.g. <c>AutoRegisterSchemas</c>, <c>UseLatestVersion</c>,
    /// <c>SubjectNameStrategy</c>). With Confluent defaults, the first DLQ publish auto-registers a new
    /// <c>{DeadLetterTopic}-value</c> subject; if your registry denies client-side registration, pre-register that
    /// subject or set <c>AutoRegisterSchemas = false</c> and <c>UseLatestVersion = true</c> here.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddKafkaWorkerRegistryJson<TMessage, THandler>(
        this IServiceCollection services, IConfiguration configuration,
        string configSection = KafkaWorkerConfig.Section,
        Action<ConsumerConfig>? configureConsumer = null,
        Action<ProducerConfig>? configureProducer = null,
        Action<JsonSerializerConfig>? configureSerializer = null)
        where TMessage : class
        where THandler : class, IMessageHandler<TMessage>
        => AddKafkaWorkerRegistryJson<string, TMessage, THandler>(services, configuration, configSection, configureConsumer, configureProducer, configureSerializer);

    /// <inheritdoc cref="AddKafkaWorkerRegistryJson{TMessage, THandler}"/>
    /// <typeparam name="TKey">The message key type.</typeparam>
    /// <typeparam name="TMessage">The message type to consume.</typeparam>
    /// <typeparam name="THandler">The message handler implementation type.</typeparam>
    public static IServiceCollection AddKafkaWorkerRegistryJson<TKey, TMessage, THandler>(
        this IServiceCollection services, IConfiguration configuration,
        string configSection = KafkaWorkerConfig.Section,
        Action<ConsumerConfig>? configureConsumer = null,
        Action<ProducerConfig>? configureProducer = null,
        Action<JsonSerializerConfig>? configureSerializer = null)
        where TMessage : class
        where THandler : class, IMessageHandler<TMessage>
    {
        RegisterSchemaRegistryClient(services, configuration);

        services.TryAddSingleton<IDeserializer<TMessage>>(sp =>
            new JsonDeserializer<TMessage>().AsSyncOverAsync());

        KafkaWorker.ServiceCollectionExtensions.RegisterProducer<TKey, TMessage>(services, configuration, (sp, b) =>
        {
            b.SetValueSerializer(new JsonSerializer<TMessage>(
                sp.GetRequiredService<ISchemaRegistryClient>(),
                BuildSerializerConfig(configureSerializer)).AsSyncOverAsync());
        }, configureProducer);

        return KafkaWorker.ServiceCollectionExtensions.RegisterHostedConsumer<TKey, TMessage, THandler>(
            services, configuration, configSection, configureConsumer, (sp, b) =>
            {
                b.SetValueDeserializer(sp.GetRequiredService<IDeserializer<TMessage>>());
            });
    }

    /// <summary>
    /// Registers a hosted Kafka consumer that processes messages in batches, deserializing them using
    /// JSON Schema with Schema Registry.
    /// </summary>
    /// <typeparam name="TMessage">The message type to consume.</typeparam>
    /// <typeparam name="THandler">The batch handler implementation type. Registered as a scoped service.</typeparam>
    /// <param name="services">The service collection to add services to.</param>
    /// <param name="configuration">The application configuration containing Kafka settings.</param>
    /// <param name="configSection">The configuration section path for consumer settings. Defaults to <c>KafkaWorker:Consumer</c>.</param>
    /// <param name="configureConsumer">Optional callback to configure the underlying Confluent <see cref="ConsumerConfig"/>.
    /// <c>EnableAutoCommit</c> (<c>false</c>) and <c>EnableAutoOffsetStore</c> (<c>false</c>) are enforced by the
    /// library: a batch consumer stores offsets only after a batch is handled and commits them synchronously
    /// at the batch boundary.</param>
    /// <param name="configureProducer">Optional callback to configure the underlying Confluent <see cref="ProducerConfig"/>
    /// used for dead letter publishing.</param>
    /// <param name="configureSerializer">Optional callback to configure the Confluent <see cref="JsonSerializerConfig"/>
    /// used when publishing to the dead letter topic.</param>
    /// <returns>The service collection for chaining.</returns>
    /// <remarks>
    /// Batching amortizes the work the handler does downstream — one bulk operation instead of one per
    /// message — and does not reduce broker round trips. A batch that fails is re-processed one message at
    /// a time, so retry and dead letter behaviour is identical to single-message mode. See
    /// <see cref="IBatchMessageHandler{TMessage}"/> and <see cref="KafkaWorkerConfig.MaxBatchSize"/>.
    /// </remarks>
    public static IServiceCollection AddKafkaWorkerRegistryJsonBatch<TMessage, THandler>(
        this IServiceCollection services, IConfiguration configuration,
        string configSection = KafkaWorkerConfig.Section,
        Action<ConsumerConfig>? configureConsumer = null,
        Action<ProducerConfig>? configureProducer = null,
        Action<JsonSerializerConfig>? configureSerializer = null)
        where TMessage : class
        where THandler : class, IBatchMessageHandler<TMessage>
        => AddKafkaWorkerRegistryJsonBatch<string, TMessage, THandler>(services, configuration, configSection, configureConsumer, configureProducer, configureSerializer);

    /// <inheritdoc cref="AddKafkaWorkerRegistryJsonBatch{TMessage, THandler}"/>
    /// <typeparam name="TKey">The message key type.</typeparam>
    /// <typeparam name="TMessage">The message type to consume.</typeparam>
    /// <typeparam name="THandler">The batch handler implementation type.</typeparam>
    public static IServiceCollection AddKafkaWorkerRegistryJsonBatch<TKey, TMessage, THandler>(
        this IServiceCollection services, IConfiguration configuration,
        string configSection = KafkaWorkerConfig.Section,
        Action<ConsumerConfig>? configureConsumer = null,
        Action<ProducerConfig>? configureProducer = null,
        Action<JsonSerializerConfig>? configureSerializer = null)
        where TMessage : class
        where THandler : class, IBatchMessageHandler<TMessage>
    {
        RegisterSchemaRegistryClient(services, configuration);

        services.TryAddSingleton<IDeserializer<TMessage>>(sp =>
            new JsonDeserializer<TMessage>().AsSyncOverAsync());

        KafkaWorker.ServiceCollectionExtensions.RegisterProducer<TKey, TMessage>(services, configuration, (sp, b) =>
        {
            b.SetValueSerializer(new JsonSerializer<TMessage>(
                sp.GetRequiredService<ISchemaRegistryClient>(),
                BuildSerializerConfig(configureSerializer)).AsSyncOverAsync());
        }, configureProducer);

        return KafkaWorker.ServiceCollectionExtensions.RegisterHostedBatchConsumer<TKey, TMessage, THandler>(
            services, configuration, configSection, configureConsumer, (sp, b) =>
            {
                b.SetValueDeserializer(sp.GetRequiredService<IDeserializer<TMessage>>());
            });
    }

    /// <summary>
    /// Materializes the user's serializer configuration, or returns null so Confluent defaults apply untouched.
    /// </summary>
    private static JsonSerializerConfig? BuildSerializerConfig(Action<JsonSerializerConfig>? configureSerializer)
    {
        if (configureSerializer is null)
        {
            return null;
        }

        var serializerConfig = new JsonSerializerConfig();
        configureSerializer(serializerConfig);
        return serializerConfig;
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
