using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace KafkaWorker.Protobuf;

/// <summary>
/// Extension methods for registering a Protobuf-based Kafka consumer with dependency injection.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers a hosted Kafka consumer that deserializes messages using Protobuf format with Schema Registry.
    /// </summary>
    /// <typeparam name="TMessage">The Protobuf-generated message type to consume. Must implement <see cref="Google.Protobuf.IMessage{T}"/>.</typeparam>
    /// <typeparam name="THandler">The message handler implementation type. Registered as a scoped service.</typeparam>
    /// <param name="services">The service collection to add services to.</param>
    /// <param name="configuration">The application configuration containing Kafka settings.</param>
    /// <param name="configSection">The configuration section path for consumer settings. Defaults to <c>KafkaWorker:Consumer</c>.</param>
    /// <param name="configureConsumer">Optional callback to configure the underlying Confluent <see cref="ConsumerConfig"/>.</param>
    /// <param name="configureProducer">Optional callback to configure the underlying Confluent <see cref="ProducerConfig"/>
    /// used for dead letter publishing.</param>
    /// <param name="configureSerializer">Optional callback to configure the Confluent <see cref="ProtobufSerializerConfig"/>
    /// used when publishing to the dead letter topic (e.g. <c>AutoRegisterSchemas</c>, <c>UseLatestVersion</c>,
    /// <c>SubjectNameStrategy</c>). With Confluent defaults, the first DLQ publish auto-registers a new
    /// <c>{DeadLetterTopic}-value</c> subject; if your registry denies client-side registration, pre-register that
    /// subject or set <c>AutoRegisterSchemas = false</c> and <c>UseLatestVersion = true</c> here.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddKafkaWorkerProtobuf<TMessage, THandler>(
        this IServiceCollection services, IConfiguration configuration,
        string configSection = KafkaWorkerConfig.Section,
        Action<ConsumerConfig>? configureConsumer = null,
        Action<ProducerConfig>? configureProducer = null,
        Action<ProtobufSerializerConfig>? configureSerializer = null)
        where TMessage : class, Google.Protobuf.IMessage<TMessage>, new()
        where THandler : class, IMessageHandler<TMessage>
        => AddKafkaWorkerProtobuf<string, TMessage, THandler>(services, configuration, configSection, configureConsumer, configureProducer, configureSerializer);

    /// <inheritdoc cref="AddKafkaWorkerProtobuf{TMessage, THandler}"/>
    /// <typeparam name="TKey">The message key type.</typeparam>
    /// <typeparam name="TMessage">The Protobuf-generated message type to consume.</typeparam>
    /// <typeparam name="THandler">The message handler implementation type.</typeparam>
    public static IServiceCollection AddKafkaWorkerProtobuf<TKey, TMessage, THandler>(
        this IServiceCollection services, IConfiguration configuration,
        string configSection = KafkaWorkerConfig.Section,
        Action<ConsumerConfig>? configureConsumer = null,
        Action<ProducerConfig>? configureProducer = null,
        Action<ProtobufSerializerConfig>? configureSerializer = null)
        where TMessage : class, Google.Protobuf.IMessage<TMessage>, new()
        where THandler : class, IMessageHandler<TMessage>
    {
        RegisterSchemaRegistryClient(services, configuration);

        services.TryAddSingleton<IDeserializer<TMessage>>(sp =>
            new ProtobufDeserializer<TMessage>().AsSyncOverAsync());

        KafkaWorker.ServiceCollectionExtensions.RegisterProducer<TKey, TMessage>(services, configuration, (sp, b) =>
        {
            b.SetValueSerializer(new ProtobufSerializer<TMessage>(
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
    /// Materializes the user's serializer configuration, or returns null so Confluent defaults apply untouched.
    /// </summary>
    private static ProtobufSerializerConfig? BuildSerializerConfig(Action<ProtobufSerializerConfig>? configureSerializer)
    {
        if (configureSerializer is null)
        {
            return null;
        }

        var serializerConfig = new ProtobufSerializerConfig();
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
