using System.ComponentModel.DataAnnotations;
using System.Net;
using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;

namespace KafkaWorker;

/// <summary>
/// Extension methods for registering Kafka consumer services with dependency injection.
/// </summary>
public static class ServiceCollectionExtensions
{
    internal static string GetDlqConfigKey<TMessage>() => $"dlq-{typeof(TMessage).Name}";

    /// <summary>
    /// Registers a hosted Kafka consumer that deserializes messages using plain JSON (System.Text.Json) without Schema Registry.
    /// </summary>
    /// <typeparam name="TMessage">The message type to consume. Must be deserializable by <see cref="System.Text.Json.JsonSerializer"/>.</typeparam>
    /// <typeparam name="TProcessor">The message processor implementation type. Registered as a scoped service.</typeparam>
    /// <param name="services">The service collection to add services to.</param>
    /// <param name="configuration">The application configuration containing Kafka settings.</param>
    /// <param name="configSection">The configuration section path for consumer settings. Defaults to <c>KafkaWorker:Consumer</c>.</param>
    /// <param name="configureConsumer">Optional callback to configure the underlying Confluent <see cref="ConsumerConfig"/>.
    /// Settings like <c>AutoOffsetReset</c> and <c>SessionTimeoutMs</c> can be changed here.
    /// <c>EnableAutoCommit</c> and <c>EnableAutoOffsetStore</c> are enforced by the library and cannot be overridden.</param>
    /// <returns>The service collection for chaining.</returns>
    /// <remarks>
    /// <para>
    /// This method does not use Confluent Schema Registry. Messages are consumed as raw strings
    /// from Kafka and deserialized using <see cref="System.Text.Json.JsonSerializer"/>.
    /// </para>
    /// <para>
    /// Required configuration sections:
    /// <list type="bullet">
    ///   <item><c>KafkaWorker:Connection</c> - Kafka cluster connection settings</item>
    ///   <item>Consumer-specific settings section (default: <c>KafkaWorker:Consumer</c>)</item>
    /// </list>
    /// </para>
    /// </remarks>
    public static IServiceCollection AddKafkaWorker<TMessage, TProcessor>(
        this IServiceCollection services,
        IConfiguration configuration,
        string configSection = KafkaWorkerConfig.Section,
        Action<ConsumerConfig>? configureConsumer = null)
      where TMessage : class
      where TProcessor : class, IMessageHandler<TMessage>
        => AddKafkaWorker<string, TMessage, TProcessor>(services, configuration, configSection, configureConsumer);

    /// <inheritdoc cref="AddKafkaWorker{TMessage, TProcessor}"/>
    /// <typeparam name="TKey">The message key type.</typeparam>
    /// <typeparam name="TMessage">The message type to consume.</typeparam>
    /// <typeparam name="TProcessor">The message processor implementation type.</typeparam>
    public static IServiceCollection AddKafkaWorker<TKey, TMessage, TProcessor>(
        this IServiceCollection services,
        IConfiguration configuration,
        string configSection = KafkaWorkerConfig.Section,
        Action<ConsumerConfig>? configureConsumer = null)
      where TMessage : class
      where TProcessor : class, IMessageHandler<TMessage>
    {
        services.TryAddSingleton<IDeserializer<TMessage>>(sp => new JsonStringDeserializer<TMessage>());

        RegisterProducer<TKey, TMessage>(services, configuration, (ProducerBuilder<TKey, TMessage> b) =>
        {
            b.SetValueSerializer(new JsonStringSerializer<TMessage>());
        });

        return RegisterHostedConsumer<TKey, TMessage, TProcessor>(services, configuration, configSection, configureConsumer, b =>
        {
            b.SetValueDeserializer(new JsonStringDeserializer<TMessage>());
        });
    }

    /// <summary>
    /// Registers a hosted dead letter queue consumer that periodically reprocesses failed messages.
    /// </summary>
    /// <typeparam name="TMessage">The message type to consume from the dead letter topic.</typeparam>
    /// <param name="services">The service collection to add services to.</param>
    /// <param name="configuration">The application configuration containing Kafka settings.</param>
    /// <param name="configSection">The configuration section path for consumer settings. Defaults to <c>KafkaWorker:Consumer</c>.</param>
    /// <returns>The service collection for chaining.</returns>
    /// <remarks>
    /// <para>
    /// The DLQ consumer runs on a configurable interval (default: 60 minutes) and reprocesses messages
    /// by sending them back to the original topic. Messages are skipped if they:
    /// <list type="bullet">
    ///   <item>Are marked as invalid messages (thrown <see cref="InvalidMessageException"/>)</item>
    ///   <item>Have exceeded the maximum reprocess attempts</item>
    /// </list>
    /// </para>
    /// <para>
    /// This method should be called after the main consumer registration, as it depends on
    /// <see cref="KafkaWorkerConfig"/> being configured.
    /// </para>
    /// </remarks>
    public static IServiceCollection AddKafkaWorkerDeadLetter<TMessage>(
        this IServiceCollection services, IConfiguration configuration, string configSection = KafkaWorkerConfig.Section) where TMessage : class
        => AddKafkaWorkerDeadLetter<string, TMessage>(services, configuration, configSection);

    /// <inheritdoc cref="AddKafkaWorkerDeadLetter{TMessage}"/>
    /// <typeparam name="TKey">The message key type.</typeparam>
    /// <typeparam name="TMessage">The message type to consume from the dead letter topic.</typeparam>
    public static IServiceCollection AddKafkaWorkerDeadLetter<TKey, TMessage>(
        this IServiceCollection services, IConfiguration configuration, string configSection = KafkaWorkerConfig.Section) where TMessage : class
    {
        var workerConfig = configuration.GetRequiredSection(configSection).Get<KafkaWorkerConfig>();
        if (string.IsNullOrWhiteSpace(workerConfig?.DeadLetterTopic))
        {
            throw new InvalidOperationException(
                $"DeadLetterTopic must be configured in '{configSection}' when using AddKafkaWorkerDeadLetter. " +
                $"Either set the DeadLetterTopic configuration value or remove the AddKafkaWorkerDeadLetter registration.");
        }

        var kafkaConnection = GetKafkaConnectionConfig(configuration);
        services.AddKeyedSingleton<ConsumerConfig>(GetDlqConfigKey<TMessage>(), (sp, _) =>
        {
            ConsumerConfig consumerConfig = new()
            {
                GroupId = $"{workerConfig.GroupId}-dlq-consumer",
                BootstrapServers = kafkaConnection.BootstrapServers,
                ClientId = Dns.GetHostName(),
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false,
                EnableAutoOffsetStore = false
            };
            ApplySecurityConfig(consumerConfig, kafkaConnection);
            return consumerConfig;
        });

        services.TryAddSingleton(TimeProvider.System);
        services.TryAddSingleton<IDlqConsumerFactory<TKey, TMessage>, DlqConsumerFactory<TKey, TMessage>>();
        services.AddHostedService<DlqConsumer<TKey, TMessage>>();

        return services;
    }

    /// <summary>
    /// Core registration method that configures a Kafka consumer with the specified deserializer.
    /// Registers <typeparamref name="TProcessor"/> as a scoped <see cref="IMessageHandler{TMessage}"/>.
    /// </summary>
    internal static IServiceCollection RegisterHostedConsumer<TKey, TMessage, TProcessor>(
        IServiceCollection services,
        IConfiguration configuration,
        string configSection,
        Action<ConsumerConfig>? configureConsumer,
        Action<ConsumerBuilder<TKey, TMessage>> deserializerConfig)
        where TMessage : class
        where TProcessor : class, IMessageHandler<TMessage>
    {
        if (services.Any(sd => sd.ServiceType == typeof(IHostedService) && sd.ImplementationType == typeof(Consumer<TKey, TMessage>)))
        {
            throw new InvalidOperationException(
                $"A consumer for {typeof(TMessage).Name} is already registered. " +
                "Use a distinct message type per consumer, or use different key types.");
        }

        services.AddScoped<IMessageHandler<TMessage>, TProcessor>();
        services.TryAddSingleton<KafkaWorkerMetrics>();

        services
            .AddOptions<KafkaWorkerConfig>(typeof(TMessage).Name)
            .Bind(configuration.GetSection(configSection))
            .ValidateDataAnnotations()
            .ValidateOnStart();

        var kafkaConfig = configuration.GetRequiredSection(configSection).Get<KafkaWorkerConfig>();
        var kafkaConnection = GetKafkaConnectionConfig(configuration);

        services.TryAddSingleton<IConsumer<TKey, TMessage>>(sp =>
        {
            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = kafkaConnection.BootstrapServers,
                ClientId = Dns.GetHostName(),
                AutoOffsetReset = AutoOffsetReset.Latest,
                GroupId = kafkaConfig!.GroupId
            };
            ApplySecurityConfig(consumerConfig, kafkaConnection);

            // Allow user overrides, then re-enforce library invariants
            configureConsumer?.Invoke(consumerConfig);
            consumerConfig.EnableAutoOffsetStore = false;
            consumerConfig.EnableAutoCommit = false;

            var builder = new ConsumerBuilder<TKey, TMessage>(consumerConfig);
            deserializerConfig(builder);
            return builder.Build();
        });

        services.AddHostedService<Consumer<TKey, TMessage>>();

        return services;
    }

    /// <summary>
    /// Registers a Kafka producer for DLQ publishing with the specified serializer configuration.
    /// </summary>
    internal static void RegisterProducer<TKey, TMessage>(
        IServiceCollection services,
        IConfiguration configuration,
        Action<ProducerBuilder<TKey, TMessage>> builderConfig) where TMessage : class
    {
        var kafkaConnection = GetKafkaConnectionConfig(configuration);
        services.TryAddSingleton<IProducer<TKey, TMessage>>(sp =>
        {
            var producerConfig = new ProducerConfig
            {
                BootstrapServers = kafkaConnection.BootstrapServers,
                ClientId = Dns.GetHostName()
            };
            ApplySecurityConfig(producerConfig, kafkaConnection);
            var builder = new ProducerBuilder<TKey, TMessage>(producerConfig);
            builderConfig(builder);
            return builder.Build();
        });
    }

    /// <summary>
    /// Reads and validates the Kafka connection configuration from the application configuration.
    /// </summary>
    internal static KafkaConnectionConfig GetKafkaConnectionConfig(IConfiguration configuration)
    {
        var config = configuration.GetRequiredSection(KafkaConnectionConfig.Section).Get<KafkaConnectionConfig>()
            ?? throw new InvalidOperationException($"Configuration section '{KafkaConnectionConfig.Section}' is required.");

        Validator.ValidateObject(config, new ValidationContext(config), validateAllProperties: true);
        return config;
    }

    internal static void ApplySecurityConfig(ClientConfig clientConfig, KafkaConnectionConfig kafkaConnection)
    {
        if (kafkaConnection.IsSecuredCluster)
        {
            clientConfig.SecurityProtocol = SecurityProtocol.SaslSsl;
            clientConfig.SaslMechanism = SaslMechanism.ScramSha512;
            clientConfig.SaslUsername = kafkaConnection.Username;
            clientConfig.SaslPassword = kafkaConnection.Password;
        }
    }
}
