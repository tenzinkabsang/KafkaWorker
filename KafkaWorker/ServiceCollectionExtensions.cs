using System.ComponentModel.DataAnnotations;
using System.Net;
using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace KafkaWorker;

/// <summary>
/// Extension methods for registering Kafka consumer services with dependency injection.
/// </summary>
public static class ServiceCollectionExtensions
{
    internal static string GetDlqConfigKey<TMessage>() => $"dlq-{typeof(TMessage).FullName}";

    /// <summary>
    /// Registers a hosted Kafka consumer that deserializes messages using plain JSON (System.Text.Json) without Schema Registry.
    /// </summary>
    /// <typeparam name="TMessage">The message type to consume. Must be deserializable by <see cref="System.Text.Json.JsonSerializer"/>.</typeparam>
    /// <typeparam name="THandler">The message handler implementation type. Registered as a scoped service.</typeparam>
    /// <param name="services">The service collection to add services to.</param>
    /// <param name="configuration">The application configuration containing Kafka settings.</param>
    /// <param name="configSection">The configuration section path for consumer settings. Defaults to <c>KafkaWorker:Consumer</c>.</param>
    /// <param name="configureConsumer">Optional callback to configure the underlying Confluent <see cref="ConsumerConfig"/>.
    /// Settings like <c>AutoOffsetReset</c> and <c>SessionTimeoutMs</c> can be changed here.
    /// <c>EnableAutoCommit</c> and <c>EnableAutoOffsetStore</c> are enforced by the library and cannot be overridden.</param>
    /// <param name="configureProducer">Optional callback to configure the underlying Confluent <see cref="ProducerConfig"/>
    /// used for dead letter publishing (e.g. security settings not covered by <see cref="KafkaConnectionConfig"/>).</param>
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
    public static IServiceCollection AddKafkaWorker<TMessage, THandler>(
        this IServiceCollection services,
        IConfiguration configuration,
        string configSection = KafkaWorkerConfig.Section,
        Action<ConsumerConfig>? configureConsumer = null,
        Action<ProducerConfig>? configureProducer = null)
      where TMessage : class
      where THandler : class, IMessageHandler<TMessage>
        => AddKafkaWorker<string, TMessage, THandler>(services, configuration, configSection, configureConsumer, configureProducer);

    /// <inheritdoc cref="AddKafkaWorker{TMessage, THandler}"/>
    /// <typeparam name="TKey">The message key type.</typeparam>
    /// <typeparam name="TMessage">The message type to consume.</typeparam>
    /// <typeparam name="THandler">The message handler implementation type.</typeparam>
    public static IServiceCollection AddKafkaWorker<TKey, TMessage, THandler>(
        this IServiceCollection services,
        IConfiguration configuration,
        string configSection = KafkaWorkerConfig.Section,
        Action<ConsumerConfig>? configureConsumer = null,
        Action<ProducerConfig>? configureProducer = null)
      where TMessage : class
      where THandler : class, IMessageHandler<TMessage>
    {
        services.TryAddSingleton<IDeserializer<TMessage>>(sp => new JsonStringDeserializer<TMessage>());

        RegisterProducer<TKey, TMessage>(services, configuration, (sp, b) =>
        {
            b.SetValueSerializer(new JsonStringSerializer<TMessage>());
        }, configureProducer);

        return RegisterHostedConsumer<TKey, TMessage, THandler>(services, configuration, configSection, configureConsumer, (sp, b) =>
        {
            b.SetValueDeserializer(sp.GetRequiredService<IDeserializer<TMessage>>());
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
    /// in place by invoking the registered <see cref="IMessageHandler{TMessage}"/> directly, so messages
    /// never reappear on the original topic. A message that fails again is re-enqueued to the dead letter
    /// topic with an incremented attempt for a future tick. Messages are skipped if they:
    /// <list type="bullet">
    ///   <item>Are marked as invalid messages (thrown <see cref="InvalidMessageException"/>)</item>
    ///   <item>Have exceeded the maximum reprocess attempts</item>
    /// </list>
    /// </para>
    /// <para>
    /// This method must be called after the main consumer registration (<c>AddKafkaWorker</c>), as it
    /// depends on <see cref="KafkaWorkerConfig"/> being configured and requires an
    /// <see cref="IMessageHandler{TMessage}"/> to be registered for reprocessing. Registration throws if
    /// no handler is available.
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

        // The DLQ consumer reprocesses messages by invoking the registered message handler directly.
        // Fail fast at startup if no handler is registered (e.g. a standalone DLQ reprocessor that never
        // called AddKafkaWorker) so the misconfiguration is obvious rather than failing per message.
        if (!services.Any(sd => sd.ServiceType == typeof(IMessageHandler<TMessage>)))
        {
            throw new InvalidOperationException(
                $"No IMessageHandler<{typeof(TMessage).Name}> is registered. Call AddKafkaWorker before " +
                $"AddKafkaWorkerDeadLetter so the handler is available for reprocessing dead letter messages.");
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
        services.TryAddSingleton<DlqReprocessSignal<TMessage>>();
        services.TryAddSingleton<IDlqReprocessTrigger<TMessage>>(sp => sp.GetRequiredService<DlqReprocessSignal<TMessage>>());
        services.AddHostedService<DlqConsumer<TKey, TMessage>>();

        return services;
    }

    /// <summary>
    /// Core registration method that configures a Kafka consumer with the specified deserializer.
    /// Registers <typeparamref name="THandler"/> as a scoped <see cref="IMessageHandler{TMessage}"/>.
    /// </summary>
    internal static IServiceCollection RegisterHostedConsumer<TKey, TMessage, THandler>(
        IServiceCollection services,
        IConfiguration configuration,
        string configSection,
        Action<ConsumerConfig>? configureConsumer,
        Action<IServiceProvider, ConsumerBuilder<TKey, TMessage>> deserializerConfig)
        where TMessage : class
        where THandler : class, IMessageHandler<TMessage>
    {
        if (services.Any(sd => sd.ServiceType == typeof(IHostedService) && sd.ImplementationType == typeof(Consumer<TKey, TMessage>)))
        {
            throw new InvalidOperationException(
                $"A consumer for {typeof(TMessage).Name} is already registered. " +
                "Use a distinct message type per consumer, or use different key types.");
        }

        services.AddScoped<IMessageHandler<TMessage>, THandler>();
        services.TryAddSingleton<KafkaWorkerMetrics>();

        services
            .AddOptions<KafkaWorkerConfig>(typeof(TMessage).FullName!)
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

            var logger = sp.GetRequiredService<ILogger<Consumer<TKey, TMessage>>>();
            var builder = new ConsumerBuilder<TKey, TMessage>(consumerConfig)
                .SetLogHandler((_, logMessage) => KafkaClientLogging.LogClientMessage(logger, logMessage.Name, logMessage.Message))
                .SetErrorHandler((_, error) => KafkaClientLogging.LogClientError(logger, error.Reason, error.IsFatal));
            deserializerConfig(sp, builder);
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
        Action<IServiceProvider, ProducerBuilder<TKey, TMessage>> builderConfig,
        Action<ProducerConfig>? configureProducer = null) where TMessage : class
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
            configureProducer?.Invoke(producerConfig);
            var builder = new ProducerBuilder<TKey, TMessage>(producerConfig);
            builderConfig(sp, builder);
            return builder.Build();
        });

        // The main consumer only touches the producer when a message is dead-lettered; resolve it
        // lazily so no producer (or broker connection) is created when DLQ publishing never happens.
        services.TryAddSingleton(sp => new Lazy<IProducer<TKey, TMessage>>(() => sp.GetRequiredService<IProducer<TKey, TMessage>>()));
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
            clientConfig.SaslMechanism = kafkaConnection.SaslMechanism;
            clientConfig.SaslUsername = kafkaConnection.Username;
            clientConfig.SaslPassword = kafkaConnection.Password;
        }
    }
}
