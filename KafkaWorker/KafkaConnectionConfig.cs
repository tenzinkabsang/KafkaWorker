using System.ComponentModel.DataAnnotations;
using Confluent.Kafka;

namespace KafkaWorker;

/// <summary>
/// Configuration for connecting to a Kafka cluster, including bootstrap servers,
/// Schema Registry URLs, and optional SASL/SSL security settings.
/// </summary>
/// <remarks>
/// Bind this from the <c>KafkaWorker:Connection</c> configuration section.
/// </remarks>
public record KafkaConnectionConfig : IValidatableObject
{
    /// <summary>
    /// The configuration section name for Kafka connection settings.
    /// </summary>
    public const string Section = "KafkaWorker:Connection";

    /// <summary>
    /// Comma-separated list of Kafka broker addresses (host:port).
    /// </summary>
    /// <example><c>"broker1:9092,broker2:9092"</c></example>
    [Required]
    public required string BootstrapServers { get; init; }

    /// <summary>
    /// Comma-separated list of Schema Registry URLs. Required when using Avro, Protobuf, or Registry JSON serialization.
    /// </summary>
    /// <example><c>"http://schema-registry:8081"</c></example>
    public string? SchemaRegistryUrls { get; init; }

    /// <summary>
    /// Whether the Kafka cluster requires SASL/SSL authentication.
    /// When <c>true</c>, <see cref="Username"/> and <see cref="Password"/> must be provided.
    /// </summary>
    public bool IsSecuredCluster { get; init; }

    /// <summary>
    /// SASL username for secured cluster authentication.
    /// </summary>
    public string? Username { get; init; }

    /// <summary>
    /// SASL password for secured cluster authentication.
    /// </summary>
    public string? Password { get; init; }

    /// <summary>
    /// The SASL mechanism used when <see cref="IsSecuredCluster"/> is <c>true</c>.
    /// Accepted values: <c>Plain</c>, <c>ScramSha256</c>, <c>ScramSha512</c>, <c>Gssapi</c>, <c>OAuthBearer</c>
    /// (case-insensitive). Use <c>Plain</c> for Confluent Cloud API keys.
    /// </summary>
    /// <value>Default: <see cref="SaslMechanism.ScramSha512"/>.</value>
    public SaslMechanism SaslMechanism { get; init; } = SaslMechanism.ScramSha512;

    /// <summary>
    /// Schema Registry basic-auth username (e.g. a Confluent Cloud Schema Registry API key).
    /// When set, <see cref="SchemaRegistryPassword"/> must also be set.
    /// </summary>
    public string? SchemaRegistryUsername { get; init; }

    /// <summary>
    /// Schema Registry basic-auth password (e.g. a Confluent Cloud Schema Registry API secret).
    /// When set, <see cref="SchemaRegistryUsername"/> must also be set.
    /// </summary>
    public string? SchemaRegistryPassword { get; init; }

    /// <inheritdoc />
    public IEnumerable<ValidationResult> Validate(ValidationContext validationContext)
    {
        if (IsSecuredCluster && string.IsNullOrWhiteSpace(Username))
            yield return new ValidationResult("Username is required when IsSecuredCluster is true.", [nameof(Username)]);

        if (IsSecuredCluster && string.IsNullOrWhiteSpace(Password))
            yield return new ValidationResult("Password is required when IsSecuredCluster is true.", [nameof(Password)]);

        if (!string.IsNullOrWhiteSpace(SchemaRegistryUsername) && string.IsNullOrWhiteSpace(SchemaRegistryPassword))
            yield return new ValidationResult("SchemaRegistryPassword is required when SchemaRegistryUsername is set.", [nameof(SchemaRegistryPassword)]);

        if (!string.IsNullOrWhiteSpace(SchemaRegistryPassword) && string.IsNullOrWhiteSpace(SchemaRegistryUsername))
            yield return new ValidationResult("SchemaRegistryUsername is required when SchemaRegistryPassword is set.", [nameof(SchemaRegistryUsername)]);
    }
}
