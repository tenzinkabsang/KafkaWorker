using System.ComponentModel.DataAnnotations;

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

    /// <inheritdoc />
    public IEnumerable<ValidationResult> Validate(ValidationContext validationContext)
    {
        if (IsSecuredCluster && string.IsNullOrWhiteSpace(Username))
            yield return new ValidationResult("Username is required when IsSecuredCluster is true.", [nameof(Username)]);

        if (IsSecuredCluster && string.IsNullOrWhiteSpace(Password))
            yield return new ValidationResult("Password is required when IsSecuredCluster is true.", [nameof(Password)]);
    }
}
