using System.ComponentModel.DataAnnotations;
using Confluent.Kafka;
using Microsoft.Extensions.Configuration;

namespace KafkaWorker.Tests;

public class KafkaConnectionConfigTests
{
    [Fact]
    public void Validate_SecuredCluster_MissingUsername_ReturnsValidationError()
    {
        var config = new KafkaConnectionConfig
        {
            BootstrapServers = "localhost:9092",
            IsSecuredCluster = true,
            Password = "pass"
        };

        var results = config.Validate(new ValidationContext(config)).ToList();

        Assert.Single(results);
        Assert.Contains(nameof(KafkaConnectionConfig.Username), results[0].MemberNames);
    }

    [Fact]
    public void Validate_SecuredCluster_MissingPassword_ReturnsValidationError()
    {
        var config = new KafkaConnectionConfig
        {
            BootstrapServers = "localhost:9092",
            IsSecuredCluster = true,
            Username = "user"
        };

        var results = config.Validate(new ValidationContext(config)).ToList();

        Assert.Single(results);
        Assert.Contains(nameof(KafkaConnectionConfig.Password), results[0].MemberNames);
    }

    [Fact]
    public void Validate_SecuredCluster_WithCredentials_Succeeds()
    {
        var config = new KafkaConnectionConfig
        {
            BootstrapServers = "localhost:9092",
            IsSecuredCluster = true,
            Username = "user",
            Password = "pass"
        };

        var results = config.Validate(new ValidationContext(config)).ToList();

        Assert.Empty(results);
    }

    [Fact]
    public void Validate_UnsecuredCluster_NoCredentials_Succeeds()
    {
        var config = new KafkaConnectionConfig
        {
            BootstrapServers = "localhost:9092",
            IsSecuredCluster = false
        };

        var results = config.Validate(new ValidationContext(config)).ToList();

        Assert.Empty(results);
    }

    [Fact]
    public void SaslMechanism_DefaultsToScramSha512()
    {
        var config = new KafkaConnectionConfig { BootstrapServers = "localhost:9092" };

        Assert.Equal(SaslMechanism.ScramSha512, config.SaslMechanism);
    }

    [Fact]
    public void Validate_SchemaRegistryUsernameWithoutPassword_ReturnsValidationError()
    {
        var config = new KafkaConnectionConfig
        {
            BootstrapServers = "localhost:9092",
            SchemaRegistryUsername = "sr-key"
        };

        var results = config.Validate(new ValidationContext(config)).ToList();

        Assert.Single(results);
        Assert.Contains(nameof(KafkaConnectionConfig.SchemaRegistryPassword), results[0].MemberNames);
    }

    [Fact]
    public void Validate_SchemaRegistryPasswordWithoutUsername_ReturnsValidationError()
    {
        var config = new KafkaConnectionConfig
        {
            BootstrapServers = "localhost:9092",
            SchemaRegistryPassword = "sr-secret"
        };

        var results = config.Validate(new ValidationContext(config)).ToList();

        Assert.Single(results);
        Assert.Contains(nameof(KafkaConnectionConfig.SchemaRegistryUsername), results[0].MemberNames);
    }

    [Fact]
    public void Validate_SchemaRegistryCredentialsBothSet_Succeeds()
    {
        var config = new KafkaConnectionConfig
        {
            BootstrapServers = "localhost:9092",
            SchemaRegistryUsername = "sr-key",
            SchemaRegistryPassword = "sr-secret"
        };

        var results = config.Validate(new ValidationContext(config)).ToList();

        Assert.Empty(results);
    }

    [Theory]
    [InlineData("Plain", SaslMechanism.Plain)]
    [InlineData("scramsha256", SaslMechanism.ScramSha256)]
    [InlineData("ScramSha512", SaslMechanism.ScramSha512)]
    public void SaslMechanism_BindsFromConfigurationString_CaseInsensitive(string configValue, SaslMechanism expected)
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["KafkaWorker:Connection:BootstrapServers"] = "localhost:9092",
                ["KafkaWorker:Connection:SaslMechanism"] = configValue,
            })
            .Build();

        var config = configuration.GetSection(KafkaConnectionConfig.Section).Get<KafkaConnectionConfig>();

        Assert.NotNull(config);
        Assert.Equal(expected, config.SaslMechanism);
    }
}
