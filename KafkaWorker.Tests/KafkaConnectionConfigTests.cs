using System.ComponentModel.DataAnnotations;

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
}
