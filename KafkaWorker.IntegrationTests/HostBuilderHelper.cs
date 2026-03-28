using KafkaWorker.Worker;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Xunit.Abstractions;

namespace KafkaWorker.IntegrationTests;

public static class HostBuilderHelper
{
    private static readonly Dictionary<string, string?> DefaultKafkaConfig = new()
    {
        ["KafkaWorker:Connection:BootstrapServers"] = "localhost:9092",
        ["KafkaWorker:Connection:SchemaRegistryUrls"] = "localhost:8082",
        ["KafkaWorker:Connection:IsSecuredCluster"] = "false"
    };

    internal static (IHost Host, TestLoggerProvider LogProvider) CreateHost(
        ITestOutputHelper testOutputHelper,
        Dictionary<string, string?>? configurationOverrides,
        Action<HostBuilderContext, IServiceCollection> configureServices)
    {
        var config = new Dictionary<string, string?>(DefaultKafkaConfig);
        if (configurationOverrides != null)
        {
            foreach (var kvp in configurationOverrides)
                config[kvp.Key] = kvp.Value;
        }

        var logProvider = new TestLoggerProvider(testOutputHelper);

        var host = Program.CreateHostBuilder([])
            .UseEnvironment("Development")
            .ConfigureAppConfiguration((context, builder) =>
            {
                builder.AddInMemoryCollection(config);
            })
            .ConfigureServices((context, services) =>
            {
                services.AddSingleton<ILoggerFactory>(new LoggerFactory(
                    [logProvider],
                    new LoggerFilterOptions
                    {
                        MinLevel = LogLevel.Debug
                    }));

                configureServices(context, services);
            })
            .Build();

        return (host, logProvider);
    }
}
