using KafkaWorker.Worker;
using Microsoft.Extensions.Logging;

namespace KafkaWorker.IntegrationTests;

/// <summary>
/// Controls how many times the handler should throw a transient (non-invalid-message) exception
/// before succeeding. Used by integration tests to exercise the DLQ round-trip flow.
/// </summary>
public class TransientFailureState
{
    private int _callCount;

    /// <summary>
    /// Number of calls that should throw before the handler starts succeeding.
    /// Set to <see cref="int.MaxValue"/> for "always fail" behavior.
    /// </summary>
    public required int FailCount { get; init; }

    public bool ShouldFail() => Interlocked.Increment(ref _callCount) <= FailCount;
}

/// <summary>
/// A test-only handler that throws a transient (non-invalid-message) exception for the first N calls,
/// then succeeds. Used to exercise the full DLQ round-trip flow in integration tests.
/// </summary>
public sealed class TransientFailureHandlerJson(
    TransientFailureState state,
    ILogger<TransientFailureHandlerJson> logger) : IMessageHandler<OrderMessage>
{
    public Task HandleMessageAsync(OrderMessage message, CancellationToken stoppingToken)
    {
        if (state.ShouldFail())
        {
            logger.LogWarning("Simulating transient failure for order {OrderId}", message.OrderId);
            throw new InvalidOperationException($"Transient failure processing order {message.OrderId}");
        }

        logger.LogInformation("Successfully processed order {OrderId}", message.OrderId);
        return Task.CompletedTask;
    }
}
