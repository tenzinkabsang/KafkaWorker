using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Xunit.Abstractions;

namespace KafkaWorker.IntegrationTests;

internal sealed class TestLoggerProvider(ITestOutputHelper testOutputHelper) : ILoggerProvider
{
    public static TimeSpan WaitTime => TimeSpan.FromSeconds(60);

    private readonly ConcurrentBag<LogEntry> _entries = [];

    public ILogger CreateLogger(string categoryName) => new TestLogger(categoryName, _entries, testOutputHelper);

    public void Dispose() { }

    public bool HasLogged(string expectedMessage)
        => _entries.Any(e => e.Message.Contains(expectedMessage, StringComparison.OrdinalIgnoreCase));

    public async Task WaitForLogAsync(string expectedMessage, Task hostTask, TimeSpan? timeout = null)
    {
        var deadline = DateTime.UtcNow.Add(timeout ?? WaitTime);
        while (!HasLogged(expectedMessage) && !hostTask.IsCompleted && DateTime.UtcNow < deadline)
        {
            await Task.Delay(200);
        }
    }

    public async Task WaitForLogCountAsync(string expectedMessage, int expectedCount, Task hostTask, TimeSpan? timeout = null)
    {
        var deadline = DateTime.UtcNow.Add(timeout ?? WaitTime);
        while (_entries.Count(e => e.Message.Contains(expectedMessage, StringComparison.OrdinalIgnoreCase)) < expectedCount
            && !hostTask.IsCompleted && DateTime.UtcNow < deadline)
        {
            await Task.Delay(200);
        }
    }

    private sealed class TestLogger(string categoryName, ConcurrentBag<LogEntry> entries, ITestOutputHelper output) : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => true;
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            var message = formatter(state, exception);
            entries.Add(new LogEntry(categoryName, logLevel, message));
            try { output.WriteLine($"[{logLevel}] {categoryName}: {message}"); } catch { }
        }
    }

    private readonly record struct LogEntry(string CategoryName, LogLevel Level, string Message);
}
