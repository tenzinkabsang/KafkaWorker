using System.Text;
using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using NSubstitute;
using NSubstitute.ExceptionExtensions;

namespace KafkaWorker.Tests;

public class DlqConsumerTests : IDisposable
{
    private const string TestDlqTopic = "test-dlq-topic";
    private const string TestOriginalTopic = "test-original-topic";
    private const string TestMessageKey = "test-key";
    private const string TestBatchId = "test-batch-id";
    private const long DefaultHighWatermark = 1000;

    private readonly IConsumer<string, TestMessage> _kafkaConsumer;
    private readonly IProducer<string, TestMessage> _producer;
    private readonly IMessageHandler<TestMessage> _messageHandler;
    private readonly ITerminalFailureSink<TestMessage> _terminalSink;
    private readonly ILogger<DlqConsumer<string, TestMessage>> _logger;
    private readonly KafkaWorkerMetrics _metrics;
    private readonly DlqReprocessSignal<TestMessage> _reprocessSignal = new();
    private readonly CancellationTokenSource _cts;

    public DlqConsumerTests()
    {
        _kafkaConsumer = Substitute.For<IConsumer<string, TestMessage>>();
        _producer = Substitute.For<IProducer<string, TestMessage>>();
        _terminalSink = Substitute.For<ITerminalFailureSink<TestMessage>>();
        _messageHandler = Substitute.For<IMessageHandler<TestMessage>>();
        _logger = Substitute.For<ILogger<DlqConsumer<string, TestMessage>>>();
        _logger.IsEnabled(Arg.Any<LogLevel>()).Returns(true);
        _metrics = new KafkaWorkerMetrics();
        _cts = new CancellationTokenSource();

        SetFinishLine(DefaultHighWatermark);
    }

    /// <summary>
    /// Points the consumer at the given partitions and sets each one's high watermark, which is what
    /// the sweep snapshots as its finish line.
    /// </summary>
    private void SetFinishLine(long highWatermark, params int[] partitions)
    {
        var assignment = (partitions.Length == 0 ? new[] { 0 } : partitions)
            .Select(p => new TopicPartition(TestDlqTopic, new Partition(p)))
            .ToList();

        _kafkaConsumer.Assignment.Returns(assignment);
        foreach (var topicPartition in assignment)
        {
            _kafkaConsumer.QueryWatermarkOffsets(topicPartition, Arg.Any<TimeSpan>())
                .Returns(new WatermarkOffsets(new Offset(0), new Offset(highWatermark)));
        }

    }

    public void Dispose()
    {
        _kafkaConsumer.Dispose();
        _producer.Dispose();
        _cts.Dispose();
        _metrics.Dispose();
        GC.SuppressFinalize(this);
    }

    #region Helpers

    public class TestMessage
    {
        public string Data { get; set; } = string.Empty;
    }

    private sealed class TestDlqConsumerFactory(IConsumer<string, TestMessage> consumer) : IDlqConsumerFactory<string, TestMessage>
    {
        public IConsumer<string, TestMessage> Create() => consumer;
    }

    private DlqConsumer<string, TestMessage> CreateConsumer(
        string? deadLetterTopic = TestDlqTopic,
        int maxReprocessAttempts = 3,
        int processingIntervalMinutes = 60,
        TimeProvider? timeProvider = null)
    {
        var config = new KafkaWorkerConfig
        {
            GroupId = "test-group",
            Topic = TestOriginalTopic,
            DeadLetterTopic = deadLetterTopic,
            DeadLetterMaxReprocessAttempts = maxReprocessAttempts,
            DeadLetterProcessingIntervalMinutes = processingIntervalMinutes
        };

        var optionsMonitor = Substitute.For<IOptionsMonitor<KafkaWorkerConfig>>();
        optionsMonitor.Get(typeof(TestMessage).FullName).Returns(config);
        var consumerFactory = new TestDlqConsumerFactory(_kafkaConsumer);

        var serviceProvider = Substitute.For<IServiceProvider>();
        serviceProvider.GetService(typeof(IMessageHandler<TestMessage>)).Returns(_messageHandler);
        serviceProvider.GetService(typeof(ITerminalFailureSink<TestMessage>)).Returns(_terminalSink);
        var scope = Substitute.For<IServiceScope>();
        scope.ServiceProvider.Returns(serviceProvider);
        var scopeFactory = Substitute.For<IServiceScopeFactory>();
        scopeFactory.CreateScope().Returns(scope);

        return new DlqConsumer<string, TestMessage>(new Lazy<IProducer<string, TestMessage>>(_producer), consumerFactory, scopeFactory, optionsMonitor, _metrics, _reprocessSignal, _logger, timeProvider ?? TimeProvider.System);
    }

    private static ConsumeResult<string, TestMessage> CreateDlqConsumeResult(
        string key = TestMessageKey,
        TestMessage? value = null,
        string? originalTopic = TestOriginalTopic,
        bool isInvalidMessage = false,
        int reprocessAttempt = 0,
        string? batchId = null,
        long offset = 1,
        int partition = 0)
    {
        var headers = new Headers();

        if (originalTopic != null)
        {
            headers.Add(KafkaHeaders.OriginalTopic, Encoding.UTF8.GetBytes(originalTopic));
        }

        if (isInvalidMessage)
        {
            headers.Add(KafkaHeaders.InvalidMessage, Encoding.UTF8.GetBytes("true"));
        }

        if (reprocessAttempt > 0)
        {
            headers.Add(KafkaHeaders.ReprocessedAttempt, Encoding.UTF8.GetBytes(reprocessAttempt.ToString()));
        }

        if (batchId != null)
        {
            headers.Add(KafkaHeaders.BatchId, Encoding.UTF8.GetBytes(batchId));
        }

        return new ConsumeResult<string, TestMessage>
        {
            Topic = TestDlqTopic,
            Partition = new Partition(partition),
            Offset = new Offset(offset),
            Message = new Message<string, TestMessage>
            {
                Key = key,
                Value = value ?? new TestMessage { Data = "dlq-data" },
                Headers = headers
            },
            IsPartitionEOF = false
        };
    }

    /// <summary>
    /// Creates a ConsumeException representing a DLQ record that failed value deserialization.
    /// </summary>
    private static ConsumeException CreatePoisonException(
        long offset = 3,
        ErrorCode code = ErrorCode.Local_ValueDeserialization,
        bool capturedByMainConsumer = false)
    {
        var headers = new Headers();
        if (capturedByMainConsumer)
        {
            headers.Add(KafkaHeaders.DeserializationFailed, Encoding.UTF8.GetBytes("true"));
        }

        return new ConsumeException(
            new ConsumeResult<byte[], byte[]>
            {
                Topic = TestDlqTopic,
                Partition = new Partition(0),
                Offset = new Offset(offset),
                Message = new Message<byte[], byte[]> { Headers = headers }
            },
            new Error(code));
    }

    private static bool HasHeader(Headers? headers, string key, string expectedValue)
    {
        if (headers == null) return false;

        try
        {
            var header = headers.GetLastBytes(key);
            if (header == null) return false;
            return Encoding.UTF8.GetString(header) == expectedValue;
        }
        catch (KeyNotFoundException)
        {
            return false;
        }
    }

    /// <summary>
    /// Sets up the Kafka consumer to return the given results in order, then return null (end of the sweep).
    /// </summary>
    private void SetupConsumeSequence(params ConsumeResult<string, TestMessage>[] results)
    {
        var callIndex = 0;
        _kafkaConsumer.Consume(Arg.Any<TimeSpan>())
            .Returns(_ =>
            {
                if (callIndex < results.Length)
                {
                    return results[callIndex++];
                }

                return null!;
            });
    }

    #endregion

    #region Subscription and cleanup

    [Fact]
    public async Task Sweep_SubscribesToDeadLetterTopic()
    {
        using var sut = CreateConsumer();
        _kafkaConsumer.Consume(Arg.Any<TimeSpan>()).Returns((ConsumeResult<string, TestMessage>)null!);

        await sut.SweepDeadLetterQueueAsync(TestBatchId, _cts.Token);

        _kafkaConsumer.Received(1).Subscribe(TestDlqTopic);
    }

    [Fact]
    public async Task Sweep_ClosesConsumerWhenDone()
    {
        using var sut = CreateConsumer();
        _kafkaConsumer.Consume(Arg.Any<TimeSpan>()).Returns((ConsumeResult<string, TestMessage>)null!);

        await sut.SweepDeadLetterQueueAsync(TestBatchId, _cts.Token);

        _kafkaConsumer.Received(1).Close();
    }

    [Fact]
    public async Task Sweep_ClosesConsumer_EvenOnError()
    {
        using var sut = CreateConsumer();
        _kafkaConsumer.Consume(Arg.Any<TimeSpan>())
            .Throws(new KafkaException(new Error(ErrorCode.BrokerNotAvailable)));

        await sut.SweepDeadLetterQueueAsync(TestBatchId, _cts.Token);

        _kafkaConsumer.Received(1).Close();
    }

    [Fact]
    public async Task Sweep_LogsFinishedProcessing()
    {
        using var sut = CreateConsumer();
        _kafkaConsumer.Consume(Arg.Any<TimeSpan>()).Returns((ConsumeResult<string, TestMessage>)null!);

        await sut.SweepDeadLetterQueueAsync(TestBatchId, _cts.Token);

        _logger.Received().Log(
            LogLevel.Information,
            Arg.Any<EventId>(),
            Arg.Is<object>(o => o.ToString()!.Contains("Finished the dead letter queue sweep")),
            Arg.Any<Exception?>(),
            Arg.Any<Func<object, Exception?, string>>());
    }

    #endregion

    #region Happy path - in-place reprocessing

    [Fact]
    public async Task Sweep_InvokesHandlerAndCommits_WithoutProducing()
    {
        using var sut = CreateConsumer();
        var dlqMessage = CreateDlqConsumeResult();
        SetupConsumeSequence(dlqMessage);

        await sut.SweepDeadLetterQueueAsync(TestBatchId, _cts.Token);

        await _messageHandler.Received(1)
            .HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>());
        _kafkaConsumer.Received(1).StoreOffset(dlqMessage);
        _kafkaConsumer.Received(1).Commit();
        // In-place success never produces anywhere (no republish, no re-enqueue)
        await _producer.DidNotReceive()
            .ProduceAsync(Arg.Any<string>(), Arg.Any<Message<string, TestMessage>>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Sweep_PassesMessageValueToHandler()
    {
        using var sut = CreateConsumer();
        var value = new TestMessage { Data = "reprocess-me" };
        var dlqMessage = CreateDlqConsumeResult(value: value);
        SetupConsumeSequence(dlqMessage);

        await sut.SweepDeadLetterQueueAsync(TestBatchId, _cts.Token);

        await _messageHandler.Received(1)
            .HandleMessageAsync(value, Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Sweep_CommitsOffsetAfterSuccessfulReprocess()
    {
        using var sut = CreateConsumer();
        var dlqMessage = CreateDlqConsumeResult();
        SetupConsumeSequence(dlqMessage);

        await sut.SweepDeadLetterQueueAsync(TestBatchId, _cts.Token);

        _kafkaConsumer.Received(1).StoreOffset(dlqMessage);
        _kafkaConsumer.Received(1).Commit();
    }

    [Fact]
    public async Task Sweep_ProcessesMultipleMessagesInOrder()
    {
        using var sut = CreateConsumer();
        var msg1 = CreateDlqConsumeResult(key: "key-1");
        var msg2 = CreateDlqConsumeResult(key: "key-2");
        SetupConsumeSequence(msg1, msg2);

        await sut.SweepDeadLetterQueueAsync(TestBatchId, _cts.Token);

        Received.InOrder(() =>
        {
            _messageHandler.HandleMessageAsync(msg1.Message.Value, Arg.Any<CancellationToken>());
            _kafkaConsumer.StoreOffset(msg1);
            _kafkaConsumer.Commit();
            _messageHandler.HandleMessageAsync(msg2.Message.Value, Arg.Any<CancellationToken>());
            _kafkaConsumer.StoreOffset(msg2);
            _kafkaConsumer.Commit();
        });
    }

    [Fact]
    public async Task Sweep_CommitsOffsetPerMessage()
    {
        using var sut = CreateConsumer();
        var msg1 = CreateDlqConsumeResult(key: "key-1");
        var msg2 = CreateDlqConsumeResult(key: "key-2");
        SetupConsumeSequence(msg1, msg2);

        await sut.SweepDeadLetterQueueAsync(TestBatchId, _cts.Token);

        _kafkaConsumer.Received(2).Commit();
    }

    [Fact]
    public async Task Sweep_LogsSuccessfulReprocess()
    {
        using var sut = CreateConsumer();
        var dlqMessage = CreateDlqConsumeResult();
        SetupConsumeSequence(dlqMessage);

        await sut.SweepDeadLetterQueueAsync(TestBatchId, _cts.Token);

        _logger.Received().Log(
            LogLevel.Information,
            Arg.Any<EventId>(),
            Arg.Is<object>(o => o.ToString()!.Contains("Successfully reprocessed")),
            Arg.Any<Exception?>(),
            Arg.Any<Func<object, Exception?, string>>());
    }

    #endregion

    #region Commit failures - routine, not fatal to the sweep

    [Fact]
    public async Task Sweep_SurvivesACommitFailureAndKeepsSweeping()
    {
        using var sut = CreateConsumer();
        SetupConsumeSequence(
            CreateDlqConsumeResult(key: "first", offset: 1),
            CreateDlqConsumeResult(key: "second", offset: 2));
        _kafkaConsumer.When(c => c.Commit()).Throw(new KafkaException(ErrorCode.RebalanceInProgress));

        await sut.SweepDeadLetterQueueAsync(TestBatchId, _cts.Token);

        // A rebalance that rejects the commit is routine: the next tick re-reads whatever was not
        // committed. It must not abandon the partitions still draining in this sweep.
        await _messageHandler.Received(2)
            .HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>());
        _logger.Received().Log(
            LogLevel.Error,
            Arg.Any<EventId>(),
            Arg.Is<object>(o => o.ToString()!.Contains("Failed to commit offsets")),
            Arg.Any<Exception?>(),
            Arg.Any<Func<object, Exception?, string>>());
        _logger.DidNotReceive().Log(
            LogLevel.Critical,
            Arg.Any<EventId>(),
            Arg.Any<object>(),
            Arg.Any<Exception?>(),
            Arg.Any<Func<object, Exception?, string>>());
    }

    [Fact]
    public async Task Sweep_TreatsNothingToCommitAsRoutine()
    {
        using var sut = CreateConsumer();
        SetupConsumeSequence(CreateDlqConsumeResult());
        _kafkaConsumer.When(c => c.Commit()).Throw(new KafkaException(ErrorCode.Local_NoOffset));

        await sut.SweepDeadLetterQueueAsync(TestBatchId, _cts.Token);

        // Having nothing stored to commit is not a failure and must not be reported as one.
        _logger.DidNotReceive().Log(
            LogLevel.Error,
            Arg.Any<EventId>(),
            Arg.Any<object>(),
            Arg.Any<Exception?>(),
            Arg.Any<Func<object, Exception?, string>>());
        _logger.DidNotReceive().Log(
            LogLevel.Critical,
            Arg.Any<EventId>(),
            Arg.Any<object>(),
            Arg.Any<Exception?>(),
            Arg.Any<Func<object, Exception?, string>>());
    }

    #endregion

    #region Null / EOF message handling - stops the sweep

    [Fact]
    public async Task Sweep_StopsOnNullConsumeResult()
    {
        using var sut = CreateConsumer();
        _kafkaConsumer.Consume(Arg.Any<TimeSpan>()).Returns((ConsumeResult<string, TestMessage>)null!);

        await sut.SweepDeadLetterQueueAsync(TestBatchId, _cts.Token);

        await _messageHandler.DidNotReceive()
            .HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>());
        _kafkaConsumer.DidNotReceive().Commit();
    }

    [Fact]
    public async Task Sweep_NullValueMessage_CommitsAndContinues()
    {
        using var sut = CreateConsumer();
        var tombstone = new ConsumeResult<string, TestMessage>
        {
            Topic = TestDlqTopic,
            Partition = new Partition(0),
            Offset = new Offset(1),
            Message = new Message<string, TestMessage> { Key = TestMessageKey, Value = null! },
            IsPartitionEOF = false
        };
        var validMsg = CreateDlqConsumeResult(key: "after-tombstone");
        SetupConsumeSequence(tombstone, validMsg);

        await sut.SweepDeadLetterQueueAsync(TestBatchId, _cts.Token);

        // A tombstone must not end the sweep (that would wedge the DLQ at its offset forever);
        // it is committed past and the sweep continues with the next message.
        _kafkaConsumer.Received(1).StoreOffset(tombstone);
        await _messageHandler.Received(1)
            .HandleMessageAsync(validMsg.Message.Value, Arg.Any<CancellationToken>());
        _kafkaConsumer.Received(2).Commit();
    }

    [Fact]
    public async Task Sweep_PartitionEof_IsNeverHandledOrCommitted()
    {
        using var sut = CreateConsumer();
        var eofResult = new ConsumeResult<string, TestMessage>
        {
            Topic = TestDlqTopic,
            Partition = new Partition(0),
            Offset = new Offset(1),
            Message = new Message<string, TestMessage> { Key = TestMessageKey, Value = new TestMessage { Data = "eof-data" } },
            IsPartitionEOF = true
        };
        SetupConsumeSequence(eofResult);

        await sut.SweepDeadLetterQueueAsync(TestBatchId, _cts.Token);

        await _messageHandler.DidNotReceive()
            .HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>());
        _kafkaConsumer.DidNotReceive().Commit();
    }

    [Fact]
    public async Task Sweep_PartitionEof_IsSkippedAndProcessingContinues()
    {
        using var sut = CreateConsumer();
        var eofResult = new ConsumeResult<string, TestMessage>
        {
            Topic = TestDlqTopic,
            Partition = new Partition(0),
            Offset = new Offset(1),
            Message = new Message<string, TestMessage> { Key = TestMessageKey, Value = new TestMessage { Data = "eof-data" } },
            IsPartitionEOF = true
        };
        var validMsg = CreateDlqConsumeResult(key: "after-eof");
        SetupConsumeSequence(eofResult, validMsg);

        await sut.SweepDeadLetterQueueAsync(TestBatchId, _cts.Token);

        // An EOF marker carries no record, and bounding the sweep is the finish line's job now, so
        // the message behind it is still processed rather than stranded until the next tick.
        await _messageHandler.Received(1)
            .HandleMessageAsync(validMsg.Message.Value, Arg.Any<CancellationToken>());
    }

    #endregion

    #region Poison messages - deserialization failures skip and commit

    [Fact]
    public async Task Sweep_ConsumeException_SkipsPoisonMessage_AndKeepsSweeping()
    {
        using var sut = CreateConsumer();
        var validMsg = CreateDlqConsumeResult(key: "after-poison");
        var callIndex = 0;
        _kafkaConsumer.Consume(Arg.Any<TimeSpan>())
            .Returns(_ =>
            {
                var call = callIndex++;
                if (call == 0)
                    throw CreatePoisonException(offset: 3);
                if (call == 1)
                    return validMsg;
                return null!;
            });

        await sut.SweepDeadLetterQueueAsync(TestBatchId, _cts.Token);

        // Committed past the poison record (failed offset + 1) and kept sweeping
        _kafkaConsumer.Received(1).StoreOffset(Arg.Is<TopicPartitionOffset>(t =>
            t.Topic == TestDlqTopic && t.Partition.Value == 0 && t.Offset.Value == 4));
        await _messageHandler.Received(1)
            .HandleMessageAsync(validMsg.Message.Value, Arg.Any<CancellationToken>());
        _kafkaConsumer.Received(2).Commit();
        _kafkaConsumer.Received(1).Close();
    }

    [Fact]
    public async Task Sweep_ConsumeException_NoRecordOffset_EndsSweepWithoutCommit()
    {
        using var sut = CreateConsumer();
        _kafkaConsumer.Consume(Arg.Any<TimeSpan>())
            .Returns(_ => throw new ConsumeException(
                new ConsumeResult<byte[], byte[]>
                {
                    Topic = TestDlqTopic,
                    Partition = new Partition(0),
                    Offset = Offset.Unset,
                    Message = new Message<byte[], byte[]>()
                },
                new Error(ErrorCode.UnknownTopicOrPart)));

        await sut.SweepDeadLetterQueueAsync(TestBatchId, _cts.Token);

        // No offset to skip past — the sweep ends cleanly and retries on the next tick
        _kafkaConsumer.DidNotReceive().StoreOffset(Arg.Any<TopicPartitionOffset>());
        _kafkaConsumer.DidNotReceive().Commit();
        _kafkaConsumer.Received(1).Close();
    }

    #endregion

    #region Invalid message skipping

    [Fact]
    public async Task Sweep_SkipsInvalidMessage_WithoutInvokingHandler()
    {
        using var sut = CreateConsumer();
        var invalidMsg = CreateDlqConsumeResult(isInvalidMessage: true);
        SetupConsumeSequence(invalidMsg);

        await sut.SweepDeadLetterQueueAsync(TestBatchId, _cts.Token);

        await _messageHandler.DidNotReceive()
            .HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Sweep_InvalidMessage_CommitsOffset()
    {
        using var sut = CreateConsumer();
        var invalidMsg = CreateDlqConsumeResult(isInvalidMessage: true);
        SetupConsumeSequence(invalidMsg);

        await sut.SweepDeadLetterQueueAsync(TestBatchId, _cts.Token);

        _kafkaConsumer.Received(1).StoreOffset(invalidMsg);
        _kafkaConsumer.Received(1).Commit();
    }

    [Fact]
    public async Task Sweep_InvalidMessage_LogsWarning()
    {
        using var sut = CreateConsumer();
        var invalidMsg = CreateDlqConsumeResult(isInvalidMessage: true);
        SetupConsumeSequence(invalidMsg);

        await sut.SweepDeadLetterQueueAsync(TestBatchId, _cts.Token);

        _logger.Received().Log(
            LogLevel.Warning,
            Arg.Any<EventId>(),
            Arg.Is<object>(o => o.ToString()!.Contains("Skipping invalid message")),
            Arg.Any<Exception?>(),
            Arg.Any<Func<object, Exception?, string>>());
    }

    [Fact]
    public async Task Sweep_ContinuesProcessingAfterInvalidMessage()
    {
        using var sut = CreateConsumer();
        var invalidMsg = CreateDlqConsumeResult(key: "invalid-key", isInvalidMessage: true);
        var validMsg = CreateDlqConsumeResult(key: "valid-key");
        SetupConsumeSequence(invalidMsg, validMsg);

        await sut.SweepDeadLetterQueueAsync(TestBatchId, _cts.Token);

        await _messageHandler.Received(1)
            .HandleMessageAsync(validMsg.Message.Value, Arg.Any<CancellationToken>());
        _kafkaConsumer.Received(2).Commit();
    }

    #endregion

    #region Max reprocess attempts exceeded

    [Fact]
    public async Task Sweep_SkipsMessageExceedingMaxReprocessAttempts()
    {
        using var sut = CreateConsumer(maxReprocessAttempts: 3);
        var exceededMsg = CreateDlqConsumeResult(reprocessAttempt: 3);
        SetupConsumeSequence(exceededMsg);

        await sut.SweepDeadLetterQueueAsync(TestBatchId, _cts.Token);

        await _messageHandler.DidNotReceive()
            .HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Sweep_MaxReprocessExceeded_CommitsOffset()
    {
        using var sut = CreateConsumer(maxReprocessAttempts: 3);
        var exceededMsg = CreateDlqConsumeResult(reprocessAttempt: 3);
        SetupConsumeSequence(exceededMsg);

        await sut.SweepDeadLetterQueueAsync(TestBatchId, _cts.Token);

        _kafkaConsumer.Received(1).StoreOffset(exceededMsg);
        _kafkaConsumer.Received(1).Commit();
    }

    [Fact]
    public async Task Sweep_MaxReprocessExceeded_LogsWarning()
    {
        using var sut = CreateConsumer(maxReprocessAttempts: 3);
        var exceededMsg = CreateDlqConsumeResult(reprocessAttempt: 3);
        SetupConsumeSequence(exceededMsg);

        await sut.SweepDeadLetterQueueAsync(TestBatchId, _cts.Token);

        _logger.Received().Log(
            LogLevel.Warning,
            Arg.Any<EventId>(),
            Arg.Is<object>(o => o.ToString()!.Contains("exceeded max reprocess attempts")),
            Arg.Any<Exception?>(),
            Arg.Any<Func<object, Exception?, string>>());
    }

    [Fact]
    public async Task Sweep_MessageAtExactMaxAttempts_IsSkipped()
    {
        using var sut = CreateConsumer(maxReprocessAttempts: 3);
        var atMaxMsg = CreateDlqConsumeResult(reprocessAttempt: 3);
        SetupConsumeSequence(atMaxMsg);

        await sut.SweepDeadLetterQueueAsync(TestBatchId, _cts.Token);

        await _messageHandler.DidNotReceive()
            .HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Sweep_MessageBelowMaxAttempts_IsProcessed()
    {
        using var sut = CreateConsumer(maxReprocessAttempts: 3);
        var belowMaxMsg = CreateDlqConsumeResult(reprocessAttempt: 2);
        SetupConsumeSequence(belowMaxMsg);

        await sut.SweepDeadLetterQueueAsync(TestBatchId, _cts.Token);

        await _messageHandler.Received(1)
            .HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Sweep_MessageAboveMaxAttempts_IsSkipped()
    {
        using var sut = CreateConsumer(maxReprocessAttempts: 3);
        var aboveMaxMsg = CreateDlqConsumeResult(reprocessAttempt: 4);
        SetupConsumeSequence(aboveMaxMsg);

        await sut.SweepDeadLetterQueueAsync(TestBatchId, _cts.Token);

        await _messageHandler.DidNotReceive()
            .HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Sweep_ContinuesProcessingAfterExceededMessage()
    {
        using var sut = CreateConsumer(maxReprocessAttempts: 3);
        var exceededMsg = CreateDlqConsumeResult(key: "exceeded-key", reprocessAttempt: 3);
        var validMsg = CreateDlqConsumeResult(key: "valid-key");
        SetupConsumeSequence(exceededMsg, validMsg);

        await sut.SweepDeadLetterQueueAsync(TestBatchId, _cts.Token);

        await _messageHandler.Received(1)
            .HandleMessageAsync(validMsg.Message.Value, Arg.Any<CancellationToken>());
        _kafkaConsumer.Received(2).Commit();
    }

    #endregion

    #region Finish line - the sweep stops at the end of the log as it stood when it began

    [Fact]
    public async Task Sweep_StopsAtTheFinishLine()
    {
        using var sut = CreateConsumer();
        SetFinishLine(highWatermark: 5);
        // Offset 5 is the snapshotted end of the log, so this record was appended during the sweep.
        var appendedDuringSweep = CreateDlqConsumeResult(key: "re-enqueued", offset: 5);
        SetupConsumeSequence(appendedDuringSweep);

        await sut.SweepDeadLetterQueueAsync(TestBatchId, _cts.Token);

        await _messageHandler.DidNotReceive()
            .HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>());
        _kafkaConsumer.DidNotReceive().Commit();
    }

    [Fact]
    public async Task Sweep_ProcessesEverythingBelowTheFinishLine()
    {
        using var sut = CreateConsumer();
        SetFinishLine(highWatermark: 3);
        var first = CreateDlqConsumeResult(key: "key-1", offset: 1);
        var second = CreateDlqConsumeResult(key: "key-2", offset: 2);
        SetupConsumeSequence(first, second);

        await sut.SweepDeadLetterQueueAsync(TestBatchId, _cts.Token);

        await _messageHandler.Received(1).HandleMessageAsync(first.Message.Value, Arg.Any<CancellationToken>());
        await _messageHandler.Received(1).HandleMessageAsync(second.Message.Value, Arg.Any<CancellationToken>());
        _kafkaConsumer.Received(2).Commit();
    }

    [Fact]
    public async Task Sweep_ProcessesMessagesUpToTheFinishLineThenStops()
    {
        using var sut = CreateConsumer();
        SetFinishLine(highWatermark: 2);
        var beforeLine = CreateDlqConsumeResult(key: "key-1", offset: 1);
        var atLine = CreateDlqConsumeResult(key: "key-2", offset: 2);
        SetupConsumeSequence(beforeLine, atLine);

        await sut.SweepDeadLetterQueueAsync(TestBatchId, _cts.Token);

        await _messageHandler.Received(1).HandleMessageAsync(beforeLine.Message.Value, Arg.Any<CancellationToken>());
        await _messageHandler.DidNotReceive().HandleMessageAsync(atLine.Message.Value, Arg.Any<CancellationToken>());
        _kafkaConsumer.Received(1).Commit();
    }

    [Fact]
    public async Task Sweep_SnapshotsTheFinishLineOnlyOnce()
    {
        using var sut = CreateConsumer();
        SetFinishLine(highWatermark: 10);
        SetupConsumeSequence(
            CreateDlqConsumeResult(key: "key-1", offset: 1),
            CreateDlqConsumeResult(key: "key-2", offset: 2),
            CreateDlqConsumeResult(key: "key-3", offset: 3));

        await sut.SweepDeadLetterQueueAsync(TestBatchId, _cts.Token);

        // Re-reading the watermark mid-sweep would let the finish line drift ahead of this sweep's
        // own re-enqueues, which is exactly what the snapshot exists to prevent.
        _kafkaConsumer.Received(1).QueryWatermarkOffsets(Arg.Any<TopicPartition>(), Arg.Any<TimeSpan>());
    }

    [Fact]
    public async Task Sweep_BatchIdHeaderNoLongerStopsTheSweep()
    {
        using var sut = CreateConsumer();
        SetFinishLine(highWatermark: 10);
        // batch-id is now written for diagnostics only; the finish line decides where a sweep ends.
        var stampedWithCurrentBatchId = CreateDlqConsumeResult(key: "looped-key", batchId: TestBatchId, offset: 1);
        SetupConsumeSequence(stampedWithCurrentBatchId);

        await sut.SweepDeadLetterQueueAsync(TestBatchId, _cts.Token);

        await _messageHandler.Received(1)
            .HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Sweep_OnePartitionReachingTheFinishLineDoesNotAbandonTheOthers()
    {
        using var sut = CreateConsumer();
        SetFinishLine(highWatermark: 4, 0, 1);
        var doneOnPartition0 = CreateDlqConsumeResult(key: "p0-end", offset: 4, partition: 0);
        var stillPendingOnPartition1 = CreateDlqConsumeResult(key: "p1-work", offset: 2, partition: 1);
        SetupConsumeSequence(doneOnPartition0, stillPendingOnPartition1);

        await sut.SweepDeadLetterQueueAsync(TestBatchId, _cts.Token);

        // The old batch-id guard broke out of the whole sweep here, stranding partition 1.
        await _messageHandler.Received(1)
            .HandleMessageAsync(stillPendingOnPartition1.Message.Value, Arg.Any<CancellationToken>());
        _kafkaConsumer.Received(1).Pause(Arg.Is<IEnumerable<TopicPartition>>(
            tps => tps.Single().Partition.Value == 0));
    }

    [Fact]
    public async Task Sweep_EndsOnceEveryPartitionHasReachedItsFinishLine()
    {
        using var sut = CreateConsumer();
        SetFinishLine(highWatermark: 3, 0, 1);
        var afterBothDone = CreateDlqConsumeResult(key: "never-reached", offset: 1, partition: 0);
        SetupConsumeSequence(
            CreateDlqConsumeResult(key: "p0-end", offset: 3, partition: 0),
            CreateDlqConsumeResult(key: "p1-end", offset: 3, partition: 1),
            afterBothDone);

        await sut.SweepDeadLetterQueueAsync(TestBatchId, _cts.Token);

        await _messageHandler.DidNotReceive()
            .HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Sweep_PartitionAssignedMidSweep_DoesNotStandInForAnUnfinishedOne()
    {
        using var sut = CreateConsumer();
        SetFinishLine(highWatermark: 4, 0, 1);
        // Partition 2 is not in the snapshot: it was assigned after the sweep began, so it has no
        // finish line and is left for the next tick.
        var rebalancedIn = CreateDlqConsumeResult(key: "p2", offset: 0, partition: 2);
        var partition0Done = CreateDlqConsumeResult(key: "p0-end", offset: 4, partition: 0);
        var partition1Work = CreateDlqConsumeResult(key: "p1-work", offset: 1, partition: 1);
        SetupConsumeSequence(rebalancedIn, partition0Done, partition1Work);

        await sut.SweepDeadLetterQueueAsync(TestBatchId, _cts.Token);

        // Counting paused partitions instead of checking the snapshot's membership would end the
        // sweep once partition 2 and partition 0 were paused, stranding partition 1's work.
        await _messageHandler.Received(1)
            .HandleMessageAsync(partition1Work.Message.Value, Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Sweep_SkipsTheSweepWhenNoPartitionsAreAssigned()
    {
        using var sut = CreateConsumer();
        _kafkaConsumer.Assignment.Returns([]);
        SetupConsumeSequence(CreateDlqConsumeResult(key: "key-1", offset: 1));

        await sut.SweepDeadLetterQueueAsync(TestBatchId, _cts.Token);

        // Without a finish line the sweep could reach its own re-enqueues, so it does nothing.
        await _messageHandler.DidNotReceive()
            .HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>());
        _kafkaConsumer.DidNotReceive().Commit();
    }

    #endregion

    #region In-place reprocessing - handler failure re-enqueues to DLQ

    [Fact]
    public async Task Sweep_HandlerFails_ReEnqueuesToDlqWithIncrementedAttempt()
    {
        using var sut = CreateConsumer();
        var dlqMessage = CreateDlqConsumeResult(reprocessAttempt: 0);
        SetupConsumeSequence(dlqMessage);
        _messageHandler.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("transient failure"));

        await sut.SweepDeadLetterQueueAsync(TestBatchId, _cts.Token);

        // Re-enqueued to the DLQ topic (not the original topic) with attempt incremented to 1 and the current sweep id
        await _producer.Received(1).ProduceAsync(
            TestDlqTopic,
            Arg.Is<Message<string, TestMessage>>(m =>
                HasHeader(m.Headers, KafkaHeaders.ReprocessedAttempt, "1") &&
                HasHeader(m.Headers, KafkaHeaders.BatchId, TestBatchId)),
            Arg.Any<CancellationToken>());
        await _producer.DidNotReceive()
            .ProduceAsync(TestOriginalTopic, Arg.Any<Message<string, TestMessage>>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Sweep_HandlerFails_IncrementsAttemptFromPreviousValue()
    {
        using var sut = CreateConsumer(maxReprocessAttempts: 5);
        var dlqMessage = CreateDlqConsumeResult(reprocessAttempt: 2);
        SetupConsumeSequence(dlqMessage);
        _messageHandler.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("transient failure"));

        await sut.SweepDeadLetterQueueAsync(TestBatchId, _cts.Token);

        await _producer.Received(1).ProduceAsync(
            TestDlqTopic,
            Arg.Is<Message<string, TestMessage>>(m =>
                HasHeader(m.Headers, KafkaHeaders.ReprocessedAttempt, "3")),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Sweep_HandlerFails_ReEnqueueSucceeds_CommitsOffset()
    {
        using var sut = CreateConsumer();
        var dlqMessage = CreateDlqConsumeResult();
        SetupConsumeSequence(dlqMessage);
        _messageHandler.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("transient failure"));

        await sut.SweepDeadLetterQueueAsync(TestBatchId, _cts.Token);

        // A successful re-enqueue means the original is safely parked, so its offset is committed
        _kafkaConsumer.Received(1).StoreOffset(dlqMessage);
        _kafkaConsumer.Received(1).Commit();
    }

    [Fact]
    public async Task Sweep_HandlerThrowsInvalidMessage_SkipsAndCommits()
    {
        using var sut = CreateConsumer();
        var dlqMessage = CreateDlqConsumeResult();
        SetupConsumeSequence(dlqMessage);
        _messageHandler.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidMessageException("permanent failure"));

        await sut.SweepDeadLetterQueueAsync(TestBatchId, _cts.Token);

        // Permanent failure: skipped (committed), never re-enqueued
        _kafkaConsumer.Received(1).StoreOffset(dlqMessage);
        _kafkaConsumer.Received(1).Commit();
        await _producer.DidNotReceive()
            .ProduceAsync(Arg.Any<string>(), Arg.Any<Message<string, TestMessage>>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Sweep_ReEnqueueProduceFails_StopsSweepWithoutCommitting()
    {
        using var sut = CreateConsumer();
        var dlqMessage = CreateDlqConsumeResult();
        SetupConsumeSequence(dlqMessage);
        _messageHandler.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("transient failure"));
        _producer.ProduceAsync(Arg.Any<string>(), Arg.Any<Message<string, TestMessage>>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new KafkaException(new Error(ErrorCode.BrokerNotAvailable)));

        await sut.SweepDeadLetterQueueAsync(TestBatchId, _cts.Token);

        _kafkaConsumer.DidNotReceive().StoreOffset(dlqMessage);
        _kafkaConsumer.DidNotReceive().Commit();
    }

    [Fact]
    public async Task Sweep_ReEnqueueProduceFails_LogsError()
    {
        using var sut = CreateConsumer();
        var dlqMessage = CreateDlqConsumeResult();
        SetupConsumeSequence(dlqMessage);
        _messageHandler.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("transient failure"));
        _producer.ProduceAsync(Arg.Any<string>(), Arg.Any<Message<string, TestMessage>>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new KafkaException(new Error(ErrorCode.BrokerNotAvailable)));

        await sut.SweepDeadLetterQueueAsync(TestBatchId, _cts.Token);

        _logger.Received().Log(
            LogLevel.Error,
            Arg.Any<EventId>(),
            Arg.Is<object>(o => o.ToString()!.Contains("Failed to re-enqueue")),
            Arg.Any<Exception?>(),
            Arg.Any<Func<object, Exception?, string>>());
    }

    [Fact]
    public async Task Sweep_ReEnqueueFailure_SecondMessageNotProcessed()
    {
        using var sut = CreateConsumer();
        var msg1 = CreateDlqConsumeResult(key: "key-fail");
        var msg2 = CreateDlqConsumeResult(key: "key-ok");
        SetupConsumeSequence(msg1, msg2);

        _messageHandler.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("transient failure"));
        _producer.ProduceAsync(Arg.Any<string>(), Arg.Any<Message<string, TestMessage>>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new KafkaException(new Error(ErrorCode.BrokerNotAvailable)));

        await sut.SweepDeadLetterQueueAsync(TestBatchId, _cts.Token);

        // Re-enqueue failure stops the sweep, no offsets committed
        _kafkaConsumer.DidNotReceive().Commit();
    }

    #endregion

    #region Cancellation handling

    [Fact]
    public async Task Sweep_StopsProcessingOnCancellation()
    {
        using var sut = CreateConsumer();
        var msg1 = CreateDlqConsumeResult(key: "key-1");
        var callIndex = 0;
        _kafkaConsumer.Consume(Arg.Any<TimeSpan>())
            .Returns(_ =>
            {
                if (callIndex++ == 0) return msg1;
                _cts.Cancel();
                throw new OperationCanceledException(_cts.Token);
            });

        await Assert.ThrowsAsync<OperationCanceledException>(
            () => sut.SweepDeadLetterQueueAsync(TestBatchId, _cts.Token));
    }

    #endregion

    #region Mixed scenarios

    [Fact]
    public async Task Sweep_MixedMessages_ProcessesCorrectly()
    {
        using var sut = CreateConsumer(maxReprocessAttempts: 3);
        var invalidMsg = CreateDlqConsumeResult(key: "invalid", isInvalidMessage: true);
        var exceededMsg = CreateDlqConsumeResult(key: "exceeded", reprocessAttempt: 3);
        var validMsg = CreateDlqConsumeResult(key: "valid");
        SetupConsumeSequence(invalidMsg, exceededMsg, validMsg);

        await sut.SweepDeadLetterQueueAsync(TestBatchId, _cts.Token);

        // Only the valid message should be handled in place
        await _messageHandler.Received(1)
            .HandleMessageAsync(validMsg.Message.Value, Arg.Any<CancellationToken>());
        // All 3 messages should have their offsets committed (2 skipped + 1 processed)
        _kafkaConsumer.Received(3).Commit();
    }

    [Fact]
    public async Task Sweep_ReEnqueueFailsAfterSkippedMessages_StopsSweepCorrectly()
    {
        using var sut = CreateConsumer(maxReprocessAttempts: 3);
        var invalidMsg = CreateDlqConsumeResult(key: "invalid", isInvalidMessage: true);
        var failMsg = CreateDlqConsumeResult(key: "will-fail");
        var afterFailMsg = CreateDlqConsumeResult(key: "after-fail");
        SetupConsumeSequence(invalidMsg, failMsg, afterFailMsg);

        _messageHandler.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("transient failure"));
        _producer.ProduceAsync(Arg.Any<string>(), Arg.Any<Message<string, TestMessage>>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new KafkaException(new Error(ErrorCode.BrokerNotAvailable)));

        await sut.SweepDeadLetterQueueAsync(TestBatchId, _cts.Token);

        // Invalid message committed, but fail message stops the sweep without committing
        _kafkaConsumer.Received(1).StoreOffset(invalidMsg);
        _kafkaConsumer.DidNotReceive().StoreOffset(failMsg);
        _kafkaConsumer.DidNotReceive().StoreOffset(afterFailMsg);
    }

    [Fact]
    public async Task Sweep_SkippedMessagesBeforeTheFinishLine_AllCommitted()
    {
        using var sut = CreateConsumer(maxReprocessAttempts: 2);
        SetFinishLine(highWatermark: 2);
        var exceededMsg = CreateDlqConsumeResult(key: "exceeded", reprocessAttempt: 2, offset: 1);
        var pastFinishLine = CreateDlqConsumeResult(key: "re-enqueued", offset: 2);
        SetupConsumeSequence(exceededMsg, pastFinishLine);

        await sut.SweepDeadLetterQueueAsync(TestBatchId, _cts.Token);

        // The exceeded message is skipped but committed; the record at the finish line ends the
        // sweep without being committed, so the next tick picks it up.
        _kafkaConsumer.Received(1).StoreOffset(exceededMsg);
        _kafkaConsumer.Received(1).Commit();
        await _messageHandler.DidNotReceive()
            .HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Sweep_EmptyTopic_NoProcessingOrCommits()
    {
        using var sut = CreateConsumer();
        _kafkaConsumer.Consume(Arg.Any<TimeSpan>())
            .Returns((ConsumeResult<string, TestMessage>)null!);

        await sut.SweepDeadLetterQueueAsync(TestBatchId, _cts.Token);

        await _messageHandler.DidNotReceive()
            .HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>());
        _kafkaConsumer.DidNotReceive().StoreOffset(Arg.Any<ConsumeResult<string, TestMessage>>());
        _kafkaConsumer.DidNotReceive().Commit();
    }

    [Fact]
    public async Task Sweep_AllMessagesSkipped_AllOffsetsCommitted()
    {
        using var sut = CreateConsumer(maxReprocessAttempts: 1);
        var invalidMsg = CreateDlqConsumeResult(key: "invalid-1", isInvalidMessage: true);
        var exceededMsg = CreateDlqConsumeResult(key: "exceeded-1", reprocessAttempt: 1);
        SetupConsumeSequence(invalidMsg, exceededMsg);

        await sut.SweepDeadLetterQueueAsync(TestBatchId, _cts.Token);

        // All skipped messages should have offsets committed
        _kafkaConsumer.Received(2).Commit();
        await _messageHandler.DidNotReceive()
            .HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>());
    }

    #endregion

    #region ExecuteAsync lifecycle

    /// <summary>
    /// Waits for ExecuteAsync to park on Task.Delay, advances the fake clock,
    /// then waits for the sweep to complete.
    /// </summary>
    private static async Task AdvanceTimeAndYieldAsync(FakeTimeProvider fakeTime, TimeSpan duration)
    {
        await Task.Delay(50);  // Let ExecuteAsync reach Task.Delay
        fakeTime.Advance(duration);
        await Task.Delay(50);  // Let the sweep complete
    }

    [Fact]
    public async Task ExecuteAsync_DoesNotSweepBeforeTimerInterval()
    {
        var fakeTime = new FakeTimeProvider();
        using var sut = CreateConsumer(timeProvider: fakeTime);

        await sut.StartAsync(_cts.Token);
        await Task.Delay(50);

        _kafkaConsumer.DidNotReceive().Subscribe(Arg.Any<string>());

        await sut.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task ExecuteAsync_SweepsAfterTimerInterval()
    {
        var fakeTime = new FakeTimeProvider();
        using var sut = CreateConsumer(timeProvider: fakeTime);
        _kafkaConsumer.Consume(Arg.Any<TimeSpan>()).Returns((ConsumeResult<string, TestMessage>)null!);

        await sut.StartAsync(_cts.Token);
        await AdvanceTimeAndYieldAsync(fakeTime, TimeSpan.FromMinutes(60));

        _kafkaConsumer.Received(1).Subscribe(TestDlqTopic);

        await sut.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task ExecuteAsync_SweepsOnEveryTick()
    {
        var fakeTime = new FakeTimeProvider();
        using var sut = CreateConsumer(timeProvider: fakeTime);
        _kafkaConsumer.Consume(Arg.Any<TimeSpan>()).Returns((ConsumeResult<string, TestMessage>)null!);

        await sut.StartAsync(_cts.Token);
        await AdvanceTimeAndYieldAsync(fakeTime, TimeSpan.FromMinutes(60));
        await AdvanceTimeAndYieldAsync(fakeTime, TimeSpan.FromMinutes(60));

        _kafkaConsumer.Received(2).Subscribe(TestDlqTopic);

        await sut.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task ExecuteAsync_RespectsConfiguredInterval()
    {
        var fakeTime = new FakeTimeProvider();
        using var sut = CreateConsumer(timeProvider: fakeTime, processingIntervalMinutes: 5);
        _kafkaConsumer.Consume(Arg.Any<TimeSpan>()).Returns((ConsumeResult<string, TestMessage>)null!);

        await sut.StartAsync(_cts.Token);

        // Advance 4 minutes — not enough for a 5-minute interval
        await AdvanceTimeAndYieldAsync(fakeTime, TimeSpan.FromMinutes(4));
        _kafkaConsumer.DidNotReceive().Subscribe(Arg.Any<string>());

        // Advance 1 more minute — now at 5 minutes total
        await AdvanceTimeAndYieldAsync(fakeTime, TimeSpan.FromMinutes(1));
        _kafkaConsumer.Received(1).Subscribe(TestDlqTopic);

        await sut.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task ExecuteAsync_LogsWarningOnGracefulShutdown()
    {
        var fakeTime = new FakeTimeProvider();
        using var sut = CreateConsumer(timeProvider: fakeTime);

        await sut.StartAsync(_cts.Token);
        await Task.Delay(50); // Let ExecuteAsync reach Task.Delay
        await sut.StopAsync(CancellationToken.None);

        _logger.Received().Log(
            LogLevel.Warning,
            Arg.Any<EventId>(),
            Arg.Is<object>(o => o.ToString()!.Contains("shutting down")),
            Arg.Any<Exception?>(),
            Arg.Any<Func<object, Exception?, string>>());
    }

    [Fact]
    public async Task ExecuteAsync_LogsCriticalOnSweepError()
    {
        var fakeTime = new FakeTimeProvider();
        using var sut = CreateConsumer(timeProvider: fakeTime);
        _kafkaConsumer.When(c => c.Subscribe(Arg.Any<string>()))
            .Throw(new InvalidOperationException("subscription failed"));

        await sut.StartAsync(_cts.Token);
        await AdvanceTimeAndYieldAsync(fakeTime, TimeSpan.FromMinutes(60));

        _logger.Received().Log(
            LogLevel.Critical,
            Arg.Any<EventId>(),
            Arg.Is<object>(o => o.ToString()!.Contains("Error during the dead letter queue sweep")),
            Arg.Any<Exception?>(),
            Arg.Any<Func<object, Exception?, string>>());

        await sut.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task Trigger_WakesConsumerAndRunsSweep_WithoutAdvancingClock()
    {
        var fakeTime = new FakeTimeProvider();
        using var sut = CreateConsumer(timeProvider: fakeTime);
        _kafkaConsumer.Consume(Arg.Any<TimeSpan>()).Returns((ConsumeResult<string, TestMessage>)null!);

        await sut.StartAsync(_cts.Token);
        await Task.Delay(50); // Let ExecuteAsync reach the wait

        _reprocessSignal.Trigger();
        await Task.Delay(100); // Let the triggered sweep run

        // A sweep ran even though the fake clock never advanced
        _kafkaConsumer.Received(1).Subscribe(TestDlqTopic);

        await sut.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task Trigger_RepeatedCallsBeforeSweep_CoalesceIntoOneSweep()
    {
        var fakeTime = new FakeTimeProvider();
        using var sut = CreateConsumer(timeProvider: fakeTime);
        _kafkaConsumer.Consume(Arg.Any<TimeSpan>()).Returns((ConsumeResult<string, TestMessage>)null!);

        // Multiple triggers before the consumer starts waiting — must coalesce
        _reprocessSignal.Trigger();
        _reprocessSignal.Trigger();
        _reprocessSignal.Trigger();

        await sut.StartAsync(_cts.Token);
        await Task.Delay(150);

        _kafkaConsumer.Received(1).Subscribe(TestDlqTopic);

        await sut.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task Trigger_AfterTimerTick_RunsImmediateSecondSweep()
    {
        var fakeTime = new FakeTimeProvider();
        using var sut = CreateConsumer(timeProvider: fakeTime);
        _kafkaConsumer.Consume(Arg.Any<TimeSpan>()).Returns((ConsumeResult<string, TestMessage>)null!);

        await sut.StartAsync(_cts.Token);
        await AdvanceTimeAndYieldAsync(fakeTime, TimeSpan.FromMinutes(60));
        _kafkaConsumer.Received(1).Subscribe(TestDlqTopic);

        _reprocessSignal.Trigger();
        await Task.Delay(100);

        // The trigger produced a second sweep without waiting another interval
        _kafkaConsumer.Received(2).Subscribe(TestDlqTopic);

        await sut.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task Trigger_DoesNotDisturbRegularSchedule()
    {
        var fakeTime = new FakeTimeProvider();
        using var sut = CreateConsumer(timeProvider: fakeTime);
        _kafkaConsumer.Consume(Arg.Any<TimeSpan>()).Returns((ConsumeResult<string, TestMessage>)null!);

        await sut.StartAsync(_cts.Token);
        await Task.Delay(50);

        // Triggered sweep, then a normal timer tick afterwards
        _reprocessSignal.Trigger();
        await Task.Delay(100);
        await AdvanceTimeAndYieldAsync(fakeTime, TimeSpan.FromMinutes(60));

        _kafkaConsumer.Received(2).Subscribe(TestDlqTopic);

        await sut.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task ExecuteAsync_ClosesConsumerOnEachTick()
    {
        var fakeTime = new FakeTimeProvider();
        using var sut = CreateConsumer(timeProvider: fakeTime);
        _kafkaConsumer.Consume(Arg.Any<TimeSpan>()).Returns((ConsumeResult<string, TestMessage>)null!);

        await sut.StartAsync(_cts.Token);
        await AdvanceTimeAndYieldAsync(fakeTime, TimeSpan.FromMinutes(60));
        await AdvanceTimeAndYieldAsync(fakeTime, TimeSpan.FromMinutes(60));

        _kafkaConsumer.Received(2).Close();

        await sut.StopAsync(CancellationToken.None);
    }

    #endregion

    #region Terminal failure sink

    [Fact]
    public async Task Sweep_InvalidHeaderSkip_NotifiesTerminalSink()
    {
        using var sut = CreateConsumer();
        var invalidMsg = CreateDlqConsumeResult(isInvalidMessage: true, reprocessAttempt: 2);
        SetupConsumeSequence(invalidMsg);

        await sut.SweepDeadLetterQueueAsync(TestBatchId, _cts.Token);

        await _terminalSink.Received(1).HandleAsync(
            Arg.Is<TerminalFailure<TestMessage>>(f =>
                f.Reason == TerminalFailureReason.InvalidMessage &&
                f.Message == invalidMsg.Message.Value &&
                f.SourceTopic == TestOriginalTopic &&
                f.ReprocessAttempts == 2),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Sweep_MaxAttemptsSkip_NotifiesTerminalSink()
    {
        using var sut = CreateConsumer(maxReprocessAttempts: 3);
        var exceededMsg = CreateDlqConsumeResult(reprocessAttempt: 3);
        SetupConsumeSequence(exceededMsg);

        await sut.SweepDeadLetterQueueAsync(TestBatchId, _cts.Token);

        await _terminalSink.Received(1).HandleAsync(
            Arg.Is<TerminalFailure<TestMessage>>(f =>
                f.Reason == TerminalFailureReason.MaxReprocessAttemptsExceeded &&
                f.Message == exceededMsg.Message.Value &&
                f.ReprocessAttempts == 3),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Sweep_InvalidDuringReprocess_NotifiesTerminalSinkWithError()
    {
        using var sut = CreateConsumer();
        var dlqMessage = CreateDlqConsumeResult();
        SetupConsumeSequence(dlqMessage);
        _messageHandler.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidMessageException("schema drift"));

        await sut.SweepDeadLetterQueueAsync(TestBatchId, _cts.Token);

        await _terminalSink.Received(1).HandleAsync(
            Arg.Is<TerminalFailure<TestMessage>>(f =>
                f.Reason == TerminalFailureReason.InvalidMessage &&
                f.Error == "schema drift"),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Sweep_SuccessfulReprocess_DoesNotNotifySink()
    {
        using var sut = CreateConsumer();
        SetupConsumeSequence(CreateDlqConsumeResult());

        await sut.SweepDeadLetterQueueAsync(TestBatchId, _cts.Token);

        await _terminalSink.DidNotReceive()
            .HandleAsync(Arg.Any<TerminalFailure<TestMessage>>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Sweep_FailedReprocess_ReEnqueued_DoesNotNotifySink()
    {
        using var sut = CreateConsumer();
        SetupConsumeSequence(CreateDlqConsumeResult(reprocessAttempt: 1));
        _messageHandler.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("still failing"));

        await sut.SweepDeadLetterQueueAsync(TestBatchId, _cts.Token);

        // Re-enqueued for a future tick — not terminal yet
        await _terminalSink.DidNotReceive()
            .HandleAsync(Arg.Any<TerminalFailure<TestMessage>>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Sweep_TerminalSinkThrows_StillCommitsAndContinues()
    {
        using var sut = CreateConsumer();
        var invalidMsg = CreateDlqConsumeResult(key: "invalid", isInvalidMessage: true);
        var normalMsg = CreateDlqConsumeResult(key: "normal");
        SetupConsumeSequence(invalidMsg, normalMsg);
        _terminalSink.HandleAsync(Arg.Any<TerminalFailure<TestMessage>>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("sink db down"));

        await sut.SweepDeadLetterQueueAsync(TestBatchId, _cts.Token);

        _kafkaConsumer.Received(1).StoreOffset(invalidMsg);
        await _messageHandler.Received(1)
            .HandleMessageAsync(normalMsg.Message.Value, Arg.Any<CancellationToken>());
        _logger.Received().Log(
            LogLevel.Error,
            Arg.Any<EventId>(),
            Arg.Is<object>(o => o.ToString()!.Contains("Terminal failure sink threw")),
            Arg.Any<Exception?>(),
            Arg.Any<Func<object, Exception?, string>>());
    }

    #endregion

    #region Captured raw poison records

    [Fact]
    public async Task Sweep_CapturedPoisonRecord_SkipsQuietly_WithoutCriticalLog()
    {
        using var sut = CreateConsumer();
        var callIndex = 0;
        _kafkaConsumer.Consume(Arg.Any<TimeSpan>())
            .Returns(_ =>
            {
                if (callIndex++ == 0)
                    throw CreatePoisonException(offset: 3, capturedByMainConsumer: true);
                return null!;
            });

        await sut.SweepDeadLetterQueueAsync(TestBatchId, _cts.Token);

        // Committed past like any poison record, but without the Critical alarm
        _kafkaConsumer.Received(1).StoreOffset(Arg.Is<TopicPartitionOffset>(t => t.Offset.Value == 4));
        _logger.DidNotReceive().Log(
            LogLevel.Critical,
            Arg.Any<EventId>(),
            Arg.Is<object>(o => o.ToString()!.Contains("failed to deserialize")),
            Arg.Any<Exception?>(),
            Arg.Any<Func<object, Exception?, string>>());
    }

    #endregion
}
