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

    private readonly IConsumer<string, TestMessage> _kafkaConsumer;
    private readonly IProducer<string, TestMessage> _producer;
    private readonly IMessageHandler<TestMessage> _messageHandler;
    private readonly ILogger<DlqConsumer<string, TestMessage>> _logger;
    private readonly KafkaWorkerMetrics _metrics;
    private readonly CancellationTokenSource _cts;

    public DlqConsumerTests()
    {
        _kafkaConsumer = Substitute.For<IConsumer<string, TestMessage>>();
        _producer = Substitute.For<IProducer<string, TestMessage>>();
        _messageHandler = Substitute.For<IMessageHandler<TestMessage>>();
        _logger = Substitute.For<ILogger<DlqConsumer<string, TestMessage>>>();
        _logger.IsEnabled(Arg.Any<LogLevel>()).Returns(true);
        _metrics = new KafkaWorkerMetrics();
        _cts = new CancellationTokenSource();
    }

    public void Dispose()
    {
        _cts.Dispose();
        _metrics.Dispose();
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
        optionsMonitor.Get(nameof(TestMessage)).Returns(config);
        var consumerFactory = new TestDlqConsumerFactory(_kafkaConsumer);

        var serviceProvider = Substitute.For<IServiceProvider>();
        serviceProvider.GetService(typeof(IMessageHandler<TestMessage>)).Returns(_messageHandler);
        var scope = Substitute.For<IServiceScope>();
        scope.ServiceProvider.Returns(serviceProvider);
        var scopeFactory = Substitute.For<IServiceScopeFactory>();
        scopeFactory.CreateScope().Returns(scope);

        return new DlqConsumer<string, TestMessage>(_producer, consumerFactory, scopeFactory, optionsMonitor, _metrics, _logger, timeProvider ?? TimeProvider.System);
    }

    private static ConsumeResult<string, TestMessage> CreateDlqConsumeResult(
        string key = TestMessageKey,
        TestMessage? value = null,
        string? originalTopic = TestOriginalTopic,
        bool isInvalidMessage = false,
        int reprocessAttempt = 0,
        string? batchId = null)
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
            Partition = new Partition(0),
            Offset = new Offset(1),
            Message = new Message<string, TestMessage>
            {
                Key = key,
                Value = value ?? new TestMessage { Data = "dlq-data" },
                Headers = headers
            },
            IsPartitionEOF = false
        };
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
    /// Sets up the Kafka consumer to return the given results in order, then return null (batch end).
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
    public async Task ProcessBatch_SubscribesToDeadLetterTopic()
    {
        var sut = CreateConsumer();
        _kafkaConsumer.Consume(Arg.Any<TimeSpan>()).Returns((ConsumeResult<string, TestMessage>)null!);

        await sut.ProcessDeadLetterQueueBatchAsync(TestBatchId, _cts.Token);

        _kafkaConsumer.Received(1).Subscribe(TestDlqTopic);
    }

    [Fact]
    public async Task ProcessBatch_ClosesConsumerAfterBatch()
    {
        var sut = CreateConsumer();
        _kafkaConsumer.Consume(Arg.Any<TimeSpan>()).Returns((ConsumeResult<string, TestMessage>)null!);

        await sut.ProcessDeadLetterQueueBatchAsync(TestBatchId, _cts.Token);

        _kafkaConsumer.Received(1).Close();
    }

    [Fact]
    public async Task ProcessBatch_ClosesConsumer_EvenOnError()
    {
        var sut = CreateConsumer();
        _kafkaConsumer.Consume(Arg.Any<TimeSpan>())
            .Throws(new KafkaException(new Error(ErrorCode.BrokerNotAvailable)));

        await sut.ProcessDeadLetterQueueBatchAsync(TestBatchId, _cts.Token);

        _kafkaConsumer.Received(1).Close();
    }

    [Fact]
    public async Task ProcessBatch_LogsFinishedProcessing()
    {
        var sut = CreateConsumer();
        _kafkaConsumer.Consume(Arg.Any<TimeSpan>()).Returns((ConsumeResult<string, TestMessage>)null!);

        await sut.ProcessDeadLetterQueueBatchAsync(TestBatchId, _cts.Token);

        _logger.Received().Log(
            LogLevel.Information,
            Arg.Any<EventId>(),
            Arg.Is<object>(o => o.ToString()!.Contains("Finished processing dead letter queue batch")),
            Arg.Any<Exception?>(),
            Arg.Any<Func<object, Exception?, string>>());
    }

    #endregion

    #region Happy path - in-place reprocessing

    [Fact]
    public async Task ProcessBatch_InvokesHandlerAndCommits_WithoutProducing()
    {
        var sut = CreateConsumer();
        var dlqMessage = CreateDlqConsumeResult();
        SetupConsumeSequence(dlqMessage);

        await sut.ProcessDeadLetterQueueBatchAsync(TestBatchId, _cts.Token);

        await _messageHandler.Received(1)
            .HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>());
        _kafkaConsumer.Received(1).StoreOffset(dlqMessage);
        _kafkaConsumer.Received(1).Commit();
        // In-place success never produces anywhere (no republish, no re-enqueue)
        await _producer.DidNotReceive()
            .ProduceAsync(Arg.Any<string>(), Arg.Any<Message<string, TestMessage>>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task ProcessBatch_PassesMessageValueToHandler()
    {
        var sut = CreateConsumer();
        var value = new TestMessage { Data = "reprocess-me" };
        var dlqMessage = CreateDlqConsumeResult(value: value);
        SetupConsumeSequence(dlqMessage);

        await sut.ProcessDeadLetterQueueBatchAsync(TestBatchId, _cts.Token);

        await _messageHandler.Received(1)
            .HandleMessageAsync(value, Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task ProcessBatch_CommitsOffsetAfterSuccessfulReprocess()
    {
        var sut = CreateConsumer();
        var dlqMessage = CreateDlqConsumeResult();
        SetupConsumeSequence(dlqMessage);

        await sut.ProcessDeadLetterQueueBatchAsync(TestBatchId, _cts.Token);

        _kafkaConsumer.Received(1).StoreOffset(dlqMessage);
        _kafkaConsumer.Received(1).Commit();
    }

    [Fact]
    public async Task ProcessBatch_ProcessesMultipleMessagesInOrder()
    {
        var sut = CreateConsumer();
        var msg1 = CreateDlqConsumeResult(key: "key-1");
        var msg2 = CreateDlqConsumeResult(key: "key-2");
        SetupConsumeSequence(msg1, msg2);

        await sut.ProcessDeadLetterQueueBatchAsync(TestBatchId, _cts.Token);

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
    public async Task ProcessBatch_CommitsOffsetPerMessage()
    {
        var sut = CreateConsumer();
        var msg1 = CreateDlqConsumeResult(key: "key-1");
        var msg2 = CreateDlqConsumeResult(key: "key-2");
        SetupConsumeSequence(msg1, msg2);

        await sut.ProcessDeadLetterQueueBatchAsync(TestBatchId, _cts.Token);

        _kafkaConsumer.Received(2).Commit();
    }

    [Fact]
    public async Task ProcessBatch_LogsSuccessfulReprocess()
    {
        var sut = CreateConsumer();
        var dlqMessage = CreateDlqConsumeResult();
        SetupConsumeSequence(dlqMessage);

        await sut.ProcessDeadLetterQueueBatchAsync(TestBatchId, _cts.Token);

        _logger.Received().Log(
            LogLevel.Information,
            Arg.Any<EventId>(),
            Arg.Is<object>(o => o.ToString()!.Contains("Successfully reprocessed")),
            Arg.Any<Exception?>(),
            Arg.Any<Func<object, Exception?, string>>());
    }

    #endregion

    #region Null / EOF message handling - stops batch

    [Fact]
    public async Task ProcessBatch_StopsOnNullConsumeResult()
    {
        var sut = CreateConsumer();
        _kafkaConsumer.Consume(Arg.Any<TimeSpan>()).Returns((ConsumeResult<string, TestMessage>)null!);

        await sut.ProcessDeadLetterQueueBatchAsync(TestBatchId, _cts.Token);

        await _messageHandler.DidNotReceive()
            .HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>());
        _kafkaConsumer.DidNotReceive().Commit();
    }

    [Fact]
    public async Task ProcessBatch_StopsOnNullMessageValue()
    {
        var sut = CreateConsumer();
        var nullValueResult = new ConsumeResult<string, TestMessage>
        {
            Topic = TestDlqTopic,
            Partition = new Partition(0),
            Offset = new Offset(1),
            Message = new Message<string, TestMessage> { Key = TestMessageKey, Value = null! },
            IsPartitionEOF = false
        };
        SetupConsumeSequence(nullValueResult);

        await sut.ProcessDeadLetterQueueBatchAsync(TestBatchId, _cts.Token);

        await _messageHandler.DidNotReceive()
            .HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>());
        _kafkaConsumer.DidNotReceive().Commit();
    }

    [Fact]
    public async Task ProcessBatch_StopsOnPartitionEof()
    {
        var sut = CreateConsumer();
        var eofResult = new ConsumeResult<string, TestMessage>
        {
            Topic = TestDlqTopic,
            Partition = new Partition(0),
            Offset = new Offset(1),
            Message = new Message<string, TestMessage> { Key = TestMessageKey, Value = new TestMessage { Data = "eof-data" } },
            IsPartitionEOF = true
        };
        SetupConsumeSequence(eofResult);

        await sut.ProcessDeadLetterQueueBatchAsync(TestBatchId, _cts.Token);

        await _messageHandler.DidNotReceive()
            .HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>());
        _kafkaConsumer.DidNotReceive().Commit();
    }

    [Fact]
    public async Task ProcessBatch_NullEofBreaksBatch_DoesNotContinueToNextMessage()
    {
        var sut = CreateConsumer();
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

        await sut.ProcessDeadLetterQueueBatchAsync(TestBatchId, _cts.Token);

        // EOF stops the batch, so the message after it is never processed
        await _messageHandler.DidNotReceive()
            .HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>());
    }

    #endregion

    #region Invalid message skipping

    [Fact]
    public async Task ProcessBatch_SkipsInvalidMessage_WithoutInvokingHandler()
    {
        var sut = CreateConsumer();
        var invalidMsg = CreateDlqConsumeResult(isInvalidMessage: true);
        SetupConsumeSequence(invalidMsg);

        await sut.ProcessDeadLetterQueueBatchAsync(TestBatchId, _cts.Token);

        await _messageHandler.DidNotReceive()
            .HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task ProcessBatch_InvalidMessage_CommitsOffset()
    {
        var sut = CreateConsumer();
        var invalidMsg = CreateDlqConsumeResult(isInvalidMessage: true);
        SetupConsumeSequence(invalidMsg);

        await sut.ProcessDeadLetterQueueBatchAsync(TestBatchId, _cts.Token);

        _kafkaConsumer.Received(1).StoreOffset(invalidMsg);
        _kafkaConsumer.Received(1).Commit();
    }

    [Fact]
    public async Task ProcessBatch_InvalidMessage_LogsWarning()
    {
        var sut = CreateConsumer();
        var invalidMsg = CreateDlqConsumeResult(isInvalidMessage: true);
        SetupConsumeSequence(invalidMsg);

        await sut.ProcessDeadLetterQueueBatchAsync(TestBatchId, _cts.Token);

        _logger.Received().Log(
            LogLevel.Warning,
            Arg.Any<EventId>(),
            Arg.Is<object>(o => o.ToString()!.Contains("Skipping invalid message")),
            Arg.Any<Exception?>(),
            Arg.Any<Func<object, Exception?, string>>());
    }

    [Fact]
    public async Task ProcessBatch_ContinuesProcessingAfterInvalidMessage()
    {
        var sut = CreateConsumer();
        var invalidMsg = CreateDlqConsumeResult(key: "invalid-key", isInvalidMessage: true);
        var validMsg = CreateDlqConsumeResult(key: "valid-key");
        SetupConsumeSequence(invalidMsg, validMsg);

        await sut.ProcessDeadLetterQueueBatchAsync(TestBatchId, _cts.Token);

        await _messageHandler.Received(1)
            .HandleMessageAsync(validMsg.Message.Value, Arg.Any<CancellationToken>());
        _kafkaConsumer.Received(2).Commit();
    }

    #endregion

    #region Max reprocess attempts exceeded

    [Fact]
    public async Task ProcessBatch_SkipsMessageExceedingMaxReprocessAttempts()
    {
        var sut = CreateConsumer(maxReprocessAttempts: 3);
        var exceededMsg = CreateDlqConsumeResult(reprocessAttempt: 3);
        SetupConsumeSequence(exceededMsg);

        await sut.ProcessDeadLetterQueueBatchAsync(TestBatchId, _cts.Token);

        await _messageHandler.DidNotReceive()
            .HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task ProcessBatch_MaxReprocessExceeded_CommitsOffset()
    {
        var sut = CreateConsumer(maxReprocessAttempts: 3);
        var exceededMsg = CreateDlqConsumeResult(reprocessAttempt: 3);
        SetupConsumeSequence(exceededMsg);

        await sut.ProcessDeadLetterQueueBatchAsync(TestBatchId, _cts.Token);

        _kafkaConsumer.Received(1).StoreOffset(exceededMsg);
        _kafkaConsumer.Received(1).Commit();
    }

    [Fact]
    public async Task ProcessBatch_MaxReprocessExceeded_LogsWarning()
    {
        var sut = CreateConsumer(maxReprocessAttempts: 3);
        var exceededMsg = CreateDlqConsumeResult(reprocessAttempt: 3);
        SetupConsumeSequence(exceededMsg);

        await sut.ProcessDeadLetterQueueBatchAsync(TestBatchId, _cts.Token);

        _logger.Received().Log(
            LogLevel.Warning,
            Arg.Any<EventId>(),
            Arg.Is<object>(o => o.ToString()!.Contains("exceeded max reprocess attempts")),
            Arg.Any<Exception?>(),
            Arg.Any<Func<object, Exception?, string>>());
    }

    [Fact]
    public async Task ProcessBatch_MessageAtExactMaxAttempts_IsSkipped()
    {
        var sut = CreateConsumer(maxReprocessAttempts: 3);
        var atMaxMsg = CreateDlqConsumeResult(reprocessAttempt: 3);
        SetupConsumeSequence(atMaxMsg);

        await sut.ProcessDeadLetterQueueBatchAsync(TestBatchId, _cts.Token);

        await _messageHandler.DidNotReceive()
            .HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task ProcessBatch_MessageBelowMaxAttempts_IsProcessed()
    {
        var sut = CreateConsumer(maxReprocessAttempts: 3);
        var belowMaxMsg = CreateDlqConsumeResult(reprocessAttempt: 2);
        SetupConsumeSequence(belowMaxMsg);

        await sut.ProcessDeadLetterQueueBatchAsync(TestBatchId, _cts.Token);

        await _messageHandler.Received(1)
            .HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task ProcessBatch_MessageAboveMaxAttempts_IsSkipped()
    {
        var sut = CreateConsumer(maxReprocessAttempts: 3);
        var aboveMaxMsg = CreateDlqConsumeResult(reprocessAttempt: 4);
        SetupConsumeSequence(aboveMaxMsg);

        await sut.ProcessDeadLetterQueueBatchAsync(TestBatchId, _cts.Token);

        await _messageHandler.DidNotReceive()
            .HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task ProcessBatch_ContinuesProcessingAfterExceededMessage()
    {
        var sut = CreateConsumer(maxReprocessAttempts: 3);
        var exceededMsg = CreateDlqConsumeResult(key: "exceeded-key", reprocessAttempt: 3);
        var validMsg = CreateDlqConsumeResult(key: "valid-key");
        SetupConsumeSequence(exceededMsg, validMsg);

        await sut.ProcessDeadLetterQueueBatchAsync(TestBatchId, _cts.Token);

        await _messageHandler.Received(1)
            .HandleMessageAsync(validMsg.Message.Value, Arg.Any<CancellationToken>());
        _kafkaConsumer.Received(2).Commit();
    }

    #endregion

    #region Batch ID loop detection

    [Fact]
    public async Task ProcessBatch_StopsWhenEncountersCurrentBatchId()
    {
        var sut = CreateConsumer();
        var msgWithCurrentBatchId = CreateDlqConsumeResult(key: "looped-key", batchId: TestBatchId);
        SetupConsumeSequence(msgWithCurrentBatchId);

        await sut.ProcessDeadLetterQueueBatchAsync(TestBatchId, _cts.Token);

        // Message with matching batch ID should stop the batch without processing
        await _messageHandler.DidNotReceive()
            .HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>());
        _kafkaConsumer.DidNotReceive().Commit();
    }

    [Fact]
    public async Task ProcessBatch_ProcessesMessagesWithDifferentBatchId()
    {
        var sut = CreateConsumer();
        var msg = CreateDlqConsumeResult(key: "key-1", batchId: "old-batch-id");
        SetupConsumeSequence(msg);

        await sut.ProcessDeadLetterQueueBatchAsync(TestBatchId, _cts.Token);

        // Different batch ID should not stop processing
        await _messageHandler.Received(1)
            .HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task ProcessBatch_ProcessesMessagesBeforeCurrentBatchIdEncountered()
    {
        var sut = CreateConsumer();
        var msg1 = CreateDlqConsumeResult(key: "key-1", batchId: "old-batch");
        var msg2 = CreateDlqConsumeResult(key: "key-2", batchId: TestBatchId);
        SetupConsumeSequence(msg1, msg2);

        await sut.ProcessDeadLetterQueueBatchAsync(TestBatchId, _cts.Token);

        // First message processed, second (current batch ID) stops the batch
        await _messageHandler.Received(1)
            .HandleMessageAsync(msg1.Message.Value, Arg.Any<CancellationToken>());
        _kafkaConsumer.Received(1).Commit();
    }

    [Fact]
    public async Task ProcessBatch_MessageWithNoBatchId_IsProcessedNormally()
    {
        var sut = CreateConsumer();
        var msgNoBatchId = CreateDlqConsumeResult(key: "key-no-batch", batchId: null);
        SetupConsumeSequence(msgNoBatchId);

        await sut.ProcessDeadLetterQueueBatchAsync(TestBatchId, _cts.Token);

        await _messageHandler.Received(1)
            .HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>());
    }

    #endregion

    #region In-place reprocessing - handler failure re-enqueues to DLQ

    [Fact]
    public async Task ProcessBatch_HandlerFails_ReEnqueuesToDlqWithIncrementedAttempt()
    {
        var sut = CreateConsumer();
        var dlqMessage = CreateDlqConsumeResult(reprocessAttempt: 0);
        SetupConsumeSequence(dlqMessage);
        _messageHandler.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("transient failure"));

        await sut.ProcessDeadLetterQueueBatchAsync(TestBatchId, _cts.Token);

        // Re-enqueued to the DLQ topic (not the original topic) with attempt incremented to 1 and the current batch id
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
    public async Task ProcessBatch_HandlerFails_IncrementsAttemptFromPreviousValue()
    {
        var sut = CreateConsumer(maxReprocessAttempts: 5);
        var dlqMessage = CreateDlqConsumeResult(reprocessAttempt: 2);
        SetupConsumeSequence(dlqMessage);
        _messageHandler.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("transient failure"));

        await sut.ProcessDeadLetterQueueBatchAsync(TestBatchId, _cts.Token);

        await _producer.Received(1).ProduceAsync(
            TestDlqTopic,
            Arg.Is<Message<string, TestMessage>>(m =>
                HasHeader(m.Headers, KafkaHeaders.ReprocessedAttempt, "3")),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task ProcessBatch_HandlerFails_ReEnqueueSucceeds_CommitsOffset()
    {
        var sut = CreateConsumer();
        var dlqMessage = CreateDlqConsumeResult();
        SetupConsumeSequence(dlqMessage);
        _messageHandler.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("transient failure"));

        await sut.ProcessDeadLetterQueueBatchAsync(TestBatchId, _cts.Token);

        // A successful re-enqueue means the original is safely parked, so its offset is committed
        _kafkaConsumer.Received(1).StoreOffset(dlqMessage);
        _kafkaConsumer.Received(1).Commit();
    }

    [Fact]
    public async Task ProcessBatch_HandlerThrowsInvalidMessage_SkipsAndCommits()
    {
        var sut = CreateConsumer();
        var dlqMessage = CreateDlqConsumeResult();
        SetupConsumeSequence(dlqMessage);
        _messageHandler.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidMessageException("permanent failure"));

        await sut.ProcessDeadLetterQueueBatchAsync(TestBatchId, _cts.Token);

        // Permanent failure: skipped (committed), never re-enqueued
        _kafkaConsumer.Received(1).StoreOffset(dlqMessage);
        _kafkaConsumer.Received(1).Commit();
        await _producer.DidNotReceive()
            .ProduceAsync(Arg.Any<string>(), Arg.Any<Message<string, TestMessage>>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task ProcessBatch_ReEnqueueProduceFails_StopsBatchWithoutCommitting()
    {
        var sut = CreateConsumer();
        var dlqMessage = CreateDlqConsumeResult();
        SetupConsumeSequence(dlqMessage);
        _messageHandler.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("transient failure"));
        _producer.ProduceAsync(Arg.Any<string>(), Arg.Any<Message<string, TestMessage>>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new KafkaException(new Error(ErrorCode.BrokerNotAvailable)));

        await sut.ProcessDeadLetterQueueBatchAsync(TestBatchId, _cts.Token);

        _kafkaConsumer.DidNotReceive().StoreOffset(dlqMessage);
        _kafkaConsumer.DidNotReceive().Commit();
    }

    [Fact]
    public async Task ProcessBatch_ReEnqueueProduceFails_LogsError()
    {
        var sut = CreateConsumer();
        var dlqMessage = CreateDlqConsumeResult();
        SetupConsumeSequence(dlqMessage);
        _messageHandler.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("transient failure"));
        _producer.ProduceAsync(Arg.Any<string>(), Arg.Any<Message<string, TestMessage>>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new KafkaException(new Error(ErrorCode.BrokerNotAvailable)));

        await sut.ProcessDeadLetterQueueBatchAsync(TestBatchId, _cts.Token);

        _logger.Received().Log(
            LogLevel.Error,
            Arg.Any<EventId>(),
            Arg.Is<object>(o => o.ToString()!.Contains("Failed to re-enqueue")),
            Arg.Any<Exception?>(),
            Arg.Any<Func<object, Exception?, string>>());
    }

    [Fact]
    public async Task ProcessBatch_ReEnqueueFailure_SecondMessageNotProcessed()
    {
        var sut = CreateConsumer();
        var msg1 = CreateDlqConsumeResult(key: "key-fail");
        var msg2 = CreateDlqConsumeResult(key: "key-ok");
        SetupConsumeSequence(msg1, msg2);

        _messageHandler.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("transient failure"));
        _producer.ProduceAsync(Arg.Any<string>(), Arg.Any<Message<string, TestMessage>>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new KafkaException(new Error(ErrorCode.BrokerNotAvailable)));

        await sut.ProcessDeadLetterQueueBatchAsync(TestBatchId, _cts.Token);

        // Re-enqueue failure stops the batch, no offsets committed
        _kafkaConsumer.DidNotReceive().Commit();
    }

    #endregion

    #region Cancellation handling

    [Fact]
    public async Task ProcessBatch_StopsProcessingOnCancellation()
    {
        var sut = CreateConsumer();
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
            () => sut.ProcessDeadLetterQueueBatchAsync(TestBatchId, _cts.Token));
    }

    #endregion

    #region Mixed scenarios

    [Fact]
    public async Task ProcessBatch_MixedMessages_ProcessesCorrectly()
    {
        var sut = CreateConsumer(maxReprocessAttempts: 3);
        var invalidMsg = CreateDlqConsumeResult(key: "invalid", isInvalidMessage: true);
        var exceededMsg = CreateDlqConsumeResult(key: "exceeded", reprocessAttempt: 3);
        var validMsg = CreateDlqConsumeResult(key: "valid");
        SetupConsumeSequence(invalidMsg, exceededMsg, validMsg);

        await sut.ProcessDeadLetterQueueBatchAsync(TestBatchId, _cts.Token);

        // Only the valid message should be handled in place
        await _messageHandler.Received(1)
            .HandleMessageAsync(validMsg.Message.Value, Arg.Any<CancellationToken>());
        // All 3 messages should have their offsets committed (2 skipped + 1 processed)
        _kafkaConsumer.Received(3).Commit();
    }

    [Fact]
    public async Task ProcessBatch_ReEnqueueFailsAfterSkippedMessages_StopsBatchCorrectly()
    {
        var sut = CreateConsumer(maxReprocessAttempts: 3);
        var invalidMsg = CreateDlqConsumeResult(key: "invalid", isInvalidMessage: true);
        var failMsg = CreateDlqConsumeResult(key: "will-fail");
        var afterFailMsg = CreateDlqConsumeResult(key: "after-fail");
        SetupConsumeSequence(invalidMsg, failMsg, afterFailMsg);

        _messageHandler.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("transient failure"));
        _producer.ProduceAsync(Arg.Any<string>(), Arg.Any<Message<string, TestMessage>>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new KafkaException(new Error(ErrorCode.BrokerNotAvailable)));

        await sut.ProcessDeadLetterQueueBatchAsync(TestBatchId, _cts.Token);

        // Invalid message committed, but fail message stops the batch without committing
        _kafkaConsumer.Received(1).StoreOffset(invalidMsg);
        _kafkaConsumer.DidNotReceive().StoreOffset(failMsg);
        _kafkaConsumer.DidNotReceive().StoreOffset(afterFailMsg);
    }

    [Fact]
    public async Task ProcessBatch_SkippedMessagesBeforeBatchIdLoop_AllCommitted()
    {
        var sut = CreateConsumer(maxReprocessAttempts: 2);
        var exceededMsg = CreateDlqConsumeResult(key: "exceeded", reprocessAttempt: 2);
        var loopMsg = CreateDlqConsumeResult(key: "loop", batchId: TestBatchId);
        SetupConsumeSequence(exceededMsg, loopMsg);

        await sut.ProcessDeadLetterQueueBatchAsync(TestBatchId, _cts.Token);

        // Exceeded message committed, loop message stops batch without committing
        _kafkaConsumer.Received(1).StoreOffset(exceededMsg);
        _kafkaConsumer.Received(1).Commit();
        await _messageHandler.DidNotReceive()
            .HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task ProcessBatch_EmptyBatch_NoProcessingOrCommits()
    {
        var sut = CreateConsumer();
        _kafkaConsumer.Consume(Arg.Any<TimeSpan>())
            .Returns((ConsumeResult<string, TestMessage>)null!);

        await sut.ProcessDeadLetterQueueBatchAsync(TestBatchId, _cts.Token);

        await _messageHandler.DidNotReceive()
            .HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>());
        _kafkaConsumer.DidNotReceive().StoreOffset(Arg.Any<ConsumeResult<string, TestMessage>>());
        _kafkaConsumer.DidNotReceive().Commit();
    }

    [Fact]
    public async Task ProcessBatch_AllMessagesSkipped_AllOffsetsCommitted()
    {
        var sut = CreateConsumer(maxReprocessAttempts: 1);
        var invalidMsg = CreateDlqConsumeResult(key: "invalid-1", isInvalidMessage: true);
        var exceededMsg = CreateDlqConsumeResult(key: "exceeded-1", reprocessAttempt: 1);
        SetupConsumeSequence(invalidMsg, exceededMsg);

        await sut.ProcessDeadLetterQueueBatchAsync(TestBatchId, _cts.Token);

        // All skipped messages should have offsets committed
        _kafkaConsumer.Received(2).Commit();
        await _messageHandler.DidNotReceive()
            .HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>());
    }

    #endregion

    #region ExecuteAsync lifecycle

    /// <summary>
    /// Waits for ExecuteAsync to park on Task.Delay, advances the fake clock,
    /// then waits for the batch to complete.
    /// </summary>
    private static async Task AdvanceTimeAndYieldAsync(FakeTimeProvider fakeTime, TimeSpan duration)
    {
        await Task.Delay(50);  // Let ExecuteAsync reach Task.Delay
        fakeTime.Advance(duration);
        await Task.Delay(50);  // Let batch processing complete
    }

    [Fact]
    public async Task ExecuteAsync_DoesNotProcessBatchBeforeTimerInterval()
    {
        var fakeTime = new FakeTimeProvider();
        var sut = CreateConsumer(timeProvider: fakeTime);

        await sut.StartAsync(_cts.Token);
        await Task.Delay(50);

        _kafkaConsumer.DidNotReceive().Subscribe(Arg.Any<string>());

        await sut.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task ExecuteAsync_ProcessesBatchAfterTimerInterval()
    {
        var fakeTime = new FakeTimeProvider();
        var sut = CreateConsumer(timeProvider: fakeTime);
        _kafkaConsumer.Consume(Arg.Any<TimeSpan>()).Returns((ConsumeResult<string, TestMessage>)null!);

        await sut.StartAsync(_cts.Token);
        await AdvanceTimeAndYieldAsync(fakeTime, TimeSpan.FromMinutes(60));

        _kafkaConsumer.Received(1).Subscribe(TestDlqTopic);

        await sut.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task ExecuteAsync_ProcessesMultipleBatchesOnMultipleTicks()
    {
        var fakeTime = new FakeTimeProvider();
        var sut = CreateConsumer(timeProvider: fakeTime);
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
        var sut = CreateConsumer(timeProvider: fakeTime, processingIntervalMinutes: 5);
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
        var sut = CreateConsumer(timeProvider: fakeTime);

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
    public async Task ExecuteAsync_LogsCriticalOnBatchError()
    {
        var fakeTime = new FakeTimeProvider();
        var sut = CreateConsumer(timeProvider: fakeTime);
        _kafkaConsumer.When(c => c.Subscribe(Arg.Any<string>()))
            .Throw(new InvalidOperationException("subscription failed"));

        await sut.StartAsync(_cts.Token);
        await AdvanceTimeAndYieldAsync(fakeTime, TimeSpan.FromMinutes(60));

        _logger.Received().Log(
            LogLevel.Critical,
            Arg.Any<EventId>(),
            Arg.Is<object>(o => o.ToString()!.Contains("Error processing dead letter queue batch")),
            Arg.Any<Exception?>(),
            Arg.Any<Func<object, Exception?, string>>());

        await sut.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task ExecuteAsync_ClosesConsumerOnEachTick()
    {
        var fakeTime = new FakeTimeProvider();
        var sut = CreateConsumer(timeProvider: fakeTime);
        _kafkaConsumer.Consume(Arg.Any<TimeSpan>()).Returns((ConsumeResult<string, TestMessage>)null!);

        await sut.StartAsync(_cts.Token);
        await AdvanceTimeAndYieldAsync(fakeTime, TimeSpan.FromMinutes(60));
        await AdvanceTimeAndYieldAsync(fakeTime, TimeSpan.FromMinutes(60));

        _kafkaConsumer.Received(2).Close();

        await sut.StopAsync(CancellationToken.None);
    }

    #endregion
}
