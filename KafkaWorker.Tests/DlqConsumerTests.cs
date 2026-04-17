using System.Text;
using Confluent.Kafka;
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
    private readonly ILogger<DlqConsumer<string, TestMessage>> _logger;
    private readonly KafkaWorkerMetrics _metrics;
    private readonly CancellationTokenSource _cts;

    public DlqConsumerTests()
    {
        _kafkaConsumer = Substitute.For<IConsumer<string, TestMessage>>();
        _producer = Substitute.For<IProducer<string, TestMessage>>();
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

        return new DlqConsumer<string, TestMessage>(_producer, consumerFactory, optionsMonitor, _metrics, _logger, timeProvider ?? TimeProvider.System);
    }

    private static ConsumeResult<string, TestMessage> CreateDlqConsumeResult(
        string key = TestMessageKey,
        TestMessage? value = null,
        string? originalTopic = TestOriginalTopic,
        bool isInvalidMessage = false,
        int reprocessAttempt = 0,
        string? batchId = null,
        string? failedConsumerGroupId = null)
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

        if (failedConsumerGroupId != null)
        {
            headers.Add(KafkaHeaders.FailedConsumerGroupId, Encoding.UTF8.GetBytes(failedConsumerGroupId));
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

    private static ConsumeResult<string, TestMessage> CreateNullMessageResult()
    {
        return new ConsumeResult<string, TestMessage>
        {
            Topic = TestDlqTopic,
            Partition = new Partition(0),
            Offset = new Offset(1),
            Message = new Message<string, TestMessage>
            {
                Key = TestMessageKey,
                Value = null!
            },
            IsPartitionEOF = false
        };
    }

    private static ConsumeResult<string, TestMessage> CreatePartitionEofResult()
    {
        return new ConsumeResult<string, TestMessage>
        {
            Topic = TestDlqTopic,
            Partition = new Partition(0),
            Offset = new Offset(1),
            Message = new Message<string, TestMessage>
            {
                Key = TestMessageKey,
                Value = new TestMessage { Data = "eof-data" }
            },
            IsPartitionEOF = true
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

    #region Happy path - message reprocessing

    [Fact]
    public async Task ProcessBatch_ReprocessesMessageToOriginalTopic()
    {
        var sut = CreateConsumer();
        var dlqMessage = CreateDlqConsumeResult(originalTopic: "original-orders");
        SetupConsumeSequence(dlqMessage);

        await sut.ProcessDeadLetterQueueBatchAsync(TestBatchId, _cts.Token);

        await _producer.Received(1)
            .ProduceAsync("original-orders", Arg.Any<Message<string, TestMessage>>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task ProcessBatch_ReprocessedMessageContainsOriginalKeyAndValue()
    {
        var sut = CreateConsumer();
        var originalValue = new TestMessage { Data = "important-order" };
        var dlqMessage = CreateDlqConsumeResult(key: "order-key", value: originalValue);
        SetupConsumeSequence(dlqMessage);

        await sut.ProcessDeadLetterQueueBatchAsync(TestBatchId, _cts.Token);

        await _producer.Received(1).ProduceAsync(
            TestOriginalTopic,
            Arg.Is<Message<string, TestMessage>>(m =>
                m.Key == "order-key" &&
                m.Value == originalValue),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task ProcessBatch_ReprocessedMessageContainsBatchIdHeader()
    {
        var sut = CreateConsumer();
        var dlqMessage = CreateDlqConsumeResult();
        SetupConsumeSequence(dlqMessage);

        await sut.ProcessDeadLetterQueueBatchAsync(TestBatchId, _cts.Token);

        await _producer.Received(1).ProduceAsync(
            TestOriginalTopic,
            Arg.Is<Message<string, TestMessage>>(m =>
                HasHeader(m.Headers, KafkaHeaders.BatchId, TestBatchId)),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task ProcessBatch_ReprocessedMessageContainsReprocessAttemptHeader()
    {
        var sut = CreateConsumer();
        var dlqMessage = CreateDlqConsumeResult(reprocessAttempt: 0);
        SetupConsumeSequence(dlqMessage);

        await sut.ProcessDeadLetterQueueBatchAsync(TestBatchId, _cts.Token);

        await _producer.Received(1).ProduceAsync(
            TestOriginalTopic,
            Arg.Is<Message<string, TestMessage>>(m =>
                HasHeader(m.Headers, KafkaHeaders.ReprocessedAttempt, "1")),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task ProcessBatch_IncrementsReprocessAttemptFromPreviousValue()
    {
        var sut = CreateConsumer(maxReprocessAttempts: 5);
        var dlqMessage = CreateDlqConsumeResult(reprocessAttempt: 2);
        SetupConsumeSequence(dlqMessage);

        await sut.ProcessDeadLetterQueueBatchAsync(TestBatchId, _cts.Token);

        await _producer.Received(1).ProduceAsync(
            TestOriginalTopic,
            Arg.Is<Message<string, TestMessage>>(m =>
                HasHeader(m.Headers, KafkaHeaders.ReprocessedAttempt, "3")),
            Arg.Any<CancellationToken>());
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
            _producer.ProduceAsync(TestOriginalTopic, Arg.Is<Message<string, TestMessage>>(m => m.Key == "key-1"), Arg.Any<CancellationToken>());
            _kafkaConsumer.StoreOffset(msg1);
            _kafkaConsumer.Commit();
            _producer.ProduceAsync(TestOriginalTopic, Arg.Is<Message<string, TestMessage>>(m => m.Key == "key-2"), Arg.Any<CancellationToken>());
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
            Arg.Is<object>(o => o.ToString()!.Contains("Successfully reprocessed message")),
            Arg.Any<Exception?>(),
            Arg.Any<Func<object, Exception?, string>>());
    }

    #endregion

    #region Null / EOF message handling - stops batch

    [Fact]
    public async Task ProcessBatch_StopsOnNullConsumeResult()
    {
        var sut = CreateConsumer();
        _kafkaConsumer.Consume(Arg.Any<TimeSpan>())
            .Returns((ConsumeResult<string, TestMessage>)null!);

        await sut.ProcessDeadLetterQueueBatchAsync(TestBatchId, _cts.Token);

        await _producer.DidNotReceive()
            .ProduceAsync(Arg.Any<string>(), Arg.Any<Message<string, TestMessage>>(), Arg.Any<CancellationToken>());
        _kafkaConsumer.DidNotReceive().Commit();
    }

    [Fact]
    public async Task ProcessBatch_StopsOnNullMessageValue()
    {
        var sut = CreateConsumer();
        var nullResult = CreateNullMessageResult();
        _kafkaConsumer.Consume(Arg.Any<TimeSpan>()).Returns(nullResult);

        await sut.ProcessDeadLetterQueueBatchAsync(TestBatchId, _cts.Token);

        await _producer.DidNotReceive()
            .ProduceAsync(Arg.Any<string>(), Arg.Any<Message<string, TestMessage>>(), Arg.Any<CancellationToken>());
        _kafkaConsumer.DidNotReceive().Commit();
    }

    [Fact]
    public async Task ProcessBatch_StopsOnPartitionEof()
    {
        var sut = CreateConsumer();
        var eofResult = CreatePartitionEofResult();
        _kafkaConsumer.Consume(Arg.Any<TimeSpan>()).Returns(eofResult);

        await sut.ProcessDeadLetterQueueBatchAsync(TestBatchId, _cts.Token);

        await _producer.DidNotReceive()
            .ProduceAsync(Arg.Any<string>(), Arg.Any<Message<string, TestMessage>>(), Arg.Any<CancellationToken>());
        _kafkaConsumer.DidNotReceive().Commit();
    }

    [Fact]
    public async Task ProcessBatch_NullEofBreaksBatch_DoesNotContinueToNextMessage()
    {
        var sut = CreateConsumer();
        var nullResult = CreateNullMessageResult();
        var validResult = CreateDlqConsumeResult();
        var callIndex = 0;
        _kafkaConsumer.Consume(Arg.Any<TimeSpan>())
            .Returns(_ =>
            {
                return callIndex++ switch
                {
                    0 => nullResult,
                    _ => validResult
                };
            });

        await sut.ProcessDeadLetterQueueBatchAsync(TestBatchId, _cts.Token);

        // DLQ consumer breaks on null, so the valid message is NOT processed
        await _producer.DidNotReceive()
            .ProduceAsync(Arg.Any<string>(), Arg.Any<Message<string, TestMessage>>(), Arg.Any<CancellationToken>());
    }

    #endregion

    #region Invalid message skipping

    [Fact]
    public async Task ProcessBatch_SkipsInvalidMessage_DoesNotProduce()
    {
        var sut = CreateConsumer();
        var invalidMsg = CreateDlqConsumeResult(key: "invalid-key", isInvalidMessage: true);
        SetupConsumeSequence(invalidMsg);

        await sut.ProcessDeadLetterQueueBatchAsync(TestBatchId, _cts.Token);

        await _producer.DidNotReceive()
            .ProduceAsync(Arg.Any<string>(), Arg.Any<Message<string, TestMessage>>(), Arg.Any<CancellationToken>());
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
            Arg.Is<object>(o => o.ToString()!.Contains("invalid message")),
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

        // Invalid message skipped, valid message reprocessed
        await _producer.Received(1)
            .ProduceAsync(TestOriginalTopic, Arg.Is<Message<string, TestMessage>>(m => m.Key == "valid-key"), Arg.Any<CancellationToken>());
        // Both offsets committed
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

        await _producer.DidNotReceive()
            .ProduceAsync(Arg.Any<string>(), Arg.Any<Message<string, TestMessage>>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task ProcessBatch_MaxReprocessExceeded_CommitsOffset()
    {
        var sut = CreateConsumer(maxReprocessAttempts: 2);
        var exceededMsg = CreateDlqConsumeResult(reprocessAttempt: 2);
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
        var sut = CreateConsumer(maxReprocessAttempts: 1);
        var atLimitMsg = CreateDlqConsumeResult(reprocessAttempt: 1);
        SetupConsumeSequence(atLimitMsg);

        await sut.ProcessDeadLetterQueueBatchAsync(TestBatchId, _cts.Token);

        await _producer.DidNotReceive()
            .ProduceAsync(Arg.Any<string>(), Arg.Any<Message<string, TestMessage>>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task ProcessBatch_MessageBelowMaxAttempts_IsProcessed()
    {
        var sut = CreateConsumer(maxReprocessAttempts: 3);
        var belowLimitMsg = CreateDlqConsumeResult(reprocessAttempt: 2);
        SetupConsumeSequence(belowLimitMsg);

        await sut.ProcessDeadLetterQueueBatchAsync(TestBatchId, _cts.Token);

        await _producer.Received(1)
            .ProduceAsync(TestOriginalTopic, Arg.Any<Message<string, TestMessage>>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task ProcessBatch_MessageAboveMaxAttempts_IsSkipped()
    {
        var sut = CreateConsumer(maxReprocessAttempts: 2);
        var aboveLimitMsg = CreateDlqConsumeResult(reprocessAttempt: 5);
        SetupConsumeSequence(aboveLimitMsg);

        await sut.ProcessDeadLetterQueueBatchAsync(TestBatchId, _cts.Token);

        await _producer.DidNotReceive()
            .ProduceAsync(Arg.Any<string>(), Arg.Any<Message<string, TestMessage>>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task ProcessBatch_ContinuesProcessingAfterExceededMessage()
    {
        var sut = CreateConsumer(maxReprocessAttempts: 2);
        var exceededMsg = CreateDlqConsumeResult(key: "exceeded-key", reprocessAttempt: 2);
        var validMsg = CreateDlqConsumeResult(key: "valid-key", reprocessAttempt: 0);
        SetupConsumeSequence(exceededMsg, validMsg);

        await sut.ProcessDeadLetterQueueBatchAsync(TestBatchId, _cts.Token);

        await _producer.Received(1)
            .ProduceAsync(TestOriginalTopic, Arg.Is<Message<string, TestMessage>>(m => m.Key == "valid-key"), Arg.Any<CancellationToken>());
        _kafkaConsumer.Received(2).Commit();
    }

    #endregion

    #region Missing original topic header

    [Fact]
    public async Task ProcessBatch_SkipsMessageWithoutOriginalTopicHeader()
    {
        var sut = CreateConsumer();
        var noTopicMsg = CreateDlqConsumeResult(originalTopic: null);
        SetupConsumeSequence(noTopicMsg);

        await sut.ProcessDeadLetterQueueBatchAsync(TestBatchId, _cts.Token);

        await _producer.DidNotReceive()
            .ProduceAsync(Arg.Any<string>(), Arg.Any<Message<string, TestMessage>>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task ProcessBatch_MissingOriginalTopic_CommitsOffset()
    {
        var sut = CreateConsumer();
        var noTopicMsg = CreateDlqConsumeResult(originalTopic: null);
        SetupConsumeSequence(noTopicMsg);

        await sut.ProcessDeadLetterQueueBatchAsync(TestBatchId, _cts.Token);

        _kafkaConsumer.Received(1).StoreOffset(noTopicMsg);
        _kafkaConsumer.Received(1).Commit();
    }

    [Fact]
    public async Task ProcessBatch_MissingOriginalTopic_LogsWarning()
    {
        var sut = CreateConsumer();
        var noTopicMsg = CreateDlqConsumeResult(originalTopic: null);
        SetupConsumeSequence(noTopicMsg);

        await sut.ProcessDeadLetterQueueBatchAsync(TestBatchId, _cts.Token);

        _logger.Received().Log(
            LogLevel.Warning,
            Arg.Any<EventId>(),
            Arg.Is<object>(o => o.ToString()!.Contains("Original topic header missing")),
            Arg.Any<Exception?>(),
            Arg.Any<Func<object, Exception?, string>>());
    }

    [Fact]
    public async Task ProcessBatch_ContinuesProcessingAfterMissingOriginalTopic()
    {
        var sut = CreateConsumer();
        var noTopicMsg = CreateDlqConsumeResult(key: "no-topic-key", originalTopic: null);
        var validMsg = CreateDlqConsumeResult(key: "valid-key");
        SetupConsumeSequence(noTopicMsg, validMsg);

        await sut.ProcessDeadLetterQueueBatchAsync(TestBatchId, _cts.Token);

        await _producer.Received(1)
            .ProduceAsync(TestOriginalTopic, Arg.Is<Message<string, TestMessage>>(m => m.Key == "valid-key"), Arg.Any<CancellationToken>());
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
        await _producer.DidNotReceive()
            .ProduceAsync(Arg.Any<string>(), Arg.Any<Message<string, TestMessage>>(), Arg.Any<CancellationToken>());
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
        await _producer.Received(1)
            .ProduceAsync(TestOriginalTopic, Arg.Any<Message<string, TestMessage>>(), Arg.Any<CancellationToken>());
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
        await _producer.Received(1)
            .ProduceAsync(TestOriginalTopic, Arg.Is<Message<string, TestMessage>>(m => m.Key == "key-1"), Arg.Any<CancellationToken>());
        _kafkaConsumer.Received(1).Commit();
    }

    [Fact]
    public async Task ProcessBatch_MessageWithNoBatchId_IsProcessedNormally()
    {
        var sut = CreateConsumer();
        var msgNoBatchId = CreateDlqConsumeResult(key: "key-no-batch", batchId: null);
        SetupConsumeSequence(msgNoBatchId);

        await sut.ProcessDeadLetterQueueBatchAsync(TestBatchId, _cts.Token);

        await _producer.Received(1)
            .ProduceAsync(TestOriginalTopic, Arg.Any<Message<string, TestMessage>>(), Arg.Any<CancellationToken>());
    }

    #endregion

    #region Produce failure - stops batch without committing

    [Fact]
    public async Task ProcessBatch_ProduceFailure_DoesNotCommitOffset()
    {
        var sut = CreateConsumer();
        var dlqMessage = CreateDlqConsumeResult();
        SetupConsumeSequence(dlqMessage);

        _producer.ProduceAsync(Arg.Any<string>(), Arg.Any<Message<string, TestMessage>>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new KafkaException(new Error(ErrorCode.BrokerNotAvailable)));

        await sut.ProcessDeadLetterQueueBatchAsync(TestBatchId, _cts.Token);

        _kafkaConsumer.DidNotReceive().StoreOffset(dlqMessage);
        _kafkaConsumer.DidNotReceive().Commit();
    }

    [Fact]
    public async Task ProcessBatch_ProduceFailure_LogsError()
    {
        var sut = CreateConsumer();
        var dlqMessage = CreateDlqConsumeResult();
        SetupConsumeSequence(dlqMessage);

        _producer.ProduceAsync(Arg.Any<string>(), Arg.Any<Message<string, TestMessage>>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new KafkaException(new Error(ErrorCode.BrokerNotAvailable)));

        await sut.ProcessDeadLetterQueueBatchAsync(TestBatchId, _cts.Token);

        _logger.Received().Log(
            LogLevel.Error,
            Arg.Any<EventId>(),
            Arg.Is<object>(o => o.ToString()!.Contains("Failed to reprocess dead letter message")),
            Arg.Any<Exception?>(),
            Arg.Any<Func<object, Exception?, string>>());
    }

    [Fact]
    public async Task ProcessBatch_ProduceFailure_LogsWarningAboutStoppingBatch()
    {
        var sut = CreateConsumer();
        var dlqMessage = CreateDlqConsumeResult();
        SetupConsumeSequence(dlqMessage);

        _producer.ProduceAsync(Arg.Any<string>(), Arg.Any<Message<string, TestMessage>>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new KafkaException(new Error(ErrorCode.BrokerNotAvailable)));

        await sut.ProcessDeadLetterQueueBatchAsync(TestBatchId, _cts.Token);

        _logger.Received().Log(
            LogLevel.Warning,
            Arg.Any<EventId>(),
            Arg.Is<object>(o => o.ToString()!.Contains("Stopping batch due to failed reprocess")),
            Arg.Any<Exception?>(),
            Arg.Any<Func<object, Exception?, string>>());
    }

    [Fact]
    public async Task ProcessBatch_ProduceFailure_SecondMessageNotProcessed()
    {
        var sut = CreateConsumer();
        var msg1 = CreateDlqConsumeResult(key: "key-fail");
        var msg2 = CreateDlqConsumeResult(key: "key-ok");
        SetupConsumeSequence(msg1, msg2);

        _producer.ProduceAsync(Arg.Any<string>(), Arg.Any<Message<string, TestMessage>>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new KafkaException(new Error(ErrorCode.BrokerNotAvailable)));

        await sut.ProcessDeadLetterQueueBatchAsync(TestBatchId, _cts.Token);

        // Batch stops on failure, no offsets committed
        _kafkaConsumer.DidNotReceive().Commit();
    }

    [Fact]
    public async Task ProcessBatch_ProduceFailure_StopsBatchImmediately()
    {
        var sut = CreateConsumer();
        var msg1 = CreateDlqConsumeResult(key: "key-1");
        var msg2 = CreateDlqConsumeResult(key: "key-2");
        var msg3 = CreateDlqConsumeResult(key: "key-3");
        SetupConsumeSequence(msg1, msg2, msg3);

        var callCount = 0;
        _producer.ProduceAsync(Arg.Any<string>(), Arg.Any<Message<string, TestMessage>>(), Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                if (callCount++ == 0)
                {
                    return Task.FromResult(new DeliveryResult<string, TestMessage>());
                }
                throw new KafkaException(new Error(ErrorCode.BrokerNotAvailable));
            });

        await sut.ProcessDeadLetterQueueBatchAsync(TestBatchId, _cts.Token);

        // First message succeeds and is committed; second fails, batch stops
        _kafkaConsumer.Received(1).Commit();
        _kafkaConsumer.Received(1).StoreOffset(msg1);
        _kafkaConsumer.DidNotReceive().StoreOffset(msg2);
        _kafkaConsumer.DidNotReceive().StoreOffset(msg3);
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

        _producer.ProduceAsync(Arg.Any<string>(), Arg.Any<Message<string, TestMessage>>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new DeliveryResult<string, TestMessage>()));

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
        var noTopicMsg = CreateDlqConsumeResult(key: "no-topic", originalTopic: null);
        var validMsg = CreateDlqConsumeResult(key: "valid");
        SetupConsumeSequence(invalidMsg, exceededMsg, noTopicMsg, validMsg);

        await sut.ProcessDeadLetterQueueBatchAsync(TestBatchId, _cts.Token);

        // Only the valid message should be produced to the original topic
        await _producer.Received(1)
            .ProduceAsync(TestOriginalTopic, Arg.Is<Message<string, TestMessage>>(m => m.Key == "valid"), Arg.Any<CancellationToken>());
        // All 4 messages should have their offsets committed (3 skipped + 1 processed)
        _kafkaConsumer.Received(4).Commit();
    }

    [Fact]
    public async Task ProcessBatch_ProduceFailsAfterSkippedMessages_StopsBatchCorrectly()
    {
        var sut = CreateConsumer(maxReprocessAttempts: 3);
        var invalidMsg = CreateDlqConsumeResult(key: "invalid", isInvalidMessage: true);
        var failMsg = CreateDlqConsumeResult(key: "will-fail");
        var afterFailMsg = CreateDlqConsumeResult(key: "after-fail");
        SetupConsumeSequence(invalidMsg, failMsg, afterFailMsg);

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
        await _producer.DidNotReceive()
            .ProduceAsync(Arg.Any<string>(), Arg.Any<Message<string, TestMessage>>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task ProcessBatch_EmptyBatch_NoProcessingOrCommits()
    {
        var sut = CreateConsumer();
        _kafkaConsumer.Consume(Arg.Any<TimeSpan>())
            .Returns((ConsumeResult<string, TestMessage>)null!);

        await sut.ProcessDeadLetterQueueBatchAsync(TestBatchId, _cts.Token);

        await _producer.DidNotReceive()
            .ProduceAsync(Arg.Any<string>(), Arg.Any<Message<string, TestMessage>>(), Arg.Any<CancellationToken>());
        _kafkaConsumer.DidNotReceive().StoreOffset(Arg.Any<ConsumeResult<string, TestMessage>>());
        _kafkaConsumer.DidNotReceive().Commit();
    }

    [Fact]
    public async Task ProcessBatch_AllMessagesSkipped_AllOffsetsCommitted()
    {
        var sut = CreateConsumer(maxReprocessAttempts: 1);
        var invalidMsg = CreateDlqConsumeResult(key: "invalid-1", isInvalidMessage: true);
        var exceededMsg = CreateDlqConsumeResult(key: "exceeded-1", reprocessAttempt: 1);
        var noTopicMsg = CreateDlqConsumeResult(key: "no-topic-1", originalTopic: null);
        SetupConsumeSequence(invalidMsg, exceededMsg, noTopicMsg);

        await sut.ProcessDeadLetterQueueBatchAsync(TestBatchId, _cts.Token);

        // All skipped messages should have offsets committed
        _kafkaConsumer.Received(3).Commit();
        await _producer.DidNotReceive()
            .ProduceAsync(Arg.Any<string>(), Arg.Any<Message<string, TestMessage>>(), Arg.Any<CancellationToken>());
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

    #region Consumer group isolation - header forwarding

    [Fact]
    public async Task ProcessBatch_WithFailedConsumerGroupId_ForwardsHeader()
    {
        var sut = CreateConsumer();
        var dlqMessage = CreateDlqConsumeResult(failedConsumerGroupId: "order-processor");
        SetupConsumeSequence(dlqMessage);

        await sut.ProcessDeadLetterQueueBatchAsync(TestBatchId, _cts.Token);

        await _producer.Received(1).ProduceAsync(
            TestOriginalTopic,
            Arg.Is<Message<string, TestMessage>>(m =>
                HasHeader(m.Headers, KafkaHeaders.FailedConsumerGroupId, "order-processor")),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task ProcessBatch_WithoutFailedConsumerGroupId_DoesNotAddHeader()
    {
        var sut = CreateConsumer();
        var dlqMessage = CreateDlqConsumeResult();
        SetupConsumeSequence(dlqMessage);

        await sut.ProcessDeadLetterQueueBatchAsync(TestBatchId, _cts.Token);

        await _producer.Received(1).ProduceAsync(
            TestOriginalTopic,
            Arg.Is<Message<string, TestMessage>>(m =>
                !m.Headers.Any(h => h.Key == KafkaHeaders.FailedConsumerGroupId)),
            Arg.Any<CancellationToken>());
    }

    #endregion
}
