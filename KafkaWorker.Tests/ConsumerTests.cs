using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using NSubstitute.ExceptionExtensions;

namespace KafkaWorker.Tests;

public class ConsumerTests : IDisposable
{
    private const string TestTopic = "test-topic";
    private const string TestDlqTopic = "test-dlq-topic";
    private const string TestMessageKey = "test-key";

    private readonly IConsumer<string, TestMessage> _kafkaConsumer;
    private readonly IProducer<string, TestMessage> _deadLetterProducer;
    private readonly IProducer<byte[], byte[]> _rawDeadLetterProducer;
    private readonly IMessageHandler<TestMessage> _messageHandler;
    private readonly ITerminalFailureSink<TestMessage> _terminalSink;
    private readonly ILogger<Consumer<string, TestMessage>> _logger;
    private readonly KafkaWorkerMetrics _metrics;
    private readonly CancellationTokenSource _cts;

    public ConsumerTests()
    {
        _kafkaConsumer = Substitute.For<IConsumer<string, TestMessage>>();
        _deadLetterProducer = Substitute.For<IProducer<string, TestMessage>>();
        _rawDeadLetterProducer = Substitute.For<IProducer<byte[], byte[]>>();
        _terminalSink = Substitute.For<ITerminalFailureSink<TestMessage>>();
        _messageHandler = Substitute.For<IMessageHandler<TestMessage>>();
        _logger = Substitute.For<ILogger<Consumer<string, TestMessage>>>();
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

    private Consumer<string, TestMessage> CreateConsumer(
        string topic = TestTopic,
        string? deadLetterTopic = TestDlqTopic,
        int maxRetries = 0,
        bool registerTerminalSink = true)
    {
        var config = new KafkaWorkerConfig
        {
            GroupId = "test-group",
            Topic = topic,
            DeadLetterTopic = deadLetterTopic,
            MaxRetries = maxRetries
        };

        var optionsMonitor = Substitute.For<IOptionsMonitor<KafkaWorkerConfig>>();
        optionsMonitor.Get(typeof(TestMessage).FullName).Returns(config);

        var serviceProvider = Substitute.For<IServiceProvider>();
        serviceProvider.GetService(typeof(IMessageHandler<TestMessage>)).Returns(_messageHandler);
        if (registerTerminalSink)
        {
            serviceProvider.GetService(typeof(ITerminalFailureSink<TestMessage>)).Returns(_terminalSink);
        }
        var scope = Substitute.For<IServiceScope>();
        scope.ServiceProvider.Returns(serviceProvider);
        var scopeFactory = Substitute.For<IServiceScopeFactory>();
        scopeFactory.CreateScope().Returns(scope);

        return new Consumer<string, TestMessage>(
            _kafkaConsumer,
            new Lazy<IProducer<string, TestMessage>>(() => _deadLetterProducer),
            new RawDeadLetterProducer<TestMessage>(() => _rawDeadLetterProducer),
            scopeFactory,
            optionsMonitor,
            _metrics,
            _logger);
    }

    private static ConsumeResult<string, TestMessage> CreateConsumeResult(
        string key = TestMessageKey,
        TestMessage? value = null,
        Headers? headers = null)
    {
        return new ConsumeResult<string, TestMessage>
        {
            Topic = TestTopic,
            Partition = new Partition(0),
            Offset = new Offset(1),
            Message = new Message<string, TestMessage>
            {
                Key = key,
                Value = value ?? new TestMessage { Data = "test-data" },
                Headers = headers
            },
            IsPartitionEOF = false
        };
    }

    private static ConsumeResult<string, TestMessage> CreateNullMessageResult()
    {
        return new ConsumeResult<string, TestMessage>
        {
            Topic = TestTopic,
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
            Topic = TestTopic,
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

    /// <summary>
    /// Creates a ConsumeException representing a record that failed value deserialization.
    /// </summary>
    private static ConsumeException CreatePoisonException(
        long offset = 5,
        ErrorCode code = ErrorCode.Local_ValueDeserialization)
    {
        return new ConsumeException(
            new ConsumeResult<byte[], byte[]>
            {
                Topic = TestTopic,
                Partition = new Partition(0),
                Offset = new Offset(offset),
                Message = new Message<byte[], byte[]>
                {
                    Key = "poison-key"u8.ToArray(),
                    Value = "not-valid-payload"u8.ToArray()
                }
            },
            new Error(code));
    }

    /// <summary>
    /// Sets up the Kafka consumer to return the given results in order, then cancel the token.
    /// </summary>
    private void SetupConsumeSequence(params ConsumeResult<string, TestMessage>[] results)
    {
        var callIndex = 0;
        _kafkaConsumer.Consume(Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                if (callIndex < results.Length)
                {
                    return results[callIndex++];
                }

                _cts.Cancel();
                throw new OperationCanceledException(_cts.Token);
            });
    }

    /// <summary>
    /// Asynchronously waits until a cancellation is requested on the specified CancellationTokenSource.
    /// </summary>
    private static async Task WaitUntilCancellationRequestedAsync(CancellationTokenSource cts)
    {
        while (cts.Token.IsCancellationRequested == false)
        {
            await Task.Delay(100);
        }
        await Task.CompletedTask;
    }


    /// <summary>
    /// Sets up the Kafka consumer to return one valid message, then cancel the token.
    /// </summary>
    private ConsumeResult<string, TestMessage> SetupSingleMessage(
        string key = TestMessageKey,
        TestMessage? value = null,
        Headers? headers = null)
    {
        var result = CreateConsumeResult(key, value, headers);
        SetupConsumeSequence(result);
        return result;
    }

    private static bool HasHeader(Headers? headers, string key, string? expectedValue = null)
    {
        if (headers == null) return false;

        try
        {
            var header = headers.GetLastBytes(key);
            if (header == null) return false;
            if (expectedValue == null) return true;
            return System.Text.Encoding.UTF8.GetString(header) == expectedValue;
        }
        catch (KeyNotFoundException)
        {
            return false;
        }
    }

    #endregion

    #region Subscription and lifecycle

    [Fact]
    public async Task ExecuteAsync_SubscribesToConfiguredTopic()
    {
        var sut = CreateConsumer(topic: "my-custom-topic");
        _kafkaConsumer.Consume(Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                _cts.Cancel();
                throw new OperationCanceledException(_cts.Token);
            });

        await sut.StartAsync(_cts.Token);
        await WaitUntilCancellationRequestedAsync(_cts);
        await sut.StopAsync(CancellationToken.None);

        _kafkaConsumer.Received(1).Subscribe("my-custom-topic");
    }

    [Fact]
    public async Task ExecuteAsync_ClosesConsumerOnShutdown()
    {
        var sut = CreateConsumer();
        _kafkaConsumer.Consume(Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                _cts.Cancel();
                throw new OperationCanceledException(_cts.Token);
            });

        await sut.StartAsync(_cts.Token);
        await WaitUntilCancellationRequestedAsync(_cts);
        await sut.StopAsync(CancellationToken.None);

        _kafkaConsumer.Received(1).Close();
    }

    [Fact]
    public async Task ExecuteAsync_ClosesConsumer_EvenOnFatalError()
    {
        var sut = CreateConsumer();
        _kafkaConsumer.Consume(Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                _cts.Cancel();
                throw new InvalidOperationException("Kafka broker unavailable");
            });

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await sut.StartAsync(_cts.Token);
            await sut.ExecuteTask!;
        });

        _kafkaConsumer.Received(1).Close();
    }

    [Fact]
    public async Task ExecuteAsync_LogsWarningOnGracefulShutdown()
    {
        var sut = CreateConsumer();
        _kafkaConsumer.Consume(Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                _cts.Cancel();
                throw new OperationCanceledException(_cts.Token);
            });

        await sut.StartAsync(_cts.Token);
        await WaitUntilCancellationRequestedAsync(_cts);
        await sut.StopAsync(CancellationToken.None);

        _logger.Received().Log(
            LogLevel.Warning,
            Arg.Any<EventId>(),
            Arg.Is<object>(o => o.ToString()!.Contains("shutting down")),
            Arg.Any<Exception?>(),
            Arg.Any<Func<object, Exception?, string>>());
    }

    [Fact]
    public async Task ExecuteAsync_LogsCriticalOnFatalError()
    {
        var sut = CreateConsumer();
        _kafkaConsumer.Consume(Arg.Any<CancellationToken>())
            .Returns(_ => throw new InvalidOperationException("fatal"));

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await sut.StartAsync(_cts.Token);
            await sut.ExecuteTask!;
        });

        _logger.Received().Log(
            LogLevel.Critical,
            Arg.Any<EventId>(),
            Arg.Is<object>(o => o.ToString()!.Contains("Fatal error")),
            Arg.Any<Exception?>(),
            Arg.Any<Func<object, Exception?, string>>());
    }

    #endregion

    #region Happy path - message handling and offset storing

    [Fact]
    public async Task ExecuteAsync_HandlesMessageAndStoresOffset()
    {
        var sut = CreateConsumer();
        var consumeResult = SetupSingleMessage();

        await sut.StartAsync(_cts.Token);
        await WaitUntilCancellationRequestedAsync(_cts);
        await sut.StopAsync(CancellationToken.None);

        await _messageHandler.Received(1)
            .HandleMessageAsync(consumeResult.Message.Value, Arg.Any<CancellationToken>());
        _kafkaConsumer.Received(1).StoreOffset(consumeResult);
        // Stored offsets are flushed by the client's background auto-commit, never synchronously
        _kafkaConsumer.DidNotReceive().Commit();
    }

    [Fact]
    public async Task ExecuteAsync_HandlesMultipleMessagesInOrder()
    {
        var sut = CreateConsumer();
        var msg1 = CreateConsumeResult("key-1", new TestMessage { Data = "first" });
        var msg2 = CreateConsumeResult("key-2", new TestMessage { Data = "second" });
        SetupConsumeSequence(msg1, msg2);

        await sut.StartAsync(_cts.Token);
        await WaitUntilCancellationRequestedAsync(_cts);
        await sut.StopAsync(CancellationToken.None);

        Received.InOrder(() =>
        {
            _messageHandler.HandleMessageAsync(msg1.Message.Value, Arg.Any<CancellationToken>());
            _kafkaConsumer.StoreOffset(msg1);
            _messageHandler.HandleMessageAsync(msg2.Message.Value, Arg.Any<CancellationToken>());
            _kafkaConsumer.StoreOffset(msg2);
        });
    }

    [Fact]
    public async Task ExecuteAsync_StoresOffsetPerMessage()
    {
        var sut = CreateConsumer();
        var msg1 = CreateConsumeResult("key-1");
        var msg2 = CreateConsumeResult("key-2");
        SetupConsumeSequence(msg1, msg2);

        await sut.StartAsync(_cts.Token);
        await WaitUntilCancellationRequestedAsync(_cts);
        await sut.StopAsync(CancellationToken.None);

        _kafkaConsumer.Received(1).StoreOffset(msg1);
        _kafkaConsumer.Received(1).StoreOffset(msg2);
        _kafkaConsumer.DidNotReceive().Commit();
    }

    #endregion

    #region Null / EOF message skipping

    [Fact]
    public async Task ExecuteAsync_SkipsNullConsumeResult()
    {
        var sut = CreateConsumer();
        var callIndex = 0;
        _kafkaConsumer.Consume(Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                if (callIndex++ == 0)
                    return null!;
                _cts.Cancel();
                throw new OperationCanceledException(_cts.Token);
            });

        await sut.StartAsync(_cts.Token);
        await WaitUntilCancellationRequestedAsync(_cts);
        await sut.StopAsync(CancellationToken.None);

        await _messageHandler.DidNotReceive()
            .HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>());
        _kafkaConsumer.DidNotReceive().StoreOffset(Arg.Any<ConsumeResult<string, TestMessage>>());
    }

    [Fact]
    public async Task ExecuteAsync_NullValueMessage_SkipsHandlerButStoresOffset()
    {
        var sut = CreateConsumer();
        var nullResult = CreateNullMessageResult();
        var callIndex = 0;
        _kafkaConsumer.Consume(Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                if (callIndex++ == 0)
                    return nullResult;
                _cts.Cancel();
                throw new OperationCanceledException(_cts.Token);
            });

        await sut.StartAsync(_cts.Token);
        await WaitUntilCancellationRequestedAsync(_cts);
        await sut.StopAsync(CancellationToken.None);

        // Tombstones are never handled, but their offset is stored so the consumer advances
        await _messageHandler.DidNotReceive()
            .HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>());
        _kafkaConsumer.Received(1).StoreOffset(nullResult);
    }

    [Fact]
    public async Task ExecuteAsync_SkipsPartitionEofMessage()
    {
        var sut = CreateConsumer();
        var eofResult = CreatePartitionEofResult();
        var callIndex = 0;
        _kafkaConsumer.Consume(Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                if (callIndex++ == 0)
                    return eofResult;
                _cts.Cancel();
                throw new OperationCanceledException(_cts.Token);
            });

        await sut.StartAsync(_cts.Token);
        await WaitUntilCancellationRequestedAsync(_cts);
        await sut.StopAsync(CancellationToken.None);

        await _messageHandler.DidNotReceive()
            .HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>());
        _kafkaConsumer.DidNotReceive().StoreOffset(Arg.Any<ConsumeResult<string, TestMessage>>());
    }

    [Fact]
    public async Task ExecuteAsync_HandlesValidMessageAfterSkippingNullAndEof()
    {
        var sut = CreateConsumer();
        var nullResult = CreateNullMessageResult();
        var eofResult = CreatePartitionEofResult();
        var validResult = CreateConsumeResult();
        SetupConsumeSequence(nullResult, eofResult, validResult);

        await sut.StartAsync(_cts.Token);
        await WaitUntilCancellationRequestedAsync(_cts);
        await sut.StopAsync(CancellationToken.None);

        await _messageHandler.Received(1)
            .HandleMessageAsync(validResult.Message.Value, Arg.Any<CancellationToken>());
        // The tombstone stores its offset; the EOF result does not
        _kafkaConsumer.Received(1).StoreOffset(nullResult);
        _kafkaConsumer.Received(1).StoreOffset(validResult);
        _kafkaConsumer.DidNotReceive().StoreOffset(eofResult);
    }

    #endregion

    #region Handler failure - DLQ publish

    [Fact]
    public async Task HandlerFailure_PublishesToDeadLetterTopic()
    {
        var sut = CreateConsumer();
        SetupSingleMessage();
        _messageHandler.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("processing failed"));

        await sut.StartAsync(_cts.Token);
        await WaitUntilCancellationRequestedAsync(_cts);
        await sut.StopAsync(CancellationToken.None);

        await _deadLetterProducer.Received()
            .ProduceAsync(TestDlqTopic, Arg.Any<Message<string, TestMessage>>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task HandlerFailure_DlqMessageContainsOriginalKeyAndValue()
    {
        var sut = CreateConsumer();
        var originalValue = new TestMessage { Data = "important-data" };
        SetupSingleMessage(key: "my-key", value: originalValue);
        _messageHandler.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("fail"));

        await sut.StartAsync(_cts.Token);
        await WaitUntilCancellationRequestedAsync(_cts);
        await sut.StopAsync(CancellationToken.None);

        await _deadLetterProducer.Received().ProduceAsync(
            TestDlqTopic,
            Arg.Is<Message<string, TestMessage>>(m =>
                m.Key == "my-key" &&
                m.Value == originalValue),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task HandlerFailure_DlqMessageContainsOriginalTopicHeader()
    {
        var sut = CreateConsumer();
        SetupSingleMessage();
        _messageHandler.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("fail"));

        await sut.StartAsync(_cts.Token);
        await WaitUntilCancellationRequestedAsync(_cts);
        await sut.StopAsync(CancellationToken.None);

        await _deadLetterProducer.Received().ProduceAsync(
            TestDlqTopic,
            Arg.Is<Message<string, TestMessage>>(m =>
                HasHeader(m.Headers, KafkaHeaders.OriginalTopic, TestTopic)),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task HandlerFailure_DlqMessageContainsErrorMessageHeader()
    {
        var sut = CreateConsumer();
        SetupSingleMessage();
        _messageHandler.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("something broke"));

        await sut.StartAsync(_cts.Token);
        await WaitUntilCancellationRequestedAsync(_cts);
        await sut.StopAsync(CancellationToken.None);

        await _deadLetterProducer.Received().ProduceAsync(
            TestDlqTopic,
            Arg.Is<Message<string, TestMessage>>(m =>
                HasHeader(m.Headers, KafkaHeaders.ErrorMessage, "something broke")),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task HandlerFailure_DlqMessageCopiesOriginalHeaders()
    {
        var sut = CreateConsumer();
        var originalHeaders = new Headers
        {
            { "correlation-id", System.Text.Encoding.UTF8.GetBytes("abc-123") }
        };
        SetupSingleMessage(headers: originalHeaders);
        _messageHandler.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("fail"));

        await sut.StartAsync(_cts.Token);
        await WaitUntilCancellationRequestedAsync(_cts);
        await sut.StopAsync(CancellationToken.None);

        await _deadLetterProducer.Received().ProduceAsync(
            TestDlqTopic,
            Arg.Is<Message<string, TestMessage>>(m =>
                HasHeader(m.Headers, "correlation-id", "abc-123")),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task HandlerFailure_StoresOffsetAfterDlqPublish()
    {
        var sut = CreateConsumer();
        var consumeResult = SetupSingleMessage();
        _messageHandler.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("fail"));

        await sut.StartAsync(_cts.Token);
        await WaitUntilCancellationRequestedAsync(_cts);
        await sut.StopAsync(CancellationToken.None);

        _kafkaConsumer.Received(1).StoreOffset(consumeResult);
    }

    [Fact]
    public async Task HandlerFailure_NormalException_NoInvalidMessageHeader()
    {
        var sut = CreateConsumer();
        SetupSingleMessage();
        _messageHandler.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("normal failure"));

        await sut.StartAsync(_cts.Token);
        await WaitUntilCancellationRequestedAsync(_cts);
        await sut.StopAsync(CancellationToken.None);

        await _deadLetterProducer.Received().ProduceAsync(
            TestDlqTopic,
            Arg.Is<Message<string, TestMessage>>(m =>
                !HasHeader(m.Headers, KafkaHeaders.InvalidMessage, null)),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task HandlerFailure_NullOriginalHeaders_DoesNotThrow()
    {
        var sut = CreateConsumer();
        // Create a message with null headers
        var result = new ConsumeResult<string, TestMessage>
        {
            Topic = TestTopic,
            Partition = new Partition(0),
            Offset = new Offset(1),
            Message = new Message<string, TestMessage>
            {
                Key = TestMessageKey,
                Value = new TestMessage { Data = "data" },
                Headers = null
            },
            IsPartitionEOF = false
        };
        SetupConsumeSequence(result);
        _messageHandler.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("fail"));

        await sut.StartAsync(_cts.Token);
        await sut.ExecuteTask!;
        await sut.StopAsync(CancellationToken.None);

        // Should still publish to DLQ without throwing NullReferenceException
        await _deadLetterProducer.Received()
            .ProduceAsync(TestDlqTopic, Arg.Any<Message<string, TestMessage>>(), Arg.Any<CancellationToken>());
        _kafkaConsumer.Received(1).StoreOffset(result);
    }

    #endregion

    #region Invalid message handling

    [Fact]
    public async Task InvalidMessage_BypassesRetryAndPublishesToDlq()
    {
        var sut = CreateConsumer(maxRetries: 3);
        SetupSingleMessage();
        _messageHandler.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidMessageException("bad schema"));

        await sut.StartAsync(_cts.Token);
        await WaitUntilCancellationRequestedAsync(_cts);
        await sut.StopAsync(CancellationToken.None);

        // Should only be called once — no retries for invalid messages
        await _messageHandler.Received(1)
            .HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>());

        await _deadLetterProducer.Received()
            .ProduceAsync(TestDlqTopic, Arg.Any<Message<string, TestMessage>>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task InvalidMessage_DlqMessageContainsInvalidMessageHeader()
    {
        var sut = CreateConsumer();
        SetupSingleMessage();
        _messageHandler.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidMessageException("bad data"));

        await sut.StartAsync(_cts.Token);
        await WaitUntilCancellationRequestedAsync(_cts);
        await sut.StopAsync(CancellationToken.None);

        await _deadLetterProducer.Received().ProduceAsync(
            TestDlqTopic,
            Arg.Is<Message<string, TestMessage>>(m =>
                HasHeader(m.Headers, KafkaHeaders.InvalidMessage, "true")),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task InvalidMessage_StoresOffset()
    {
        var sut = CreateConsumer();
        var consumeResult = SetupSingleMessage();
        _messageHandler.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidMessageException("bad"));

        await sut.StartAsync(_cts.Token);
        await WaitUntilCancellationRequestedAsync(_cts);
        await sut.StopAsync(CancellationToken.None);

        _kafkaConsumer.Received(1).StoreOffset(consumeResult);
    }

    [Fact]
    public async Task InvalidMessage_DlqMessageContainsErrorMessageFromException()
    {
        var sut = CreateConsumer();
        SetupSingleMessage();
        _messageHandler.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidMessageException("invalid OrderId format"));

        await sut.StartAsync(_cts.Token);
        await WaitUntilCancellationRequestedAsync(_cts);
        await sut.StopAsync(CancellationToken.None);

        await _deadLetterProducer.Received().ProduceAsync(
            TestDlqTopic,
            Arg.Is<Message<string, TestMessage>>(m =>
                HasHeader(m.Headers, KafkaHeaders.ErrorMessage, "invalid OrderId format")),
            Arg.Any<CancellationToken>());
    }

    #endregion

    #region No dead letter topic configured

    [Fact]
    public async Task NoDlqConfigured_DoesNotPublishToDlq()
    {
        var sut = CreateConsumer(deadLetterTopic: null);
        SetupSingleMessage();
        _messageHandler.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("fail"));

        await sut.StartAsync(_cts.Token);
        await WaitUntilCancellationRequestedAsync(_cts);
        await sut.StopAsync(CancellationToken.None);

        await _deadLetterProducer.DidNotReceive()
            .ProduceAsync(Arg.Any<string>(), Arg.Any<Message<string, TestMessage>>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task NoDlqConfigured_EmptyStringDeadLetterTopic_DoesNotPublishToDlq()
    {
        var sut = CreateConsumer(deadLetterTopic: "   ");
        SetupSingleMessage();
        _messageHandler.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("fail"));

        await sut.StartAsync(_cts.Token);
        await WaitUntilCancellationRequestedAsync(_cts);
        await sut.StopAsync(CancellationToken.None);

        await _deadLetterProducer.DidNotReceive()
            .ProduceAsync(Arg.Any<string>(), Arg.Any<Message<string, TestMessage>>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task NoDlqConfigured_LogsWarningAboutMessageLoss()
    {
        var sut = CreateConsumer(deadLetterTopic: null);
        SetupSingleMessage();
        _messageHandler.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("fail"));

        await sut.StartAsync(_cts.Token);
        await WaitUntilCancellationRequestedAsync(_cts);
        await sut.StopAsync(CancellationToken.None);

        _logger.Received().Log(
            LogLevel.Warning,
            Arg.Any<EventId>(),
            Arg.Is<object>(o => o.ToString()!.Contains("No dead letter topic configured")),
            Arg.Any<Exception?>(),
            Arg.Any<Func<object, Exception?, string>>());
    }

    [Fact]
    public async Task NoDlqConfigured_StillStoresOffset()
    {
        var sut = CreateConsumer(deadLetterTopic: null);
        var consumeResult = SetupSingleMessage();
        _messageHandler.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("fail"));

        await sut.StartAsync(_cts.Token);
        await WaitUntilCancellationRequestedAsync(_cts);
        await sut.StopAsync(CancellationToken.None);

        _kafkaConsumer.Received(1).StoreOffset(consumeResult);
    }

    [Fact]
    public async Task NoDlqConfigured_InvalidMessage_DoesNotPublishToDlq()
    {
        var sut = CreateConsumer(deadLetterTopic: null);
        SetupSingleMessage();
        _messageHandler.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidMessageException("bad"));

        await sut.StartAsync(_cts.Token);
        await WaitUntilCancellationRequestedAsync(_cts);
        await sut.StopAsync(CancellationToken.None);

        await _deadLetterProducer.DidNotReceive()
            .ProduceAsync(Arg.Any<string>(), Arg.Any<Message<string, TestMessage>>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task NoDlqConfigured_InvalidMessage_StillStoresOffset()
    {
        var sut = CreateConsumer(deadLetterTopic: null);
        var consumeResult = SetupSingleMessage();
        _messageHandler.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidMessageException("bad"));

        await sut.StartAsync(_cts.Token);
        await sut.ExecuteTask!;
        await sut.StopAsync(CancellationToken.None);

        _kafkaConsumer.Received(1).StoreOffset(consumeResult);
    }

    #endregion

    #region DLQ publish failure - best effort, never crash main consumer

    [Fact]
    public async Task DlqPublishFailure_DoesNotCrashConsumer_ContinuesProcessing()
    {
        var sut = CreateConsumer();
        var msg1 = CreateConsumeResult("key-1");
        var msg2 = CreateConsumeResult("key-2");
        SetupConsumeSequence(msg1, msg2);

        var callCount = 0;
        _messageHandler.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                if (callCount++ == 0)
                    throw new InvalidOperationException("fail first");
                return Task.CompletedTask;
            });

        _deadLetterProducer.ProduceAsync(Arg.Any<string>(), Arg.Any<Message<string, TestMessage>>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new KafkaException(new Error(ErrorCode.InvalidMsg)));

        await sut.StartAsync(_cts.Token);
        await WaitUntilCancellationRequestedAsync(_cts);
        await sut.StopAsync(CancellationToken.None);

        // Second message should still be processed despite DLQ failure on first
        await _messageHandler.Received(2)
            .HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>());

        // Initial call + default retryCount of 3
        await _deadLetterProducer.Received(4)
            .ProduceAsync(Arg.Any<string>(), Arg.Any<Message<string, TestMessage>>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task DlqPublishFailure_LogsCritical()
    {
        var sut = CreateConsumer();
        SetupSingleMessage();
        _messageHandler.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("fail"));
        _deadLetterProducer.ProduceAsync(Arg.Any<string>(), Arg.Any<Message<string, TestMessage>>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new KafkaException(new Error(ErrorCode.BrokerNotAvailable)));

        await sut.StartAsync(_cts.Token);
        await WaitUntilCancellationRequestedAsync(_cts);
        await sut.StopAsync(CancellationToken.None);

        _logger.Received().Log(
            LogLevel.Critical,
            Arg.Any<EventId>(),
            Arg.Is<object>(o => o.ToString()!.Contains("Failed to publish message to dead letter topic")),
            Arg.Any<Exception?>(),
            Arg.Any<Func<object, Exception?, string>>());
    }

    [Fact]
    public async Task DlqPublishFailure_StillStoresOffset()
    {
        var sut = CreateConsumer();
        var consumeResult = SetupSingleMessage();
        _messageHandler.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("fail"));
        _deadLetterProducer.ProduceAsync(Arg.Any<string>(), Arg.Any<Message<string, TestMessage>>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new KafkaException(new Error(ErrorCode.BrokerNotAvailable)));

        await sut.StartAsync(_cts.Token);
        await WaitUntilCancellationRequestedAsync(_cts);
        await sut.StopAsync(CancellationToken.None);

        _kafkaConsumer.Received(1).StoreOffset(consumeResult);
    }

    #endregion

    #region Retry behavior

    [Fact]
    public async Task Retry_RetriesConfiguredNumberOfTimes()
    {
        var sut = CreateConsumer(maxRetries: 2);
        SetupSingleMessage();
        _messageHandler.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("transient"));

        await sut.StartAsync(_cts.Token);

        await WaitUntilCancellationRequestedAsync(_cts);

        await sut.StopAsync(CancellationToken.None);

        // 1 initial + 2 retries = 3 total calls
        await _messageHandler.Received(3)
            .HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Retry_ZeroRetries_NoRetryAttempts()
    {
        var sut = CreateConsumer(maxRetries: 0);
        SetupSingleMessage();
        _messageHandler.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("fail"));

        await sut.StartAsync(_cts.Token);
        await WaitUntilCancellationRequestedAsync(_cts);
        await sut.StopAsync(CancellationToken.None);

        // Only the initial attempt, no retries
        await _messageHandler.Received(1)
            .HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Retry_SucceedsOnSecondAttempt_DoesNotPublishToDlq()
    {
        var sut = CreateConsumer(maxRetries: 2);
        SetupSingleMessage();

        var callCount = 0;
        _messageHandler.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                if (callCount++ == 0)
                    throw new InvalidOperationException("transient");
                return Task.CompletedTask;
            });

        await sut.StartAsync(_cts.Token);
        await WaitUntilCancellationRequestedAsync(_cts);
        await sut.StopAsync(CancellationToken.None);

        await _deadLetterProducer.DidNotReceive()
            .ProduceAsync(Arg.Any<string>(), Arg.Any<Message<string, TestMessage>>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Retry_SucceedsOnSecondAttempt_StoresOffset()
    {
        var sut = CreateConsumer(maxRetries: 2);
        var consumeResult = SetupSingleMessage();

        var callCount = 0;
        _messageHandler.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                if (callCount++ == 0)
                    throw new InvalidOperationException("transient");
                return Task.CompletedTask;
            });

        await sut.StartAsync(_cts.Token);
        await WaitUntilCancellationRequestedAsync(_cts);
        await sut.StopAsync(CancellationToken.None);

        _kafkaConsumer.Received(1).StoreOffset(consumeResult);
    }

    [Fact]
    public async Task Retry_InvalidMessageException_NotRetried_EvenWithRetriesConfigured()
    {
        var sut = CreateConsumer(maxRetries: 3);
        SetupSingleMessage();
        _messageHandler.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidMessageException("permanent failure"));

        await sut.StartAsync(_cts.Token);
        await WaitUntilCancellationRequestedAsync(_cts);
        await sut.StopAsync(CancellationToken.None);

        // Only 1 call - InvalidMessageException bypasses retry
        await _messageHandler.Received(1)
            .HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Retry_AllRetriesExhausted_PublishesToDlq()
    {
        var sut = CreateConsumer(maxRetries: 1);
        SetupSingleMessage();
        _messageHandler.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("persistent failure"));

        await sut.StartAsync(_cts.Token);
        await WaitUntilCancellationRequestedAsync(_cts);
        await sut.StopAsync(CancellationToken.None);

        // 1 initial + 1 retry = 2 total
        await _messageHandler.Received(2)
            .HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>());

        await _deadLetterProducer.Received()
            .ProduceAsync(TestDlqTopic, Arg.Any<Message<string, TestMessage>>(), Arg.Any<CancellationToken>());
    }

    #endregion

    #region Continued handling after failure

    [Fact]
    public async Task ContinuesProcessing_AfterFailedMessage()
    {
        var sut = CreateConsumer();
        var msg1 = CreateConsumeResult("key-fail");
        var msg2 = CreateConsumeResult("key-success");
        SetupConsumeSequence(msg1, msg2);

        var callCount = 0;
        _messageHandler.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                if (callCount++ == 0)
                    throw new InvalidOperationException("transient");
                return Task.CompletedTask;
            });

        await sut.StartAsync(_cts.Token);
        await WaitUntilCancellationRequestedAsync(_cts);
        await sut.StopAsync(CancellationToken.None);

        // Both messages processed
        await _messageHandler.Received(2)
            .HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>());
        // Both offsets stored
        _kafkaConsumer.Received(1).StoreOffset(msg1);
        _kafkaConsumer.Received(1).StoreOffset(msg2);
    }

    [Fact]
    public async Task ContinuesProcessing_AfterInvalidMessage()
    {
        var sut = CreateConsumer();
        var msg1 = CreateConsumeResult("key-invalid-msg");
        var msg2 = CreateConsumeResult("key-good");
        SetupConsumeSequence(msg1, msg2);

        var callCount = 0;
        _messageHandler.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                if (callCount++ == 0)
                    throw new InvalidMessageException("bad");
                return Task.CompletedTask;
            });

        await sut.StartAsync(_cts.Token);
        await WaitUntilCancellationRequestedAsync(_cts);
        await sut.StopAsync(CancellationToken.None);

        await _messageHandler.Received(2)
            .HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>());
        _kafkaConsumer.Received(1).StoreOffset(msg1);
        _kafkaConsumer.Received(1).StoreOffset(msg2);
    }

    [Fact]
    public async Task ContinuesProcessing_AfterDlqPublishFailure()
    {
        var sut = CreateConsumer();
        var msg1 = CreateConsumeResult("key-dlq-fail");
        var msg2 = CreateConsumeResult("key-ok");
        SetupConsumeSequence(msg1, msg2);

        var callCount = 0;
        _messageHandler.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                if (callCount++ == 0)
                    throw new InvalidOperationException("fail");
                return Task.CompletedTask;
            });

        _deadLetterProducer.ProduceAsync(Arg.Any<string>(), Arg.Any<Message<string, TestMessage>>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new KafkaException(new Error(ErrorCode.BrokerNotAvailable)));

        await sut.StartAsync(_cts.Token);
        await WaitUntilCancellationRequestedAsync(_cts);
        await sut.StopAsync(CancellationToken.None);

        // Second message still processed
        await _messageHandler.Received(2)
            .HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>());
        _kafkaConsumer.Received(1).StoreOffset(msg1);
        _kafkaConsumer.Received(1).StoreOffset(msg2);
    }

    #endregion

    #region Poison messages - deserialization failures skip and store past

    [Fact]
    public async Task ConsumeException_PoisonMessage_StoresPastFailedOffset_AndContinues()
    {
        var sut = CreateConsumer();
        var validResult = CreateConsumeResult();
        var callIndex = 0;
        _kafkaConsumer.Consume(Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                var call = callIndex++;
                if (call == 0)
                    throw CreatePoisonException(offset: 5);
                if (call == 1)
                    return validResult;
                _cts.Cancel();
                throw new OperationCanceledException(_cts.Token);
            });

        await sut.StartAsync(_cts.Token);
        await WaitUntilCancellationRequestedAsync(_cts);
        await sut.StopAsync(CancellationToken.None);

        // Stored past the poison record (failed offset + 1), then processed the next message
        _kafkaConsumer.Received(1).StoreOffset(Arg.Is<TopicPartitionOffset>(t =>
            t.Topic == TestTopic && t.Partition.Value == 0 && t.Offset.Value == 6));
        _kafkaConsumer.Received(1).StoreOffset(validResult);
        await _messageHandler.Received(1)
            .HandleMessageAsync(validResult.Message.Value, Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task ConsumeException_PoisonMessage_CapturesRawBytesToDlq()
    {
        var sut = CreateConsumer();
        var callIndex = 0;
        _kafkaConsumer.Consume(Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                if (callIndex++ == 0)
                    throw CreatePoisonException();
                _cts.Cancel();
                throw new OperationCanceledException(_cts.Token);
            });

        await sut.StartAsync(_cts.Token);
        await WaitUntilCancellationRequestedAsync(_cts);
        await sut.StopAsync(CancellationToken.None);

        // The raw key and value are preserved verbatim, marked with the deserialization-failed header
        await _rawDeadLetterProducer.Received(1).ProduceAsync(
            TestDlqTopic,
            Arg.Is<Message<byte[], byte[]>>(m =>
                System.Text.Encoding.UTF8.GetString(m.Key) == "poison-key" &&
                System.Text.Encoding.UTF8.GetString(m.Value) == "not-valid-payload" &&
                HasHeader(m.Headers, KafkaHeaders.DeserializationFailed, "true") &&
                HasHeader(m.Headers, KafkaHeaders.OriginalTopic, TestTopic) &&
                HasHeader(m.Headers, KafkaHeaders.ErrorMessage, null)),
            Arg.Any<CancellationToken>());

        // The typed producer is never involved — the payload cannot be represented as TMessage
        await _deadLetterProducer.DidNotReceive()
            .ProduceAsync(Arg.Any<string>(), Arg.Any<Message<string, TestMessage>>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task ConsumeException_PoisonMessage_WithDlq_LogsErrorCaptured()
    {
        var sut = CreateConsumer();
        var callIndex = 0;
        _kafkaConsumer.Consume(Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                if (callIndex++ == 0)
                    throw CreatePoisonException();
                _cts.Cancel();
                throw new OperationCanceledException(_cts.Token);
            });

        await sut.StartAsync(_cts.Token);
        await WaitUntilCancellationRequestedAsync(_cts);
        await sut.StopAsync(CancellationToken.None);

        _logger.Received().Log(
            LogLevel.Error,
            Arg.Any<EventId>(),
            Arg.Is<object>(o => o.ToString()!.Contains("captured to dead letter topic")),
            Arg.Any<Exception?>(),
            Arg.Any<Func<object, Exception?, string>>());
    }

    [Fact]
    public async Task ConsumeException_PoisonMessage_NoDlqConfigured_LogsCritical_AndDoesNotCapture()
    {
        var sut = CreateConsumer(deadLetterTopic: null);
        var callIndex = 0;
        _kafkaConsumer.Consume(Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                if (callIndex++ == 0)
                    throw CreatePoisonException(offset: 5);
                _cts.Cancel();
                throw new OperationCanceledException(_cts.Token);
            });

        await sut.StartAsync(_cts.Token);
        await WaitUntilCancellationRequestedAsync(_cts);
        await sut.StopAsync(CancellationToken.None);

        await _rawDeadLetterProducer.DidNotReceive()
            .ProduceAsync(Arg.Any<string>(), Arg.Any<Message<byte[], byte[]>>(), Arg.Any<CancellationToken>());
        _logger.Received().Log(
            LogLevel.Critical,
            Arg.Any<EventId>(),
            Arg.Is<object>(o => o.ToString()!.Contains("will be lost")),
            Arg.Any<Exception?>(),
            Arg.Any<Func<object, Exception?, string>>());
        // Still stored past so the consumer survives
        _kafkaConsumer.Received(1).StoreOffset(Arg.Is<TopicPartitionOffset>(t => t.Offset.Value == 6));
    }

    [Fact]
    public async Task ConsumeException_PoisonMessage_CaptureFails_LogsCritical_StillStoresPastOffset()
    {
        var sut = CreateConsumer();
        var callIndex = 0;
        _kafkaConsumer.Consume(Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                if (callIndex++ == 0)
                    throw CreatePoisonException(offset: 5);
                _cts.Cancel();
                throw new OperationCanceledException(_cts.Token);
            });
        _rawDeadLetterProducer.ProduceAsync(Arg.Any<string>(), Arg.Any<Message<byte[], byte[]>>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new KafkaException(new Error(ErrorCode.BrokerNotAvailable)));

        await sut.StartAsync(_cts.Token);
        await WaitUntilCancellationRequestedAsync(_cts);
        await sut.StopAsync(CancellationToken.None);

        // Capture is best-effort: the failure is logged as lost, but the consumer never wedges
        _logger.Received().Log(
            LogLevel.Critical,
            Arg.Any<EventId>(),
            Arg.Is<object>(o => o.ToString()!.Contains("will be lost")),
            Arg.Any<Exception?>(),
            Arg.Any<Func<object, Exception?, string>>());
        _kafkaConsumer.Received(1).StoreOffset(Arg.Is<TopicPartitionOffset>(t => t.Offset.Value == 6));
    }

    [Fact]
    public async Task ConsumeException_UnsetOffset_ContinuesWithoutStoring()
    {
        var sut = CreateConsumer();
        var validResult = CreateConsumeResult();
        var callIndex = 0;
        _kafkaConsumer.Consume(Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                var call = callIndex++;
                if (call == 0)
                    throw new ConsumeException(
                        new ConsumeResult<byte[], byte[]>
                        {
                            Topic = TestTopic,
                            Partition = new Partition(0),
                            Offset = Offset.Unset,
                            Message = new Message<byte[], byte[]>()
                        },
                        new Error(ErrorCode.UnknownTopicOrPart));
                if (call == 1)
                    return validResult;
                _cts.Cancel();
                throw new OperationCanceledException(_cts.Token);
            });

        await sut.StartAsync(_cts.Token);
        await WaitUntilCancellationRequestedAsync(_cts);
        await sut.StopAsync(CancellationToken.None);

        // No offset to skip past — nothing stored for the error, only the valid message's offset is stored
        _kafkaConsumer.DidNotReceive().StoreOffset(Arg.Any<TopicPartitionOffset>());
        _kafkaConsumer.Received(1).StoreOffset(validResult);
    }

    [Fact]
    public async Task ConsumeException_FatalError_RethrowsAndClosesConsumer()
    {
        var sut = CreateConsumer();
        _kafkaConsumer.Consume(Arg.Any<CancellationToken>())
            .Returns(_ => throw new ConsumeException(
                new ConsumeResult<byte[], byte[]>
                {
                    Topic = TestTopic,
                    Partition = new Partition(0),
                    Offset = new Offset(5),
                    Message = new Message<byte[], byte[]>()
                },
                new Error(ErrorCode.Local_Fatal, "fatal client error", true)));

        await Assert.ThrowsAsync<ConsumeException>(async () =>
        {
            await sut.StartAsync(_cts.Token);
            await sut.ExecuteTask!;
        });

        _kafkaConsumer.Received(1).Close();
        _kafkaConsumer.DidNotReceive().StoreOffset(Arg.Any<ConsumeResult<string, TestMessage>>());
        _kafkaConsumer.DidNotReceive().StoreOffset(Arg.Any<TopicPartitionOffset>());
    }

    #endregion

    #region Terminal failure sink

    [Fact]
    public async Task NoDlqConfigured_HandlerFailure_NotifiesTerminalSink()
    {
        var sut = CreateConsumer(deadLetterTopic: null);
        var consumeResult = SetupSingleMessage();
        _messageHandler.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("downstream exploded"));

        await sut.StartAsync(_cts.Token);
        await WaitUntilCancellationRequestedAsync(_cts);
        await sut.StopAsync(CancellationToken.None);

        await _terminalSink.Received(1).HandleAsync(
            Arg.Is<TerminalFailure<TestMessage>>(f =>
                f.Reason == TerminalFailureReason.NoDeadLetterTopicConfigured &&
                f.Message == consumeResult.Message.Value &&
                Equals(f.MessageKey, TestMessageKey) &&
                f.SourceTopic == TestTopic &&
                f.Error == "downstream exploded"),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task DlqPublishFailure_NotifiesTerminalSink_WithOriginalError()
    {
        var sut = CreateConsumer();
        var consumeResult = SetupSingleMessage();
        _messageHandler.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("processing failed"));
        _deadLetterProducer.ProduceAsync(Arg.Any<string>(), Arg.Any<Message<string, TestMessage>>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new KafkaException(new Error(ErrorCode.BrokerNotAvailable)));

        await sut.StartAsync(_cts.Token);
        await WaitUntilCancellationRequestedAsync(_cts);
        await sut.StopAsync(CancellationToken.None);

        // The sink receives the processing error (why the message failed), not the publish error
        await _terminalSink.Received(1).HandleAsync(
            Arg.Is<TerminalFailure<TestMessage>>(f =>
                f.Reason == TerminalFailureReason.DeadLetterPublishFailed &&
                f.Message == consumeResult.Message.Value &&
                f.Error == "processing failed"),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task HandlerFailure_DlqPublishSucceeds_DoesNotNotifySink()
    {
        var sut = CreateConsumer();
        SetupSingleMessage();
        _messageHandler.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("fail"));

        await sut.StartAsync(_cts.Token);
        await WaitUntilCancellationRequestedAsync(_cts);
        await sut.StopAsync(CancellationToken.None);

        // The message reached the DLQ — it is not terminal yet (the DLQ consumer owns it now)
        await _terminalSink.DidNotReceive()
            .HandleAsync(Arg.Any<TerminalFailure<TestMessage>>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task TerminalSinkThrows_StillStoresOffset_AndLogsError()
    {
        var sut = CreateConsumer(deadLetterTopic: null);
        var consumeResult = SetupSingleMessage();
        _messageHandler.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("fail"));
        _terminalSink.HandleAsync(Arg.Any<TerminalFailure<TestMessage>>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("sink db down"));

        await sut.StartAsync(_cts.Token);
        await WaitUntilCancellationRequestedAsync(_cts);
        await sut.StopAsync(CancellationToken.None);

        _kafkaConsumer.Received(1).StoreOffset(consumeResult);
        _logger.Received().Log(
            LogLevel.Error,
            Arg.Any<EventId>(),
            Arg.Is<object>(o => o.ToString()!.Contains("Terminal failure sink threw")),
            Arg.Any<Exception?>(),
            Arg.Any<Func<object, Exception?, string>>());
    }

    [Fact]
    public async Task TerminalSinkNotRegistered_NoDlq_StillStoresOffset()
    {
        var sut = CreateConsumer(deadLetterTopic: null, registerTerminalSink: false);
        var consumeResult = SetupSingleMessage();
        _messageHandler.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("fail"));

        await sut.StartAsync(_cts.Token);
        await WaitUntilCancellationRequestedAsync(_cts);
        await sut.StopAsync(CancellationToken.None);

        _kafkaConsumer.Received(1).StoreOffset(consumeResult);
    }

    #endregion

    #region Cancellation during handling - no DLQ, no offset stored

    [Fact]
    public async Task CancellationDuringProcessing_DoesNotPublishToDlq()
    {
        var sut = CreateConsumer();
        var consumeResult = CreateConsumeResult();
        _kafkaConsumer.Consume(Arg.Any<CancellationToken>())
            .Returns(consumeResult);

        _messageHandler.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                _cts.Cancel();
                throw new OperationCanceledException(_cts.Token);
            });

        await sut.StartAsync(_cts.Token);
        await WaitUntilCancellationRequestedAsync(_cts);
        await sut.StopAsync(CancellationToken.None);

        await _deadLetterProducer.DidNotReceive()
            .ProduceAsync(Arg.Any<string>(), Arg.Any<Message<string, TestMessage>>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task CancellationDuringProcessing_DoesNotStoreOffset()
    {
        var sut = CreateConsumer();
        var consumeResult = CreateConsumeResult();
        _kafkaConsumer.Consume(Arg.Any<CancellationToken>())
            .Returns(consumeResult);

        _messageHandler.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                _cts.Cancel();
                throw new OperationCanceledException(_cts.Token);
            });

        await sut.StartAsync(_cts.Token);
        await WaitUntilCancellationRequestedAsync(_cts);
        await sut.StopAsync(CancellationToken.None);

        _kafkaConsumer.DidNotReceive().StoreOffset(Arg.Any<ConsumeResult<string, TestMessage>>());
    }

    [Fact]
    public async Task CancellationDuringProcessing_WithRetriesConfigured_DoesNotPublishToDlq()
    {
        var sut = CreateConsumer(maxRetries: 3);
        var consumeResult = CreateConsumeResult();
        _kafkaConsumer.Consume(Arg.Any<CancellationToken>())
            .Returns(consumeResult);

        _messageHandler.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                _cts.Cancel();
                throw new OperationCanceledException(_cts.Token);
            });

        await sut.StartAsync(_cts.Token);
        await WaitUntilCancellationRequestedAsync(_cts);
        await sut.StopAsync(CancellationToken.None);

        await _deadLetterProducer.DidNotReceive()
            .ProduceAsync(Arg.Any<string>(), Arg.Any<Message<string, TestMessage>>(), Arg.Any<CancellationToken>());
        _kafkaConsumer.DidNotReceive().StoreOffset(Arg.Any<ConsumeResult<string, TestMessage>>());
    }

    [Fact]
    public async Task CancellationDuringProcessing_LogsShutdownWarning()
    {
        var sut = CreateConsumer();
        var consumeResult = CreateConsumeResult();
        _kafkaConsumer.Consume(Arg.Any<CancellationToken>())
            .Returns(consumeResult);

        _messageHandler.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                _cts.Cancel();
                throw new OperationCanceledException(_cts.Token);
            });

        await sut.StartAsync(_cts.Token);
        await WaitUntilCancellationRequestedAsync(_cts);
        await sut.StopAsync(CancellationToken.None);

        _logger.Received().Log(
            LogLevel.Warning,
            Arg.Any<EventId>(),
            Arg.Is<object>(o => o.ToString()!.Contains("shutting down")),
            Arg.Any<Exception?>(),
            Arg.Any<Func<object, Exception?, string>>());
    }

    [Fact]
    public async Task CancellationDuringProcessing_ClosesConsumer()
    {
        var sut = CreateConsumer();
        var consumeResult = CreateConsumeResult();
        _kafkaConsumer.Consume(Arg.Any<CancellationToken>())
            .Returns(consumeResult);

        _messageHandler.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                _cts.Cancel();
                throw new OperationCanceledException(_cts.Token);
            });

        await sut.StartAsync(_cts.Token);
        await WaitUntilCancellationRequestedAsync(_cts);
        await sut.StopAsync(CancellationToken.None);

        _kafkaConsumer.Received(1).Close();
    }

    #endregion
}
