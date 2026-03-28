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
    private readonly IMessageHandler<TestMessage> _messageProcessor;
    private readonly ILogger<Consumer<string, TestMessage>> _logger;
    private readonly KafkaWorkerMetrics _metrics;
    private readonly CancellationTokenSource _cts;

    public ConsumerTests()
    {
        _kafkaConsumer = Substitute.For<IConsumer<string, TestMessage>>();
        _deadLetterProducer = Substitute.For<IProducer<string, TestMessage>>();
        _messageProcessor = Substitute.For<IMessageHandler<TestMessage>>();
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
        int maxRetries = 0)
    {
        var config = new KafkaWorkerConfig
        {
            GroupId = "test-group",
            Topic = topic,
            DeadLetterTopic = deadLetterTopic,
            MaxRetries = maxRetries
        };

        var optionsMonitor = Substitute.For<IOptionsMonitor<KafkaWorkerConfig>>();
        optionsMonitor.Get(nameof(TestMessage)).Returns(config);

        var serviceProvider = Substitute.For<IServiceProvider>();
        serviceProvider.GetService(typeof(IMessageHandler<TestMessage>)).Returns(_messageProcessor);
        var scope = Substitute.For<IServiceScope>();
        scope.ServiceProvider.Returns(serviceProvider);
        var scopeFactory = Substitute.For<IServiceScopeFactory>();
        scopeFactory.CreateScope().Returns(scope);

        return new Consumer<string, TestMessage>(
            _kafkaConsumer,
            _deadLetterProducer,
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

    #region Happy path - message processing and offset commit

    [Fact]
    public async Task ExecuteAsync_ProcessesMessageAndCommitsOffset()
    {
        var sut = CreateConsumer();
        var consumeResult = SetupSingleMessage();

        await sut.StartAsync(_cts.Token);
        await WaitUntilCancellationRequestedAsync(_cts);
        await sut.StopAsync(CancellationToken.None);

        await _messageProcessor.Received(1)
            .HandleMessageAsync(consumeResult.Message.Value, Arg.Any<CancellationToken>());
        _kafkaConsumer.Received(1).StoreOffset(consumeResult);
        _kafkaConsumer.Received(1).Commit();
    }

    [Fact]
    public async Task ExecuteAsync_ProcessesMultipleMessagesInOrder()
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
            _messageProcessor.HandleMessageAsync(msg1.Message.Value, Arg.Any<CancellationToken>());
            _kafkaConsumer.StoreOffset(msg1);
            _kafkaConsumer.Commit();
            _messageProcessor.HandleMessageAsync(msg2.Message.Value, Arg.Any<CancellationToken>());
            _kafkaConsumer.StoreOffset(msg2);
            _kafkaConsumer.Commit();
        });
    }

    [Fact]
    public async Task ExecuteAsync_CommitsOffsetPerMessage_NotInBatch()
    {
        var sut = CreateConsumer();
        var msg1 = CreateConsumeResult("key-1");
        var msg2 = CreateConsumeResult("key-2");
        SetupConsumeSequence(msg1, msg2);

        await sut.StartAsync(_cts.Token);
        await WaitUntilCancellationRequestedAsync(_cts);
        await sut.StopAsync(CancellationToken.None);

        _kafkaConsumer.Received(2).Commit();
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

        await _messageProcessor.DidNotReceive()
            .HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>());
        _kafkaConsumer.DidNotReceive().Commit();
    }

    [Fact]
    public async Task ExecuteAsync_SkipsMessageWithNullValue()
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

        await _messageProcessor.DidNotReceive()
            .HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>());
        _kafkaConsumer.DidNotReceive().Commit();
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

        await _messageProcessor.DidNotReceive()
            .HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>());
        _kafkaConsumer.DidNotReceive().Commit();
    }

    [Fact]
    public async Task ExecuteAsync_ProcessesValidMessageAfterSkippingNullAndEof()
    {
        var sut = CreateConsumer();
        var nullResult = CreateNullMessageResult();
        var eofResult = CreatePartitionEofResult();
        var validResult = CreateConsumeResult();
        SetupConsumeSequence(nullResult, eofResult, validResult);

        await sut.StartAsync(_cts.Token);
        await WaitUntilCancellationRequestedAsync(_cts);
        await sut.StopAsync(CancellationToken.None);

        await _messageProcessor.Received(1)
            .HandleMessageAsync(validResult.Message.Value, Arg.Any<CancellationToken>());
        _kafkaConsumer.Received(1).StoreOffset(validResult);
        _kafkaConsumer.Received(1).Commit();
    }

    #endregion

    #region Processing failure - DLQ publish

    [Fact]
    public async Task ProcessingFailure_PublishesToDeadLetterTopic()
    {
        var sut = CreateConsumer();
        SetupSingleMessage();
        _messageProcessor.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("processing failed"));

        await sut.StartAsync(_cts.Token);
        await WaitUntilCancellationRequestedAsync(_cts);
        await sut.StopAsync(CancellationToken.None);

        await _deadLetterProducer.Received()
            .ProduceAsync(TestDlqTopic, Arg.Any<Message<string, TestMessage>>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task ProcessingFailure_DlqMessageContainsOriginalKeyAndValue()
    {
        var sut = CreateConsumer();
        var originalValue = new TestMessage { Data = "important-data" };
        SetupSingleMessage(key: "my-key", value: originalValue);
        _messageProcessor.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
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
    public async Task ProcessingFailure_DlqMessageContainsOriginalTopicHeader()
    {
        var sut = CreateConsumer();
        SetupSingleMessage();
        _messageProcessor.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
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
    public async Task ProcessingFailure_DlqMessageContainsErrorMessageHeader()
    {
        var sut = CreateConsumer();
        SetupSingleMessage();
        _messageProcessor.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
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
    public async Task ProcessingFailure_DlqMessageCopiesOriginalHeaders()
    {
        var sut = CreateConsumer();
        var originalHeaders = new Headers
        {
            { "correlation-id", System.Text.Encoding.UTF8.GetBytes("abc-123") }
        };
        SetupSingleMessage(headers: originalHeaders);
        _messageProcessor.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
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
    public async Task ProcessingFailure_CommitsOffsetAfterDlqPublish()
    {
        var sut = CreateConsumer();
        var consumeResult = SetupSingleMessage();
        _messageProcessor.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("fail"));

        await sut.StartAsync(_cts.Token);
        await WaitUntilCancellationRequestedAsync(_cts);
        await sut.StopAsync(CancellationToken.None);

        _kafkaConsumer.Received(1).StoreOffset(consumeResult);
        _kafkaConsumer.Received(1).Commit();
    }

    [Fact]
    public async Task ProcessingFailure_NormalException_NoInvalidMessageHeader()
    {
        var sut = CreateConsumer();
        SetupSingleMessage();
        _messageProcessor.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
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
    public async Task ProcessingFailure_NullOriginalHeaders_DoesNotThrow()
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
        _messageProcessor.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("fail"));

        await sut.StartAsync(_cts.Token);
        await sut.ExecuteTask!;
        await sut.StopAsync(CancellationToken.None);

        // Should still publish to DLQ without throwing NullReferenceException
        await _deadLetterProducer.Received()
            .ProduceAsync(TestDlqTopic, Arg.Any<Message<string, TestMessage>>(), Arg.Any<CancellationToken>());
        _kafkaConsumer.Received(1).Commit();
    }

    #endregion

    #region Invalid message handling

    [Fact]
    public async Task InvalidMessage_BypassesRetryAndPublishesToDlq()
    {
        var sut = CreateConsumer(maxRetries: 3);
        SetupSingleMessage();
        _messageProcessor.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidMessageException("bad schema"));

        await sut.StartAsync(_cts.Token);
        await WaitUntilCancellationRequestedAsync(_cts);
        await sut.StopAsync(CancellationToken.None);

        // Should only be called once — no retries for invalid messages
        await _messageProcessor.Received(1)
            .HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>());

        await _deadLetterProducer.Received()
            .ProduceAsync(TestDlqTopic, Arg.Any<Message<string, TestMessage>>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task InvalidMessage_DlqMessageContainsInvalidMessageHeader()
    {
        var sut = CreateConsumer();
        SetupSingleMessage();
        _messageProcessor.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
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
    public async Task InvalidMessage_CommitsOffset()
    {
        var sut = CreateConsumer();
        var consumeResult = SetupSingleMessage();
        _messageProcessor.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidMessageException("bad"));

        await sut.StartAsync(_cts.Token);
        await WaitUntilCancellationRequestedAsync(_cts);
        await sut.StopAsync(CancellationToken.None);

        _kafkaConsumer.Received(1).StoreOffset(consumeResult);
        _kafkaConsumer.Received(1).Commit();
    }

    [Fact]
    public async Task InvalidMessage_DlqMessageContainsErrorMessageFromException()
    {
        var sut = CreateConsumer();
        SetupSingleMessage();
        _messageProcessor.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
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
        _messageProcessor.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
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
        _messageProcessor.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
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
        _messageProcessor.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
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
    public async Task NoDlqConfigured_StillCommitsOffset()
    {
        var sut = CreateConsumer(deadLetterTopic: null);
        var consumeResult = SetupSingleMessage();
        _messageProcessor.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("fail"));

        await sut.StartAsync(_cts.Token);
        await WaitUntilCancellationRequestedAsync(_cts);
        await sut.StopAsync(CancellationToken.None);

        _kafkaConsumer.Received(1).StoreOffset(consumeResult);
        _kafkaConsumer.Received(1).Commit();
    }

    [Fact]
    public async Task NoDlqConfigured_InvalidMessage_DoesNotPublishToDlq()
    {
        var sut = CreateConsumer(deadLetterTopic: null);
        SetupSingleMessage();
        _messageProcessor.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidMessageException("bad"));

        await sut.StartAsync(_cts.Token);
        await WaitUntilCancellationRequestedAsync(_cts);
        await sut.StopAsync(CancellationToken.None);

        await _deadLetterProducer.DidNotReceive()
            .ProduceAsync(Arg.Any<string>(), Arg.Any<Message<string, TestMessage>>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task NoDlqConfigured_InvalidMessage_StillCommitsOffset()
    {
        var sut = CreateConsumer(deadLetterTopic: null);
        var consumeResult = SetupSingleMessage();
        _messageProcessor.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidMessageException("bad"));

        await sut.StartAsync(_cts.Token);
        await sut.ExecuteTask!;
        await sut.StopAsync(CancellationToken.None);

        _kafkaConsumer.Received(1).StoreOffset(consumeResult);
        _kafkaConsumer.Received(1).Commit();
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
        _messageProcessor.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
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
        await _messageProcessor.Received(2)
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
        _messageProcessor.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
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
    public async Task DlqPublishFailure_StillCommitsOffset()
    {
        var sut = CreateConsumer();
        var consumeResult = SetupSingleMessage();
        _messageProcessor.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("fail"));
        _deadLetterProducer.ProduceAsync(Arg.Any<string>(), Arg.Any<Message<string, TestMessage>>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new KafkaException(new Error(ErrorCode.BrokerNotAvailable)));

        await sut.StartAsync(_cts.Token);
        await WaitUntilCancellationRequestedAsync(_cts);
        await sut.StopAsync(CancellationToken.None);

        _kafkaConsumer.Received(1).StoreOffset(consumeResult);
        _kafkaConsumer.Received(1).Commit();
    }

    #endregion

    #region Retry behavior

    [Fact]
    public async Task Retry_RetriesConfiguredNumberOfTimes()
    {
        var sut = CreateConsumer(maxRetries: 2);
        SetupSingleMessage();
        _messageProcessor.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("transient"));

        await sut.StartAsync(_cts.Token);
        
        await WaitUntilCancellationRequestedAsync(_cts);

        await sut.StopAsync(CancellationToken.None);

        // 1 initial + 2 retries = 3 total calls
        await _messageProcessor.Received(3)
            .HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Retry_ZeroRetries_NoRetryAttempts()
    {
        var sut = CreateConsumer(maxRetries: 0);
        SetupSingleMessage();
        _messageProcessor.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("fail"));

        await sut.StartAsync(_cts.Token);
        await WaitUntilCancellationRequestedAsync(_cts);
        await sut.StopAsync(CancellationToken.None);

        // Only the initial attempt, no retries
        await _messageProcessor.Received(1)
            .HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Retry_SucceedsOnSecondAttempt_DoesNotPublishToDlq()
    {
        var sut = CreateConsumer(maxRetries: 2);
        SetupSingleMessage();

        var callCount = 0;
        _messageProcessor.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
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
    public async Task Retry_SucceedsOnSecondAttempt_CommitsOffset()
    {
        var sut = CreateConsumer(maxRetries: 2);
        var consumeResult = SetupSingleMessage();

        var callCount = 0;
        _messageProcessor.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
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
        _kafkaConsumer.Received(1).Commit();
    }

    [Fact]
    public async Task Retry_InvalidMessageException_NotRetried_EvenWithRetriesConfigured()
    {
        var sut = CreateConsumer(maxRetries: 3);
        SetupSingleMessage();
        _messageProcessor.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidMessageException("permanent failure"));

        await sut.StartAsync(_cts.Token);
        await WaitUntilCancellationRequestedAsync(_cts);
        await sut.StopAsync(CancellationToken.None);

        // Only 1 call - InvalidMessageException bypasses retry
        await _messageProcessor.Received(1)
            .HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Retry_AllRetriesExhausted_PublishesToDlq()
    {
        var sut = CreateConsumer(maxRetries: 1);
        SetupSingleMessage();
        _messageProcessor.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("persistent failure"));

        await sut.StartAsync(_cts.Token);
        await WaitUntilCancellationRequestedAsync(_cts);
        await sut.StopAsync(CancellationToken.None);

        // 1 initial + 1 retry = 2 total
        await _messageProcessor.Received(2)
            .HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>());

        await _deadLetterProducer.Received()
            .ProduceAsync(TestDlqTopic, Arg.Any<Message<string, TestMessage>>(), Arg.Any<CancellationToken>());
    }

    #endregion

    #region Continued processing after failure

    [Fact]
    public async Task ContinuesProcessing_AfterFailedMessage()
    {
        var sut = CreateConsumer();
        var msg1 = CreateConsumeResult("key-fail");
        var msg2 = CreateConsumeResult("key-success");
        SetupConsumeSequence(msg1, msg2);

        var callCount = 0;
        _messageProcessor.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
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
        await _messageProcessor.Received(2)
            .HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>());
        // Both offsets committed
        _kafkaConsumer.Received(2).Commit();
    }

    [Fact]
    public async Task ContinuesProcessing_AfterInvalidMessage()
    {
        var sut = CreateConsumer();
        var msg1 = CreateConsumeResult("key-invalid-msg");
        var msg2 = CreateConsumeResult("key-good");
        SetupConsumeSequence(msg1, msg2);

        var callCount = 0;
        _messageProcessor.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                if (callCount++ == 0)
                    throw new InvalidMessageException("bad");
                return Task.CompletedTask;
            });

        await sut.StartAsync(_cts.Token);
        await WaitUntilCancellationRequestedAsync(_cts);
        await sut.StopAsync(CancellationToken.None);

        await _messageProcessor.Received(2)
            .HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>());
        _kafkaConsumer.Received(2).Commit();
    }

    [Fact]
    public async Task ContinuesProcessing_AfterDlqPublishFailure()
    {
        var sut = CreateConsumer();
        var msg1 = CreateConsumeResult("key-dlq-fail");
        var msg2 = CreateConsumeResult("key-ok");
        SetupConsumeSequence(msg1, msg2);

        var callCount = 0;
        _messageProcessor.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
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
        await _messageProcessor.Received(2)
            .HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>());
        _kafkaConsumer.Received(2).Commit();
    }

    #endregion

    #region Cancellation during processing - no DLQ, no commit

    [Fact]
    public async Task CancellationDuringProcessing_DoesNotPublishToDlq()
    {
        var sut = CreateConsumer();
        var consumeResult = CreateConsumeResult();
        _kafkaConsumer.Consume(Arg.Any<CancellationToken>())
            .Returns(consumeResult);

        _messageProcessor.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
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
    public async Task CancellationDuringProcessing_DoesNotCommitOffset()
    {
        var sut = CreateConsumer();
        var consumeResult = CreateConsumeResult();
        _kafkaConsumer.Consume(Arg.Any<CancellationToken>())
            .Returns(consumeResult);

        _messageProcessor.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                _cts.Cancel();
                throw new OperationCanceledException(_cts.Token);
            });

        await sut.StartAsync(_cts.Token);
        await WaitUntilCancellationRequestedAsync(_cts);
        await sut.StopAsync(CancellationToken.None);

        _kafkaConsumer.DidNotReceive().StoreOffset(Arg.Any<ConsumeResult<string, TestMessage>>());
        _kafkaConsumer.DidNotReceive().Commit();
    }

    [Fact]
    public async Task CancellationDuringProcessing_WithRetriesConfigured_DoesNotPublishToDlq()
    {
        var sut = CreateConsumer(maxRetries: 3);
        var consumeResult = CreateConsumeResult();
        _kafkaConsumer.Consume(Arg.Any<CancellationToken>())
            .Returns(consumeResult);

        _messageProcessor.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
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
        _kafkaConsumer.DidNotReceive().Commit();
    }

    [Fact]
    public async Task CancellationDuringProcessing_LogsShutdownWarning()
    {
        var sut = CreateConsumer();
        var consumeResult = CreateConsumeResult();
        _kafkaConsumer.Consume(Arg.Any<CancellationToken>())
            .Returns(consumeResult);

        _messageProcessor.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
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

        _messageProcessor.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
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
