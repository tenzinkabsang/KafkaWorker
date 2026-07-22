using System.Text;
using Confluent.Kafka;

namespace KafkaWorker.Tests;

public class KafkaHeaderExtensionsTests
{
    [Fact]
    public void GetValue_ExistingHeader_ReturnsDecodedValue()
    {
        var headers = new Headers { { "my-key", Encoding.UTF8.GetBytes("my-value") } };

        Assert.Equal("my-value", headers.GetValue("my-key"));
    }

    [Fact]
    public void GetValue_MissingHeader_ReturnsNull()
    {
        var headers = new Headers { { "other-key", Encoding.UTF8.GetBytes("value") } };

        Assert.Null(headers.GetValue("my-key"));
    }

    [Fact]
    public void GetValue_NullHeaders_ReturnsNull()
    {
        Headers? headers = null;

        Assert.Null(headers.GetValue("my-key"));
    }

    [Fact]
    public void GetValue_NullValuedHeader_ReturnsNull()
    {
        // A producer may legally write a header with a null value; this must not throw.
        var headers = new Headers { { "my-key", null } };

        Assert.Null(headers.GetValue("my-key"));
    }

    [Fact]
    public void IsInvalidMessage_NullValuedHeader_ReturnsFalse()
    {
        var headers = new Headers { { KafkaHeaders.InvalidMessage, null } };

        Assert.False(headers.IsInvalidMessage());
    }

    [Fact]
    public void GetBatchId_NullValuedHeader_ReturnsEmpty()
    {
        var headers = new Headers { { KafkaHeaders.BatchId, null } };

        Assert.Equal(string.Empty, headers.GetBatchId());
    }

    [Fact]
    public void GetReprocessAttemptCount_NullValuedHeader_ReturnsZero()
    {
        var headers = new Headers { { KafkaHeaders.ReprocessedAttempt, null } };

        Assert.Equal(0, headers.GetReprocessAttemptCount());
    }
}
