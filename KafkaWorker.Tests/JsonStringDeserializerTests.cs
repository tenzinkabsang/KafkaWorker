using System.Text;
using System.Text.Json;
using Confluent.Kafka;

namespace KafkaWorker.Tests;

public class JsonStringDeserializerTests
{
    private readonly JsonStringDeserializer<TestDto> _sut = new();

    public class TestDto
    {
        public string Name { get; set; } = string.Empty;
        public int Value { get; set; }
    }

    [Fact]
    public void Deserialize_ValidJson_ReturnsDeserializedObject()
    {
        var json = JsonSerializer.SerializeToUtf8Bytes(new TestDto { Name = "test", Value = 42 });

        var result = _sut.Deserialize(json, false, SerializationContext.Empty);

        Assert.Equal("test", result.Name);
        Assert.Equal(42, result.Value);
    }

    [Fact]
    public void Deserialize_IsNull_ReturnsDefault()
    {
        var result = _sut.Deserialize(ReadOnlySpan<byte>.Empty, isNull: true, SerializationContext.Empty);

        Assert.Null(result);
    }

    [Fact]
    public void Deserialize_JsonNullLiteral_ThrowsInvalidOperationException()
    {
        var nullJson = Encoding.UTF8.GetBytes("null");

        Assert.Throws<InvalidOperationException>(
            () => _sut.Deserialize(nullJson, false, SerializationContext.Empty));
    }

    [Fact]
    public void Deserialize_EmptyObject_ReturnsObjectWithDefaults()
    {
        var json = Encoding.UTF8.GetBytes("{}");

        var result = _sut.Deserialize(json, false, SerializationContext.Empty);

        Assert.NotNull(result);
        Assert.Equal(string.Empty, result.Name);
        Assert.Equal(0, result.Value);
    }
}
