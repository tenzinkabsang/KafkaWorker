using System.Text.Json;
using Confluent.Kafka;

namespace KafkaWorker;

internal class JsonStringDeserializer<T> : IDeserializer<T>
{
    public T Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
    {
        if (isNull)
            return default!;

        return JsonSerializer.Deserialize<T>(data)
            ?? throw new InvalidOperationException($"Deserialization of {typeof(T).Name} returned null.");
    }
}