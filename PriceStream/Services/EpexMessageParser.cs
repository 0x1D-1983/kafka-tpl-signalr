using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
using System.Text.Json.Serialization;
using Confluent.Kafka;
using PriceStream.Models;

namespace PriceStream.Services;

/// <summary>
/// Parses EPEX JSON straight from UTF-8 bytes. Symbols come out trimmed and upper-case, and a
/// message without a symbol is rejected, so nothing downstream has to normalize or guard again.
/// </summary>
public static class EpexMessageParser
{
    public static bool TryParse(ReadOnlySpan<byte> utf8, [NotNullWhen(true)] out EpexMarketData? message)
    {
        message = null;
        if (utf8.IsEmpty)
            return false;

        try
        {
            message = JsonSerializer.Deserialize(utf8, EpexJsonContext.Default.EpexMarketData);
        }
        catch (JsonException)
        {
            return false;
        }

        if (message is null || string.IsNullOrEmpty(message.Symbol))
        {
            message = null;
            return false;
        }

        return true;
    }
}

/// <summary>
/// Kafka value deserializer. Reads from librdkafka's buffer with no intermediate byte[] or string.
/// Returns null for payloads that are not valid EPEX messages.
/// </summary>
public sealed class EpexMarketDataDeserializer : IDeserializer<EpexMarketData?>
{
    public EpexMarketData? Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context) =>
        !isNull && EpexMessageParser.TryParse(data, out var message) ? message : null;
}

[JsonSourceGenerationOptions(RespectNullableAnnotations = true, PropertyNameCaseInsensitive = true)]
[JsonSerializable(typeof(EpexMarketData))]
internal sealed partial class EpexJsonContext : JsonSerializerContext;

/// <summary>
/// Trims and upper-cases the symbol while it is read. Allocates a second string only when the
/// value actually needs changing.
/// </summary>
public sealed class SymbolJsonConverter : JsonConverter<string>
{
    public override string Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        if (reader.TokenType != JsonTokenType.String)
            throw new JsonException("Symbol must be a string.");

        var symbol = reader.GetString()!.Trim();
        foreach (var c in symbol)
        {
            if (char.IsLower(c))
                return symbol.ToUpperInvariant();
        }

        return symbol;
    }

    public override void Write(Utf8JsonWriter writer, string value, JsonSerializerOptions options) =>
        writer.WriteStringValue(value);
}
