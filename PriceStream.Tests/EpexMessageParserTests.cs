using System.Text;
using Confluent.Kafka;
using PriceStream.Services;
using Xunit;

namespace PriceStream.Tests;

public sealed class EpexMessageParserTests
{
    private const string Valid =
        """{"Symbol":" de ","Market":"Day-Ahead","DeliveryArea":"DE-LU","Timestamp":"2026-10-12T12:00:00+00:00","Price":42.35,"Unit":"EUR/MWh","Contract":"P60"}""";

    [Fact]
    public void Parses_utf8_and_normalizes_the_symbol()
    {
        Assert.True(EpexMessageParser.TryParse(Encoding.UTF8.GetBytes(Valid), out var message));
        Assert.Equal("DE", message.Symbol);
        Assert.Equal(42.35m, message.Price);
        Assert.Equal(DateTimeOffset.Parse("2026-10-12T12:00:00+00:00"), message.Timestamp);
        Assert.Equal("P60", message.Contract);
    }

    [Fact]
    public void Upper_case_symbol_passes_through()
    {
        var json = Encoding.UTF8.GetBytes(Valid.Replace("\" de \"", "\"FR\""));
        Assert.True(EpexMessageParser.TryParse(json, out var message));
        Assert.Equal("FR", message.Symbol);
    }

    [Theory]
    [InlineData("")]
    [InlineData("not json")]
    [InlineData("""{"Market":"Day-Ahead","DeliveryArea":"DE"}""")]
    [InlineData("""{"Symbol":null,"Market":"Day-Ahead","DeliveryArea":"DE"}""")]
    [InlineData("""{"Symbol":"   ","Market":"Day-Ahead","DeliveryArea":"DE"}""")]
    [InlineData("""{"Symbol":7,"Market":"Day-Ahead","DeliveryArea":"DE"}""")]
    public void Rejects_payloads_that_are_not_epex_messages(string json)
    {
        Assert.False(EpexMessageParser.TryParse(Encoding.UTF8.GetBytes(json), out var message));
        Assert.Null(message);
    }

    [Fact]
    public void Kafka_deserializer_returns_null_for_tombstones_and_bad_payloads()
    {
        var deserializer = new EpexMarketDataDeserializer();
        var context = new SerializationContext(MessageComponentType.Value, "epex-market-data");

        Assert.Equal("DE", deserializer.Deserialize(Encoding.UTF8.GetBytes(Valid), isNull: false, context)!.Symbol);
        Assert.Null(deserializer.Deserialize(ReadOnlySpan<byte>.Empty, isNull: true, context));
        Assert.Null(deserializer.Deserialize("{"u8, isNull: false, context));
    }
}
