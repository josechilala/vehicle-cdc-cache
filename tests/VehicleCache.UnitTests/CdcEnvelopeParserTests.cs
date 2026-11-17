using System.Text;
using VehicleCache.Worker.Configuration;
using VehicleCache.Worker.Consumers;
using VehicleCache.Worker.Models;

namespace VehicleCache.UnitTests;

public sealed class CdcEnvelopeParserTests
{
    private const string VehicleTopic = "vehicle_cdc.public.vehicles";
    private const string PositionTopic = "vehicle_cdc.public.positions";
    private static readonly Guid VehicleId = Guid.Parse("10000000-0000-4000-8000-000000000001");
    private static readonly Guid PositionId = Guid.Parse("20000000-0000-4000-8000-000000000020");
    private readonly CdcEnvelopeParser _parser = new();

    [Theory]
    [InlineData("c")]
    [InlineData("u")]
    [InlineData("r")]
    public void VehicleCreateUpdateAndSnapshotParseBusinessVersion(string operation)
    {
        var parsed = ParseVehicleEnvelope(operation, """{"id":"10000000-0000-4000-8000-000000000001"}""");

        Assert.Equal(CdcTable.Vehicles, parsed.Table);
        Assert.Equal(CdcOperation.VehicleState, parsed.Operation);
        Assert.Equal(VehicleId, parsed.VehicleId);
        Assert.Equal("IDLE", parsed.Vehicle!.State);
        Assert.Equal(42, parsed.Vehicle.StateVersion);
        Assert.Equal("2026-01-02T11:59:00.123456Z", parsed.Vehicle.StateUpdatedAt);
        Assert.Equal("vehicle_cache", parsed.Source!.Database);
        Assert.Equal("public", parsed.Source.Schema);
        Assert.Equal("vehicles", parsed.Source.Table);
        Assert.Equal("9007199254740993", parsed.Source.Lsn);
        Assert.Equal("18", parsed.Source.TransactionId);
    }

    [Fact]
    public void VehicleDeleteUsesBeforeImageVersionAndRequiresNullAfter()
    {
        const string value =
            """
            {
              "before": {
                "id": "10000000-0000-4000-8000-000000000001",
                "state": "IDLE",
                "state_version": 42,
                "state_updated_at": "2026-01-02T11:59:00.123456Z"
              },
              "after": null,
              "source": {
                "db": "vehicle_cache",
                "schema": "public",
                "table": "vehicles",
                "lsn": 123,
                "txId": 18
              },
              "op": "d"
            }
            """;

        var parsed = _parser.Parse(
            Bytes("""{"id":"10000000-0000-4000-8000-000000000001"}"""),
            Bytes(value),
            VehicleTopic,
            VehicleTopic,
            PositionTopic);

        Assert.Equal(CdcOperation.Delete, parsed.Operation);
        Assert.Equal(42, parsed.DeleteStateVersion);
        Assert.Null(parsed.Vehicle);
        Assert.Null(parsed.Position);
    }

    [Fact]
    public void VehicleKafkaTombstoneIsDistinctFromDeleteEnvelope()
    {
        var parsed = _parser.Parse(
            Bytes("""{"id":"10000000-0000-4000-8000-000000000001"}"""),
            null,
            VehicleTopic,
            VehicleTopic,
            PositionTopic);

        Assert.Equal(CdcOperation.Tombstone, parsed.Operation);
        Assert.Equal(VehicleId, parsed.VehicleId);
        Assert.Null(parsed.Source);
    }

    [Fact]
    public void PositionSnapshotParsesConfiguredVehicleKeyAndPreservesMicroseconds()
    {
        var epochMicroseconds =
            (new DateTimeOffset(2026, 1, 2, 12, 0, 0, TimeSpan.Zero).Ticks -
             DateTimeOffset.UnixEpoch.Ticks) / 10 + 123_457;
        var value = $$"""
            {
              "before": null,
              "after": {
                "id": "{{PositionId:D}}",
                "vehicle_id": "{{VehicleId:D}}",
                "lat": 40.7138,
                "lng": -74.005,
                "recorded_at": {{epochMicroseconds}}
              },
              "source": {
                "db": "vehicle_cache",
                "schema": "public",
                "table": "positions",
                "lsn": 123,
                "snapshot": "true"
              },
              "op": "r"
            }
            """;

        var parsed = _parser.Parse(
            Bytes("""{"vehicle_id":"10000000-0000-4000-8000-000000000001"}"""),
            Bytes(value),
            PositionTopic,
            VehicleTopic,
            PositionTopic);

        Assert.Equal(CdcOperation.Position, parsed.Operation);
        Assert.Equal(VehicleId, parsed.VehicleId);
        Assert.Equal(PositionId, parsed.Position!.PositionId);
        Assert.Equal(40.7138, parsed.Position.Latitude);
        Assert.Equal(-74.005, parsed.Position.Longitude);
        Assert.Equal("2026-01-02T12:00:00.123457Z", parsed.Position.RecordedAt);
        Assert.Equal("true", parsed.Source!.Snapshot);
    }

    [Theory]
    [InlineData("x")]
    [InlineData("z")]
    public void UnsupportedOperationsAreRejected(string operation)
    {
        var value = $$"""
            {
              "before": null,
              "after": null,
              "source": {
                "db": "vehicle_cache",
                "schema": "public",
                "table": "vehicles"
              },
              "op": "{{operation}}"
            }
            """;

        Assert.Throws<InvalidDataException>(() =>
            _parser.Parse(
                Bytes("""{"id":"10000000-0000-4000-8000-000000000001"}"""),
                Bytes(value),
                VehicleTopic,
                VehicleTopic,
                PositionTopic));
    }

    [Fact]
    public void InvalidJsonKeySourceOrRowIsRejected()
    {
        Assert.Throws<InvalidDataException>(() =>
            _parser.Parse(Bytes("{}"), Bytes(VehicleEnvelope("c", "not-a-uuid")), VehicleTopic, VehicleTopic, PositionTopic));
        Assert.Throws<InvalidDataException>(() =>
            _parser.Parse(Bytes("{"), Bytes(VehicleEnvelope("c", "REGISTERED")), VehicleTopic, VehicleTopic, PositionTopic));
        Assert.Throws<InvalidDataException>(() =>
            _parser.Parse(
                Bytes("""{"id":"10000000-0000-4000-8000-000000000001"}"""),
                Bytes("""{"op":"c","source":{"db":"vehicle_cache","schema":"public","table":"positions"},"after":{}}"""),
                VehicleTopic,
                VehicleTopic,
                PositionTopic));
        Assert.Throws<InvalidDataException>(() =>
            _parser.Parse(
                Bytes("""{"id":"10000000-0000-4000-8000-000000000001"}"""),
                Bytes(VehicleEnvelope("c", "REGISTERED")),
                "unexpected-topic",
                VehicleTopic,
                PositionTopic));
    }

    [Fact]
    public void TimestampNormalizationRejectsPrecisionBeyondPostgresMicroseconds()
    {
        using var document = System.Text.Json.JsonDocument.Parse("\"2026-01-02T12:00:00.1234567Z\"");
        Assert.Throws<InvalidDataException>(() =>
            CdcEnvelopeParser.NormalizeTimestamp(document.RootElement, "recorded_at"));
    }

    [Fact]
    public void WorkerConfigurationValidationRejectsInvalidTopicsAndValkeyEndpoint()
    {
        var kafka = new KafkaOptions
        {
            BootstrapServers = "localhost:9092",
            VehicleTopic = VehicleTopic,
            PositionTopic = PositionTopic,
            ConsumerGroup = "vehicle-cache-worker"
        };
        var valkey = new ValkeyOptions { ConnectionString = "localhost:6379" };

        Assert.True(WorkerOptionsValidation.IsValidKafka(kafka));
        Assert.True(WorkerOptionsValidation.IsValidValkey(valkey));
        Assert.False(WorkerOptionsValidation.IsValidKafka(new KafkaOptions
        {
            BootstrapServers = "localhost:9092",
            VehicleTopic = VehicleTopic,
            PositionTopic = VehicleTopic,
            ConsumerGroup = "vehicle-cache-worker"
        }));
        Assert.False(WorkerOptionsValidation.IsValidValkey(new ValkeyOptions
        {
            ConnectionString = ""
        }));
    }

    private CdcChange ParseVehicleEnvelope(string operation, string key) =>
        _parser.Parse(
            Bytes(key),
            Bytes(VehicleEnvelope(operation, "IDLE")),
            VehicleTopic,
            VehicleTopic,
            PositionTopic);

    private static string VehicleEnvelope(string operation, string state)
    {
        var before = operation == "u"
            ? """{"id":"10000000-0000-4000-8000-000000000001","state_version":41}"""
            : "null";
        return $$"""
        {
          "before": {{before}},
          "after": {
            "id": "10000000-0000-4000-8000-000000000001",
            "state": "{{state}}",
            "state_version": 42,
            "state_updated_at": "2026-01-02T11:59:00.123456Z"
          },
          "source": {
            "db": "vehicle_cache",
            "schema": "public",
            "table": "vehicles",
            "lsn": 9007199254740993,
            "txId": 18,
            "snapshot": false
          },
          "op": "{{operation}}"
        }
        """;
    }

    private static byte[] Bytes(string value) => Encoding.UTF8.GetBytes(value);
}
