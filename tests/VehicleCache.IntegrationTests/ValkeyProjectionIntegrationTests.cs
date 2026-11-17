using System.Collections.Concurrent;
using Microsoft.Extensions.Options;
using StackExchange.Redis;
using VehicleCache.Worker.Configuration;
using VehicleCache.Worker.Models;
using VehicleCache.Worker.Projection;

namespace VehicleCache.IntegrationTests;

public sealed class ValkeyProjectionIntegrationTests : IAsyncLifetime
{
    private const string VehicleTopic = "vehicle_cdc.public.vehicles";
    private const string PositionTopic = "vehicle_cdc.public.positions";
    private readonly ConcurrentBag<Guid> _vehicleIds = [];
    private IConnectionMultiplexer? _connection;
    private IDatabase? _database;
    private ValkeyProjection? _projection;
    private string? _consumerGroup;

    public Task InitializeAsync()
    {
        var connectionString = Environment.GetEnvironmentVariable(
            "VEHICLE_CACHE_TEST_VALKEY_CONNECTION_STRING");
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            throw new InvalidOperationException(
                "Set VEHICLE_CACHE_TEST_VALKEY_CONNECTION_STRING to run Valkey integration tests.");
        }

        _connection = ConnectionMultiplexer.Connect(connectionString);
        _database = _connection.GetDatabase();
        _consumerGroup = $"phase-04-2-tests-{Guid.NewGuid():N}";
        _projection = new ValkeyProjection(
            _connection,
            Options.Create(new KafkaOptions { ConsumerGroup = _consumerGroup }));
        return Task.CompletedTask;
    }

    public async Task DisposeAsync()
    {
        if (_database is not null)
        {
            foreach (var vehicleId in _vehicleIds)
            {
                await _database.KeyDeleteAsync(ValkeyProjection.VehicleKey(vehicleId));
            }

            if (_consumerGroup is not null)
            {
                await _database.KeyDeleteAsync($"vehicle-cache:progress:{_consumerGroup}");
            }
        }

        _connection?.Dispose();
    }

    [Fact]
    public async Task VehicleCreationInitializesMarkersAndAllowsStateUpdate()
    {
        var vehicleId = NewVehicleId();

        await ApplyAsync(Vehicle(vehicleId, VehicleTopic, 0, 0, 1, "REGISTERED"));
        await ApplyAsync(Vehicle(vehicleId, VehicleTopic, 0, 1, 2, "ACTIVE"));

        var hash = await ReadVehicleAsync(vehicleId);
        Assert.Equal("1", hash["vehicle_seen"]);
        Assert.Equal("0", hash["is_deleted"]);
        Assert.Equal("1", hash["state_present"]);
        Assert.Equal("0", hash["position_seen"]);
        Assert.Equal("ACTIVE", hash["state"]);
        Assert.Equal("2", hash["state_version"]);
    }

    [Fact]
    public async Task VehicleCreationAllowsPositionAndPreservesAllMarkers()
    {
        var vehicleId = NewVehicleId();

        await ApplyAsync(Vehicle(vehicleId, VehicleTopic, 0, 0, 1, "REGISTERED"));
        await ApplyAsync(Position(vehicleId, PositionTopic, 0, 0, NewPositionId(), "2026-01-02T12:00:00.123456Z"));

        var hash = await ReadVehicleAsync(vehicleId);
        Assert.Equal("1", hash["vehicle_seen"]);
        Assert.Equal("0", hash["is_deleted"]);
        Assert.Equal("1", hash["state_present"]);
        Assert.Equal("1", hash["position_seen"]);
        Assert.Equal("REGISTERED", hash["state"]);
        Assert.Equal("2026-01-02T12:00:00.123456Z", hash["recorded_at"]);
    }

    [Fact]
    public async Task PositionBeforeVehicleIsPreservedWhenVehicleArrives()
    {
        var vehicleId = NewVehicleId();
        var positionId = NewPositionId();

        await ApplyAsync(Position(vehicleId, PositionTopic, 0, 0, positionId, "2026-01-02T12:00:00.123456Z"));
        await ApplyAsync(Vehicle(vehicleId, VehicleTopic, 0, 0, 1, "REGISTERED"));

        var hash = await ReadVehicleAsync(vehicleId);
        Assert.Equal("1", hash["vehicle_seen"]);
        Assert.Equal("1", hash["state_present"]);
        Assert.Equal("1", hash["position_seen"]);
        Assert.Equal(positionId.ToString("D").ToLowerInvariant(), hash["position_id"]);
    }

    [Fact]
    public async Task StaleAndDuplicateCandidatesAreNoOpsAndAdvanceProgress()
    {
        var vehicleId = NewVehicleId();
        var winningPositionId = NewPositionId();

        await ApplyAsync(Vehicle(vehicleId, VehicleTopic, 0, 0, 2, "ACTIVE"));
        await ApplyAsync(Vehicle(vehicleId, VehicleTopic, 0, 1, 1, "REGISTERED"));
        await ApplyAsync(Vehicle(vehicleId, VehicleTopic, 0, 2, 2, "ACTIVE"));
        await ApplyAsync(Position(vehicleId, PositionTopic, 0, 0, winningPositionId, "2026-01-02T12:00:00.200000Z"));
        await ApplyAsync(Position(vehicleId, PositionTopic, 0, 1, NewPositionId(), "2026-01-02T12:00:00.100000Z"));
        await ApplyAsync(Position(vehicleId, PositionTopic, 0, 2, winningPositionId, "2026-01-02T12:00:00.200000Z"));

        var hash = await ReadVehicleAsync(vehicleId);
        Assert.Equal("ACTIVE", hash["state"]);
        Assert.Equal("2", hash["state_version"]);
        Assert.Equal(winningPositionId.ToString("D").ToLowerInvariant(), hash["position_id"]);
        Assert.Equal(3, await ProgressAsync(VehicleTopic));
        Assert.Equal(3, await ProgressAsync(PositionTopic));
    }

    [Fact]
    public async Task EqualTimestampUsesCanonicalUuidOrdering()
    {
        var vehicleId = NewVehicleId();
        var lowerId = Guid.Parse("20000000-0000-4000-8000-000000000001");
        var higherId = Guid.Parse("20000000-0000-4000-8000-000000000002");
        var timestamp = "2026-01-02T12:00:00.123456Z";

        await ApplyAsync(Position(vehicleId, PositionTopic, 0, 0, higherId, timestamp));
        await ApplyAsync(Position(vehicleId, PositionTopic, 0, 1, lowerId, timestamp));

        var hash = await ReadVehicleAsync(vehicleId);
        Assert.Equal(higherId.ToString("D"), hash["position_id"]);
    }

    [Fact]
    public async Task DeleteIsTerminalAndDuplicateDeleteAndTombstoneAdvanceProgress()
    {
        var vehicleId = NewVehicleId();

        await ApplyAsync(Vehicle(vehicleId, VehicleTopic, 0, 0, 1, "ACTIVE"));
        await ApplyAsync(Position(vehicleId, PositionTopic, 0, 0, NewPositionId(), "2026-01-02T12:00:00.123456Z"));
        await ApplyAsync(Delete(vehicleId, 0, 2, 1));
        await ApplyAsync(Delete(vehicleId, 0, 3, 1));
        await ApplyAsync(Position(vehicleId, PositionTopic, 0, 1, NewPositionId(), "2026-01-02T12:01:00.123456Z"));
        await ApplyAsync(Tombstone(vehicleId, 0, 4));

        var hash = await ReadVehicleAsync(vehicleId);
        Assert.Equal("1", hash["vehicle_seen"]);
        Assert.Equal("1", hash["is_deleted"]);
        Assert.Equal("0", hash["state_present"]);
        Assert.Equal("0", hash["position_seen"]);
        Assert.Equal("1", hash["delete_state_version"]);
        Assert.DoesNotContain("state", hash.Keys);
        Assert.DoesNotContain("position_id", hash.Keys);
        Assert.Equal(5, await ProgressAsync(VehicleTopic));
        Assert.Equal(2, await ProgressAsync(PositionTopic));
    }

    [Fact]
    public async Task InvalidCandidateDoesNotPartiallyMutateProjectionOrCursor()
    {
        var vehicleId = NewVehicleId();
        await ApplyAsync(Vehicle(vehicleId, VehicleTopic, 0, 0, 1, "ACTIVE"));
        var originalHash = await ReadVehicleAsync(vehicleId);
        var invalidPosition = new CdcChange(
            CdcTable.Positions,
            CdcOperation.Position,
            vehicleId,
            null,
            new PositionChange(vehicleId, NewPositionId(), 91, 0, "2026-01-02T12:00:00.123456Z"),
            null,
            null);

        await Assert.ThrowsAsync<RedisServerException>(() =>
            ApplyAsync(new CdcDelivery(invalidPosition, PositionTopic, 0, 0)));

        var afterRejectedCandidate = await ReadVehicleAsync(vehicleId);
        Assert.Equal(
            originalHash.OrderBy(entry => entry.Key).ToArray(),
            afterRejectedCandidate.OrderBy(entry => entry.Key).ToArray());
        Assert.Null(await ProgressAsync(PositionTopic));
    }

    private Guid NewVehicleId()
    {
        var vehicleId = Guid.NewGuid();
        _vehicleIds.Add(vehicleId);
        return vehicleId;
    }

    private static Guid NewPositionId() => Guid.NewGuid();

    private Task<int> ApplyAsync(CdcDelivery delivery) =>
        (_projection ?? throw new InvalidOperationException("Integration test is not initialized."))
        .ApplyAsync(delivery);

    private async Task<Dictionary<string, string>> ReadVehicleAsync(Guid vehicleId)
    {
        var entries = await RequiredDatabase().HashGetAllAsync(ValkeyProjection.VehicleKey(vehicleId));
        return entries.ToDictionary(entry => entry.Name.ToString(), entry => entry.Value.ToString());
    }

    private Task<long?> ProgressAsync(string topic) =>
        (_projection ?? throw new InvalidOperationException("Integration test is not initialized."))
        .GetProcessedNextOffsetAsync(topic, 0);

    private IDatabase RequiredDatabase() =>
        _database ?? throw new InvalidOperationException("Integration test is not initialized.");

    private static CdcDelivery Vehicle(
        Guid vehicleId,
        string topic,
        int partition,
        long offset,
        long stateVersion,
        string state) =>
        new(
            new CdcChange(
                CdcTable.Vehicles,
                CdcOperation.VehicleState,
                vehicleId,
                new VehicleStateChange(
                    vehicleId,
                    state,
                    stateVersion,
                    "2026-01-02T12:00:00.123456Z"),
                null,
                null,
                null),
            topic,
            partition,
            offset);

    private static CdcDelivery Position(
        Guid vehicleId,
        string topic,
        int partition,
        long offset,
        Guid positionId,
        string recordedAt) =>
        new(
            new CdcChange(
                CdcTable.Positions,
                CdcOperation.Position,
                vehicleId,
                null,
                new PositionChange(vehicleId, positionId, 40.0, -74.0, recordedAt),
                null,
                null),
            topic,
            partition,
            offset);

    private static CdcDelivery Delete(Guid vehicleId, int partition, long offset, long version) =>
        new(
            new CdcChange(
                CdcTable.Vehicles,
                CdcOperation.Delete,
                vehicleId,
                null,
                null,
                version,
                null),
            VehicleTopic,
            partition,
            offset);

    private static CdcDelivery Tombstone(Guid vehicleId, int partition, long offset) =>
        new(
            new CdcChange(
                CdcTable.Vehicles,
                CdcOperation.Tombstone,
                vehicleId,
                null,
                null,
                null,
                null),
            VehicleTopic,
            partition,
            offset);
}
