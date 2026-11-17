using PositionWriter.Models;

namespace PositionWriter.Scenarios;

public static class PositionScenario
{
    private static readonly DateTimeOffset PositionTime =
        new(2026, 1, 2, 12, 0, 0, TimeSpan.Zero);

    public static readonly Guid CurrentVehicleId =
        Guid.Parse("10000000-0000-4000-8000-000000000001");

    public static readonly Guid DeletedVehicleId =
        Guid.Parse("10000000-0000-4000-8000-000000000002");

    public static readonly Guid PositionBeforeCreationVehicleId =
        Guid.Parse("10000000-0000-4000-8000-000000000003");

    public static IReadOnlyList<VehiclePosition> Build(string scenario) =>
        scenario.ToLowerInvariant() switch
        {
            "before-vehicles" => BuildBeforeVehicles(),
            "after-delete" => BuildAfterDelete(),
            "conflicting-duplicate" => BuildConflictingDuplicate(),
            _ => throw new ArgumentException($"Unknown deterministic position scenario '{scenario}'.", nameof(scenario))
        };

    public static VehiclePosition CreateContinuousPosition(long sequence)
    {
        if (sequence <= 0 || sequence > 0xFFFFFFFFFFFF)
        {
            throw new ArgumentOutOfRangeException(nameof(sequence));
        }

        var positionId = Guid.Parse($"30000000-0000-4000-8000-{sequence:x12}");
        var latitude = 40.0 + sequence % 800_000 / 100_000.0;
        var longitude = -74.0 + sequence % 800_000 / 100_000.0;
        return new VehiclePosition(
            positionId,
            CurrentVehicleId,
            latitude,
            longitude,
            TruncateToMicroseconds(DateTimeOffset.UtcNow));
    }

    private static IReadOnlyList<VehiclePosition> BuildBeforeVehicles() =>
    [
        new(
            Guid.Parse("20000000-0000-4000-8000-000000000010"),
            CurrentVehicleId,
            40.7128,
            -74.0060,
            PositionTime.AddTicks(1_234_570)),
        new(
            Guid.Parse("20000000-0000-4000-8000-000000000040"),
            CurrentVehicleId,
            40.7000,
            -74.0100,
            PositionTime.AddTicks(1_234_560)),
        new(
            Guid.Parse("20000000-0000-4000-8000-000000000020"),
            CurrentVehicleId,
            40.7138,
            -74.0050,
            PositionTime.AddTicks(1_234_570)),
        new(
            Guid.Parse("20000000-0000-4000-8000-000000000020"),
            CurrentVehicleId,
            40.7138,
            -74.0050,
            PositionTime.AddTicks(1_234_570)),
        new(
            Guid.Parse("20000000-0000-4000-8000-000000000030"),
            PositionBeforeCreationVehicleId,
            34.0522,
            -118.2437,
            PositionTime.AddTicks(1_234_560))
    ];

    private static IReadOnlyList<VehiclePosition> BuildAfterDelete() =>
    [
        new(
            Guid.Parse("20000000-0000-4000-8000-000000000050"),
            DeletedVehicleId,
            37.7749,
            -122.4194,
            PositionTime.AddTicks(2_000_000))
    ];

    private static IReadOnlyList<VehiclePosition> BuildConflictingDuplicate() =>
    [
        new(
            Guid.Parse("20000000-0000-4000-8000-000000000020"),
            CurrentVehicleId,
            40.7138,
            -74.0000,
            PositionTime.AddTicks(1_234_570))
    ];

    private static DateTimeOffset TruncateToMicroseconds(DateTimeOffset value)
    {
        var utc = value.ToUniversalTime();
        return new DateTimeOffset(utc.Ticks - utc.Ticks % 10, TimeSpan.Zero);
    }
}
