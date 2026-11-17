using System.Globalization;
using VehicleWriter.Models;

namespace VehicleWriter.Scenarios;

public enum VehicleScenarioOperation
{
    Create,
    Update,
    InjectStaleUpdate,
    Delete
}

public sealed record VehicleScenarioStep(
    VehicleScenarioOperation Operation,
    Guid VehicleId,
    VehicleTransition? Transition);

public static class VehicleScenario
{
    public static readonly Guid CurrentVehicleId =
        Guid.Parse("10000000-0000-4000-8000-000000000001");

    public static readonly Guid DeletedVehicleId =
        Guid.Parse("10000000-0000-4000-8000-000000000002");

    public static readonly Guid PositionBeforeCreationVehicleId =
        Guid.Parse("10000000-0000-4000-8000-000000000003");

    public static readonly Guid ContinuousVehicleId =
        Guid.Parse("10000000-0000-4000-8000-000000000004");

    public static IReadOnlyList<VehicleScenarioStep> BuildBaseline() =>
    [
        Create(CurrentVehicleId, VehicleState.REGISTERED, 1, "2026-01-02T12:00:00Z"),
        Update(CurrentVehicleId, VehicleState.ACTIVE, 2, "2026-01-02T12:01:00Z"),
        Update(CurrentVehicleId, VehicleState.IDLE, 3, "2026-01-02T11:59:00Z"),
        Update(CurrentVehicleId, VehicleState.ACTIVE, 2, "2026-01-02T12:01:00Z"),
        StaleUpdate(CurrentVehicleId, VehicleState.ACTIVE, 2, "2026-01-02T12:01:00Z"),
        Create(DeletedVehicleId, VehicleState.REGISTERED, 1, "2026-01-02T12:03:00Z"),
        Update(DeletedVehicleId, VehicleState.DECOMMISSIONED, 2, "2026-01-02T12:04:00Z"),
        new(VehicleScenarioOperation.Delete, DeletedVehicleId, null),
        Create(PositionBeforeCreationVehicleId, VehicleState.REGISTERED, 1, "2026-01-02T12:05:00Z")
    ];

    public static DateTimeOffset TruncateToMicroseconds(DateTimeOffset value)
    {
        var utc = value.ToUniversalTime();
        var truncatedTicks = utc.Ticks - utc.Ticks % 10;
        return new DateTimeOffset(truncatedTicks, TimeSpan.Zero);
    }

    private static VehicleScenarioStep Create(
        Guid vehicleId,
        VehicleState state,
        long version,
        string timestamp) =>
        Step(VehicleScenarioOperation.Create, vehicleId, state, version, timestamp);

    private static VehicleScenarioStep Update(
        Guid vehicleId,
        VehicleState state,
        long version,
        string timestamp) =>
        Step(VehicleScenarioOperation.Update, vehicleId, state, version, timestamp);

    private static VehicleScenarioStep StaleUpdate(
        Guid vehicleId,
        VehicleState state,
        long version,
        string timestamp) =>
        Step(VehicleScenarioOperation.InjectStaleUpdate, vehicleId, state, version, timestamp);

    private static VehicleScenarioStep Step(
        VehicleScenarioOperation operation,
        Guid vehicleId,
        VehicleState state,
        long version,
        string timestamp) =>
        new(
            operation,
            vehicleId,
            new VehicleTransition(
                vehicleId,
                state,
                version,
                DateTimeOffset.Parse(timestamp, CultureInfo.InvariantCulture)));
}
