namespace VehicleWriter.Models;

public sealed record VehicleTransition(
    Guid VehicleId,
    VehicleState State,
    long StateVersion,
    DateTimeOffset StateUpdatedAt);

public sealed record VehicleSnapshot(
    Guid VehicleId,
    VehicleState State,
    long StateVersion,
    DateTimeOffset StateUpdatedAt);
