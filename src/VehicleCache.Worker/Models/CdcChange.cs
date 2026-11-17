namespace VehicleCache.Worker.Models;

public enum CdcTable
{
    Vehicles,
    Positions
}

public enum CdcOperation
{
    VehicleState,
    Position,
    Delete,
    Tombstone
}

public sealed record CdcSourceMetadata(
    string Database,
    string Schema,
    string Table,
    string? Lsn,
    string? TransactionId,
    string? Snapshot);

public sealed record VehicleStateChange(
    Guid VehicleId,
    string State,
    long StateVersion,
    string StateUpdatedAt);

public sealed record PositionChange(
    Guid VehicleId,
    Guid PositionId,
    double Latitude,
    double Longitude,
    string RecordedAt);

public sealed record CdcChange(
    CdcTable Table,
    CdcOperation Operation,
    Guid VehicleId,
    VehicleStateChange? Vehicle,
    PositionChange? Position,
    long? DeleteStateVersion,
    CdcSourceMetadata? Source);

public sealed record CdcDelivery(
    CdcChange Change,
    string Topic,
    int Partition,
    long Offset);
