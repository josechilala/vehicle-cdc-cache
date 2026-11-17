namespace PositionWriter.Models;

public sealed record VehiclePosition(
    Guid PositionId,
    Guid VehicleId,
    double Latitude,
    double Longitude,
    DateTimeOffset RecordedAt);
