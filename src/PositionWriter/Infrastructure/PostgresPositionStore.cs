using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;
using PositionWriter.Models;

namespace PositionWriter.Infrastructure;

public sealed class PostgresPositionStore(NpgsqlDataSource dataSource, ILogger logger)
{
    public async Task InsertAsync(VehiclePosition position, CancellationToken cancellationToken)
    {
        Validate(position);
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = new NpgsqlCommand(
            """
            INSERT INTO public.positions (id, vehicle_id, lat, lng, recorded_at)
            VALUES (@id, @vehicle_id, @lat, @lng, @recorded_at)
            ON CONFLICT (id) DO NOTHING;
            """,
            connection);
        AddPositionParameters(command, position);

        var inserted = await command.ExecuteNonQueryAsync(cancellationToken);
        if (inserted == 1)
        {
            logger.LogInformation(
                "Position inserted. PositionId={PositionId} VehicleId={VehicleId} RecordedAt={RecordedAt}",
                position.PositionId,
                position.VehicleId,
                position.RecordedAt);
            return;
        }

        var existing = await ReadAsync(connection, position.PositionId, cancellationToken);
        if (existing is null || !Matches(existing, position))
        {
            throw new InvalidOperationException(
                $"Position ID {position.PositionId} already exists with conflicting content.");
        }

        logger.LogInformation(
            "Duplicate position insert was idempotent. PositionId={PositionId}",
            position.PositionId);
    }

    private static async Task<VehiclePosition?> ReadAsync(
        NpgsqlConnection connection,
        Guid positionId,
        CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(
            """
            SELECT id, vehicle_id, lat, lng, recorded_at
            FROM public.positions
            WHERE id = @id;
            """,
            connection);
        command.Parameters.AddWithValue("id", NpgsqlDbType.Uuid, positionId);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return null;
        }

        return new VehiclePosition(
            reader.GetGuid(0),
            reader.GetGuid(1),
            reader.GetDouble(2),
            reader.GetDouble(3),
            reader.GetFieldValue<DateTimeOffset>(4));
    }

    private static void AddPositionParameters(NpgsqlCommand command, VehiclePosition position)
    {
        command.Parameters.AddWithValue("id", NpgsqlDbType.Uuid, position.PositionId);
        command.Parameters.AddWithValue("vehicle_id", NpgsqlDbType.Uuid, position.VehicleId);
        command.Parameters.AddWithValue("lat", NpgsqlDbType.Double, position.Latitude);
        command.Parameters.AddWithValue("lng", NpgsqlDbType.Double, position.Longitude);
        command.Parameters.AddWithValue("recorded_at", NpgsqlDbType.TimestampTz, position.RecordedAt);
    }

    private static bool Matches(VehiclePosition existing, VehiclePosition candidate) =>
        existing.PositionId == candidate.PositionId &&
        existing.VehicleId == candidate.VehicleId &&
        existing.Latitude.Equals(candidate.Latitude) &&
        existing.Longitude.Equals(candidate.Longitude) &&
        existing.RecordedAt == candidate.RecordedAt;

    private static void Validate(VehiclePosition position)
    {
        if (position.PositionId == Guid.Empty || position.VehicleId == Guid.Empty)
        {
            throw new ArgumentException("Position and vehicle IDs must not be empty.", nameof(position));
        }

        if (!double.IsFinite(position.Latitude) || position.Latitude is < -90 or > 90)
        {
            throw new ArgumentOutOfRangeException(nameof(position), "Latitude must be between -90 and 90.");
        }

        if (!double.IsFinite(position.Longitude) || position.Longitude is < -180 or > 180)
        {
            throw new ArgumentOutOfRangeException(nameof(position), "Longitude must be between -180 and 180.");
        }

        if (position.RecordedAt.Offset != TimeSpan.Zero ||
            position.RecordedAt.UtcDateTime.Ticks % 10 != 0)
        {
            throw new ArgumentException(
                "Position timestamp must be UTC and have microsecond precision.",
                nameof(position));
        }
    }
}
