using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;
using VehicleWriter.Models;
using VehicleWriter.Scenarios;

namespace VehicleWriter.Infrastructure;

public sealed class PostgresVehicleStore(NpgsqlDataSource dataSource, ILogger logger)
{
    public async Task ApplyAsync(VehicleScenarioStep step, CancellationToken cancellationToken)
    {
        switch (step.Operation)
        {
            case VehicleScenarioOperation.Create:
                await CreateAsync(RequireTransition(step), cancellationToken);
                break;
            case VehicleScenarioOperation.Update:
                await UpdateAsync(RequireTransition(step), cancellationToken);
                break;
            case VehicleScenarioOperation.InjectStaleUpdate:
                await InjectStaleUpdateAsync(RequireTransition(step), cancellationToken);
                break;
            case VehicleScenarioOperation.Delete:
                await DeleteAsync(step.VehicleId, cancellationToken);
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(step), step.Operation, "Unknown vehicle operation.");
        }
    }

    public async Task CreateAsync(VehicleTransition transition, CancellationToken cancellationToken)
    {
        Validate(transition);
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = new NpgsqlCommand(
            """
            INSERT INTO public.vehicles (id, state, state_version, state_updated_at)
            VALUES (@id, @state, @state_version, @state_updated_at)
            ON CONFLICT (id) DO NOTHING
            RETURNING id;
            """,
            connection);
        AddTransitionParameters(command, transition);
        var insertedId = await command.ExecuteScalarAsync(cancellationToken);
        if (insertedId is Guid)
        {
            logger.LogInformation(
                "Vehicle created. VehicleId={VehicleId} State={State} StateVersion={StateVersion}",
                transition.VehicleId,
                transition.State,
                transition.StateVersion);
            return;
        }

        var existing = await ReadAsync(connection, transition.VehicleId, cancellationToken);
        if (existing is null || !Matches(existing, transition))
        {
            throw new InvalidOperationException(
                $"Vehicle {transition.VehicleId} already exists with different scenario data.");
        }

        logger.LogInformation(
            "Vehicle creation retry was idempotent. VehicleId={VehicleId} StateVersion={StateVersion}",
            transition.VehicleId,
            transition.StateVersion);
    }

    public async Task UpdateAsync(VehicleTransition transition, CancellationToken cancellationToken)
    {
        Validate(transition);
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = new NpgsqlCommand(
            """
            UPDATE public.vehicles
            SET state = @state,
                state_version = @state_version,
                state_updated_at = @state_updated_at
            WHERE id = @id AND state_version < @state_version;
            """,
            connection);
        AddTransitionParameters(command, transition);
        var updated = await command.ExecuteNonQueryAsync(cancellationToken);
        if (updated == 1)
        {
            logger.LogInformation(
                "Vehicle state updated. VehicleId={VehicleId} State={State} StateVersion={StateVersion}",
                transition.VehicleId,
                transition.State,
                transition.StateVersion);
            return;
        }

        var existing = await ReadAsync(connection, transition.VehicleId, cancellationToken);
        if (existing is null)
        {
            throw new InvalidOperationException($"Vehicle {transition.VehicleId} does not exist.");
        }

        if (existing.StateVersion > transition.StateVersion)
        {
            logger.LogWarning(
                "Stale vehicle update rejected. VehicleId={VehicleId} IncomingVersion={IncomingVersion} CurrentVersion={CurrentVersion}",
                transition.VehicleId,
                transition.StateVersion,
                existing.StateVersion);
            return;
        }

        if (Matches(existing, transition))
        {
            logger.LogInformation(
                "Vehicle update retry was idempotent. VehicleId={VehicleId} StateVersion={StateVersion}",
                transition.VehicleId,
                transition.StateVersion);
            return;
        }

        throw new InvalidOperationException(
            $"Vehicle {transition.VehicleId} has conflicting data for state version {transition.StateVersion}.");
    }

    public async Task InjectStaleUpdateAsync(
        VehicleTransition transition,
        CancellationToken cancellationToken)
    {
        Validate(transition);
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        long currentVersion;
        await using (var readCommand = new NpgsqlCommand(
                         "SELECT state_version FROM public.vehicles WHERE id = @id FOR UPDATE;",
                         connection,
                         transaction))
        {
            readCommand.Parameters.AddWithValue("id", NpgsqlDbType.Uuid, transition.VehicleId);
            var value = await readCommand.ExecuteScalarAsync(cancellationToken);
            if (value is null)
            {
                throw new InvalidOperationException($"Vehicle {transition.VehicleId} does not exist.");
            }

            currentVersion = (long)value;
        }

        if (transition.StateVersion >= currentVersion)
        {
            throw new InvalidOperationException(
                $"Fault injection requires an older version. Current={currentVersion}, incoming={transition.StateVersion}.");
        }

        await using (var updateCommand = new NpgsqlCommand(
                         """
                         UPDATE public.vehicles
                         SET state = @state,
                             state_version = @state_version,
                             state_updated_at = @state_updated_at
                         WHERE id = @id;
                         """,
                         connection,
                         transaction))
        {
            AddTransitionParameters(updateCommand, transition);
            await updateCommand.ExecuteNonQueryAsync(cancellationToken);
        }

        await transaction.CommitAsync(cancellationToken);
        logger.LogWarning(
            "Fault injection persisted a stale vehicle state. VehicleId={VehicleId} PreviousVersion={PreviousVersion} PersistedVersion={PersistedVersion}",
            transition.VehicleId,
            currentVersion,
            transition.StateVersion);
    }

    public async Task DeleteAsync(Guid vehicleId, CancellationToken cancellationToken)
    {
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = new NpgsqlCommand(
            "DELETE FROM public.vehicles WHERE id = @id;",
            connection);
        command.Parameters.AddWithValue("id", NpgsqlDbType.Uuid, vehicleId);

        if (await command.ExecuteNonQueryAsync(cancellationToken) != 1)
        {
            throw new InvalidOperationException($"Vehicle {vehicleId} does not exist or was already deleted.");
        }

        logger.LogInformation("Vehicle physically deleted. VehicleId={VehicleId}", vehicleId);
    }

    public async Task<VehicleSnapshot> GetOrCreateContinuousVehicleAsync(
        CancellationToken cancellationToken)
    {
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        var existing = await ReadAsync(connection, VehicleScenario.ContinuousVehicleId, cancellationToken);
        if (existing is not null)
        {
            return existing;
        }

        var initial = new VehicleTransition(
            VehicleScenario.ContinuousVehicleId,
            VehicleState.REGISTERED,
            1,
            VehicleScenario.TruncateToMicroseconds(DateTimeOffset.UtcNow));
        await using var command = new NpgsqlCommand(
            """
            INSERT INTO public.vehicles (id, state, state_version, state_updated_at)
            VALUES (@id, @state, @state_version, @state_updated_at)
            ON CONFLICT (id) DO NOTHING;
            """,
            connection);
        AddTransitionParameters(command, initial);
        await command.ExecuteNonQueryAsync(cancellationToken);
        return await ReadAsync(connection, VehicleScenario.ContinuousVehicleId, cancellationToken)
               ?? throw new InvalidOperationException("Could not initialize the continuous vehicle.");
    }

    private static VehicleTransition RequireTransition(VehicleScenarioStep step) =>
        step.Transition ?? throw new InvalidOperationException(
            $"Vehicle operation {step.Operation} requires a state transition.");

    private static void AddTransitionParameters(NpgsqlCommand command, VehicleTransition transition)
    {
        command.Parameters.AddWithValue("id", NpgsqlDbType.Uuid, transition.VehicleId);
        command.Parameters.AddWithValue("state", NpgsqlDbType.Text, transition.State.ToString());
        command.Parameters.AddWithValue("state_version", NpgsqlDbType.Bigint, transition.StateVersion);
        command.Parameters.AddWithValue(
            "state_updated_at",
            NpgsqlDbType.TimestampTz,
            transition.StateUpdatedAt);
    }

    private static async Task<VehicleSnapshot?> ReadAsync(
        NpgsqlConnection connection,
        Guid vehicleId,
        CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(
            """
            SELECT id, state, state_version, state_updated_at
            FROM public.vehicles
            WHERE id = @id;
            """,
            connection);
        command.Parameters.AddWithValue("id", NpgsqlDbType.Uuid, vehicleId);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return null;
        }

        if (!Enum.TryParse<VehicleState>(reader.GetString(1), out var state) ||
            !Enum.IsDefined(state))
        {
            throw new InvalidOperationException($"Database contains an unsupported vehicle state for {vehicleId}.");
        }

        return new VehicleSnapshot(
            reader.GetGuid(0),
            state,
            reader.GetInt64(2),
            reader.GetFieldValue<DateTimeOffset>(3));
    }

    private static bool Matches(VehicleSnapshot existing, VehicleTransition transition) =>
        existing.VehicleId == transition.VehicleId &&
        existing.State == transition.State &&
        existing.StateVersion == transition.StateVersion &&
        existing.StateUpdatedAt == transition.StateUpdatedAt;

    private static void Validate(VehicleTransition transition)
    {
        if (transition.VehicleId == Guid.Empty)
        {
            throw new ArgumentException("Vehicle ID must not be empty.", nameof(transition));
        }

        if (transition.StateVersion <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(transition),
                "Vehicle state version must be positive.");
        }

        if (!Enum.IsDefined(transition.State))
        {
            throw new ArgumentOutOfRangeException(nameof(transition), "Vehicle state is not supported.");
        }

        if (transition.StateUpdatedAt.Offset != TimeSpan.Zero ||
            transition.StateUpdatedAt.UtcDateTime.Ticks % 10 != 0)
        {
            throw new ArgumentException(
                "Vehicle state timestamp must be UTC and have microsecond precision.",
                nameof(transition));
        }
    }
}
