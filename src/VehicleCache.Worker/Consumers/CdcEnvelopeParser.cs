using System.Globalization;
using System.Text.Json;
using VehicleCache.Worker.Models;

namespace VehicleCache.Worker.Consumers;

public sealed class CdcEnvelopeParser
{
    private static readonly HashSet<string> VehicleStates =
        new(StringComparer.Ordinal)
        {
            "REGISTERED",
            "ACTIVE",
            "IDLE",
            "MAINTENANCE",
            "DECOMMISSIONED"
        };

    public CdcChange Parse(byte[]? keyBytes, byte[]? valueBytes, string topic, string vehicleTopic, string positionTopic)
    {
        var table = GetTable(topic, vehicleTopic, positionTopic);
        var vehicleId = ParseKey(keyBytes, table);

        if (valueBytes is null)
        {
            return new CdcChange(table, CdcOperation.Tombstone, vehicleId, null, null, null, null);
        }

        try
        {
            using var document = JsonDocument.Parse(valueBytes);
            var envelope = document.RootElement;
            RequireObject(envelope, "Debezium envelope");

            var operation = GetRequiredString(envelope, "op");
            var source = ParseSource(GetRequiredProperty(envelope, "source"), table);
            return (table, operation) switch
            {
                (CdcTable.Vehicles, "c") or
                (CdcTable.Vehicles, "r") => ParseVehicleState(envelope, vehicleId, source, operation),
                (CdcTable.Vehicles, "u") => ParseVehicleUpdate(envelope, vehicleId, source),
                (CdcTable.Vehicles, "d") => ParseVehicleDelete(envelope, vehicleId, source),
                (CdcTable.Positions, "c") or
                (CdcTable.Positions, "r") => ParsePosition(envelope, vehicleId, source),
                (CdcTable.Positions, "u") =>
                    throw new InvalidDataException("Position CDC updates are not supported; positions are insert-only."),
                (CdcTable.Positions, "d") =>
                    throw new InvalidDataException("Position CDC deletes are not supported; positions are insert-only."),
                _ => throw new InvalidDataException(
                    $"Unsupported Debezium operation '{operation}' for topic '{topic}'.")
            };
        }
        catch (JsonException exception)
        {
            throw new InvalidDataException("CDC value is not a valid JSON Debezium envelope.", exception);
        }
    }

    public static string NormalizeTimestamp(JsonElement value, string fieldName)
    {
        DateTimeOffset timestamp;
        if (value.ValueKind == JsonValueKind.String)
        {
            var text = value.GetString();
            if (text is null ||
                !DateTimeOffset.TryParse(
                    text,
                    CultureInfo.InvariantCulture,
                    DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal,
                    out timestamp))
            {
                throw new InvalidDataException($"CDC field '{fieldName}' is not a valid timestamp.");
            }
        }
        else if (value.ValueKind == JsonValueKind.Number && value.TryGetInt64(out var epochMicroseconds))
        {
            try
            {
                timestamp = DateTimeOffset.UnixEpoch.AddTicks(checked(epochMicroseconds * 10));
            }
            catch (ArgumentOutOfRangeException exception)
            {
                throw new InvalidDataException(
                    $"CDC field '{fieldName}' is outside the supported timestamp range.",
                    exception);
            }
            catch (OverflowException exception)
            {
                throw new InvalidDataException(
                    $"CDC field '{fieldName}' is outside the supported timestamp range.",
                    exception);
            }
        }
        else
        {
            throw new InvalidDataException(
                $"CDC field '{fieldName}' must be an ISO-8601 string or an integer epoch-microsecond logical value.");
        }

        if (timestamp.Year is < 1 or > 9999 || timestamp.UtcDateTime.Ticks % 10 != 0)
        {
            throw new InvalidDataException(
                $"CDC field '{fieldName}' must be representable at PostgreSQL microsecond precision.");
        }

        return timestamp.ToUniversalTime().ToString(
            "yyyy-MM-dd'T'HH:mm:ss.ffffff'Z'",
            CultureInfo.InvariantCulture);
    }

    private static CdcChange ParseVehicleState(
        JsonElement envelope,
        Guid keyVehicleId,
        CdcSourceMetadata source,
        string operation)
    {
        var before = GetRequiredProperty(envelope, "before");
        if ((operation is "c" or "r") && before.ValueKind != JsonValueKind.Null)
        {
            throw new InvalidDataException(
                $"Vehicle {operation} envelope must have a null 'before' value.");
        }

        var row = GetRequiredProperty(envelope, "after");
        RequireObject(row, "vehicle after row");
        var rowId = GetGuid(row, "id");
        EnsureMatchingKey(keyVehicleId, rowId, "vehicle id");

        var state = GetRequiredString(row, "state");
        if (!VehicleStates.Contains(state))
        {
            throw new InvalidDataException($"Unsupported vehicle state '{state}'.");
        }

        var stateVersion = GetPositiveInt64(row, "state_version");
        var stateUpdatedAt = NormalizeTimestamp(GetRequiredProperty(row, "state_updated_at"), "state_updated_at");
        return new CdcChange(
            CdcTable.Vehicles,
            CdcOperation.VehicleState,
            rowId,
            new VehicleStateChange(rowId, state, stateVersion, stateUpdatedAt),
            null,
            null,
            source);
    }

    private static CdcChange ParseVehicleUpdate(
        JsonElement envelope,
        Guid keyVehicleId,
        CdcSourceMetadata source)
    {
        var before = GetRequiredProperty(envelope, "before");
        RequireObject(before, "vehicle update before row");
        EnsureMatchingKey(keyVehicleId, GetGuid(before, "id"), "vehicle update before id");
        _ = GetPositiveInt64(before, "state_version");
        return ParseVehicleState(envelope, keyVehicleId, source, "u");
    }

    private static CdcChange ParseVehicleDelete(
        JsonElement envelope,
        Guid keyVehicleId,
        CdcSourceMetadata source)
    {
        var before = GetRequiredProperty(envelope, "before");
        RequireObject(before, "vehicle before row");
        var rowId = GetGuid(before, "id");
        EnsureMatchingKey(keyVehicleId, rowId, "deleted vehicle id");
        var stateVersion = GetPositiveInt64(before, "state_version");

        if (GetRequiredProperty(envelope, "after").ValueKind is not JsonValueKind.Null)
        {
            throw new InvalidDataException("Vehicle delete envelope must have a null 'after' value.");
        }

        return new CdcChange(
            CdcTable.Vehicles,
            CdcOperation.Delete,
            rowId,
            null,
            null,
            stateVersion,
            source);
    }

    private static CdcChange ParsePosition(
        JsonElement envelope,
        Guid keyVehicleId,
        CdcSourceMetadata source)
    {
        if (GetRequiredProperty(envelope, "before").ValueKind != JsonValueKind.Null)
        {
            throw new InvalidDataException("Position insert or snapshot envelope must have a null 'before' value.");
        }

        var row = GetRequiredProperty(envelope, "after");
        RequireObject(row, "position after row");
        var vehicleId = GetGuid(row, "vehicle_id");
        EnsureMatchingKey(keyVehicleId, vehicleId, "position vehicle_id");
        var positionId = GetGuid(row, "id");
        var latitude = GetFiniteDouble(row, "lat");
        var longitude = GetFiniteDouble(row, "lng");

        if (latitude is < -90 or > 90)
        {
            throw new InvalidDataException("Position latitude must be between -90 and 90.");
        }

        if (longitude is < -180 or > 180)
        {
            throw new InvalidDataException("Position longitude must be between -180 and 180.");
        }

        var recordedAt = NormalizeTimestamp(GetRequiredProperty(row, "recorded_at"), "recorded_at");
        return new CdcChange(
            CdcTable.Positions,
            CdcOperation.Position,
            vehicleId,
            null,
            new PositionChange(vehicleId, positionId, latitude, longitude, recordedAt),
            null,
            source);
    }

    private static CdcSourceMetadata ParseSource(JsonElement sourceElement, CdcTable expectedTable)
    {
        RequireObject(sourceElement, "Debezium source metadata");
        var database = GetRequiredString(sourceElement, "db");
        var schema = GetRequiredString(sourceElement, "schema");
        var table = GetRequiredString(sourceElement, "table");
        var expectedTableName = expectedTable == CdcTable.Vehicles ? "vehicles" : "positions";
        if (!string.Equals(schema, "public", StringComparison.Ordinal) ||
            !string.Equals(table, expectedTableName, StringComparison.Ordinal))
        {
            throw new InvalidDataException(
                $"CDC source metadata '{schema}.{table}' does not match the Kafka topic for {expectedTableName}.");
        }

        return new CdcSourceMetadata(
            database,
            schema,
            table,
            GetOptionalRaw(sourceElement, "lsn"),
            GetOptionalRaw(sourceElement, "txId"),
            GetOptionalRaw(sourceElement, "snapshot"));
    }

    private static CdcTable GetTable(string topic, string vehicleTopic, string positionTopic)
    {
        if (string.Equals(topic, vehicleTopic, StringComparison.Ordinal))
        {
            return CdcTable.Vehicles;
        }

        if (string.Equals(topic, positionTopic, StringComparison.Ordinal))
        {
            return CdcTable.Positions;
        }

        throw new InvalidDataException($"Received a message from unexpected topic '{topic}'.");
    }

    private static Guid ParseKey(byte[]? keyBytes, CdcTable table)
    {
        if (keyBytes is null)
        {
            throw new InvalidDataException("CDC record has no Kafka key.");
        }

        try
        {
            using var document = JsonDocument.Parse(keyBytes);
            RequireObject(document.RootElement, "Kafka key");
            var fieldName = table == CdcTable.Vehicles ? "id" : "vehicle_id";
            return GetGuid(document.RootElement, fieldName);
        }
        catch (JsonException exception)
        {
            throw new InvalidDataException("Kafka key is not valid JSON.", exception);
        }
    }

    private static string? GetOptionalRaw(JsonElement element, string propertyName)
    {
        if (!element.TryGetProperty(propertyName, out var value) || value.ValueKind == JsonValueKind.Null)
        {
            return null;
        }

        return value.ValueKind == JsonValueKind.String ? value.GetString() : value.GetRawText();
    }

    private static string GetRequiredString(JsonElement element, string propertyName)
    {
        var value = GetRequiredProperty(element, propertyName);
        if (value.ValueKind != JsonValueKind.String || string.IsNullOrWhiteSpace(value.GetString()))
        {
            throw new InvalidDataException($"CDC field '{propertyName}' must be a non-empty string.");
        }

        return value.GetString()!;
    }

    private static Guid GetGuid(JsonElement element, string propertyName)
    {
        var text = GetRequiredString(element, propertyName);
        if (!Guid.TryParse(text, out var value) || value == Guid.Empty)
        {
            throw new InvalidDataException($"CDC field '{propertyName}' must be a non-empty UUID.");
        }

        return value;
    }

    private static long GetPositiveInt64(JsonElement element, string propertyName)
    {
        var value = GetRequiredProperty(element, propertyName);
        if (value.ValueKind != JsonValueKind.Number ||
            !value.TryGetInt64(out var number) ||
            number <= 0)
        {
            throw new InvalidDataException($"CDC field '{propertyName}' must be a positive 64-bit integer.");
        }

        return number;
    }

    private static double GetFiniteDouble(JsonElement element, string propertyName)
    {
        var value = GetRequiredProperty(element, propertyName);
        if (value.ValueKind != JsonValueKind.Number ||
            !value.TryGetDouble(out var number) ||
            !double.IsFinite(number))
        {
            throw new InvalidDataException($"CDC field '{propertyName}' must be a finite number.");
        }

        return number;
    }

    private static JsonElement GetRequiredProperty(JsonElement element, string propertyName)
    {
        if (!element.TryGetProperty(propertyName, out var property))
        {
            throw new InvalidDataException($"CDC message is missing required field '{propertyName}'.");
        }

        return property;
    }

    private static void RequireObject(JsonElement element, string description)
    {
        if (element.ValueKind != JsonValueKind.Object)
        {
            throw new InvalidDataException($"{description} must be a JSON object.");
        }
    }

    private static void EnsureMatchingKey(Guid keyVehicleId, Guid rowVehicleId, string description)
    {
        if (keyVehicleId != rowVehicleId)
        {
            throw new InvalidDataException($"Kafka key does not match {description} in the CDC row.");
        }
    }
}
