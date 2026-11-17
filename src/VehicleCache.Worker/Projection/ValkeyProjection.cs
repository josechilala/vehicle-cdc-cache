using System.Globalization;
using System.Reflection;
using System.Text;
using Confluent.Kafka;
using StackExchange.Redis;
using VehicleCache.Worker.Configuration;
using VehicleCache.Worker.Models;
using Microsoft.Extensions.Options;

namespace VehicleCache.Worker.Projection;

public sealed class ValkeyProjection
{
    private static readonly string ProjectionScript = LoadProjectionScript();
    private readonly IDatabase _database;
    private readonly string _progressKey;

    public ValkeyProjection(
        IConnectionMultiplexer connectionMultiplexer,
        IOptions<KafkaOptions> kafkaOptions)
    {
        _database = connectionMultiplexer.GetDatabase();
        _progressKey = $"vehicle-cache:progress:{kafkaOptions.Value.ConsumerGroup}";
    }

    public async Task<int> ApplyAsync(CdcDelivery delivery)
    {
        if (delivery.Offset < 0 || delivery.Partition < 0)
        {
            throw new InvalidDataException("Kafka delivery has a negative partition or offset.");
        }

        var nextOffset = checked(delivery.Offset + 1)
            .ToString(CultureInfo.InvariantCulture);
        var progressField = $"{delivery.Topic}|{delivery.Partition.ToString(CultureInfo.InvariantCulture)}";
        var keys = new RedisKey[]
        {
            VehicleKey(delivery.Change.VehicleId),
            _progressKey
        };
        var arguments = BuildArguments(delivery, nextOffset, progressField);
        var result = await _database.ScriptEvaluateAsync(ProjectionScript, keys, arguments);
        return checked((int)(long)result);
    }

    public async Task<long?> GetProcessedNextOffsetAsync(string topic, int partition)
    {
        var field = $"{topic}|{partition.ToString(CultureInfo.InvariantCulture)}";
        var value = await _database.HashGetAsync(_progressKey, field);
        if (value.IsNull)
        {
            return null;
        }

        if (!long.TryParse(
                value.ToString(),
                NumberStyles.None,
                CultureInfo.InvariantCulture,
                out var offset) ||
            offset < 0)
        {
            throw new InvalidDataException(
                $"Valkey progress cursor for {topic}[{partition}] is invalid.");
        }

        return offset;
    }

    public static string VehicleKey(Guid vehicleId) =>
        $"vehicle:{{{vehicleId.ToString("D").ToLowerInvariant()}}}";

    private static RedisValue[] BuildArguments(
        CdcDelivery delivery,
        string nextOffset,
        string progressField)
    {
        var change = delivery.Change;
        var arguments = new List<RedisValue>
        {
            OperationName(change.Operation),
            nextOffset,
            progressField
        };

        switch (change.Operation)
        {
            case CdcOperation.VehicleState:
                var vehicle = change.Vehicle
                    ?? throw new InvalidDataException("Vehicle CDC operation has no state candidate.");
                arguments.Add(vehicle.StateVersion.ToString(CultureInfo.InvariantCulture));
                arguments.Add(vehicle.State);
                arguments.Add(vehicle.StateUpdatedAt);
                break;
            case CdcOperation.Position:
                var position = change.Position
                    ?? throw new InvalidDataException("Position CDC operation has no position candidate.");
                arguments.Add(position.PositionId.ToString("D").ToLowerInvariant());
                arguments.Add(position.RecordedAt);
                arguments.Add(position.Latitude.ToString("R", CultureInfo.InvariantCulture));
                arguments.Add(position.Longitude.ToString("R", CultureInfo.InvariantCulture));
                break;
            case CdcOperation.Delete:
                var deleteVersion = change.DeleteStateVersion
                    ?? throw new InvalidDataException("Vehicle delete operation has no before-image version.");
                arguments.Add(deleteVersion.ToString(CultureInfo.InvariantCulture));
                break;
            case CdcOperation.Tombstone:
                break;
            default:
                throw new InvalidDataException($"Unsupported projection operation '{change.Operation}'.");
        }

        return arguments.ToArray();
    }

    private static string OperationName(CdcOperation operation) =>
        operation switch
        {
            CdcOperation.VehicleState => "vehicle",
            CdcOperation.Position => "position",
            CdcOperation.Delete => "delete",
            CdcOperation.Tombstone => "tombstone",
            _ => throw new ArgumentOutOfRangeException(nameof(operation))
        };

    private static string LoadProjectionScript()
    {
        var assembly = Assembly.GetExecutingAssembly();
        const string resourceName = "VehicleCache.Worker.Projection.vehicle-projection.lua";
        using var resource = assembly.GetManifestResourceStream(resourceName)
            ?? throw new InvalidOperationException($"Embedded Valkey script '{resourceName}' was not found.");
        using var reader = new StreamReader(resource, Encoding.UTF8);
        return reader.ReadToEnd();
    }
}
