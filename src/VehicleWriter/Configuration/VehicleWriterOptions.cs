using System.Globalization;
using Npgsql;

namespace VehicleWriter.Configuration;

public enum WriterExecutionMode
{
    Deterministic,
    Continuous
}

public sealed record VehicleWriterOptions(
    string ConnectionString,
    WriterExecutionMode Mode,
    double OperationsPerSecond,
    TimeSpan OperationInterval)
{
    public const string Usage =
        "VehicleWriter [--connection-string <value>] [--mode deterministic|continuous] " +
        "[--rate <operations-per-second>] [--scenario baseline]";

    public static VehicleWriterOptions Parse(
        string[] args,
        Func<string, string?>? environmentValue = null)
    {
        environmentValue ??= Environment.GetEnvironmentVariable;
        var values = ParseArguments(args);
        var connectionString = values.TryGetValue("connection-string", out var argumentConnectionString)
            ? argumentConnectionString
            : environmentValue("VEHICLE_CACHE_CONNECTION_STRING");
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            throw new ArgumentException(
                "Provide --connection-string or set VEHICLE_CACHE_CONNECTION_STRING.");
        }

        try
        {
            _ = new NpgsqlConnectionStringBuilder(connectionString);
        }
        catch (ArgumentException)
        {
            throw new ArgumentException("The PostgreSQL connection string is invalid.");
        }

        var modeValue = GetValue(values, "mode", "deterministic");
        var mode = modeValue.ToLowerInvariant() switch
        {
            "deterministic" => WriterExecutionMode.Deterministic,
            "continuous" => WriterExecutionMode.Continuous,
            _ => throw new ArgumentException("--mode must be deterministic or continuous.")
        };

        var scenario = GetValue(values, "scenario", mode == WriterExecutionMode.Deterministic ? "baseline" : "stream");
        var expectedScenario = mode == WriterExecutionMode.Deterministic ? "baseline" : "stream";
        if (!string.Equals(scenario, expectedScenario, StringComparison.OrdinalIgnoreCase))
        {
            throw new ArgumentException(
                $"Scenario '{scenario}' is not supported for {mode.ToString().ToLowerInvariant()} mode. " +
                $"Use '{expectedScenario}'.");
        }

        var rateValue = GetValue(values, "rate", "10");
        if (!double.TryParse(rateValue, NumberStyles.Float, CultureInfo.InvariantCulture, out var rate) ||
            !double.IsFinite(rate) ||
            rate is < 0.01 or > 1000)
        {
            throw new ArgumentException("--rate must be a number between 0.01 and 1000 operations per second.");
        }

        return new VehicleWriterOptions(
            connectionString,
            mode,
            rate,
            TimeSpan.FromSeconds(1 / rate));
    }

    private static Dictionary<string, string> ParseArguments(string[] args)
    {
        var values = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        for (var index = 0; index < args.Length; index++)
        {
            var argument = args[index];
            if (!argument.StartsWith("--", StringComparison.Ordinal) || index + 1 >= args.Length)
            {
                throw new ArgumentException($"Invalid command-line option '{argument}'. Expected --name value.");
            }

            var name = argument[2..];
            if (name is not ("connection-string" or "mode" or "rate" or "scenario"))
            {
                throw new ArgumentException($"Unknown command-line option '{argument}'.");
            }

            if (!values.TryAdd(name, args[++index]))
            {
                throw new ArgumentException($"Command-line option '{argument}' was specified more than once.");
            }
        }

        return values;
    }

    private static string GetValue(
        IReadOnlyDictionary<string, string> values,
        string name,
        string? defaultValue) =>
        values.TryGetValue(name, out var value) ? value : defaultValue
            ?? throw new ArgumentException($"Missing value for --{name}.");
}
