namespace VehicleCache.Worker.Configuration;

public sealed class ValkeyOptions
{
    public const string SectionName = "Valkey";

    public string ConnectionString { get; set; } = string.Empty;
    public int OperationTimeoutMs { get; set; } = 5_000;
    public int RetryCount { get; set; } = 3;
    public int RetryDelayMs { get; set; } = 250;
}
