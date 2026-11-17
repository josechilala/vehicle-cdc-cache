namespace VehicleCache.Worker.Configuration;

public sealed class KafkaOptions
{
    public const string SectionName = "Kafka";

    public string BootstrapServers { get; set; } = string.Empty;
    public string VehicleTopic { get; set; } = string.Empty;
    public string PositionTopic { get; set; } = string.Empty;
    public string ConsumerGroup { get; set; } = string.Empty;
    public int SessionTimeoutMs { get; set; } = 10_000;
    public int MaxPollIntervalMs { get; set; } = 300_000;
    public int CommitTimeoutMs { get; set; } = 10_000;
    public int ExpectedPartitionCount { get; set; } = 3;
    public long ReplayBaselineOffset { get; set; }
}
