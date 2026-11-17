using StackExchange.Redis;

namespace VehicleCache.Worker.Configuration;

public static class WorkerOptionsValidation
{
    public static bool IsValidKafka(KafkaOptions options) =>
        !string.IsNullOrWhiteSpace(options.BootstrapServers) &&
        !string.IsNullOrWhiteSpace(options.VehicleTopic) &&
        !string.IsNullOrWhiteSpace(options.PositionTopic) &&
        !string.Equals(options.VehicleTopic, options.PositionTopic, StringComparison.Ordinal) &&
        !string.IsNullOrWhiteSpace(options.ConsumerGroup) &&
        options.SessionTimeoutMs is >= 1_000 and <= 60_000 &&
        options.MaxPollIntervalMs >= options.SessionTimeoutMs &&
        options.CommitTimeoutMs is >= 1_000 and <= 60_000 &&
        options.ExpectedPartitionCount is >= 1 and <= 64 &&
        options.ReplayBaselineOffset >= 0;

    public static bool IsValidValkey(ValkeyOptions options)
    {
        if (options.OperationTimeoutMs is < 100 or > 60_000 ||
            options.RetryCount is < 0 or > 10 ||
            options.RetryDelayMs is < 0 or > 10_000 ||
            string.IsNullOrWhiteSpace(options.ConnectionString))
        {
            return false;
        }

        try
        {
            _ = ConfigurationOptions.Parse(options.ConnectionString);
            return true;
        }
        catch (Exception exception) when (exception is ArgumentException or FormatException)
        {
            return false;
        }
    }
}
