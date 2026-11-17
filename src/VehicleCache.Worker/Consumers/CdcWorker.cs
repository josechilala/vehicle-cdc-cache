using Confluent.Kafka;
using Microsoft.Extensions.Options;
using StackExchange.Redis;
using VehicleCache.Worker.Configuration;
using VehicleCache.Worker.Models;
using VehicleCache.Worker.Projection;

namespace VehicleCache.Worker.Consumers;

public sealed class CdcWorker(
    ILogger<CdcWorker> logger,
    IOptions<KafkaOptions> kafkaOptions,
    IOptions<ValkeyOptions> valkeyOptions,
    CdcEnvelopeParser parser,
    ValkeyProjection projection) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var options = kafkaOptions.Value;
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = options.BootstrapServers,
            GroupId = options.ConsumerGroup,
            EnableAutoCommit = false,
            EnableAutoOffsetStore = false,
            AutoOffsetReset = AutoOffsetReset.Error,
            AllowAutoCreateTopics = false,
            SessionTimeoutMs = options.SessionTimeoutMs,
            MaxPollIntervalMs = options.MaxPollIntervalMs,
            SocketTimeoutMs = options.CommitTimeoutMs
        };

        var assignedPartitions = new HashSet<TopicPartition>();
        var verifiedPartitions = new HashSet<TopicPartition>();
        var assignmentChanged = false;
        using var consumer = new ConsumerBuilder<byte[], byte[]>(consumerConfig)
            .SetKeyDeserializer(Deserializers.ByteArray)
            .SetValueDeserializer(Deserializers.ByteArray)
            .SetPartitionsAssignedHandler((assignedConsumer, partitions) =>
            {
                foreach (var partition in partitions)
                {
                    assignedPartitions.Add(partition);
                    logger.LogInformation(
                        "Kafka partition assigned. Topic={Topic} Partition={Partition}",
                        partition.Topic,
                        partition.Partition.Value);
                }

                VerifyReplayBaseline(assignedConsumer, options);
                assignmentChanged = true;
                return partitions
                    .Select(partition => new TopicPartitionOffset(
                        partition,
                        new Offset(options.ReplayBaselineOffset)))
                    .ToList();
            })
            .SetPartitionsRevokedHandler((_, partitions) =>
            {
                foreach (var assignment in partitions)
                {
                    var partition = assignment.TopicPartition;
                    assignedPartitions.Remove(partition);
                    verifiedPartitions.Remove(partition);
                    logger.LogInformation(
                        "Kafka partition revoked. Topic={Topic} Partition={Partition}",
                        partition.Topic,
                        partition.Partition.Value);
                }

                assignmentChanged = true;
            })
            .SetPartitionsLostHandler((_, partitions) =>
            {
                foreach (var assignment in partitions)
                {
                    var partition = assignment.TopicPartition;
                    assignedPartitions.Remove(partition);
                    verifiedPartitions.Remove(partition);
                    logger.LogWarning(
                        "Kafka partition lost before a safe commit. Topic={Topic} Partition={Partition}",
                        partition.Topic,
                        partition.Partition.Value);
                }

                assignmentChanged = true;
            })
            .Build();

        consumer.Subscribe([options.VehicleTopic, options.PositionTopic]);
        logger.LogInformation(
            "CDC worker started. VehicleTopic={VehicleTopic} PositionTopic={PositionTopic} ConsumerGroup={ConsumerGroup} Valkey={ValkeyEndpoint}",
            options.VehicleTopic,
            options.PositionTopic,
            options.ConsumerGroup,
            SafeEndpoint(valkeyOptions.Value.ConnectionString));

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                ConsumeResult<byte[], byte[]>? result;
                try
                {
                    result = consumer.Consume(TimeSpan.FromMilliseconds(500));
                }
                catch (ConsumeException exception)
                {
                    logger.LogError(exception, "Kafka consume failed. Error={KafkaError}", exception.Error);
                    throw;
                }

                if (assignmentChanged)
                {
                    await VerifyAssignedPartitionsAsync(
                        consumer,
                        assignedPartitions,
                        verifiedPartitions,
                        options,
                        stoppingToken);
                    assignmentChanged = false;
                    result = null;
                }

                if (result is null)
                {
                    continue;
                }

                if (!verifiedPartitions.Contains(result.TopicPartition))
                {
                    throw new InvalidOperationException(
                        $"Kafka partition {result.TopicPartition} has not passed cache progress validation.");
                }

                await ProcessMessageAsync(consumer, result, options, stoppingToken);
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            logger.LogInformation("CDC worker cancellation requested.");
        }
        catch (Exception exception)
        {
            Environment.ExitCode = 1;
            logger.LogCritical(exception, "CDC worker stopped after an unrecoverable processing failure.");
            throw;
        }
        finally
        {
            consumer.Close();
            logger.LogInformation("CDC worker stopped.");
        }
    }

    private async Task ProcessMessageAsync(
        IConsumer<byte[], byte[]> consumer,
        ConsumeResult<byte[], byte[]> result,
        KafkaOptions options,
        CancellationToken cancellationToken)
    {
        CdcChange change;
        try
        {
            change = parser.Parse(
                result.Message.Key,
                result.Message.Value,
                result.Topic,
                options.VehicleTopic,
                options.PositionTopic);
        }
        catch (InvalidDataException exception)
        {
            logger.LogCritical(
                exception,
                "Invalid CDC message blocks its Kafka partition. Topic={Topic} Partition={Partition} Offset={Offset}",
                result.Topic,
                result.Partition.Value,
                result.Offset.Value);
            throw;
        }

        var delivery = new CdcDelivery(
            change,
            result.Topic,
            result.Partition.Value,
            result.Offset.Value);

        var status = await ApplyWithBoundedRetriesAsync(delivery, cancellationToken);
        LogProjectionResult(delivery, status);

        try
        {
            await CommitWithRetriesAsync(
                result.TopicPartition,
                () => consumer.Assignment.Contains(result.TopicPartition),
                () => consumer.Commit(result),
                logger,
                cancellationToken);
        }
        catch (KafkaException exception)
        {
            logger.LogError(
                exception,
                "Kafka offset commit failed after Valkey projection; the message will be replayed. Topic={Topic} Partition={Partition} Offset={Offset}",
                result.Topic,
                result.Partition.Value,
                result.Offset.Value);
            throw;
        }

    }

    internal static async Task CommitWithRetriesAsync(
        TopicPartition partition,
        Func<bool> ownsPartition,
        Action commit,
        ILogger logger,
        CancellationToken cancellationToken)
    {
        const int maxAttempts = 3;
        var attempt = 1;
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!ownsPartition())
            {
                throw new InvalidOperationException(
                    $"Kafka partition {partition} is no longer assigned; its projected offset was not committed.");
            }

            try
            {
                commit();
                return;
            }
            catch (KafkaException exception)
                when (IsTransientCommitFailure(exception) && attempt < maxAttempts)
            {
                logger.LogWarning(
                    exception,
                    "Kafka offset commit failed with a retriable error; retrying the same projected offset. " +
                    "Topic={Topic} Partition={Partition} Attempt={Attempt} MaxAttempts={MaxAttempts}",
                    partition.Topic,
                    partition.Partition.Value,
                    attempt,
                    maxAttempts);
                await Task.Delay(TimeSpan.FromMilliseconds(250 * attempt), cancellationToken);
                attempt++;
            }
        }
    }

    private static bool IsTransientCommitFailure(KafkaException exception) =>
        exception is TopicPartitionOffsetException partitionException
            ? partitionException.Results.Any(result => result.Error.IsError) &&
              partitionException.Results
                  .Where(result => result.Error.IsError)
                  .All(result => IsTransientCommitError(result.Error.Code))
            : IsTransientCommitError(exception.Error.Code);

    private static bool IsTransientCommitError(ErrorCode errorCode) =>
        errorCode is ErrorCode.NotCoordinatorForGroup
            or ErrorCode.GroupCoordinatorNotAvailable
            or ErrorCode.RequestTimedOut
            or ErrorCode.NetworkException
            or ErrorCode.BrokerNotAvailable
            or ErrorCode.Local_Transport
            or ErrorCode.Local_AllBrokersDown
            or ErrorCode.Local_TimedOut
            or ErrorCode.Local_TimedOutQueue;

    private async Task<int> ApplyWithBoundedRetriesAsync(
        CdcDelivery delivery,
        CancellationToken cancellationToken)
    {
        var settings = valkeyOptions.Value;
        for (var attempt = 0; ; attempt++)
        {
            try
            {
                return await projection.ApplyAsync(delivery);
            }
            catch (RedisException exception) when (attempt < settings.RetryCount)
            {
                logger.LogWarning(
                    exception,
                    "Valkey projection attempt failed; retrying. Topic={Topic} Partition={Partition} Offset={Offset} Attempt={Attempt}",
                    delivery.Topic,
                    delivery.Partition,
                    delivery.Offset,
                    attempt + 1);
                await Task.Delay(settings.RetryDelayMs, cancellationToken);
            }
            catch (RedisException exception)
            {
                logger.LogError(
                    exception,
                    "Valkey projection failed after bounded retries. Topic={Topic} Partition={Partition} Offset={Offset}",
                    delivery.Topic,
                    delivery.Partition,
                    delivery.Offset);
                throw;
            }
        }
    }

    private async Task VerifyAssignedPartitionsAsync(
        IConsumer<byte[], byte[]> consumer,
        IReadOnlyCollection<TopicPartition> assigned,
        HashSet<TopicPartition> verified,
        KafkaOptions options,
        CancellationToken cancellationToken)
    {
        var newAssignments = assigned.Where(partition => !verified.Contains(partition)).ToArray();
        if (newAssignments.Length == 0)
        {
            return;
        }

        var committedResults = consumer.Committed(
            newAssignments,
            TimeSpan.FromMilliseconds(options.CommitTimeoutMs));
        if (committedResults.Count != newAssignments.Length)
        {
            throw new InvalidOperationException(
                "Kafka did not return a committed-offset result for every newly assigned partition.");
        }

        var committedOffsets = committedResults.ToDictionary(result => result.TopicPartition);
        foreach (var committed in committedOffsets)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var partition = committed.Key;
            if (!newAssignments.Contains(partition))
            {
                throw new InvalidOperationException(
                    $"Kafka returned an unexpected committed offset for {partition}.");
            }

            var watermarks = consumer.QueryWatermarkOffsets(
                partition,
                TimeSpan.FromMilliseconds(options.CommitTimeoutMs));
            if (watermarks.Low.Value != options.ReplayBaselineOffset)
            {
                throw new InvalidOperationException(
                    $"Kafka replay baseline for {partition} is unavailable: earliest offset is " +
                    $"{watermarks.Low.Value}, expected {options.ReplayBaselineOffset}.");
            }

            var committedOffset = committed.Value.Offset.Value;
            if (committedOffset < 0 && committedOffset != Offset.Unset.Value)
            {
                throw new InvalidOperationException(
                    $"Kafka returned invalid committed offset {committedOffset} for {partition}.");
            }

            var cacheOffset = await projection.GetProcessedNextOffsetAsync(
                partition.Topic,
                partition.Partition.Value);

            if (cacheOffset is not null &&
                (cacheOffset.Value < watermarks.Low.Value || cacheOffset.Value > watermarks.High.Value))
            {
                throw new InvalidOperationException(
                    $"Valkey progress cursor {cacheOffset.Value} is outside Kafka's retained range " +
                    $"[{watermarks.Low.Value}, {watermarks.High.Value}] for {partition}. " +
                    "Offline replay is required.");
            }

            if (committedOffset < 0)
            {
                consumer.Seek(new TopicPartitionOffset(partition, new Offset(options.ReplayBaselineOffset)));
                verified.Add(partition);
                continue;
            }

            if (committedOffset < watermarks.Low.Value || committedOffset > watermarks.High.Value)
            {
                throw new InvalidOperationException(
                    $"Kafka committed offset {committedOffset} is outside the retained range " +
                    $"[{watermarks.Low.Value}, {watermarks.High.Value}] for {partition}. " +
                    "Required CDC history may be unavailable; offline replay is required.");
            }

            if (cacheOffset is null && committedOffset > options.ReplayBaselineOffset)
            {
                throw new InvalidOperationException(
                    $"Valkey projection progress is missing for {partition} " +
                    $"while Kafka has committed offset {committedOffset}. Cache loss requires offline replay.");
            }

            if (cacheOffset is not null && cacheOffset.Value < committedOffset)
            {
                throw new InvalidOperationException(
                    $"Valkey projection cursor {cacheOffset.Value} is behind Kafka committed offset " +
                    $"{committedOffset} for {partition}. Cache recovery is required.");
            }

            consumer.Seek(new TopicPartitionOffset(partition, new Offset(committedOffset)));
            verified.Add(partition);
        }
    }

    private void VerifyReplayBaseline(IConsumer<byte[], byte[]> consumer, KafkaOptions options)
    {
        using var adminClient = new AdminClientBuilder(new AdminClientConfig
        {
            BootstrapServers = options.BootstrapServers
        }).Build();

        foreach (var topic in new[] { options.VehicleTopic, options.PositionTopic })
        {
            var metadata = adminClient.GetMetadata(topic, TimeSpan.FromMilliseconds(options.CommitTimeoutMs));
            var topicMetadata = metadata.Topics.SingleOrDefault(candidate =>
                string.Equals(candidate.Topic, topic, StringComparison.Ordinal));
            if (topicMetadata is null || topicMetadata.Error.IsError)
            {
                throw new InvalidOperationException(
                    $"Kafka topic '{topic}' is unavailable while validating the CDC replay baseline.");
            }

            if (topicMetadata.Partitions.Count != options.ExpectedPartitionCount)
            {
                throw new InvalidOperationException(
                    $"Kafka topic '{topic}' has {topicMetadata.Partitions.Count} partitions; " +
                    $"the configured replay baseline expects {options.ExpectedPartitionCount}.");
            }

            foreach (var partitionMetadata in topicMetadata.Partitions)
            {
                if (partitionMetadata.Error.IsError)
                {
                    throw new KafkaException(partitionMetadata.Error);
                }

                var partition = new TopicPartition(topic, new Partition(partitionMetadata.PartitionId));
                var watermarks = consumer.QueryWatermarkOffsets(
                    partition,
                    TimeSpan.FromMilliseconds(options.CommitTimeoutMs));
                if (watermarks.Low.Value != options.ReplayBaselineOffset)
                {
                    throw new InvalidOperationException(
                        $"Kafka replay baseline for {topic}[{partition.Partition.Value}] is unavailable: " +
                        $"earliest offset is {watermarks.Low.Value}, expected {options.ReplayBaselineOffset}. " +
                        "Do not start projection after required CDC history has expired.");
                }
            }
        }

        logger.LogInformation(
            "Kafka replay baseline verified. Topics={VehicleTopic},{PositionTopic} PartitionCount={PartitionCount} BaselineOffset={BaselineOffset}",
            options.VehicleTopic,
            options.PositionTopic,
            options.ExpectedPartitionCount,
            options.ReplayBaselineOffset);
    }

    private void LogProjectionResult(CdcDelivery delivery, int status)
    {
        switch (status)
        {
            case 0:
                logger.LogInformation(
                    "CDC event was stale or an idempotent duplicate. Topic={Topic} Partition={Partition} Offset={Offset} Operation={Operation}",
                    delivery.Topic,
                    delivery.Partition,
                    delivery.Offset,
                    delivery.Change.Operation);
                break;
            case 1:
                logger.LogInformation(
                    "CDC event projected. Topic={Topic} Partition={Partition} Offset={Offset} Operation={Operation} VehicleId={VehicleId}",
                    delivery.Topic,
                    delivery.Partition,
                    delivery.Offset,
                    delivery.Change.Operation,
                    delivery.Change.VehicleId);
                break;
            case 2:
                logger.LogInformation(
                    "Vehicle deletion projected. Topic={Topic} Partition={Partition} Offset={Offset} VehicleId={VehicleId} StateVersion={StateVersion}",
                    delivery.Topic,
                    delivery.Partition,
                    delivery.Offset,
                    delivery.Change.VehicleId,
                    delivery.Change.DeleteStateVersion);
                break;
            case 3:
                logger.LogDebug(
                    "Kafka tombstone recorded without changing vehicle state. Topic={Topic} Partition={Partition} Offset={Offset} VehicleId={VehicleId}",
                    delivery.Topic,
                    delivery.Partition,
                    delivery.Offset,
                    delivery.Change.VehicleId);
                break;
            case 4:
                logger.LogInformation(
                    "CDC event ignored because vehicle deletion is terminal. Topic={Topic} Partition={Partition} Offset={Offset} Operation={Operation} VehicleId={VehicleId}",
                    delivery.Topic,
                    delivery.Partition,
                    delivery.Offset,
                    delivery.Change.Operation,
                    delivery.Change.VehicleId);
                break;
            default:
                throw new InvalidOperationException($"Valkey Lua script returned unknown status {status}.");
        }
    }

    private static string SafeEndpoint(string connectionString)
    {
        try
        {
            var configuration = ConfigurationOptions.Parse(connectionString);
            return string.Join(",", configuration.EndPoints);
        }
        catch (Exception exception) when (exception is ArgumentException or FormatException)
        {
            return "configured";
        }
    }
}
