using Confluent.Kafka;
using Microsoft.Extensions.Logging.Abstractions;
using VehicleCache.Worker.Consumers;

namespace VehicleCache.UnitTests;

public sealed class KafkaCommitRecoveryTests
{
    private static readonly TopicPartition Partition =
        new("vehicle_cdc.public.vehicles", new Partition(1));

    [Fact]
    public async Task RetriableCommitFailureRetriesSameOffsetWithoutReapplyingProjection()
    {
        var operations = new List<string> { "project" };
        var attempts = 0;

        await CdcWorker.CommitWithRetriesAsync(
            Partition,
            () => true,
            () =>
            {
                operations.Add("commit");
                attempts++;
                if (attempts == 1)
                {
                    throw new KafkaException(new Error(ErrorCode.NotCoordinatorForGroup));
                }
            },
            NullLogger<CdcWorker>.Instance,
            CancellationToken.None);

        Assert.Equal(["project", "commit", "commit"], operations);
    }

    [Fact]
    public async Task CommitStopsRetryingWhenPartitionOwnershipIsLost()
    {
        var ownsPartition = true;
        var attempts = 0;

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            CdcWorker.CommitWithRetriesAsync(
                Partition,
                () => ownsPartition,
                () =>
                {
                    attempts++;
                    ownsPartition = false;
                    throw new KafkaException(new Error(ErrorCode.NotCoordinatorForGroup));
                },
                NullLogger<CdcWorker>.Instance,
                CancellationToken.None));

        Assert.Equal(1, attempts);
    }

    [Fact]
    public async Task PermanentCommitFailureIsNotRetried()
    {
        var attempts = 0;

        await Assert.ThrowsAsync<KafkaException>(() =>
            CdcWorker.CommitWithRetriesAsync(
                Partition,
                () => true,
                () =>
                {
                    attempts++;
                    throw new KafkaException(new Error(ErrorCode.IllegalGeneration));
                },
                NullLogger<CdcWorker>.Instance,
                CancellationToken.None));

        Assert.Equal(1, attempts);
    }
}
