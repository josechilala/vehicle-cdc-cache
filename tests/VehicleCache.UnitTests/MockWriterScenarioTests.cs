using PositionWriter.Configuration;
using PositionWriter.Scenarios;
using VehicleWriter.Configuration;
using VehicleWriter.Models;
using VehicleWriter.Scenarios;

namespace VehicleCache.UnitTests;

public sealed class MockWriterScenarioTests
{
    [Fact]
    public void VehicleBaselineAssignsVersionsBeforeTheStaleFaultInjection()
    {
        var steps = VehicleScenario.BuildBaseline();
        var currentVehicleSteps = steps
            .Where(step => step.VehicleId == VehicleScenario.CurrentVehicleId)
            .ToArray();

        Assert.Collection(
            currentVehicleSteps,
            create => AssertTransition(create, VehicleScenarioOperation.Create, 1, VehicleState.REGISTERED),
            update => AssertTransition(update, VehicleScenarioOperation.Update, 2, VehicleState.ACTIVE),
            update => AssertTransition(update, VehicleScenarioOperation.Update, 3, VehicleState.IDLE),
            stale => AssertTransition(stale, VehicleScenarioOperation.Update, 2, VehicleState.ACTIVE),
            injection => AssertTransition(injection, VehicleScenarioOperation.InjectStaleUpdate, 2, VehicleState.ACTIVE));
        Assert.True(
            currentVehicleSteps[2].Transition!.StateUpdatedAt <
            currentVehicleSteps[1].Transition!.StateUpdatedAt);

        var deletedVehicleSteps = steps
            .Where(step => step.VehicleId == VehicleScenario.DeletedVehicleId)
            .ToArray();
        Assert.Collection(
            deletedVehicleSteps,
            create => AssertTransition(create, VehicleScenarioOperation.Create, 1, VehicleState.REGISTERED),
            update => AssertTransition(update, VehicleScenarioOperation.Update, 2, VehicleState.DECOMMISSIONED),
            delete =>
            {
                Assert.Equal(VehicleScenarioOperation.Delete, delete.Operation);
                Assert.Null(delete.Transition);
            });
    }

    [Fact]
    public void PositionFixturesCoverOutOfOrderTiesRetriesAndPositionBeforeCreation()
    {
        var positions = PositionScenario.Build("before-vehicles");
        var currentVehiclePositions = positions
            .Where(position => position.VehicleId == PositionScenario.CurrentVehicleId)
            .ToArray();

        Assert.Equal(4, currentVehiclePositions.Length);
        Assert.True(currentVehiclePositions[0].RecordedAt > currentVehiclePositions[1].RecordedAt);
        Assert.Equal(currentVehiclePositions[0].RecordedAt, currentVehiclePositions[2].RecordedAt);
        Assert.True(
            StringComparer.Ordinal.Compare(
                currentVehiclePositions[2].PositionId.ToString("D").ToLowerInvariant(),
                currentVehiclePositions[0].PositionId.ToString("D").ToLowerInvariant()) > 0);
        Assert.Equal(currentVehiclePositions[2], currentVehiclePositions[3]);
        Assert.Contains(
            positions,
            position => position.VehicleId == PositionScenario.PositionBeforeCreationVehicleId);
        Assert.Equal(
            VehicleScenario.PositionBeforeCreationVehicleId,
            PositionScenario.PositionBeforeCreationVehicleId);
    }

    [Fact]
    public void ConflictingDuplicateScenarioReusesIdWithDifferentContent()
    {
        var original = PositionScenario.Build("before-vehicles")
            .First(position => position.PositionId == Guid.Parse("20000000-0000-4000-8000-000000000020"));
        var conflicting = Assert.Single(PositionScenario.Build("conflicting-duplicate"));

        Assert.Equal(original.PositionId, conflicting.PositionId);
        Assert.NotEqual(original, conflicting);
    }

    [Fact]
    public void VehicleAndPositionScenariosShareStableVehicleIdentifiers()
    {
        Assert.Equal(VehicleScenario.CurrentVehicleId, PositionScenario.CurrentVehicleId);
        Assert.Equal(VehicleScenario.DeletedVehicleId, PositionScenario.DeletedVehicleId);
        Assert.Equal(
            VehicleScenario.PositionBeforeCreationVehicleId,
            PositionScenario.PositionBeforeCreationVehicleId);

        var latePosition = Assert.Single(PositionScenario.Build("after-delete"));
        Assert.Equal(VehicleScenario.DeletedVehicleId, latePosition.VehicleId);
    }

    [Fact]
    public void WriterConfigurationsReadConnectionStringAndValidateRateAndScenario()
    {
        var vehicleOptions = VehicleWriterOptions.Parse(
            ["--mode", "deterministic", "--scenario", "baseline", "--rate", "25"],
            key => key == "VEHICLE_CACHE_CONNECTION_STRING" ? ValidConnectionString : null);
        var positionOptions = PositionWriterOptions.Parse(
            ["--mode", "deterministic", "--scenario", "after-delete", "--rate", "5"],
            key => key == "VEHICLE_CACHE_CONNECTION_STRING" ? ValidConnectionString : null);

        Assert.Equal(25, vehicleOptions.OperationsPerSecond);
        Assert.Equal(5, positionOptions.OperationsPerSecond);
        Assert.Equal("after-delete", positionOptions.Scenario);
        Assert.Throws<ArgumentException>(() =>
            VehicleWriterOptions.Parse(["--rate", "0"], _ => ValidConnectionString));
        Assert.Throws<ArgumentException>(() =>
            PositionWriterOptions.Parse(["--scenario", "unknown"], _ => ValidConnectionString));
        Assert.Throws<ArgumentException>(() =>
            VehicleWriterOptions.Parse([], _ => null));
    }

    private static void AssertTransition(
        VehicleScenarioStep step,
        VehicleScenarioOperation operation,
        long version,
        VehicleState state)
    {
        Assert.Equal(operation, step.Operation);
        Assert.NotNull(step.Transition);
        Assert.Equal(version, step.Transition.StateVersion);
        Assert.Equal(state, step.Transition.State);
        Assert.Equal(TimeSpan.Zero, step.Transition.StateUpdatedAt.Offset);
        Assert.Equal(0, step.Transition.StateUpdatedAt.UtcDateTime.Ticks % 10);
    }

    private const string ValidConnectionString =
        "Host=localhost;Port=5432;Database=vehicle_cache;Username=vehicle_writer;Password=test-only";
}
