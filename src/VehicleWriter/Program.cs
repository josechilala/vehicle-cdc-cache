using Microsoft.Extensions.Logging;
using Npgsql;
using VehicleWriter.Configuration;
using VehicleWriter.Infrastructure;
using VehicleWriter.Models;
using VehicleWriter.Scenarios;

using var loggerFactory = LoggerFactory.Create(builder =>
    builder.AddSimpleConsole(options => options.SingleLine = true));
var logger = loggerFactory.CreateLogger("VehicleWriter");

if (args.Contains("--help", StringComparer.OrdinalIgnoreCase) ||
    args.Contains("-h", StringComparer.OrdinalIgnoreCase))
{
    Console.WriteLine(VehicleWriterOptions.Usage);
    return 0;
}

using var cancellationSource = new CancellationTokenSource();
Console.CancelKeyPress += (_, eventArgs) =>
{
    eventArgs.Cancel = true;
    cancellationSource.Cancel();
};

try
{
    var options = VehicleWriterOptions.Parse(args);
    await using var dataSource = NpgsqlDataSource.Create(options.ConnectionString);
    var store = new PostgresVehicleStore(dataSource, logger);

    if (options.Mode == WriterExecutionMode.Deterministic)
    {
        var steps = VehicleScenario.BuildBaseline();
        for (var index = 0; index < steps.Count; index++)
        {
            await store.ApplyAsync(steps[index], cancellationSource.Token);
            if (index < steps.Count - 1)
            {
                await Task.Delay(options.OperationInterval, cancellationSource.Token);
            }
        }

        logger.LogInformation("Deterministic vehicle scenario completed. Steps={StepCount}", steps.Count);
        return 0;
    }

    var currentSnapshot = await store.GetOrCreateContinuousVehicleAsync(cancellationSource.Token);
    var current = new VehicleTransition(
        currentSnapshot.VehicleId,
        currentSnapshot.State,
        currentSnapshot.StateVersion,
        currentSnapshot.StateUpdatedAt);
    var states = new[]
    {
        VehicleState.REGISTERED,
        VehicleState.ACTIVE,
        VehicleState.IDLE,
        VehicleState.MAINTENANCE
    };
    var stateIndex = Array.IndexOf(states, current.State);

    logger.LogInformation(
        "Continuous vehicle writing started. VehicleId={VehicleId} StateVersion={StateVersion}",
        current.VehicleId,
        current.StateVersion);

    while (true)
    {
        stateIndex = (stateIndex + 1) % states.Length;
        var transition = new VehicleTransition(
            VehicleScenario.ContinuousVehicleId,
            states[stateIndex],
            checked(current.StateVersion + 1),
            VehicleScenario.TruncateToMicroseconds(DateTimeOffset.UtcNow));

        await store.UpdateAsync(transition, cancellationSource.Token);
        current = transition;
        await Task.Delay(options.OperationInterval, cancellationSource.Token);
    }
}
catch (OperationCanceledException) when (cancellationSource.IsCancellationRequested)
{
    logger.LogInformation("Vehicle writer stopped after cancellation.");
    return 0;
}
catch (Exception exception)
{
    logger.LogError(exception, "Vehicle writer failed.");
    return 1;
}
