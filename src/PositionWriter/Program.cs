using Microsoft.Extensions.Logging;
using Npgsql;
using PositionWriter.Configuration;
using PositionWriter.Infrastructure;
using PositionWriter.Models;
using PositionWriter.Scenarios;

using var loggerFactory = LoggerFactory.Create(builder =>
    builder.AddSimpleConsole(options => options.SingleLine = true));
var logger = loggerFactory.CreateLogger("PositionWriter");

if (args.Contains("--help", StringComparer.OrdinalIgnoreCase) ||
    args.Contains("-h", StringComparer.OrdinalIgnoreCase))
{
    Console.WriteLine(PositionWriterOptions.Usage);
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
    var options = PositionWriterOptions.Parse(args);
    await using var dataSource = NpgsqlDataSource.Create(options.ConnectionString);
    var store = new PostgresPositionStore(dataSource, logger);

    if (options.Mode == WriterExecutionMode.Deterministic)
    {
        var records = PositionScenario.Build(options.Scenario);
        for (var index = 0; index < records.Count; index++)
        {
            await store.InsertAsync(records[index], cancellationSource.Token);
            if (index < records.Count - 1)
            {
                await Task.Delay(options.OperationInterval, cancellationSource.Token);
            }
        }

        logger.LogInformation(
            "Deterministic position scenario completed. Scenario={Scenario} Attempts={AttemptCount}",
            options.Scenario,
            records.Count);
        return 0;
    }

    long sequence = 0;
    logger.LogInformation(
        "Continuous position writing started. VehicleId={VehicleId}",
        PositionScenario.CurrentVehicleId);
    while (true)
    {
        sequence = checked(sequence + 1);
        var position = PositionScenario.CreateContinuousPosition(sequence);
        await store.InsertAsync(position, cancellationSource.Token);
        await Task.Delay(options.OperationInterval, cancellationSource.Token);
    }
}
catch (OperationCanceledException) when (cancellationSource.IsCancellationRequested)
{
    logger.LogInformation("Position writer stopped after cancellation.");
    return 0;
}
catch (Exception exception)
{
    logger.LogError(exception, "Position writer failed.");
    return 1;
}
