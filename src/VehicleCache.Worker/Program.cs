using Confluent.Kafka;
using StackExchange.Redis;
using VehicleCache.Worker.Configuration;
using VehicleCache.Worker.Consumers;
using VehicleCache.Worker.Projection;

var builder = Host.CreateApplicationBuilder(new HostApplicationBuilderSettings
{
    Args = args,
    ContentRootPath = AppContext.BaseDirectory
});

builder.Services
    .AddOptions<KafkaOptions>()
    .Bind(builder.Configuration.GetSection(KafkaOptions.SectionName))
    .Validate(WorkerOptionsValidation.IsValidKafka, "Kafka configuration is invalid.")
    .ValidateOnStart();

builder.Services
    .AddOptions<ValkeyOptions>()
    .Bind(builder.Configuration.GetSection(ValkeyOptions.SectionName))
    .Validate(WorkerOptionsValidation.IsValidValkey, "Valkey configuration is invalid.")
    .ValidateOnStart();

builder.Services.AddSingleton<IConnectionMultiplexer>(serviceProvider =>
{
    var options = serviceProvider.GetRequiredService<Microsoft.Extensions.Options.IOptions<ValkeyOptions>>().Value;
    var configuration = ConfigurationOptions.Parse(options.ConnectionString);
    configuration.AbortOnConnectFail = true;
    configuration.AsyncTimeout = options.OperationTimeoutMs;
    configuration.SyncTimeout = options.OperationTimeoutMs;
    configuration.ConnectTimeout = options.OperationTimeoutMs;
    return ConnectionMultiplexer.Connect(configuration);
});
builder.Services.AddSingleton<ValkeyProjection>();
builder.Services.AddSingleton<CdcEnvelopeParser>();
builder.Services.AddHostedService<CdcWorker>();

var host = builder.Build();
await host.RunAsync();
