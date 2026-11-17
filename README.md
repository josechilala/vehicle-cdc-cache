# Vehicle CDC Cache

This repository implements the mock writers and the .NET 10 CDC Worker for the
NsTech CDC-Driven Vehicle Cache challenge, alongside the local Kubernetes
infrastructure and Phase 06 end-to-end validation workflow.
The original requirements are preserved in
[`case-cdc-vehicle-cache.md`](./case-cdc-vehicle-cache.md).

## Architecture

`VehicleWriter` and `PositionWriter` are .NET 10 console applications that
write only to PostgreSQL. Debezium captures committed vehicle and position
changes and publishes separate Kafka topics. `VehicleCache.Worker` consumes
both topics and atomically merges their independently ordered fields into
Valkey. The Worker never connects to PostgreSQL. Readers use Valkey only.

Vehicle state order is a source-assigned per-vehicle `state_version`, assigned
when the business transition is created—not a database write counter. Normal
writer updates conditionally reject lower versions; an explicit deterministic
fault-injection scenario bypasses that condition to produce stale CDC and can
temporarily regress the source row. The CDC projection independently ignores
the lower version. A later snapshot of the regressed row cannot recover the
overwritten higher version; recovery requires the retained complete CDC history.
Position order is fixed-precision UTC `recorded_at` plus canonical position
UUID, compared deterministically. These business ordering rules are distinct
from PostgreSQL log positions and Kafka offsets.

The cache keeps terminal physical-delete markers, distinct from the visible
`DECOMMISSIONED` state and Kafka's null tombstone. Deletion removes visible
state and position fields; later state and position events are ignored. A
per-topic/partition processed-next-offset cursor is updated atomically with
each projection, including tombstones. On assignment, the Worker compares
these cursors with Kafka's committed offsets and fails closed if progress is
missing or behind. Complete cache loss is detectable when progress metadata
is also lost; partial key loss is not detected. Full replay requires complete retained
history and remains an offline operation; `just e2e` verifies this recovery
path. Readers may see partial data during replay.

The deployed infrastructure was inspected during the preventive audit:
PostgreSQL logical replication/publication, Kafka topics and offset baselines,
the running Debezium connector, and Valkey readiness were verified. All 26
unit/integration tests pass, including the production Lua projection against
the live Valkey service. Live Worker testing verified recovery from a transient
Kafka coordinator commit error, restart, duplicate replay, an actual rebalance,
and cache equality after offset-zero replay over the existing projection. The
latest clean `just e2e` passed all required scenarios, including
writer-to-cache assertions, restart, duplicate delivery, and cache-cleared
full replay from offset 0 that rebuilt the identical canonical cache.

The infrastructure targets the `vehicle-cache` namespace in the selected
Kubernetes context (`KUBE_CONTEXT`, default `docker-desktop`). It reuses that
cluster and does not create or delete clusters. PostgreSQL uses logical
replication; Kafka runs one KRaft broker with a single-partition internal offsets topic and
separate three-partition vehicle and position topics, no compaction, and
unlimited time/size retention. A fixed version-1 canary row is included in
Debezium's initial snapshot; `just up` waits for that snapshot record and a
version-2 PostgreSQL change observed in Kafka before reporting readiness.
Valkey is standalone with AOF persistence and no eviction. `just down` removes
the namespace and its volumes, resetting this canary and its CDC baseline.
`just e2e` resets only this namespace before deploying, then stops its temporary
Worker and port-forwards on exit while leaving the namespace deployed.

## Repository structure

- `src/VehicleCache.Worker/` - one `BackgroundService`, Debezium parser,
  Kafka consumer, and atomic Valkey projection.
- `src/VehicleWriter/` - PostgreSQL-only vehicle creation, conditional state
  updates, fault injection, and deletion.
- `src/PositionWriter/` - PostgreSQL-only deterministic or continuous position
  inserts.
- `tests/VehicleCache.UnitTests/`, `tests/VehicleCache.IntegrationTests/` -
  writer scenario/configuration unit tests and the integration-test project;
  no fake database or CDC tests are included.
- `docs/` - RFC, implemented C4 model, accepted ADR, projection specification,
  and test behaviors.
- `infrastructure/` - Kubernetes, PostgreSQL, Kafka, Debezium, and Valkey
  configuration.
- `scripts/` - prerequisite checks, infrastructure lifecycle, and smoke-test
  validation.

## Prerequisites

- .NET 10 SDK
- WSL2 with a Linux distribution, Docker Desktop Kubernetes, Nix/devenv,
  kubectl, DevSpace, and just

The solution currently targets `net10.0`. Infrastructure prerequisites are
not needed to restore, build, or run parser/configuration/writer scenario tests.

## Documentation

- [RFC](./docs/rfc.md)
- [C4 diagrams](./docs/c4.md)
- [ADR](./docs/adr.md)
- [Projection specification](./docs/spec.md)
- [Test behaviors](./docs/test-behaviors.md)

## Mock writer configuration and execution

Both writers require `VEHICLE_CACHE_CONNECTION_STRING` or
`--connection-string`. The string is validated at startup and never written to
logs. `--mode` accepts `deterministic` or `continuous`; `--rate` sets operations
per second from `0.01` through `1000`. Deterministic mode is finite and uses
fixed IDs, UTC timestamps, coordinates, and event sequence. Continuous mode
runs until Ctrl+C. `--help` prints the writer-specific options.

Run the coordinated fixture against an empty scenario database with the CDC
connector already running. A local connection must be provided separately
(for example, through an explicitly started localhost port-forward); the
writers do not create or expose infrastructure:

```powershell
$env:VEHICLE_CACHE_CONNECTION_STRING = "Host=localhost;Port=5432;Database=vehicle_cache;Username=vehicle_writer;Password=<local-secret>"
dotnet run --project src/PositionWriter -- --mode deterministic --scenario before-vehicles --rate 100
dotnet run --project src/VehicleWriter -- --mode deterministic --scenario baseline --rate 100
dotnet run --project src/PositionWriter -- --mode deterministic --scenario after-delete --rate 100
```

The sequence writes a position for one vehicle before its row is created,
applies regular and stale vehicle transitions, physically deletes another
vehicle, then writes a position for that deleted ID. Independent topic
consumption is not ordered by these command timings; the expected final
projection is deterministic because state/position winners use their own
ordering keys and deletion is terminal.

Validated logical projection after the Worker processes the complete
fixture:

- Vehicle `10000000-0000-4000-8000-000000000001`: `IDLE`, version `3`, plus
  `state_updated_at=2026-01-02T11:59:00Z`, which is earlier than version 2, and
  position `20000000-0000-4000-8000-000000000020` at
  `2026-01-02T12:00:00.123457Z`, coordinates `(40.7138, -74.0050)`. The later
  persisted version-2 fault-injection event must not replace version 3.
- Vehicle `10000000-0000-4000-8000-000000000003`: `REGISTERED`, version `1`,
  plus the position inserted before its vehicle row.
- Vehicle `10000000-0000-4000-8000-000000000002`: terminal deleted marker
  with delete version `2`; no visible state or position fields, including
  after the post-delete position event.

`before-vehicles` also retries one exact position insert; this is a no-op.
`conflicting-duplicate` deliberately reuses its position ID with different
content and must exit non-zero. `after-delete` and `baseline` create stable
fixture IDs, so rerun the full scenario only after resetting the local source
database/CDC baseline; do not reuse deleted IDs within a retained history.

Vehicle updates assign their business version in the fixture before any
configured delay. Normal SQL updates accept only a greater version. The
separate fault-injection operation requires an existing greater version and
intentionally bypasses that condition to emit stale CDC; it can regress the
PostgreSQL row and is not normal writer behavior. A later snapshot of the
regressed row cannot recover the overwritten higher version.

For ongoing sample data, run either writer with `--mode continuous --rate 2`;
Ctrl+C requests graceful shutdown. The vehicle writer uses its dedicated
stable ID and advances versions from the current row. The position writer
uses deterministic sequence-based IDs and current UTC timestamps.

## Worker configuration and execution

The Worker binds standard .NET configuration keys from `appsettings.json`;
environment variables override them using double underscores:

| Environment variable | Default | Purpose |
|---|---|---|
| `Kafka__BootstrapServers` | `localhost:9092` | Kafka bootstrap list |
| `Kafka__VehicleTopic` | `vehicle_cdc.public.vehicles` | Vehicle CDC topic |
| `Kafka__PositionTopic` | `vehicle_cdc.public.positions` | Position CDC topic |
| `Kafka__ConsumerGroup` | `vehicle-cache-worker` | Consumer group |
| `Kafka__SessionTimeoutMs` | `10000` | Kafka session timeout |
| `Kafka__MaxPollIntervalMs` | `300000` | Kafka maximum poll interval |
| `Kafka__CommitTimeoutMs` | `10000` | Metadata/commit request timeout |
| `Kafka__ExpectedPartitionCount` | `3` | Required fixed topic partition count |
| `Kafka__ReplayBaselineOffset` | `0` | Required earliest offset in every partition |
| `Valkey__ConnectionString` | `localhost:6379` | Valkey endpoint/credentials |
| `Valkey__OperationTimeoutMs` | `5000` | Redis command/connect timeout |
| `Valkey__RetryCount` | `3` | Bounded retries for cache failures |
| `Valkey__RetryDelayMs` | `250` | Delay between cache retries |

Run the Worker only after Kafka topics and Valkey are available:

```powershell
dotnet run --project src/VehicleCache.Worker
```

The Worker loads `appsettings.json` from its executable directory, so running
the built DLL from the repository root still loads its defaults; environment
variables override those values.

The single consumer subscribes to both configured topics and processes one
record at a time. It validates topic partition counts and earliest offsets
against the configured replay baseline before projection. Offsets are committed
synchronously only after the Lua projection and its Valkey cursor update
succeed. A missing or lagging Valkey cursor against an existing Kafka commit
stops the Worker and requires offline recovery. Invalid CDC blocks the
partition; there is no skip or dead-letter behavior. Each assignment checks
retained ranges and cursors, then explicitly seeks to the committed next
offset, or to the configured baseline when the group has no commit. Missing or
out-of-range history fails closed; offsets and Valkey are never reset
automatically.

Partial loss of individual vehicle keys while the progress hash remains is
not automatically detected or repaired. Supported recovery is an offline
full replay from retained history after an operator verifies the baseline and
deliberately clears projection and progress data, then starts a fresh consumer
group with no existing Kafka commits. The E2E duplicate and full-replay phases
use separate fresh groups instead of mutating commits with the Kafka group
administration CLI.

The separate integration-test project executes the production Lua script
against a real Valkey instance. Configure
`VEHICLE_CACHE_TEST_VALKEY_CONNECTION_STRING` and run it explicitly:

```powershell
$env:VEHICLE_CACHE_TEST_VALKEY_CONNECTION_STRING = "localhost:6379"
dotnet test tests\VehicleCache.IntegrationTests\VehicleCache.IntegrationTests.csproj
```

These are integration tests, not unit tests. All 7 passed against the deployed
Valkey service during the preventive audit.

## Infrastructure commands

- `just up` - deploys infrastructure to the selected context's `vehicle-cache` namespace,
  registers the connector, and runs readiness/CDC smoke checks. This lifecycle
  passed as part of the latest clean `just e2e`.
  It allows up to 20 minutes for the infrastructure startup, waits for
  PostgreSQL, Kafka, Valkey, and Kafka Connect readiness before creating the
  connector-registration Job, and prints pod events and logs on failure.
  Kafka uses TCP probes to avoid repeatedly launching JVM-based checks; its
  Connect init container still verifies Kafka's topic API and provisions the
  required topics before Connect becomes ready. PostgreSQL and Valkey retain
  their functional `pg_isready` and `valkey-cli ping` probes. Kafka Connect
  loads only the PostgreSQL plugin and uses the `Recreate` strategy.
- `just status` - reports Kubernetes resources, connector task, and snapshot
  readiness.
- `just down` - removes only this project's namespace and its data from the
  selected context; it does not remove the cluster.
- `just e2e` - builds the solution, recreates only the `vehicle-cache`
  namespace, deploys infrastructure, runs deterministic writers and the Worker,
  asserts the final Valkey hashes, exercises duplicate delivery with a fresh
  group, and verifies that a full replay from offset 0 rebuilds the identical
  cache with another fresh group. It checks retained offsets before clearing
  project vehicle hashes and progress, cleans up Worker/port-forward processes
  on exit, and leaves the namespace running for inspection. **This deletes
  existing data and workloads in the `vehicle-cache` namespace, including its
  volumes; it never deletes the cluster or resources in other namespaces.
  Infrastructure startup has a 20-minute readiness deadline.

`just e2e` requires the ports `15432`, `19092`, and `16379` to be available on
the WSL host. `KUBE_CONTEXT` defaults to `docker-desktop` and can be overridden
for all infrastructure commands:

```bash
KUBE_CONTEXT=my-context just e2e
```

To connect to PostgreSQL or Valkey from the host, start each port-forward in a
separate WSL terminal:

```bash
kubectl --context "${KUBE_CONTEXT:-docker-desktop}" --namespace vehicle-cache port-forward service/postgres 5432:5432
kubectl --context "${KUBE_CONTEXT:-docker-desktop}" --namespace vehicle-cache port-forward service/valkey 6379:6379
```

Use the generated PostgreSQL credentials from the local Kubernetes Secret; do
not commit or log them. Kafka has an external listener for local Worker
connections; use port `19092` when forwarding it:

```bash
kubectl --context "${KUBE_CONTEXT:-docker-desktop}" --namespace vehicle-cache port-forward service/kafka 19092:19092
```

The Windows/WSL2 assessment environment has 8 GiB of host RAM, with
approximately 3.7 GiB available to Docker. Kafka requests 100m CPU/512 MiB and
is limited to 1 GiB, with a 512 MiB JVM heap and one active processor. Kafka
Connect requests 100m CPU/512 MiB and is limited to 768 MiB, also with a
512 MiB heap and one active processor. These settings target a small local
assessment environment; the latest clean E2E passed at these limits. This
run does not establish long-term runtime stability. The active Kubernetes
context is `docker-desktop`, which is also the default selected by the scripts. Ubuntu 24.04 and Nix are installed, and
the project `devenv shell` evaluates successfully and provides DevSpace, just,
kubectl, and the .NET 10 SDK. The lock file is present. The latest clean
`just e2e` reached Docker Desktop and Kubernetes from WSL, deployed fresh
infrastructure with one internal offsets-topic partition, and passed CDC
readiness and all required scenarios. Do not stop or remove unrelated containers
already running in Docker.

The currently pinned infrastructure images are PostgreSQL 17.12, Apache Kafka
3.9.2 (KRaft), Debezium Kafka Connect 3.0.0.Final, Valkey 8.1.11, and
curlimages/curl 8.12.1. The Nix package set and `devenv.lock` are pinned to
their configured revisions. The pinned nixpkgs provides a .NET 10 SDK, kubectl 1.34.3, DevSpace
6.3.18, and just 1.43.1. Set `KUBE_CONTEXT` to override the default context
for `just up`, `just status`, and `just down` (for example,
`KUBE_CONTEXT=my-context just up` in WSL). `just up` deploys to the
`vehicle-cache` namespace; `just down` deletes only that namespace, including
its volumes. `just e2e` also recreates only that namespace before testing and
requires the listed local forwarding ports to be free. Local database
passwords are generated into a Kubernetes Secret by `just up`; no credentials
are committed to the repository.

Build the .NET solution with:

```powershell
dotnet restore VehicleCache.slnx
dotnet build VehicleCache.slnx --no-restore
```

The mock writers and Worker are implemented. The end-to-end script automates
PostgreSQL/CDC delivery, Kafka offset replay, and Valkey projection assertions.
The latest clean run passed all required scenarios, including identical full
replay from offset 0. To rerun the workflow:

```powershell
wsl -d Ubuntu-24.04 -- bash -lc 'cd "/mnt/c/Users/Jose Jacinto/Documents/Projectos/Vehicle CDC Cache - Desafio Técnico nstech" && devenv shell -- just e2e'
```

## Required local setup (Windows)

The project workflow is intended to run in a WSL2 Linux distribution. After
human approval to install/configure the local prerequisites:

1. Install a WSL2 distribution from an elevated Windows terminal:

   ```powershell
   wsl --install -d Ubuntu-24.04
   wsl --set-default-version 2
   ```

   Restart Windows if requested, then launch Ubuntu and complete its first-run
   user setup.
2. Install Docker Desktop if needed:

   ```powershell
   winget install -e --id Docker.DockerDesktop
   ```

   Start it, enable Kubernetes, its WSL2 engine and Ubuntu integration, and
   confirm `docker info` succeeds and the `docker-desktop` context is
   available inside WSL. Set `KUBE_CONTEXT` if using another context.
3. In Ubuntu/WSL, install Nix in single-user mode using the official WSL
   instructions:

   ```bash
   curl -L https://nixos.org/nix/install -o /tmp/install-nix.sh
   sh /tmp/install-nix.sh --no-daemon
   ```

   Install devenv using the official package instructions:

   ```bash
   nix-env --install --attr devenv \
     -f https://github.com/NixOS/nixpkgs/tarball/nixpkgs-unstable
   ```

   Then follow the official
   [devenv installation guide](https://devenv.sh/getting-started/).
4. Open the repository from the Ubuntu distribution and enter its devenv shell.
   Run `just up` to start the infrastructure, or `just e2e` to execute the
   complete deterministic validation. The devenv provides kubectl, DevSpace,
   and just.

No system software or operating-system settings were changed during this
phase.
