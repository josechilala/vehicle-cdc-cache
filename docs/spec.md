# Specification: vehicle cache projection

**Status:** Implemented and validated

**Phase:** 06 - clean `just e2e` passed all required scenarios, including
identical cache-cleared full replay from offset 0.

## Read model

The Worker consumes two Debezium topics and maintains one Valkey hash per
vehicle. State and location are independent projections, so a position can be
stored before the Worker has consumed the vehicle row. A reader must distinguish
an active vehicle from a partial (`vehicle_seen=0`) or deleted
(`is_deleted=1`) entry.

### Ordering and convergence

- Each vehicle state transition carries a positive `state_version BIGINT`
  assigned by the authoritative business-event producer before delivery to
  PostgreSQL. Versions are unique and increasing per vehicle; retries reuse
  their original version and payload. The mock writer accepts an explicit
  version from each deterministic scenario rather than generating one at
  database write time.
- Normal mock-writer updates use a conditional PostgreSQL update that accepts
  only a greater `state_version`; a lower version is rejected and leaves the
  authoritative row unchanged. Equal-version identical retries are no-ops;
  equal-version conflicting content is rejected.
- A separate, explicitly named deterministic fault-injection operation
  deliberately bypasses the normal version condition and persists a lower
  version. It emits stale CDC and can temporarily regress the PostgreSQL row;
  the projector still keeps the greatest version. A new snapshot after such a
  regression cannot recover the overwritten higher version. Replay is
  guaranteed only from the retained complete CDC history containing that
  higher-version event, not from an arbitrary replacement snapshot.
- A database-generated counter alone would incorrectly rank a stale late
  write as newest.
- `state_updated_at` is descriptive business data, not an ordering key. A
  later version may carry an earlier timestamp and still wins.
- Keep `state_version` as a canonical decimal string when comparing it in Lua
  (digit count, then bytewise lexical order), never as a floating-point number.
- Position winner is the greatest `(recorded_at, position_id)`. PostgreSQL
  `timestamptz` microseconds are normalized to fixed-width UTC
  `YYYY-MM-DDTHH:mm:ss.ffffffZ` (years 0001-9999) and compared bytewise;
  `position_id` is a canonical lowercase UUID compared bytewise. No timestamp
  precision is discarded and equal timestamps always have a deterministic
  winner.
- A position insert retry uses the same primary-key UUID and exact payload.
  On primary-key conflict the writer compares the existing row: identical
  content is a no-op, conflicting content is rejected. Two distinct IDs with
  identical time and coordinates are distinct events; the greater UUID wins
  the tie. Duplicate CDC delivery for an identical tuple/value is a no-op;
  conflicting values for the same winning tuple are invalid and uncommitted.
- State and position maxima are independent and commute across topics.
  Physical deletion is terminal for the ID, clears all visible state and
  position fields, and causes all later state and position events to be ignored.
- Database log order (`source.lsn`, transaction metadata) is not business
  order; LSN alone is not a unique event identifier. Kafka offsets are local
  to a topic/partition, not comparable across streams, and only represent
  progress after successful projection.

## PostgreSQL schema

```sql
CREATE TABLE vehicles (
    id uuid PRIMARY KEY,
    state text NOT NULL CHECK (
        state IN ('REGISTERED', 'ACTIVE', 'IDLE', 'MAINTENANCE', 'DECOMMISSIONED')
    ),
    state_version bigint NOT NULL CHECK (state_version > 0),
    state_updated_at timestamptz NOT NULL
);

CREATE TABLE positions (
    id uuid PRIMARY KEY,
    vehicle_id uuid NOT NULL,
    lat double precision NOT NULL CHECK (lat BETWEEN -90 AND 90),
    lng double precision NOT NULL CHECK (lng BETWEEN -180 AND 180),
    recorded_at timestamptz NOT NULL
);
```

The authoritative scenario supplies `state_version` with each transition;
initial creation starts at 1. The version is assigned when the business
transition is created, before a writer may delay or reorder delivery. The
deterministic VehicleWriter assigns 1, 2, 3, ... per vehicle in intended
business-transition order before dispatch; delaying or retrying a transition
preserves its original version. Real producers must supply the equivalent
authoritative sequence. Normal updates use a conditional
`UPDATE ... WHERE state_version < incoming_version`, preserving the
authoritative row against a stale delivery. A separate deterministic
fault-injection operation intentionally bypasses the condition, emits stale
CDC, and may temporarily regress the row. A subsequent snapshot cannot
recover the overwritten higher version; replay requires the retained complete
CDC history. Equal-version identical retries are no-ops; equal-version
conflicting content is rejected. Deterministic fixtures include the
fault-injection case and older `state_updated_at` values. Positions are
insert-only, use stable UUID primary keys, and intentionally have no vehicle
foreign key. This permits position-before-vehicle creation and preserves
historical positions after vehicle deletion. Writers are responsible for
valid IDs. Vehicle IDs are never reused. This schema is configured for the
local environment and was deployed and exercised by the latest clean E2E.

## Kafka and Debezium

- Use separate append-only topics named
  `vehicle_cdc.public.vehicles` and `vehicle_cdc.public.positions`.
- Enable PostgreSQL logical replication and a Debezium PostgreSQL connector
  capturing only these two tables. Enable transaction metadata if supported by
  the chosen connector version.
- When transaction metadata is enabled, create the single-partition
  `vehicle_cdc.transaction` topic before connector registration.
- A new project namespace creates empty CDC topics/cache and one fixed
  infrastructure canary row at version 1 before the initial snapshot. The
  readiness check observes its Debezium `op=r` snapshot record, writes version
  2 through PostgreSQL, and confirms the resulting `op=u` event in Kafka
  before application mock writers start. A snapshot cannot communicate
  physical deletions that happened before capture began; such pre-baseline
  history is out of scope. `just down` deletes the project namespace and its
  PVCs, resetting the canary, topics, source, and cache. Reserve canary UUID
  `00000000-0000-4000-8000-000000000001` for infrastructure; because a reset
  deletes all of its CDC history, reuse across resets does not reuse an ID
  within a retained history.
- Configure topic retention to preserve all events from the declared
  canary/capture baseline; disable compaction on both topics. The challenge
  uses unlimited time retention, with storage monitoring; set
  `cleanup.policy=delete`, `retention.ms=-1`, and `retention.bytes=-1`. Verify
  actual beginning offsets before any replay. Vehicle changes are keyed by
  vehicle ID; positions use `vehicle_id` as the Kafka key so one vehicle's
  positions share a partition. Use a fixed
  three-partition count for each topic; partition counts cannot change without
  a new baseline. Position merge correctness still comes from its stable ID
  and event-time tuple.
- Expected Debezium envelope fields include `op`, `before`, `after`, `source`,
  `ts_ms`, and optional transaction metadata. `c` and `u` carry an `after` row;
  `r` is a snapshot row; `d` carries a `before` row followed by a null-valued
  Kafka tombstone. Position events are inserts (`c`); unexpected updates or
  deletes fail visibly until an explicit policy is defined.
- Extract IDs from the Kafka key for tombstones. A tombstone is not a second
  delete event and carries no business version.
- A consistent initial snapshot is assumed to precede streamed changes.
  Snapshot `op=r` rows use the same `state_version` and position tuple as
  streaming events, so late processing cannot overwrite a higher winner. The
  two topics need not be consumed in matching order. A full replay queries
  beginning offsets for every partition of both topics, verifies they match
  the configured replay baseline, then explicitly seeks each partition there.
  Store per-partition baseline offsets in durable deployment configuration
  before capture starts. If retention advanced beyond any required baseline,
  recovery fails closed before touching Valkey; automatic earliest-offset
  reset is disabled so missing history cannot be skipped silently. A fresh
  consumer group explicitly seeks to the configured baseline only after the
  retained range has been checked. Full replay is offline: clear projection
  and progress keys, then use a fresh group rather than restarting an old
  group with commits beyond the baseline. This avoids an offset-reset command
  while preserving an explicit, verified replay start.
  Topic partition counts cannot change without establishing a new baseline.

The infrastructure pins PostgreSQL `17.12`, Apache Kafka `3.9.2` in combined
KRaft mode, Debezium Kafka Connect `3.0.0.Final`, and Valkey `8.1.11`.
Debezium 3.0 documents PostgreSQL 12-17 and Kafka Connect 3.9 in its tested
versions. JSON converters are used with schemas disabled; no schema registry
is introduced. Two CDC topics have three partitions each; their clean-start
per-partition baselines are recorded as offset 0 in a Kubernetes ConfigMap.
The deployed infrastructure passed readiness checks during the preventive
audit: connector and task were `RUNNING`, PostgreSQL logical replication and
publication were present, and both topics retained the configured offset-0
baseline. Live Worker recovery, rebalance, duplicate replay, and cache equality
over an existing projection were subsequently verified through the active
Windows port-forwards. The latest clean `just e2e` passed all required
writer-to-cache scenarios from WSL, including a cache-cleared full replay from
offset 0 that rebuilt the identical canonical cache.

## Valkey schema

Key: `vehicle:{<uuid>}`

Type: hash

| Field | Meaning |
|---|---|
| `vehicle_seen` | `1` when a vehicle row or delete has been observed; otherwise `0` |
| `is_deleted` | `1` after physical row deletion; sticky because IDs cannot be reused |
| `state_present` | `1` for a current state value, `0` for position-only or deleted entries |
| `state` | Current winning state; absent unless `state_present=1` |
| `state_version` | Decimal producer-assigned version; compared without floating point |
| `delete_state_version` | Version in Debezium's delete before-image |
| `state_updated_at` | Descriptive business timestamp carried by the winning row |
| `position_seen` | `1` after at least one position has been observed |
| `position_id` | Canonical UUID and deterministic equal-time tie-breaker |
| `lat`, `lng` | Coordinates from the winning position |
| `recorded_at` | Fixed-width UTC timestamp at microsecond precision |

The canonical physical-delete entry is `vehicle_seen=1`, `is_deleted=1`,
`state_present=0`, `position_seen=0`, `delete_state_version` copied from
Debezium's `before` row, and no `state`, `state_updated_at`, `position_id`,
`lat`, `lng`, or `recorded_at`. Duplicate deletes with the same before-image
are no-ops; a conflicting delete before-image is an invariant error. A
`DECOMMISSIONED` row instead has
`is_deleted=0`, `state_present=1`, and `state=DECOMMISSIONED`. The Kafka null
tombstone changes no cache fields. A position-only entry has
`vehicle_seen=0`, `is_deleted=0`, `state_present=0`, and `position_seen=1`.

An atomic Lua operation compares independent state/position candidates and
updates related fields together. Delete is sticky; later vehicle rows are
ignored. It clears position fields, and later position candidates are ignored.
Thus deletion-first or deletion-last processing converges to the same canonical
deleted hash with no visible state or position. Invalid payloads or
equal-version conflicting state content are explicit errors and must not be
offset-committed.

The Lua script also updates `vehicle-cache:progress:<consumer-group>`, a hash
with `<topic>|<partition>` fields containing processed next-offset values.
Projection mutation and cursor advancement happen in the same script for every
record, including tombstones. Kafka offsets are synchronously committed only
after script success. On assignment, the Worker compares stored cursors to
Kafka's committed offsets; missing or behind progress fails closed. A cursor
ahead of the Kafka commit is expected after a crash between cache success and
commit and is safe because event projection is idempotent. A complete Valkey
loss that also removes progress metadata is detectable when Kafka has
committed offsets. Loss of individual vehicle keys while the progress hash
remains is not automatically detected.

## Worker CDC contract and execution

The Worker uses a single `BackgroundService` and one `Confluent.Kafka`
consumer subscribed to both configured topics. It processes one record at a
time, disables automatic offset commit and storage, and commits the consumed
record synchronously only after its Valkey Lua operation succeeds. Valkey
failures receive a configured bounded number of retries; after exhaustion the
Worker fails so Kafka can redeliver after restart. Commit failures also fail
the Worker. Malformed or unsupported messages are logged with topic,
partition, and offset, left uncommitted, and block further processing.

The connector uses `JsonConverter` with schemas disabled. Kafka values are
Debezium envelopes containing `op`, `before`, `after`, `source`, and `ts_ms`;
the parser uses `source.db`, `source.schema`, `source.table`, `source.lsn`,
`source.txId`, and `source.snapshot` as metadata, never as business ordering.
Vehicle keys are JSON objects with `id`; position keys are JSON objects with
`vehicle_id` due to the configured `message.key.columns`. Keys must match the
row identity. Vehicle `c`, `u`, and `r` events use `after`; physical `d` uses
the `before` row's `state_version`. Position `c` and `r` events use `after`;
position updates/deletes are invalid. Kafka null tombstones are recognized by
null value and a valid key and advance only progress.

With `time.precision.mode=adaptive`, PostgreSQL `timestamptz` is expected as a
Debezium `ZonedTimestamp` ISO-8601 string. The parser normalizes it to fixed
UTC microsecond text. It also accepts integer epoch-microsecond logical values
without converting through floating point. Values with sub-microsecond
precision, out-of-range dates, malformed UUIDs, invalid coordinates, unknown
states, source/table mismatches, and unsupported operations fail explicitly.
LSN and transaction IDs are retained as raw JSON text so large numbers are
not rounded by floating-point conversion.

On every partition assignment, the Worker checks that both topics exist, have
the configured partition count, and retain the configured replay baseline
(default offset 0). Each assigned partition's committed offset must be within
its retained low/high watermark range. If no group offset exists, the Worker
explicitly seeks to the configured baseline; otherwise it explicitly seeks to
the committed next offset. A Valkey cursor must not be behind the committed
offset or beyond Kafka's high watermark. A cursor ahead of the committed
offset is valid after a crash between projection and commit. Missing or
out-of-range history fails closed. Startup never clears Valkey or resets
consumer offsets.

Supported cache recovery is an offline full replay from complete retained
history after an operator verifies the baseline and clears both projection
and progress data. Loss of individual vehicle keys while the progress hash
remains is not automatically detected or repaired.

## Responsibilities

- **VehicleWriter:** deterministic scenario driver; inserts, conditionally
  updates and deletes in PostgreSQL only. Normal updates reject lower business
  versions. A distinct deterministic fault-injection operation bypasses that
  condition to emit stale CDC and may temporarily regress the source row.
  Older business timestamps with a higher version remain valid. Continuous
  mode uses a dedicated vehicle ID and advances from the current row version.
- **PositionWriter:** deterministic insert driver; exact duplicate retries use
  the same UUID/payload and become no-ops; older `recorded_at` values and
  distinct same-time position IDs exercise projection ordering. A repeated ID
  with conflicting content fails explicitly. Continuous mode uses a fixed
  vehicle ID, sequence-derived UUIDs, and UTC timestamps. Writes only to
  PostgreSQL.
- **Debezium/Kafka:** publish the committed source-table changes and tombstones.
  PostgreSQL has logical WAL enabled, a publication restricted to both source
  tables, and `REPLICA IDENTITY FULL` so a vehicle delete includes its
  before-image. The Kafka broker uses one KRaft combined broker/controller;
  internal and CDC topics use replication factor 1. CDC topics use three
  partitions, `cleanup.policy=delete`, and unlimited time/size retention.
- **Kafka Connect registration:** an idempotent Kubernetes Job upserts the
  connector configuration. Readiness requires a running connector and task,
  the canary's snapshot event, and its subsequent PostgreSQL update observed
  as CDC in Kafka; arbitrary sleep is not used as a readiness signal.
- **Valkey:** one standalone Service/Deployment with AOF `appendfsync
  everysec`, RDB snapshots disabled, and `noeviction`. A persistent volume
  reduces loss risk; an abrupt failure can still lose up to roughly one second
  of acknowledged writes. Namespace removal discards the volume, so recovery
  then requires complete retained Kafka history and offline replay.
- **VehicleCache.Worker:** decode and validate CDC, process each partition
  sequentially, apply event-specific ordering, call atomic Valkey merge, and
  commit offsets only after success. It has no PostgreSQL client or connection
  string. Invalid CDC stops processing without skipping the record.
- **Valkey:** hold the derived read model; no writer other than the Worker.
- **Readers:** read the view and honor pending/deleted flags; they do not query
  PostgreSQL.

## Deterministic mock-writer execution contract

The finite fixtures share these stable vehicle IDs:

| Vehicle ID | Purpose |
|---|---|
| `10000000-0000-4000-8000-000000000001` | State ordering and position ordering |
| `10000000-0000-4000-8000-000000000002` | Decommissioned state, physical deletion, then late position |
| `10000000-0000-4000-8000-000000000003` | Position inserted before vehicle creation |

Run against a fresh source/CDC baseline, with the connector already capturing
changes, in this order:

1. `PositionWriter --mode deterministic --scenario before-vehicles`
2. `VehicleWriter --mode deterministic --scenario baseline`
3. `PositionWriter --mode deterministic --scenario after-delete`

The command order makes the position-only insert happen before creation and
the final position insert happen after source deletion. It does not impose
cross-topic consumer order; the projection must converge for either order.
The baseline scenario creates vehicle 1 at version 1, writes versions 2 and 3,
attempts a normal stale version 2 update (rejected), then explicitly persists
that same version 2 through fault injection. It creates vehicle 2, transitions
to `DECOMMISSIONED` at version 2, and physically deletes it. Vehicle 3 is
created at version 1 after its position was inserted.

Vehicle 1's version-3 transition deliberately carries
`state_updated_at=2026-01-02T11:59:00Z`, earlier than its version-2 timestamp.
The projection must select version 3 by business version, not wall-clock time.
The position sequence for vehicle 1 inserts timestamp
`2026-01-02T12:00:00.123457Z`, then an older microsecond timestamp, followed
by a distinct ID at the original timestamp with a greater canonical UUID. It
then retries that winning ID and payload exactly. The expected projection is:

- Vehicle 1: state `IDLE`, version `3`; position ID
  `20000000-0000-4000-8000-000000000020`, time
  `2026-01-02T12:00:00.123457Z`, latitude `40.7138`, longitude `-74.0050`.
- Vehicle 3: state `REGISTERED`, version `1`; retain its pre-creation
  position ID `20000000-0000-4000-8000-000000000030`.
- Vehicle 2: terminal deleted representation with
  `delete_state_version=2`, no visible state or position, regardless of the
  position event inserted after deletion.

Deterministic vehicle IDs are not reused. The full scenario is intended for a
fresh source/CDC baseline; a post-fault-injection snapshot of the regressed
vehicle row cannot reconstruct the overwritten higher version. Resetting the
namespace/source is outside the writer programs. Exact position retries are
safe against an existing identical row; a conflicting duplicate is an
intentional error scenario.

Both programs accept `--connection-string`, `--mode`, `--rate`, and
`--scenario`. The connection string may instead be supplied by
`VEHICLE_CACHE_CONNECTION_STRING`; it is never logged. Operations are
asynchronous, parameterized, cancellation-aware, and emitted through structured
console logs. PostgreSQL access and CDC delivery passed the latest clean
`just e2e` using these deterministic fixtures.

## Recovery and operational assumptions

- A tombstone does not change the vehicle projection but advances the Valkey
  progress cursor. Auto commit and automatic offset storage are disabled.
  Commit only after the Lua operation succeeds; cache failure leaves the offset uncommitted. A crash
  after the cache write but before commit replays the event idempotently.
  Retry Kafka commit errors marked retriable a bounded number of times for the
  same projected record, while the partition remains assigned. If ownership
  is lost or retries are exhausted, stop the Worker; the next owner/restart
  re-reads from the last committed offset.
- On rebalance, stop dispatch for revoked partitions; finish or abandon
  in-flight work and commit only while ownership remains valid. Per-partition
  sequential processing prevents commits from passing unfinished records.
- Invalid CDC is logged/alerted and blocks that partition; there is no
  silent dead-letter-and-skip behavior in this implementation.
- At startup, both topics must have the configured partition count and every
  partition's earliest offset must equal the configured retained-history
  baseline. Existing Kafka commits must not be ahead of their Valkey cursors.
  The Worker never clears cache data or resets offsets automatically.
- Snapshot rows and streaming events use the same business-version/position
  ordering. Initial snapshot and subsequent streaming readiness passed the
  clean E2E; this does not establish every concurrent snapshot/transaction
  interleaving.
- Full cache replay requires all records since the declared capture baseline.
  Kafka retention and the baseline are checked before clearing the cache.
  Stop the Worker, clear only this application's key namespace, reset both
  topic subscriptions to the earliest retained offsets, and replay offline.
  Partial key loss is not automatically detected. Cache loss recovery is an
  operator procedure; the Worker never queries PostgreSQL.
- Logical cache equality compares sorted per-vehicle canonical fields and
  comparison metadata (state version, deleted/presence markers, delete
  before-image version, position value/timestamp/coordinates). Kafka offsets
  are checked separately against the replay baseline and are not cache fields.
  Generation IDs and volatile process state are excluded.
- Rebuild runs offline and readers can see partial contents. Zero-downtime
  cache generation swap is outside the challenge scope.

## Implementation status

1. Ordering, snapshot readiness, retention, replay, and ID-reuse decisions are
   accepted in ADR 001.
2. PostgreSQL-only deterministic mock writers and their unit tests are complete;
   database behavior passed the clean E2E.
3. Local Kubernetes, Kafka, Debezium snapshot/stream capture, and Valkey
   deployment passed clean E2E readiness.
4. Debezium parsing, the sequential consumer, atomic Lua projection/progress,
   startup guards, and parser/configuration unit tests are complete. Live
   integration validation passed.
5. All 19 unit tests and 7 production-Lua Valkey integration tests passed.
6. The latest clean `just e2e` passed all required scenarios, including
   identical cache-cleared full replay from offset 0; C4 reflects the
   implemented containers and Worker components.
