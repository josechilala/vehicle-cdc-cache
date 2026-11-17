# RFC: CDC-driven vehicle cache

**Status:** Implemented and validated; decision recorded in ADR 001

**Phase:** 06 - clean `just e2e` passed all required scenarios, including
identical cache-cleared full replay from offset 0.

## Problem

Tracking and alerting readers need a low-latency view of each vehicle's current
state and latest position. Reading the operational database for those requests
adds read load and couples consumers to the write path. A cache written directly
by the mock applications would introduce a dual-write failure mode.

## Goals

- Build one per-vehicle read model from Debezium CDC events in Kafka only.
- Keep vehicle state and latest position correct despite duplicates, retries,
  independent topic consumption, and stale events.
- Make cache updates atomic and conditional, and make full replay deterministic.
- Provide an inspectable .NET 10 Worker, focused tests, and reproducible local
  infrastructure without introducing unnecessary services.

## Non-goals

- Add a read API, production authentication, fleet-wide analytics, or a general
  event platform.
- Let the Worker query PostgreSQL or let writers publish directly to Kafka/Valkey.
- Guarantee a historical position timeline; the projection keeps one winning
  position per vehicle.

## Implemented architecture

VehicleWriter and PositionWriter write rows only to PostgreSQL. PostgreSQL
logical decoding feeds a Debezium connector, which publishes vehicle and
position change events to separate Kafka topics. A .NET 10 Worker consumes
both topics and uses atomic Valkey Lua scripts to merge the state and position
fields independently. The Worker updates a per-topic/partition
processed-next-offset cursor atomically with every record, including
tombstones, then synchronously commits the Kafka offset. Startup checks topic
partition counts and beginning offsets against the configured replay baseline,
and verifies that Valkey progress is not behind existing Kafka commits. Cache
readers access Valkey only.

Vehicle state ordering uses a source-assigned `state_version` attached to the
business transition before it is written. Normal mock-writer updates are
conditional and reject a lower version, preserving the authoritative row. A
separate deterministic fault-injection operation deliberately bypasses that
condition to persist an older version and emit stale CDC; the row may
temporarily regress. The projection independently keeps the greatest business
version. A snapshot taken after regression cannot recover the overwritten
higher version; replay correctness therefore requires the retained complete
CDC history, not an arbitrary replacement snapshot. Position ordering uses
UTC `recorded_at` at PostgreSQL's
microsecond precision, then
the canonical position UUID as a deterministic tie-breaker. These are
business-event ordering rules, not PostgreSQL log ordering. LSNs and Kafka
offsets describe capture/delivery progress; they are not substitutes.

The cache retains only a terminal deletion marker instead of physically
deleting the key. Physical deletion removes visible state and position fields;
all later vehicle and position events for that ID are ignored. Deleted IDs
cannot be reused. A physical row delete is distinct from a `DECOMMISSIONED`
state transition and from the following Kafka null tombstone.

## Risks and assumptions

- Kafka topics must retain all records since the declared replay baseline,
  without compaction, for the supported recovery window. Expired history
  cannot be reconstructed from PostgreSQL by the Worker.
- A new cache rebuild verifies the earliest offsets of every topic partition
  against its recorded baseline before clearing the projection. It stops
  consumers and rebuilds while readers may observe partial data; online
  cutover is outside this challenge.
- Debezium's initial snapshot is assumed to represent a consistent boundary;
  streaming changes follow the snapshot boundary. Snapshot events may still
  be processed after higher-version state events, which the projection rejects.
- The authoritative producer assigns a stable, strictly increasing
  `state_version` per vehicle before delivery. Normal writer updates reject
  lower versions; a separate explicit fault-injection scenario bypasses that
  guard to exercise stale CDC handling. The initial capture contains a fixed
  infrastructure canary row. Readiness observes its snapshot record and a
  subsequent version-2 streaming event before application mock writes begin.
  IDs are not reused.
  Position timestamps are UTC instants representable at microsecond precision.
- Kafka partitions and offsets are local to a topic/partition; they do not
  establish a total order between the two topics.
- Loss of the full Valkey dataset is detected when the progress hash is also
  absent while Kafka has committed offsets; isolated vehicle-key loss is not
  automatically detected. Recovery requires an operator to stop the Worker,
  verify complete retained Kafka history, clear the projection and progress
  hash, and reset both consumer topics to their declared baselines before
  replay. Replay is offline; active readers may see partial data.
- Lua scripts must validate inputs and update all visible fields for one
  vehicle atomically. Redis-compatible Valkey scripting is required.

## Acceptance criteria

1. The Worker has no PostgreSQL dependency or database connection.
2. Both projections use atomic compare-and-merge operations and duplicate
   delivery is a no-op.
3. Position and state winners are independent of cross-topic arrival order.
4. A stale position/state cannot replace a newer winner; deletion remains
   terminal for a vehicle ID, regardless of position/state delivery order.
5. Manual Kafka commits happen only after the corresponding atomic Valkey
   projection succeeds. Cache failure or invalid CDC is not committed;
   redelivery after a crash is safe.
6. Full replay starts at the earliest available offset for every partition
   only after verifying that Kafka still retains the complete replay baseline.
7. Automated tests cover the mandatory scenarios in `test-behaviors.md`,
   including replay equality.
8. `just up` brings up infrastructure from a clean checkout. The latest clean
   `just e2e` passed infrastructure readiness, deterministic assertions, restart,
   duplicate delivery, and identical cache-cleared replay from offset 0.
9. Mock writers support deterministic fixtures, exact duplicate insert retries,
   distinct same-time positions, and stale business state updates.
