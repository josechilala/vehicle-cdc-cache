# Test behaviors

**Status:** Validated. All 19 unit tests and 7 Valkey integration tests pass.
Live infrastructure validation exercised Worker restart, duplicate replay, an
actual rebalance, commit recovery, and equality over an existing projection.
The latest clean `just e2e` passed all required scenarios, including
cache-cleared full replay from offset 0 with identical canonical hashes.

1. **Vehicle creation and state update appear**
   - Given a vehicle creation with producer-assigned `state_version=1` is
     projected
   - When a valid transition with `state_version=2` is written and consumed
   - Then the hash exposes the new state and version 2.

2. **New position appears**
   - Given a vehicle has a visible state and a position at time T
   - When a position with a greater `recorded_at` is consumed
   - Then its coordinates, ID, and timestamp become the position fields.

3. **Stale position is ignored**
   - Given the cache contains position `(T2, id2)`
   - When a position `(T1, id1)` with `T1 < T2` is consumed
   - Then no position fields change.

4. **Equal-time position tie-break and timestamp precision are deterministic**
   - Given two distinct positions differ only in microsecond timestamp or have
     the same timestamp and distinct canonical UUIDs
   - When they are consumed in either order
   - Then the greatest fixed-width UTC microsecond timestamp wins, or the
     greater bytewise UUID wins when timestamps are equal.

5. **Normal stale state rejection and injected stale CDC**
   - Given version 8 is stored and projected
   - When a normal version 7 update is attempted, followed by the separate
     deterministic fault-injection path persisting version 7
   - Then the normal update leaves PostgreSQL unchanged; fault injection
     deliberately regresses the row and emits CDC, but the cache remains at
     version 8. A later snapshot of the regressed row cannot recover version 8;
     replay requires the retained complete CDC history.

6. **Business version, not wall-clock time, orders vehicle state**
   - Given version 8 is cached with a later `state_updated_at`
   - When version 9 arrives with an earlier `state_updated_at`
   - Then version 9 is current because producer-assigned business version, not
     timestamp, LSN, or database write order, defines state order.

7. **Duplicate vehicle event and conflicting version**
   - Given a vehicle transition `(version=4, state=ACTIVE)` has been applied
   - When the identical writer/CDC event is retried
   - Then the source row and canonical cache remain unchanged; an event with
     `(version=4, state=IDLE)` is rejected and not committed.

8. **Duplicate position insert and duplicate CDC delivery**
    - Given an insert `(id=P, timestamp=T, coordinates=C)` has been accepted
    - When the writer retries the exact same insert and the CDC event is
      replayed
    - Then PostgreSQL contains one row and the canonical cache is unchanged;
      reusing P with conflicting content is rejected.

9. **Delete, tombstone, and DECOMMISSIONED are distinct**
    - Given a vehicle and position are cached
    - When state changes to `DECOMMISSIONED`, and separately a physical delete
      (`op=d`) is applied followed by its null tombstone
    - Then decommissioning remains a visible non-deleted state; physical delete
      yields `vehicle_seen=1`, `is_deleted=1`, `state_present=0`,
      `position_seen=0`, `delete_state_version` from its before-image, and no
      state/timestamp/position fields; duplicate delete and null tombstone are
      no-ops and readers do not expose the entry as active.

10. **Position arrives before vehicle creation**
    - Given the Worker has not consumed the vehicle row
    - When a position for that ID arrives first
    - Then a partial, non-active entry stores the position; when the vehicle row
      arrives, both fields are present.

11. **Events arriving after deletion cannot resurrect a vehicle**
    - Given the vehicle has been physically deleted and its deletion marker is
      projected
    - When a stale vehicle row event and further positions arrive afterward
    - Then no row or position event changes the canonical deleted hash; the
      vehicle remains inactive with no visible state or position.

12. **Position/delete order converges**
    - Given a delete event and positions before and after the delete exist
    - When the same events are processed with positions before delete, after
      delete, or interleaved
    - Then every result has the same deletion marker, delete version, and
      absence of visible state and position fields.

13. **State and position cross-stream order**
    - Given the same vehicle state and positions are projected
    - When all valid events are interleaved in any order across both topics
    - Then the canonical final cache is identical.

14. **Snapshot cannot overwrite newer streaming information**
    - Given a snapshot row at state version N and a streaming update at N+1
    - When the N+1 update is applied before the delayed snapshot record
    - Then N+1 remains the state; position snapshot/stream records likewise use
      the position tuple and preserve the greatest candidate.

15. **Worker restart between write and commit**
    - Given a Valkey update succeeds but the Worker stops before committing its
      Kafka offset
    - When the Worker restarts and redelivers the event
    - Then the update is idempotent and subsequent events are not lost.

16. **Cache failure does not commit**
    - Given Valkey is unavailable or rejects a script
    - When the Worker processes a CDC event
    - Then the Kafka offset is not committed, the event is retried after
      recovery, and the error is visible.

17. **Rebalance/commit failure replays safely; invalid CDC blocks**
    - Given a transient offset commit failure after cache success, partition
      ownership loss, or an invalid CDC record
    - When the partition is reassigned/restarted
    - Then transient commits retry only the same offset while ownership holds;
      unresolved failures stop the Worker and already-applied events may replay
      idempotently; processing does not commit past unfinished work, and
      invalid CDC remains uncommitted and visibly blocks rather than being
      silently skipped.

18. **Full replay equals the prior logical cache**
    - Given both topics retain the complete history from their declared
      per-partition baseline
    - When the Worker stops, the vehicle-key namespace is cleared, all
      partitions seek to their earliest retained baseline, and both topics are
      replayed
    - Then the sorted canonical vehicle hashes and comparison markers equal
      the pre-replay logical cache; Kafka offsets are verified separately and
      volatile/generation metadata is excluded.

19. **Retention loss blocks replay before cache deletion**
    - Given required CDC records have expired from Kafka
    - When beginning offsets are checked before a rebuild
    - Then recovery fails closed before clearing Valkey and reports that
      identical replay is unsupported.

20. **Suspected cache loss is recovered offline without PostgreSQL**
    - Given the Valkey cache is suspected lost and Kafka still retains the
      complete replay baseline
    - When an operator stops the Worker, verifies the retained baselines,
      clears the projection and progress, and starts a fresh consumer group
      with no prior Kafka commits
    - Then offline replay rebuilds the same canonical cache without querying
      PostgreSQL. Partial key loss is not automatically detected.

21. **Debezium envelope parsing and timestamp precision**
   - Given connector JSON with schemas disabled, source metadata, a vehicle
     or position key, and either ISO-8601 `ZonedTimestamp` or integer
     epoch-microsecond timestamps
   - When the parser handles insert, update, snapshot, delete, or tombstone
     records
   - Then it returns the typed event, preserves source cursors as metadata,
     normalizes timestamps to UTC microseconds, and rejects malformed or
     unsupported events.

22. **Commit follows atomic projection and progress cursor**
   - Given an assigned CDC record
   - When the Worker applies its Lua script successfully
   - Then the per-topic/partition next-offset cursor is recorded atomically
     with the projection (including tombstones), and only then is the Kafka
     offset synchronously committed.

23. **Startup rejects missing cache progress or expired baseline**
   - Given Kafka has committed offsets but the Valkey cursor is missing or
     behind, or a topic's earliest offset/partition count differs from the
     configured baseline
   - When the Worker is assigned partitions
   - Then it stops before processing and requires offline recovery. A group
     without a committed offset explicitly seeks to the configured baseline;
     committed offsets outside the retained range fail closed.

24. **End-to-end deterministic run, restart, duplicate delivery, and full replay**
   - Given `just e2e` is run from the configured devenv shell
   - When it recreates only the `vehicle-cache` namespace, waits for the
     Debezium snapshot/stream readiness, and writes positions before vehicle
     creation
   - Then the Worker projects those positions, is restarted, and subsequently
     merges the deterministic vehicle transitions and post-delete position.
     Assertions require state version 3 to beat injected stale version 2,
     duplicate source inserts to be idempotent, position-before-vehicle data
     to merge, and deletion plus its Kafka tombstone to leave a terminal
     deleted hash without visible state or position fields.
   - When a distinct fresh group replays from the earliest offsets over the
     existing cache
   - Then the canonical Valkey vehicle hashes remain identical.
   - When the retained beginning offsets are verified as zero, the Worker is
     stopped, only `vehicle:*` hashes are cleared, and a second fresh group
     consumes both topics again from offset 0
   - Then every expected projection field is reasserted and the sorted
     canonical cache is identical to the pre-replay cache. Worker and
     port-forward processes are stopped on success or failure; the namespace
     remains available for inspection.

The unit tests cover deterministic writer fixture generation, shared vehicle
identifiers, duplicate fixture data, writer configuration, Debezium envelope
parsing, timestamp normalization, invalid CDC, Worker configuration
validation, and bounded Kafka commit recovery. They do not execute Kafka
commits, PostgreSQL, or Debezium. The seven Valkey integration tests execute
the production Lua projection against the active Valkey service. Partial loss
of individual vehicle keys is not automatically detected.

The separate `VehicleCache.IntegrationTests` project executes the production
Lua script against Valkey and covers marker initialization, vehicle updates,
position-before-vehicle, stale and duplicate candidates, equal-time UUID
ordering, terminal deletion, tombstones, progress advancement, and rejection
without partial mutation. All 7 tests passed against the deployed Valkey
service in the latest run. Future runs require
`VEHICLE_CACHE_TEST_VALKEY_CONNECTION_STRING` and must be run explicitly:

```powershell
dotnet test tests\VehicleCache.IntegrationTests\VehicleCache.IntegrationTests.csproj
```

The `just e2e` workflow automates behavior 24 using the deterministic writer
scenarios, production Worker, and production Valkey projection. It uses
separate fresh consumer groups for duplicate replay and cache-cleared full
replay, validates retained offsets before clearing the projection, compares
every sorted vehicle hash and field, and does not delete the Kubernetes
cluster. The latest clean E2E passed the full writer-to-cache assertions and
cache-cleared replay from offset 0, rebuilding the identical canonical cache.
