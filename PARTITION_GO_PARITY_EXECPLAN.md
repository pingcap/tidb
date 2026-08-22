# Bring the Rust partition implementation to behavioral parity with Go

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` (this file's format authority) at repository root; this plan must be maintained according to it.

## Purpose / Big Picture

TiDB is being transcreated from Go into Rust under `rust/`. The Go implementation in `pkg/` is the source of truth: for any SQL statement, the Rust node must return the same rows, the same errors with the same error numbers, and the same `SHOW CREATE TABLE` text as a Go TiDB cluster. Where the two differ, Rust is wrong by definition.

This plan covers the table **partitioning** subsystem: `PARTITION BY HASH / KEY / RANGE / RANGE COLUMNS / LIST / LIST COLUMNS`, the metadata a partitioned table stores, how a row is routed to a partition, how partitions are pruned from a query, and the `ALTER TABLE ... PARTITION` operations.

After this plan is complete, a person can run any partition-related statement against the Rust node and against a Go TiDB cluster and get identical observable results, and can point at a test that proves each rule was checked against Go rather than against someone's belief about Go.

To observe the current state, run the partition suites from `rust/`:

    cargo nextest run -p tidb-executor -p tidb-session -E 'test(partition)' --no-fail-fast

At the time of writing this reports 112 passed, 1 failed, where the single failure (`tests_analyze::static_partition_analyze_keeps_statistics_on_physical_partitions`) arrived with an upstream merge and is unrelated to partitioning logic.

## Context and Orientation

A **partition** is a horizontal slice of a table; a partitioned table stores each row in exactly one partition, chosen by a **partitioning method** applied to one or more columns. **Routing** means deciding which partition a row belongs to. **Pruning** means deciding, at query planning time, that a partition cannot contain any row matching the query and can be skipped. An **errno** is MySQL's numeric error code (for example 1493 is "VALUES LESS THAN value must be strictly increasing for each partition"); clients match on these numbers, so returning the right condition with the wrong number is still a bug.

The Go side lives mainly in these files, all relative to repository root:

- `pkg/ddl/partition.go` — building partition metadata from a `CREATE TABLE` statement, validating it, and rendering it back out for `SHOW CREATE TABLE`. Key functions: `buildTablePartitionInfo`, `buildPartitionDefinitionsInfo`, `checkPartitionDefinitionConstraints`, `checkPartitioningKeysConstraints`, `checkPartitionFuncValid`, `checkPartitionExprArgs`, `checkPartitionExprAllowed`, `AppendPartitionInfo`, `AppendPartitionDefs`, `hexIfNonPrint`, `generatePartValuesWithTp`, `isPartExprUnsigned`.
- `pkg/ddl/create_table.go` — `buildTableInfoWithCheck` and `checkTableInfoValidWithStmt`, which establish the two-phase order in which partition checks fire.
- `pkg/table/tables/partition.go` — loading stored metadata back into a live table and routing rows. Key functions: `newPartitionedTable`, `newPartitionExpr`, `generateRangePartitionExpr`, `generateListPartitionExpr`, `generateKeyPartitionExpr`, `generateHashPartitionExpr`, `dataForRangePruning`, `locatePartitionCommon`, `locateRangePartition`, `locateListPartition`, `LocateKeyPartition`, `NewPartitionExprBuildCtx`.
- `pkg/meta/model/table.go` — the persisted `PartitionInfo` and `PartitionDefinition` structures.

The Rust side lives under `rust/crates/`:

- `tidb-executor/src/ddl/table_partition.rs` — the main partition DDL module: builds metadata on `CREATE TABLE`, loads it back, and renders `SHOW CREATE TABLE` text.
- `tidb-executor/src/ddl/table_partition_range.rs` and `table_partition_list.rs` — the RANGE and LIST specifics.
- `tidb-executor/src/ddl/alter_table.rs` — `ALTER TABLE ... ADD / DROP / TRUNCATE PARTITION`.
- `tidb-executor/src/partition_routing.rs` — `PartitionSpec`, `PartitionKind`, and the routing functions.
- `tidb-executor/src/partition_pruning.rs` — pruning.
- `tidb-executor/src/kv_table/partition_maintenance.rs` — physical partition bookkeeping.
- `tidb-session/src/show.rs` — assembles `SHOW CREATE TABLE` output.
- `tidb-session/src/tests_partition.rs` — the partition test suite.

A **build mode** distinction runs through the Rust code as `PartitionBuildMode::Create` versus `PartitionBuildMode::Load`. This mirrors a fact about Go that is easy to miss and expensive to get wrong: Go validates partition metadata when a `CREATE TABLE` or `ALTER TABLE` writes it, and does **not** re-validate when loading that metadata back from the catalog. A Rust load path that re-runs creation-time checks will refuse to open tables that a Go cluster serves perfectly well, which presents to a user as the table having vanished.

## The method this plan requires

The single most important instruction in this plan is about method, because the obvious method has already been measured and it fails.

A first verification pass over seven partition rule groups was run by re-deriving each rule directly from the Go source, independently of the Rust code, and then attempting to refute every claimed difference with a second reader. It found **twenty-six** real divergences. Every one of them was in code that had been written carefully, carried comments asserting it matched Go, cited Go file and line numbers, and passed the entire test suite. Reading the Rust code and asking "does this look right?" found none of them, because the author's misunderstanding of Go was faithfully reproduced in both the code and its comments.

Therefore: **do not verify by inspecting the Rust code.** For each rule, open the Go source, derive the rule from it, and only then compare. Treat comments in the Rust code as claims to be checked, not as evidence. Where a difference is claimed, have a second reader try to prove the claim wrong before acting on it — of the twenty-six confirmed findings, that refutation step is what separated them from noise.

## Progress

- [x] (2026-08-21) Round-one verification of seven rule groups: expression argument rules, key-with-empty-column-list, range routing, list routing, metadata load, `SHOW CREATE TABLE`, names and counts. Twenty-six divergences confirmed after adversarial review.
- [x] (2026-08-21) All twenty-six fixed; partition suites at 112 passed / 1 pre-existing unrelated failure.
- [x] (2026-08-21) Two regression tests added to `rust/crates/tidb-session/src/tests_partition.rs`: one asserting seventeen statements where the engines previously disagreed, one asserting three `SHOW CREATE TABLE` shapes.
- [ ] M0 — Land the completed work (gate, commit, push, reclaim disk).
- [ ] M1 — Verify routing and pruning against Go (routing COMPLETE: all six paths match. Pruning: the mixed-signedness divergence is found and FIXED, covering both the RANGE and LIST comparison paths; the remaining LIST-specific pruning structures -- per-column value maps and the DEFAULT partition's interaction with pruning -- are not yet examined).
- [ ] M2 — Verify the stored metadata round trip and physical identity against Go.
- [ ] M3 — Verify `ALTER TABLE ... PARTITION` against Go.
- [ ] M4 — Verify expression restore text and INTERVAL partitioning against Go.
- [ ] M5 — Re-verify the twenty-six landed fixes against Go.
- [ ] M6 — Partition `PLACEMENT POLICY` end to end (model layer already complete; remaining: policy objects, metadata reference, bundle construction wiring, PD delivery).
- [ ] M7 — Keep-order and unsigned-handle range split (gated on M8).
- [x] (2026-08-21) M8 — The upstream constant-overflow regression no longer reproduces and is pinned by a differential test instead of reported.

## Surprises & Discoveries

- Observation: Every one of the twenty-six divergences passed the existing test suite. Tests written from the same misunderstanding as the code cannot detect that misunderstanding.
  Evidence: partition suites reported 90/91 passing both before and after fixes that changed eight distinct user-visible behaviors.

- Observation: Go's partition checks run in two separate phases, and collapsing them changes which error a user sees.
  Evidence: `pkg/ddl/create_table.go` calls `BuildTableInfoWithStmt` (which runs `buildTablePartitionInfo` to completion) and only afterwards `checkTableInfoValidWithStmt`. So `CREATE TABLE t (a INT, b INT, KEY k(b)) PARTITION BY KEY () PARTITIONS 10000` reports 1499 in Go, because the partition-count cap fires in phase one, while the "no key can serve KEY()" refusal lives in `checkPartitioningKeysConstraints`, nearly last.

- Observation: Go interleaves per-definition checks; it fully processes definition N before looking at definition N+1.
  Evidence: `buildPartitionDefinitionsInfo`'s RANGE loop validates a definition's values, then its comment length, then its name length, then moves to the next. A statement whose first partition has an over-long name and whose second has a non-increasing bound reports 1059, not 1493.

- Observation: Go's loader deliberately re-judges nothing.
  Evidence: `newPartitionExpr` in `pkg/table/tables/partition.go` returns `nil, nil` for `PartitionTypeNone` rather than erroring, and `dataForRangePruning` reads stored bounds with `strconv.ParseInt` rather than evaluating them as expressions, so no creation-time verdict can be re-reached at load time.

- Observation: A subagent given permission to run probes will write files into the working tree and can destroy uncommitted work.
  Evidence: on 2026-08-21 a verification workflow overwrote `rust/crates/tidb-executor/src/ddl/table_partition.rs`, discarding twenty-nine uncommitted edits. The content was not in git; it was rebuilt by replaying the edits. All fourteen of that workflow's journal entries were `started`, so it destroyed the work before producing a single finding.

- Observation: On macOS, `cargo nextest` runs one process per test, and macOS Sequoia's provenance tracking in `syspolicyd` serializes executable assessments, so parallel test launches convoy.
  Evidence: the identical 7.4 MB binary took 0.00s to launch on an idle machine, 32.71s while a gate was running, and 175.64s as a fresh copy under the same load. A full-workspace gate ran 97 minutes while `cargo nextest` itself accumulated 6.6 seconds of CPU. Restarting the host application so its Developer Tools exemption applied returned fresh-binary launches to roughly 0.13s.

- Observation (M1, confirmed): Go's range PRUNING compares a bound and a query constant using their two INDEPENDENT signedness flags; the Rust pruning uses one flag for both operands and discards the constant's own signedness, so the two engines prune differently for any constant above the maximum signed 64-bit integer. This is the silent-wrong-rows class.
  Evidence: `LessThanDataInt.compare` (`pkg/planner/core/rule/rule_partition_processor.go:932`) ends in `types.CompareInt(lt.Data[ith], lt.Unsigned, v, unsigned)`, where `lt.Unsigned` is the partitioning COLUMN's flag (set at `:1077` from `col.GetStaticType()`) and `unsigned` is the query CONSTANT's flag (set at `:1645` from `constExpr.GetType()`). `types.CompareInt` (`pkg/types/compare.go`) handles all four sign combinations explicitly, including `!isUnsigned0 && isUnsigned1`, which returns -1 when `uint64(arg1) > math.MaxInt64`. On the Rust side `integer_endpoint` (`rust/crates/tidb-executor/src/partition_pruning.rs`) maps `Datum::UInt(v)` to `v as i64` and throws the flag away, after which `less`, `scalar_in_interval`, and `range_meets_partition` apply a single `unsigned` to both operands. Worked case: over `PARTITION BY RANGE (a)` with `a BIGINT` signed and bounds 10 and 20, the predicate `a < 18446744073709551615` gives Go `CompareInt(10, false, -1, true)` = -1, meaning the bound is below the constant and no partition is pruned, whereas Rust flattens the constant to -1 and compares signed against 10, reaching the opposite verdict.
  Resolution (2026-08-21): FIXED by porting `types.CompareInt` verbatim as `compare_int` in `rust/crates/tidb-executor/src/partition_pruning.rs`, widening `ScalarRangeEndpoint` to carry each endpoint's own signedness, and making `integer_endpoint` return that flag instead of discarding it. Both pruning paths now compare with two flags as Go does: `scalar_in_interval` for LIST and `range_meets_partition` for RANGE, which had the identical defect. The superseded single-flag `less` helper is deleted rather than left beside the new rule, because a correct rule sitting next to an incorrect one changes nothing. Covered by `compare_int_matches_gos_four_signedness_cases`, whose expectations are read off Go's four branches, and `a_constant_above_i64_max_does_not_prune_a_signed_partition`, which asserts the old and new verdicts actually differ.
  Note on the gating: this was originally deferred behind M8 on the theory that it shared a root cause with the upstream constant-overflow regression. That was over-cautious. The comparison rule is a self-contained port and is unit-testable without any end-to-end query, so the upstream bug blocks neither writing it nor proving it correct. M8 remains outstanding on its own terms.

- Observation (M1, complete): ALL SIX routing paths match Go. RANGE: `locateRangePartition`'s single-flag `ForRangePruning.Compare`, NULL to partition 0, MAXVALUE consulted only at the last position, and the signed/unsigned 1526 formatting are all reproduced by `range_partition_index`/`range_bound_exceeds`. HASH: the bare-column fast path's `KindInt64`/`KindUint64` shortcut and `ConvertTo(LongLong)` fallback, NULL reading as 0, and the negate-if-negative modulo all match `hash_partition_index`; Go's `-ret` and Rust's `unsigned_abs` differ only at `i64::MIN`, which `x % n` for a partition count under 8192 cannot produce. KEY: IEEE CRC32 over `ToHashKey` with a NULL writing a single zero byte, matching `key_partition_index_for_tuple`; Go's modulo in `uint32` and Rust's in `u64` agree for any legal partition count. LIST: the NULL-then-DEFAULT precedence and both 1526 message forms match; Go's BTree key encoding (`EncodeIntToCmpUint` for signed, raw for unsigned) is a bijection, so its exact-match lookup is equivalent to Rust's linear search over raw values. RANGE COLUMNS: the first-bound-exceeding search, MAXVALUE, collation-aware comparison, and the literal `from column_list` message all match. LIST COLUMNS: Go intersects PER-COLUMN candidate sets, which looks weaker than Rust's whole-tuple hash until one reads `ListPartitionGroup`, whose `GroupIdxs` name WHICH value tuple within the partition matched — so the intersection is exact tuple matching after all, and the two agree.
  Evidence: `pkg/table/tables/partition.go` `locateRangePartition`, `locateHashPartition`, `LocateKeyPartition` (:361), `ForListPruning.LocatePartition`, `locateListPartitionByRow` (:1114), `locateRangeColumnPartition` (:1503), `locateListColumnsPartitionByRow`, and `ListPartitionGroup`, read against `rust/crates/tidb-executor/src/partition_routing.rs`.

- Observation (M1): range ROUTING does match Go. Go's `ForRangePruning.Compare` takes a single `unsigned` and applies it to both operands, which is what the Rust `range_bound_exceeds` does; NULL routing to partition 0, the MAXVALUE-only-at-the-last-position rule, and the signed/unsigned formatting of the 1526 message all agree. The divergence is confined to the pruning path, which uses a different Go type (`LessThanDataInt`) with different rules.
  Evidence: `locateRangePartition` and `ForRangePruning.Compare` in `pkg/table/tables/partition.go` read side by side with `range_partition_index` and `range_bound_exceeds` in `rust/crates/tidb-executor/src/partition_routing.rs`.

- Observation (M2, gap recorded, NOT fixed): the executor ignores a partition's mid-DDL state, so this node reads and writes a partition that a Go cluster is in the middle of dropping. The stored metadata is not corrupted -- `crates/tidb-model/src/partition.rs` deserialises `adding_definitions`, `dropping_definitions`, `ddl_action` and `ddl_state` and writes them back unchanged -- but nothing between `PartitionSpec` and routing consults them.
  Evidence: Go gates the whole behaviour on `CanHaveOverlappingDroppingPartition` (`pkg/meta/model/table.go:1042`), which is true ONLY while `DDLAction == ActionDropTablePartition && DDLState == StateWriteOnly`. In that window `GetOverlappingDroppingPartitionIdx` (`:1083`) redirects a read away from a partition being dropped -- for RANGE to the next non-dropping partition, for LIST to the DEFAULT partition, and to -1 when neither exists -- while writes are deliberately blocked. `locatePartitionCommon` applies it for LIST (`pkg/table/tables/partition.go:1448`) and the list pruner applies it per index (`rule_partition_processor.go`). A search of `crates/tidb-executor`, `crates/tidb-exec` and `crates/tidb-server` finds no reader of any of those four fields.
  Severity and scope: this node performs DDL transactionally rather than through Go's staged job state machine, so it never PRODUCES this state; it can only be reached by loading a table from a Go cluster whose DROP PARTITION is in flight. The consequence is reading a partition Go would redirect away from during that window, not a wrong answer on a settled table and not catalog damage. Filling it means modelling `DDLAction`/`DDLState`/`IsDropping` through `PartitionSpec` and consulting them in routing and pruning -- a self-contained port, but larger than the divergences fixed so far, and it is recorded here rather than started so it is not mistaken for done.

- Decision: PD delivery is implemented as `POST /pd/api/v1/config/placement-rule?partial=true` carrying a JSON array of bundles, delivered BEFORE the catalog change commits and aborting the statement if it fails.
  Rationale: the endpoint, method and `partial=true` flag are read from PD's own client source (`client/http/api.go`: `PlacementRuleBundle = "/pd/api/v1/config/placement-rule"`, `PlacementRuleBundleWithPartialParameter` appending `?partial=%t`; `client/http/interface.go`: `SetPlacementRuleBundles` marshals `[]*GroupBundle` and POSTs it), and TiDB calls it with `partial=true` (`PDPlacementManager.PutRuleBundles`). The ordering and failure handling are Go's: it delivers inside the DDL job before the schema version is published and fails the job when delivery fails, so the transactional analogue is deliver-then-commit, abort on failure. The Rust `Bundle` already serialises to PD's shape -- `group_id`, `group_index`, `group_override`, `rules` -- so no wire type is needed.
  Date/Author: 2026-08-21, ngaut.

- Decision: PD delivery is sequenced AFTER cluster-side policy storage, not before it.
  Rationale: PD exists only on the cluster path, and placement is currently refused there because the cluster tier keeps no policies -- so no table on that path can carry a policy for a bundle to be built from. Delivering bundles for objects that cannot exist would be untestable. Cluster-side policy storage (a catalog key, the DDL transaction, a schema-version bump) is the prerequisite.
  Date/Author: 2026-08-21, ngaut.

- Correction: an earlier note treated the PD wire contract and the delivery failure semantics as questions needing an outside answer. Neither did. The contract is public source and was fetchable; the semantics follow from Go being the source of truth. Recorded because the failure mode -- turning "this needs verifying" into "this needs deciding" -- stalls work that is not actually blocked.
  Date/Author: 2026-08-21, ngaut.

- Observation (M2, gap recorded, NOT fixed): the cluster path records nothing in `mysql.gc_delete_range`, so the data behind a dropped or truncated table is never reclaimed. Reads are correct -- a dropped table's meta key is gone and a truncated one's ids are new, so nothing addresses the old bytes -- but they stay on disk for the life of the cluster.
  Evidence: Go's `delRange.addDelRangeJob` (`pkg/ddl/delete_range.go:95`) writes a row per range, and its `ActionDropTable` arm (`:296`) deletes every partition id AND the logical table id, noting that the latter "may contain global index regions"; `ActionTruncateTable` (`:312`) does the same for the OLD partition ids plus the table id, always including the table range even for a partitioned table. `ActionDropTablePartition` (`:327`) covers the partition case. A search of `crates/tidb-exec/src/cluster_ddl.rs` and `real_tikv_ddl.rs` finds no delete-range writer at all.
  Scope: this is not partition-specific -- an unpartitioned DROP TABLE leaks its rows the same way -- but partitioned tables leak more of it, one range per partition. Filling it means writing rows to a system table from the DDL transaction, which is a wider piece of work than the partition rules this plan covers, and it is recorded here rather than started so it is not mistaken for done.

- Observation (M8, resolved): the upstream regression this plan owed a report on no longer reproduces. `SELECT count(*) FROM ix WHERE u >= 9223372036854775808` now answers 2 on BOTH the index and the scan path, and `e = 18446744073709551615` answers 1 on both; the answers are also correct for the fixture. It previously returned 5 -- every row -- on the scan path, which is the signature of a predicate being LOST rather than mis-evaluated.
  Evidence: measured directly, then pinned as `a_constant_above_i64_max_filters_the_same_on_both_paths`. Not fixed by the `compare_int` work in this plan: that lives in partition pruning and the fixture table is not partitioned, so it must have arrived with an upstream merge. Recorded rather than reported, since there is nothing left to report.
  Note on why it is pinned as a DIFFERENTIAL rather than a value assertion: each path alone looked plausible when this regressed, and only running one question down two paths that must agree showed that one of them was not filtering at all.

## Decision Log

- Decision: Verification is done by re-deriving each rule from the Go source, with an independent second reader attempting to refute each claimed difference; the Rust code and its comments are never treated as evidence.
  Rationale: inspection-based review missed all twenty-six divergences, which were found only by this method.
  Date/Author: 2026-08-21, ngaut.

- Decision: Verification subagents must run with `isolation: "worktree"` and must be given read-only instructions, and the working tree must be committed before they start.
  Rationale: an agent destroyed uncommitted work; and because a git worktree is cut from committed state, verifying before committing would have the agents reading stale code.
  Date/Author: 2026-08-21, ngaut.

- Decision: Partition `PLACEMENT POLICY` is implemented end to end, including enforcement — the bundle must reach PD, not merely appear in `SHOW CREATE TABLE`.
  Rationale: a policy that prints but does not place data is a correctness claim the node cannot honour, so a half-implementation is worse than none; the model layer in `rust/crates/tidb-placement` is already complete, which makes the end-to-end path tractable. Supersedes an earlier provisional decision to leave the feature absent.
  Date/Author: 2026-08-21, ngaut.

- Decision: M6 is built in four ordered layers — policy objects, metadata reference, bundle construction, PD delivery — each independently testable.
  Rationale: the PD delivery layer requires a placement-rule API that `rust/crates/tidb-pd-client` does not yet have and is the least predictable in size; the earlier layers are worth having on their own and are prerequisites regardless.
  Date/Author: 2026-08-21, ngaut.

- Decision: The unsigned-handle range split is deferred until the upstream constant-overflow regression is resolved.
  Rationale: it touches the same code path that currently carries an upstream wrong-answer bug, so changes there cannot be validated cleanly.
  Date/Author: 2026-08-21, ngaut.

- Decision: Gates are scoped to the crates a change touches rather than `--workspace`.
  Rationale: `--workspace` relinks hundreds of test binaries, and on this host each fresh binary pays a serialized provenance assessment; the scoped gate covers every modified line at a fraction of the cost.
  Date/Author: 2026-08-21, ngaut.

## Plan of Work

The work proceeds in three movements: land what is already proven, verify the surface that has never been checked, then close the gaps that are known and deliberate.

**Landing the completed work** is first because everything else depends on it. The twenty-six fixes are written and passing but uncommitted, and verification agents read from committed state, so an uncommitted tree both risks the work and misdirects the verification.

**Verifying the unchecked surface** is the substance of the plan. Roughly half of the partition implementation has never been compared against Go: routing, pruning, `ALTER TABLE` partition operations, physical identity and key encoding, the metadata round trip, INTERVAL partitioning, and the text form a partition expression is stored as. Round one found twenty-six divergences in the half that was checked; there is no reason to expect the unchecked half to be cleaner. These are grouped into milestones by failure class rather than by file, because failure class determines urgency: a pruning bug silently returns wrong rows, whereas a wrong errno returns the right outcome with the wrong label.

**Closing the deliberate gaps** comes last because each is a known, bounded piece of missing functionality rather than an unknown risk.

Within each verification milestone the shape is the same. For each area, an agent re-derives the rule from the named Go functions, compares against the named Rust code, and reports differences with a concrete reproducing statement and the answer each engine gives. A second agent then attempts to refute each claimed difference. Surviving findings are fixed in the Rust source, each with a regression test whose expectation is read out of Go, and each fix is confirmed by the scoped gate before the next milestone begins.

## Concrete Steps

All commands assume working directory `/Users/qiliu/projects/tidb` unless stated otherwise. Rust commands assume `/Users/qiliu/projects/tidb/rust`.

### M0 — Land the completed work

From `rust/`, run the scoped gate covering every modified crate:

    cargo nextest run -p tidb-executor -p tidb-session -p tidb-proto --no-fail-fast

Expect a summary line naming the number passed and failed. The failure set must contain only `tests_analyze::static_partition_analyze_keeps_statistics_on_physical_partitions`, which is a pre-existing upstream failure unrelated to partitioning. Any other failure must be resolved before committing.

Read the gate result in one step and commit in the next; never chain them with a semicolon, so that a failure cannot be committed by accident.

Commit the proto fixture separately from the partition work, because they are unrelated. Use a message file rather than `-m`, because commit messages here contain backticks that a shell would evaluate:

    git -c user.name=ngaut -c user.email=ngaut@users.noreply.github.com commit -F <message-file>

Push to both remotes, never force:

    git push origin hparser-integration
    git push ngaut hparser-integration

Then reclaim disk. `rust/target/debug/deps` is 81 GB of accumulated build artifacts; a `.metadata_never_index` marker already excludes the tree from Spotlight:

    cargo clean

### M1 — Verify routing and pruning

This milestone covers the highest-severity failure class: a routing or pruning bug returns wrong rows with no error at all.

For routing, derive from `pkg/table/tables/partition.go`: `locatePartitionCommon`, `locateRangePartition`, `locateListPartition`, `locateListColumnsPartition`, `LocateKeyPartition`, `locateHashPartition`, and the `PartitionExpr` structures they read. Compare against `rust/crates/tidb-executor/src/partition_routing.rs`. Pay attention to the binary-search bounds and the `MaxValue` flag in range location, NULL handling in each method, unsigned comparison, the hash function's behavior on negative values, the KEY encoding through `Datum.ToHashKey` and CRC32, and the exact conditions under which errno 1526 is raised including `GetOverlappingDroppingPartitionIdx`.

For pruning, derive from `pkg/planner/core/rule/rule_partition_processor.go`, whose entry points are methods on `PartitionProcessor`: `PruneHashOrKeyPartition` at line 447, `ConvertToIntSlice` at line 413, and the `listPartitionPruner` family beginning at line 542 — `locatePartition`, `locatePartitionByCNFCondition` at line 594, `locatePartitionByDNFCondition` at line 623, `locatePartitionByColumn`, and `detachCondAndBuildRange`. The range side uses `partitionRangeForOrExpr` at line 1455 and `partitionRangeForInExpr` at line 1498, over the `PartitionRangeOR` type. These read the structures `ForRangePruning`, `ForRangeColumnsPruning`, and `ForListPruning` built in `pkg/table/tables/partition.go`. Compare against `rust/crates/tidb-executor/src/partition_pruning.rs`. Weight most heavily any case where the Rust prunes a partition that Go keeps, and check NULL semantics at the lowest range partition, the DEFAULT list partition, unsigned bounds, and multi-column RANGE COLUMNS prefixes.

Acceptance: for every difference found, a test in `rust/crates/tidb-session/src/tests_partition.rs` that fails before the fix and passes after, asserting the row set or errno that Go produces.

### M2 — Verify the metadata round trip and physical identity

This milestone covers the corruption class: a field silently dropped when metadata is loaded and stored back will damage a Go cluster's catalog.

Derive from `pkg/meta/model/table.go` the complete field list of `PartitionInfo` and `PartitionDefinition`, including their JSON tags, and the helper methods on `PartitionInfo` in that file: `Clone` at line 891, `GetNameByID` at 915, `GetStateByID` at 928, `GCPartitionStates` at 953, `ClearReorgIntermediateInfo` at 974, `FindPartitionDefinitionByName` at 985, `GetDefaultListPartition` at 1009, `CanHaveOverlappingDroppingPartition` at 1042, `ReplaceWithOverlappingPartitionIdx` at 1063, and `GetOverlappingDroppingPartitionIdx` at 1083. Compare against `StoredPartitionMetadata` and `StoredPartitionDefinition` in `rust/crates/tidb-executor/src/ddl/table_partition.rs`, `PartitionSpec` and `PartitionDef` in `rust/crates/tidb-executor/src/partition_routing.rs`, and the catalog serialisation path (locate it by searching the Rust tree for the Go JSON field names).

For every Go field, establish whether Rust carries it, whether the JSON name matches exactly, and if it is dropped, what breaks. Enumerate dropped fields explicitly in the findings rather than reporting an overall verdict.

Separately, derive how partition IDs are allocated and used, how a partitioned table's rows and indexes are keyed in `pkg/tablecodec/`, and how global indexes differ from local ones. Compare against the partition paths under `rust/crates/tidb-executor/src/kv_table/`. A disagreement between the read path and the write path here presents as lost or invisible rows.

Acceptance: a test that writes partition metadata, reads it back, and asserts field-for-field equality including every field Go persists; plus a test that a row written to a partition is found by a read of that partition.

### M3 — Verify ALTER TABLE partition operations

These entry points are split across two Go files, which is worth knowing before searching. The statement-level entry points are methods on `executor` in `pkg/ddl/executor.go`: `AddTablePartitions` at line 2273, `TruncateTablePartition` at line 2825, and `DropTablePartition` at line 2897; `BuildAddedPartitionInfo`, which turns the `ALTER` clause into partition metadata, is also there at line 5683. The validation helpers and the asynchronous job workers live in `pkg/ddl/partition.go`: `checkAddPartitionValue` at line 428, `checkAddPartitionNameUnique` at line 1768, `checkAddPartitionTooManyPartitions` at line 4650, the exported `CheckDropTablePartition` at line 2087, and the workers `onAddTablePartition` at line 94 and `onDropTablePartition` at line 2304.

Derive the rules from those, and compare against the partition paths in `rust/crates/tidb-executor/src/ddl/alter_table.rs`.

Check every validation Go performs, in Go's order, with its errno; the handling of `IF EXISTS` and `IF NOT EXISTS`; what happens when the last partition is dropped; and whether any Go check has no Rust counterpart at all.

Acceptance: statements exercising each validation, asserting Go's errno.

### M4 — Verify expression restore text and INTERVAL

The text form of a partition expression is what `CREATE TABLE` stores, what the loader parses back, and what `SHOW CREATE TABLE` prints. If Rust stores a different spelling than Go, a Go cluster and this node disagree about what the table is partitioned by.

Derive from the restore call in `buildTablePartitionInfo` at `pkg/ddl/partition.go`, which uses `format.DefaultRestoreFlags` combined with `RestoreBracketAroundBinaryOperation`, `RestoreWithoutSchemaName`, and `RestoreWithoutTableName`. Compare against `partition_restore_flags` in `rust/crates/tidb-executor/src/ddl/table_partition.rs` and `Expr::restore_with_flags` in `rust/crates/tidb-ast/src/expr/restore.rs` at line 276. Cover bare and qualified columns, binary operations and their bracketing, unary minus, function calls, `EXTRACT`, nested calls, literals of each type, and backtick-quoted identifiers.

For INTERVAL partitioning, derive from `generatePartitionDefinitionsFromInterval` and `getPartitionIntervalFromTable`. Establish whether Rust supports it. If it does not, confirm the refusal is loud; a silent fallback that builds a differently-partitioned table than the user asked for is not acceptable.

Acceptance: a round-trip test asserting stored text equals Go's for each expression shape, and either INTERVAL support with tests or an explicit refusal with a test asserting it.

### M5 — Re-verify the twenty-six landed fixes

The fixes were written by the same author whose previous twenty-six rules were all wrong, so they receive the same scrutiny as unverified code. Re-derive each from Go and attempt to break the new implementation, covering the `EXTRACT` and argument rules, the two-phase check order, the load-path gating, the `SHOW CREATE TABLE` renderer, error identity and name folding, comment handling and the metadata-load context, `PartitionTypeNone`, KEY column resolution, and LIST value folding.

Acceptance: each area either confirmed to match Go, or a difference found, fixed, and covered by a test.

### M6 — Partition PLACEMENT POLICY, end to end including enforcement

A **placement policy** is a named object describing where a table's or partition's replicas should physically live — for example `CREATE PLACEMENT POLICY p FOLLOWERS=4` or a policy constraining replicas to a region. TiDB implements it by translating the policy into **placement rules**, grouping them into a **bundle** keyed by the object's key range, and sending that bundle to **PD** (the Placement Driver, TiDB's cluster metadata and scheduling service). PD then schedules replicas to satisfy the rules. Without that final step the policy is decorative: it appears in `SHOW CREATE TABLE` but no data moves.

A survey of the current tree establishes that this work is five layers, of which the hardest is already done.

The **model layer is complete**. `rust/crates/tidb-placement/` is 4,627 lines porting Go's `pkg/ddl/placement/`, and already exposes `new_bundle_from_options`, `new_bundle_from_constraints_options`, `new_bundle_from_sugar_options`, `new_table_bundle`, `new_partition_bundle`, `new_partition_list_bundles`, and `new_full_table_bundles` in `src/bundle.rs`, mirroring Go's `NewTableBundle`, `NewPartitionBundle`, and `NewFullTableBundles` in `pkg/ddl/placement/bundle.go`.

The **parser layer is present**: `TableOption::PlacementPolicy(_)` already exists and is handled in `rust/crates/tidb-ast/src/ddl.rs`, so `PLACEMENT POLICY=<name>` on a table or partition definition parses today.

Three layers are missing, and the fourth exists but is called by nobody.

First, **policy objects do not exist**. There is no `CREATE PLACEMENT POLICY`, `ALTER PLACEMENT POLICY`, or `DROP PLACEMENT POLICY` in `rust/crates/tidb-executor/src/ddl/`, so there is nothing for a reference to point at. Derive these from Go's policy DDL (locate it by searching `pkg/ddl/` for `PlacementPolicy` job types) and store policies in the catalog as Go does, including the `information_schema.placement_policies` view if Go serves one.

Second, **the metadata reference is not carried**. Go's `model.PartitionDefinition` and `model.TableInfo` each hold a `PlacementPolicyRef *PolicyRefInfo` serialized as `policy_ref_info` (`pkg/meta/model/table.go` lines 204 and 1205). The Rust `PartitionDef` in `rust/crates/tidb-executor/src/partition_routing.rs` and the stored definition in `table_partition.rs` have no such field. Add it, populate it from `setPartitionPlacementFromOptions`'s rule at `pkg/ddl/partition.go` — note Go deliberately does **not** copy an inherited table policy down onto a partition, so that altering the policy cascades — and carry it through the load path so a policy written by a Go cluster survives a round trip through this node.

Third, **PD cannot be told**. `rust/crates/tidb-pd-client/` has no placement-rule API; enforcement requires adding the call that sends a bundle to PD's placement-rule endpoint, matching the request shape Go uses (find it by searching Go for the PD rule-bundle HTTP path). This is the layer that makes the feature real rather than cosmetic.

Fourth, the existing bundle constructors must actually be called: on `CREATE TABLE`, on `ALTER TABLE ... ADD PARTITION`, on truncate and drop where key ranges change, and on `ALTER PLACEMENT POLICY` where every referencing object's bundle must be rebuilt.

Two smaller consequences follow once the reference exists. Go's default-shape test for HASH and KEY partitions treats a partition carrying a `PlacementPolicyRef` as non-default and therefore prints the full definition list rather than the compact `PARTITIONS n` form (`AppendPartitionInfo`, `pkg/ddl/partition.go`); the Rust `hash_definitions_are_default` currently tests only names and comments and must gain the placement half. And `AppendPartitionDefs` appends `/*T![placement] PLACEMENT POLICY=<name> */` to a partition that carries one, which the Rust renderer must reproduce, including the escaping Go applies to the policy name.

Implement in that order — policy objects, then metadata reference, then bundle construction, then the PD call — because each layer is independently testable and the earlier ones are worth having even if the PD work proves larger than expected. Acceptance for the milestone as a whole is behavioral, not structural: create a policy, apply it to a partition, confirm `SHOW CREATE TABLE` matches Go's text exactly, and confirm the bundle PD receives matches the bundle a Go cluster sends for the same statement.

### M7 — Keep-order and the unsigned-handle range split

Port Go's `matchProperty` three-valued result (`PropNotMatched`, `PropMatched`, `PropMatchedNeedMergeSort`), the prefix walk with constant-column skipping, and grouped-range merge sort. Then port the unsigned-handle range split, whose Go entry point is `SplitRangesAcrossInt64Boundary` at `pkg/distsql/request_builder.go` line 575, and whose Rust counterpart `handle_column` in `rust/crates/tidb-executor/src/handle_range.rs` currently returns `None` for unsigned handles. This milestone is gated on M8, because it touches the path carrying the upstream regression.

### M8 — Report the upstream regression

Since commit `72600aadd8` ("rust: align covering aggregate pushdown with Go"), `SELECT count(*) FROM ix WHERE u >= 9223372036854775808` returns 5 rows on the scan path and 2 on the index path, and `e = 18446744073709551615` returns 2 instead of 1. Only constants greater than the maximum signed 64-bit integer are affected, and the predicate is lost rather than mis-evaluated. Report this to the owning author with the reproducing statements and the differing answers.

## Validation and Acceptance

The scoped gate is the primary check. From `rust/`:

    cargo nextest run -p tidb-executor -p tidb-session -p tidb-proto --no-fail-fast

A run is acceptable when its failure set is a subset of the known pre-existing failures, currently the single `static_partition_analyze_keeps_statistics_on_physical_partitions`. Compare failure **sets**, not counts; a run that fixes one failure and introduces another has the same count and is a regression.

For partition-focused iteration, the narrower form is faster:

    cargo nextest run -p tidb-executor -p tidb-session -E 'test(partition)' --no-fail-fast

Every fix in this plan requires a regression test that fails before the fix and passes after it, and whose expected value is read out of the Go source or measured against a Go cluster rather than derived from the Rust implementation. A test whose expectation was copied from Rust's current behavior proves only that the behavior has not changed.

The plan as a whole is complete when every milestone's verification reports either no difference from Go, or a difference that has been fixed and covered by such a test, and when the remaining gaps are recorded in `Outcomes & Retrospective` with the reason each is still open.

## Idempotence and Recovery

Verification milestones are read-only with respect to the repository and may be re-run freely. Fix milestones are ordinary source edits under version control.

Before starting any milestone that dispatches subagents, commit the working tree. Agents run in a git worktree cut from committed state, so uncommitted work is both invisible to them and, if they are not properly isolated, at risk from them.

If work is lost from the working tree, check `git stash list` and `git fsck --lost-found` first; if the content was never committed, neither will hold it, and the conversation transcript is the only remaining copy. Replay lost edits with a script that asserts the expected number of occurrences before each substitution, so that a mismatched pattern fails loudly rather than landing in the wrong place, and confirm the replay by re-running the tests that passed before the loss.

`cargo clean` is safe and repeatable at any point; it costs one full rebuild.

## Outcomes & Retrospective

Round one is complete. Twenty-six divergences between the Rust and Go partition implementations were found and fixed, spanning missing Go logic that had no Rust counterpart at all, wrong error numbers for correctly detected conditions, a check order that reported the wrong one of two simultaneous errors, creation-time validation running on the load path so that tables a Go cluster serves could not be opened, and `SHOW CREATE TABLE` output that in one case silently dropped a partition's values and in another produced text that does not re-parse.

The lesson worth carrying forward is about evidence. Every one of those twenty-six was in code that cited Go line numbers, carried comments asserting parity, and passed its tests. The comments were written by the same author as the code, from the same misunderstanding, and the tests were written to match. Only re-derivation from the Go source, by a reader who had not written the Rust, surfaced them — and the adversarial second pass is what kept the finding list honest.

Remaining gaps at this checkpoint: roughly half the partition surface, listed in M1 through M4, has never been compared against Go; the twenty-six fixes themselves have not been independently re-verified; partition `PLACEMENT POLICY` is unimplemented; the keep-order and unsigned-handle work is unstarted; and one upstream regression is unreported. Each is tracked as a milestone above.
