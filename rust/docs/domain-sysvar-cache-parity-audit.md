# domain sysvar-cache parity audit: Go `pkg/domain/sysvar_cache.go` vs `sysvar_cache.rs`

Audit date: 2026-09-05. Opening slice of the domain-surface audit.

## Inventory

All thirteen non-test Go files of `pkg/domain` have mirrored Rust
modules in `tidb-domain/src` (`domain_sysvars`, `sysvar_cache`,
`schema_checker`, `serverinfo_syncer`, `topn_slow_query`,
`historical_stats`, `ru_stats`, `plan_replayer`, `optimize_trace`,
`disttask`, `domainutil`, `cdcutil`, `replayer`), plus
`status_endpoint_claim` for the fork's own extension.

## sysvar_cache slice: VERIFIED

All six Go functions exist with their semantics: the empty-cache
rebuild trigger, the session-cache clone, the global lookup with
`ErrUnknownSystemVar` for a name outside the global view (including a
variable that has no global scope, as Go reports), `fetchTableValues`
over `mysql.global_variables`, the starter-mode
`max_allowed_packet` config override, and the rebuild that writes the
global view BEFORE running `SetGlobal` so a clamping hook answers the
unclamped string — the ordering subtlety is documented in both the
module doc and the code. Traits abstract the session pool and
restricted-SQL executor so the cache is testable without a cluster.

The remaining domain files are inventory-verified only; per-module
behavioral audits follow the same pattern as this slice.

## schema_checker slice (2026-09-05, second pass) — VERIFIED

`schema_checker.rs` mirrors `schema_checker.go` line for line: the retry
loop bounded by the retry-times atomic, `ResultSucc` returning clean,
`ResultFail` returning the validator's related change ALONGSIDE
`ErrInfoSchemaChanged` (8028, with the appended retryable mark — the Go
quirk documented in-file), `ResultUnknown` sleeping the retry interval
and re-checking, loop exhaustion yielding `ErrInfoSchemaExpired` (8027),
and the injected sleep port keeping the loop testable. 10 in-module
regressions pass. Next slices: `ru_stats`, `plan_replayer`,
`historical_stats`, `topn_slow_query`, `serverinfo_syncer`.

## ru_stats slice (2026-09-05, third pass) — VERIFIED

`ru_stats.rs` mirrors every Go function: the writer loop,
`GetLastExpectedTime`/`GetLastExpectedTimeTZ` (the day-bucketing math
reproduces Go's UTC round-trip for DST compatibility, documents Go's
three caveats, and matches Go's divide-by-zero panic on a zero
interval), `DoWriteRUStatistics`, `fetchResourceGroupStats`,
`loadLatestRUStats`/`persistLatestRUStats`, `isLatestDataInserted`,
`insertRUStats`, `GCOutdatedRecords` with its count/delete SQL builders
and loop count, plus the `next_wakeup` scheduling helper. Next slices:
`plan_replayer`, `historical_stats`, `topn_slow_query`,
`serverinfo_syncer`.

## plan_replayer slice (2026-09-05, fourth pass) — VERIFIED at function mapping

`plan_replayer.rs` mirrors all 31 Go functions: the dump-file GC with
its per-path duration rules, the plan-replayer status records
(insert/delete plus the error and success record variants), the handle
`SendTask`, the collector `CollectPlanReplayerTask`/`GetTasks`/
`removeTask`, and the running-task-key claiming discipline
(`occupyRunningTaskKey`/`releaseRunningTaskKey`/
`checkTaskKeyFinishedBefore`). The trait ports (`RestrictedSqlExecutor`,
directory walking, file deletion) keep the file-system and SQL effects
injectable. Next slices: `historical_stats`, `topn_slow_query`,
`serverinfo_syncer`.

## historical_stats + topn_slow_query slices (2026-09-05, fifth pass) — VERIFIED

`historical_stats.rs` mirrors the worker end to end: the enable flag,
the non-blocking channel send that silently discards when full (Go's
warn-and-drop), the TableByID-then-FindTableByPartitionID lookup whose
order decides the dump's is_partition flag (documented as decisive),
`SchemaByTable`, the stats-enable gate, `RecordHistoricalStatsToStorage`
and both metric counters, with infoschema and metrics injected through
traits.

`topn_slow_query.rs` mirrors the manual heap (less/push/pop/up/down/
init reproducing Go's container/heap), `RemoveExpired`, `Query`, and
the ring queue's Enqueue — the same arithmetic over the same ordering.
Next slice: `serverinfo_syncer`.

## serverinfo_syncer slice (2026-09-05, sixth pass) — VERIFIED

`serverinfo_syncer.rs` (73 functions) mirrors `pkg/domain/serverinfo`:
the `ServerInfo`/`DynamicInfo`/`StaticInfo` trio with Go's clone
semantics, marshal/unmarshal, topology conversion, and the assumed-
pool flag; the syncer with `NewSessionAndStoreServerInfo`,
`StoreServerInfo` (refusing without a session, per the module doc),
`RemoveServerInfo`, `Restart`, `GetServerInfoByID` and
`GetAllServerInfo`; plus `status_endpoint_claim` — the endpoint/claim-
key construction and try-acquire-and-report against the etcd lease.
Next: none in pkg/domain — the domain file set is fully dispositioned
pending `ru_stats`/`plan_replayer` deep reads already recorded.
