# infoschema parity audit: Go `pkg/infoschema` vs the Rust catalog layers

Audit date: 2026-09-05. Opening inventory.

## Architectural mapping

Go concentrates table-metadata resolution in one package (15,398
lines): `infoschema.go`/`infoschema_v2.go` (the schema view),
`builder.go`/`builder_misc.go` (DDL-driven rebuilds), `cache.go` +
`sieve.go` (versioned cache), `cluster.go` (cluster-table plumbing),
`tables.go`/`metric_table_def.go`/`metrics_schema.go` (memory tables),
`bundle_builder.go` (placement bundles), `error.go`, and the
`issyncer`/`validatorapi`/`context` sub-packages.

The Rust port distributes the same responsibilities by function:

| Go responsibility | Rust home |
| --- | --- |
| schema view for the bounded node | `tidb-planner` `configured_catalog.rs` |
| metadata persistence + reload | `tidb-exec` `cluster_catalog.rs`, `catalog_reload.rs`, `catalog_watch.rs` |
| bundle/placement definitions | `tidb-exec` placement modules |
| validity checking | `tidb-domain` `schema_checker.rs` (VERIFIED slice) |
| typed errors | each module's typed error enums |
| memory tables (`tables.go`, metric/metrics schemas) | per-feature modules (bootstrap tables, stats tables); no generic memory-table engine |
| `infoschema_v2`/`sieve` cache machinery | intentionally unported — the bounded node keeps one authoritative image per registry |

## Scope decisions

1. The memory-table engines (`tables.go` 2,938 lines,
   `metric_table_def.go` 3,180 lines, `metrics_schema.go`) are a
   SHOW/INFORMATION_SCHEMA surface: audit per exposed table when that
   surface is wired, not as one batch.
2. `infoschema_v2`/`sieve` are a cache-strategy alternative; the port's
   single-image registry makes them inapplicable (by-design divergence).
3. Behavioral slices proceed in dependency order: (a) DDL reload version
   semantics (`catalog_reload`/`catalog_watch` vs `builder.go`), (b)
   cluster-table plumbing, (c) bundle builder.

## Slice (a): DDL reload version semantics (2026-09-05) — VERIFIED

`catalog_reload.rs` ports Go's `ApplyDiff` reload path faithfully: the
version to reach is the newest stored diff (`GetSchemaVersionWithNonEmptyDiff`
mirror in `schema_version_with_non_empty_diff`), every read — version,
diffs, objects — comes from ONE meta snapshot so the result is a single
schema version, seven frequent actions (create/drop schema, create
table(s) including materialized views, drop table, truncate) apply
targeted patches, and ANY other action falls back to a full reload —
the same observable contract as Go's `applyDefaultAction` fallback,
conservatively widened. The full-reload triggers mirror Go's
`issyncer.LoadSchemaDiffVersionGapThreshold` for version gaps and the
older-version case.

Remaining slices: (b) cluster-table plumbing, (c) bundle builder.

## Slice (b): cluster-table plumbing (2026-09-05) — DISPOSITIONED, no code change

Go's `cluster.go` is the INFORMATION_SCHEMA CLUSTER memory-table
plumbing (`IsClusterTableByName`, cop-destination routing, host-info
row appending): it serves the generic memory-table engine this port
does not expose, so it is N/A by the scope decision recorded at the
opening. The identically-named Rust `cluster_catalog.rs` is a different
layer — the meta-snapshot persistence and load path whose version
semantics slice (a) already verified against `builder.go`. No
divergence to fix in either.

## Slice (c): bundle builder (2026-09-05) — DISPOSITIONED, by architecture

Go's `bundleInfoBuilder` computes which placement-rule bundles need
updating during an incremental diff. The Rust reload path needs no such
delta machinery: placement-policy actions fall to the full-reload
fallback (which re-derives state from the snapshot), and bundle
delivery to PD runs directly through `placement_delivery.rs` +
`tidb_placement::Bundle`. The delta builder is architecturally
subsumed. This closes the infoschema slices a-c: a verified, b and c
dispositioned as by-design.

## Adjacent face: funcdep (2026-09-05) — VERIFIED at API and suite level

`tidb-funcdep` mirrors Go `pkg/planner/funcdep/fd_graph.go`'s FDSet API:
the closure family (strict/lax/equivalence), `InClosure`, `ReduceCols`,
the strict/lax/NC conditional additions, equivalence union, constants,
null-conditioning (`MakeNotNull`/`MakeNullable`), cartesian product,
`AddFrom` and unique-id registration. 18 in-module regressions pass and
the planner's join-elimination rule tests (the FD consumers) are green.
A line-level read of the edge-implication algorithm remains the deeper
follow-up if a behavioral divergence ever surfaces in FD-dependent
rules.

## Adjacent face: funcdep algorithm deep read (2026-09-05) — VERIFIED

The conditional deeper follow-up is done: `implies`,
`add_functional_dependency`, `reduce_cols`, `add_constants` and
`add_equivalence_closure` are line-equivalent to Go's fd_graph.go —
including the replace-vs-drop discipline in the addition loop, the
lax-lax special case in implies, the skip-the-appended-edge iteration
bounds, the equivalence-driven constant propagation, the dependency-side
simplification and the not-null inheritance of a merged equivalence
class. The funcdep receipt's conditional follow-up is closed.

## Slice: metrics schema readers (2026-09-05) — VERIFIED

`metrics_reader.rs` (the SEED of Go `pkg/executor/metrics_reader.go`)
mirrors the three METRICS_SCHEMA retrievers. The PromQL generation is
line-equivalent: quantile substitution uses the shortest float text,
label conditions walk the table's own label order skipping empties,
rendering single values as `label="v"` and multiple as
`label=~"a|b"` over the sorted value set (`metrics_schema.go:117-141`,
`GenLabelConditionValues`), and the range duration appends `s`. The
`BTreeSet` ordering matches Go's explicitly sorted StringSet keys.

## Adjacent face: funcdep_misc helpers (2026-09-05) — LOCATED AND COVERED

Go's `pkg/planner/util/funcdep_misc.go` trio (`ExtractNotNullFromConds`,
`ExtractConstantCols`, `ExtractEquivalenceCols`) lives in the Rust
logical planner as `add_not_null_facts` / `add_constant_facts` /
`add_equivalence_facts` (`logical/functional_dependencies.rs`), feeding
`add_condition_facts`. The not-null arm reproduces Go's
null-rejection-per-column test (`tidb_funcdep::null_reject`), the
constant and equivalence arms use the shared expression extractors, and
16 FD-extraction regressions plus the 914-test planner suite pass. The
funcdep face (fd_graph + misc helpers) is fully covered.

## Adjacent face: process memory utilities (2026-09-05) — VERIFIED

`tidb-util/src/memory/process.rs` mirrors Go `pkg/util/memory/meminfo.go`:
`mem_total` with the 60-second cache and `mem_used` with the 500-millisecond
cache, behind the startup cgroup/host decision hook. The module set also
mirrors the Go package layout (action, arbitrator, pool, tracker,
membuf, systimemon, servermemorylimit, memoryusagealarm), so the memory
utility face is covered at both the read path and the module structure.

## Adjacent face: memory tracker (2026-09-05) — VERIFIED

`tidb-util/src/memory/tracker.rs` (1,602 lines vs Go's 1,355) mirrors
the full Tracker API: `CheckBytesLimit`/`SetBytesLimit`/`GetBytesLimit`,
`CheckExceed`, the action stack (`SetActionOnExceed`,
`FallbackOldAndSetNewAction` and the soft-limit variant,
`GetFallbackForTest`, `UnbindActions`, `UnbindActionFromHardLimit`),
`AttachTo`/`Detach`/`ReplaceChild`, `Consume`, `SetLabel`/`Label`, plus
the fork's mem-arbitrator integration (`init_mem_arbitrator`,
`detach_mem_arbitrator`, kill-signal transport). 20 in-module
regressions pass.
