# The two read tiers: what each serves, and which one new features belong in

Measured at `c58dd3fb76`. This is a measurement record, not a refactor plan.

## Verdict: (b) DEMOTE

`ReadOnlyScanPlan` and its family are a **bounded, real-TiKV wire-proof path**.
They serve nothing the cluster tier is structurally incapable of serving. New
read features MUST target the `cluster_session` tier.

## What the two tiers actually are

They are not two lowerings of the same plan shape. They are two *stacks*:

| | `ReadOnlyScanPlan` family | `cluster_session` tier |
| --- | --- | --- |
| Lowering | hand-written AST -> fixed DAG in `tidb-planner/src/read_only_scan.rs` | `tidb-session::Session` -> `tidb-executor` driver + `tidb-planner` proper |
| Catalog | `tidb-exec/src/cluster_catalog.rs`, one or two admitted tables | whole loaded cluster catalog, `catalog_watch` |
| Scan transport | `tidb-exec/src/real_tikv_read.rs` direct coprocessor DAG | `tidb-executor::remote_scan::PushdownScanner` via `cluster_storage` |
| Writes / DDL / txn | none in the scan path | full (`cluster_session_node/{ddl,transactions,statistics}.rs`) |
| Lines (src) | 8,746 | 3,171 |

`ReadOnlyScanPlan` appears **nowhere** in `cluster_session.rs` or
`cluster_session_node/`. There is no shared lowering to deduplicate; the
overlap is conceptual (both eventually issue a coprocessor DAG request), not
textual. So "8,746 lines of duplication" is **not** what was measured — what
was measured is 8,746 lines of a *second, weaker* read stack that keeps
attracting features.

## Capability inventory, both directions

`ReadOnlyScanPlan` refuses, at `read_only_scan.rs`:

- ORDER BY (1151), DISTINCT (1154), aggregates (1157, 1498, 1521)
- `SELECT *` wildcard (1513), any non-column projection expression (1531)
- joins (1464/1469), derived tables and subqueries (1468, 1529)
- GROUP BY (1480), window functions (1483), LIMIT (1486)
- CTEs (1448), set operations (1089), INTO OUTFILE (1490)
- partitions (1740), stale read (1743), index hints (1746), TABLESAMPLE (1749)
- `SELECT ... FOR UPDATE` (1275, 1571)
- predicates: only column-vs-integer comparisons (1667); everything else
  refused as `UnsupportedReadOnlyPredicate`

Sibling files in the same family add back a slice of these for the configured
node only — `configured_order_limit.rs` (ORDER BY/LIMIT),
`configured_join_plan.rs` (two-relation join) — as *separate* plan types, not
as `ReadOnlyScanPlan` capabilities.

The cluster tier serves all of the above through the real planner/executor;
that is what every `run-realtikv-*.sh` and the sysbench ladder exercise.

**The other direction — what only `ReadOnlyScanPlan` has today:**

1. The autocommit `MaxUint64` point-get snapshot shortcut
   (`real_tikv_read.rs:96` `MAX_TS_POINT_GET_SNAPSHOT`, applied at 1268 in
   `execute_lowered_plan_with_cancellation`). The cluster tier unconditionally
   reads at the session snapshot's `start_ts`
   (`tidb-executor/src/cluster_storage.rs:429-446`). This is a **portable
   feature, not a tier capability** — porting it means teaching
   `cluster_storage`'s snapshot seam the same `IsAutoCommitTxn` +
   point-get-on-handle guard conjunction that `real_tikv_read.rs:1252-1266`
   already documents from Go.
2. Nothing else was found that the cluster tier structurally cannot do.

## Which node modes are live

`tidb-server/src/lib.rs:186 run_configured_node` routes:

- `--cluster-session` -> `cluster_session_node`. **This is the deployed tier.**
  Used by `run-sysbench-ladder.sh:242`, and by `run-realtikv-{access-path,
  analyze, convergence, repeatable-read, scan-pushdown, session-driver}.sh`.
- `--load-table` -> `real_tikv_node` (1 table) or `real_tikv_multi_node` (2).
  Used by `run-realtikv-{ddl,ddl-notify,catalog-load,multi-statement-txn}.sh`.
- `--read-table` (1 or 2, no PD catalog read) -> same two nodes.
  Used by the eight `run-live-*-sql-node.sh` proof scripts.
- `--load-privileges` without `--load-table` -> hard error.

So the `ReadOnlyScanPlan` family **is** live — but only under scripted
real-TiKV proofs, never under the benchmark.

## Confirmations of the motivating claims

- **#140/#142/#146/#151 (max-ts point get in the wrong tier)** — CONFIRMED with
  one correction. `MAX_TS_POINT_GET_SNAPSHOT` is referenced only by
  `real_tikv_read.rs` and `tidb-exec/tests/autocommit_point_get_max_ts_source.rs`.
  It *is* reached by a served node (`real_tikv_node/mod.rs:462,505`) — the
  brief's "the served node never sends it" is too strong. The accurate claim:
  **no `--cluster-session` node can reach it**, so no sysbench run measures it.
- **#101 (`signed_bigint_ranger` behind ORDER BY/DISTINCT/aggregate refusals)** —
  CONFIRMED verbatim at `read_only_scan.rs:1149-1158`.
- **#142 (sysbench's `id INTEGER` refused)** — CONFIRMED, and located: the
  refusal is in the family's catalog loader,
  `tidb-exec/src/cluster_catalog.rs:314` ("column `{name}` is the row handle
  but has type {}, not signed BIGINT"), with siblings at 224 and 230.
- `Unknown database` (1049) is `sql_node.rs:618`, shared by both tiers'
  connection path — the `--load-table` failure is that the node admits only
  its named tables, not that it lacks the schema concept.

## Cost of the demotion

`ReadOnlyScanPlan` / `RealTiKvReadSession` / the configured plan types are
referenced by **49 source files, 22 of them under `tests/`**, plus eight
`run-live-*-sql-node.sh` scripts and four `run-realtikv-*.sh` scripts. Deleting
the tier is therefore **not** cheap and is not proposed. Demoting it is free.

## The mechanism that stops the next feature landing here

Documentation alone did not stop four tasks. The boundary must be *pinned*:

1. Module doc on `tidb-planner/src/read_only_scan.rs` and
   `tidb-exec/src/real_tikv_read.rs` stating: bounded proof path, no new
   capability, new read features go to `cluster_session_node`.
2. A source pin test that fails when the family grows: assert the
   `UnsupportedReadOnlyFeature` variant set is unchanged, and that
   `scripts/source_size_bounds.txt` caps the family's files at their current
   sizes rather than a generous ceiling. A new feature in this tier either adds
   a variant, removes one, or adds lines — all three trip the pin, and the
   failure message names the tier boundary.
3. Any feature that must exist in both tiers (the max-ts point get is the live
   example) is written at the cluster seam **first**; the proof path may then
   consume it, never the reverse.
