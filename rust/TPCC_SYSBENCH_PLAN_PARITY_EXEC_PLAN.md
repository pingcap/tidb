# Match Go TiDB plans before benchmarking TPCC and Sysbench

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`,
`Decision Log`, and `Outcomes & Retrospective` current as work proceeds.

Reference: `PLANS.md` at the repository root. This plan follows the root
`AGENTS.md`; all Rust edits, builds, tests, and benchmarks run on the EC2 host
`i4i-test-4`, and all deployed database processes run on the four AI-Sandbox
EC2 instances described below.

## Purpose / Big Picture

The Rust SQL node on branch `hparser-integration` must be evaluated as a
drop-in replacement for Go TiDB in a distributed nightly cluster. Before a
throughput number is accepted, every SQL template emitted by the pinned TPCC
and Sysbench clients must have the same physical execution plan as Go TiDB.
The observable result is a checked plan-parity report with no missing SQL
templates and no physical-plan mismatches, followed by correctness checks and
16-client benchmark results against the frozen nightly baseline.

This is a release-blocking compatibility gate, not a best-effort performance
comparison. Go TiDB is the sole plan oracle. Plans are compared with the same
schema, data, statistics, bindings, session variables, parameter values, and
transaction context. A semantically equivalent or faster Rust plan still
fails the gate when its protected physical-plan fields differ from Go.

Plan parity means the same physical operator type and tree, root versus
`cop[tikv]` task boundary, access object, index or table ranges, ordering
property, join/aggregation algorithm, and pushed Selection/Limit placement.
Only generated plan-node numeric suffixes and internal `Column#N` ordinals may
be normalized. A faster query reached through a different plan is a failure.

The benchmark topology is one nightly PD, one nightly TiProxy, three nightly
TiKV stores, three Rust TiDB nodes, three replicas, no TiFlash, and exactly 16
total benchmark threads. Frozen baseline results and benchmark procedure come
from CSE branch `codex/table-group-m1`; the baseline is not rerun.

## Progress

- [x] (2026-08-09 01:13Z) Restored BatchMode public-key SSH to all four
  `i4i-test-1` through `i4i-test-4` hosts and verified `/mnt/nvme`.
- [x] (2026-08-09 01:21Z) Rolled the tested Rust TiDB binary SHA-256
  `2b793eb9f0aef7f6e6d895b021837ce5660e368413a57c3c2bce48362e5126fe`
  to all three SQL nodes while keeping nightly PD, TiKV, and TiProxy unchanged.
- [x] (2026-08-09 01:24Z) Pinned official go-tpc v1.0.12, source commit
  `688d62f3be7ea6b68c2bb5fbbeb925bde681fb05`, and verified binary SHA-256
  `864cf82c57ffdfb74a8d75644dcf3b4943ca264e16f684618f340bb0e9ce719d`.
- [x] (2026-08-09 01:40Z) Minimized the first plan/performance mismatch to
  the TPCC `history` aggregate and captured Go/Rust EXPLAIN evidence.
- [x] (2026-08-09) Promoted complete Go/Rust physical-plan parity to a hard
  prerequisite for dataset preparation, correctness acceptance, and every
  formal TPCC/Sysbench benchmark result.
- [x] (2026-08-09) Proved prepared-plan extraction on both servers by sending
  `EXPLAIN FORMAT='brief' <original SQL with ?>` through MySQL binary
  prepare/bind/execute; captured the first Go/Rust mismatch with bound values.
- [x] (2026-08-09 02:50Z) Added the fail-closed differential runner, generated
  candidate manifest, exact MySQL parameter encoders, protected-field diff,
  source commit/digest pins, JSON/Markdown receipts, and focused unit tests.
- [x] (2026-08-09 02:51Z) Ran 87 candidate plan cases with zero collection
  errors in TPCC run and Sysbench run: TPCC run matched 14/63, TPCC checks
  matched 0/12 with two compatibility errors, and Sysbench run matched 2/12.
- [ ] (2026-08-09 03:32Z) Complete TPCC prepare/cleanup and Sysbench
  prepare/cleanup/bulk-batching inventory (completed: exact default TPCC DDL,
  32-table Sysbench DDL/split/cleanup, 32 common prepared-table variants,
  16 worker-local variants, and every 1..851 bulk width; remaining: runtime
  coverage receipts and execution compatibility for non-plan statements).
- [x] (2026-08-09 03:34Z) Expanded the fail-closed manifest from 92 candidate
  statements to 563 static cases plus 57 dynamic families, or 15,248 expanded
  SQL shapes, while retaining `coverage_status=incomplete`.
- [x] (2026-08-09 03:38Z) Created all 32 empty Sysbench diagnostic tables
  through a temporary localhost-only Go DDL owner, verified both Go and Rust
  see 32 tables, stopped the owner, and verified port 4200 is closed.
- [x] (2026-08-09 03:42Z) Established a four-second deterministic point-get
  differential loop: Go emits one `Point_Get`; Rust emits
  `Projection -> Selection -> Point_Get` for the same prepared statement.
- [x] (2026-08-09) Isolated the upstream rebase in
  `/mnt/nvme/src/tidb-hparser-integration-rebased` on branch
  `codex/hparser-integration-parity`, based on upstream commit
  `8b28739bf921a34f3c3dc98035aa9dab46641d02`. The latest 27 upstream commits
  rebased without conflict; the earlier six conflict resolutions already gave
  upstream ownership and APIs precedence. The original dirty detached
  worktree remains untouched.
- [x] (2026-08-09) Restored exact parity for all 63 TPCC transaction plans and
  all 13,984 expanded Sysbench run-plan cases. Receipts are
  `tpcc-run-63-after-index-join.json` and
  `sysbench-static-after-post-filter.json` under the plan-parity evidence
  directory.
- [x] (2026-08-09) Matched TPCC consistency conditions 05 and 07 exactly.
  Condition 07 now proves the result set, common-handle range access on both
  leaves, MergeJoin ordering, and the cross-leaf `other cond`; the complete
  join-focused WIP suite passes 11/11.
- [x] (2026-08-09) Matched TPCC condition 03 exactly with the executed
  `Projection -> root StreamAgg -> TableReader -> cop StreamAgg ->
  TableRangeScan` package. The common-handle prefix range carries
  `keep order:true`; TiKV lowers grouped MAX/MIN/COUNT(1), and the root final
  stage merges the partial columns. Focused aggregate tests pass 9/9 and the
  TiPB lowering regression passes 1/1. Receipt:
  `tpcc-check-03-exact.json`; candidate SHA-256
  `b3f530c87aeb101874c0545561a87e7ff94a3d73c70f9585110d7a1c894155ac`.
- [x] (2026-08-09) Matched TPCC conditions 01, 02, and 04 exactly without
  regressing 03, 05, or 07. Condition 02 now executes Go's covering
  `idx_order` range, partial/final StreamAgg, ordered MergeJoin pair,
  source-column lineage, retained join-key NDV, and POWER real-argument casts.
  The complete check receipt is `tpcc-check-after-condition02-full.json`;
  candidate SHA-256 is
  `e2856b07a5b06b36775050a42c92386bc98e8976c93ee23d0b4113b5834079e1`.
- [ ] (2026-08-09) Match the remaining TPCC consistency plans (completed:
  6/12 exact and 0 protocol errors; remaining: conditions 06 and 08-12).
- [ ] Promote the generated manifest from `incomplete` to `complete` only after
  source and runtime coverage both prove no unknown or unreachable SQL shapes.
- [ ] Implement Go-faithful coprocessor `IndexLookUp` execution and plan text
  for the first mismatch; keep staged-write and transaction semantics exact.
- [ ] Iterate plan families until the complete TPCC/Sysbench manifest passes.
- [ ] Bootstrap clean formal TPCC and Sysbench datasets and run correctness,
  prepare-time, throughput, latency, health, and CPU-profile measurements.
- [ ] Update the branch README, run the Ready validation profile, self-review,
  commit with signoff, push the feature branch, and create a Draft PR targeting
  `pingcap/tidb:hparser-integration`.

## Surprises & Discoveries

- Observation: the retained cluster and all four instances survived the SSH
  interruption. Three PD stores report `Up`, and TiProxy reports all three
  Rust TiDB backends healthy.
  Evidence: PD store addresses `172.31.45.253`, `172.31.41.190`, and
  `172.31.39.241` each have 67 Regions and state `Up`.
- Observation: an ordered common-handle range built below an aggregate did
  not carry its cost estimate into `TableScanExec`. The trace therefore named
  the correct range while the executor rejected partial aggregation as an
  uncosted source. Passing the estimate at the physical-path commit boundary
  made the partial offer truthful; the focused test moved from
  `TableFullScan`, to `TableRangeScan`, to the exact partial/final tree.
  Evidence: `tpcc-check-03-grouped-stream.json` then
  `tpcc-check-03-exact.json`.
- Observation: the prior smoke database was not a valid correctness fixture,
  so a fresh one-warehouse dataset was loaded with the pinned official client.
- Observation: one TPCC check query remained active after client disconnect,
  MySQL `KILL`, and graceful server shutdown. The affected test-only Rust TiDB
  process required a forced restart. Query cancellation is a separate
  compatibility gap and must not be hidden by the plan fix.
- Observation: the first slow query is not caused by missing schema indexes.
  Go and Rust both choose `idx_h_c_w_id`, but Rust executes the double read at
  root and performs one point get per index row. `IGNORE INDEX` makes Rust use
  its coprocessor table scan and reduces the diagnostic query from a repeatable
  5-second timeout to 0.14 seconds; this is diagnostic evidence only and must
  never enter the benchmark SQL.
- Observation: `crates/tidb-executor/src/access_path.rs` already documents and
  tests the known gap as `the_double_read_issues_one_point_get_per_index_row`.
  The test currently asserts 50 gets where Go performs one batched table-reader
  task, so it is the correct red-capable regression seam.
- Observation: neither Go nor Rust accepts SQL text in the form
  `EXPLAIN FORMAT='brief' EXECUTE statement USING ...`; both return a syntax
  error. Both do accept `EXPLAIN FORMAT='brief' <SQL containing ?>` as a
  COM_STMT_PREPARE request and return the plan through COM_STMT_EXECUTE after
  parameters are bound. This exercises the binary prepared path without
  rewriting placeholders into literals.
  Evidence: for the `history` aggregate with `(1, 1, 1)`, prepared EXPLAIN
  returns Go's `StreamAgg -> IndexLookUp -> cop Build/Probe` tree and Rust's
  `HashAgg -> root Selection -> root IndexRangeScan` tree.
- Observation: the first generated candidate inventory contains 92 accounted
  statements: 75 physical-plan cases and five transaction-control statements,
  plus 12 TPCC consistency-check plans. All New-Order item/stock/order-line
  forms from width 5 through 15 are explicit cases. Formal mode refuses this
  manifest because prepare/cleanup, dynamic Sysbench bulk batching, and runtime
  coverage remain incomplete.
- Observation: among TPCC transaction plans, only the 14 INSERT cases currently
  match. The other 49 differ in protected fields. Representative gaps include
  Go `Point_Get` versus Rust `Projection -> Selection -> Point_Get`, Go
  `Batch_Point_Get` versus the same extra root wrappers, Go coprocessor readers
  versus root scans, and Go `IndexJoin` versus Rust root `HashJoin`.
- Observation: TPCC checks produced ten mismatches and two hard errors. Rust
  rejects condition 5 with `this join's plan is not supported yet`; condition
  10 did not return its prepared EXPLAIN within the five-second protocol bound.
  Neither left an active query after the client disconnected.
- Observation: the 12 materialized Sysbench event plan cases matched only the
  prepared insert-after-delete and direct `oltp_insert` shapes. Point/range
  reads, aggregates, UPDATE, DELETE, random point-IN, and random range-OR all
  mismatch Go.
- Observation: the diagnostic Go TiDB on port 4100 intentionally runs with
  `--run-ddl=false`; DDL submitted there remains queued. A temporary Go DDL
  owner bound only to `127.0.0.1:4200` cancelled the diagnostic job, created a
  one-row Sysbench plan fixture, was stopped after both endpoints read it, and
  left port 4200 closed. This process must never be present during a formal run.
- Observation: the original Sysbench candidate inventory covered only
  `sbtest1`, but the pinned harness prepares common OLTP statements for all 32
  tables and 16 workers execute worker-local SQL against tables 1 through 16.
  The access object is protected plan state, so one representative table cannot
  prove workload-wide parity.
  Evidence: the expanded manifest contains 563 static cases, 57 compact
  dynamic families, and 15,248 expanded shapes; unit tests reject a seventeenth
  worker-local table.
- Observation: direct multi-row INSERT plans match at the tested extremes, but
  query plans do not become equivalent merely because schemas are isomorphic.
  Evidence: TPCC order-line widths 1 and 1024, Sysbench prepare width 1000, and
  bulk widths 1 and 851 matched; `sbtest32` point select and `sbtest16`
  random-points still mismatched.
- Observation: the minimal prepared point-select mismatch is deterministic
  across repeated runs and does not involve handle recognition.
  Evidence: Go prints `Point_Get ... table:sbtest32 handle:1`; Rust prints
  `Projection -> Selection(eq(id,1)) -> Point_Get ... handle:1`.
- Observation: a common-handle scan ordered by `(w_id,d_id,o_id,...)` was
  rejected for a MergeJoin asking only for the `(w_id,d_id,o_id)` prefix
  because the leaf compared the vectors for equality instead of testing a
  prefix. That forced condition 07's `order_line` leaf back to a full scan.
  Evidence: after changing the physical-delivery test to `starts_with`, both
  Go and Rust print `TableRangeScan range:[1,1], keep order:true`.
- Observation: condition 07's disjunction references both join children. It
  had been removed from the root residual inventory without being admitted to
  the join because predicate pushdown retained only bare column equalities.
  The executor therefore under-filtered the row set and EXPLAIN omitted Go's
  `other cond`.
  Evidence: the volatility-screened spanning-condition path now executes the
  disjunction at the lowest inner join; `tpcc-check-07-exact.json` reports one
  match, zero mismatches, and zero errors.
- Observation: after conditions 05 and 07, a complete TPCC check pass returns
  2 matches, 10 mismatches, and no timeouts. Every remaining mismatch belongs
  to grouped/derived aggregation property selection or the join/access path
  above such an aggregation; prepared execution itself is no longer the
  source of the former condition 10/12 timeout.

## Decision Log

- Decision: compare plans before preparing formal datasets or measuring
  throughput.
  Rationale: throughput from a different access path does not validate a Rust
  rewrite of Go TiDB, even if rows are correct.
  Date/Author: 2026-08-09 / Codex.
- Decision: inventory SQL from pinned go-tpc and materialized Sysbench sources,
  then validate runtime coverage; do not maintain a hand-written partial list.
  Rationale: TPCC has branch-dependent prepared statements and Sysbench builds
  SQL dynamically.
  Date/Author: 2026-08-09 / Codex.
- Decision: normalize only plan-node IDs and internal column ordinals.
  Rationale: task placement, operator algorithm, access object, ranges, and
  ordering are the behavior under test and cannot be normalized away.
  Date/Author: 2026-08-09 / Codex.
- Decision: capture each case through the same direct or prepared-statement
  path used by the pinned client and compare it under identical planner inputs.
  Transaction-control, session, and DDL statements that have no physical plan
  remain in the inventory and need compatibility receipts, but are explicitly
  classified as non-plan statements rather than silently omitted.
  Rationale: text substitution alone can miss prepared-plan-cache behavior,
  while asking EXPLAIN to model statements that have no plan creates false
  coverage. The inventory must account for both classes without weakening the
  physical-plan gate.
  Date/Author: 2026-08-09 / Codex.
- Decision: represent client parameter types explicitly as MySQL wire types:
  go-tpc integers use signed LONGLONG while Sysbench `INT` uses signed LONG;
  strings, doubles, NULLs, and unsigned forms have bounded encoders.
  Rationale: Python/client-library inference can silently change parameter
  metadata and therefore cannot be authoritative for prepared-plan parity.
  Date/Author: 2026-08-09 / Codex.
- Decision: an `incomplete` manifest is executable only with the explicit WIP
  override; formal mode exits before plan collection and cannot emit a passing
  report.
  Rationale: a green partial inventory is more dangerous than a visible red
  mismatch because it can be mistaken for workload-wide compatibility.
  Date/Author: 2026-08-09 / Codex.
- Decision: implement the TPCC history fix as Go's two-stage coprocessor
  `IndexLookUp`, not as a cost penalty, forced table scan, added index, raw KV
  batch-get substitute, client SQL rewrite, or changed transaction boundary.
  Rationale: the user requires physical plan and execution agreement with Go.
  Date/Author: 2026-08-09 / Codex.
- Decision: preserve a correctness fallback for unsupported or dirty-transaction
  shapes; never approximate an unsupported coprocessor descriptor.
  Rationale: Go semantics and read-your-own-writes take priority over speed.
  Date/Author: 2026-08-09 / Codex.
- Decision: enumerate the Sysbench access-object dimension explicitly: 32
  common prepared-table variants, 16 worker-local variants, 32 prepare-data
  INSERT families, and 16 bulk INSERT families.
  Rationale: table and index names are protected fields, and the pinned harness
  has different prepare-time and execute-time reachability.
  Date/Author: 2026-08-09 / Codex.
- Decision: resolve rebase conflicts in favor of the new upstream
  `hparser-integration` implementation boundary, then reapply only prototype
  behavior that remains necessary and compatible with those APIs.
  Rationale: the user explicitly made updated upstream behavior authoritative,
  and preserving an obsolete local shape would defeat the rebase.
  Date/Author: 2026-08-09 / Codex.
- Decision: verify every physical-order claim against the path actually built.
  A required order may be a prefix of a longer delivered common-handle order,
  but no promise may be inferred from an unchosen index.
  Rationale: this admits Go's legal MergeJoin path without reviving the earlier
  promise-without-delivery row-loss hazard.
  Date/Author: 2026-08-09 / Codex.

## Outcomes & Retrospective

No final outcome yet. The cluster is operational. The differential gate now
expands to 15,248 SQL shapes and remains explicitly incomplete pending runtime
coverage. All 63 TPCC transaction plans and 13,984 expanded Sysbench run plans
match; TPCC checks are at 6/12 with no protocol errors. Formal datasets and
benchmark results remain blocked on complete plan parity.

## Context and Orientation

The active source worktree is
`/mnt/nvme/src/tidb-hparser-integration-rebased` on `i4i-test-4`, branch
`codex/hparser-integration-parity`, based on upstream commit
`8b28739bf921a34f3c3dc98035aa9dab46641d02`. The original detached worktree at
`/mnt/nvme/src/tidb-hparser-integration` and its pre-existing local changes are
preserved untouched. The Rust workspace is `rust/`.

`rust/crates/tidb-executor/src/access_path.rs` owns the current
`IndexRangeSourceExec`. It batches handles for ordering but calls
`KvTable::get_row_by_handle` once per row. `rust/crates/tidb-exec/src/cop_scan.rs`
already lowers a table scan, pushed Selection, and Limit into a TiKV DAG and
streams rows through `tidb-distsql`. `rust/crates/tidb-exec/src/dag_request.rs`
and `rust/crates/tidb-planner/src/physical_index_scan.rs` already support
schema-resolved `PhysicalIndexScan` DAG encoding, but that path is not wired
into `IndexRangeSourceExec`. `rust/crates/tidb-executor/src/plan_trace.rs` owns
the EXPLAIN tree and currently collapses the double read into one root
`IndexRangeScan` node.

The first mismatch is:

    SELECT SUM(h_amount)
    FROM history
    WHERE h_c_w_id = 1 AND h_c_d_id = 1 AND h_c_id = 1;

Go TiDB answers in about 0.03 seconds with:

    StreamAgg root
      IndexLookUp root
        IndexRangeScan Build cop[tikv] on idx_h_c_w_id
        Selection Probe cop[tikv]
          TableRowIDScan cop[tikv]

Rust currently times out after 5 seconds with:

    HashAgg root
      Selection root
        IndexRangeScan root on idx_h_c_w_id

The official benchmark client is in
`/mnt/nvme/hparser-bench/tools/go-tpc-v1.0.12/go-tpc`. Its source is pinned at
`/mnt/nvme/src/go-tpc-v1.0.12`. A read-only archive of the CSE benchmark files
is in `/mnt/nvme/src/cse-table-group-m1-snapshot`.

`rust/scripts/generate-plan-manifest.py` is the source of the generated
`rust/scripts/tpcc-sysbench-plan-manifest.json`. `rust/scripts/plan-parity.py`
owns source-pin validation, exact direct/prepared plan acquisition,
normalization, comparison, and evidence rendering. It reuses the bounded
handshake/metadata parser in `rust/scripts/mysql-prepared-client.py` but owns
the generic typed parameter and string-plan result codec. The focused tests are
in `rust/scripts/test-plan-parity.py`.

## Plan of Work

First, generate a manifest of every prepared or direct SQL template reachable
from the five TPCC transactions, TPCC consistency checks, TPCC preparation,
and the ten retained Sysbench workloads. Record branch-dependent variants such
as customer-by-id versus customer-by-last-name and variable-width `IN`/VALUES
lists as separate plan cases when they can change the physical plan. Run short
pinned client phases and compare observed normalized SQL fingerprints with the
manifest; a missing observed template or an unobserved manifest entry fails.
The manifest also records protocol mode, transaction state, parameter types
and representative/boundary values, schema version, statistics version,
bindings, and all plan-affecting session variables. Statements without a
physical plan are retained with an explicit non-plan classification and an
execution-compatibility result.

Second, build a plan-differential runner under `rust/scripts/`. It initializes
the same schema and representative rows once and obtains
`EXPLAIN FORMAT='brief'` from the direct Go TiDB endpoint on port 4100 and a
direct Rust TiDB endpoint on port 4000. Prepared cases send
`EXPLAIN FORMAT='brief' <original SQL with ?>` itself through COM_STMT_PREPARE,
bind the client's parameter types and values, then collect rows from
COM_STMT_EXECUTE. Direct cases use direct EXPLAIN. The runner normalizes only
generated IDs/ordinals and emits Markdown plus machine-readable JSON. It exits
nonzero on missing cases, SQL errors, or any protected-field difference. Each
output record includes the SQL fingerprint, parameter set, environment
fingerprint, raw plans, normalized plans, and a structural diff.

Third, turn `the_double_read_issues_one_point_get_per_index_row` into the first
failing regression: the expected behavior is one coprocessor index stream and
batched coprocessor table-reader requests, with no per-row snapshot gets. Add a
real-TiKV integration case that checks the emitted DAG receipts and the exact
Go-shaped EXPLAIN tree. Wire the existing physical index scan lowering into a
new remote index-lookup seam. The Build request scans index ranges and returns
handles. Each handle batch becomes point record ranges for the Probe table DAG,
which carries residual Selection and projection. Preserve index order when Go
requires it and merge/fallback correctly when the transaction has staged index
or table writes.

Fourth, run the differential gate and fix the next mismatching plan family in
Go-source order. Each fix receives a focused unit test, an end-to-end query
case, and a new plan-manifest receipt. Do not group unrelated plan families in
one edit.

Finally, recreate a clean three-replica cluster for formal data. Prepare 100
TPCC warehouses and 32 Sysbench tables with 10,000,000 rows each through
nightly TiProxy using 16 preparation threads. Run the full plan gate before
each workload family, then correctness checks, warm-up, measurement, post-check,
three simultaneous Rust CPU profiles, and PD/TiProxy/TiKV health gates. Compare
only with the frozen nightly `3 TiDB + 3 TiKV` baseline from
`codex/table-group-m1`.

## Concrete Steps

Run all commands from EC2. The coding-loop validation profile is WIP:

    ssh i4i-test-4
    cd /mnt/nvme/src/tidb-hparser-integration/rust
    cargo test --offline --locked -j12 -p tidb-executor --lib the_double_read
    cargo test --offline --locked -j12 -p tidb-exec --test all tikv_scan_dag

Use the direct differential diagnostic after each candidate binary deployment:

    timeout 5 mysql -h127.0.0.1 -P4000 -uroot -Nse \
      'SELECT SUM(h_amount) FROM history WHERE h_c_w_id=1 AND h_c_d_id=1 AND h_c_id=1' \
      tpcc_pinned_smoke_20260809

The pre-fix result is exit 124 after exactly 5 seconds. The fixed result must
equal Go's `10.00`, finish below one second on an idle node, and print the same
normalized physical plan as Go.

Regenerate and check the workload manifest from the repository root:

    python3 rust/scripts/generate-plan-manifest.py
    python3 rust/scripts/generate-plan-manifest.py --check
    python3 rust/scripts/test-plan-parity.py

Formal plan collection deliberately fails while coverage is incomplete. WIP
collection requires `--allow-incomplete-manifest`; current receipts are under
`/mnt/nvme/hparser-bench/evidence/plan-parity/`.

At completion use the Ready profile from the repository skill. Run Rust format,
the affected targeted and all-target crate tests, the real-TiKV plan gate, the
full TPCC/Sysbench plan manifest, repository-required lint, and diff checks.
Do not run concurrent Cargo commands.

## Validation and Acceptance

Acceptance requires all of the following:

1. The manifest contains every SQL template/shape emitted by the pinned TPCC
   and retained Sysbench prepare/run/check phases, including every reachable
   branch and plan-relevant variable-width form. Runtime capture finds no
   unknown SQL, and every manifest entry is either observed or has a documented
   source-reachability receipt.
2. Every manifest case has identical normalized Go/Rust physical plans under
   the protected fields defined above. There are zero allowlisted operator,
   task-boundary, access-path, range, algorithm, or pushdown mismatches.
3. Both sides use identical schema/data/statistics versions, bindings,
   plan-affecting global/session variables, transaction context, protocol mode,
   parameter types, and parameter values. Prepared workload SQL has a prepared
   execution-plan receipt; direct EXPLAIN substitution is insufficient.
4. Every transaction-control, session, or DDL statement without a physical
   plan is inventoried and passes execution compatibility; none is silently
   excluded to make coverage appear complete.
5. The index-lookup regression proves batched coprocessor Build/Probe requests,
   correct row/order/NULL results, no per-row point-get loop, correct
   read-your-own-writes behavior or an explicit correctness fallback, and exact
   Go-shaped EXPLAIN output.
6. Standard TPCC pre/post checks pass with the official unmodified client;
   workload logs contain zero execution failures or ignored errors.
7. Every accepted Sysbench scenario reports zero errors/reconnects, and all
   three stores remain `Up` with no missing/down/pending peers.
8. Formal results use exactly 16 total client threads through nightly TiProxy,
   one PD, three nightly TiKV, three Rust TiDB, three replicas, and no TiFlash.
9. README evidence names source commits, binary hashes, commands, plan-parity
   report identity, throughput, latency, prepare time, profiles, and health.

## Idempotence and Recovery

SQL inventory and plan comparison are read-only after their fixture schema is
created and may be rerun. Every evidence run uses a new timestamped directory.
Long data preparation runs as an EC2 systemd transient unit so SSH loss does
not kill it; its unit exit status is authoritative. Candidate binaries are
copied to `.next`, checksum-verified, and rolled one node at a time, with the
previous SHA retained beside them. Never reset or clean the source worktree.

A cancelled query that remains active is recovered by rolling only its Rust
TiDB node after two others are confirmed healthy. Formal datasets are created
under new data roots; debug schemas are never reused for accepted results.

## Artifacts and Notes

Current diagnostic evidence is under
`/mnt/nvme/hparser-bench/evidence/pinned-go-tpc-smoke-t16-20260809T0126Z`.
The first full candidate receipts are
`/mnt/nvme/hparser-bench/evidence/plan-parity/wip-tpcc-run-candidate.json`,
`wip-tpcc-check-candidate.json`, and `wip-sysbench-run-candidate.json`.
Dynamic-shape and access-object boundary receipts are
`wip-expanded-dimensions-boundaries.json`,
`wip-sysbench-table-dimension-boundaries.json`, and
`repro-point-select-table32-run{1,2}.json`.
The latest three-node Rust binary is SHA-256 `2b793e...e5126fe`. The frozen
nightly TPCC baseline is 41,011.5 tpmC at 32 threads; it is historical context,
not a thread-matched ratio for the new required 16-thread candidate run.

## Interfaces and Dependencies

The implementation may extend the optional remote-scan capability between
`tidb-executor` and `tidb-exec`, but the in-process storage fallback remains
object-safe and exact. `tidb-exec` owns TiPB DAG construction and
`tidb-distsql` transport; `tidb-executor` must not reimplement region routing
or RPC. `tidb-planner` remains the authority for validated table/index scan
metadata. Public interfaces need explicit docs and must not expose a partial
or guessed descriptor.

The Go TiDB source in this repository is authoritative. For every plan-family
fix, record the exact Go planner/executor functions used as the source model in
the regression-test comment and commit message.

Plan revision note (2026-08-09): promoted exact workload-wide plan parity to a
fail-closed gate, documented binary prepared EXPLAIN acquisition, and recorded
the first 87-case Go/Rust differential receipts and remaining inventory gaps.
