# Next runtime campaign and planner access-task closure

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`,
`Decision Log`, and `Outcomes & Retrospective` current as work proceeds.

Reference: `PLANS.md` at repository root. This plan follows the repository
requirement for an ExecPlan on a significant refactor.

## Purpose / Big Picture

Build the next parallel Rust-rewrite campaign from real Go-to-Rust consumer
paths. Campaign 05's initially isolated planner primitive was corrected in its
still-unclaimed slot into an index-only access-path-to-task seam that constructs
`PhysicalIndexScanPlan` values from a logical datasource. The next campaign
must retain disjoint checked write sets and production-facing consumers rather
than standalone models.

## Progress

- [x] (2026-07-18) Released the five connected Campaign 05 slices after one
  receipted 12-job gate.
- [x] (2026-07-18) Audited the planner primitive and established that it has no
  Rust equivalent of Go `find_best_task -> convertToIndexScan`.
- [x] (2026-07-18) Consolidated the exact datatype 6-source/18-test and error
  7-source/15-test ownership unions, retiring seven overlapping predecessors.
- [x] (2026-07-18) Corrected the still-unclaimed Campaign 05 planner slot into
  a bounded `LogicalDataSource -> IndexTask` runtime after independent review
  found six semantic mismatches in its original model.
- [x] (2026-07-18) Ran the frozen Campaign 05 planner integration gate and
  released its receipt-backed claim; Campaign 05 is integrated.
- [x] (2026-07-18) Re-audited the proposed expression, DistSQL, and session
  candidates against regenerated ledgers; rejected the speculative DistSQL
  5/9 and session 8/13 counts because no declarative anchor lists support them.
- [x] (2026-07-18) Independently reviewed the datatype proposal, deleted the
  invented cross-source formatter, retained the six real direct translations
  as one collision-free ownership bundle, and released its 6-source/18-test
  claim from `integration_receipt 1`.
- [x] (2026-07-18) Froze the exact expression 5-source/5-anchor successor,
  transferred both predecessors, and connected bounded aliased `COUNT(t.a)`
  through automatic COM_QUERY. Independent review passed after aliasless and
  schema-qualified forms were closed and the false shared-`AggFuncDesc` claim
  was removed. Its receipt-backed claim is released.
- [x] (2026-07-18) Declared and cross-reviewed exact ownership successors for
  DistSQL query/response (2 sources/15 anchors), session warning/status (5/11),
  and coprocessor read-task authorities (5/48), then integrated them as
  Campaign 06. The session stores are composed with Go wrapping; raw response
  and bounded cop-read paths are live, with their transport/session gaps still
  explicitly `PARTIAL`.
- [x] (2026-07-18) Froze integrated campaign membership in
  `workstreams/campaigns/integrated-members.tsv`; batch floors now govern
  planned/active admission without rewriting historical receipts after later
  terminal ownership transfers.
- [x] (2026-07-18) The first expression integration attempt exposed
  `COUNT(t.missing)` falling through to the legacy generic COUNT evaluator.
  Added snapshot-local catalog binding inside every cloned Database attempt;
  exact error identity, failed-SELECT status, and COM_QUERY guards now pass.
- [x] (2026-07-18) Reran the full frozen 12-job gate after independent state
  review, received `integration_receipt 1`, and released the exact expression
  claim. The queue returns to zero active claims.
- [x] (2026-07-18) Integrated Campaign 07 as three cross-reviewed direct-Go
  read-path slices: planner scan-to-DAG lowering, table/index reader runtime,
  and the RPC-ready TiKV unary request/response contract. Receipt 3 was fully
  consumed, exact membership was archived, and the queue returned to zero.

## Surprises & Discoveries

- Observation: `PhysicalIndexScanPlan::init` and `choose_lower_cost` were
  called only by `crates/tidb-planner/tests/cardinality_live_index_choice_source.rs`.
  Evidence: no non-test `LiveIndexCandidate` construction or physical-plan
  task attachment exists in the Rust planner.
- Observation: old window-ranking and LEAD/LAG slices were transfer
  predecessors with deleted evidence paths.
  Evidence: their Go anchors terminate at
  `executor-window-complete-live-runtime`; predecessors are now `retired`.
- Observation: the proposed DistSQL and session composite counts were prose,
  not checked contracts. Every plausible seam is already owned, and excluding
  the named prior owners leaves no derivable 5/9 or 8/13 anchor union.
  Evidence: regenerated source/test inventories and all current slice
  manifests contain no matching declarative lists.
- Observation: the six datatype sources do not define one production operation
  joining FieldType metadata, Datum rendering, truncation, conversion context,
  and OutputFormat.
  Evidence: Go consumes OutputFormat from FieldType metadata, produces and
  handles truncation inside actual Datum conversion paths, and renders Datum
  values independently. The proposed adapter manufactured a compatibility
  matrix and paired unrelated errors and values, so it was deleted before the
  receipt gate.
- Observation: Go warning publication wraps through `uint16` after counts can
  exceed 65,535, while current Rust stores saturate at `u16::MAX`; the
  translated `StaticWarningHandler` is also independent from live
  `StatementStatus`.
  Evidence: independent review traced both call graphs and the 65,536-warning
  boundary. The session successor records both as open gaps and remains
  ownership-only `PARTIAL`.
- Observation: retiring the complete coprocessor read predecessor set requires
  42 exact anchors, not the proposed 40; the region-location parent and
  generated anchor must move together.
  Evidence: its 47-row transfer contains five source and 42 test records while
  broader RegionCache obligations remain untriaged.
- Observation: a shape-only positive capability is insufficient for a
  table-backed aggregate; column binding must use the same catalog snapshot as
  execution on every retry.
  Evidence: the shared gate admitted `COUNT(t.missing)` until `Database` invoked
  the existing catalog/FieldName resolver before the generic evaluator in each
  cloned attempt. The pre-fix regression failed; the post-fix test asserts
  `UnknownColumn("missing")` plus `ROW_COUNT() = -1`, and both focused guards
  pass.
- Observation: planner cost/range ownership and emitted pushdown metadata can
  silently diverge when represented as two independent index authorities.
  Evidence: Campaign 07 cross-review found the selected candidate could be
  costed as index A while the DAG serialized index B. One validated descriptor
  now binds candidate ID, schema ID, uniqueness, and column count before task
  or DAG construction.
- Observation: truncating a returned row vector does not implement Go's
  `RequiredRows` contract.
  Evidence: the reader review traced the caller budget through every iterator
  layer and moved it into `QueryResponse::next_with_required_rows`; tests now
  assert the exact request sequence for both table and index readers.
- Observation: request-source provenance is part of the TiKV wire contract,
  not optional observability metadata.
  Evidence: transport review caught `RequestOrigin::Unknown`; the request now
  encodes TiDB origin and owns the exact `TestRequestSource` obligation.

## Decision Log

- Decision: close Campaign 05 through its existing planner index-choice slot.
  Rationale: the original public helper and test-only PlanNode were not a live
  planner runtime. Its unclaimed active-campaign slot was the correct owner of
  the replacement consumer; a successor would duplicate ownership.
  Date/Author: 2026-07-18 / Codex.
- Decision: keep datasource, access path, property, scan, and task helper
  leaves in the one active Campaign 05 planner slice.
  Rationale: splitting those files would recreate test-only seams and allow
  incorrect row-count or cost ownership.
  Date/Author: 2026-07-18 / Codex.
- Decision: do not create DistSQL or session manifests from target counts.
  Rationale: exact source paths and original test anchors are the isolation
  boundary; a count cannot prove ownership or prevent parallel collisions.
  Date/Author: 2026-07-18 / Codex.
- Decision: use `datatype-value-context-and-format` only as a collision-free
  ownership bundle, not as evidence for a new runtime composition.
  Rationale: consolidation is useful for parallel write-set isolation, but a
  production consumer must be a direct port of a real Go call path. The next
  conversion closure should start at `Datum.ConvertTo` and its string helpers;
  OutputFormat remains on its separate FieldType metadata path.
  Date/Author: 2026-07-18 / Codex.
- Decision: apply campaign size floors at planned/active admission and freeze
  integrated membership separately from live member manifests.
  Rationale: terminal ownership consolidation may shrink a retired member's
  current anchors, but adding a post-gate successor to keep a historical count
  above 50 falsifies what the receipt validated.
  Date/Author: 2026-07-18 / Codex.
- Decision: call Campaign 07's third slice RPC-ready, not RPC-complete.
  Rationale: it serializes the exact dependency-closed TiKV unary envelope and
  decodes response precedence, but it has no socket, gRPC client, PD/RegionCache
  lookup, lock resolver, retry scheduler, or RealTiKV execution. Naming the
  transport live would turn a checked boundary into a false parity claim.
  Date/Author: 2026-07-18 / Codex.

## Context and Orientation

The authoritative coverage state is generated in `rust/STATUS.md`; source and
test ownership live in `rust/difftests/corpus/coverage/go_*_inventory.tsv`.
`rust/scripts/work-unit-queue.py` validates manifests, active claims, transfer
chains, and integration receipts. A slice is a checked unit of Go sources,
exact original test anchors, Rust paths, consumer, and prerequisites.

Campaign 05 is integrated. Its final corrected bounded planner runtime spans
`crates/tidb-planner/src/{logical_data_source,access_path,index_task,logical_data_source_task}.rs`
beside the original cost and physical-scan primitives.

In Go, the missing live ownership path is
`pkg/planner/core/find_best_task.go`:
`findBestTask4LogicalDataSource` chooses paths, `convertToIndexScan` creates a
physical scan, and task conversion compares candidate costs. Do not model
table, TiFlash, index-merge, point-get, partition, or skyline behavior as a
successful index path in the first closure.

## Plan of Work

Freeze only exact ledger-reconstructible composite slices. Each agent must own
only its crate-local consumer, tests, evidence fragment, and declared Rust
paths. Existing transferred owners must not be reintroduced.

1. `datatype-value-context-and-format` now owns six Go sources and 18 exact
   anchors around FieldType, conversion context, output formatting, truncation,
   and datum sentinel behavior. It is an integrated ownership consolidation:
   each authority remains on its real independent Go call path, and it claims
   no FieldType/Datum/conversion/format pipeline. Exclude blocked CoreTime work.
2. `expression-aggregate-and-field-resolution` is the next exact union: five Go
   sources (`aggregation/{aggregation,base_func,descriptor}.go`,
   `types/field_name.go`, and `expression/simple_rewriter.go`) and five anchors
   owned by `expression-aggregate-descriptor-authority` and
   `expression-field-name-resolution`. Transfer both predecessors atomically;
   one successor owns the shared result-field resolver. Do not concurrently
   edit executor aggregate routing.
3. `error-catalog-and-terror-identity` combines seven Go sources and 15
   anchors. It creates one canonical error identity used by protocol, txn, and
   session consumers; catalog and terror are one authority.
4. Do not create `distsql-context-cache-and-location` yet. Context, region,
   paging, and cache seams already terminate at distinct owners; explicitly
   select a new source/test set and retirement boundary before choosing a name.
5. `session-warning-status-publication` now exists as the exact five-source,
   ten-anchor ownership union and transfer graph for warning, status, no-op,
   isolation, and COM_QUERY publication seams. It remains PARTIAL and is not
   claimable: StaticWarningHandler and the live StatementStatus stores are
   independent, no direct Go call-path closure exists, and Rust warning-count
   publication still saturates at `u16::MAX` where Go's `uint16` cast wraps.

Campaign 05's existing planner slice now owns only the dependency-closed
index-only path from these Go
boundaries:

- `pkg/planner/core/operator/logicalop/logical_datasource.go` for possible
  access paths;
- `pkg/planner/util/path.go` for path identity and post-access count;
- `pkg/planner/property/physical_property.go` for task/ordering admission;
- `pkg/planner/core/find_best_task.go` around lines 2156-2327 and 2571-2728;
- `pkg/planner/core/operator/physicalop/physical_index_scan.go` around
  lines 645-702; and
- `pkg/planner/core/operator/physicalop/task_base.go` and `task.go` for the
  bounded Cop/root task shape.

Create `logical_data_source.rs`, `access_path.rs`, `index_task.rs`, and
`logical_data_source_task.rs`, and extend `physical_index_scan.rs` and
`physical_property.rs`. `IndexAccessPath` owns an explicit source
`CountAfterAccess`, point-get admission, and optional exact upstream
`ExpectedCnt` cardinality beside its cost candidate. Empty ranges immediately
return typed zero-row `TableDual`; only a TiKV single-read path explicitly
proven ineligible for point-get can build a Cop task. Root/IndexReader,
table, lookup, and all unrepresented cardinality cases must fail closed. Do
not compare this bounded index cost with incomplete table or lookup costs.

## Concrete Steps

Run from `rust/`:

    CARGO_BUILD_JOBS=12 CARGO_TARGET_DIR=/private/tmp/tidb-wave123 \
      cargo run --offline --locked -j12 -q -p difftest --bin go_source_ledger -- --write
    CARGO_BUILD_JOBS=12 CARGO_TARGET_DIR=/private/tmp/tidb-wave123 \
      cargo run --offline --locked -j12 -q -p difftest --bin go_test_ledger -- --write
    python3 scripts/work-unit-queue.py check

For each ready manifest, claim it with the same slice and owner name:

    python3 scripts/work-unit-queue.py claim-slice --owner <slice> --slice <slice>

Feature agents run formatting and static checks only. After all claims freeze,
the integrator runs exactly one campaign batch:

    CARGO_BUILD_JOBS=12 CARGO_TARGET_DIR=/private/tmp/tidb-wave123 \
      scripts/rewrite-gate.sh integrate

Expected final output includes `integration_receipt <claim-count>`. Only then
change each claimed slice from `ready` to `partial` or `covered` and release it
with `python3 scripts/work-unit-queue.py release --owner <slice> --integrated`.

## Validation and Acceptance

The Campaign 05 planner source test must prove, through a non-test datasource/task API,
that a TopN/CMS/histogram candidate becomes an index scan task with the same
row count and cost; a lower-cost index task replaces the first task, equal cost
does not, and empty/TiFlash/index-merge/point-get paths are rejected rather
than silently treated as index scans. Exact Go formula vectors must cover
row sizes 32 and 48, the session cost factor, and signed index-ID remainder.

The campaign gate must pass format, strict workspace Clippy, workspace tests,
queue/dashboard/ledger checks, parser isolation, plan inventory, dependency
boundary checks, and `git diff --check -- rust`. `STATUS.md` must show no
active claims after release; Campaign 05 becomes integrated only after its
planner slice is genuinely connected and separately receipted.

The datatype consolidation is accepted only if the five predecessor evidence
fragments are retired atomically, the 6-source/18-test claim is exact, and no
new production API composes unrelated Go paths. Its source-shaped smoke target
may prove the independent FieldType/OutputFormat, conversion/truncation, and
Datum/sentinel translations, but cannot upgrade any partial obligation merely
because they share an owner.

## Idempotence and Recovery

Ledger and dashboard writers are safe to rerun. If evidence or any checked
workspace input changes after `gate-begin`, the receipt is invalid by design:
let the trap abort the attempt, fix the issue, and run a new frozen gate. Never
use `--abandon` to describe completed work. Release only slice-named claims.

## Artifacts and Notes

Campaign 05 released six connected slices across 32 Go sources and 130 exact
original obligations. Campaign 07 released three slices across 14 Go sources
and 65 exact original obligations. The generated status page and campaign
manifest are the current queue authority; this plan remains the
next-implementation contract.

## Interfaces and Dependencies

At planner completion, expose a small production-facing API equivalent to:

    struct LogicalDataSource { paths: Vec<IndexAccessPath> }
    struct IndexAccessPath { candidate, count_after_access, point_get_admission, ... }
    enum IndexTask { CopSingleRead(...), TableDual(...), Invalid }
    fn build_index_task(source: &LogicalDataSource, property: IndexTaskProperty)
        -> IndexTask

The API must return an explicit invalid/unsupported outcome for every excluded
Go path. It must accept only source-owned precomputed row counts; the local
statistics adapter is restricted to upstream-proven equality ranges.

## Outcomes & Retrospective

Campaign 05 is complete. The datatype and expression claims are released from
separate receipts while their deliberately incomplete authorities remain
`PARTIAL`; the error authority is consolidated and partial. Expression owns
five sources/five anchors with bounded aliased `COUNT(t.a)` live from
snapshot-local catalog validation to COM_QUERY. It remains `PARTIAL` because
no shared bound `AggFuncDesc` reaches execution and harder FieldName rules are
not live callers. The queue has zero active claims.

Campaign 06 integrated the three exact successors at 2/15, 5/11, and 5/48,
for 12 Go sources and 74 original obligations. Session composes the sole
warning handler through live statement status and wrapping protocol counts;
DistSQL owns raw response subsets through one-way iterator conversion; and
cop-read composes checked tasks, per-attempt cache, shared EMA, paging, and
bounded publication without inventing transport. All remain `PARTIAL`: real
RegionCache/PD, locks/backoff/endpoints/RPC/cancellation, shared unordered
dispatch, cache-backend and unused-topology parity, production table-reader
and concrete memory wiring, Close-error/subset-plus-error shapes, SHOW
WARNINGS/errno identity, and broad SessionVars/SysVar producers remain open.
The official 12-job gate issued `integration_receipt 3`; all claims are
released and exact membership is archived.

Campaign 07 integrated three exact successors across 14 Go production sources
and 65 original obligations. The planner slice lowers exactly one validated
TiKV table/index scan into a tipb DAG; the reader slice owns ordered table and
index request dispatch, one-way response ownership, caller-required row
budgets, dummy temporary-table zero-send behavior, and exact-once cleanup; the
transport slice builds and decodes an exact TiKV unary coprocessor envelope,
including region/lock/other/batch precedence, TiDB request origin, scopes,
timeouts, replica-read metadata, and predicted bytes. Cross-review repaired
split index authority, late RequiredRows truncation, and unknown request
origin before the final gate. All three remain `PARTIAL`: there is no real
PD/RegionCache/TiKV transport, lock/backoff/retry/cancellation path, plan/range
lowering beyond the bounded scan, general DAG tree, signed/unsigned range
split, sorted merge, virtual columns, or production BaseExecutor/chunk wiring.
The 12-job gate issued `integration_receipt 3`; all claims are released and
the exact membership archive is durable. Generated ledgers are
1,907/447/36/0 production and 15,284/976/311/14 test/support obligations
(UNTRIAGED/PARTIAL/COVERED/BLOCKED), with zero active claims. These ownership
states are not a product-parity percentage.

Updated 2026-07-18: the active Campaign 05 planner ownership was retargeted
before claim because its prior receipt covered only the five named released
slices. The duplicate blocked successor was deleted, the final 12-job gate
issued `integration_receipt 1`, and that exact claim was released.

Updated 2026-07-18: independent review blocked the proposed datatype formatter
because it was not a Go call path and had incorrect BIT/signedness admission,
warning ordering, invalid-UTF-8, and float-rendering semantics. The adapter was
deleted instead of patched. After regenerating the evidence-derived source
inventory, the official 12-job gate issued `integration_receipt 1` for claim
SHA-256 `1f884fba2fddbab06a0fd59feaddeef1d2a409d1e46f4535aa82768da6b125ee`;
the exact claim was released with zero active claims.
