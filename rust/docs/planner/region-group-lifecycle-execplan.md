# Share Go's region grouping lifecycle

This living ExecPlan follows `PLANS.md` at the repository root. The owning
upstream package is `pkg/store/mockstore/unistore/cophandler`; its complete
source/build/test inventory is retained in the preceding aggregation evidence.
This is integration repair and seed evidence, not a completed package claim.

## Purpose / Big Picture


Region aggregate requests must use Go's grouping lifecycle, including typed
computed keys, first-seen group order and hash/StreamAgg behavior. A sequence
of group keys 5,3,5 must reuse the first group's state; the deprecated protobuf
Aggregation.streamed flag must not select a separate Rust implementation.
The user explicitly instructed “follow go” after the aggregate descriptor fix.

## Progress


- [x] Pull master and hparser-integration; both remain at the previous references.
- [x] Confirm prior change 17ad39d246 passed both locked server builds and was pushed.
- [x] Trace Go builders and verify the LIVE request dispatch, not only legacy closures.
- [x] Capture 11,232 distinct full Go requests; unchanged Rust fails 5,824 cases.
- [x] Replace split grouping pipelines with typed keys and insertion-ordered states.
- [x] Preserve live aggExec empty-input behavior; both scan callers use the request context.
- [x] Run grouping and aggregate regressions, 190 Unistore tests, 16 producer integration tests, and make lint.
- [ ] Pass the locked Rust server build in the commit hook and again immediately before push.
- [ ] Publish on hparser-integration and report remaining package gaps.

## Surprises & Discoveries


The initial source trace followed legacy buildStreamAggProcessor, which delegates
to buildHashAggProcessor except eligible count requests. Full request capture
then showed handleCopDAGRequest actually dispatches through buildAndRunMPPExecutor.
Both aggregation executor types now use buildMPPAgg and aggExec, including empty
input (no groups, no row). The legacy closure count/NULL empty rules MUST NOT be
ported into the live Rust path. The old Rust implementation tested Aggregation.streamed
and maintained contiguous group state. Go encodes group expressions through
codec.EncodeValue and appends group keys to output in first-seen order. Rust previously
used collation keys plus an ad hoc 0xff fence, and sorted hash groups by that key.
The previous change deliberately postponed this independent contract.

## Decision Log


- Decision: Capture full Go coprocessor results before editing group management.
  Rationale: A standalone aggregate fixture cannot prove group identity, wire
  order, empty-input behavior, builder selection or selection interactions.
  Date/Author: 2026-09-29, Codex.
- Decision: Follow the mock coprocessor's codec-value group keys, preserving
  typed original group values for output; do not reuse a SQL final-aggregation
  collation key implementation at this intermediate execution stage.
  Rationale: Go's intermediate row representation and representative values
  are observable contracts; use the exact live Go group identity rule.
  Date/Author: 2026-09-29, Codex.

- Decision: Pin the acceptance oracle to HandleCopRequest, not buildClosureExecutor.
  Rationale: Live master uses the MPP tree for ordinary DAG requests as well.
  This revealed an execution-path mismatch before any production changes.
  Date/Author: 2026-09-29, Codex.

## Context and Orientation


`rust/crates/tidb-unistore/src/cophandler.rs` decodes the executor list and invokes
RegionAggregator from both table and index scans. The previous change moved
aggregate functions into `tidb-expr::aggregation::DistAggregate`. Group management
previously held SimpleExpr group keys, a BTreeMap, and a separate streamed accumulator.
`pkg/store/mockstore/unistore/cophandler/mpp.go` and `mpp_exec.go` are the live
reference for buildMPPAgg and aggExec. closure_exec.go remains a legacy/test path.
Go master is 12b639a116; the initial execution archive 8936d7bdcb has identical
owning-package source. The final reference snapshot was refreshed to master
including its client-go pin; all 7,726 tracked blobs match master. The final Go
run passes with byte-identical responses for all 11,232 requests.

## Plan of Work


Use a Go overlay test in the owning package and its existing testStore helpers
to execute real encoded requests. Save exact input KVs, request bytes, result
bytes, errors and warnings. Replay them through Rust's handle_cop_request.
Cover interleaved/compound/computed keys, collation, NULLs, empty scans, selection,
executor kind and deprecated streamed metadata. Prove failures before editing.

Replace contiguous/hash alternatives with a map from encoded key to a vector
index and a vector of group states in first-seen order. Decode group expressions
using PBToExpr against scan FieldTypes and evaluate them on the reusable typed
row. Encode group values with the shared codec and request timezone. Use one
lifecycle for both actual executor kinds. The live Go path emits no
row for either empty input or fully filtered input; do not add legacy defaults.

## Milestones


The first milestone is a Go-produced wire fixture that fails against old Rust.
The second is a single grouping lifecycle with the same fixture passing and the
existing 3,129 aggregate-function cases unchanged. The last milestone is focused
integration verification, required lint, and successful locked server builds in
the commit hook and immediately before push.

## Concrete Steps


From the reference archive, use the repository failpoint wrapper with an overlay:

    GOTOOLCHAIN=go1.25.14 GOFLAGS='-overlay=/private/tmp/region-group-overlay.json' ./tools/check/failpoint-go-test.sh pkg/store/mockstore/unistore/cophandler -run '^TestRustRegionGroupLifecycle$' -count=1 -v

From the repository root:

    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-unistore --lib region_group -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-unistore --lib region_aggregate -- --test-threads=1
    make lint
    cd rust && cargo build --locked -p tidb-server

The existing Unistore selection test failure on `abc` was reproduced on the
previous untouched branch. Do not classify it as new or silently call it passing.
No Go/Bazel/dependency inputs are edited, so bazel_prepare is not triggered.

## Validation and Acceptance


Exact Go request responses must match for the covered valid shapes. The 5,3,5
case emits one state per key in first-seen order in both general aggregate paths.
Computed group keys use the same typed evaluator as aggregate arguments and
preserve diagnostics. Changing only the deprecated streamed field has no effect.
Empty input follows live aggExec: no groups means no rows for either executor type.
Full package parity, real TiKV, and benchmark speedups require separate evidence.

## Idempotence and Recovery


Oracle fixtures are pinned and replayable offline. Temporary Go test overlays do
not alter tracked source. Close and remove oracle-owned temporary databases.
Preserve unrelated work, fast-forward or rebase on remote updates, never force push.

## Artifacts and Notes


The oracle, complete package inventory, all baseline differences and exact
validation commands are in `region-group-lifecycle-20260929/`. Previous complete source inventory
is `region-aggregation-context-20260929/source-inventory.json`.

## Interfaces and Dependencies


RegionAggregator retains typed Expressions, DistAggregate descriptors and a
reusable MutRow. Group states are indexed by codec-encoded values. Both scan
paths call the same lifecycle. Shared tidb-codec owns encoding; no ad hoc
collation or numeric key formatter should be introduced in cophandler.

## Outcomes & Retrospective


The grouping repair is implemented: all 11,232 live-request cases pass, versus
5,824 failures on unchanged baseline 17ad39d246. The 3,129 existing aggregate
function cases also pass. Complete package parity remains open: live Go uses
an executor tree and GetResult materialization, whereas Rust still has a
restricted flat composition and partial-result finalization. The receipt records
these larger structural gaps, excluded baseline test, and benchmark limitations.
Publication follows the required hook and pre-push locked server builds; the
commit ID and push result are reported in the task response.
