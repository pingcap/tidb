# `pkg/dxf/importinto` parity audit ExecPlan

This living plan follows `PLANS.md` at the repository root.

## Objective

Inventory every direct Go-master production, test, fixture, generated,
platform, and build artifact in the parent ImportInto package; trace its
planner/scheduler/executor and job/task transaction contracts; and compare
them with Rust owners without fabricating a partial runtime port.

## Progress

- [x] (2026-09-01) Fetch and pin Go `origin/master` at
  `5e8a1a229a7591ddac49a0cd3b795587c2595ab9`.
- [x] (2026-09-01) Read all 26 direct tracked artifacts and all 9,158 lines:
  13 production/build files and 13 test files. Verify no direct fixture,
  testdata, platform variant, fuzz target, benchmark, generated source,
  generator input, or `OWNERS` file exists; identify `job_doc.go` as checked-in
  design documentation. Inventory the five nested package units separately.
- [x] (2026-09-01) Trace all 170 production function/method declarations and
  45 top-level test functions/suite methods: submission and async prepare,
  planner metadata and range splitting, scheduler state/cancellation and
  keyspace transactions, local/global encode/merge/ingest, conflict collection
  and resolution, checksum verification, cleanup/metering, metrics, and all
  mock-store/failpoint lifecycle cases. Search Rust `tidb-dxf`, parser,
  session, storage, and test-mock owners; confirm only generic vocabulary and
  SQL statement support exist, not a dependency-closed ImportInto runtime.
- [x] (2026-09-01) Run the exact failpoint-enabled package suite; it passed in
  11.138s and the wrapper disabled failpoints afterward. Run the Ready
  documentation gates and capture results in the receipt.
- [ ] Publish this receipt batch to `origin/hparser-integration`, verify the
  remote SHA, pull the branch's latest state, and continue the rolling package
  audit.

## Scope and decision

The parent package is one atomic Go ImportInto planner/scheduler/executor
unit. Its APIs are coupled through `TaskMeta`, `PlanCtx`, DXF task/subtask
states, Lightning writers and encoders, object-store metadata, TiKV codecs,
table-mode SQL, and checksum/metering consumers. The existing Rust step labels
are vocabulary evidence only; they do not constitute an implementation owner.
Keep this package explicitly Go-only until Rust has a dependency-closed
ImportInto runtime and executable test seam. The nested packages retain their
own atomic receipts and are not silently folded into this claim.

## Validation gate

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
./tools/check/failpoint-go-test.sh ./pkg/dxf/importinto -count=1
cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint
git diff --check
```

No Go/Bazel/module or Rust source changed, so neither `make bazel_prepare` nor
a Rust test target is required for this receipt-only boundary batch.

## Outcome

The complete parent-package inventory, test evidence, Rust ownership search,
and residual risks are recorded in
`rust/testport/receipts/dxf_importinto.md`. The rolling audit continues with
the next unrecorded Go package; repository-wide parity is not claimed.
