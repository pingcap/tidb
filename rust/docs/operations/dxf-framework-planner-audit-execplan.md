# `pkg/dxf/framework/planner` parity audit ExecPlan

This living plan follows `PLANS.md` at the repository root.

## Objective

Inventory every Go-master DXF planner production, test, and build artifact,
trace logical/physical plan and task-creation contracts, and compare them with
Rust owners without conflating the separate SQL optimizer.

## Progress

- [x] (2026-09-02) Fetch and pin Go `origin/master` at
  `5e8a1a229a7591ddac49a0cd3b795587c2595ab9`.
- [x] (2026-09-02) Read all five tracked artifacts and all 320 lines: the
  public library/two-shard flaky test target, two production files, and two
  test files. Verify no package doc, fixture, testdata, generated/platform
  variant, benchmark, fuzz target, or OWNERS file exists.
- [x] (2026-09-02) Trace all four production and two test declarations,
  `PlanCtx`, logical/physical/pipeline contracts, processor filtering and
  metadata conversion, session-aware task creation, and mock-store assertions.
  Confirm Rust's SQL `tidb-planner` is not a dependency-closed DXF planner
  owner.
- [x] (2026-09-02) Run the exact `-tags=intest` Go-master suite and Ready
  documentation gates; record the pass and explicit Go-only DXF boundary in
  the receipt.
- [ ] Publish this receipt batch to `origin/hparser-integration`, verify the
  remote SHA, pull the branch's latest state, and continue the rolling package
  audit.

## Scope and decision

This package plans DXF task metadata and processor DAGs; it is not the SQL
optimizer. Rust's existing planner crate cannot replace its session/storage
task-creation seam. Keep the complete Go package as an explicit boundary until
a native Rust DXF scheduler/storage integration exists.

## Validation gate

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test -tags=intest ./pkg/dxf/framework/planner -count=1
cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint
git diff --check
```

No Go/Bazel/module or Rust source changed, so neither `make bazel_prepare` nor
a Rust test target is required for this receipt-only boundary batch.

## Outcome

The complete planner inventory, tag-aware validation evidence, and Rust
ownership decision are recorded in
`rust/testport/receipts/dxf_framework_planner.md`. The rolling audit continues
with the next unrecorded Go package; repository-wide parity is not claimed.
