# `pkg/dxf/framework/dxfmetric` parity audit ExecPlan

This living plan follows `PLANS.md` at the repository root.

## Objective

Inventory every Go-master artifact in the DXF metric package and compare its
Prometheus collector, snapshot, label, duration, and registration contract with
Rust owners without fabricating a disconnected metrics facade.

## Progress

- [x] (2026-09-02) Fetch and pin Go `origin/master` at
  `5e8a1a229a7591ddac49a0cd3b795587c2595ab9`.
- [x] (2026-09-02) Read all three tracked artifacts and all 296 lines: the
  Bazel target and complete collector/metric production sources. Verify no
  package documentation, tests, fixtures, generated input/output,
  platform-specific variant, benchmark, fuzz target, or `OWNERS` file exists.
- [x] (2026-09-02) Trace all collector and metric functions: atomic task and
  subtask snapshots, UUID test labels, status aggregation, duration gauges,
  event vectors, initialization, and registration. Search Rust `tidb-dxf`,
  timer, and registry owners and confirm no dependency-closed equivalent exists.
- [x] (2026-09-02) Run the package compile probe and Ready documentation gates;
  capture the no-test result and metric integration risks in the receipt.
- [ ] Publish this receipt batch to `origin/hparser-integration`, verify the
  remote SHA, pull the branch's latest state, and continue the rolling package
  audit.

## Scope and decision

This package is one atomic observability-support unit. Its descriptors, labels,
atomic publication, duration semantics, metric vectors, and registration
lifecycle are consumed by the DXF framework as a whole. Rust task/resource
types and unrelated timer counters are not substitutes; keep the package
explicitly Go-only until a dependency-closed Rust DXF metric registry exists.

## Validation gate

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/dxf/framework/dxfmetric -count=1 -run '^$'
cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint
git diff --check
```

No Go/Bazel/module or Rust source changed, so neither `make bazel_prepare` nor
a Rust test target is required for this receipt-only boundary batch.

## Outcome

The complete inventory, validation evidence, Rust ownership search, and
observability risks are recorded in
`rust/testport/receipts/dxf_framework_dxfmetric.md`. The rolling audit
continues with the next unrecorded Go package; repository-wide parity is not
claimed.
