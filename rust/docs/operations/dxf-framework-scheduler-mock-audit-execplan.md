# `pkg/dxf/framework/scheduler/mock` parity audit ExecPlan

This living plan follows `PLANS.md` at the repository root.

## Objective

Inventory the complete generated scheduler-extension mock package and compare
its test-support contract with Rust owners without fabricating a disconnected
mock API.

## Progress

- [x] (2026-09-02) Fetch and pin Go `origin/master` at
  `5e8a1a229a7591ddac49a0cd3b795587c2595ab9`.
- [x] (2026-09-02) Read both tracked artifacts and all 173 lines: the Bazel
  target and complete MockGen output. Verify no package doc, tests, fixtures,
  testdata, platform variants, benchmark/fuzz targets, generator inputs, or
  OWNERS file exists.
- [x] (2026-09-02) Trace all 19 generated functions and the parent
  `scheduler.Extension` method signatures; search Rust `tidb-dxf` owners and
  confirm no dependency-closed equivalent exists.
- [x] (2026-09-02) Run the package compile probe and Ready documentation gates;
  capture the no-test result and generated-support boundary in the receipt.
- [ ] Publish this receipt batch to `origin/hparser-integration`, verify the
  remote SHA, pull the branch's latest state, and continue the rolling package
  audit.

## Scope and decision

This package is generated test support for the Go scheduler extension. Runtime
semantics and regression coverage live in the parent scheduler/testutil
packages. Rust's generic DXF values cannot replace this GoMock contract; keep
the boundary explicit until a dependency-closed Rust scheduler test seam
exists.

## Validation gate

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/dxf/framework/scheduler/mock -count=1 -run '^$'
cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint
git diff --check
```

No Go/Bazel/module or Rust source changed, so neither `make bazel_prepare` nor
a Rust test target is required for this receipt-only boundary batch.

## Outcome

The complete generated package inventory, validation evidence, and native
ownership decision are recorded in
`rust/testport/receipts/dxf_framework_scheduler_mock.md`. The rolling audit
continues with the next unrecorded Go package; repository-wide parity is not
claimed.
