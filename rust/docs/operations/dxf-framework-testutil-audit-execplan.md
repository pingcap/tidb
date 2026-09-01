# `pkg/dxf/framework/testutil` parity audit ExecPlan

This living plan follows `PLANS.md` at the repository root.

## Objective

Inventory every Go-master DXF test-support and build artifact, trace all helper
contracts and failpoint/session-store interactions, and compare them with Rust
owners without inventing a disconnected utility API.

## Progress

- [x] (2026-09-02) Fetch and pin Go `origin/master` at
  `5e8a1a229a7591ddac49a0cd3b795587c2595ab9`.
- [x] (2026-09-02) Read all seven tracked artifacts and all 1,258 lines: the
  Bazel target plus context, distributed-test, executor, scheduler, table, and
  task helpers. Verify there is no package doc, test file, fixture, testdata,
  generated/platform variant, benchmark, fuzz target, or OWNERS file.
- [x] (2026-09-02) Trace all 68 function/method declarations, including
  multi-node lifecycle/owner election, GoMock expectation setup, SQL-backed
  task/subtask helpers, keyspace selection, and failpoint cleanup. Search Rust
  `tidb-dxf` owners and confirm no dependency-closed equivalent exists.
- [x] (2026-09-02) Run the exact Go-master compile probe and Ready
  documentation gates; capture the no-test result and explicit Go-only support
  boundary in the receipt.
- [ ] Publish this receipt batch to `origin/hparser-integration`, verify the
  remote SHA, pull the branch's latest state, and continue the rolling package
  audit.

## Scope and decision

This package is test infrastructure rather than production behavior. Its
semantics depend on Go SQL sessions, mock stores, GoMock, failpoints, scheduler
globals, and DXF task-table schemas. Rust's generic framework types cannot
replace that dependency-closed harness, so keep the complete package as an
explicit Go-only boundary until a native Rust integration-test surface exists.

## Validation gate

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/dxf/framework/testutil -count=1 -run '^$'
cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint
git diff --check
```

No Go/Bazel/module or Rust source changed, so neither `make bazel_prepare` nor
a Rust test target is required for this receipt-only boundary batch.

## Outcome

The complete test-support inventory, ownership decision, and validation
evidence are recorded in
`rust/testport/receipts/dxf_framework_testutil.md`. The rolling audit
continues with the next unrecorded Go package; repository-wide parity is not
claimed.
