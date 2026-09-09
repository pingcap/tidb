# `pkg/dxf/framework/dxfutil` parity audit ExecPlan

This living plan follows `PLANS.md` at the repository root.

## Objective

Inventory every Go-master artifact in the DXF runtime utility package and
compare its cross-keyspace runtime ownership, validation, release, and holder
ID contract with Rust owners without inventing a disconnected facade.

## Progress

- [x] (2026-09-01) Fetch and pin Go `origin/master` at
  `5e8a1a229a7591ddac49a0cd3b795587c2595ab9`.
- [x] (2026-09-01) Read all three tracked artifacts and all 314 lines: the
  public Bazel target, complete production utility, and complete test suite.
  Verify no package documentation, fixture/testdata, generated input/output,
  platform variant, benchmark, fuzz target, or `OWNERS` file exists.
- [x] (2026-09-01) Trace `AcquireTaskRuntime`, `CheckTaskRuntime`,
  `GenHolderID`, private release, and the session-provider contract, including
  current/cross-keyspace behavior, release closure, and mismatch errors.
  Search Rust `tidb-dxf`, SQL runtime, and test-mock owners and confirm no
  dependency-closed equivalent exists.
- [x] (2026-09-01) Run the exact package suite and Ready documentation gates;
  capture the passing result and live-server residual risk in the receipt.
- [ ] Publish this receipt batch to `origin/hparser-integration`, verify the
  remote SHA, pull the branch's latest state, and continue the rolling package
  audit.

## Scope and decision

This package is one atomic runtime-support unit. Runtime acquisition,
cross-keyspace handle release, session-pool validation, and holder identity
must remain coupled to SQL-server ownership and task-manager lifecycle. Rust's
generic task/resource model is not a substitute, so keep the package explicit
as Go-only until a dependency-closed Rust SQL runtime exists.

## Validation gate

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/dxf/framework/dxfutil -count=1
cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint
git diff --check
```

No Go/Bazel/module or Rust source changed, so neither `make bazel_prepare` nor
a Rust test target is required for this receipt-only boundary batch.

## Outcome

The complete inventory, validation evidence, Rust ownership search, and
residual live-runtime risk are recorded in
`rust/testport/receipts/dxf_framework_dxfutil.md`. The rolling audit continues
with the next unrecorded Go package; repository-wide parity is not claimed.
