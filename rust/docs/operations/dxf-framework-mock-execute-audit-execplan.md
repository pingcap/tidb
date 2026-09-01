# `pkg/dxf/framework/mock/execute` parity audit ExecPlan

This living plan follows `PLANS.md` at the repository root.

## Objective

Inventory every Go-master artifact in the generated DXF StepExecutor mock
package and compare its test-support contract with Rust owners without
fabricating a disconnected mock API.

## Progress

- [x] (2026-09-02) Fetch and pin Go `origin/master` at
  `5e8a1a229a7591ddac49a0cd3b795587c2595ab9`.
- [x] (2026-09-02) Read both tracked artifacts and all 253 lines: the Bazel
  target and complete MockGen output. Verify there is no package doc, test,
  fixture, benchmark, fuzz target, generator input, platform variant, or
  additional generated artifact.
- [x] (2026-09-02) Trace every generated method and its
  `taskexecutor/execute.StepExecutor` dependency, then search Rust's
  `tidb-dxf` and test-mock owners for a dependency-closed equivalent.
- [x] (2026-09-02) Run the package compile probe and Ready documentation gates;
  capture the no-test result and generated-source boundary in the receipt.
- [ ] Publish this receipt batch to `origin/hparser-integration`, verify the
  remote SHA, pull the branch's latest state, and continue the rolling package
  audit.

## Scope and decision

This package is one generated test-support unit. Its only behavior is the
MockGen implementation of `StepExecutor`; execution semantics and focused tests
live in parent DXF packages. Rust's generic task/resource/step types cannot
substitute for the Go lifecycle and GoMock recorder contract. Keep this
boundary explicit until a dependency-closed Rust DXF executor owner exists.

## Validation gate

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/dxf/framework/mock/execute -count=1 -run '^$'
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
`rust/testport/receipts/dxf_framework_mock_execute.md`. The rolling audit
continues with the next unrecorded Go package; repository-wide parity is not
claimed.
