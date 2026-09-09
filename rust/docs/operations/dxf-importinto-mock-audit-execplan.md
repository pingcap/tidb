# `pkg/dxf/importinto/mock` parity audit ExecPlan

This living plan follows `PLANS.md` at the repository root.

## Objective

Inventory every Go-master artifact in the generated ImportInto mock package
and compare its interface and test-support contract with Rust owners without
fabricating a disconnected mock API.

## Progress

- [x] (2026-09-01) Fetch and pin Go `origin/master` at
  `5e8a1a229a7591ddac49a0cd3b795587c2595ab9`.
- [x] (2026-09-01) Read both tracked artifacts and all 74 lines: the Bazel
  target and complete MockGen output. Verify there is no package doc, test,
  fixture, benchmark, fuzz target, generator input, platform variant, or
  additional generated artifact.
- [x] (2026-09-01) Trace all generated methods and the parent
  `MiniTaskExecutor` dependency: constructor/recorder lifecycle, exact `Run`
  signature, GoMock call forwarding, and the parent encode-and-sort tests that
  consume it. Search Rust test-mock and `tidb-dxf` owners and confirm no
  dependency-closed equivalent exists.
- [x] (2026-09-01) Run the package compile probe and Ready documentation gates;
  capture the no-test result and generated-source boundary in the receipt.
- [ ] Publish this receipt batch to `origin/hparser-integration`, verify the
  remote SHA, pull the branch's latest state, and continue the rolling package
  audit.

## Scope and decision

This package is one generated test-support unit. Its only behavior is the
MockGen implementation of `MiniTaskExecutor`; the execution semantics live in
the parent `pkg/dxf/importinto` package and its tests. Rust mocks for unrelated
traits cannot substitute for this contract, and introducing one before a Rust
ImportInto operator owner exists would be speculative. Keep the boundary
explicit until the parent operator and writer/collector abstractions have a
dependency-closed Rust test seam.

## Validation gate

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/dxf/importinto/mock -count=1 -run '^$'
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
`rust/testport/receipts/dxf_importinto_mock.md`. The rolling audit continues
with the next unrecorded Go package; repository-wide parity is not claimed.
