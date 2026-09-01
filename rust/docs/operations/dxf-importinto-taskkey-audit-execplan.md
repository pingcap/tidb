# `pkg/dxf/importinto/taskkey` parity audit ExecPlan

This living plan follows `PLANS.md` at the repository root.

## Objective

Inventory every Go-master artifact in `pkg/dxf/importinto/taskkey` and compare
its classic/next-generation task-key contract with Rust owners without
fabricating a formatter disconnected from kernel-mode configuration and DXF
task consumers.

## Progress

- [x] (2026-09-01) Fetch and pin Go `origin/master` at
  `5e8a1a229a7591ddac49a0cd3b795587c2595ab9`.
- [x] (2026-09-01) Read both tracked artifacts and all 57 lines: the BUILD
  target and the complete production source. Verify no package documentation,
  tests, fixtures, benchmarks, generated input/output, owner metadata, or
  platform variant exists.
- [x] (2026-09-01) Trace all three functions and both mode branches, including
  explicit-keyspace behavior in classic mode and configured-keyspace lookup in
  next-generation mode. Search Rust DXF, metadata, executor, and session owners
  and confirm that only adjacent task-type/step labels and table columns exist.
- [x] (2026-09-01) Run the exact Go-master package check and Ready documentation
  gates; record the explicit ownership boundary in the parity receipt.
- [x] (2026-09-01) Publish one meaningful receipt batch to
  `origin/hparser-integration`, verify the remote SHA, pull the branch's latest
  state, and continue the rolling package audit.

## Scope and decision

This two-file package is an atomic key-format contract. `ForJob` and
`ForJobInKeyspace` select classic versus next-generation names through the
kernel-type mode and configured keyspace; DXF metadata and scheduler/storage
consumers give those names meaning. A standalone string helper would not
preserve collision, keyspace, or mode semantics, so implementation is deferred
until those owners are dependency-closed in Rust.

## Validation gate

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/dxf/importinto/taskkey -count=1
cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint
git diff --check
```

No Go/Bazel/module or Rust source changed, so neither `make bazel_prepare` nor
a Rust test target is required for this receipt-only boundary batch.

## Outcome

The complete inventory and native ownership decision are recorded in
`rust/testport/receipts/dxf_importinto_taskkey.md`. The rolling audit continues
with the next unrecorded Go package; repository-wide parity is not claimed.
