# `pkg/dxf/importinto/conflictrows` parity audit ExecPlan

This living plan follows `PLANS.md` at the repository root.

## Objective

Inventory every Go-master production, test, support, generated/platform,
fixture, and build artifact in `pkg/dxf/importinto/conflictrows`; compare its
complete storage-lifecycle contract with Rust owners; and avoid implementing a
standalone cleanup policy without the importer, object-store, task-metadata,
and scheduling dependencies that make it observable.

## Progress

- [x] (2026-09-01) Fetch and pin Go `origin/master` at
  `5e8a1a229a7591ddac49a0cd3b795587c2595ab9`.
- [x] (2026-09-01) Read all four tracked artifacts and all 872 lines: two
  production sources, the complete 529-line test, and the BUILD target. Verify
  that the package has no doc, owner file, fixture, benchmark, generated input
  or output, or platform variant.
- [x] (2026-09-01) Trace every production function and method and all four
  top-level tests with their 38 leaf cases, including exact path grammar,
  retention cutoff, task-type/state decisions, batch overflow behavior,
  retry accounting, bounded diagnostics, cancellation, and URI secrecy.
- [x] (2026-09-01) Search Rust task, session, executor, metadata, importer, and
  object-storage owners. Confirm that `tidb-dxf` exposes adjacent task-step
  constants but not this package's cleanup execution path.
- [x] (2026-09-01) Run the exact Go-master package suite and the Ready
  documentation gates; record the explicit ownership boundary in the parity
  receipt.
- [x] (2026-09-01) Publish one meaningful receipt batch to
  `origin/hparser-integration`, verify the remote SHA, pull the branch's latest
  state, and continue the rolling package audit.

## Scope and decision

This package is one atomic lifecycle unit. `NewFileNamePrefix` establishes the
namespace consumed by `CleanConflictRowFiles`; cleanup couples object-store
walking/deletion, DXF cleanup metadata, ImportInto task state, the seven-day
success retention rule, batch limits, failure retry behavior, and bounded
structured diagnostics. Porting a helper or constant alone would not create
observable parity. Preserve the whole boundary until Rust has a producer,
metadata owner, cleanup caller, and storage implementation.

## Validation gate

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/dxf/importinto/conflictrows -count=1
cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint
git diff --check
```

No Go/Bazel/module or Rust source changed, so neither `make bazel_prepare` nor
a Rust test target is required for this receipt-only boundary batch.

## Outcome

The complete inventory and native ownership decision are recorded in
`rust/testport/receipts/dxf_importinto_conflictrows.md`. The rolling audit
continues with the next unrecorded Go package; repository-wide parity is not
claimed.
