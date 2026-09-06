# `pkg/dxf/framework/proto` Go-master alignment ExecPlan

This living plan records the complete package audit and the cleanup-batch
behavior restored from Go master.

## Progress

- [x] (2026-09-02) Pinned Go master at
  `5e8a1a229a7591ddac49a0cd3b795587c2595ab9` and inventoried all 11 files,
  1,283 lines, 55 functions, and 11 top-level tests before editing.
- [x] (2026-09-02) Restored the owner-local cleanup batch-size constants,
  atomic getter/setter, range validation, test restore helper, and Bazel shard
  metadata with the focused `TestTaskCleanupBatchSize` regression.
- [x] (2026-09-02) Ran the package unit suite and recorded the Rust ownership
  boundary in the parity receipt.
- [x] (2026-09-06) Corrected the regression from `8d42bcc7035`: re-read all
  nine current Rust owner artifacts (1,883 lines) and removed the five restored
  Rust-only `#[must_use]` diagnostics from direct Go-shaped step and task-type
  returns. The restored-tree probe failed with exactly five diagnostics and
  both deny-on-discard regressions pass afterward.
- [x] (2026-09-06) Both focused regressions, all 13 owner tests, all-target
  compilation, standalone formatting, Ready lint, and diff hygiene pass. No
  Go/Bazel/module/import change requires `make bazel_prepare`.
- [x] (2026-09-06) Prepared one package-scoped corrective commit for
  publication to `origin/hparser-integration`; remote verification is the
  publication gate immediately after this plan-bearing commit.

## Scope and decision

The Go package is a value/protocol leaf. Rust `tidb-dxf` already owns generic
task and step values but not the Go owner-local cleanup knob or its HTTP
integration. Keep the setting Go-native rather than adding a disconnected
Rust API. The setting is in-memory, bounded to [1, 1000], and resettable for
tests exactly as Go master specifies.

## Validation

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/dxf/framework/proto -count=1
```

Because Go and Bazel files changed, `make bazel_prepare` is mandatory. The
final Ready batch also runs `cargo +nightly-2026-08-22 fmt --manifest-path
rust/Cargo.toml --all -- --check`, `make lint`, and `git diff --check`.

## Outcome

The complete inventory and Go-only ownership decision are in
`rust/testport/receipts/dxf_framework_proto.md`. The 2026-09-02 batch changed
no Rust production behavior. This corrective Rust-only batch restores the
discardable return contract previously established by `8d087ece625`, without
changing the values returned by any function.
