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
- [x] (2026-09-06) Re-read the complete owner and removed five Rust-only
  `#[must_use]` annotations from Go-shaped step and task-type conversion
  APIs. The deny-on-discard regressions failed pre-fix with exactly five
  diagnostics and pass after the edit.
- [x] (2026-09-06) Focused regressions, all 13 owner tests, all-target check,
  formatting, Ready lint, and diff hygiene pass. No Go/Bazel/module changes
  require `make bazel_prepare`.
- [ ] Publish one package-scoped return-contract commit, verify
  `origin/hparser-integration`, and continue the rolling audit.

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
`rust/testport/receipts/dxf_framework_proto.md`; no Rust production behavior
was changed in this package.
