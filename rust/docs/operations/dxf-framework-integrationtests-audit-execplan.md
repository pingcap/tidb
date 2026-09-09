# `pkg/dxf/framework/integrationtests` parity audit ExecPlan

This living plan follows `PLANS.md` at the repository root.

## Objective

Inventory every Go-master DXF integration-test and build artifact, then align
its task-key and cleanup-registration consumers with the Go scheduler/storage
contracts without fabricating a Rust integration harness.

## Progress

- [x] (2026-09-02) Pinned Go master at
  `5e8a1a229a7591ddac49a0cd3b795587c2595ab9`; read all 11 direct artifacts and
  2,210 lines before editing, including the benchmark and 25-shard Bazel
  target. Confirmed there are no fixtures, testdata, generated/platform
  variants, fuzz targets, or OWNERS files.
- [x] (2026-09-02) Aligned cleanup registrations with `Cleaner` and updated
  manual-recovery/task-key SQL assertions to use canonical `TaskIDToKey`.
- [x] (2026-09-02) Focused failpoint-aware integration tests passed for cleanup,
  error handling, and task-key consumers. The required Bazel preparation was
  attempted but is blocked because `bazel` is unavailable; remaining Ready
  gates and publication are tracked with this batch.
- [ ] Publish one batch commit, verify/pull `origin/hparser-integration`, and
  continue the rolling package audit.

## Scope and decision

This package is Go integration coverage for the DXF scheduler, storage,
taskexecutor, failpoint, mock-store, and TiKV lifecycle. Rust owns no
dependency-closed equivalent harness or runtime; keep these tests Go-native.

## Validation gate

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
./tools/check/failpoint-go-test.sh ./pkg/dxf/framework/integrationtests \
  -run '^(TestOnTaskError|TestFrameworkCleaner|TestModifyTaskMaxNodeCountForSubtaskBalance)$' -count=1
make bazel_prepare  # required; currently blocked because bazel is unavailable
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint
cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
git diff --check
```

## Outcome

The complete package inventory and Go-only ownership boundary are recorded in
`rust/testport/receipts/dxf_framework_integrationtests.md`; the rolling audit
continues after this bounded test/commit batch.
