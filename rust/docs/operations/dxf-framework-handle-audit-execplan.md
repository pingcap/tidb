# `pkg/dxf/framework/handle` parity audit ExecPlan

This living plan follows `PLANS.md` at the repository root.

## Objective

Inventory every Go-master handle production/test/build artifact, trace task
submission, waiting, scheduling-status, object-store, metering, and failpoint
contracts, and compare them with Rust owners without inventing a facade.

## Progress

- [x] (2026-09-02) Fetch and pin Go `origin/master` at
  `5e8a1a229a7591ddac49a0cd3b795587c2595ab9`.
- [x] (2026-09-02) Read all six tracked artifacts and all 1,361 lines: the
  public library/11-shard flaky test target, two production files, and three
  test files. Verify no package doc, fixture, testdata, generated/platform
  variant, benchmark, fuzz target, or OWNERS file exists.
- [x] (2026-09-02) Trace all 31 production and 11 test declarations, including
  task/history lifecycle, retry/cancel/pause/resume, kernel-specific defaults,
  PD-aware cloud URI, node/owner status, TTL flags, tune factors, metering,
  SQL integration, and failpoint hooks. Search Rust `tidb-dxf` and consumers;
  confirm no dependency-closed handle owner exists.
- [x] (2026-09-02) Run the failpoint-aware exact Go-master suite and Ready
  documentation gates; record the pass and explicit Go-only boundary in the
  receipt.
- [ ] Publish this receipt batch to `origin/hparser-integration`, verify the
  remote SHA, pull the branch's latest state, and continue the rolling package
  audit.

## Scope and decision

This package is the Go DXF service handle boundary: SQL task-table operations,
scheduler wakeups, kernel-specific defaults, object-store setup, status
summaries, and metering. Its behavior depends on Go storage/session/PD/domain
owners and failpoint-driven tests. Rust's generic DXF types cannot substitute
for those contracts; keep the complete package as an explicit Go-only boundary
until a dependency-closed service owner exists.

## Validation gate

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
./tools/check/failpoint-go-test.sh ./pkg/dxf/framework/handle -count=1
cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint
git diff --check
```

No Go/Bazel/module or Rust source changed, so neither `make bazel_prepare` nor
a Rust test target is required for this receipt-only boundary batch.

## Outcome

The complete handle inventory, failpoint-aware validation evidence, and Rust
ownership decision are recorded in
`rust/testport/receipts/dxf_framework_handle.md`. The rolling audit continues
with the next unrecorded Go package; repository-wide parity is not claimed.
