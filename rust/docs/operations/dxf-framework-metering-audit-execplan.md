# `pkg/dxf/framework/metering` parity audit ExecPlan

This living plan follows `PLANS.md` at the repository root.

## Objective

Inventory every Go-master production, test, and build artifact in DXF metering,
trace recorder/flush/retry/failpoint behavior, and compare it with Rust owners
without introducing a speculative telemetry facade.

## Progress

- [x] (2026-09-02) Fetch and pin Go `origin/master` at
  `5e8a1a229a7591ddac49a0cd3b795587c2595ab9`.
- [x] (2026-09-02) Read all seven tracked artifacts and all 1,411 lines: the
  12-shard flaky Bazel target, three production sources, and three test files.
  Verify no package doc, fixture, testdata, generated/platform variant,
  benchmark, fuzz target, or OWNERS file exists.
- [x] (2026-09-02) Trace all 30 production and 16 test/helper declarations,
  classic/next-gen gates, recorder lifecycle, monotonic delta calculation,
  flush/retry loops, SDK/object-store writer, failure metrics, and both
  failpoint hooks. Search Rust config/executor owners and confirm no
  dependency-closed metering implementation exists.
- [x] (2026-09-02) Run the failpoint-aware exact Go-master suite and Ready
  documentation gates; record the pass and explicit Go-only boundary in the
  receipt.
- [ ] Publish this receipt batch to `origin/hparser-integration`, verify the
  remote SHA, pull the branch's latest state, and continue the rolling package
  audit.

## Scope and decision

This package is a next-gen telemetry subsystem backed by the external
metering SDK and object storage. Its behavior includes concurrency, timestamp
idempotence, retry/drop policy, and failpoint-controlled cleanup. Rust has no
dependency-closed owner, so keep the complete Go implementation as an explicit
boundary and do not fabricate a disconnected Rust writer.

## Validation gate

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
./tools/check/failpoint-go-test.sh ./pkg/dxf/framework/metering -count=1
cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint
git diff --check
```

No Go/Bazel/module or Rust source changed, so neither `make bazel_prepare` nor
a Rust test target is required for this receipt-only boundary batch.

## Outcome

The complete metering inventory, failpoint-aware validation evidence, and Rust
ownership decision are recorded in
`rust/testport/receipts/dxf_framework_metering.md`. The rolling audit continues
with the next unrecorded Go package; repository-wide parity is not claimed.
