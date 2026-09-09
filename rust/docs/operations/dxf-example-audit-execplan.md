# `pkg/dxf/example` parity audit ExecPlan

This living plan follows `PLANS.md` at the repository root.

## Objective

Inventory every Go-master production, test, documentation, and build artifact
in the DXF example package and compare its executable demonstration contract
with Rust owners before adding or removing behavior.

## Progress

- [x] (2026-09-02) Fetch and pin Go `origin/master` at
  `5e8a1a229a7591ddac49a0cd3b795587c2595ab9`.
- [x] (2026-09-02) Read `doc.go` first, then all six tracked artifacts and all
  332 lines: the Bazel library/flaky test target, scheduler, executor,
  metadata structs, package guide, and end-to-end test. Verify no fixtures,
  testdata, generated/platform variants, benchmarks, fuzz targets, or OWNERS
  files exist.
- [x] (2026-09-02) Trace all 15 function/method declarations and the complete
  registration/submission/await flow. Compare the Go example with Rust's
  `tidb-dxf` task-type and step constants and confirm no dependency-closed
  scheduler/executor demo owner exists.
- [x] (2026-09-02) Run the exact Go-master suite and Ready documentation gates;
  record the unrelated shared-worktree parser failure separately.
- [ ] Publish this receipt batch to `origin/hparser-integration`, verify the
  remote SHA, pull the branch's latest state, and continue the rolling package
  audit.

## Scope and decision

This package is an executable DXF teaching example, not production behavior.
Rust already mirrors its framework task/step constants but has no equivalent
factory-registration, JSON metadata, mock-store, or end-to-end harness. Keep
the boundary explicit and avoid a disconnected duplicate until a Rust
dependency-closed example test surface is defined.

## Validation gate

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/dxf/example -count=1
cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint
git diff --check
```

The Go suite command above was run in the pinned detached Go-master worktree
because the shared worktree contains an unrelated in-progress parser edit. No
Go/Bazel/module or Rust source changed, so `make bazel_prepare` is not required
for this receipt-only batch.

## Outcome

The complete package inventory, Rust ownership decision, and validation evidence
are recorded in `rust/testport/receipts/dxf_example.md`. The rolling audit
continues with the next unrecorded Go package; repository-wide parity is not
claimed.
