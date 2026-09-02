# `pkg/meta/model` — DDL argument parity receipt

Status: complete direct-package inventory; restored the Go-master durable DDL
argument fields needed by the existing Rust automatic-pre-split and columnar
storage boundaries. This is one package-level batch, not a claim that the
surrounding DDL executor is complete.

Comparison source: Go `origin/master` at
`1c1a334d2be1dce64888b6e1f054462c566b0734` (2026-09-02).

## Complete Go inventory

The direct package contains 23 tracked artifacts and 9,525 lines before this
batch: `BUILD.bazel`; 14 production files (`bdr.go`, `column.go`, `db.go`,
`engine_attribute.go`, `flags.go`, `index.go`, `job.go`, `job_args.go`,
`masking_policy.go`, `placement.go`, `reorg.go`, `resource_group.go`,
`table.go`, and `table_mode.go`); and eight test files (`bdr_test.go`,
`column_test.go`, `index_test.go`, `job_args_test.go`, `job_test.go`,
`placement_test.go`, `table_mode_test.go`, and `table_test.go`). All production,
test, benchmark/fixture, generated/platform, and build artifacts were
inventory-read before editing. There is no `doc.go`, benchmark, fuzz corpus,
fixture directory, generated output, or platform-specific variant in this
direct package.

## Go behavior restored

`SetTiFlashReplicaArgs` now persists `SkipColumnarStorageGate` in v2 job JSON
while preserving v1 compatibility (v1 arguments continue to contain only the
replica specification). `IndexArg` now carries `AutoPreSplit` independently
from manual `SplitOpt`; v2 jobs can request automatic pre-splitting without
serializing a non-nil manual split option, and v1 decoding safely drops the
unknown optimization field. This separation is required for rolling upgrades:
old DDL owners ignore the new best-effort marker instead of rejecting an
empty manual split.

The existing Rust `tidb-model::IndexArg` and DDL job-argument consumers already
use the corresponding fields, so no Rust-only duplicate or compatibility shim
was added. The live DDL scheduler and PD split execution remain the explicit
`pkg/ddl` boundary documented in `receipts/ddl_auto_presplit_audit.md`.

## Regression and validation

Before the production field was restored, the focused test failed to compile
with `unknown field AutoPreSplit in struct literal of type IndexArg` and the
missing selector on the decoded argument. After the restoration:

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
./tools/check/failpoint-go-test.sh pkg/meta/model \
  -run '^(TestIndexArgAutoPreSplitIsSeparateFromManualSplit|TestGetSetTiFlashReplicaArgs)$' -count=1
# PASS

PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
./tools/check/failpoint-go-test.sh pkg/meta/model -count=1
# PASS

PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
make lint
# PASS

cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
# PASS

git diff --check
# PASS

make bazel_prepare
# BLOCKED: make: bazel: No such file or directory
```

`make bazel_prepare` is required because a Go test function and production Go
fields changed; the local Bazel executable is unavailable. No Rust owner test
was changed in this Go-only batch.

## Risks and unverified surfaces

- Correctness risk is limited to rolling-upgrade JSON compatibility and the
  distinction between automatic and manual index splitting; both v1/v2 paths
  and the absence of `split_opt` are covered by the regression.
- Full DDL job scheduling, placement-rule repair, live PD split execution,
  Windows builds, Bazel analysis, and full-workspace tests were not run.
