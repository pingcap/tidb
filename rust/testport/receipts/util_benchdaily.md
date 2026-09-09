# `pkg/util/benchdaily` — Go-master parity boundary receipt

Go baseline: `origin/master` at
`c6054025ed4c32ab3672a2a24ea46892714d21ec` (2026-09-02). The package is
unchanged from the previous authority pin. It is a CI benchmark-result JSON
harness, not a server runtime owner; no Rust crate provides its
command-line/file aggregation lifecycle.

## Complete inventory

All four artifacts were read in full:

| Artifact | Lines | Git blob | SHA-256 | Disposition |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 23 | `242d81f40d931743a1d0da3c3ad12f1ea1f9fc0e` | `397a7ecb21b6618561e5d701514e7500e363ca7b3ff12cb9319fbd4d0c929163` | library/test target inventoried |
| `bench_daily.go` | 126 | `7722def61cced47181f9d5f7dc2ef02085201c4f` | `80682801420bdff585216959f44e43201bcab017b22ddc587f458515baf168cb` | benchmark conversion, caller-name reflection, flags, and JSON writer inventoried |
| `bench_daily_test.go` | 79 | `2139b1d93502efa4874c5d135c72f6dd5a718846` | `5cd26c8e9f3a959e3d2955caa938c04188ff3e5d0a658aee44375e0feca19517` | repository scan/aggregation test inventoried |
| `main_test.go` | 33 | `8b1f2a0a264558bc008e8539130ddcafeb0504ce` | `40ed894bd41ccda562b575813ff13ee89de7d2129ba18c020eea670f38403985` | common setup/goleak harness inventoried |

There is no `doc.go`, fixture, generated or platform variant, benchmark
function in this package (the code invokes arbitrary caller-supplied Go
benchmarks), fuzz target, or nested package.

## Go behavior

`Run` parses the `-outfile` flag, returns immediately when no output path is
provided, runs each supplied `testing.B` through `testing.Benchmark`, captures
name/ns/op/allocs/op/bytes/op, and writes a JSON array. The private reader
panics on open/decode errors and the writer exits on file/encode errors. The
test-only `TestBenchDaily` parses `-date`/`-commit`, recursively finds every
`bench_daily.json` outside `.git`, combines result arrays, and writes the
date/commit envelope. `TestMain` supplies common setup and leak exclusions.

## Rust ownership and integration decision

Rust has ordinary benchmark targets and source-derived fixed-workload tests,
but no equivalent `testing.B` reflection adapter, `-outfile`/`-date`/`-commit`
flags, repository-wide result-file scan, or CI JSON envelope. The behavior is
CI tooling rather than a database runtime contract; adding a Rust benchmark
serializer would be a second, Rust-only reporting path. No source change is
justified.

## Validation

Profile: **WIP**. This is an inventory and explicit boundary audit with no
code fix and no package-completion claim; `make bazel_prepare` and the Ready
lint gate are not triggered.

```text
git ls-tree -r -l c6054025ed4c32ab3672a2a24ea46892714d21ec pkg/util/benchdaily
# exactly the four artifacts and sizes listed above
git diff --exit-code c6054025ed4c32ab3672a2a24ea46892714d21ec..HEAD -- pkg/util/benchdaily
# no Go package drift at the latest authority
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/util/benchdaily -count=1
# ok; benchmark test returns early without -date/-outfile, in the current and
# exact detached latest-master (/tmp/tidb-go-latest-c605) worktrees
git diff --check
# passed
```

## Risks and unverified behavior

- Correctness: the no-op default benchmark path builds and passes; no Rust
  benchmark-reporting owner is claimed.
- Compatibility: CI result JSON shape and file discovery remain Go tooling
  contracts.
- Performance: no runtime code changed. A future port must avoid scanning the
  repository during ordinary unit tests.
- Not verified locally: CI invocation with populated benchmark files, output
  encoding failures, cross-platform path behavior, and a Rust tooling owner.
