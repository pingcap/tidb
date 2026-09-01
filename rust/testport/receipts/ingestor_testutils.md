# `pkg/ingestor/testutils` — complete Go-master parity boundary receipt

Comparison source: Go `origin/master` at commit
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01).

## Complete inventory

The package contains two tracked artifacts and 73 lines. Both were read in
full before this receipt was written. There are no package-local tests,
fixtures, benchmarks, fuzz targets, generated sources, ownership files, or
build/platform variants.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 14 | `80007d39f3ce4b8851203b485a7c1f6559513701` | `81eae9c6044478199a0b996c30548833e52985bb737f66ce00d75b55b37d0dc1` | public ingestor-test helper target |
| `util.go` | 59 | `c712c4cf66f90af3c868694d0b2fdaeedcf6e86c` | `867117532ca5d61743c1ec509afad56040ed62c37db7676fe53c32c77a2a4dec` | memory-object-store reader-open counter wrapper |

`TrackOpenMemStorage` and `TrackOpenFileReader` are test probes used by the Go
global-sort and simple-SST suites to assert that readers are closed. They add
no production ingest behavior.

## Rust ownership and explicit boundary

Rust has no transcreated Go object-store memory backend or global-sort/simple-
SST test suite that consumes this counter wrapper. Introducing a public Rust
test helper without those consumers would be speculative scaffolding. No
Rust-only behavior was found to remove, so this test-only package remains an
explicit boundary.

## Validation and risk

Profile: **WIP** for this documentation-only boundary record. No Go, Bazel,
module, or Rust source changed, so `make bazel_prepare` and Ready lint are not
required.

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/ingestor/testutils -count=1
# passed: package compiled; no test files
```

Not verified here: downstream global-sort/simple-SST test suites, Bazel, or
full workspace tests.
