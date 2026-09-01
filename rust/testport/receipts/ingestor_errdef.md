# `pkg/ingestor/errdef` — complete Go-master parity boundary receipt

Comparison source: Go `origin/master` at commit
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01).

## Complete inventory

The package contains two tracked artifacts and 76 lines. Both were read in
full, including the current-master `ErrTooManyDataFiles` addition. There are
no tests, fixtures, benchmarks, fuzz targets, generated sources, ownership
files, or build/platform variants.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 9 | `19529402a904d062279a8d97d3e8f13dbdd8fe1b` | `91b3fbae3ca8d76f6232edb1feba19873366fac0bbd76e306eae59d14f89c058` | public ingestor-error library target |
| `errors.go` | 67 | `0e19c12e8e8d6046005b4bf124c6bed5ba67d914` | `b0430216adcfa9404375cf6aa81b3c2f4816d9dd3faa29857b0775c4804e665d` | normalized ingest/global-sort errors and HTTP status error |

The current package defines nine TiKV ingest retry/failure sentinels, the
global-sort target-file-limit error, RFC-code-aware disk-full detection, and a
non-200 HTTP status error. Relative to the earlier source pin, Go master adds
`GlobalSort:TooManyDataFiles`; its production consumer is the new merge-group
planning logic in `pkg/ingestor/globalsort`, not this definition-only package.

## Rust ownership and explicit boundary

Rust has TiDB/MySQL error catalogs and DXF step metadata, but no ingestor error
owner, global-sort merge planner, or write-and-ingest HTTP/RPC client. None of
the Go RFC codes or `IsKVDiskFullError`/`HTTPStatusError` contracts is consumed
by a Rust ingest path. Adding only an unused Rust error constant—especially the
new target-file-limit error without its planner—would invent a disconnected
API and would not implement the Go behavior.

No Rust-only behavior was found to remove. The definitions remain an explicit
boundary until the consuming ingest/global-sort package has a real Rust owner.

## Validation and risk

Profile: **WIP** for this documentation-only boundary record. No Go, Bazel,
module, or Rust source changed, so `make bazel_prepare` and Ready lint are not
required.

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/ingestor/errdef -count=1
# passed: package compiled; no test files
```

Not verified here: the current-master global-sort merge planner, ingest retry
consumers, TiKV RPC behavior, Bazel, or full workspace tests.
