# `pkg/ingestor/ingestmetric` — complete Go-master parity boundary receipt

Comparison source: Go `origin/master` at commit
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01).

## Complete inventory

The package contains two tracked artifacts and 67 lines. Both were read in
full before this receipt was written. There are no tests, fixtures, benchmarks,
fuzz targets, generated sources, ownership files, or build/platform variants.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 12 | `f5a2f7e528cfce5d4b7817df8cc17b4548981836` | `171746a64d9e5d34e5d7c952ee84f27cf2cade50d134cead540378d7a28faa0b` | public Prometheus metric library target |
| `metric.go` | 55 | `0ef1669af747901a7c7ea7fa19491de8f86b1e46` | `d4ed8f148f11a4495e0ef7618e54e88f5582851bb41efc98e7f395350722a983` | write/ingest API histogram initialization and registration |

The package creates `tidb_ingestor_write_ingest_api_duration`, pre-binds
`write` and `ingest` observers, and registers the histogram. Its Go consumers
are the next-generation ingest client and central metrics registry.

## Rust ownership and explicit boundary

No Rust crate exposes this Prometheus metric or a next-generation write/ingest
client that could observe it. Existing Rust metrics owners cover other
subsystems and are not an execution owner for this package. Adding an unused
histogram would create Rust-only registration behavior rather than preserve a
live Go call path.

No Rust-only behavior was found to remove, so the package remains an explicit
boundary until the ingest client itself is transcreated.

## Validation and risk

Profile: **WIP** for this documentation-only boundary record. No Go, Bazel,
module, or Rust source changed, so `make bazel_prepare` and Ready lint are not
required.

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/ingestor/ingestmetric -count=1
# passed: package compiled; no test files
```

Not verified here: Prometheus registry integration, ingest-client consumers,
Bazel, or full workspace tests.
