# `pkg/ingestor/engineapi` — complete Go-master parity boundary receipt

Comparison source: Go `origin/master` at commit
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01).

## Complete inventory

The package contains three tracked artifacts and 212 lines. Every production
and Bazel file was read in full before this receipt was written. There is no
package doc, ownership file, test, fixture, benchmark, fuzz target, generated
source, or build/platform variant in this package.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 12 | `d7b098166ac1848d8bceb7bdddcf7897fd09db81` | `c826d7b076bd7350eec66055e632a516d1f18fd6e3210b7397bb1d74d8ddb7df` | public engine-interface library target |
| `engine.go` | 126 | `76a3e8be222f0bc85f35cd770846423273cbe67a` | `b9ceff85bbe141caf0ca3a57a8284b613c521c9c826bf42bb693f34f1307e8bf` | engine/range/conflict interfaces and duplicate-key policy |
| `ingest_data.go` | 74 | `dd5ba82987da6a56e6ea57af70612c349974b8b3` | `c6dd96afbfa2f9dd43d1c40f5c032524e864638d02ca61900a04d31d7386f823` | ingest-data and forward-iterator contracts |

The package is an interface layer used by Go global sort, simple SST,
ingest-control, Lightning, DDL backfill, and DXF import code. It defines no
engine implementation of its own.

## Rust ownership and explicit boundary

Rust's `tidb-dxf` crate currently owns task/step metadata only. No Rust crate
implements the Go ingest engine, write-and-ingest RPC path, conflict-file
contract, reference-counted ingest data, or forward iterator represented by
this package. The Rust workspace also documents physical region splitting and
ingest backfill as unimplemented boundaries.

No Rust-only behavior was found to remove. Creating stand-alone traits without
an engine or consumer would be a speculative compatibility API, so this
interface package remains an explicit boundary rather than a completed Rust
port.

## Validation and risk

Profile: **WIP** for this documentation-only boundary record. No Go, Bazel,
module, or Rust source changed, so `make bazel_prepare` and Ready lint are not
required.

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/ingestor/engineapi -count=1
# passed: package compiled; no test files
```

Not verified here: Go ingest-engine consumers, TiKV write/ingest RPCs, Bazel,
or full workspace tests.
