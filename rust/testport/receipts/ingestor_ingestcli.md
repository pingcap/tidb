# `pkg/ingestor/ingestcli` — complete Go-master parity boundary receipt

Comparison source: Go `origin/master` at commit
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01).

## Complete inventory

The client package and its generated mock package contain eight tracked
artifacts and 1,086 lines. Every production, test, Bazel, and generated mock
artifact was read in full before this receipt was written. There are no
additional fixtures, benchmarks, fuzz targets, ownership files, generated
inputs, or build/platform variants.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 54 | `0e4b8bcf0f3b9f2bbd2e357fce57f9b356e369ff` | `1c4ece24808ddfa1d729e91e68d6ba06ce26313e0529b2b4180fa0567a184720` | client library and flaky sharded test target |
| `client.go` | 302 | `cdae8e832250c4c14ede5abb233fedc75ebfee7e` | `99964ebfe99a700342425039621ff6ee3b975ba3a475661038ea268987c3f726` | streaming next-generation write and region-ingest HTTP client |
| `client_test.go` | 283 | `761c90b30bf7e4e320e1e43e590885313ef8b34e` | `54715d58f1a650a2df446a3d9b95f3f8343af40fb7c989020201b087bd75c5c5` | HTTP framing, error, URL, ingest, and duration-metric tests |
| `ingest_err.go` | 133 | `b722764e9e46605b283b4db47f37c67753cb5be3` | `6f8f53133f305b0ad7810f0cd8cf9c1640250c422d66d939515c4ecb0eaa06e8` | `errorpb` to normalized ingest-error conversion |
| `ingest_err_test.go` | 91 | `f92ebb5dab873f57ff8397cce0333e816d03d391` | `174c28ca2f4c8c9a208f356142d33ebbf1925b55fbfdfa37ab9093cef5309d3d` | retryability and protobuf-error mapping tests |
| `interface.go` | 67 | `108305b668c27d2ea27c307c164508d9f92fffac` | `fa556af2140199e6166fe121b8a6c8f9be93e178ab623d95923757668e163a6d` | write/receive/ingest request and client contracts |
| `mock/BUILD.bazel` | 12 | `34327d7193836e3700d41f789cfc6c08f76b2326` | `e246e0ba6d339d322e3a1cc197ed83f0c747044f19ef8b02d7e287c3736573e6` | generated GoMock target |
| `mock/client_mock.go` | 144 | `c4340bd0453513e7ddab7827c30878b1690765f1` | `bb2dcb6746088fe7e4c0af87ecc83ccef0bf2e2ea4440392b1dbc0c017e98671` | MockGen output for `Client` and `WriteClient` |

The client streams length-prefixed key/value pairs to `/write_sst`, receives
the next-generation JSON SST metadata representation, resolves a region's
leader store, posts metadata to `/ingest_s3`, records write/ingest histograms,
and maps TiKV protobuf errors into retryable or terminal ingestor errors. The
source tests cover the wire framing, asynchronous response/error handling,
metric cardinality, status URL selection, SST-ID annotation, and every
supported protobuf error class.

## Rust ownership and explicit boundary

Rust has external-sort SST test helpers and DXF task-step metadata, but no
next-generation TiKV worker HTTP client, PD split-client integration, Go
`errorpb` conversion owner, or ingestor metric consumer. The local
`tidb-util::extsort` file writer is not a substitute for the `/write_sst` and
`/ingest_s3` protocol owned by this Go package.

No Rust-only behavior was found to remove. Adding an HTTP wire client or
public request traits without the Go ingest engine, region jobs, retry loop,
and metrics ownership would create a disconnected implementation, so this
package remains an explicit boundary.

## Validation and risk

Profile: **WIP** for this documentation-only boundary record. No Go, Bazel,
module, or Rust source changed, so `make bazel_prepare` and Ready lint are not
required.

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/ingestor/ingestcli -count=1
# passed: all package source tests in 0.966s
```

Not verified here: a live next-generation TiKV worker, PD region resolution,
Bazel, or full workspace tests.
