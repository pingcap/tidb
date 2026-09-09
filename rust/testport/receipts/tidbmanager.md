# `pkg/tidbmanager` — Go-master parity boundary receipt

Comparison source: Go `origin/master` at commit
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01).

## Complete inventory

The package contains exactly three tracked artifacts and 212 lines. Every
production source, test, and Bazel target was read in full; there is no
`doc.go`, fixture, generated source, benchmark, platform variant, or extra
build input.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 18 | `1cab935fd9ea329a72b8ab88e81a2922352afdb7` | `af43da879b2a1e57ebd4cc79f3d5389c1c081868056c9b8a6ba5865edcb3d19e` | public client library and three-shard tests |
| `tidbmanager.go` | 102 | `ff6ff5c5c3a7d5642e2024d8e0af49766c27f766` | `38913bb4bce48ef06c7f0ec21f531bcaaab9b5a93a8a446900e30361e27c642c` | HTTPS/HTTP manager client and `Free` request |
| `tidbmanager_test.go` | 92 | `0547908d25c5419e90e2239967b0d2570c61cb2c` | `65b4e042c07318164eb6d3383bd08c0729d2fc77a5dc943a7c7ef7a626d291d7` | success, status-error, and body-read-error tests |

The complete production surface is `NewClient`, `Client.Free`, the
`DefaultTimeout`/path constants, and HTTP/TLS/query/error handling. The test
suite verifies PUT method, endpoint, all query values, non-200 body reporting,
and propagation of body read errors.

## Rust ownership and decision

No Rust crate owns the TiDB manager HTTP protocol or its pod lifecycle. Rust
server bootstrap and node modules contain independent lifecycle wiring, but
no `/api/tidb/free` client, TLS transport configuration, or manager endpoint
contract. Adding a helper without the manager server integration would be a
Rust-only, uncalled path rather than Go behavior.

The package is recorded as an explicit boundary with no source edit and no
new regression test. The existing Go tests pass and remain the authoritative
HTTP contract for a future dependency-closed port.

## Validation and risk

Profile: **WIP** for this docs-only audit; the rolling repository loop remains
in progress. No Go or Bazel source changed, so `make bazel_prepare` is not
required.

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/tidbmanager -count=1
# passed
```

- Correctness: manager notification remains an unported integration boundary;
  no SQL/runtime behavior was changed.
- Compatibility: a future implementation must preserve URL normalization,
  TLS/HTTP selection, timeout, query names, and response-body errors.
- Performance: unchanged.
- Not verified locally: Bazel analysis and a live manager endpoint.
