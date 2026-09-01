# `pkg/metaservice` — Go-master parity boundary receipt

Comparison source: Go `origin/master` at commit
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01).

## Complete inventory

The package contains exactly five tracked artifacts and 708 lines. Every
production file, test, and Bazel target was read in full before comparing Rust
owners. There is no `doc.go`, fixture, generated source, benchmark, or
platform-specific Go variant.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 39 | `14f5928e51f60e0498fba9692793358edee0999e` | `02d9d0db1986eeddd2680ed70ff5b82017e87086779b776b29cfc5977299a555` | etcd/PD client library and seven-shard tests |
| `etcd.go` | 149 | `119e7a4042d8b75e4bf1f528a561e29d3a0b1718` | `519af72b7ba0f82e4a15c97de3f4fd6ebb31ba82b22b2396f37f526d2e4fc837` | PD member discovery, URL parsing, and service client |
| `etcd_test.go` | 156 | `f9b63531fa04fcdbf85742fa78724c694123ab95` | `f591ba23d7410115aaafd5c463b719cbdbedbd01e3bef65bb43e03304940c550` | PD-only, URL, and optional real-etcd tests |
| `metamanager.go` | 191 | `7e7db59e298d5fddf0c640017d2a434c992b7d54` | `1838a0c2a3ebd72a510e625afbe4d028bdc87cbf86731b33d5900f297dbb42cf` | keyspace group validation and info assembly |
| `metamanager_test.go` | 173 | `7cf4a6b209379057c1dd28f5e19be7c2d78f37f1` | `802a83ca1cd74b6cf4a4d2424f73de4bf5bbde0633a3d4166bb6ee1223c458a0` | group/config/error behavior tests |

The complete production surface includes `NewEtcdMetaServiceClient`,
`GetPDAddrs`, `ParseURL`, `GetPDHttpAddrs`, `GetGroup`, `GetInfo`, `FetchInfo`,
`GetInfoAndGroupAddrs`, `Info.GroupAddrs`, and the service-client interfaces.
Tests cover nil clients, PD member URL extraction/backoff, IPv4/IPv6 and
malformed URL handling, keyspace-level-GC requirements, group-ID validation,
address trimming, default groups, and error identity. The real-etcd test is
an integration-only optional path; no fixture or generated input exists.

## Rust ownership and decision

No Rust crate owns the meta-service group protocol. `tidb-pd-client` has
independent etcd/PD transport pieces, and `tidb-txnkv` has keyspace loading,
but neither exposes the Go `ServiceClient`, PD member URL normalization,
keyspace config group selection, or keyspace-level-GC validation. Combining
those fragments without the Go server's keyspace routing would create an
uncalled Rust-only client path.

The package is recorded as an explicit boundary with no speculative source
change and no new regression test. The focused non-etcd Go tests pass; the
real-etcd test remains an integration boundary.

## Validation and risk

Profile: **WIP** for this docs-only audit; the rolling repository loop remains
in progress. No Go or Bazel source changed, so `make bazel_prepare` is not
required.

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/metaservice -run 'Test(GetGroup|GetInfo|ParseURL|NewClient|GetPDAddrsPDOnlyClient)' -count=1
# passed
```

- Correctness: no SQL or routing behavior changed; the full group-selection
  contract remains unported.
- Compatibility: a future owner must preserve PD backoff, URL forms, config
  validation, and wrapped error identities together.
- Performance: unchanged.
- Not verified locally: external etcd integration, Bazel analysis, and live
  keyspace routing.
