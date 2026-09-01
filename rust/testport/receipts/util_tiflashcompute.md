# `pkg/util/tiflashcompute` — Go-master parity boundary receipt

Go baseline: `origin/master` at
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01).
This package owns TiFlash Compute dispatch policy and AutoScaler topology
fetching; all source is included below before any ownership decision.

## Complete inventory

All three Go-master artifacts were read in full. There are no package docs,
source tests, fixtures, generated outputs, platform variants, benchmarks, fuzz
targets, or nested packages.

| Artifact | Lines | Git blob | SHA-256 | Disposition |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 20 | `6ed9fc2fd9de52e691ee8157fb0a9388c2fe5f57` | `5f6eecd9bc267eab94cf9d3628bbbf989e742a4b8e22f880de92593a371ba3c4` | public library target with config/errno/error/log dependencies inventoried |
| `dispatch_policy.go` | 62 | `b49c5281f483d2258564bae40ee64625df9a072e` | `e0fa637c302bfcf7106c7e2d825474cac7c182a22cdabfc7e6aa0ce8df73b872` | RR/consistent-hash enum and string conversion API inventoried |
| `topo_fetcher.go` | 419 | `583b2ef4f2530ee4bb022bd874f06c816652e044` | `64f7370c7288b832049815be3ae5d6babe5f01527857fb97eb672af224fe87d3` | mock/AWS/test fetchers, HTTP parsing, timestamp CAS, and recovery paths inventoried |

The package has 22 function/method declarations (including the private
helpers), the `DispatchPolicy` and `RecoveryType` enums, `TopoFetcher`
interface, three fetcher implementations, and the AWS response carrier. The
dispatch API accepts only the two vardef strings and returns a stable invalid
sentinel/error for all other input. The topology owner validates AutoScaler
configuration, fetches mock or AWS endpoints, parses semicolon-delimited or
JSON topology, rejects malformed timestamps, uses a read/write lock and
monotonic timestamp update, and carries the memory-limit recovery query with
the original CN count. Fixed pools reuse a non-empty cached topology; test
fetchers return an empty list. Unsupported GCP and recovery operations remain
explicit errors in the Go source.

## Rust ownership and integration decision

`tidb-config::tiflash` and `tidb-vardef` preserve AutoScaler names, enum values,
defaults, and configuration validation, while `tidb-txnkv::EndpointType`
preserves the TiFlash Compute endpoint identity. No Rust crate owns the
dispatch-policy conversion, HTTP AutoScaler topology fetchers, recovery query
construction, timestamp-monotonic cache, or server consumer that refreshes
TiFlash Compute nodes. Adding an HTTP client or a cache-only topology helper
would create Rust-only behavior without the Go executor/bootstrap integration.
The package is explicitly unclaimed; no source change is justified.

## Validation

Profile: **WIP**. This is a complete three-artifact inventory and explicit
boundary audit with no code change, so `make bazel_prepare` and the Ready lint
gate are not triggered.

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/util/tiflashcompute -count=1
# ? github.com/pingcap/tidb/pkg/util/tiflashcompute [no test files]
```

## Risks and unverified behavior

- Correctness: dispatch strings, URL paths, recovery parameters, timestamp
  ordering, and empty-topology behavior remain Go-owned contracts.
- Compatibility: AutoScaler type/config metadata is present in Rust, but
  topology refresh and node selection are not claimed; a future port must move
  the HTTP, cache, and startup consumers atomically.
- Performance: the Go fetcher serializes updates with an RWMutex and avoids
  fixed-pool refetches; no runtime code changed.
- Not verified locally: Bazel analysis, live mock/AWS AutoScaler endpoints,
  GCP behavior, and any Rust server path that would consume refreshed topology.
