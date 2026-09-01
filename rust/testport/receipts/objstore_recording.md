# `pkg/objstore/recording` — complete Go-master parity boundary receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

## Complete inventory

The package contains three tracked artifacts and 167 lines. Every production
source, test source, and BUILD target was read in full before this receipt was
written. It has no `doc.go`, fixtures, generated files, platform variants,
benchmarks, fuzz inputs, or additional build artifacts.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 17 | `4c37a470411b310c217f84233df926f85c4288d5` | `fef86ed70ba52d54d8b38b816d0c1da517f417b1bcacfcbda45964f323c1bced` | public recording library and one-shard flaky test target |
| `recording.go` | 107 | `17a54cba8478e8aebf117baf9f192067b4de4fd2` | `f9f0f3010aaaca94043ac2193c2d51b29a9a31ee6f37765f500bdec5facb843` | atomic request/traffic counters and nil-safe access-recording methods |
| `recording_test.go` | 43 | `551d67524f5aef8db8e3e1b116d6ad78770739cc` | `49664a465cfa24802e0657da05fa83f9c0c3447a0c2650f680997a9da797b75a` | HTTP method classification regression test |

The production source contains nine methods: request method classification,
request merge/string formatting, traffic formatting, access-stat merge,
request/read/write recording, and aggregate formatting. The one test exercises
nil, GET, HEAD, PUT, POST, and ignored DELETE requests with exact atomic
counters. No package fixture or test-support file exists beyond that test.

The package is unchanged between the earlier pinned source
`e2788410d8d696605e8cb002585877a063ccc909` and Go master. No source, test,
BUILD, generated, or platform delta required reconciliation.

## Rust ownership and explicit boundary

Rust has unrelated network/RPC traffic counters in the TiKV client and
task-scoped execution details, but no object-storage `Requests`, `Traffic`, or
`AccessStats` owner with this package's HTTP method classification and nil-safe
recording API. Those counters cannot be substituted: this package is consumed
by Go object-store backends and records cloud request/byte effects, not TiKV
region RPC traffic.

No Rust-only behavior was found to remove, and adding a disconnected metrics
facade would not connect to any Rust object-store backend. The package remains
an explicit parity boundary.

## Validation and risk

Profile: **WIP** for this documentation-only boundary record. No Go, Bazel,
module, or Rust source changed; failpoints are not used by this package, so
`make bazel_prepare`, Ready lint, and Rust cargo gates are not required.

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/objstore/recording -count=1
# exact Go origin/master source: passed (one test)
```

Not verified here: Bazel, concurrent stress beyond the unit test, cloud
backends, or full-workspace tests. No Rust validation was applicable because no
Rust source changed.
