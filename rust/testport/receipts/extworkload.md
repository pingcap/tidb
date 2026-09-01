# `pkg/extworkload` — Go-master parity boundary receipt

Comparison source: Go `origin/master` at commit
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01).

## Complete inventory

The package and its nested client package contain exactly nine tracked
artifacts and 1,326 lines. Every production file, source test, and Bazel
target was read line by line before comparing Rust owners. There is no
`doc.go`, fixture/testdata directory, generated source, benchmark, fuzz target,
build-tagged source, or other platform variant.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 43 | `d50f2725e7d4bd99dac08cd0f7c8ebd5c0a3c79d` | `2d6bca673905e368ff8fff822a163f973b72efb73698f76217d9edc3f3babcde` | manager library and six-shard tests |
| `external_workload.go` | 62 | `53514ae617a1e778b47d417384d7c27921665391` | `56cccc3b48bd9abdd2154ca25e9158f0b7c7cb0be6b75e5afffc342ff2516b64` | Manager interface for GCV2, TTL, and auto-analyze |
| `manager.go` | 214 | `7f699afbeeb18701246e207e08d703b4a3579cc3` | `d6c4a516689e4b11c0aeb0e2e0cd3aedd12a00313e83ac6c5b6b6e1ba05e67ec` | manager construction, deadlines, metrics, and RPC delegation |
| `util.go` | 36 | `db17bb6a4a82fcafb56149c0fb19d91791ba5098` | `2ad15b66a7d7f176a921bdb50728bdd81a81706299d69aba2394d3fae1f18ec9` | role predicates |
| `manager_test.go` | 313 | `017ef7cf1faff4db11d56c9eefce9d2a5fb856e3` | `382e08a31f9a9ed772651b6d4c6e85109cc48b056c7ab2477ce019add7bf1632` | manager lifecycle, RPC delegation, labels, and errors |
| `util_test.go` | 61 | `613a3e3d8315c35b8176fa70f297d339ac7b7313` | `9cca8e16450c51b050149197404e20a8990875da195ec8220c92e35349690c64` | role predicate tests |
| `client/BUILD.bazel` | 29 | `525796fa92eceb37445a97f051b2c53b7a5ba7e7` | `0c805eb64e2f974d469018a5e3dc6300cb3480d94b2c814b150d4574c34495f5` | client library and six-shard test target |
| `client/client.go` | 258 | `37733bd08ef5dcfcff3615b7de7f6b4ab363adc5` | `e0c0a1ff4cb17cb3e0b766fad0f576a2da05d61d5634bba30d44b7ff1573a10a` | gRPC client, request headers, error mapping, address normalization |
| `client/client_test.go` | 310 | `f88ea59a6f7a49de7eaa508d7d1e76501c06de82` | `f5e584abab22c81f6442b6e9cc69a32542481742f17f5187c4796847bd9ad07d` | round trips, headers, interceptor, error, and validation tests |

### Production symbols

`external_workload.go` defines the `Manager` interface and all eleven
operations: lifecycle `Close`, `Role`, `Meta`; GCV2 initialize/abort/register/
recycle/lifetime; TTL register/delete/recycle/enable; and auto-analyze
register/recycle.

`util.go` implements `IsEnabled`, `IsMaster`, `IsGCV2Worker`,
`IsTTLTaskWorker`, `IsAutoAnalyzeWorker`, and the private `roleIs` predicate.

`manager.go` defines the constants and private manager/metric-label types,
then implements `NewManager`, `dialClient`, `Close`, `Role`, `Meta`,
`metricsInterceptor`, `withMetric`, `withRequestTimeout`, and all ten RPC
delegates (`InitializeGCV2`, `AbortGCV2`, `RegisterGCV2`, `RecycleGCV2`,
`UpdateGCLifeTime`, `RegisterTTLTask`, `DeleteTTLTableInfo`,
`RecycleTTLTask`, `UpdateTTLJobEnable`, `RegisterAutoAnalyze`, and
`RecycleAutoAnalyze`). Construction validates enabled config and keyspace
metadata, derives cluster TLS, dials and pings the controller, and closes on
ping failure. Delegates preserve request-specific 30-second deadlines and
Prometheus labels.

`client/client.go` defines `ErrControllerPaused`, `Option`, the composed
`Client`/GCV2/TTL/auto-analyze interfaces, and implements `New`,
`normalizeAddr`, `grpcClient.Close`, `header`, all ten RPC methods,
`mapResponse`, and `callerRPCName`. Requests carry keyspace ID/name and TiDB
pool; TLS or insecure credentials and unary interceptors are selected from
options; response errors preserve PAUSED identity and RPC names.

### Tests, test by test

`util_test.go`: `TestRolePredicatesWhenDisabled` checks nil safety and
`TestRolePredicatesDedicated` checks each of the four role predicates against
all role values.

`manager_test.go` defines the gRPC stub, fake client, and helpers, then covers
`TestNewManagerLifecycle`, `TestNewManagerPingFailure`, the table-driven
`TestManagerMethodsSetDeadlineAndMetrics` (all eleven manager delegates),
`TestManagerMethodErrorPropagation`, and `requireLabels`.

`client/client_test.go` defines the controller stub and lifecycle helpers, then
covers `TestClientRoundTrip` (Ping plus all nine mutating RPC request shapes),
`TestClientInterceptor`, `TestClientErrorMapping`,
`TestMapResponseNilResponse`, `TestNewClientValidation`, and
`TestNormalizeAddr`. The Bazel targets preserve the six-shard metadata and
all kvproto/gRPC dependencies.

## Rust ownership and decision

Rust currently owns only the configuration model and validation for
`external-workload` in `tidb-config::external_workload`, plus session/domain
tests explicitly marked as `go-parity-gap` because the manager is absent. The
existing `tidb-workloadrepo` crate is the separate `pkg/util/workloadrepo`
repository worker; it does not implement this external controller protocol.
No Rust crate provides the `externalworkloadpb` gRPC client, request-header
identity, PAUSED error mapping, manager role/deadline/metric wrappers, or the
GCV2/TTL/auto-analyze lifecycle. Implementing a partial client beside the
config fragments would create an uncalled Rust-only path and would not satisfy
the package-atomic contract.

This package is recorded as an explicit boundary with no speculative source
change and no new regression test. A future owner must port the manager and
nested client together, then wire the existing ignored session/domain/DDL
hooks before claiming parity.

## Validation and risk

Profile: **WIP** for this docs-only audit; the rolling repository loop remains
in progress. No Go or Bazel source changed, so `make bazel_prepare` is not
required.

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/extworkload/... -count=1
# passed
```

- Correctness: no external workload RPC or role behavior changed; the Go
  implementation remains authoritative.
- Compatibility: a future Rust owner must preserve protobuf field values,
  TLS/insecure dialing, address normalization, request deadlines, metrics
  labels, error identity, and manager close-on-ping-failure behavior.
- Performance: unchanged.
- Not verified locally: a live external controller, TLS certificate matrix,
  session/domain/DDL integration hooks, Bazel analysis, and workspace-wide
  Ready validation.
