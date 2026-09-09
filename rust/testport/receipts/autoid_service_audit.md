# `pkg/autoid_service` — Go-master auto-ID service audit receipt

Status: complete inventory; no Rust production edit was required for the
current Go-master delta. The source change only switches callers from the
generated kvproto oneof accessor (`req.GetKeyspaceID()`) to the generated
field spelling used by an older kvproto revision. The Rust auto-ID client
boundary already carries `keyspace_id` as a scalar and the wire tag remains
the same, so adding another adapter would be a duplicate carrier rather than
missing behavior.

Comparison source: Go `origin/master` at
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01). The package delta is
from the earlier kvproto migration commit `17c0dd0fe4` and changes only the
`AllocAutoID` request accessor, its test request constructors, and OWNERS
filter syntax.

## Complete Go inventory

The package has exactly four tracked artifacts and 969 Go lines. There is no
`doc.go`, fixture/testdata directory, generated Go source, platform variant,
benchmark, or nested package.

| Artifact | Lines | Role |
| --- | ---: | --- |
| `BUILD.bazel` | 50 | library/test targets, dependencies, and three-shard test metadata |
| `OWNERS` | 11 | package ownership and BUILD review filters |
| `autoid.go` | 621 | allocator ranges, signed/unsigned rebasing, gRPC service, owner lifecycle |
| `autoid_test.go` | 287 | concurrent/API/GRPC service tests and request helpers |

All four files were read in full before comparison. `autoid.go` contains 22
function/method declarations; `autoid_test.go` contains 10 declarations,
including `TestConcurrent`, `TestAPI`, and `TestGRPC`. The source tests cover
range allocation, explicit rebases (forced and monotonic), signed/unsigned
overflow, keyspace routing, concurrent allocation, mock clients, and a live
etcd/GRPC service.

## Rust ownership and boundary

The corresponding Rust owners were inspected before deciding the scope:

- `tidb-exec/src/cluster_auto_id.rs` carries typed allocation/rebase requests,
  scalar `keyspace_id`, generation-safe RPC retries, cancellation-aware
  backoff, and the cluster counter transaction.
- `third_party/tikv-client-rs/proto/autoid.proto` and its generated
  `kvproto` bindings retain the Go wire-compatible `oneof keyspace` field;
  field 7 (`AutoIDRequest`) and field 6 (`RebaseRequest`) encode the same
  uint32 keyspace value as Go's accessor.
- No Rust crate currently exposes a server-side `AutoIDAlloc` implementation
  equivalent to Go's etcd owner manager. Implementing that service would
  require the owner/etcd and Go meta transaction lifecycle, so it remains an
  explicit dependency boundary rather than an isolated partial port.

The Rust owner already has six source-shaped allocator regressions for batch
recomputation, rebase windows, keyspace leader paths, cancellation, and
backoff. No Rust-only auto-ID behavior was removed in this audit.

## Validation and boundaries

- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/autoid_service -count=1` — attempted; `TestConcurrent` and `TestAPI` fail at the existing `testkit.go:83` `require.True` schema bootstrap assertion after the mock-domain setup, before exercising the accessor delta.
- `OPENSSL_DIR=... DYLD_LIBRARY_PATH=... cargo +nightly-2026-08-22 test --offline --locked -p tidb-exec --lib cluster_auto_id -- --test-threads=1` — 6 passed.
- `git diff --check` — passed.

This is an audit receipt, not a package-complete Rust transcreation claim:
the Go etcd owner service/GRPC server and live integration test are not
implemented in the Rust dependency closure. No Ready code-fix gate or
`make bazel_prepare` was required because this package produced no Rust or Go
source change.

