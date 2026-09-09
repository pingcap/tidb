# Complete `pkg/util/tiflash` package receipt

Status: package behavior complete against the current Go-master source. This
is a Ready package claim inside the ongoing repository parity goal.

## Pinned inventory

Behavioral source: Go `origin/master` commit
`c6054025ed4c32ab3672a2a24ea46892714d21ec` (2026-09-02).

| Artifact | Lines | Blob |
| --- | ---: | --- |
| `pkg/util/tiflash/tiflash_replica_read.go` | 72 | `c7c6e7b7cad678429fa51ae1c4ba8a0a824fc3a8` |
| `pkg/util/tiflash/BUILD.bazel` | 9 | `c273bc14734923b458a8627dae45c6f6413f86bf` |

There is no `doc.go`, package test, test support, fixture, testdata, benchmark,
generated source, or platform variant. The production file's only dependency
is `pkg/sessionctx/vardef` for three string constants.

## Go-to-Rust mapping

| Go contract | Rust owner | Decision |
| --- | --- | --- |
| `type ReplicaRead int` | `tidb_txnkv::ReplicaRead(pub isize)` | Open native-width integer domain; unnamed values remain constructible |
| `AllReplicas`, `ClosestAdaptive`, `ClosestReplicas` (`0,1,2`) | Associated constants on `ReplicaRead` | Direct discriminants |
| `IsAllReplicas`, `IsClosestReplicas` | Same-named snake-case methods | Exact equality predicates, including false for unnamed values |
| `GetTiFlashReplicaRead` | `get_tiflash_replica_read` | Exact three-way conversion; every unnamed integer falls back to `ALL_REPLICA_STR` |
| `GetTiFlashReplicaReadByStr` | `get_tiflash_replica_read_by_str` | Exact case-sensitive conversion; every other string falls back to zero |
| `MaxRemoteReadCountPerNodeForClosestReplicas == 3` | `MAX_REMOTE_READ_COUNT_PER_NODE_FOR_CLOSEST_REPLICAS` | Direct constant |
| vardef dependency | `tidb-vardef` path dependency | Uses `ALL_REPLICA_STR`, `CLOSEST_ADAPTIVE_STR`, and `CLOSEST_REPLICAS_STR`; no duplicate spelling owner |
| distsql request consumer | `DistSqlContext -> KvRequestMetadata -> ReadRequestMetadata` | The canonical `ReplicaRead` is propagated unchanged into ordinary requests |
| Bazel library target | `tidb-txnkv::tiflash`, re-exported by `tidb-distsql` | Native crate mapping |

## Removed non-source surface

The previous Rust mapping exposed `TiFlashReplicaRead` as a second public name,
five raw/string adapter methods, three duplicate string constants, and `const
fn` capability not present in Go. It also carried three tests in
`tidb-txnkv/tests/tiflash_package_source.rs` although the complete Go package
has no tests. All are removed.

The separate `tidb-distsql/tests/tiflash_replica_read_source.rs` remains: it
maps the downstream Go `pkg/distsql/context` request-propagation contract and
exercises the real consumer rather than inventing tests for this package.

## Validation

Profile: **Ready**. Commands run from the repository root:

    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/util/tiflash -count=1
    # same command in /tmp/tidb-go-latest-c605
    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go list -f '{{.GoFiles}}|{{.IgnoredGoFiles}}|{{.TestGoFiles}}' ./pkg/util/tiflash
    # same command in /tmp/tidb-go-latest-c605
    env OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_FALLBACK_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-distsql --test all tiflash_replica_read_source -- --nocapture
    env OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_FALLBACK_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
    git diff --exit-code c6054025ed4c32ab3672a2a24ea46892714d21ec -- pkg/util/tiflash
    git diff --check -- rust/testport/receipts/util_tiflash.md rust/docs/operations/util-tiflash-audit-execplan.md rust/testport/TESTPORT_EXECPLAN.md

The Bazel preparation gate is not required: no Go/Bazel/module source changed
in this boundary refresh. The Cargo lockfile changes from the earlier Rust
owner work are retained because `tidb-txnkv` consumes the existing workspace
`tidb-vardef` crate, matching the pinned Go dependency.

## Risk

The compatibility boundary is the public request policy type. Downstream
compilation and the source-backed request-projection test verify that replacing
the alias with Go's canonical name does not change request values. The package
contains no platform-specific behavior.
