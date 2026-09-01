# Complete `pkg/util/tiflash` package receipt

Status: package behavior complete against the pinned Go source. This is a WIP
package claim inside the ongoing repository parity goal, not a Ready claim.

## Pinned inventory

Behavioral source: Go commit
`e2788410d8d696605e8cb002585877a063ccc909`.

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

WIP commands, run from `rust/`:

    cargo fmt --all -- --check
    cargo check --offline -p tidb-txnkv -p tidb-distsql
    cargo test --offline -p tidb-distsql --test all tiflash_replica_read_source -- --nocapture
    git diff --check

The Bazel preparation gate is not required: no Go/Bazel/module source changed.
The Cargo lockfile changes only because `tidb-txnkv` now consumes the existing
workspace `tidb-vardef` crate, matching the pinned Go dependency.

## Risk

The compatibility boundary is the public request policy type. Downstream
compilation and the source-backed request-projection test verify that replacing
the alias with Go's canonical name does not change request values. The package
contains no platform-specific behavior.
