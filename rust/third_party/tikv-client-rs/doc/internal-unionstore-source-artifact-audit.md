# `internal/unionstore` source-artifact audit

Source of truth: `tikv/client-go@52c1e76cec993571493c81de442bcbef90cdc106`.

Rust toolchain: `nightly-2026-08-22`.

## Atomic inventory

The pinned parent package contains exactly 15 top-level artifacts and 4,813
lines. This claim includes every production source, test/support file,
build-tag variant, benchmark, metadata artifact, and direct integration edge.

| Artifact | Role | Lines | SHA-256 |
| --- | --- | ---: | --- |
| `internal/unionstore/OWNERS` | package ownership metadata | 5 | `0f9e3c1ecb1ff9ddf3fe73ad5510d6c7dd276424686a50c256d68b4b2d07f485` |
| `internal/unionstore/main_test.go` | package leak-checking test harness | 25 | `22b9694e14ee0791d07a5064d6af485b1204c7df72a339a8fb36eb954266723e` |
| `internal/unionstore/membuffer_snapshot.go` | synchronized snapshots, sequence checks, growing batched iteration, and range traversal | 222 | `95c98eb15fdd645366ae0d28f6376a47979ff154918937008dcfc420e3033102` |
| `internal/unionstore/memdb.go` | source-default ART constructor and aliases | 55 | `e04e69fb1557387ab1159e836da5304e039f004efb5e11cf7a6751d67d27cf19` |
| `internal/unionstore/memdb_art.go` | ART `MemBuffer` adapter and snapshot wrapper | 205 | `5261236251f69f39bc7baddc1223c507e39087b66969d294ffd7f1bcb7fbe7c8` |
| `internal/unionstore/memdb_bench_test.go` | twelve benchmark declarations and their workload helpers | 350 | `f62175aea1cafac2eae97a74377695ac0aeb7134865939ffbfa6ee8366b7a64d` |
| `internal/unionstore/memdb_norace_test.go` | three source-scale randomized tests excluded under Go's race detector | 234 | `a2ff5529bf7abf24dd8698b7263036e3f1da842355097d122211a0ae9dd90d77` |
| `internal/unionstore/memdb_rbt.go` | optional RBT `MemBuffer` adapter and snapshot wrapper | 242 | `020f17545162050b50f4f1875d17b6c49f1e71791e198de92cd035610c66ea3e` |
| `internal/unionstore/memdb_test.go` | 34 ordinary MemBuffer tests, including five named batched-snapshot subtests | 1,633 | `808fe52793e9dd794fc09d9a7851d35c68b141571b046c64aeae76505980171e` |
| `internal/unionstore/mock.go` | deterministic snapshot and iterator support | 93 | `844c977c4565060488d807c3ed1635b3c5a3fe6b493e6759c0a0c4701e5354c3` |
| `internal/unionstore/pipelined_memdb.go` | rotating MemDB generations, flush scheduling, reads, cache, limits, and metrics | 569 | `40d904f96787afd52637b8db958496ed5ac0ba189ff436f828073e7a6740a11d` |
| `internal/unionstore/pipelined_memdb_test.go` | all nine pipelined MemDB tests | 463 | `b6e6d884929112246020527f8fa0d9f9592315b3f17d22c01c1dd4d031605f0d` |
| `internal/unionstore/union_iter.go` | forward/reverse dirty-plus-snapshot merge iteration | 209 | `0273d20eac1ad3c685228e9ddc4a6e9f7e463019e8d088976594689660e3ede3` |
| `internal/unionstore/union_store.go` | MemBuffer interfaces and local-write/snapshot-read UnionStore | 332 | `bfb19c10cd8e78bfa69c884383c016d6c89ef2ba1f5ae05e10c24ea9b6387d19` |
| `internal/unionstore/union_store_test.go` | all four UnionStore tests | 176 | `916dca448712313b2dceb8b5218476ed6d1bcac5850c29ebf43fb3cf7e6d8f47` |

There is no package-local `doc.go`, generated source/input, fixture, example,
platform-specific source, Bazel file, or other build artifact. The only build
variant is `memdb_norace_test.go` under `//go:build !race`.

The package contains 50 ordinary top-level tests plus `TestMain`, five named
subtests, and twelve benchmarks. Splitting the named subtests into independent
Rust cases yields 54 direct unit-test cases. The twelve benchmark workloads are
also executable Rust functional contracts.

## Production surface and Rust integration

`src/transaction/unionstore.rs` owns the parent adapters and composition.
`src/transaction/art.rs` and `src/transaction/rbt.rs` supply the two completed
child-package implementations documented by their own atomic receipts.

| client-go surface | Rust owner and decision |
| --- | --- |
| `MemBuffer`, `MemBufferSnapshot`, iterator and checkpoint interfaces | Public native types in `transaction::unionstore`; source methods, errors, tombstones, options, flags, stage handles, checkpoints, limits, metrics, and unsupported-operation contracts are represented. |
| `NewMemDB`, ART adapter, RBT adapter | `MemDb` is the source-default ART facade; `RbtMemDb` is the optional parity facade. Both use the same direct test abstraction and independently pass every shared Go MemBuffer test. |
| `SnapshotGetter`, `SnapshotIter`, `SnapshotIterReverse` | Deprecated readers now retain a per-snapshot key/value-log-version view. Equal-size in-place updates to the same pre-stage version remain visible, appended versions/new keys do not, and the first outer stage freezes the view exactly as client-go does. |
| `GetSnapshot`, sequence checking, `BatchedSnapshotIter`, range traversal | `MemDbSnapshot` distinguishes the modern sequence-checked wrapper from deprecated getters. Batched iterators preserve bounds, direction, growing batches, stable staged reads, and outer-stage invalidation. |
| mutable iterators | `KvIterator::value` takes `&mut self` so retained snapshot iterators can refresh their safe owned value cache. This is an explicit Rust API compatibility change on the public `tikv::Iterator` re-export; it models client-go's stateful pointer iterator without unsafe shared-byte mutation. |
| `Checkpoint`, `RevertToCheckpoint`, stages | ART and RBT append value-log undo records outside staging as well as inside it, making every ordinary checkpoint revertible. Equal-sized in-place updates retain their source optimization and version identity. |
| `KVUnionStore` reads and merge iteration | `UnionStore`, `UnionIterator`, and `MapSnapshot` preserve local precedence, tombstone hiding, commit timestamps, forward/reverse bounds, and source snapshot fallback. |
| `PipelinedMemDB` | `PipelinedMemDb` preserves threshold conjunctions, force blocking, one active generation, cumulative length/size, generation numbers, mutable/flushing/remote read precedence, batch cache behavior, error enrichment, metrics, and source panics/errors. Flush closures receive `Arc<MemDb>` and may use the reusable `unistore` crate as a remote backend. |
| failpoint-adjusted thresholds | Test-only `set_flush_thresholds` injects the same three threshold values directly. This is the native equivalent of source failpoints and executes each branch without a Rust failpoint runtime. |
| arena memory/RWMutex details | Native ownership, `Arc`, channels, atomics, and locks replace Go arena pointers and goroutines while preserving externally observable behavior and thread lifetime. |
| `mockSnapshot`, `mockIterator` | `MapSnapshot` and owned vector iterators preserve deterministic point, batch, bounds, order, and requested commit-timestamp behavior. |

`TestMain`'s goleak contract maps to deterministic channel timeouts and an
explicit `flush_wait` in every test that starts a worker. The complete workspace
suite exits with no detached test worker. Rust has no package-global test-main
hook that is needed in addition to those ownership checks.

## Complete unit-test port

Every source declaration is independently accounted for. Direct ports live in
`src/transaction/unionstore_source_tests.rs`; the three source-scale `!race`
cases retain their parent helper infrastructure in
`src/transaction/unionstore.rs`.

| Source artifact | Source declarations | Rust accounting |
| --- | --- | --- |
| `memdb_test.go` | `TestGetSet`, `TestIterator`, `TestDiscard`, `TestFlushOverwrite`, `TestComplexUpdate`, `TestNestedSandbox`, `TestOverwrite`, `TestReset`, `TestInspectStage`, `TestDirty`, `TestFlags`, `TestKVGetSet`, `TestNewIterator`, `TestIterNextUntil`, `TestBasicNewIterator`, `TestNewIteratorMin`, `TestMemDBStaging`, `TestMemDBMultiLevelStaging`, `TestInvalidStagingHandle`, `TestMemDBCheckpoint`, `TestBufferLimit`, `TestUnsetTemporaryFlag`, `TestSnapshotGetIter`, `TestCleanupKeepPersistentFlag`, `TestIterNoResult`, `TestMemBufferCache`, `TestMemDBLeafFragmentation`, `TestReadOnlyZeroMem`, `TestKeyValueOversize`, `TestSetMemoryFootprintChangeHook`, `TestSelectValueHistory`, `TestSnapshotReaderWithWrite`, `TestBatchedSnapshotIter`, `TestBatchedSnapshotIterEdgeCase` and all five named subtests | 38 independent `source_test_*` cases; every shared case runs against both RBT and ART except source ART-specific batched iteration. Original 10,000-key scales, 100 snapshot handles/iterators, 10,000 flags, 1,000 cache reads, and all child-shape counts are retained. |
| `union_store_test.go` | `TestUnionStoreGetSet`, `TestUnionStoreDelete`, `TestUnionStoreSeek`, `TestUnionStoreIterReverse` | Four independent `source_test_union_store_*` cases. |
| `pipelined_memdb_test.go` | `TestPipelinedFlushTrigger`, `TestPipelinedFlushSkip`, `TestPipelinedFlushBlock`, `TestPipelinedFlushGet`, `TestPipelinedFlushSize`, `TestPipelinedFlushGeneration`, `TestErrorIterator`, `TestPipelinedAdjustFlushCondition`, `TestMemBufferBatchGetCache` | Nine independent source-named cases. Threshold magnitudes are reduced for deterministic speed, but all conjunction, skip, force-block, generation, cache, and read-precedence branches are unchanged. Worker starts/releases use bounded channels. |
| `memdb_norace_test.go` | `TestRandom`, `TestRandomDerive`, `TestRandomAB` | `source_test_random`, `source_test_random_derive`, and `source_test_random_ab`; the 50,000-operation mutation and A/B scales plus 101-level/512-write recursive staging scale are retained with deterministic native PRNG input. |
| `memdb_bench_test.go` | all twelve `Benchmark*` declarations | Twelve independently named `source_benchmark_*_contract` tests execute both backends and preserve each workload's key shape, value shape, operation ordering, iteration, snapshot, cache, creation, and long-key behavior at deterministic unit-test scale. |
| `main_test.go` | `TestMain` with goleak verification | all worker-owning source tests use explicit completion gates and `flush_wait`; complete workspace completion is the process-lifetime gate. |

The focused parent module contains 82 tests: 66 direct source test/benchmark
contracts plus 16 cross-cutting source-uncovered and integration tests.

## Differential findings and fixes

Two parent-package comparisons produced red-then-green production fixes:

1. ART and RBT recorded undo entries only while staging. client-go checkpoints
   are global value-log positions, so ordinary writes outside a stage must also
   be revertible. Both backends now append value records whenever an update
   cannot modify the current value in place.
2. Rust snapshots were immutable clones and ART deprecated getters also used
   the modern sequence check. client-go snapshots retain checkpointed key and
   value-log-version identity: existing equal-size values can change in place
   before staging, while appended values, newly inserted keys, and staged
   versions remain invisible. Both backends now register per-snapshot versioned
   views, propagate only matching in-place writes, and distinguish deprecated
   getters from modern sequence-checked snapshots. The exact retained getter
   and iterator objects from `TestSnapshotGetIter` now pass for both backends.

An initial shared-stage-zero implementation made every outer-stage release
clone the complete map, which the 50,000-stage A/B test exposed immediately.
The final per-snapshot registry is lazy: transactions with no snapshots pay no
registry-update or map-clone cost, and dead views are removed through `Weak`
references during the next in-place update.

The complete library gate also caught five stale child-package expectations
that treated snapshots as universally immutable. The final version-aware model
passes both the pinned child ART snapshot tests and the parent retained-reader
test: new keys remain absent from old snapshots, while same-version in-place
updates remain visible.

## Direct client-go consumers

Exact import search outside `internal/unionstore` finds six direct integration
files:

- `tikv/unionstore_export.go`
- `txnkv/transaction/2pc.go`
- `txnkv/transaction/test_probe.go`
- `txnkv/transaction/txn.go`
- `txnkv/transaction/txn_file_test.go`
- `txnkv/txnsnapshot/snapshot.go`

The Rust transaction buffer and transaction tests compile and run in both
library matrices, covering these corresponding integration edges. Deeper ART,
RBT, and arena imports are child-package edges covered by their separate atomic
receipts.

## Validation

Pinned source package and race baseline:

```text
env GOCACHE=/private/tmp/client-go-art-build-cache \
    GOMODCACHE=/private/tmp/client-go-art-module-cache \
    /private/tmp/go1.25.12/bin/go test ./internal/unionstore -count=1
# ok, 1.956s

env GOCACHE=/private/tmp/client-go-art-build-cache \
    GOMODCACHE=/private/tmp/client-go-art-module-cache \
    /private/tmp/go1.25.12/bin/go test -race \
    ./internal/unionstore -count=1
# ok, 4.418s
```

Focused Rust parent gates:

```text
cargo +nightly-2026-08-22 test -p tikv-client \
    transaction::unionstore::tests --lib --no-default-features
cargo +nightly-2026-08-22 test -p tikv-client \
    transaction::unionstore::tests --lib
# 82 passed in each configuration
```

Complete library matrices:

```text
cargo +nightly-2026-08-22 test -p tikv-client --lib \
    --no-default-features --quiet
# 1004 passed; 1 unrelated intentional ignore

cargo +nightly-2026-08-22 test -p tikv-client --lib \
    --all-features --quiet
# 1001 passed; 1 unrelated intentional ignore
```

Workspace and strict completion gates:

```text
cargo +nightly-2026-08-22 test --workspace \
    --no-default-features --quiet
cargo +nightly-2026-08-22 check --workspace \
    --all-targets --all-features
cargo +nightly-2026-08-22 clippy --workspace \
    --all-targets --all-features -- -D warnings
env RUSTDOCFLAGS='-Dwarnings --document-private-items' \
    cargo +nightly-2026-08-22 doc --workspace --all-features --no-deps
cargo +nightly-2026-08-22 test --workspace --doc \
    --all-features --quiet
# all passed; 51 doctests

cargo +nightly-2026-08-22 fmt --all -- --check
git diff --check
# passed
```

No live TiKV/PD cluster is required. Pipelined persistence is deterministic and
in-process; existing integration coverage also exercises the reusable
`unistore` crate as a remote flush backend.
