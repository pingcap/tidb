# Transaction Workstream

Owns transaction lifecycle and eventual TiKV-client equivalence. The current
seed executor uses transaction regressions only to prove it does not mutate
state for unsupported statements. Real read/write correctness requires the
design's `tidb-txnkv` boundary, real TiKV integration, and the transaction
differential ring; no local in-memory approximation may be presented as that
future behavior.

The first real storage-facing boundaries are now `crates/tidb-codec` and
`crates/tidb-txnkv/{key,version,handle,key_flags,assertion,error,checker,txn_source,txn_scope}.rs`,
verified independently under `difftests/transaction-tests/tests/`. They own
dependency-closed portions of
`pkg/util/codec/{bytes,codec,decimal,float,number}.go` plus the complete
`pkg/kv/{version,keyflags,assertion,error,checker}.go` contracts and a bounded
`pkg/kv/key.go` handle/key slice. `txn_source.rs` separately owns the complete
transaction-source bitfield at `pkg/kv/option.go:243-295`; the rest of
`option.go` remains queued rather than being hidden behind a file-level claim.
`txn_scope.rs` now owns the dependency-closed metadata pair from
`pkg/kv/txn_scope_var.go`: global scope keeps (`global`, `global`), local scope
keeps (`local`, configured oracle scope), and default selection chooses global
only for the exact configured global value. Configuration lookup, PD/oracle
requests, and session/context propagation remain caller-owned; no fake client
or process-global configuration is embedded in the crate.
`Handle` is a closed `Int`/`Common`/`Partition` enum, and one closed map-key
enum replaces the Go implementation's four-map branching without copying Go
unsafe layout or GC accounting constants into Rust. The slice deliberately
has no RPC, timestamp oracle, MVCC, lock resolution, or commit protocol yet.
Retry classification is an exact class/code identity over the three source
errors, not a transaction protocol or a boolean shortcut. Continue by
completing an exact dependency-closed Go source/test unit;
do not add empty design-shaped modules. Living plans are
`execplans/2026-07-15-txnkv-key-foundation.md` and
`execplans/2026-07-15-handle-codec-vertical-slice.md`.

The iteration leaf now ports `pkg/kv/iter.go::NextUntil` and
`pkg/kv/utils.go::WalkMemBuffer` through explicit `KvIterator` and
`KvRetriever` traits. The helper preserves valid-before-predicate-before-next
ordering, propagates creation/advance/callback errors unchanged, and closes
the iterator on every `walk_mem_buffer` exit. Its seven direct tests run as an
independent `difftest-transaction-tests` target; the broader `pkg/kv/utils.go`
source file remains partial because the counter and keyspace portions have
separate owners and the storage protocol is not yet ported.

The key-helper leaf now exercises the portable byte semantics already exposed
by `tidb-txnkv::Key` and `KeyRange`: append-zero `Next`, carry-aware
`PrefixNext`, unsigned comparison, prefix checks, deep cloning, lowercase
hexadecimal formatting, and safe half-open point boundaries. The direct
`key_helpers` target is registered independently so it can run in parallel with
the handle and iterator targets. The separate `txnkv-copr-key-ranges` leaf now
makes `TestKeyRangeDefinition` partial through real typed `tidb-proto`
conversion of both source ranges. Its unsafe Go struct-layout alias and
architecture-specific memory-size assertion remain deliberately unported.

The inner-transaction timestamp leaf now ports the dependency-closed part of
`pkg/kv/txn.go`: a mutex-protected timestamp set, idempotent store/delete, and
the strict lower/upper minimum-selection rule. It deliberately does not claim
TiDB's oracle-backed long-running-transaction logging, process-global server
registry, `RunInNewTxn`, or jittered retry backoff; those require session,
storage-client, and timestamp-oracle state outside `tidb-txnkv`.

The retry leaf exposes the deterministic `BackOff` upper-bound arithmetic
(`min(100, 1*2^attempts)`) and its capped boundary tests, plus the exact
post-failure count decision from `RunInNewTxn`: retryable failures continue
until the final index of `MaxRetryCnt`, where the last error is returned, and
non-retryable failures return immediately. Random jitter, sleeping, storage
`Begin`/`Rollback`/`Commit`, failpoint injection, and the session-facing
`RunInNewTxn` loop remain outside this dependency-closed crate;
`TestRetryExceedCountError` is therefore covered for its deterministic
count/error contract but remains partial for orchestration.

`ResourceGroupTagBuilder.EncodeTagWithKey` is now a dependency-closed
protocol leaf. The first shared protocol contract lives in `crates/tidb-proto`:
its checked-in `proto/resourcetag.proto` is compiled by `prost-build` into the
exact `tipb.ResourceGroupTag` field numbers and `ResourceGroupTagLabel` values.
`tidb-codec::decode_table_id` ports the legacy `t` plus eight-byte
mem-comparable table-ID path and preserves Go's zero fallback; row/index label
classification reuses the existing key-kind codec. Direct tests cover Go's
nullable=false `table_id` wire presence, empty/non-empty and 510-byte digest
round trips, optional keyspace, and row/index/unknown labels. API-V2 keyspace
prefix decoding, the `tikvrpc.Request` sum type needed by
`GetFirstKeyFromRequest`, resourcegrouptag decode utility, and global
kernel/keyspace ownership remain explicit partial seams. Do not hand-roll
protobuf bytes or freeze global kernel configuration into `tidb-txnkv`.

The MVCC metadata leaf now owns the dependency-closed byte contracts from
`pkg/store/mockstore/unistore/tikv/mvcc/{mvcc,tikv}.go`: write-CF type and
varint records, the source remainder semantics of `ParseWriteCFValue`, the
fixed little-endian `DBUserMeta` timestamp pair, version-key timestamp
suffixes, extra transaction-status key markers, and safe semantic lock
metadata. It intentionally does not serialize Go's unsafe `LockHdr`, map
`kvrpcpb` operation values, or provide `ToLockInfo`, Badger/RocksDB storage,
lock resolution, PD/oracle calls, request dispatch, transaction buffers, or a
commit protocol. The broad `pkg/store/mockstore/unistore/tikv/mvcc_test.go`
suite remains partial until those storage and protocol owners exist; direct
metadata tests run in the isolated `mvcc_metadata` transaction target.

Inside the seed, shared cluster/catalog state and per-session transaction state
are separate types. Concurrent autocommit statements snapshot under a short
lock, execute without the lock, and publish through a version check with the
source-backed retry limit. Holding a cluster mutex for the duration of
`Database::run` is forbidden: it makes the final rows look correct by removing
the conflict/retry behavior that the Go test actually exercises.
