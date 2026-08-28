// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Gap tests for Go `pkg/executor/point_get_test.go` (items 541-546 of the
//! batch enumeration). Every test in that file drives a REAL storage
//! transaction: GC safe points, pessimistic lock caches, point-get result
//! caching under table locks, or `@@tidb_snapshot`. This tier's driver reads
//! an in-memory catalog snapshot; it has no transaction context, no lock
//! manager, and no session variables, so none of the six can run yet. The
//! point/batch-point PLAN SELECTION contracts that this tier DOES own are
//! pinned by `driver::tests::point_get`; here only the Go file's own
//! behaviors are recorded.

/// Go `pkg/executor/point_get_test.go:40::TestSelectCheckVisibility`: after
/// advancing the store's txn safe point past the transaction's start TS
/// (`UpdateTxnSafePointCache(ts+1)`), point get, batch point get, index
/// lookup, index read, and table read all fail with
/// `storeerr.ErrTxnAbortedByGC` (pkg/store/driver/error/error.go:39) when
/// their results are drained. Needs a tikv Storage handle and per-request
/// safe-point visibility checks.
#[test]
#[ignore = "go-parity-gap: no transaction/storage seam — txn safe point checks raising ErrTxnAbortedByGC (pkg/store/driver/error/error.go:39) are unported"]
fn select_checks_data_visibility_against_the_gc_safe_point() {}

/// Go `pkg/executor/point_get_test.go:74::TestReturnValues`: a pessimistic
/// `select ... for update` on a unique-index key populates the transaction
/// context's pessimistic-lock cache with BOTH the index seek key
/// (`tablecodec.EncodeIndexSeekKey(tid, 1, idxVal)`) and the row key
/// (`EncodeRowKeyWithHandle`), readable via
/// `TransactionContext.GetKeyInPessimisticLockCache`
/// (pkg/sessionctx/variable/session.go:415). Needs pessimistic
/// transactions and the txn context key cache.
#[test]
#[ignore = "go-parity-gap: the pessimistic-lock key cache (TransactionContext.GetKeyInPessimisticLockCache, pkg/sessionctx/variable/session.go:415) and `for update` execution are unported"]
fn return_values_caches_index_and_row_keys_of_a_pessimistic_point_read() {}

/// Go `pkg/executor/point_get_test.go:104::TestMemCacheReadLock`: with
/// `EnablePointGetCache` and `lock tables point read`, a point get caches
/// its result; Go asserts via `explain analyze ... num_rpc` that the second
/// identical read issues no RPC while the lock is held, that non-exist keys
/// stay cached only until `unlock tables`, and that re-locking re-caches.
/// Needs the union-store mem-cache point-get path
/// (pkg/executor/point_get.go:689), table locks, and explain-analyze
/// execution counters.
#[test]
#[ignore = "go-parity-gap: EnablePointGetCache result caching (pkg/executor/point_get.go:689), LOCK TABLES, and explain-analyze num_rpc counters are unported"]
fn mem_cache_read_lock_serves_cached_point_gets_until_unlock() {}

/// Go `pkg/executor/point_get_test.go:189::TestPartitionMemCacheReadLock`:
/// the same point-get cache over a hash-partitioned table, pinning that
/// `_tidb_rowid` values stay unique across partitions and that a cached
/// rowid answer for `id = 1` is dropped after `unlock tables` + row update
/// (`update point set id = -id`), then re-cached under a fresh lock. Needs
/// the partitioned point-get cache, table locks, and `_tidb_rowid` access.
#[test]
#[ignore = "go-parity-gap: partitioned point-get result caching under LOCK TABLES (pkg/executor/point_get.go:689) and _tidb_rowid synthesis are unported"]
fn partition_mem_cache_read_lock_releases_cached_rowids_on_unlock() {}

/// Go `pkg/executor/point_get_test.go:231::TestPointGetLockExistKey`: under
/// pessimistic transactions, `select/update/delete ... for update` on an
/// EXISTING key (primary or unique, RC or RR) blocks a concurrent session's
/// insert of that key until the holder commits, while READ-COMMITTED does
/// not lock non-exist keys; the surviving row order pins each race winner.
/// Needs two live sessions with lock waits.
#[test]
#[ignore = "go-parity-gap: cross-session pessimistic lock waits on existing point-get keys (LockKeys in pkg/executor/point_get.go) are unported"]
fn point_get_lock_exist_key_blocks_conflicting_inserts_until_commit() {}

/// Go `pkg/executor/point_get_test.go:347::TestWithTiDBSnapshot` (issue
/// 22436): with `@@tidb_snapshot` set to a recorded TSO, a point get by the
/// snapshot must NOT use math.MaxUint64 as its TS — `select * from xx where
/// id = 8` sees nothing (the row was inserted after the snapshot TSO) while
/// `select * from xx` returns the pre-snapshot rows. Needs the
/// `tidb_snapshot` session variable and snapshot reads
/// (pkg/sessionctx/variable/varsutil.go:356 validation path).
#[test]
#[ignore = "go-parity-gap: @@tidb_snapshot snapshot reads (issue 22436 fix in the point-get TS selection) are unported"]
fn point_get_uses_the_tidb_snapshot_tso_instead_of_max_uint64() {}
