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

//! Gap tests for the pessimistic halves of Go
//! `pkg/executor/batch_point_get_test.go`. The retained operator boundary is
//! built in `driver/physical_builder.rs`: `LockKeys`
//! (`pkg/executor/batch_point_get.go:503`), the cache-table snapshot
//! (`pkg/executor/batch_point_get.go:126 cacheTableSnapshot` /
//! `:587 newCacheBatchGetter`), and the session's pessimistic-lock context
//! are all unported. The observable temporary-table half of this Go file IS
//! ported in `tests_batch_point_get_temporary_source`.

use crate::{run_create_table_on, Catalog, StmtContext};

/// Go `pkg/executor/batch_point_get_test.go:34::TestBatchPointGetLockExistKey`:
/// under pessimistic REPEATABLE READ, `select/update/delete ... where (id, v)
/// in (...) for update` locks only the EXISTING keys' handles while
/// READ-COMMITTED locks no non-exist key; a second session's insert of a
/// locked key blocks until the first session commits, and the surviving row
/// order pins the winner of each race. Requires two live sessions, lock
/// wait/queueing, and isolation levels -- none of which this tier models.
#[test]
#[ignore = "go-parity-gap: pessimistic lock waits between two sessions (LockKeys, pkg/executor/batch_point_get.go:503) and tx_isolation are not modeled"]
fn batch_point_get_lock_exist_key_blocks_conflicting_inserts_until_commit() {}

/// Go `pkg/executor/batch_point_get_test.go:178::TestCacheSnapShot`:
/// `executor.MockNewCacheTableSnapShot` (`pkg/executor/batch_point_get.go:177`)
/// wraps a transaction memBuffer so `Get`/`BatchGet` read staged bytes
/// (`1111`/`2222`) as `kv.ValueEntry` pairs with the membuffer's timestamp 0.
/// The tier's storage seam merges the staged buffer under one read
/// (`driver/physical_builder.rs`) and has no separate
/// cache-snapshot object to construct.
#[test]
#[ignore = "go-parity-gap: MockNewCacheTableSnapShot/cacheTableSnapshot (pkg/executor/batch_point_get.go:126/:177) have no Rust counterpart; staged-buffer reads are inlined into the storage seam"]
fn cache_snapshot_reads_staged_membuffer_values_through_get_and_batch_get() {}

/// Keeps the catalog import honest if the ignored bodies above stay empty:
/// the Go fixture this module documents (`batch_point_get_test.go:46-49`,
/// `create table t_x (id int, v int, k int, primary key(id, v))`) is
/// buildable on this tier's catalog, which pins the schema half of the Go
/// test even though the locking race cannot run.
#[test]
fn batch_point_get_fixture_tables_create() {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "create table t_0 (id int, v int, k int, primary key(id, v))",
        &mut catalog,
    )
    .expect("the Go fixture schema creates");
    run_create_table_on(
        "create table t_1 (id int, v int, k int, unique key key0(id, v))",
        &mut catalog,
    )
    .expect("the unique-key variant creates");
    let _ = StmtContext::for_query();
}
