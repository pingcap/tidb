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
