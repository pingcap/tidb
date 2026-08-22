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

//! Go `pkg/meta/meta_test.go`'s `TestIterDatabases`, ported row for row over
//! the deterministic in-memory transaction instead of a mock TiKV store.

use tidb_ast::CiString;
use tidb_meta::transaction::{MemoryTransaction, Mutator};
use tidb_meta::MetaError;
use tidb_model::DBInfo;

// Go `TestIterDatabases`.
#[test]
fn iter_databases_visits_every_database_and_stops_on_the_callback_error() {
    let meta = Mutator::new(MemoryTransaction::default());

    // Prepare multiple databases.
    let databases = [
        DBInfo {
            id: 1,
            name: CiString::new("db1"),
            ..Default::default()
        },
        DBInfo {
            id: 2,
            name: CiString::new("db2"),
            ..Default::default()
        },
        DBInfo {
            id: 3,
            name: CiString::new("db3"),
            ..Default::default()
        },
    ];
    for database in &databases {
        meta.create_database(database).unwrap();
    }

    // Iterate all databases and collect names.
    let mut names = Vec::new();
    meta.iter_databases(|info| {
        names.push(info.name.original().to_owned());
        Ok(())
    })
    .unwrap();
    names.sort();
    assert_eq!(names, ["db1", "db2", "db3"]);

    // Verify early stop behavior by returning a sentinel error from the callback.
    let mut count = 0;
    let sentinel = Err(MetaError::Storage("stop".to_owned()));
    assert_eq!(
        meta.iter_databases(|_| {
            count += 1;
            if count == 2 {
                sentinel.clone()
            } else {
                Ok(())
            }
        }),
        sentinel,
    );
    assert_eq!(count, 2);
}
