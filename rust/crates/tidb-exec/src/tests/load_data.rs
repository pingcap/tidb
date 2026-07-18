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

//! Execution boundary for the source-complete `LOAD DATA` parser surface.

use super::*;

#[test]
fn load_data_is_unsupported_before_transaction_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table load_boundary (id int)");
    step(&mut db, "begin");
    step(&mut db, "insert into load_boundary values (1)");

    assert!(matches!(
        db.run(
            &tidb_parser::parse(
                "load data local infile '/file.csv' replace into table load_boundary \
                 fields terminated by ',' lines terminated by '\\n' with detached"
            )
            .expect("parse complete LOAD DATA surface")
        ),
        Err(ExecError::Unsupported("LOAD DATA"))
    ));

    // Rejection must not commit, roll back, replace, or otherwise consume the
    // already-active transaction. A subsequent explicit rollback removes the
    // row written before LOAD DATA reached the executor boundary.
    step(&mut db, "rollback");
    assert_eq!(step(&mut db, "select id from load_boundary"), "RS:");
}
