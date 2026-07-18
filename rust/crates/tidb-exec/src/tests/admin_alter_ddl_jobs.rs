// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Executor boundary for the typed `ADMIN ALTER DDL JOBS` parser leaf.

use super::*;

#[test]
fn admin_alter_ddl_jobs_is_unsupported_before_mutation() {
    let mut db = Database::new();
    step(
        &mut db,
        "create table admin_alter_ddl_jobs_boundary (id int)",
    );
    step(&mut db, "begin");
    step(
        &mut db,
        "insert into admin_alter_ddl_jobs_boundary values (1)",
    );

    assert!(matches!(
        db.run(
            &tidb_parser::parse(
                "admin alter ddl jobs 10 thread = 3, batch_size = 100, max_write_speed = '10MiB'"
            )
            .expect("parse ADMIN ALTER DDL JOBS")
        ),
        Err(ExecError::Unsupported("ADMIN ALTER DDL JOBS"))
    ));

    step(&mut db, "rollback");
    assert_eq!(
        step(&mut db, "select id from admin_alter_ddl_jobs_boundary"),
        "RS:"
    );
}
