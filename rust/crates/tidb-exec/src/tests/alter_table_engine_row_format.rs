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

//! Executor boundary for comma-separated generic ALTER TABLE options.

use super::*;

#[test]
fn source_engine_row_format_rejects_before_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table alter_option_boundary (a int)");
    step(&mut db, "begin");
    step(&mut db, "savepoint before_alter_option");
    assert_eq!(
        step(
            &mut db,
            "alter table alter_option_boundary engine=innodb, row_format=dynamic"
        ),
        "Unsupported(\"ALTER TABLE multiple actions\")"
    );
    assert!(db.transaction.is_active());
    assert!(db.transaction.savepoint_count() != 0);
    assert_eq!(
        db.tables["alter_option_boundary"].cols,
        vec!["a".to_owned()]
    );
}
