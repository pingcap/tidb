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

//! Executor boundary for parser-owned generic ALTER TABLE options.

use super::*;

#[test]
fn source_generic_options_reject_before_catalog_or_transaction_mutation() {
    for sql in [
        "alter table generic_option_boundary insert_method = last",
        "alter table generic_option_boundary pre_split_regions = 6",
        "alter table generic_option_boundary union = (other_table)",
    ] {
        let mut db = Database::new();
        step(&mut db, "create table generic_option_boundary (a int)");
        step(&mut db, "begin");
        step(&mut db, "insert into generic_option_boundary values (1)");
        step(&mut db, "savepoint before_generic_option");

        assert_eq!(
            step(&mut db, sql),
            "Unsupported(\"ALTER TABLE table options\")",
            "source SQL: {sql}"
        );
        assert!(db.transaction.is_active(), "source SQL: {sql}");
        assert!(db.transaction.savepoint_count() != 0, "source SQL: {sql}");
        assert_eq!(
            db.tables["generic_option_boundary"].cols,
            vec!["a".to_owned()],
            "source SQL: {sql}"
        );
    }
}
