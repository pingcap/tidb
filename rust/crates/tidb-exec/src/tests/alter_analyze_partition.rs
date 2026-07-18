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

//! Executor boundary for the source-owned `ALTER TABLE ... ANALYZE PARTITION`
//! parser diversion.

use super::*;

#[test]
fn alter_analyze_partition_rejects_before_catalog_or_transaction_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table atp (a int)");
    assert!(matches!(
        db.run(&tidb_parser::parse("alter table atp analyze partition p3").expect("parse")),
        Err(ExecError::Unsupported("ANALYZE TABLE"))
    ));
    assert_eq!(step(&mut db, "select * from atp"), "RS:");
}
