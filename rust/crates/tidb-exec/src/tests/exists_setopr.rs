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

//! Execution coverage for `EXISTS` over a source-backed set-operation body.

use super::*;

#[test]
fn exists_set_operation_executes_without_catalog_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table t0 (c0 int)");
    assert_eq!(
        step(
            &mut db,
            r#"select * from (select (92 / 4) as c4) as subq_0 where exists (
select 1 as c0
union all
select 1 as c0 from (t0 as ref_88) where (subq_0.c4) >= (subq_0.c4)
)"#,
        ),
        "RS:23.0000"
    );
}
