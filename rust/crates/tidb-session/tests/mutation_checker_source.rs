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

//! Go `tidb_enable_mutation_checker`: with it on, every `AddRecord` /
//! `UpdateRecord` checks the mutations it staged (`CheckDataConsistency`),
//! and consistent writes pass it.

use crate::support::testkit::TestKit;

#[test]
fn consistent_writes_pass_the_mutation_checker() {
    let mut tk = TestKit::new();
    tk.must_exec("set @@tidb_enable_mutation_checker = 1");
    tk.must_exec("create table mc_t (a int primary key, b varchar(10), c int, key(b), unique key(c))");
    tk.must_exec("insert into mc_t values (1, 'x', 10), (2, 'y', 20)");
    tk.must_exec("update mc_t set b = 'z' where a = 1");
    tk.must_exec("update mc_t set a = 3 where a = 2");
    tk.must_exec("insert into mc_t values (1, 'w', 30) on duplicate key update b = 'v'");
    tk.must_exec("replace into mc_t values (3, 'u', 40)");
    tk.must_exec("delete from mc_t where a = 1");
    let rows = tk.must_query("select a, b, c from mc_t order by a");
    assert_eq!(rows.rows(), &[vec!["3".to_owned(), "u".to_owned(), "40".to_owned()]]);
}
