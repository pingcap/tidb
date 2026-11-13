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

//! Go `pkg/executor/insert_test.go` over global temporary tables.

use crate::support::testkit::TestKit;

/// Go `TestGlobalTempTableParallel`: eight sessions insert into one global
/// temporary table, each inside its own transaction, and each sees only its
/// own rows, whose auto-increment ids its own allocator draws (1, 2, 3).
#[test]
fn global_temp_table_parallel() {
    let mut tk = TestKit::new();
    tk.must_exec("drop table if exists temp_test");
    tk.must_exec("create global temporary table temp_test(id int primary key auto_increment) on commit delete rows");

    let threads = 8;
    let loops = 1;
    let sessions: Vec<TestKit> = (0..threads).map(|_| tk.new_tk()).collect();
    std::thread::scope(|scope| {
        for mut new_tk in sessions {
            scope.spawn(move || {
                new_tk.must_exec("use test");
                new_tk.must_exec("begin");
                for _ in 0..loops {
                    new_tk.must_exec("insert temp_test value(0)");
                    new_tk.must_exec("insert temp_test value(0), (0)");
                }
                let max_id = (loops * 3).to_string();
                new_tk.must_query("select max(id) from temp_test").check([max_id]);
                new_tk.must_exec("commit");
            });
        }
    });
    tk.must_exec("drop table if exists temp_test");
}
