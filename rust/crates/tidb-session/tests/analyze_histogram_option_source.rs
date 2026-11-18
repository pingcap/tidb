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

//! Go `buildAnalyze` / `handleAnalyzeOptions` for `UPDATE|DROP HISTOGRAM ON`
//! and `NDVRATE`.

use crate::support::testkit::TestKit;

/// Go's planner never reads `HistogramOperation`: `UPDATE|DROP HISTOGRAM ON`
/// runs an ordinary ANALYZE of the table once `getAnalyzeColumnList` finds
/// the named columns (8137 otherwise). `NDVRATE` has no `analyzeOptionLimit`
/// entry, so `handleAnalyzeOptions` refuses every value against a limit of
/// 0. Answers as Go over unistore.
#[test]
fn histogram_operations_run_as_an_ordinary_analyze() {
    let mut tk = TestKit::new();
    tk.must_exec("create table t (a int, b int)");
    tk.must_exec("insert into t values (1,1),(2,2)");
    tk.must_exec("analyze table t update histogram on a");
    tk.must_query("show warnings").check([
        "Note 1105 Analyze use auto adjusted sample rate 1.000000 for table test.t, reason to use this rate is \"use min(1, 110000/10000) as the sample-rate=1\"",
    ]);
    tk.must_exec("analyze table t drop histogram on b");
    tk.must_exec("analyze table t update histogram on a with 2 buckets");
    tk.must_query(
        "select count(*) from mysql.stats_histograms where table_id = \
         (select tidb_table_id from information_schema.tables where table_name = 't')",
    )
    .check(["2"]);
    tk.must_contain_err_msg(
        "analyze table t update histogram on zz",
        "[planner:8137]Column 'zz' in ANALYZE column option does not exist in table 't'",
    );
    tk.must_contain_err_msg(
        "analyze table t with 0.5 ndvrate",
        "Value of analyze option NDVRATE should be positive and not larger than 0",
    );
}
