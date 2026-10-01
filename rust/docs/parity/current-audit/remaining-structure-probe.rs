// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Diagnostic only: successful execution does not establish parity.

use tidb_session::Session;

fn main() {
    let mut session = Session::new();
    for sql in [
        "SET GLOBAL tidb_enable_stmt_summary=ON",
        "SET GLOBAL tidb_stmt_summary_internal_query=ON",
        "CREATE TABLE structure_audit_summary (id INT PRIMARY KEY, value INT)",
        "INSERT INTO structure_audit_summary VALUES (1, 2)",
        "SELECT value FROM structure_audit_summary WHERE id=1",
        "SELECT COUNT(*) FROM information_schema.tidb_statements_stats",
        "SELECT @@GLOBAL.tidb_enable_stmt_summary, @@GLOBAL.tidb_stmt_summary_internal_query",
        "SET tidb_opt_range_max_count=1000",
    ] {
        println!("{sql}\n  {:?}", session.run(sql));
    }
}
