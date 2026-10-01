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

//! Diagnostic observations only; exit zero is not a parity assertion.

use tidb_session::Session;

fn main() {
    let mut session = Session::new();
    for sql in [
        "SET sql_mode='STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION'",
        "CREATE TABLE audit_ordinary (a TINYINT)",
        "INSERT INTO audit_ordinary VALUES (1000)",
        "CREATE TABLE audit_generated (a INT, b TINYINT AS (a) STORED)",
        "INSERT INTO audit_generated(a) VALUES (1000)",
        "SHOW WARNINGS",
        "SELECT * FROM audit_generated",
        "SHOW SESSION_STATES",
        "SET SESSION_STATES '{}'",
        "SHOW BR JOB 1",
        "SET tidb_enable_cascades_planner=ON",
        "SELECT @@tidb_enable_cascades_planner",
        "SET tidb_read_staleness=-1",
        "SELECT * FROM audit_ordinary",
        "SET tidb_read_staleness=0",
        "CREATE TABLE audit_cache (a INT)",
        "ALTER TABLE audit_cache CACHE",
        "SHOW CREATE TABLE audit_cache",
    ] {
        println!("{sql}\n  {:?}", session.run(sql));
    }
}
