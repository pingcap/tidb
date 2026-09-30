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

//! Diagnostic SQL, not an acceptance test or an assertion of correct output.
//! Run as a temporary tidb-session example; see session-ownership-review.md.

use tidb_session::Session;

fn run(session: &mut Session, sql: &str) {
    println!("{sql}\n  {:?}", session.run(sql));
}

fn main() {
    let mut session = Session::new();
    for sql in [
        "CREATE TABLE alias_merge (id INT PRIMARY KEY, x INT, y INT)",
        "INSERT INTO alias_merge VALUES (1,10,20)",
        "UPDATE alias_merge a JOIN alias_merge b ON a.id=b.id SET a.x=11,b.y=21",
        "SELECT * FROM alias_merge",
        "CREATE TABLE using_left (id INT PRIMARY KEY, x INT)",
        "CREATE TABLE using_right (id INT PRIMARY KEY, y INT)",
        "INSERT INTO using_left VALUES (1,10)",
        "INSERT INTO using_right VALUES (1,20)",
        "UPDATE using_left a JOIN using_right b USING (id) SET a.x=11,b.y=21",
        "SELECT * FROM using_left",
        "SELECT * FROM using_right",
        "CREATE TABLE fk_parent (id INT PRIMARY KEY)",
        "CREATE TABLE fk_child (id INT PRIMARY KEY, pid INT, FOREIGN KEY (pid) REFERENCES fk_parent(id))",
        "INSERT INTO fk_parent VALUES (1)",
        "INSERT INTO fk_child VALUES (1,1)",
        "UPDATE fk_child SET pid=999 WHERE id=1",
        "UPDATE fk_child c JOIN fk_parent p ON c.pid=p.id SET c.pid=999",
        "SELECT * FROM fk_child",
        "UPDATE fk_child SET pid=1 WHERE id=1",
        "DELETE p FROM fk_parent p JOIN fk_child c ON c.pid=p.id",
        "CREATE TABLE alter_atomic (id INT)",
        "ALTER TABLE alter_atomic ADD COLUMN added INT, ADD COLUMN id INT",
        "SHOW COLUMNS FROM alter_atomic",
        "CREATE SEQUENCE audit_sequence START WITH 7",
        "SELECT NEXTVAL(audit_sequence)",
        "SELECT SEQUENCE_NAME FROM information_schema.SEQUENCES WHERE SEQUENCE_NAME='audit_sequence'",
        "SELECT INSTANCE, VALUE FROM information_schema.CLUSTER_CONFIG WHERE `KEY`='port'",
        "SHOW WARNINGS",
        "SELECT * FROM information_schema.CLUSTER_LOG WHERE TIME > '2026-01-01 00:00:00' AND TIME < '2026-01-02 00:00:00' AND MESSAGE LIKE '%' LIMIT 1",
        "SET tidb_enable_non_prepared_plan_cache=OFF",
        "SET tidb_enable_prepared_plan_cache=ON",
        "SET tidb_session_plan_cache_size=1",
        "CREATE TABLE cache_owner (id INT PRIMARY KEY, v INT)",
        "INSERT INTO cache_owner VALUES (1,10),(2,20)",
        "PREPARE first_query FROM 'SELECT v FROM cache_owner WHERE id=?'",
        "PREPARE second_query FROM 'SELECT v+1 FROM cache_owner WHERE id=?'",
        "SET @id=1",
        "EXECUTE first_query USING @id",
        "SELECT @@last_plan_from_cache",
        "EXECUTE second_query USING @id",
        "SELECT @@last_plan_from_cache",
        "EXECUTE first_query USING @id",
        "SELECT @@last_plan_from_cache",
        "ADMIN FLUSH SESSION PLAN_CACHE",
        "EXECUTE first_query USING @id",
        "SELECT @@last_plan_from_cache",
    ] {
        run(&mut session, sql);
    }
}
