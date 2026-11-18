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

//! A foreign key's MATCH clause, which Go's parser warns about and its DDL
//! ignores (`ddl_index_parser.go`'s `warnNear`; no DDL reads `ReferenceDef.Match`).

use crate::support::testkit::TestKit;

/// Go master over unistore: `MATCH PARTIAL` creates the table, warns 1064
/// at the clause, and leaves no trace in `SHOW CREATE TABLE`. The warning's
/// column is the hand-written parser's (`warnNear` reports the offset of the
/// `)` that follows, 69); Go master's yacc grammar reports 70.
#[test]
fn match_partial_is_parsed_but_ignored() {
    let mut tk = TestKit::new();
    tk.must_exec("create table p (a int primary key)");
    tk.must_exec("create table ch (a int, foreign key (a) references p(a) match partial)");
    tk.must_query("show warnings").check([
        "Warning 1064 You have an error in your SQL syntax; check the manual that corresponds to your TiDB version for the right syntax to use line 1 column 69 near \")\"The MATCH clause is parsed but ignored by all storage engines. ",
    ]);
    tk.must_query("show create table ch").check([
        "ch CREATE TABLE `ch` (\n  `a` int DEFAULT NULL,\n  KEY `fk_1` (`a`),\n  CONSTRAINT `fk_1` FOREIGN KEY (`a`) REFERENCES `p` (`a`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
    ]);
    tk.must_exec("create table ch2 (a int)");
    tk.must_exec("alter table ch2 add foreign key (a) references p(a) match partial");
    tk.must_query("select count(*) from information_schema.referential_constraints where table_name = 'ch2'").check(["1"]);
}
