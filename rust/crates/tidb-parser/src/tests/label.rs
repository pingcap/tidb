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

//! Statement-label assertions transcreated from `pkg/parser/ast/ast.go`.

use crate::parse;

#[test]
fn statement_labels_match_source_contract() {
    let cases = [
        ("ALTER TABLE t ADD COLUMN a INT", "AlterTable"),
        ("ANALYZE TABLE t", "AnalyzeTable"),
        ("BEGIN", "Begin"),
        ("COMMIT", "Commit"),
        ("CREATE DATABASE d", "CreateDatabase"),
        ("CREATE INDEX i ON t(a)", "CreateIndex"),
        ("CREATE TABLE t(a INT)", "CreateTable"),
        ("CREATE VIEW v AS SELECT 1", "CreateView"),
        ("CREATE USER 'u'@'%'", "CreateUser"),
        ("DELETE FROM t", "Delete"),
        ("DROP DATABASE d", "DropDatabase"),
        ("DROP INDEX i ON t", "DropIndex"),
        ("DROP TABLE t", "DropTable"),
        ("DROP VIEW v", "DropView"),
        ("EXPLAIN SELECT 1", "ExplainSQL"),
        ("EXPLAIN ANALYZE SELECT 1", "ExplainAnalyzeSQL"),
        ("DESC t", "DescTable"),
        ("INSERT INTO t VALUES (1)", "Insert"),
        ("REPLACE INTO t VALUES (1)", "Replace"),
        ("LOAD DATA INFILE 'x' INTO TABLE t", "LoadData"),
        ("ROLLBACK", "Rollback"),
        ("SELECT 1", "Select"),
        ("SET @@x = 1", "Set"),
        ("SHOW DATABASES", "Show"),
        ("TRUNCATE TABLE t", "TruncateTable"),
        ("UPDATE t SET a = 1", "Update"),
        ("GRANT SELECT ON t TO 'u'", "Grant"),
        ("REVOKE SELECT ON t FROM 'u'", "Revoke"),
        ("DEALLOCATE PREPARE s", "Deallocate"),
        ("EXECUTE s", "Execute"),
        ("PREPARE s FROM 'SELECT 1'", "Prepare"),
        ("USE d", "Use"),
        ("SAVEPOINT s", "Savepoint"),
        (
            "CREATE GLOBAL BINDING FOR SELECT 1 USING SELECT 1",
            "CreateBinding",
        ),
        ("DROP GLOBAL BINDING FOR SELECT 1", "DropBinding"),
        ("DO 1", "other"),
        ("SELECT 1 UNION SELECT 2", "other"),
    ];

    for (sql, expected) in cases {
        let stmt = parse(sql).unwrap_or_else(|error| panic!("parse {sql:?}: {error:?}"));
        assert_eq!(stmt.label(), expected, "{sql}");
    }
}
