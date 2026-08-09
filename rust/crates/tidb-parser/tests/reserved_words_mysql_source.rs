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

//! Direct transcreation of the build-tagged
//! `pkg/parser/reserved_words_test.go`.

use std::process::{Command, Output};

use tidb_ast::{AdminStmt, Stmt};
use tidb_lexer::KEYWORDS;
use tidb_parser::parse;

const TIDB_ONLY_RESERVED: &[&str] = &[
    "ARRAY",
    "CURRENT_ROLE",
    "ILIKE",
    "STATS_EXTENDED",
    "TABLESAMPLE",
    "TIDB_CURRENT_TSO",
    "UNTIL",
];

const MYSQL_ONLY_RESERVED: &[&str] =
    &["DECLARE", "FUNCTION", "PURGE", "SEPARATOR", "SYSTEM"];

const WINDOW_FUNCTION_KEYWORDS: &[&str] = &[
    "CUME_DIST",
    "DENSE_RANK",
    "FIRST_VALUE",
    "GROUPS",
    "LAG",
    "LAST_VALUE",
    "LEAD",
    "NTH_VALUE",
    "NTILE",
    "OVER",
    "PERCENT_RANK",
    "RANK",
    "ROW_NUMBER",
    "WINDOW",
];

fn query(keyword: &str) -> String {
    format!("do (select 1 as {keyword})")
}

fn is_do_statement(result: Result<Stmt, tidb_parser::ParseError>) -> bool {
    matches!(result, Ok(Stmt::Admin(statement)) if matches!(&*statement, AdminStmt::Do(_)))
}

#[test]
fn test_reserved_words_match_tidb_parser() {
    for keyword in KEYWORDS {
        let sql = query(keyword.word);
        if keyword.reserved {
            if TIDB_ONLY_RESERVED.contains(&keyword.word)
                || WINDOW_FUNCTION_KEYWORDS.contains(&keyword.word)
            {
                continue;
            }
            assert!(
                parse(&sql).is_err(),
                "reserved keyword {} parsed as a bare alias: {sql}",
                keyword.word
            );
        } else {
            assert!(
                is_do_statement(parse(&sql)),
                "non-reserved keyword {} was rejected as a bare alias: {sql}",
                keyword.word
            );
        }
    }
}

fn mysql_exec(mysql: &str, sql: &str) -> Output {
    Command::new(mysql)
        .args([
            "--protocol=TCP",
            "--host=127.0.0.1",
            "--port=3306",
            "--user=root",
            "--skip-password",
            "--batch",
            "--execute",
            sql,
        ])
        .output()
        .unwrap_or_else(|error| panic!("failed to execute {mysql}: {error}"))
}

fn mysql_diagnostic(output: &Output) -> String {
    format!(
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

#[test]
#[ignore = "requires MySQL at 127.0.0.1:3306 with root and no password"]
fn test_compare_reserved_words_with_mysql() {
    let mysql = std::env::var("TIDB_RUST_MYSQL_BIN").unwrap_or_else(|_| "mysql".to_owned());
    let probe = mysql_exec(&mysql, "select 1");
    assert!(
        probe.status.success(),
        "MySQL connectivity probe failed: {}",
        mysql_diagnostic(&probe)
    );

    for keyword in KEYWORDS {
        let sql = query(keyword.word);
        if keyword.reserved {
            if TIDB_ONLY_RESERVED.contains(&keyword.word) {
                continue;
            }
            let output = mysql_exec(&mysql, &sql);
            let diagnostic = mysql_diagnostic(&output);
            assert!(
                !output.status.success(),
                "MySQL suggests that {} should not be reserved: {sql}\n{diagnostic}",
                keyword.word
            );
            assert!(
                diagnostic.contains(keyword.word),
                "MySQL error did not identify {}: {sql}\n{diagnostic}",
                keyword.word
            );
        } else {
            if MYSQL_ONLY_RESERVED.contains(&keyword.word) {
                continue;
            }
            let output = mysql_exec(&mysql, &sql);
            assert!(
                output.status.success(),
                "MySQL suggests that {} should be reserved: {sql}\n{}",
                keyword.word,
                mysql_diagnostic(&output)
            );
        }
    }
}
