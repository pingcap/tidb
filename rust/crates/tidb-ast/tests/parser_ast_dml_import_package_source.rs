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

//! Ports of `pkg/parser/ast/dml_test.go` (origin/master) covering import
//! statements: `TestImportActions`, `TestImportIntoRestore`,
//! `TestFulltextSearchModifier`, `TestImportIntoSecureText`, and
//! `TestImportIntoFromSelectInvalidStmt`.
//!
//! Go parses each SQL case and restores the extracted node under
//! `format.DefaultRestoreFlags`. This crate owns that AST state, so cases
//! hand-build it; the SecureText rows assert against this crate's
//! deterministic redaction order (Go iterates a map there and therefore
//! needed a regex — see each comment).

use tidb_ast::{
    AdminStmt, Assignment, BinaryOp, ColumnOrUserVar, DmlStmt, Expr, ImportIntoStmt,
    ImportSource, Join, LoadDataOption, NodeBox, QueryStmt, SelectField, SelectStatementKind,
    SelectStmt, SetOprStmt, SetOprTerm, SetOprTermBody, SetOp, ShowImportGroupsStmt,
    ShowImportJobsStmt, Stmt, TableRef, WithClause,
};

fn int(value: &str) -> Expr {
    Expr::Int(value.to_string())
}

fn string(value: &str) -> Expr {
    Expr::String(value.to_string())
}

fn column(path: &[&str]) -> Expr {
    Expr::Column(path.iter().map(|name| name.to_string()).collect())
}

fn empty_select() -> SelectStmt {
    SelectStmt {
        kind: SelectStatementKind::Select,
        is_in_braces: false,
        with: None,
        hints: Vec::new(),
        priority: Default::default(),
        sql_small_result: false,
        sql_big_result: false,
        sql_buffer_result: false,
        sql_no_cache: false,
        straight_join: false,
        calc_found_rows: false,
        distinct: false,
        all: false,
        fields: Default::default(),
        values: Vec::new(),
        from: None,
        where_clause: None,
        group_by: Vec::new(),
        rollup: false,
        having: None,
        windows: Vec::new(),
        order_by: Vec::new(),
        limit: None,
        lock: None,
        into_outfile: None,
        into_vars: Vec::new(),
    }
}

/// Adds a wildcard field and single-table FROM clause.
fn select_star_from(table: &[&str]) -> NodeBox<QueryStmt> {
    let mut select = empty_select();
    select.fields.push(SelectField::Wildcard(Vec::new()));
    select.from = Some(single_join(table));
    NodeBox::new(QueryStmt::Select(Box::new(select)))
}

fn single_join(table: &[&str]) -> Join {
    Join {
        left: tidb_ast::JoinNode::Table(TableRef {
            name: table.iter().map(|name| name.to_string()).collect(),
            partitions: Vec::new(),
            alias: None,
            as_of: None,
            hints: Vec::new(),
            sample: None,
        }),
        right: None,
        tp: tidb_ast::JoinType::Cross,
        straight: false,
        on: None,
        using: Vec::new(),
        natural: false,
        explicit_parens: false,
    }
}

/// Wraps an admin statement into a restorable root.
fn admin(stmt: AdminStmt) -> Stmt {
    Stmt::Admin(NodeBox::new(stmt))
}

/// Builds an IMPORT INTO `t` statement from its parts.
fn import_into_stmt(
    columns_and_user_vars: Vec<ColumnOrUserVar>,
    column_assignments: Vec<Assignment>,
    source: ImportSource,
    options: Vec<LoadDataOption>,
) -> Stmt {
    Stmt::Dml(NodeBox::new(DmlStmt::ImportInto(Box::new(ImportIntoStmt {
        table: vec!["t".to_string()],
        columns_and_user_vars,
        column_assignments,
        source,
        options,
    }))))
}

/// Returns the SecureText of an import statement root.
fn secure_text(stmt: &Stmt) -> String {
    let Stmt::Dml(dml) = stmt else {
        panic!("expected DML");
    };
    let DmlStmt::ImportInto(import) = dml.as_ref() else {
        panic!("expected import");
    };
    import.secure_text()
}

fn option(name: &str, value: Option<Expr>) -> LoadDataOption {
    LoadDataOption {
        name: name.to_string(),
        value,
    }
}

/// A CTE `c AS (SELECT * FROM xx)` body for reuse below.
fn cte_c(query: NodeBox<QueryStmt>) -> WithClause {
    WithClause {
        recursive: false,
        ctes: vec![tidb_ast::Cte {
            name: "c".to_string(),
            columns: Vec::new(),
            query,
        }],
    }
}

fn bare_select_body(from_table: &'static [&'static str]) -> SelectStmt {
    let mut select = empty_select();
    select.fields.push(SelectField::Wildcard(Vec::new()));
    select.from = Some(single_join(from_table));
    select
}

/// `pkg/parser/ast/dml_test.go::TestImportActions`.
///
/// Go parses and restores nine statements verbatim: CANCEL IMPORT JOB,
/// SHOW [RAW] IMPORT JOB[S] (with WHERE filters), and SHOW IMPORT
/// GROUP[S]. The same typed states are built here — Go's own parser keeps
/// both inspection shapes on `ShowStmt`; Rust folds them into dedicated
/// payloads of `AdminStmt` with identical restore text.
#[test]
fn import_actions() {
    let jobs =
        |raw: bool, job_id: Option<i64>, filter: Option<Expr>| {
            admin(AdminStmt::ShowImportJobs(Box::new(ShowImportJobsStmt {
                raw,
                job_id,
                where_clause: filter,
            })))
        };

    let cases: [(Stmt, &str); 9] = [
        (
            admin(AdminStmt::CancelImportJob(123)),
            "CANCEL IMPORT JOB 123",
        ),
        (jobs(false, None, None), "SHOW IMPORT JOBS"),
        (jobs(false, Some(123), None), "SHOW IMPORT JOB 123"),
        (jobs(true, None, None), "SHOW RAW IMPORT JOBS"),
        (jobs(true, Some(123), None), "SHOW RAW IMPORT JOB 123"),
        (
            jobs(
                true,
                None,
                Some(Expr::Binary(
                    BinaryOp::Eq,
                    Box::new(column(&["group_key"])),
                    Box::new(string("g")),
                )),
            ),
            "SHOW RAW IMPORT JOBS WHERE `group_key`=_UTF8MB4'g'",
        ),
        (
            jobs(
                false,
                None,
                Some(Expr::Binary(
                    BinaryOp::Gt,
                    Box::new(column(&["aa"])),
                    Box::new(int("1")),
                )),
            ),
            "SHOW IMPORT JOBS WHERE `aa`>1",
        ),
        (
            admin(AdminStmt::ShowImportGroups(Box::new(ShowImportGroupsStmt {
                group_key: None,
                where_clause: None,
            }))),
            "SHOW IMPORT GROUPS",
        ),
        (
            admin(AdminStmt::ShowImportGroups(Box::new(ShowImportGroupsStmt {
                group_key: Some("123".to_string()),
                where_clause: None,
            }))),
            // A group key restores as a plain quoted string, no _UTF8MB4.
            "SHOW IMPORT GROUP '123'",
        ),
    ];
    for (stmt, want) in cases {
        assert_eq!(stmt.restore(), want);
    }
}

/// `pkg/parser/ast/dml_test.go::TestImportIntoRestore`.
#[test]
fn import_into_restore() {
    let file = |path: &str| ImportSource::File {
        path: path.to_string(),
        format: None,
    };
    let union_xx_yy = SetOprStmt {
        with: None,
        is_in_braces: false,
        terms: vec![
            SetOprTerm {
                op: None,
                in_braces: false,
                body: SetOprTermBody::Select(Box::new(bare_select_body(&["xx"]))),
            },
            SetOprTerm {
                op: Some(SetOp::Union { all: false }),
                in_braces: false,
                body: SetOprTermBody::Select(Box::new(bare_select_body(&["yy"]))),
            },
        ],
        order_by: Vec::new(),
        limit: None,
        lock: None,
        outer_order_by: Vec::new(),
        outer_limit: None,
        outer_lock: None,
    };
    // The CTE-scoped variant references `c` twice, per the Go row.
    let union_cc = SetOprStmt {
        with: Some(cte_c(select_star_from(&["xx"]))),
        is_in_braces: false,
        terms: vec![
            SetOprTerm {
                op: None,
                in_braces: false,
                body: SetOprTermBody::Select(Box::new(bare_select_body(&["c"]))),
            },
            SetOprTerm {
                op: Some(SetOp::Union { all: false }),
                in_braces: false,
                body: SetOprTermBody::Select(Box::new(bare_select_body(&["c"]))),
            },
        ],
        order_by: Vec::new(),
        limit: None,
        lock: None,
        outer_order_by: Vec::new(),
        outer_limit: None,
        outer_lock: None,
    };

    let cases: [(Stmt, String); 14] = [
        (
            import_into_stmt(Vec::new(), Vec::new(), file("/file.csv"), Vec::new()),
            "IMPORT INTO `t` FROM '/file.csv'".to_string(),
        ),
        (
            import_into_stmt(
                vec![
                    ColumnOrUserVar::Column("a".to_string()),
                    ColumnOrUserVar::UserVar("1".to_string()),
                    ColumnOrUserVar::Column("c".to_string()),
                ],
                Vec::new(),
                file("/file.csv"),
                Vec::new(),
            ),
            "IMPORT INTO `t` (`a`,@`1`,`c`) FROM '/file.csv'".to_string(),
        ),
        (
            import_into_stmt(
                Vec::new(),
                vec![Assignment {
                    col: vec!["a".to_string()],
                    value: int("100"),
                }],
                file("/file.csv"),
                Vec::new(),
            ),
            "IMPORT INTO `t` SET `a`=100 FROM '/file.csv'".to_string(),
        ),
        (
            import_into_stmt(
                vec![
                    ColumnOrUserVar::Column("b".to_string()),
                    ColumnOrUserVar::Column("c".to_string()),
                ],
                vec![Assignment {
                    col: vec!["a".to_string()],
                    value: int("100"),
                }],
                file("/file.csv"),
                Vec::new(),
            ),
            "IMPORT INTO `t` (`b`,`c`) SET `a`=100 FROM '/file.csv'".to_string(),
        ),
        (
            import_into_stmt(
                Vec::new(),
                Vec::new(),
                ImportSource::File {
                    path: "/file.csv".to_string(),
                    format: Some("csv".to_string()),
                },
                Vec::new(),
            ),
            "IMPORT INTO `t` FROM '/file.csv' FORMAT 'csv'".to_string(),
        ),
        (
            import_into_stmt(
                Vec::new(),
                Vec::new(),
                file("/file.csv"),
                vec![option("detached", None)],
            ),
            "IMPORT INTO `t` FROM '/file.csv' WITH detached".to_string(),
        ),
        (
            import_into_stmt(
                Vec::new(),
                Vec::new(),
                file("/file.csv"),
                vec![option("detached", None), option("thread", Some(int("1")))],
            ),
            "IMPORT INTO `t` FROM '/file.csv' WITH detached, thread=1".to_string(),
        ),
        (
            import_into_stmt(
                Vec::new(),
                Vec::new(),
                file("/file.csv"),
                vec![
                    option("fields_terminated_by", Some(string("\t"))),
                    option("detached", None),
                ],
            ),
            // `_UTF8MB4'\t'` carries a literal TAB between the quotes,
            // matching Go's expectation byte-for-byte.
            "IMPORT INTO `t` FROM '/file.csv' WITH fields_terminated_by=_UTF8MB4'\t', detached"
                .to_string(),
        ),
        (
            import_into_stmt(
                Vec::new(),
                Vec::new(),
                file("/file.csv"),
                vec![
                    option("fields_terminated_by", Some(string("\t"))),
                    option("detached", None),
                    option("thread", Some(int("1"))),
                ],
            ),
            "IMPORT INTO `t` FROM '/file.csv' WITH fields_terminated_by=_UTF8MB4'\t', detached, thread=1"
                .to_string(),
        ),
        (
            import_into_stmt(
                Vec::new(),
                Vec::new(),
                ImportSource::Select {
                    query: select_star_from(&["xx"]),
                    parenthesized: false,
                },
                Vec::new(),
            ),
            "IMPORT INTO `t` FROM SELECT * FROM `xx`".to_string(),
        ),
        (
            // FROM with-clause query plus a trailing WITH thread=1 option.
            import_into_stmt(
                Vec::new(),
                Vec::new(),
                ImportSource::Select {
                    query: NodeBox::new(QueryStmt::SetOpr(Box::new(SetOprStmt {
                        with: Some(cte_c(select_star_from(&["xx"]))),
                        is_in_braces: false,
                        terms: vec![SetOprTerm {
                            op: None,
                            in_braces: false,
                            body: SetOprTermBody::Select(Box::new(bare_select_body(&["c"]))),
                        }],
                        order_by: Vec::new(),
                        limit: None,
                        lock: None,
                        outer_order_by: Vec::new(),
                        outer_limit: None,
                        outer_lock: None,
                    }))),
                    parenthesized: false,
                },
                vec![option("thread", Some(int("1")))],
            ),
            "IMPORT INTO `t` FROM WITH `c` AS (SELECT * FROM `xx`) SELECT * FROM `c` WITH thread=1"
                .to_string(),
        ),
        (
            import_into_stmt(
                Vec::new(),
                Vec::new(),
                ImportSource::Select {
                    query: NodeBox::new(QueryStmt::SetOpr(Box::new(union_xx_yy))),
                    parenthesized: false,
                },
                vec![option("thread", Some(int("1")))],
            ),
            "IMPORT INTO `t` FROM SELECT * FROM `xx` UNION SELECT * FROM `yy` WITH thread=1"
                .to_string(),
        ),
        (
            import_into_stmt(
                Vec::new(),
                Vec::new(),
                ImportSource::Select {
                    query: NodeBox::new(QueryStmt::SetOpr(Box::new(union_cc))),
                    parenthesized: false,
                },
                vec![option("thread", Some(int("1")))],
            ),
            "IMPORT INTO `t` FROM WITH `c` AS (SELECT * FROM `xx`) SELECT * FROM `c` UNION SELECT * FROM `c` WITH thread=1"
                .to_string(),
        ),
        (
            import_into_stmt(
                Vec::new(),
                Vec::new(),
                ImportSource::Select {
                    query: select_star_from(&["xx"]),
                    parenthesized: true,
                },
                Vec::new(),
            ),
            "IMPORT INTO `t` FROM (SELECT * FROM `xx`)".to_string(),
        ),
    ];
    for (stmt, want) in cases {
        assert_eq!(stmt.restore(), want);
    }
}

/// `pkg/parser/ast/dml_test.go::TestFulltextSearchModifier`.
///
/// Go pins three predicates on the zero-value
/// `FulltextSearchModifierNaturalLanguageMode`. Rust spells the same state
/// as [`tidb_ast::MatchModifier`] (its `None` variant covers both no
/// modifier and the implicit natural-language mode).
#[test]
fn fulltext_search_modifier() {
    let modifier = tidb_ast::MatchModifier::None;
    assert!(!modifier.is_boolean_mode());
    assert!(modifier.is_natural_language_mode());
    assert!(!modifier.with_query_expansion());
}

/// `pkg/parser/ast/dml_test.go::TestImportIntoSecureText`.
///
/// Go matched redacted output with regexes because the sensitive query
/// parameters flow through a map (random order). This crate's `redact_url`
/// sorts parameters deterministically, so exact secured strings are
/// asserted after substituting every sensitive value with `xxxxxx`.
#[test]
fn import_into_secure_text() {
    let build = |path: String, cloud_uri: Option<String>| {
        let options = match cloud_uri {
            Some(uri) => vec![LoadDataOption {
                name: "cloud_storage_uri".to_string(),
                value: Some(Expr::RawString(uri)),
            }],
            None => Vec::new(),
        };
        import_into_stmt(
            Vec::new(),
            Vec::new(),
            ImportSource::File {
                path,
                format: None,
            },
            options,
        )
    };

    // Case 1: s3 credentials are redacted.
    let s3 = build(
        "s3://bucket/prefix?access-key=aaaaa&secret-access-key=bbbbb".to_string(),
        None,
    );
    assert_eq!(
        secure_text(&s3),
        "IMPORT INTO `t` FROM 's3://bucket/prefix?access-key=xxxxxx&secret-access-key=xxxxxx'"
    );

    // Case 2: gcs keys are NOT redacted — secured text equals the plain
    // restore byte-for-byte.
    let gcs = build(
        "gcs://bucket/prefix?access-key=aaaaa&secret-access-key=bbbbb".to_string(),
        None,
    );
    let expected_plain =
        "IMPORT INTO `t` FROM 'gcs://bucket/prefix?access-key=aaaaa&secret-access-key=bbbbb'";
    assert_eq!(gcs.restore(), expected_plain);
    assert_eq!(secure_text(&gcs), expected_plain);

    // Case 3: s3 path plus WITH cloud_storage_uri whose own URL also gets
    // redacted; the rewritten option restores as a plain quoted string.
    let combined = build(
        "s3://bucket/prefix?access-key=aaaaa&secret-access-key=bbbbb".to_string(),
        Some("s3://bucket/prefix?access-key=cccccc&secret-access-key=dddddd".to_string()),
    );
    assert_eq!(
        secure_text(&combined),
        "IMPORT INTO `t` FROM 's3://bucket/prefix?access-key=xxxxxx&secret-access-key=xxxxxx' \
         WITH cloud_storage_uri='s3://bucket/prefix?access-key=xxxxxx&secret-access-key=xxxxxx'"
    );
    let _ = string("");
}

// go-parity-gap: TestImportIntoFromSelectInvalidStmt pins PARSER grammar
// validation ("Cannot use user variable(1) in IMPORT INTO FROM SELECT
// statement", "... user variable(b) ...", "Cannot use SET clause in IMPORT
// INTO FROM SELECT statement."). Those checks run inside pkg/parser parsing
// actions; this AST crate cannot parse, so they live behind tidb-parser's
// grammar instead.
#[test]
#[ignore = "go-parity-gap: FROM-SELECT import validation runs in tidb-parser grammar actions"]
fn import_into_from_select_invalid_stmt() {}
