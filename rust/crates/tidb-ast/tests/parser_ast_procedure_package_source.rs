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

//! Ports of `pkg/parser/ast/procedure_test.go` (origin/master).
//!
//! The parse-driven presence tests stay behind explicit gaps; the visitor
//! coverage and the full `TestProcedureRestore` table are transcreated over
//! hand-built [`tidb_ast::CreateProcedureStmt`] trees whose restored text
//! must equal the Go expectations byte-for-byte.

use tidb_ast::{
    AdminStmt, BinaryOp, ColumnType, ColumnTypeArg, CreateProcedureStmt, DdlStmt, Expr,
    NodeBox, ProcedureDeclaration, ProcedureHandlerAction, ProcedureHandlerCondition,
    ProcedureParameter, ProcedureParameterMode, ProcedureStatement, ProcedureWhen,
    QueryStmt, SelectField, SelectStatementKind, SelectStmt, SessionStmt, SetStmt,
    SetVariableValue, Stmt, SystemVariableAssignment, SystemVariableScope, Visitable, Visitor,
};

fn column(path: &[&str]) -> Expr {
    Expr::Column(path.iter().map(|name| name.to_string()).collect())
}

fn string(value: &str) -> Expr {
    Expr::String(value.to_string())
}

fn int(value: &str) -> Expr {
    Expr::Int(value.to_string())
}

fn binary(op: BinaryOp, l: Expr, r: Expr) -> Expr {
    Expr::Binary(op, Box::new(l), Box::new(r))
}

fn ty(name: &str, width: &str) -> ColumnType {
    ColumnType {
        name: name.to_string(),
        args: vec![ColumnTypeArg::text(width)],
        unsigned: false,
        zerofill: false,
        binary: false,
        charset: None,
    }
}

/// Builds `SELECT <expr>` as an ordinary nested SQL statement.
fn select_expr(expr: Expr) -> ProcedureStatement {
    let mut select = empty_select();
    select.fields.push(SelectField::Expr { expr, alias: None });
    ProcedureStatement::Sql(Box::new(Stmt::Query(NodeBox::new(
        QueryStmt::Select(Box::new(select)),
    ))))
}

fn select_star_from(table: &[&str]) -> ProcedureStatement {
    let mut select = empty_select();
    select.fields.push(SelectField::Wildcard(Vec::new()));
    select.from = Some(tidb_ast::Join {
        left: tidb_ast::JoinNode::Table(tidb_ast::TableRef {
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
    });
    ProcedureStatement::Sql(Box::new(Stmt::Query(NodeBox::new(
        QueryStmt::Select(Box::new(select)),
    ))))
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

fn parameter(mode: ProcedureParameterMode, name: &str, ty: ColumnType) -> ProcedureParameter {
    ProcedureParameter { mode, name: name.to_string(), ty }
}

/// Wraps a CREATE PROCEDURE into a restorable root.
fn create_procedure(parameters: Vec<ProcedureParameter>, body: ProcedureStatement) -> Stmt {
    Stmt::Ddl(NodeBox::new(DdlStmt::CreateProcedure(Box::new(
        CreateProcedureStmt {
            if_not_exists: false,
            name: vec!["proc_2".to_string()],
            parameters,
            body: NodeBox::new(body),
        },
    ))))
}

fn insert_into_t1_111() -> ProcedureStatement {
    let stmt = Stmt::Dml(NodeBox::new(tidb_ast::DmlStmt::Insert(Box::new(
        tidb_ast::InsertStmt {
            hints: Vec::new(),
            priority: Default::default(),
            ignore: false,
            table: vec!["t1".to_string()],
            partitions: Vec::new(),
            columns: Vec::new(),
            columns_specified: false,
            set_columns: Vec::new(),
            rows: vec![vec![int("111")]],
            source: None,
            source_parenthesized: false,
            on_duplicate: Vec::new(),
            row_alias: None,
            column_aliases: Vec::new(),
            returning: Default::default(),
            set_syntax: false,
            replace: false,
        },
    ))));
    ProcedureStatement::Sql(Box::new(stmt))
}

/// Go's `visitor{}`-style traversal over every node family below the
/// procedure statements (parameter, declaration, block, drop).
#[derive(Default)]
struct ProcCounter {
    entered: usize,
    left: usize,
    stops_first_enter: bool,
}

impl Visitor for ProcCounter {
    fn enter(&mut self, _node: &mut dyn std::any::Any) -> bool {
        self.entered += 1;
        self.stops_first_enter
    }

    fn leave(&mut self, _node: &mut dyn std::any::Any) -> bool {
        self.left += 1;
        true
    }
}

/// `pkg/parser/ast/procedure_test.go::TestProcedureVisitorCover`.
///
/// Go accepts StoreParameter/ProcedureDecl plus ProcedureBlock/
/// ProcedureInfo/DropProcedureStmt under both no-op visitors. The
/// corresponding Rust families are walked here twice: once fully and once
/// with children skipped on Enter (leave still fires), pinning that both
/// traversals complete and leave balanced.
#[test]
fn procedure_visitor_cover() {
    // Parameter + declaration (StoreParameter / ProcedureDecl).
    let param = parameter(ProcedureParameterMode::In, "id", ty("BIGINT", "20"));
    let mut param = param;
    assert!(param.accept(&mut ProcCounter::default()));
    let mut declaration = ProcedureDeclaration::Variable {
        names: vec!["s".to_string()],
        ty: ty("VARCHAR", "100"),
        default: None,
    };
    assert!(declaration.accept(&mut ProcCounter::default()));

    // Block (ProcedureBlock) with a nested variable declaration so the
    // deep-descent case carries children.
    let mut block = ProcedureStatement::Block {
        declarations: vec![declaration],
        statements: Vec::new(),
    };
    let mut full = ProcCounter::default();
    assert!(block.accept(&mut full));
    assert!(full.entered > 1);
    assert_eq!(full.entered, full.left);

    // Skip-all traversal still leaves each entered node (visitor1 parity).
    let mut block_again = ProcedureStatement::OpenCursor("c".to_string());
    let mut skipper = ProcCounter {
        stops_first_enter: true,
        ..ProcCounter::default()
    };
    assert!(block_again.accept(&mut skipper));
    assert!(skipper.left >= 1);

    // CreateProcedureStmt wraps the whole graph (ProcedureInfo parity).
    let mut root = create_procedure(
        vec![parameter(ProcedureParameterMode::In, "id", ty("INT", "11"))],
        ProcedureStatement::Block {
            declarations: Vec::new(),
            statements: Vec::new(),
        },
    );
    let mut full_root = ProcCounter::default();
    assert!(root.accept(&mut full_root));
    assert!(full_root.entered > 1);

    // DropProcedureStmt family.
    let mut drop = Stmt::Ddl(NodeBox::new(DdlStmt::DropProcedure(Box::new(
        tidb_ast::DropProcedureStmt {
            if_exists: false,
            name: vec!["proc_2".to_string()],
        },
    ))));
    assert!(drop.accept(&mut ProcCounter::default()));
}

/// `pkg/parser/ast/procedure_test.go::TestShowCreateProcedure`.
#[test]
fn show_create_procedure() {
    // SHOW CREATE PROCEDURE proc_2 rides on the typed SHOW CREATE payload.
    let show = Stmt::Admin(NodeBox::new(AdminStmt::ShowCreate {
        kind: tidb_ast::ShowCreateKind::Procedure,
        if_not_exists: false,
        name: vec!["proc_2".to_string()],
    }));
    assert_eq!(show.restore(), "SHOW CREATE PROCEDURE `proc_2`");

    // DROP PROCEDURE proc_2 has its own dedicated DDL statement type.
    let drop = Stmt::Ddl(NodeBox::new(DdlStmt::DropProcedure(Box::new(
        tidb_ast::DropProcedureStmt {
            if_exists: false,
            name: vec!["proc_2".to_string()],
        },
    ))));
    assert_eq!(drop.restore(), "DROP PROCEDURE `proc_2`");
}

/// `pkg/parser/ast/procedure_test.go::TestProcedureVisitor`.
#[test]
#[ignore = "go-parity-gap: parse-driven visitor scripts require tidb-parser"]
fn procedure_visitor() {}

/// `pkg/parser/ast/procedure_test.go::TestProcedure`.
#[test]
#[ignore = "go-parity-gap: TestProcedure only asserts parses succeed (tidb-parser grammar)"]
fn procedure_parse_presence() {}

/// `pkg/parser/ast/procedure_test.go::TestProcedureRestore`.
///
/// Every Go row's extracted `ProcedureInfo` state is rebuilt here; source
/// spellings collapse exactly where Go's parser collapses (`in id bigint`
/// keeps written mode/case, `SET id = id + 1` becomes
/// ``SET @@SESSION.`id`=`id`+1``, cursor and loop labels upper-case their
/// keywords, `sqlstate` normalizes to `SQLSTATE`, `1211, SQLSTATE 'xdw'`
/// keeps mixed condition families).
#[test]
fn procedure_restore() {
    let declared_s = || ProcedureDeclaration::Variable {
        names: vec!["s".to_string()],
        ty: ty("VARCHAR", "100"),
        default: Some(Box::new(func_from_unixtime())),
    };
    let from_unixtime_body = |statements: Vec<ProcedureStatement>| {
        ProcedureStatement::Block {
            declarations: vec![declared_s()],
            statements,
        }
    };

    let simple_select_two =
        || select_expr(int("2"));

    let cases: [(Stmt, &str); 19] = [
        // Parameters + declare + four statements.
        (
            create_procedure(
                vec![
                    parameter(ProcedureParameterMode::In, "id", ty("BIGINT", "20")),
                    parameter(ProcedureParameterMode::In, "id2", ty("VARCHAR", "100")),
                    parameter(
                        ProcedureParameterMode::In,
                        "id3",
                        decimal_type(30, 2),
                    ),
                ],
                from_unixtime_body(vec![
                    select_expr(column(&["s"])),
                    select_star_from(&["t1"]),
                    select_star_from(&["t2"]),
                    insert_into_t1_111(),
                ]),
            ),
            "CREATE PROCEDURE `proc_2`( IN `id` BIGINT(20), IN `id2` VARCHAR(100), IN `id3` DECIMAL(30,2)) BEGIN DECLARE `s` VARCHAR(100) DEFAULT FROM_UNIXTIME(1447430881);SELECT `s`;SELECT * FROM `t1`;SELECT * FROM `t2`;INSERT INTO `t1` VALUES (111); END",
        ),
        (
            create_procedure(
                Vec::new(),
                ProcedureStatement::Block {
                    declarations: Vec::new(),
                    statements: vec![
                        select_star_from(&["t1"]),
                        if_stmt(
                            vec![
                                (greater_i_one(), vec![simple_select_two()]),
                                (eq_i_three(), vec![select_expr(int("4"))]),
                            ],
                            vec![select_expr(int("5"))],
                        ),
                    ],
                },
            ),
            "CREATE PROCEDURE `proc_2`() BEGIN SELECT * FROM `t1`;IF `i`>1 THEN SELECT 2;ELSEIF `i`=3 THEN SELECT 4;ELSE SELECT 5;END IF; END",
        ),
        (
            create_procedure(
                Vec::new(),
                ProcedureStatement::Block {
                    declarations: Vec::new(),
                    statements: vec![
                        select_star_from(&["t1"]),
                        if_stmt(
                            vec![
                                (greater_i_one(), vec![simple_select_two()]),
                                (eq_i_three(), vec![select_expr(int("4"))]),
                            ],
                            Vec::new(),
                        ),
                    ],
                },
            ),
            "CREATE PROCEDURE `proc_2`() BEGIN SELECT * FROM `t1`;IF `i`>1 THEN SELECT 2;ELSEIF `i`=3 THEN SELECT 4;END IF; END",
        ),
        (
            create_procedure(
                Vec::new(),
                ProcedureStatement::Block {
                    declarations: Vec::new(),
                    statements: vec![
                        select_star_from(&["t1"]),
                        if_stmt(
                            vec![(greater_i_one(), vec![simple_select_two()])],
                            Vec::new(),
                        ),
                    ],
                },
            ),
            "CREATE PROCEDURE `proc_2`() BEGIN SELECT * FROM `t1`;IF `i`>1 THEN SELECT 2;END IF; END",
        ),
        (
            create_procedure(
                Vec::new(),
                ProcedureStatement::Block {
                    declarations: Vec::new(),
                    statements: vec![
                        select_star_from(&["t1"]),
                        if_stmt(
                            vec![(greater_i_one(), vec![simple_select_two()])],
                            vec![select_expr(int("5"))],
                        ),
                    ],
                },
            ),
            "CREATE PROCEDURE `proc_2`() BEGIN SELECT * FROM `t1`;IF `i`>1 THEN SELECT 2;ELSE SELECT 5;END IF; END",
        ),
        (
            create_procedure(
                vec![parameter(ProcedureParameterMode::In, "id", ty("INT", "11"))],
                while_set_and_select(binary(BinaryOp::Lt, column(&["id"]), int("10"))),
            ),
            "CREATE PROCEDURE `proc_2`( IN `id` INT(11)) BEGIN WHILE `id`<10 DO SET @@SESSION.`id`=`id`+1;SELECT 1;END WHILE; END",
        ),
        (
            create_procedure(
                Vec::new(),
                cursor_block(true),
            ),
            "CREATE PROCEDURE `proc_2`() BEGIN DECLARE `a` INT(11);DECLARE TEST1 CURSOR FOR SELECT 1;SELECT 1;OPEN TEST1;FETCH TEST1 INTO A;CLOSE TEST1; END",
        ),
        (
            create_procedure(
                Vec::new(),
                handler_block(
                    ProcedureHandlerAction::Exit,
                    vec![
                        ProcedureHandlerCondition::SqlWarning,
                        ProcedureHandlerCondition::NotFound,
                        ProcedureHandlerCondition::SqlException,
                    ],
                ),
            ),
            "CREATE PROCEDURE `proc_2`() BEGIN DECLARE `a` INT(11);DECLARE EXIT HANDLER FOR SQLWARNING, NOT FOUND, SQLEXCEPTION SELECT 1; END",
        ),
        (
            create_procedure(
                vec![
                    parameter(ProcedureParameterMode::InOut, "id", ty("BIGINT", "20")),
                    parameter(ProcedureParameterMode::Out, "id1", ty("BIGINT", "20")),
                ],
                ProcedureStatement::Block {
                    declarations: vec![
                        int_declaration(),
                        continue_handler(vec![
                            ProcedureHandlerCondition::ErrorCode(1211),
                            ProcedureHandlerCondition::SqlState("xdw".to_string()),
                        ]),
                    ],
                    statements: Vec::new(),
                },
            ),
            "CREATE PROCEDURE `proc_2`( INOUT `id` BIGINT(20), OUT `id1` BIGINT(20)) BEGIN DECLARE `a` INT(11);DECLARE CONTINUE HANDLER FOR 1211, SQLSTATE 'xdw' SELECT 1; END",
        ),
        (
            create_procedure(
                Vec::new(),
                labeled_while_block(),
            ),
            "CREATE PROCEDURE `proc_2`() BEGIN DECLARE `a` INT(11);DECLARE CONTINUE HANDLER FOR SQLSTATE 'ssss' WHILE `id`<10 DO SET @@SESSION.`id`=`id`+1;SELECT 1;END WHILE; END",
        ),
        (
            create_procedure(
                Vec::new(),
                simple_case(vec![(string("1980-10-01"), vec![select_expr(int("1"))])], Vec::new()),
            ),
            "CREATE PROCEDURE `proc_2`() CASE NOW() WHEN _UTF8MB4'1980-10-01' THEN SELECT 1; END CASE",
        ),
        (
            create_procedure(
                Vec::new(),
                simple_case(
                    vec![
                        (string("1980-10-01"), vec![select_expr(int("1"))]),
                        (string("1980-10-02"), vec![select_expr(int("2"))]),
                    ],
                    Vec::new(),
                ),
            ),
            "CREATE PROCEDURE `proc_2`() CASE NOW() WHEN _UTF8MB4'1980-10-01' THEN SELECT 1;WHEN _UTF8MB4'1980-10-02' THEN SELECT 2; END CASE",
        ),
        (
            create_procedure(
                Vec::new(),
                simple_case(
                    vec![
                        (string("1980-10-01"), vec![select_expr(int("1"))]),
                        (string("1980-10-02"), vec![select_expr(int("2"))]),
                    ],
                    vec![select_expr(int("3"))],
                ),
            ),
            "CREATE PROCEDURE `proc_2`() CASE NOW() WHEN _UTF8MB4'1980-10-01' THEN SELECT 1;WHEN _UTF8MB4'1980-10-02' THEN SELECT 2; ELSE SELECT 3; END CASE",
        ),
        (
            create_procedure(
                Vec::new(),
                searched_case(
                    vec![(eq_id_one(), vec![select_expr(int("1"))])],
                    Vec::new(),
                ),
            ),
            "CREATE PROCEDURE `proc_2`() CASE WHEN `id`=1 THEN SELECT 1; END CASE",
        ),
        (
            create_procedure(
                Vec::new(),
                searched_case(
                    vec![
                        (eq_id_one(), vec![select_expr(int("1"))]),
                        (binary(BinaryOp::Eq, column(&["id"]), int("2")), vec![select_expr(int("2"))]),
                    ],
                    Vec::new(),
                ),
            ),
            "CREATE PROCEDURE `proc_2`() CASE WHEN `id`=1 THEN SELECT 1;WHEN `id`=2 THEN SELECT 2; END CASE",
        ),
        (
            create_procedure(
                Vec::new(),
                searched_case(
                    vec![
                        (eq_id_one(), vec![select_expr(int("1"))]),
                        (binary(BinaryOp::Eq, column(&["id"]), int("2")), vec![select_expr(int("2"))]),
                    ],
                    vec![select_expr(int("3"))],
                ),
            ),
            "CREATE PROCEDURE `proc_2`() CASE WHEN `id`=1 THEN SELECT 1;WHEN `id`=2 THEN SELECT 2; ELSE SELECT 3; END CASE",
        ),
        (
            create_procedure(
                Vec::new(),
                labeled_begin_exit_handler(),
            ),
            "CREATE PROCEDURE `proc_2`() `labelname`: BEGIN DECLARE `a` INT(11);DECLARE CONTINUE HANDLER FOR SQLWARNING, NOT FOUND, SQLEXCEPTION SELECT 1; END `labelname`",
        ),
        (
            create_procedure(
                Vec::new(),
                labeled_nested_while_end_label(),
            ),
            "CREATE PROCEDURE `proc_2`() BEGIN `labelname`: WHILE `id`<10 DO SET @@SESSION.`id`=`id`+1;SELECT 1;END WHILE `labelname`; END",
        ),
        (
            create_procedure(
                vec![parameter(ProcedureParameterMode::In, "id", ty("INT", "11"))],
                labeled_repeat(),
            ),
            "CREATE PROCEDURE `proc_2`( IN `id` INT(11)) BEGIN `labelname`: REPEAT SET @@SESSION.`id`=`id`+1;SELECT 1;UNTIL `id`<10 END REPEAT `labelname`; END",
        ),
    ];
    for (stmt, want) in cases {
        assert_eq!(stmt.restore(), want);
    }
}

fn func_from_unixtime() -> Expr {
    Expr::Func {
        name: "FROM_UNIXTIME".to_string(),
        args: vec![int("1447430881")],
        origin_position: 0,
    }
}

fn greater_i_one() -> Expr {
    binary(BinaryOp::Gt, column(&["i"]), int("1"))
}

fn eq_i_three() -> Expr {
    binary(BinaryOp::Eq, column(&["i"]), int("3"))
}

fn eq_id_one() -> Expr {
    binary(BinaryOp::Eq, column(&["id"]), int("1"))
}

fn decimal_type(flen: u32, scale: u32) -> ColumnType {
    ColumnType {
        name: "DECIMAL".to_string(),
        args: vec![
            ColumnTypeArg::text(flen.to_string()),
            ColumnTypeArg::text(scale.to_string()),
        ],
        unsigned: false,
        zerofill: false,
        binary: false,
        charset: None,
    }
}

/// `SET @@SESSION.\`id\`=\`id\`+1` (Go keeps a session-scoped write).
fn set_session_id_plus_one() -> ProcedureStatement {
    let stmt = Stmt::Session(NodeBox::new(SessionStmt::Set(Box::new(SetStmt {
        assignments: vec![SystemVariableAssignment {
            scope: SystemVariableScope::Session,
            name: "id".to_string(),
            value: SetVariableValue::Expr(binary(
                BinaryOp::Plus,
                column(&["id"]),
                int("1"),
            )),
        }],
    }))));
    ProcedureStatement::Sql(Box::new(stmt))
}

fn if_stmt(
    branches: Vec<(Expr, Vec<ProcedureStatement>)>,
    else_statements: Vec<ProcedureStatement>,
) -> ProcedureStatement {
    ProcedureStatement::If {
        branches,
        else_statements,
    }
}

fn while_loop(condition: Expr) -> ProcedureStatement {
    ProcedureStatement::While {
        condition,
        body: vec![set_session_id_plus_one(), select_expr(int("1"))],
    }
}

fn while_set_and_select(condition: Expr) -> ProcedureStatement {
    ProcedureStatement::Block {
        declarations: Vec::new(),
        statements: vec![while_loop(condition)],
    }
}

fn cursor_block(with_extras: bool) -> ProcedureStatement {
    let declarations = vec![
        int_declaration(),
        ProcedureDeclaration::Cursor {
            name: "test1".to_string(),
            query: Box::new(select_int_stmt()),
        },
    ];
    let statements = match with_extras {
        true => vec![
            select_expr(int("1")),
            ProcedureStatement::OpenCursor("test1".to_string()),
            ProcedureStatement::FetchInto {
                cursor: "test1".to_string(),
                variables: vec!["a".to_string()],
            },
            ProcedureStatement::CloseCursor("test1".to_string()),
        ],
        false => Vec::new(),
    };
    ProcedureStatement::Block {
        declarations,
        statements,
    }
}

fn select_int_stmt() -> Stmt {
    let mut select = empty_select();
    select.fields.push(SelectField::Expr {
        expr: int("1"),
        alias: None,
    });
    Stmt::Query(NodeBox::new(QueryStmt::Select(Box::new(select))))
}

fn int_declaration() -> ProcedureDeclaration {
    ProcedureDeclaration::Variable {
        names: vec!["a".to_string()],
        ty: ty("INT", "11"),
        default: None,
    }
}

fn continue_handler(conditions: Vec<ProcedureHandlerCondition>) -> ProcedureDeclaration {
    ProcedureDeclaration::Handler {
        action: ProcedureHandlerAction::Continue,
        conditions,
        body: Box::new(select_expr(int("1"))),
    }
}

fn handler_block(
    action: ProcedureHandlerAction,
    conditions: Vec<ProcedureHandlerCondition>,
) -> ProcedureStatement {
    ProcedureStatement::Block {
        declarations: vec![
            int_declaration(),
            ProcedureDeclaration::Handler {
                action,
                conditions,
                body: Box::new(select_expr(int("1"))),
            },
        ],
        statements: Vec::new(),
    }
}

/// Row 10: the WHILE loop IS the SQLSTATE 'ssss' handler's own body.
fn labeled_while_block() -> ProcedureStatement {
    ProcedureStatement::Block {
        declarations: vec![
            int_declaration(),
            ProcedureDeclaration::Handler {
                action: ProcedureHandlerAction::Continue,
                conditions: vec![ProcedureHandlerCondition::SqlState("ssss".to_string())],
                body: Box::new(while_loop(binary(
                    BinaryOp::Lt,
                    column(&["id"]),
                    int("10"),
                ))),
            },
        ],
        statements: Vec::new(),
    }
}

fn simple_case(
    when: Vec<(Expr, Vec<ProcedureStatement>)>,
    else_statements: Vec<ProcedureStatement>,
) -> ProcedureStatement {
    ProcedureStatement::SimpleCase {
        value: func_now(),
        when: when
            .into_iter()
            .map(|(expression, statements)| ProcedureWhen { expression, statements })
            .collect(),
        else_statements,
    }
}

fn searched_case(
    when: Vec<(Expr, Vec<ProcedureStatement>)>,
    else_statements: Vec<ProcedureStatement>,
) -> ProcedureStatement {
    ProcedureStatement::SearchedCase {
        when: when
            .into_iter()
            .map(|(expression, statements)| ProcedureWhen { expression, statements })
            .collect(),
        else_statements,
    }
}

fn func_now() -> Expr {
    Expr::Func {
        name: "NOW".to_string(),
        args: Vec::new(),
        origin_position: 0,
    }
}

fn labeled_begin_exit_handler() -> ProcedureStatement {
    ProcedureStatement::Label {
        name: "labelname".to_string(),
        statement: Box::new(ProcedureStatement::Block {
            declarations: vec![
                int_declaration(),
                ProcedureDeclaration::Handler {
                    action: ProcedureHandlerAction::Continue,
                    conditions: vec![
                        ProcedureHandlerCondition::SqlWarning,
                        ProcedureHandlerCondition::NotFound,
                        ProcedureHandlerCondition::SqlException,
                    ],
                    body: Box::new(select_expr(int("1"))),
                },
            ],
            statements: Vec::new(),
        }),
    }
}

fn labeled_nested_while_end_label() -> ProcedureStatement {
    ProcedureStatement::Block {
        declarations: Vec::new(),
        statements: vec![ProcedureStatement::Label {
            name: "labelname".to_string(),
            statement: Box::new(ProcedureStatement::While {
                condition: binary(BinaryOp::Lt, column(&["id"]), int("10")),
                body: vec![set_session_id_plus_one(), select_expr(int("1"))],
            }),
        }],
    }
}

fn labeled_repeat() -> ProcedureStatement {
    ProcedureStatement::Block {
        declarations: Vec::new(),
        statements: vec![ProcedureStatement::Label {
            name: "labelname".to_string(),
            statement: Box::new(ProcedureStatement::Repeat {
                body: vec![set_session_id_plus_one(), select_expr(int("1"))],
                condition: binary(BinaryOp::Lt, column(&["id"]), int("10")),
            }),
        }],
    }
}
