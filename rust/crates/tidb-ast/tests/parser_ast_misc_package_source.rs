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

//! Ports of `pkg/parser/ast/misc_test.go` (origin/master).
//!
//! Statement-level visitor covers built from parsed multi-statement scripts
//! are transcreated as direct-construction walks over the same node
//! families; the remaining rows hand-build the extracted AST states Go
//! asserts on (hints, secure text, restore text, URL redaction).

use tidb_ast::{
    AdminStmt, AnalyzeTableStmt, BeginStmt, BinaryOp, BrieKind, BrieOption,
    BrieOptionValue, BrieStmt, CompactReplicaKind, CompletionType, DdlStmt, DropStatsStmt,
    Expr, FlushStmt, FlushTarget, GrantLevel,
    GrantStmt, Hint, HintKind, HintTable, JoinNode, KillStmt, KillTarget,
    LeadingElement, NodeBox, PlanReplayerStmt, PlanReplayerTarget,
    QueryWatchOption, QueryWatchTextOption, ResourceGroupRunawayAction, RunawayWatchType,
    SelectStatementKind, SelectStmt, ServerControlStmt, SessionStmt, SetPasswordStmt, SetStmt,
    SetVariableValue, Stmt, SystemVariableAssignment, SystemVariableScope, TableRef,
    TrafficCaptureOption, TrafficReplayOption, TrafficStmt, TransactionMode, UserSpec,
    Visitable, Visitor,
};

fn column(path: &[&str]) -> Expr {
    Expr::Column(path.iter().map(|name| name.to_string()).collect())
}

fn int(value: &str) -> Expr {
    Expr::Int(value.to_string())
}

fn string(value: &str) -> Expr {
    Expr::String(value.to_string())
}

fn admin(stmt: AdminStmt) -> Stmt {
    Stmt::Admin(NodeBox::new(stmt))
}

fn ddl(stmt: DdlStmt) -> Stmt {
    Stmt::Ddl(NodeBox::new(stmt))
}

fn session(stmt: SessionStmt) -> Stmt {
    Stmt::Session(NodeBox::new(stmt))
}

/// Go's `visitor{}`: descend into every child.
#[derive(Default)]
struct FullWalk {
    nodes: usize,
}

impl Visitor for FullWalk {
    fn enter(&mut self, _node: &mut dyn std::any::Any) -> bool {
        self.nodes += 1;
        false
    }

    fn leave(&mut self, _node: &mut dyn std::any::Any) -> bool {
        true
    }
}

/// Go's `visitor1{}`: skip children on Enter but still Leave every node.
#[derive(Default)]
struct SkipAll {
    entered: usize,
    left: usize,
}

impl Visitor for SkipAll {
    fn enter(&mut self, _node: &mut dyn std::any::Any) -> bool {
        self.entered += 1;
        true
    }

    fn leave(&mut self, _node: &mut dyn std::any::Any) -> bool {
        self.left += 1;
        true
    }
}

/// Walks both visitor styles over one statement and returns the total full
/// walk node count.
fn cover(stmt: &mut Stmt) -> usize {
    let mut full = FullWalk::default();
    assert!(stmt.accept(&mut full));
    let mut skip = SkipAll::default();
    assert!(stmt.accept(&mut skip));
    // Skip-all enters once per top-level node type it meets; leave runs the
    // same number of times regardless.
    assert_eq!(skip.entered, skip.left);
    let _ = 1;
    full.nodes
}

/// A tiny single-table select for wrapper statements.
fn star_select() -> SelectStmt {
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

/// `pkg/parser/ast/misc_test.go::TestMiscVisitorCover`.
///
/// Go lists twenty-five statement/value nodes and accepts each under both
/// no-op visitors. This port builds one representative instance per Rust
/// family (Go's internal-only helpers — `PrivElem`,
/// `VariableAssignment`, and the empty-`AdminStmt` zero value — have no
/// standalone node here; their families are covered through the enclosing
/// assignments/`AdminStmt` variants below), asserting each walk completes
/// for a full traversal AND for the skip-children traversal.
#[test]
fn misc_visitor_cover() {
    let mut stmts = [
        // &ast.AdminStmt{} → a representative Admin payload.
        admin(AdminStmt::ShowBdrRole),
        // &ast.AlterUserStmt{}
        ddl(DdlStmt::AlterUser(Box::new(tidy_alter_user()))),
        // &ast.BeginStmt{}
        session(SessionStmt::Begin(Box::new(BeginStmt {
            mode: TransactionMode::Default,
            read_only: false,
            causal_consistency_only: false,
            as_of: None,
        }))),
        // &ast.BinlogStmt{}
        admin(AdminStmt::Binlog(Box::new(tidy_binlog()))),
        // &ast.CommitStmt{}
        session(SessionStmt::Commit(CompletionType::Default)),
        // &ast.CompactTableStmt{Table: &TableName{}}
        ddl(DdlStmt::AlterTable(Box::new(compact_table("t")))),
        // &ast.CreateUserStmt{}
        ddl(DdlStmt::CreateUser {
            if_not_exists: false,
            users: Vec::new(),
            tls_options: Vec::new(),
            resource_options: Vec::new(),
            password_options: Vec::new(),
            comment_or_attribute: None,
            resource_group: None,
        }),
        // &ast.DeallocateStmt{}
        session(SessionStmt::Deallocate("stmt".to_string())),
        // &ast.DoStmt{}
        admin(AdminStmt::Do(vec![int("42")])),
        // &ast.ExecuteStmt{UsingVars}
        session(SessionStmt::Execute {
            name: "stmt".to_string(),
            using: vec![int("42")],
        }),
        // &ast.ExplainStmt{Stmt: &ShowStmt{}}
        admin(AdminStmt::Explain(Box::new(tidb_ast::ExplainStmt {
            analyze: false,
            format: "row".to_string(),
            target: tidb_ast::ExplainTarget::Statement(Box::new(
                admin(AdminStmt::ShowBdrRole),
            )),
        }))),
        // &ast.GrantStmt{}
        admin(AdminStmt::Grant(Box::new(GrantStmt {
            privileges: Vec::new(),
            object_type: None,
            level: GrantLevel::Global,
            users: Vec::new(),
            tls_options: Vec::new(),
            with_grant: false,
        }))),
        // &ast.PrepareStmt{SQLVar}
        session(SessionStmt::Prepare {
            name: "stmt".to_string(),
            source: tidb_ast::PrepareSource::Var("v".to_string()),
        }),
        // &ast.RollbackStmt{}
        session(SessionStmt::Rollback {
            savepoint: None,
            completion: CompletionType::Default,
        }),
        // &ast.SetPwdStmt{}
        session(SessionStmt::SetPassword(Box::new(SetPasswordStmt {
            user: None,
            password: String::new(),
            retain_current_password: false,
        }))),
        // &ast.SetStmt{Variables:[{Value}]}
        session(SessionStmt::Set(Box::new(SetStmt {
            assignments: vec![SystemVariableAssignment {
                scope: SystemVariableScope::Session,
                name: String::new(),
                value: SetVariableValue::Expr(int("42")),
            }],
        }))),
        // &ast.UseStmt{}
        session(SessionStmt::Use("db".to_string())),
        // &ast.AnalyzeTableStmt{TableNames}
        admin(AdminStmt::AnalyzeTable(Box::new(AnalyzeTableStmt {
            tables: vec![vec!["t".to_string()]],
            partitions: Vec::new(),
            no_write_to_binlog: false,
            target: tidb_ast::AnalyzeTarget::Default,
            options: Vec::new(),
        }))),
        // &ast.FlushStmt{}
        admin(AdminStmt::Flush(Box::new(FlushStmt {
            no_write_to_binlog: false,
            target: FlushTarget::Status,
        }))),
        // &ast.VariableAssignment{Value} → covered by the SET assignment
        // above; Do's expression keeps an expression-level walk covered.
        admin(AdminStmt::Do(vec![column(&["x"])])),
        // &ast.KillStmt{}
        admin(AdminStmt::Kill(Box::new(KillStmt {
            query: false,
            tidb_extension: false,
            target: KillTarget::ConnectionId(1),
        }))),
        // &ast.DropStatsStmt{Tables}
        admin(AdminStmt::DropStats(Box::new(DropStatsStmt {
            tables: vec![vec!["t".to_string()]],
            global: false,
            partitions: Vec::new(),
        }))),
        // &ast.ShutdownStmt{}
        admin(AdminStmt::ServerControl(Box::new(ServerControlStmt::Shutdown))),
    ];

    for stmt in stmts.iter_mut() {
        let count = cover(stmt);
        assert!(count > 0, "every covered statement must walk its root");
    }
}

fn tidy_alter_user() -> tidb_ast::AlterUserStmt {
    tidb_ast::AlterUserStmt {
        if_exists: false,
        users: Vec::new(),
        user_function_auth: None,
        user_function_dual_password: None,
        tls_options: Vec::new(),
        resource_options: Vec::new(),
        password_options: Vec::new(),
        comment_or_attribute: None,
        resource_group: None,
    }
}

fn tidy_binlog() -> tidb_ast::BinlogStmt {
    tidb_ast::BinlogStmt { value: String::new() }
}

/// Builds `ALTER TABLE <name> COMPACT` (Go's separate `CompactTableStmt`).
pub fn compact_table(name: &str) -> tidb_ast::AlterTableStmt {
    tidb_ast::AlterTableStmt {
        name: vec![name.to_string()],
        actions: vec![tidb_ast::AlterTableAction::Compact {
            partitions: Vec::new(),
            replica_kind: CompactReplicaKind::All,
        }],
    }
}

/// `pkg/parser/ast/misc_test.go::TestDDLVisitorCoverMisc` /
/// `TestDMLVistorCover`.
///
/// Both parse multi-statement SQL scripts and accept every resulting
/// statement. Traversal itself is pinned per-family by
/// `misc_visitor_cover`/`functions_visitor_cover`; the parse step belongs
/// to tidb-parser, so the script-driven shapes stay behind an ignored gap
/// here rather than being approximated.
#[test]
#[ignore = "go-parity-gap: parse-driven visitor scripts require tidb-parser"]
fn ddl_visitor_cover_misc() {}

#[test]
#[ignore = "go-parity-gap: parse-driven visitor scripts require tidb-parser"]
fn dml_vistor_cover() {}

/// `pkg/parser/ast/misc_test.go::TestSensitiveStatement`.
///
/// Go pins interface membership via type assertion; this crate exposes the
/// same classification as [`Stmt::is_sensitive`]. Positives are exactly
/// Go's list (SetPwd/CreateUser/AlterUser/Grant); negatives include Go's
/// RevokeStmt/DropUserStmt/non-account DDL set.
#[test]
fn sensitive_statement() {
    let positive = [
        session(SessionStmt::SetPassword(Box::new(SetPasswordStmt {
            user: None,
            password: String::new(),
            retain_current_password: false,
        }))),
        ddl(DdlStmt::CreateUser {
            if_not_exists: false,
            users: Vec::new(),
            tls_options: Vec::new(),
            resource_options: Vec::new(),
            password_options: Vec::new(),
            comment_or_attribute: None,
            resource_group: None,
        }),
        ddl(DdlStmt::AlterUser(Box::new(tidy_alter_user()))),
        admin(AdminStmt::Grant(Box::new(GrantStmt {
            privileges: Vec::new(),
            object_type: None,
            level: GrantLevel::Global,
            users: Vec::new(),
            tls_options: Vec::new(),
            with_grant: false,
        }))),
    ];
    for stmt in &positive {
        assert!(stmt.is_sensitive(), "{stmt:?} must be sensitive");
    }

    let negative = [
        ddl(DdlStmt::DropUser {
            is_role: false,
            if_exists: false,
            users: Vec::new(),
        }),
        admin(AdminStmt::Revoke(Box::new(RevokedGrant::revoke()))),
        ddl(DdlStmt::AlterTable(Box::new(alter_table_noop()))),
        ddl(DdlStmt::CreateDatabase {
            if_not_exists: false,
            name: "d".to_string(),
            options: Vec::new(),
        }),
        ddl(DdlStmt::CreateIndex(Box::new(CreateIndexStub::empty()))),
        ddl(DdlStmt::CreateTable(Box::new(CreateTableStub::empty()))),
        ddl(DdlStmt::DropDatabase {
            if_exists: false,
            name: "d".to_string(),
        }),
        ddl(DdlStmt::DropIndex(Box::new(DropIndexStub::empty()))),
        ddl(DdlStmt::DropTable(Box::new(DropTableStub::empty()))),
        ddl(DdlStmt::RenameTable(Box::new(RenameStub::empty()))),
        ddl(DdlStmt::TruncateTable(Box::new(vec!["t".to_string()]))),
    ];
    for stmt in &negative {
        assert!(!stmt.is_sensitive(), "{stmt:?} must NOT be sensitive");
    }
}

struct RevokedGrant;

impl RevokedGrant {
    fn revoke() -> tidb_ast::RevokeStmt {
        tidb_ast::RevokeStmt {
            privileges: Vec::new(),
            object_type: None,
            level: GrantLevel::Global,
            users: Vec::new(),
        }
    }
}

fn alter_table_noop() -> tidb_ast::AlterTableStmt {
    tidb_ast::AlterTableStmt {
        name: vec!["t".to_string()],
        actions: Vec::new(),
    }
}

struct CreateIndexStub;

impl CreateIndexStub {
    fn empty() -> tidb_ast::CreateIndexStmt {
        tidb_ast::CreateIndexStmt {
            kind: tidb_ast::IndexKind::Ordinary,
            if_not_exists: false,
            name: String::new(),
            table: Vec::new(),
            parts: Vec::new(),
            options: Default::default(),
            online: tidb_ast::IndexOnlineDdl { algorithm: None, lock: None },
        }
    }
}

struct CreateTableStub;

impl CreateTableStub {
    fn empty() -> tidb_ast::CreateTableStmt {
        tidb_ast::CreateTableStmt {
            temporary: tidb_ast::CreateTableTemporary::None,
            on_commit_delete: false,
            if_not_exists: false,
            name: vec!["t".to_string()],
            like_table: None,
            columns: Vec::new(),
            table_constraints: Vec::new(),
            table_options: Vec::new(),
            partitioning: None,
            splits: Vec::new(),
            ctas: None,
        }
    }
}

struct DropIndexStub;

impl DropIndexStub {
    fn empty() -> tidb_ast::DropIndexStmt {
        tidb_ast::DropIndexStmt {
            is_hypo: false,
            if_exists: false,
            name: String::new(),
            table: Vec::new(),
            algorithm: None,
            lock: None,
        }
    }
}

struct DropTableStub;

impl DropTableStub {
    fn empty() -> tidb_ast::DropTableStmt {
        tidb_ast::DropTableStmt {
            temporary: tidb_ast::DropTemporary::None,
            if_exists: false,
            names: Vec::new(),
        }
    }
}

struct RenameStub;

impl RenameStub {
    fn empty() -> tidb_ast::RenameTableStmt {
        tidb_ast::RenameTableStmt { pairs: Vec::new() }
    }
}

/// `pkg/parser/ast/misc_test.go::TestTableOptimizerHintRestore`.
///
/// Go extracts `SelectStmt.TableHints[0]` and restores it alone; Rust
/// spells hints as `Hint` with the same canonical names, asserted via
/// `Hint::restore()` here. `MEMORY_QUOTA(1 GB)` stores bytes in Go too, so
/// the normalized `1024 MB` output is checked against byte input.
#[test]
fn table_optimizer_hint_restore() {
    let hint = |name: &str, kind: HintKind| Hint {
        name: name.to_string(),
        kind,
    };
    let table = |name: &str| HintTable {
        db_name: None,
        name: name.to_string(),
        qb_name: None,
        partitions: Vec::new(),
    };
    let qualified =
        |schema: &str, name: &str| HintTable {
            db_name: Some(schema.to_string()),
            name: name.to_string(),
            qb_name: None,
            partitions: Vec::new(),
        };
    let table_qb = |name: &str, qb: &str| HintTable {
        db_name: None,
        name: name.to_string(),
        qb_name: Some(qb.to_string()),
        partitions: Vec::new(),
    };
    let qb_of = |kind: HintKind, qb: &str| match kind {
        HintKind::Nullary { .. } => HintKind::Nullary {
            qb_name: Some(qb.to_string()),
        },
        HintKind::Tables { tables, .. } => HintKind::Tables {
            qb_name: Some(qb.to_string()),
            tables,
        },
        HintKind::Index { table, indexes, .. } => HintKind::Index {
            qb_name: Some(qb.to_string()),
            table,
            indexes,
        },
        HintKind::Bool { value, .. } => HintKind::Bool {
            qb_name: Some(qb.to_string()),
            value,
        },
        HintKind::Keyword { value, .. } => HintKind::Keyword {
            qb_name: Some(qb.to_string()),
            value,
        },
        HintKind::Name { name, .. } => HintKind::Name {
            qb_name: Some(qb.to_string()),
            name,
        },
        HintKind::Number { value, .. } => HintKind::Number {
            qb_name: Some(qb.to_string()),
            value,
        },
        other => other,
    };

    let cases: [(Hint, &str); 109] = [
        (
            hint("USE_INDEX", HintKind::Index { qb_name: None, table: table("t1"), indexes: vec!["c1".to_string()] }),
            "USE_INDEX(`t1` `c1`)",
        ),
        (
            hint("USE_INDEX", HintKind::Index { qb_name: None, table: qualified("test", "t1"), indexes: vec!["c1".to_string()] }),
            "USE_INDEX(`test`.`t1` `c1`)",
        ),
        (
            hint("USE_INDEX", qb_of(HintKind::Index { qb_name: None, table: table("t1"), indexes: vec!["c1".to_string()] }, "sel_1")),
            "USE_INDEX(@`sel_1` `t1` `c1`)",
        ),
        (
            hint("USE_INDEX", HintKind::Index { qb_name: None, table: table_qb("t1", "sel_1"), indexes: vec!["c1".to_string()] }),
            "USE_INDEX(`t1`@`sel_1` `c1`)",
        ),
        (
            hint("USE_INDEX", HintKind::Index { qb_name: None, table: HintTable { db_name: Some("test".to_string()), name: "t1".to_string(), qb_name: Some("sel_1".to_string()), partitions: Vec::new() }, indexes: vec!["c1".to_string()] }),
            "USE_INDEX(`test`.`t1`@`sel_1` `c1`)",
        ),
        (
            hint("USE_INDEX", HintKind::Index { qb_name: None, table: HintTable { db_name: Some("test".to_string()), name: "t1".to_string(), qb_name: Some("sel_1".to_string()), partitions: vec!["p0".to_string()] }, indexes: vec!["c1".to_string()] }),
            "USE_INDEX(`test`.`t1`@`sel_1` PARTITION(`p0`) `c1`)",
        ),
        (
            hint("FORCE_INDEX", HintKind::Index { qb_name: None, table: table("t1"), indexes: vec!["c1".to_string()] }),
            "FORCE_INDEX(`t1` `c1`)",
        ),
        (
            hint("FORCE_INDEX", HintKind::Index { qb_name: None, table: qualified("test", "t1"), indexes: vec!["c1".to_string()] }),
            "FORCE_INDEX(`test`.`t1` `c1`)",
        ),
        (
            hint("FORCE_INDEX", qb_of(HintKind::Index { qb_name: None, table: table("t1"), indexes: vec!["c1".to_string()] }, "sel_1")),
            "FORCE_INDEX(@`sel_1` `t1` `c1`)",
        ),
        (
            hint("FORCE_INDEX", HintKind::Index { qb_name: None, table: table_qb("t1", "sel_1"), indexes: vec!["c1".to_string()] }),
            "FORCE_INDEX(`t1`@`sel_1` `c1`)",
        ),
        (
            hint("FORCE_INDEX", HintKind::Index { qb_name: None, table: HintTable { db_name: Some("test".to_string()), name: "t1".to_string(), qb_name: Some("sel_1".to_string()), partitions: Vec::new() }, indexes: vec!["c1".to_string()] }),
            "FORCE_INDEX(`test`.`t1`@`sel_1` `c1`)",
        ),
        (
            hint("FORCE_INDEX", HintKind::Index { qb_name: None, table: HintTable { db_name: Some("test".to_string()), name: "t1".to_string(), qb_name: Some("sel_1".to_string()), partitions: vec!["p0".to_string()] }, indexes: vec!["c1".to_string()] }),
            "FORCE_INDEX(`test`.`t1`@`sel_1` PARTITION(`p0`) `c1`)",
        ),
        (
            hint("IGNORE_INDEX", HintKind::Index { qb_name: None, table: table("t1"), indexes: vec!["c1".to_string()] }),
            "IGNORE_INDEX(`t1` `c1`)",
        ),
        (
            hint("IGNORE_INDEX", qb_of(HintKind::Index { qb_name: None, table: table("t1"), indexes: vec!["c1".to_string()] }, "sel_1")),
            "IGNORE_INDEX(@`sel_1` `t1` `c1`)",
        ),
        (
            hint("IGNORE_INDEX", HintKind::Index { qb_name: None, table: table_qb("t1", "sel_1"), indexes: vec!["c1".to_string()] }),
            "IGNORE_INDEX(`t1`@`sel_1` `c1`)",
        ),
        (
            hint("IGNORE_INDEX", HintKind::Index { qb_name: None, table: HintTable { db_name: None, name: "t1".to_string(), qb_name: Some("sel_1".to_string()), partitions: vec!["p0".to_string(), "p1".to_string()] }, indexes: vec!["c1".to_string()] }),
            "IGNORE_INDEX(`t1`@`sel_1` PARTITION(`p0`, `p1`) `c1`)",
        ),
        (
            hint("ORDER_INDEX", HintKind::Index { qb_name: None, table: table("t1"), indexes: vec!["c1".to_string()] }),
            "ORDER_INDEX(`t1` `c1`)",
        ),
        (
            hint("ORDER_INDEX", HintKind::Index { qb_name: None, table: qualified("test", "t1"), indexes: vec!["c1".to_string()] }),
            "ORDER_INDEX(`test`.`t1` `c1`)",
        ),
        (
            hint("ORDER_INDEX", qb_of(HintKind::Index { qb_name: None, table: table("t1"), indexes: vec!["c1".to_string()] }, "sel_1")),
            "ORDER_INDEX(@`sel_1` `t1` `c1`)",
        ),
        (
            hint("ORDER_INDEX", HintKind::Index { qb_name: None, table: table_qb("t1", "sel_1"), indexes: vec!["c1".to_string()] }),
            "ORDER_INDEX(`t1`@`sel_1` `c1`)",
        ),
        (
            hint("ORDER_INDEX", HintKind::Index { qb_name: None, table: HintTable { db_name: Some("test".to_string()), name: "t1".to_string(), qb_name: Some("sel_1".to_string()), partitions: Vec::new() }, indexes: vec!["c1".to_string()] }),
            "ORDER_INDEX(`test`.`t1`@`sel_1` `c1`)",
        ),
        (
            hint("ORDER_INDEX", HintKind::Index { qb_name: None, table: HintTable { db_name: Some("test".to_string()), name: "t1".to_string(), qb_name: Some("sel_1".to_string()), partitions: vec!["p0".to_string()] }, indexes: vec!["c1".to_string()] }),
            "ORDER_INDEX(`test`.`t1`@`sel_1` PARTITION(`p0`) `c1`)",
        ),
        (
            hint("NO_ORDER_INDEX", HintKind::Index { qb_name: None, table: table("t1"), indexes: vec!["c1".to_string()] }),
            "NO_ORDER_INDEX(`t1` `c1`)",
        ),
        (
            hint("NO_ORDER_INDEX", HintKind::Index { qb_name: None, table: qualified("test", "t1"), indexes: vec!["c1".to_string()] }),
            "NO_ORDER_INDEX(`test`.`t1` `c1`)",
        ),
        (
            hint("NO_ORDER_INDEX", qb_of(HintKind::Index { qb_name: None, table: table("t1"), indexes: vec!["c1".to_string()] }, "sel_1")),
            "NO_ORDER_INDEX(@`sel_1` `t1` `c1`)",
        ),
        (
            hint("NO_ORDER_INDEX", HintKind::Index { qb_name: None, table: table_qb("t1", "sel_1"), indexes: vec!["c1".to_string()] }),
            "NO_ORDER_INDEX(`t1`@`sel_1` `c1`)",
        ),
        (
            hint("NO_ORDER_INDEX", HintKind::Index { qb_name: None, table: HintTable { db_name: Some("test".to_string()), name: "t1".to_string(), qb_name: Some("sel_1".to_string()), partitions: Vec::new() }, indexes: vec!["c1".to_string()] }),
            "NO_ORDER_INDEX(`test`.`t1`@`sel_1` `c1`)",
        ),
        (
            hint("NO_ORDER_INDEX", HintKind::Index { qb_name: None, table: HintTable { db_name: Some("test".to_string()), name: "t1".to_string(), qb_name: Some("sel_1".to_string()), partitions: vec!["p0".to_string()] }, indexes: vec!["c1".to_string()] }),
            "NO_ORDER_INDEX(`test`.`t1`@`sel_1` PARTITION(`p0`) `c1`)",
        ),
        (
            hint("INDEX_LOOKUP_PUSHDOWN", HintKind::Index { qb_name: None, table: table("t1"), indexes: vec!["c1".to_string()] }),
            "INDEX_LOOKUP_PUSHDOWN(`t1` `c1`)",
        ),
        (
            hint("INDEX_LOOKUP_PUSHDOWN", HintKind::Index { qb_name: None, table: qualified("test", "t1"), indexes: vec!["c1".to_string()] }),
            "INDEX_LOOKUP_PUSHDOWN(`test`.`t1` `c1`)",
        ),
        (
            hint("INDEX_LOOKUP_PUSHDOWN", qb_of(HintKind::Index { qb_name: None, table: table("t1"), indexes: vec!["c1".to_string()] }, "sel_1")),
            "INDEX_LOOKUP_PUSHDOWN(@`sel_1` `t1` `c1`)",
        ),
        (
            hint("INDEX_LOOKUP_PUSHDOWN", HintKind::Index { qb_name: None, table: table_qb("t1", "sel_1"), indexes: vec!["c1".to_string()] }),
            "INDEX_LOOKUP_PUSHDOWN(`t1`@`sel_1` `c1`)",
        ),
        (
            hint("INDEX_LOOKUP_PUSHDOWN", HintKind::Index { qb_name: None, table: HintTable { db_name: Some("test".to_string()), name: "t1".to_string(), qb_name: Some("sel_1".to_string()), partitions: Vec::new() }, indexes: vec!["c1".to_string()] }),
            "INDEX_LOOKUP_PUSHDOWN(`test`.`t1`@`sel_1` `c1`)",
        ),
        (
            hint("INDEX_LOOKUP_PUSHDOWN", HintKind::Index { qb_name: None, table: HintTable { db_name: Some("test".to_string()), name: "t1".to_string(), qb_name: Some("sel_1".to_string()), partitions: vec!["p0".to_string()] }, indexes: vec!["c1".to_string()] }),
            "INDEX_LOOKUP_PUSHDOWN(`test`.`t1`@`sel_1` PARTITION(`p0`) `c1`)",
        ),
        // Table-list join/aggregate pushdown hints.
        (
            hint("TIDB_SMJ", HintKind::Tables { qb_name: None, tables: vec![table("t1")] }),
            "TIDB_SMJ(`t1`)",
        ),
        (
            hint("TIDB_SMJ", HintKind::Tables { qb_name: None, tables: vec![table("t1")] }),
            "TIDB_SMJ(`t1`)",
        ),
        (
            hint("TIDB_SMJ", HintKind::Tables { qb_name: None, tables: vec![table("t1"), table("t2")] }),
            "TIDB_SMJ(`t1`, `t2`)",
        ),
        (
            hint("TIDB_SMJ", qb_of(HintKind::Tables { qb_name: None, tables: vec![table("t1"), table("t2")] }, "sel1")),
            "TIDB_SMJ(@`sel1` `t1`, `t2`)",
        ),
        (
            hint("TIDB_SMJ", HintKind::Tables { qb_name: None, tables: vec![table_qb("t1", "sel1"), table_qb("t2", "sel2")] }),
            "TIDB_SMJ(`t1`@`sel1`, `t2`@`sel2`)",
        ),
        (
            hint("TIDB_INLJ", HintKind::Tables { qb_name: None, tables: vec![table("t1"), table("t2")] }),
            "TIDB_INLJ(`t1`, `t2`)",
        ),
        (
            hint("TIDB_INLJ", qb_of(HintKind::Tables { qb_name: None, tables: vec![table("t1"), table("t2")] }, "sel1")),
            "TIDB_INLJ(@`sel1` `t1`, `t2`)",
        ),
        (
            hint("TIDB_INLJ", HintKind::Tables { qb_name: None, tables: vec![table_qb("t1", "sel1"), table_qb("t2", "sel2")] }),
            "TIDB_INLJ(`t1`@`sel1`, `t2`@`sel2`)",
        ),
        (
            hint("TIDB_HJ", HintKind::Tables { qb_name: None, tables: vec![table("t1"), table("t2")] }),
            "TIDB_HJ(`t1`, `t2`)",
        ),
        (
            hint("TIDB_HJ", qb_of(HintKind::Tables { qb_name: None, tables: vec![table("t1"), table("t2")] }, "sel1")),
            "TIDB_HJ(@`sel1` `t1`, `t2`)",
        ),
        (
            hint("TIDB_HJ", HintKind::Tables { qb_name: None, tables: vec![table_qb("t1", "sel1"), table_qb("t2", "sel2")] }),
            "TIDB_HJ(`t1`@`sel1`, `t2`@`sel2`)",
        ),
        (
            hint("MERGE_JOIN", HintKind::Tables { qb_name: None, tables: vec![table("t1"), table("t2")] }),
            "MERGE_JOIN(`t1`, `t2`)",
        ),
        (
            hint("BROADCAST_JOIN", HintKind::Tables { qb_name: None, tables: vec![table("t1"), table("t2")] }),
            "BROADCAST_JOIN(`t1`, `t2`)",
        ),
        (
            hint("INL_HASH_JOIN", HintKind::Tables { qb_name: None, tables: vec![table("t1"), table("t2")] }),
            "INL_HASH_JOIN(`t1`, `t2`)",
        ),
        (
            hint("INL_MERGE_JOIN", HintKind::Tables { qb_name: None, tables: vec![table("t1"), table("t2")] }),
            "INL_MERGE_JOIN(`t1`, `t2`)",
        ),
        (
            hint("INL_JOIN", HintKind::Tables { qb_name: None, tables: vec![table("t1"), table("t2")] }),
            "INL_JOIN(`t1`, `t2`)",
        ),
        (
            hint("HASH_JOIN", HintKind::Tables { qb_name: None, tables: vec![table("t1"), table("t2")] }),
            "HASH_JOIN(`t1`, `t2`)",
        ),
        (
            hint("HASH_JOIN_BUILD", HintKind::Tables { qb_name: None, tables: vec![table("t1")] }),
            "HASH_JOIN_BUILD(`t1`)",
        ),
        (
            hint("HASH_JOIN_PROBE", HintKind::Tables { qb_name: None, tables: vec![table("t1")] }),
            "HASH_JOIN_PROBE(`t1`)",
        ),
        (
            hint("LEADING", HintKind::Leading { qb_name: None, elements: vec![LeadingElement::Table(table("t1"))] }),
            "LEADING(`t1`)",
        ),
        (
            hint("LEADING", HintKind::Leading { qb_name: None, elements: vec![LeadingElement::Table(table("t1")), LeadingElement::Table(table("c1"))] }),
            "LEADING(`t1`, `c1`)",
        ),
        (
            hint("LEADING", HintKind::Leading { qb_name: None, elements: vec![
                LeadingElement::Group(vec![LeadingElement::Table(table("t1")), LeadingElement::Table(table("c1"))]),
                LeadingElement::Table(table("t2")),
            ] }),
            "LEADING((`t1`, `c1`), `t2`)",
        ),
        (
            hint("LEADING", HintKind::Leading { qb_name: None, elements: vec![
                LeadingElement::Table(table("t1")),
                LeadingElement::Group(vec![LeadingElement::Table(table("c1")), LeadingElement::Table(table("t2"))]),
            ] }),
            "LEADING(`t1`, (`c1`, `t2`))",
        ),
        (
            hint("LEADING", HintKind::Leading { qb_name: None, elements: vec![
                LeadingElement::Group(vec![
                    LeadingElement::Group(vec![LeadingElement::Table(table("t1")), LeadingElement::Table(table("c1"))]),
                    LeadingElement::Table(table("t2")),
                ]),
                LeadingElement::Table(table("t3")),
            ] }),
            "LEADING(((`t1`, `c1`), `t2`), `t3`)",
        ),
        (
            hint("LEADING", HintKind::Leading { qb_name: None, elements: vec![
                LeadingElement::Table(table("t1")),
                LeadingElement::Group(vec![
                    LeadingElement::Table(table("c1")),
                    LeadingElement::Group(vec![LeadingElement::Table(table("t2")), LeadingElement::Table(table("t3"))]),
                ]),
            ] }),
            "LEADING(`t1`, (`c1`, (`t2`, `t3`)))",
        ),
        (
            hint("LEADING", HintKind::Leading { qb_name: None, elements: vec![LeadingElement::Table(table("t1")), LeadingElement::Table(table("c1")), LeadingElement::Table(table("t2"))] }),
            "LEADING(`t1`, `c1`, `t2`)",
        ),
        (
            hint("LEADING", HintKind::Leading { qb_name: Some("sel1".to_string()), elements: vec![LeadingElement::Table(table("t1")), LeadingElement::Table(table("c1"))] }),
            "LEADING(@`sel1` `t1`, `c1`)",
        ),
        (
            hint("LEADING", HintKind::Leading { qb_name: Some("sel1".to_string()), elements: vec![LeadingElement::Table(table("t1"))] }),
            "LEADING(@`sel1` `t1`)",
        ),
        (
            hint("LEADING", HintKind::Leading { qb_name: Some("sel1".to_string()), elements: vec![LeadingElement::Table(table("t1")), LeadingElement::Table(table("c1")), LeadingElement::Table(table("t2"))] }),
            "LEADING(@`sel1` `t1`, `c1`, `t2`)",
        ),
        (
            hint("LEADING", HintKind::Leading { qb_name: Some("sel1".to_string()), elements: vec![
                LeadingElement::Table(table("t1")),
                LeadingElement::Group(vec![LeadingElement::Table(table("c1")), LeadingElement::Table(table("t2"))]),
            ] }),
            "LEADING(@`sel1` `t1`, (`c1`, `t2`))",
        ),
        (
            hint("LEADING", HintKind::Leading { qb_name: Some("sel1".to_string()), elements: vec![
                LeadingElement::Table(table("t1")),
                LeadingElement::Group(vec![LeadingElement::Table(table("c1")), LeadingElement::Table(table("t2"))]),
                LeadingElement::Table(table("d3")),
            ] }),
            "LEADING(@`sel1` `t1`, (`c1`, `t2`), `d3`)",
        ),
        (
            hint("LEADING", HintKind::Leading { qb_name: None, elements: vec![LeadingElement::Table(table_qb("t1", "sel1"))] }),
            "LEADING(`t1`@`sel1`)",
        ),
        (
            hint("LEADING", HintKind::Leading { qb_name: None, elements: vec![LeadingElement::Table(table_qb("t1", "sel1")), LeadingElement::Table(table("c1"))] }),
            "LEADING(`t1`@`sel1`, `c1`)",
        ),
        (
            hint("LEADING", HintKind::Leading { qb_name: None, elements: vec![LeadingElement::Table(table_qb("t1", "sel1")), LeadingElement::Table(table("c1")), LeadingElement::Table(table("t2"))] }),
            "LEADING(`t1`@`sel1`, `c1`, `t2`)",
        ),
        (
            hint("LEADING", HintKind::Leading { qb_name: None, elements: vec![
                LeadingElement::Group(vec![LeadingElement::Table(table_qb("t1", "sel1")), LeadingElement::Table(table("c1"))]),
                LeadingElement::Table(table("t2")),
            ] }),
            "LEADING((`t1`@`sel1`, `c1`), `t2`)",
        ),
        (
            hint("LEADING", HintKind::Leading { qb_name: None, elements: vec![
                LeadingElement::Table(table_qb("t1", "sel1")),
                LeadingElement::Group(vec![LeadingElement::Table(table("c1")), LeadingElement::Table(table("t2"))]),
            ] }),
            "LEADING(`t1`@`sel1`, (`c1`, `t2`))",
        ),
        (
            hint("LEADING", HintKind::Leading { qb_name: None, elements: vec![LeadingElement::Table(table_qb("t1", "sel1")), LeadingElement::Table(table("c1")), LeadingElement::Table(table("t2")), LeadingElement::Table(table("d3"))] }),
            "LEADING(`t1`@`sel1`, `c1`, `t2`, `d3`)",
        ),
        (
            hint("LEADING", HintKind::Leading { qb_name: None, elements: vec![
                LeadingElement::Table(table_qb("t1", "sel1")),
                LeadingElement::Group(vec![LeadingElement::Table(table("c1")), LeadingElement::Table(table("t2"))]),
                LeadingElement::Table(table("d3")),
            ] }),
            "LEADING(`t1`@`sel1`, (`c1`, `t2`), `d3`)",
        ),
        (
            hint("MAX_EXECUTION_TIME", HintKind::Number { qb_name: None, value: 3000 }),
            "MAX_EXECUTION_TIME(3000)",
        ),
        (
            hint("MAX_EXECUTION_TIME", HintKind::Number { qb_name: Some("sel1".to_string()), value: 3000 }),
            "MAX_EXECUTION_TIME(@`sel1` 3000)",
        ),
        (
            hint("USE_INDEX_MERGE", HintKind::Index { qb_name: None, table: table("t1"), indexes: vec!["c1".to_string()] }),
            "USE_INDEX_MERGE(`t1` `c1`)",
        ),
        (
            hint("USE_INDEX_MERGE", HintKind::Index { qb_name: Some("sel1".to_string()), table: table("t1"), indexes: vec!["c1".to_string()] }),
            "USE_INDEX_MERGE(@`sel1` `t1` `c1`)",
        ),
        (
            hint("USE_INDEX_MERGE", HintKind::Index { qb_name: None, table: table_qb("t1", "sel1"), indexes: vec!["c1".to_string()] }),
            "USE_INDEX_MERGE(`t1`@`sel1` `c1`)",
        ),
        (
            hint("USE_TOJA", HintKind::Bool { qb_name: None, value: true }),
            "USE_TOJA(TRUE)",
        ),
        (
            hint("USE_TOJA", HintKind::Bool { qb_name: None, value: false }),
            "USE_TOJA(FALSE)",
        ),
        (
            hint("USE_TOJA", HintKind::Bool { qb_name: Some("sel1".to_string()), value: true }),
            "USE_TOJA(@`sel1` TRUE)",
        ),
        (
            hint("USE_CASCADES", HintKind::Bool { qb_name: None, value: true }),
            "USE_CASCADES(TRUE)",
        ),
        (
            hint("USE_CASCADES", HintKind::Bool { qb_name: None, value: false }),
            "USE_CASCADES(FALSE)",
        ),
        (
            hint("USE_CASCADES", HintKind::Bool { qb_name: Some("sel1".to_string()), value: true }),
            "USE_CASCADES(@`sel1` TRUE)",
        ),
        (
            hint("QUERY_TYPE", HintKind::Keyword { qb_name: None, value: "OLAP".to_string() }),
            "QUERY_TYPE(OLAP)",
        ),
        (
            hint("QUERY_TYPE", HintKind::Keyword { qb_name: None, value: "OLTP".to_string() }),
            "QUERY_TYPE(OLTP)",
        ),
        (
            hint("QUERY_TYPE", HintKind::Keyword { qb_name: Some("sel1".to_string()), value: "OLTP".to_string() }),
            "QUERY_TYPE(@`sel1` OLTP)",
        ),
        (
            hint("NTH_PLAN", HintKind::Number { qb_name: None, value: 10 }),
            "NTH_PLAN(10)",
        ),
        (
            hint("NTH_PLAN", HintKind::Number { qb_name: Some("sel1".to_string()), value: 30 }),
            "NTH_PLAN(@`sel1` 30)",
        ),
        (
            hint("MEMORY_QUOTA", HintKind::MemoryQuota { qb_name: None, bytes: 1_073_741_824 }),
            "MEMORY_QUOTA(1024 MB)",
        ),
        (
            hint("MEMORY_QUOTA", HintKind::MemoryQuota { qb_name: Some("sel1".to_string()), bytes: 1_073_741_824 }),
            "MEMORY_QUOTA(@`sel1` 1024 MB)",
        ),
        (
            hint("HASH_AGG", HintKind::Nullary { qb_name: None }),
            "HASH_AGG()",
        ),
        (
            hint("HASH_AGG", HintKind::Nullary { qb_name: Some("sel1".to_string()) }),
            "HASH_AGG(@`sel1`)",
        ),
        (
            hint("STREAM_AGG", HintKind::Nullary { qb_name: None }),
            "STREAM_AGG()",
        ),
        (
            hint("STREAM_AGG", HintKind::Nullary { qb_name: Some("sel1".to_string()) }),
            "STREAM_AGG(@`sel1`)",
        ),
        (
            hint("AGG_TO_COP", HintKind::Nullary { qb_name: None }),
            "AGG_TO_COP()",
        ),
        (
            hint("AGG_TO_COP", HintKind::Nullary { qb_name: Some("sel_1".to_string()) }),
            "AGG_TO_COP(@`sel_1`)",
        ),
        (
            hint("LIMIT_TO_COP", HintKind::Nullary { qb_name: None }),
            "LIMIT_TO_COP()",
        ),
        (
            hint("MERGE", HintKind::Nullary { qb_name: None }),
            "MERGE()",
        ),
        (
            hint("STRAIGHT_JOIN", HintKind::Nullary { qb_name: None }),
            "STRAIGHT_JOIN()",
        ),
        (
            hint("NO_INDEX_MERGE", HintKind::Nullary { qb_name: None }),
            "NO_INDEX_MERGE()",
        ),
        (
            hint("NO_INDEX_MERGE", HintKind::Nullary { qb_name: Some("sel1".to_string()) }),
            "NO_INDEX_MERGE(@`sel1`)",
        ),
        (
            hint("READ_CONSISTENT_REPLICA", HintKind::Nullary { qb_name: None }),
            "READ_CONSISTENT_REPLICA()",
        ),
        (
            hint("READ_CONSISTENT_REPLICA", HintKind::Nullary { qb_name: Some("sel1".to_string()) }),
            "READ_CONSISTENT_REPLICA(@`sel1`)",
        ),
        (
            hint("QB_NAME", HintKind::QbName { qb_name: "sel1".to_string(), views: Vec::new() }),
            "QB_NAME(`sel1`)",
        ),
        (
            hint("READ_FROM_STORAGE", HintKind::ReadFromStorage {
                qb_name: Some("sel".to_string()),
                groups: vec![("TIFLASH".to_string(), vec![table("t1"), table("t2")])],
            }),
            "READ_FROM_STORAGE(@`sel` TIFLASH[`t1`, `t2`])",
        ),
        (
            hint("READ_FROM_STORAGE", HintKind::ReadFromStorage {
                qb_name: Some("sel".to_string()),
                groups: vec![("TIFLASH".to_string(), vec![HintTable {
                    db_name: None,
                    name: "t1".to_string(),
                    qb_name: None,
                    partitions: vec!["p0".to_string()],
                }])],
            }),
            "READ_FROM_STORAGE(@`sel` TIFLASH[`t1` PARTITION(`p0`)])",
        ),
        (
            hint("TIME_RANGE", HintKind::TimeRange {
                from: "2020-02-02 10:10:10".to_string(),
                to: "2020-02-02 11:10:10".to_string(),
            }),
            "TIME_RANGE('2020-02-02 10:10:10', '2020-02-02 11:10:10')",
        ),
        (
            hint("RESOURCE_GROUP", HintKind::Name { qb_name: None, name: "rg1".to_string() }),
            "RESOURCE_GROUP(`rg1`)",
        ),
        (
            hint("RESOURCE_GROUP", HintKind::Name { qb_name: None, name: "default".to_string() }),
            "RESOURCE_GROUP(`default`)",
        ),
    ];
    for (hint_value, want) in cases {
        assert_eq!(hint_value.restore(), want, "{want}");
    }
}

/// `pkg/parser/ast/misc_test.go::TestBRIESecureText`.
///
/// Go used regexes because the BRIE option map iterated randomly; this
/// crate redacts via the same sorted-order `redact_url`, so exact strings
/// are pinned. The gcs row confirms credentials-file survives unredacted.
#[test]
fn brie_secure_text() {
    let brie = |kind: BrieKind, storage: &str, options: Vec<BrieOption>| {
        Stmt::Admin(NodeBox::new(AdminStmt::Brie(Box::new(BrieStmt {
            kind,
            schemas: Vec::new(),
            tables: Vec::new(),
            storage: storage.to_string(),
            job_id: 0,
            options,
        }))))
    };
    let secure_of_brie = |stmt: &Stmt| -> String {
        let Stmt::Admin(a) = stmt else { panic!("admin expected") };
        let AdminStmt::Brie(brie_stmt) = a.as_ref() else { panic!("brie expected") };
        brie_stmt.secure_text()
    };

    // RESTORE DATABASE * FROM local path + SNAPSHOT = 23333 (untouched).
    let restored = brie(
        BrieKind::Restore,
        "local:///tmp/br01",
        vec![BrieOption {
            name: "SNAPSHOT".to_string(),
            value: BrieOptionValue::Unsigned(23333),
        }],
    );
    assert_eq!(
        secure_of_brie(&restored),
        "RESTORE DATABASE * FROM 'local:///tmp/br01' SNAPSHOT = 23333"
    );

    // BACKUP to s3 with region only — nothing sensitive.
    let region_backup = brie(
        BrieKind::Backup,
        "s3://bucket/prefix?region=us-west-2",
        Vec::new(),
    );
    assert_eq!(
        secure_of_brie(&region_backup),
        "BACKUP DATABASE * TO 's3://bucket/prefix?region=us-west-2'"
    );

    // BACKUP to s3 with three parameters; sensitive values become xxxxxx.
    let redacted_backup = brie(
        BrieKind::Backup,
        "s3://bucket/prefix?access-key=abcdefghi&secret-access-key=123&force-path-style=true",
        Vec::new(),
    );
    assert_eq!(
        secure_of_brie(&redacted_backup),
        "BACKUP DATABASE * TO 's3://bucket/prefix?access-key=xxxxxx&force-path-style=true&secret-access-key=xxxxxx'"
    );

    // gcs: access-key and credentials-file are NOT redacted for gcs.
    let gcs_backup = brie(
        BrieKind::Backup,
        "gcs://bucket/prefix?access-key=irrelevant&credentials-file=/home/user/secrets.txt",
        Vec::new(),
    );
    assert_eq!(
        secure_of_brie(&gcs_backup),
        "BACKUP DATABASE * TO 'gcs://bucket/prefix?access-key=irrelevant&credentials-file=/home/user/secrets.txt'"
    );

}

/// `pkg/parser/ast/misc_test.go::TestCompactTableStmtRestore`.
#[test]
fn compact_table_stmt_restore() {
    let alter = |name: &str, replica_kind: CompactReplicaKind| {
        Stmt::Ddl(NodeBox::new(DdlStmt::AlterTable(Box::new(
            tidb_ast::AlterTableStmt {
                name: vec![name.to_string()],
                actions: vec![tidb_ast::AlterTableAction::Compact {
                    partitions: Vec::new(),
                    replica_kind,
                }],
            },
        ))))
    };
    assert_eq!(
        alter("abc", CompactReplicaKind::TiFlash).restore(),
        "ALTER TABLE `abc` COMPACT TIFLASH REPLICA"
    );
    assert_eq!(
        alter("abc", CompactReplicaKind::All).restore(),
        "ALTER TABLE `abc` COMPACT"
    );
    // test.abc qualified name.
    assert_eq!(
        Stmt::Ddl(NodeBox::new(DdlStmt::AlterTable(Box::new(
            tidb_ast::AlterTableStmt {
                name: vec!["test".to_string(), "abc".to_string()],
                actions: vec![tidb_ast::AlterTableAction::Compact {
                    partitions: Vec::new(),
                    replica_kind: CompactReplicaKind::All,
                }],
            },
        ))))
        .restore(),
        "ALTER TABLE `test`.`abc` COMPACT"
    );
}

/// `pkg/parser/ast/misc_test.go::TestPlanReplayerStmtRestore`.
#[test]
fn plan_replayer_stmt_restore() {
    let explain_target_select = || {
        let mut select = star_select();
        select.fields.push(tidb_ast::SelectField::Wildcard(Vec::new()));
        select.from = Some(tidb_ast::Join {
            left: JoinNode::Table(TableRef {
                name: vec!["t".to_string()],
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
        select.where_clause = Some(BinaryGtAHundred::expr());
        Stmt::Query(NodeBox::new(tidb_ast::QueryStmt::Select(Box::new(select))))
    };

    let dump = |historical_stats: Option<Expr>, analyze: bool, target: PlanReplayerTarget| {
        admin(AdminStmt::PlanReplayer(Box::new(PlanReplayerStmt::Dump {
            historical_stats: historical_stats.map(Box::new),
            analyze,
            target: Box::new(target),
        })))
    };

    let cases: [(Stmt, &str); 7] = [
        (
            dump(
                Some(string("2023-06-28 12:34:00")),
                false,
                PlanReplayerTarget::Statement(Box::new(explain_target_select())),
            ),
            "PLAN REPLAYER DUMP WITH STATS AS OF TIMESTAMP _UTF8MB4'2023-06-28 12:34:00' EXPLAIN SELECT * FROM `t` WHERE `a`>10",
        ),
        (
            dump(
                None,
                true,
                PlanReplayerTarget::Statement(Box::new(explain_target_select())),
            ),
            "PLAN REPLAYER DUMP EXPLAIN ANALYZE SELECT * FROM `t` WHERE `a`>10",
        ),
        (
            dump(
                Some(int("12345")),
                true,
                PlanReplayerTarget::Statement(Box::new(explain_target_select())),
            ),
            "PLAN REPLAYER DUMP WITH STATS AS OF TIMESTAMP 12345 EXPLAIN ANALYZE SELECT * FROM `t` WHERE `a`>10",
        ),
        (
            dump(None, true, PlanReplayerTarget::File("test".to_string())),
            "PLAN REPLAYER DUMP EXPLAIN ANALYZE 'test'",
        ),
        (
            dump(
                Some(string("12345")),
                true,
                PlanReplayerTarget::File("test2".to_string()),
            ),
            "PLAN REPLAYER DUMP WITH STATS AS OF TIMESTAMP _UTF8MB4'12345' EXPLAIN ANALYZE 'test2'",
        ),
        (
            dump(
                None,
                false,
                PlanReplayerTarget::Statements(vec![
                    "SELECT * FROM t1".to_string(),
                    "SELECT * FROM t2".to_string(),
                ]),
            ),
            "PLAN REPLAYER DUMP EXPLAIN ('SELECT * FROM t1', 'SELECT * FROM t2')",
        ),
        (
            dump(
                None,
                true,
                PlanReplayerTarget::Statements(vec!["SELECT * FROM t1".to_string()]),
            ),
            "PLAN REPLAYER DUMP EXPLAIN ANALYZE ('SELECT * FROM t1')",
        ),
    ];
    for (stmt, want) in cases {
        assert_eq!(stmt.restore(), want);
    }
}

struct BinaryGtAHundred;

impl BinaryGtAHundred {
    /// `a > 10`.
    fn expr() -> Expr {
        Expr::Binary(BinaryOp::Gt, Box::new(column(&["a"])), Box::new(int("10")))
    }
}

/// `pkg/parser/ast/misc_test.go::TestRedactURL`.
#[test]
fn redact_url() {
    use tidb_ast::redact_url;
    let cases: [(&str, &str); 22] = [
        ("", ""),
        (":", ":"),
        ("~/file", "~/file"),
        ("gs://bucket/file", "gs://bucket/file"),
        // gs has no access-key/secret-access-key, so nothing is redacted.
        ("gs://bucket/file?access-key=123", "gs://bucket/file?access-key=123"),
        (
            "gs://bucket/file?secret-access-key=123",
            "gs://bucket/file?secret-access-key=123",
        ),
        ("s3://bucket/file", "s3://bucket/file"),
        ("s3://bucket/file?other-key=123", "s3://bucket/file?other-key=123"),
        ("s3://bucket/file?access-key=123", "s3://bucket/file?access-key=xxxxxx"),
        (
            "s3://bucket/file?secret-access-key=123",
            "s3://bucket/file?secret-access-key=xxxxxx",
        ),
        ("ks3://bucket/file?access-key=123", "ks3://bucket/file?access-key=xxxxxx"),
        (
            "ks3://bucket/file?secret-access-key=123",
            "ks3://bucket/file?secret-access-key=xxxxxx",
        ),
        ("oss://bucket/file?access-key=123", "oss://bucket/file?access-key=xxxxxx"),
        (
            "oss://bucket/file?secret-access-key=123",
            "oss://bucket/file?secret-access-key=xxxxxx",
        ),
        // Underscore spellings normalize onto the dash keys for detection.
        ("s3://bucket/file?access_key=123", "s3://bucket/file?access_key=xxxxxx"),
        (
            "s3://bucket/file?secret_access_key=123",
            "s3://bucket/file?secret_access_key=xxxxxx",
        ),
        ("azure://bucket/file?sas-token=123", "azure://bucket/file?sas-token=xxxxxx"),
        (
            "azblob://container/file?sas-token=123",
            "azblob://container/file?sas-token=xxxxxx",
        ),
        (
            "azure://container/file?account-name=test&sas_token=123",
            "azure://container/file?account-name=test&sas_token=xxxxxx",
        ),
        (
            "azure://container/file?account-name=test&account-key=123",
            "azure://container/file?account-key=xxxxxx&account-name=test",
        ),
        ("azblob://container/file?encryption-key=123", "azblob://container/file?encryption-key=xxxxxx"),
        (
            "azure://container/file?account_key=123&encryption_key=456",
            "azure://container/file?account_key=xxxxxx&encryption_key=xxxxxx",
        ),
    ];
    for (input, want) in cases {
        assert_eq!(redact_url(input), want, "input {input}");
    }
}

/// `pkg/parser/ast/misc_test.go::TestAddQueryWatchStmtRestore`.
#[test]
fn add_query_watch_stmt_restore() {
    let watch = |options: Vec<QueryWatchOption>| {
        admin(AdminStmt::AddQueryWatch(Box::new(
            tidb_ast::AddQueryWatchStmt { options },
        )))
    };
    let pattern = |text: &str| string(text);

    let cases: [Stmt; 4] = [
        watch(vec![
            QueryWatchOption::Action(ResourceGroupRunawayAction::Kill),
            QueryWatchOption::Text(QueryWatchTextOption {
                watch_type: RunawayWatchType::Exact,
                pattern: pattern("select * from test.t2"),
                type_specified: true,
            }),
        ]),
        watch(vec![
            QueryWatchOption::ResourceGroup("rg1".to_string()),
            QueryWatchOption::Text(QueryWatchTextOption {
                watch_type: RunawayWatchType::Similar,
                pattern: pattern("select * from test.t2"),
                type_specified: true,
            }),
        ]),
        watch(vec![
            QueryWatchOption::ResourceGroup("rg1".to_string()),
            QueryWatchOption::Action(ResourceGroupRunawayAction::Cooldown),
            QueryWatchOption::Text(QueryWatchTextOption {
                watch_type: RunawayWatchType::Plan,
                pattern: pattern(
                    "d08bc323a934c39dc41948b0a073725be3398479b6fa4f6dd1db2a9b115f7f57",
                ),
                type_specified: false,
            }),
        ]),
        watch(vec![
            QueryWatchOption::Action(ResourceGroupRunawayAction::SwitchGroup(
                "rg1".to_string(),
            )),
            QueryWatchOption::Text(QueryWatchTextOption {
                watch_type: RunawayWatchType::Exact,
                pattern: pattern("select * from test.t1"),
                type_specified: true,
            }),
        ]),
    ];
    let expectations = [
        "QUERY WATCH ADD ACTION = KILL SQL TEXT EXACT TO _UTF8MB4'select * from test.t2'",
        "QUERY WATCH ADD RESOURCE GROUP `rg1` SQL TEXT SIMILAR TO _UTF8MB4'select * from test.t2'",
        "QUERY WATCH ADD RESOURCE GROUP `rg1` ACTION = COOLDOWN PLAN DIGEST _UTF8MB4'd08bc323a934c39dc41948b0a073725be3398479b6fa4f6dd1db2a9b115f7f57'",
        "QUERY WATCH ADD ACTION = SWITCH_GROUP(`rg1`) SQL TEXT EXACT TO _UTF8MB4'select * from test.t1'",
    ];
    for (stmt, want) in cases.into_iter().zip(expectations) {
        assert_eq!(stmt.restore(), want);
    }
    // The raw-string storage prints without `_UTF8MB4`; pin that too by
    // proving the `_UTF8MB4` spelling comes from Expr::String values only.
    let digest_with_intro = admin(AdminStmt::AddQueryWatch(Box::new(
        tidb_ast::AddQueryWatchStmt {
            options: vec![QueryWatchOption::Text(QueryWatchTextOption {
                watch_type: RunawayWatchType::Plan,
                pattern: string("digest"),
                type_specified: false,
            })],
        },
    )));
    assert!(digest_with_intro.restore().contains("_UTF8MB4'digest'"));
}

/// `pkg/parser/ast/misc_test.go::TestRedactTrafficStmt`.
#[test]
fn redact_traffic_stmt() {
    let capture = Stmt::Admin(NodeBox::new(AdminStmt::Traffic(Box::new(
        TrafficStmt::Capture {
            dir: "s3://bucket/prefix?access-key=abcdefghi&secret-access-key=123&force-path-style=true"
                .to_string(),
            options: vec![TrafficCaptureOption::Duration("1m".to_string())],
        },
    ))));
    let Stmt::Admin(admin_stmt) = &capture else { panic!() };
    let AdminStmt::Traffic(traffic) = admin_stmt.as_ref() else { panic!() };
    assert_eq!(
        traffic.secure_text(),
        "TRAFFIC CAPTURE TO 's3://bucket/prefix?access-key=xxxxxx&force-path-style=true&secret-access-key=xxxxxx' DURATION = '1m'"
    );

    let replay = Stmt::Admin(NodeBox::new(AdminStmt::Traffic(Box::new(
        TrafficStmt::Replay {
            dir: "s3://bucket/prefix?access-key=abcdefghi&secret-access-key=123&force-path-style=true"
                .to_string(),
            options: vec![
                TrafficReplayOption::User("root".to_string()),
                TrafficReplayOption::Password("123456".to_string()),
            ],
        },
    ))));
    let Stmt::Admin(admin_stmt) = &replay else { panic!() };
    let AdminStmt::Traffic(traffic) = admin_stmt.as_ref() else { panic!() };
    assert_eq!(
        traffic.secure_text(),
        "TRAFFIC REPLAY FROM 's3://bucket/prefix?access-key=xxxxxx&force-path-style=true&secret-access-key=xxxxxx' USER = 'root' PASSWORD = 'xxxxxx'"
    );
}

// go-parity-gap: TestSetStmtSecureTextRedactsEmbeddingAPIKeys needs
// `SetStmt.SecureText`'s embedding API-key sysvar redaction
// (`SET @@GLOBAL.<key>='******'`); that transcreation does not exist yet —
// there is no secure-text entry point for SET at all.
#[test]
#[ignore = "go-parity-gap: embedding API-key sysvar redaction is not transcreated"]
fn set_stmt_secure_text_redacts_embedding_api_keys() {}

/// `pkg/parser/ast/misc_test.go::TestSetPwdStmtSecureText`.
///
/// Direct construction, matching the Go test's own approach: SecureText
/// must not leak the password and must not print `<nil>` for the
/// current-user form.
#[test]
fn set_pwd_stmt_secure_text() {
    let named_user = || UserSpec {
        current_user: false,
        user: "u".to_string(),
        host: "%".to_string(),
    };
    let cases: [(Option<UserSpec>, bool, &str); 4] = [
        (None, false, "set password"),
        (None, true, "set password RETAIN CURRENT PASSWORD"),
        (Some(named_user()), false, "set password for user u@%"),
        (Some(named_user()), true, "set password for user u@% RETAIN CURRENT PASSWORD"),
    ];
    for (user, retain, want) in cases {
        let stmt = SetPasswordStmt {
            user,
            password: "x".to_string(),
            retain_current_password: retain,
        };
        assert_eq!(stmt.secure_text(), want);
        // The password never leaks into any secured form.
        assert!(!stmt.secure_text().contains('x'));
    }
}
