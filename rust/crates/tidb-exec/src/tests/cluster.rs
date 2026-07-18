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

//! Shared-cluster session tests.  These are intentionally separate from
//! `session.rs`: each source worker owns a different `Session`, and the test
//! must use real OS threads rather than replaying a chosen interleaving.

use std::{sync::mpsc, thread};

use super::*;

fn cluster_step(session: &mut Session, sql: &str) -> String {
    match session.run(&tidb_parser::parse(sql).expect("parse cluster SQL")) {
        Ok(Outcome::Done) => "OK".to_string(),
        Ok(Outcome::Rows(rows)) => rows.label(),
        Err(error) => format!("{error:?}"),
    }
}

/// The automatic response adapter must use the planner-shaped output schema
/// for the same LEFT/USING forms that the seed row executor already supports.
/// This checks both outer-side width and source-defined USING common-column
/// ordering without inferring anything from returned Datum values.
#[test]
fn shared_cluster_join_metadata_matches_seed_join_output_shape() {
    let cluster = Cluster::new();
    let mut session = cluster.session();
    assert_eq!(cluster_step(&mut session, "use test"), "OK");
    assert_eq!(
        cluster_step(
            &mut session,
            "create table join_left (z int not null, id int not null, payload int not null)"
        ),
        "OK"
    );
    assert_eq!(
        cluster_step(
            &mut session,
            "create table join_right (id int not null, z int not null, extra int not null)"
        ),
        "OK"
    );

    let using = session
        .resolve_query_result_columns(
            "select * from join_left join join_right using (id, z)",
            tidb_datatype::Collation::Utf8Mb4GeneralCi,
            "test",
        )
        .expect("USING result metadata");
    assert_eq!(
        using
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>(),
        ["z", "id", "payload", "extra"]
    );

    let left = session
        .resolve_query_result_columns(
            "select * from join_left left join join_right on join_left.id = join_right.id",
            tidb_datatype::Collation::Utf8Mb4GeneralCi,
            "test",
        )
        .expect("LEFT result metadata");
    assert_eq!(left.len(), 6);
    assert_eq!(left[0].name, "z");
    assert_eq!(left[5].name, "extra");

    let qualified = session
        .resolve_query_result_columns(
            "select join_right.id from join_left left join join_right on join_left.id = join_right.id",
            tidb_datatype::Collation::Utf8Mb4GeneralCi,
            "test",
        )
        .expect("qualified LEFT projection uses planner output names");
    assert_eq!(qualified.len(), 1);
    assert_eq!(qualified[0].name, "id");
    assert_eq!(qualified[0].table, "join_right");

    let right = session
        .resolve_query_result_columns(
            "select * from join_left l right join join_right r on l.id = r.id",
            tidb_datatype::Collation::Utf8Mb4GeneralCi,
            "test",
        )
        .expect("ordinary RIGHT metadata");
    assert_eq!(
        right
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>(),
        ["z", "id", "payload", "id", "z", "extra"]
    );
    assert!(right[..3]
        .iter()
        .all(|column| column.flag & crate::result_metadata::NOT_NULL_FLAG == 0));

    for sql in [
        "select * from join_left l right join join_right r using (z, id)",
        "select * from join_left l natural right join join_right r",
    ] {
        let columns = session
            .resolve_query_result_columns(sql, tidb_datatype::Collation::Utf8Mb4GeneralCi, "test")
            .expect("RIGHT coalescing metadata");
        assert_eq!(
            columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            ["id", "z", "extra", "payload"],
            "SQL: {sql}"
        );
    }

    assert_eq!(
        cluster_step(
            &mut session,
            "create table join_third (id int not null, third_only int not null)"
        ),
        "OK"
    );
    let nested = session
        .resolve_query_result_columns(
            "select r.id as nested_id, t.id as outer_id from (join_left l left join join_right r using (id)) right join join_third t using (id)",
            tidb_datatype::Collation::Utf8Mb4GeneralCi,
            "test",
        )
        .expect("nested RIGHT FullSchema mapping");
    assert_eq!(nested[0].name, "nested_id");
    assert_eq!(nested[0].table, "r");
    assert_eq!(nested[0].org_table, "join_right");
    assert_eq!(nested[0].org_name, "id");
    assert_eq!(
        nested[0].flag & crate::result_metadata::NOT_NULL_FLAG,
        0,
        "the complete original-left subtree is nullable under the outer RIGHT join"
    );
    assert_eq!(nested[1].table, "t");
}

#[test]
fn shared_cluster_executes_right_and_natural_join_runtime() {
    let cluster = Cluster::new();
    let mut session = cluster.session();
    for sql in [
        "create table runtime_left (id int, v int)",
        "create table runtime_outer (id int, v int)",
        "insert into runtime_left values (1, 10), (2, null)",
        "insert into runtime_outer values (2, 5), (1, 5), (4, 40)",
    ] {
        assert_eq!(cluster_step(&mut session, sql), "OK", "SQL: {sql}");
    }
    assert_eq!(
        session.execute_sql(
            "select l.id, l.v, r.id from runtime_left l right join runtime_outer r on l.id = r.id and l.v > r.v"
        ),
        Ok(Outcome::Rows(ResultSet {
            rows: vec![
                vec![Datum::Null, Datum::Null, Datum::Int(2)],
                vec![Datum::Int(1), Datum::Int(10), Datum::Int(1)],
                vec![Datum::Null, Datum::Null, Datum::Int(4)],
            ],
            ordered: false,
        }))
    );

    for sql in [
        "create table runtime_natural_left (z int, id int, left_only int)",
        "create table runtime_natural_outer (id int, z int, right_only int)",
        "insert into runtime_natural_left values (10, 1, 100)",
        "insert into runtime_natural_outer values (1, 10, 200), (2, 20, 300)",
    ] {
        assert_eq!(cluster_step(&mut session, sql), "OK", "SQL: {sql}");
    }
    assert_eq!(
        cluster_step(
            &mut session,
            "select * from runtime_natural_left l natural right join runtime_natural_outer r"
        ),
        "RS:1|10|200|100;2|20|300|<nil>"
    );
}

/// Structural concurrent port of `pkg/session/test/txn/txn_test.go:346
/// TestErrorRollback`.
///
/// Go starts four independent sessions against one MockStore.  Each repeats a
/// INSERT (whose result Go intentionally ignores: one first insert can
/// succeed and every later attempt duplicates) followed by an autocommit
/// increment.  The test intentionally does not prescribe goroutine ordering:
/// the only valid observable outcome is every successful increment reaching
/// the one shared row.  Scoped OS threads preserve that source topology
/// instead of faking a particular schedule.
#[test]
fn shared_cluster_error_rollback_source_contract() {
    let cluster = Cluster::new();
    let mut setup = cluster.session();
    assert_eq!(cluster_step(&mut setup, "use test"), "OK");
    assert_eq!(
        cluster_step(&mut setup, "drop table if exists t_rollback"),
        "OK"
    );
    assert_eq!(
        cluster_step(
            &mut setup,
            "create table t_rollback (c1 int, c2 int, primary key(c1))",
        ),
        "OK"
    );
    assert_eq!(
        cluster_step(&mut setup, "insert into t_rollback values (0, 0)"),
        "OK"
    );
    // Four workers execute their first INSERT against the same committed
    // snapshot. The barrier only aligns real CAS attempts; the cluster still
    // detects stale versions and retries the whole statement itself.
    cluster.synchronize_first_commits(4);

    thread::scope(|scope| {
        for _ in 0..4 {
            let cluster = cluster.clone();
            scope.spawn(move || {
                let mut session = cluster.session();
                assert_eq!(cluster_step(&mut session, "use test"), "OK");
                assert_eq!(
                    cluster_step(&mut session, "set @@session.tidb_retry_limit = 100"),
                    "OK"
                );
                assert_eq!(session.retry_limit(), 100);
                for _ in 0..20 {
                    // Source: `_, _ = tk.Exec(...)`.  The first worker to
                    // reach this may insert `(1, 1)` successfully; every
                    // later attempt sees the duplicate.  Neither outcome is
                    // the behavior under test, so retain both precisely.
                    let insert = cluster_step(&mut session, "insert into t_rollback values (1, 1)");
                    assert!(matches!(insert.as_str(), "OK" | "DuplicateKey"));
                    assert_eq!(
                        cluster_step(
                            &mut session,
                            "update t_rollback set c2 = c2 + 1 where c1 = 0",
                        ),
                        "OK"
                    );
                }
            });
        }
    });

    assert_eq!(
        cluster_step(&mut setup, "select c2 from t_rollback where c1 = 0"),
        "RS:80"
    );
    assert!(cluster.commit_conflicts() >= 3);
    assert!(cluster.retries() >= 3);
}

#[test]
fn shared_cluster_keeps_retry_limit_session_local() {
    let cluster = Cluster::new();
    let mut first = cluster.session();
    let mut second = cluster.session();

    assert_eq!(
        cluster_step(&mut first, "set @@session.tidb_retry_limit = 100"),
        "OK"
    );
    assert_eq!(first.retry_limit(), 100);
    assert_eq!(second.retry_limit(), 10);

    assert_eq!(
        cluster_step(&mut first, "create table session_local (id int)"),
        "OK"
    );
    assert_eq!(
        cluster_step(&mut second, "insert into session_local values (7)"),
        "OK"
    );
    assert_eq!(
        cluster_step(&mut first, "select id from session_local"),
        "RS:7"
    );
}

#[test]
fn shared_cluster_rejects_transaction_affecting_set_before_mutation() {
    let cluster = Cluster::new();
    let mut first = cluster.session();
    let mut second = cluster.session();
    assert_eq!(
        cluster_step(
            &mut first,
            "create table session_txn_gate (id int primary key)"
        ),
        "OK"
    );

    assert_eq!(
        cluster_step(&mut first, "set autocommit = 0"),
        "Unsupported(\"shared-session capability\")"
    );
    assert_eq!(first.retry_limit(), 10);
    assert_eq!(
        cluster_step(&mut second, "select id from session_txn_gate"),
        "RS:"
    );
    assert_eq!(
        cluster_step(&mut first, "set transaction isolation level read committed"),
        "Unsupported(\"shared-session capability\")"
    );
    assert_eq!(
        cluster_step(&mut second, "select id from session_txn_gate"),
        "RS:"
    );

    // Since `autocommit=0` was rejected before reaching Database, this DML
    // remains a normal shared autocommit publication, not a leaked local
    // transaction snapshot.
    assert_eq!(
        cluster_step(&mut first, "insert into session_txn_gate values (1)"),
        "OK"
    );
    assert_eq!(
        cluster_step(&mut second, "select id from session_txn_gate"),
        "RS:1"
    );
}

#[test]
fn shared_cluster_admits_only_immutable_noop_warning_set_lists() {
    let cluster = Cluster::new();
    let mut session = cluster.session();

    assert_eq!(
        session.execute_sql(
            "set tidb_enable_noop_functions = warn, tx_read_only = 1, transaction_read_only = 0",
        ),
        Ok(Outcome::Done)
    );
    assert_eq!(session.statement_status().previous().warnings.len(), 1);

    assert_eq!(
        session.execute_sql("set global tx_read_only = 1"),
        Err(ExecError::Unsupported("shared-session capability"))
    );
    assert_eq!(
        session.execute_sql("set tx_read_only = (@warning_capability_leak := 1)"),
        Err(ExecError::Unsupported("shared-session capability"))
    );
    assert!(!session.has_user_var("warning_capability_leak"));
    assert_eq!(
        session.execute_sql("set tidb_enable_noop_functions = concat('w', 'arn')"),
        Err(ExecError::Unsupported("shared-session capability"))
    );
    assert_eq!(
        session.execute_sql("set tidb_enable_noop_functions = default"),
        Err(ExecError::Unsupported("shared-session capability"))
    );
    assert_eq!(
        session.execute_sql("set tx_read_only = 0, autocommit = 0"),
        Err(ExecError::Unsupported("shared-session capability"))
    );
}

#[test]
fn shared_cluster_rejects_rc_backed_assignment_before_clone_or_retry() {
    let cluster = Cluster::new();
    let mut session = cluster.session();
    assert_eq!(
        cluster_step(
            &mut session,
            "create table capability_gate (id int primary key)"
        ),
        "OK"
    );

    // `Expr::Assign` mutates Database.user_vars through an Rc<RefCell<_>>.
    // A shallow Database clone would leak this write even if a later CAS
    // attempt were discarded, so the positive capability envelope must stop
    // the AST before any clone/evaluation happens.
    let assignment = tidb_parser::parse("insert into capability_gate values (@leak := 1)")
        .expect("parse assignment expression");
    assert!(matches!(
        session.run(&assignment),
        Err(ExecError::Unsupported("shared-session capability"))
    ));
    assert!(!session.has_user_var("leak"));
    assert_eq!(
        cluster_step(&mut session, "select id from capability_gate"),
        "RS:"
    );
}

#[test]
fn shared_cluster_admits_only_pure_order_by_expressions() {
    let cluster = Cluster::new();
    let mut session = cluster.session();
    assert_eq!(
        cluster_step(
            &mut session,
            "create table ordered_capability (id int primary key)"
        ),
        "OK"
    );
    assert_eq!(
        cluster_step(
            &mut session,
            "insert into ordered_capability values (2), (1)"
        ),
        "OK"
    );

    assert_eq!(
        session.execute_sql("select id from ordered_capability order by id"),
        Ok(Outcome::Rows(ResultSet {
            rows: vec![vec![Datum::Int(1)], vec![Datum::Int(2)]],
            ordered: true,
        }))
    );

    // ORDER BY crosses the same shallow-clone boundary as every other
    // expression position. Admitting deterministic reads must not let an
    // Rc-backed assignment mutate session state before CAS publication.
    assert_eq!(
        session.execute_sql(
            "select id from ordered_capability order by (@order_capability_leak := id)",
        ),
        Err(ExecError::Unsupported("shared-session capability"))
    );
    assert!(!session.has_user_var("order_capability_leak"));
}

#[test]
fn shared_cluster_admits_only_recursively_pure_between_operands() {
    let cluster = Cluster::new();
    let mut session = cluster.session();
    assert_eq!(
        cluster_step(
            &mut session,
            "create table between_capability (id int primary key)"
        ),
        "OK"
    );
    assert_eq!(
        cluster_step(
            &mut session,
            "insert into between_capability values (5), (20)"
        ),
        "OK"
    );

    assert_eq!(
        session.execute_sql("select id from between_capability where id between 1 and 10"),
        Ok(Outcome::Rows(ResultSet {
            rows: vec![vec![Datum::Int(5)]],
            ordered: false,
        }))
    );

    assert_eq!(
        session.execute_sql(
            "select id from between_capability where id between 1 and (@between_capability_leak := 10)",
        ),
        Err(ExecError::Unsupported("shared-session capability"))
    );
    assert!(!session.has_user_var("between_capability_leak"));
}

/// Bounded connection of Go's `countFunction` complete-mode contract
/// (`pkg/expression/aggregation/count.go`) to the shared Session. A table-less
/// SELECT still forms one input row: a non-NULL argument counts once, while a
/// NULL argument does not. Result metadata is derived before execution and is
/// never guessed from the returned Datum.
#[test]
fn shared_cluster_executes_tableless_count_with_typed_metadata() {
    let cluster = Cluster::new();
    let mut session = cluster.session();

    assert_eq!(cluster_step(&mut session, "select count(*)"), "RS:1");
    assert_eq!(cluster_step(&mut session, "select count(null)"), "RS:0");
    assert_eq!(
        cluster_step(&mut session, "select count(*) where false"),
        "RS:0"
    );

    let columns = session
        .resolve_query_result_columns(
            "select count(*) as counted",
            tidb_datatype::Collation::Utf8Mb4GeneralCi,
            "test",
        )
        .expect("table-less COUNT metadata");
    assert_eq!(columns.len(), 1);
    assert_eq!(columns[0].name, "counted");
    assert_eq!(columns[0].type_code, tidb_protocol::TYPE_LONGLONG);
    assert_eq!(columns[0].charset, 63);
    assert_eq!(columns[0].column_length, 21);
    assert_eq!(
        columns[0].flag,
        tidb_protocol::BINARY_FLAG | crate::result_metadata::NOT_NULL_FLAG
    );
    assert_eq!(columns[0].decimal, 0);

    // The argument purity check runs before Database is shallow-cloned. An
    // assignment would mutate Rc-backed user-variable state and therefore
    // cannot be smuggled through the newly admitted aggregate node.
    assert_eq!(
        cluster_step(&mut session, "select count(@count_leak := 1)"),
        "Unsupported(\"shared-session capability\")"
    );
    assert!(!session.has_user_var("count_leak"));

    cluster_step(&mut session, "create table count_input (v int)");
    assert_eq!(
        cluster_step(&mut session, "select count(*) from count_input"),
        "Unsupported(\"shared-session capability\")"
    );
}

/// Bounded live COUNT(column) closure. The qualified column is validated
/// against the catalog before the independently translated fixed COUNT type is
/// consumed, and execution uses the existing complete-mode runtime: NULL is
/// excluded and an empty input still produces one zero row. The planner-owned
/// descriptor remains a separate partial authority until a real plan carries
/// it into execution.
#[test]
fn shared_cluster_executes_one_bound_count_column_with_fixed_metadata() {
    let cluster = Cluster::new();
    let mut session = cluster.session();
    assert_eq!(
        cluster_step(
            &mut session,
            "create table count_column_input (a int, b int)"
        ),
        "OK"
    );

    assert_eq!(
        cluster_step(
            &mut session,
            "select count(t.a) as c from count_column_input as t"
        ),
        "RS:0"
    );
    assert_eq!(
        cluster_step(
            &mut session,
            "insert into count_column_input values (1, 10), (null, 20), (2, 30)"
        ),
        "OK"
    );
    assert_eq!(
        cluster_step(
            &mut session,
            "select count(t.a) as c from count_column_input as t"
        ),
        "RS:2"
    );
    assert_eq!(
        cluster_step(
            &mut session,
            "select count(count_column_input.a) as counted from count_column_input"
        ),
        "RS:2"
    );

    let columns = session
        .resolve_query_result_columns(
            "select count(t.a) as c from count_column_input as t",
            tidb_datatype::Collation::Utf8Mb4GeneralCi,
            "test",
        )
        .expect("bound COUNT(column) metadata");
    assert_eq!(columns.len(), 1);
    assert_eq!(columns[0].schema, "");
    assert_eq!(columns[0].table, "");
    assert_eq!(columns[0].org_table, "");
    assert_eq!(columns[0].name, "c");
    assert_eq!(columns[0].org_name, "");
    assert_eq!(columns[0].type_code, tidb_protocol::TYPE_LONGLONG);
    assert_eq!(columns[0].charset, 63);
    assert_eq!(columns[0].column_length, 21);
    assert_eq!(
        columns[0].flag,
        tidb_protocol::BINARY_FLAG | crate::result_metadata::NOT_NULL_FLAG
    );
    assert_eq!(columns[0].decimal, 0);
}

/// The bounded COUNT(column) seam is a positive capability, not a declaration
/// that aggregates or aggregate arguments are generally safe across the
/// shallow-clone retry adapter. Every adjacent shape remains closed.
#[test]
fn shared_cluster_count_column_rejects_unbound_or_stateful_shapes() {
    let cluster = Cluster::new();
    let mut session = cluster.session();
    assert_eq!(
        cluster_step(&mut session, "create table count_guard (a int, b int)"),
        "OK"
    );

    for sql in [
        "select count(*) as c from count_guard as t",
        "select count(distinct t.a) as c from count_guard as t",
        "select count(t.a) from count_guard as t",
        "select count(a) as c from count_guard as t",
        "select count(x.a) as c from count_guard as t",
        "select count(t.a) as c, t.b from count_guard as t",
        "select count(t.a + 1) as c from count_guard as t",
        "select sum(t.a) as c from count_guard as t",
        "select count(t.a) as c from count_guard as t join count_guard as u on t.a = u.a",
        "select count(t.a) as c from count_guard as t where t.a > 0",
        "select count(t.a) as c from count_guard as t group by t.b",
        "select count(t.a) as c from count_guard as t having c > 0",
        "select count(t.a) over () as c from count_guard as t",
        "select count(t.a) as c from count_guard as t order by c",
        "select count(t.a) as c from count_guard as t limit 1",
        "select count(t.a) as c from otherdb.count_guard as t",
        "select count(t.a + (@count_column_leak := 1)) as c from count_guard as t",
    ] {
        assert!(
            session.execute_sql(sql).is_err(),
            "unexpectedly admitted: {sql}"
        );
    }
    assert_eq!(
        session
            .execute_sql("select count(t.missing) as c from count_guard as t")
            .unwrap_err(),
        ExecError::UnknownColumn("missing".to_owned())
    );
    assert_eq!(session.statement_status().previous().row_count, -1);
    assert!(!session.has_user_var("count_column_leak"));
}

/// Source-backed by `difftests/corpus/table/drop_table.txt`: Go removes both
/// existing targets in `DROP TABLE ta, tb, tc`, then reports the missing
/// middle table. The held first CAS makes that erroring attempt stale, proving
/// the whole statement is re-evaluated and its error effects are published
/// only with the fresh catalog version.
#[test]
fn shared_cluster_publishes_and_retries_catalog_effects_returned_with_error() {
    let cluster = Cluster::new();
    let mut setup = cluster.session();
    assert_eq!(cluster_step(&mut setup, "create table ta (id int)"), "OK");
    assert_eq!(cluster_step(&mut setup, "create table tc (id int)"), "OK");

    let (control_tx, control_rx) = mpsc::sync_channel(1);
    thread::scope(|scope| {
        let worker_cluster = cluster.clone();
        let drop = scope.spawn(move || {
            // Session stays on its owning worker because Database deliberately
            // contains connection-local Rc state and is not Send.
            let mut dropper = worker_cluster.session();
            let control = dropper.hold_next_cas();
            control_tx.send(control).expect("send test CAS control");
            cluster_step(&mut dropper, "drop table ta, tb, tc")
        });
        let control = control_rx.recv().expect("receive test CAS control");
        control.wait_until_arrived();

        // Advance the cluster version after DROP evaluated but before it can
        // publish. Releasing the gate forces DROP through the ordinary stale
        // CAS/retry path; no result or effect is injected by the test hook.
        let mut competitor = cluster.session();
        let create = cluster_step(&mut competitor, "create table tr (id int)");
        control.release();
        assert_eq!(create, "OK");
        assert_eq!(drop.join().expect("drop worker"), "UnknownTable(\"tb\")");
    });

    assert_eq!(cluster.commit_conflicts(), 1);
    assert_eq!(cluster.retries(), 1);
    assert_eq!(
        cluster_step(&mut setup, "select id from ta"),
        "UnknownTable(\"ta\")"
    );
    assert_eq!(
        cluster_step(&mut setup, "select id from tc"),
        "UnknownTable(\"tc\")"
    );
    assert_eq!(cluster_step(&mut setup, "select id from tr"), "RS:");
}

#[test]
fn shared_cluster_does_not_publish_error_without_catalog_effects() {
    let cluster = Cluster::new();
    let mut first = cluster.session();
    let mut observer = cluster.session();
    assert_eq!(
        cluster_step(&mut first, "create table no_effect (id int primary key)"),
        "OK"
    );
    assert_eq!(
        cluster_step(&mut first, "insert into no_effect values (1)"),
        "OK"
    );

    let version_before_error = cluster.catalog_version();
    assert_eq!(
        cluster_step(&mut first, "insert into no_effect values (1)"),
        "DuplicateKey"
    );
    assert_eq!(cluster.catalog_version(), version_before_error);
    assert_eq!(
        cluster_step(&mut observer, "select id from no_effect"),
        "RS:1"
    );
}

/// Source-shaped `ExecuteStmt`/`ResetContextOfStmt`/`FinishExecuteStmt`
/// boundary on the real shared [`Session`] API.  The status snapshot is
/// connection-local even though catalog evaluation uses cloned retry
/// attempts: DDL publishes zero, DML publishes its affected count, SELECT
/// publishes `ROW_COUNT() = -1`, and an ordinary session command publishes
/// zero affected rows.  The duplicate DML also proves that an error
/// publishes a fresh zero rather than leaking the preceding successful DML.
#[test]
fn session_execute_sql_publishes_statement_status_on_success_and_error() {
    let cluster = Cluster::new();
    let mut session = cluster.session();

    assert_eq!(
        session.execute_sql("create table session_status (id int primary key)"),
        Ok(Outcome::Done)
    );
    let ddl = session.statement_status().previous();
    assert_eq!(ddl.affected_rows, 0);
    assert_eq!(ddl.row_count, 0);
    assert_eq!(ddl.last_insert_id, 0);
    assert!(ddl.warnings.is_empty());

    assert_eq!(
        session.execute_sql("insert into session_status values (1)"),
        Ok(Outcome::Done)
    );
    let insert = session.statement_status().previous();
    assert_eq!(insert.affected_rows, 1);
    assert_eq!(insert.row_count, 1);
    assert_eq!(insert.last_insert_id, 0);

    assert_eq!(
        session.execute_sql("select id from session_status"),
        Ok(Outcome::Rows(ResultSet {
            rows: vec![vec![tidb_datatype::Datum::Int(1)]],
            ordered: false,
        }))
    );
    let select = session.statement_status().previous();
    assert_eq!(select.affected_rows, 0);
    assert_eq!(select.row_count, -1);
    assert_eq!(select.last_insert_id, 0);

    assert_eq!(
        session.execute_sql("set @@session.tidb_retry_limit = 20"),
        Ok(Outcome::Done)
    );
    let session_command = session.statement_status().previous();
    assert_eq!(session_command.row_count, 0);
    assert_eq!(session_command.last_insert_id, 0);

    assert!(matches!(
        session.execute_sql("insert into session_status values (1)"),
        Err(ExecError::DuplicateKey)
    ));
    let failed_insert = session.statement_status().previous();
    assert_eq!(failed_insert.affected_rows, 0);
    assert_eq!(failed_insert.row_count, 0);
    assert_eq!(failed_insert.last_insert_id, 0);
    assert!(failed_insert.warnings.is_empty());
}
