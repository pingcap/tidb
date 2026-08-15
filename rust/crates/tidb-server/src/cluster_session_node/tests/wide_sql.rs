//! The wide-SQL surface this node exists for, over cluster storage: joins,
//! subqueries, aggregates and window functions, plus the two boundaries of
//! what a loaded catalog can answer -- a stored `ENUM`/`SET` column, and a
//! table the storage tier refuses to lay out.

use super::super::*;
use super::node_fixture::*;
use crate::resultset_source::ResultSetSource;
use tidb_datatype::Datum;
use tidb_protocol::PreparedValue;

fn execute_prepared_rows(
    session: &mut ClusterServerSession,
    statement: &PreparedGeneral,
    values: &[PreparedValue],
) -> Vec<Vec<Datum>> {
    let outcome = session
        .execute_general(statement, values)
        .expect("execute prepared statement");
    let GeneralExecuteOutcome::Rows(mut result) = outcome else {
        panic!("a prepared query must answer with rows");
    };
    let source = result.source();
    let mut rows = Vec::new();
    loop {
        let batch = source.next_batch(8).expect("batch");
        if batch.is_empty() {
            break;
        }
        rows.extend(batch);
    }
    source.finish().expect("finish");
    source.close().expect("close");
    rows
}

/// Go retains `PlanCacheStmt.PreparedAst`, so changing `sql_mode` after
/// PREPARE cannot re-lex the prepared text. Captured from Go TiDB:
///
/// ```sql
/// set sql_mode='';
/// prepare s from 'select "literal", ?';
/// set sql_mode='ANSI_QUOTES';
/// execute s using @p; -- literal, 7
/// ```
///
/// Re-parsing at EXECUTE would instead treat `"literal"` as an identifier.
#[test]
fn prepared_execution_uses_the_ast_parsed_at_prepare_time() {
    let (mut session, _) = open_session();
    session
        .execute_write("SET sql_mode = ''")
        .expect("default quote mode");
    let statement = session
        .prepare_general("SELECT \"literal\", ?")
        .expect("prepare");

    session
        .execute_write("SET sql_mode = 'ANSI_QUOTES'")
        .expect("change scanner mode after prepare");

    assert_eq!(
        execute_prepared_rows(
            &mut session,
            &statement,
            &[PreparedValue::SignedLongLong(7)],
        ),
        vec![vec![Datum::new_string("literal"), Datum::Int(7)]]
    );
}

#[test]
fn cluster_sessions_share_the_transaction_tier_advisory_lock_authority() {
    let node = MockNode::start();
    let mut first = open_session_on(&node);
    let mut second = open_session_on(&node);

    assert_eq!(
        first
            .session
            .run("SELECT GET_LOCK('cluster-lock', 0)")
            .unwrap(),
        StmtResult::Rows(vec![vec![Datum::Int(1)]])
    );
    assert_eq!(
        second
            .session
            .run("SELECT IS_USED_LOCK('CLUSTER-LOCK'), GET_LOCK('cluster-lock', 0)")
            .unwrap(),
        StmtResult::Rows(vec![vec![Datum::Int(1), Datum::Int(0)]])
    );

    drop(first);
    assert_eq!(
        second
            .session
            .run("SELECT GET_LOCK('cluster-lock', 0)")
            .unwrap(),
        StmtResult::Rows(vec![vec![Datum::Int(1)]])
    );
}

/// A `SELECT` over a stored `ENUM`/`SET` column answers with the element
/// NAME, the way MySQL prints it.
///
/// This is the shape that used to abort the SQL worker: the scan decoded
/// the row into an `Enum` datum and then panicked appending it to the
/// output chunk, so `SELECT ... FROM mysql.user` crashed the node rather
/// than answering. The row here is seeded the way a Go bootstrap seeds
/// `mysql.user`'s -- written into the committed store, never through this
/// node's own INSERT -- because that is the case that crashed.
#[test]
fn a_select_over_stored_enum_and_set_columns_answers_with_their_names() {
    let (mut session, cluster) = open_session();
    let row = tidb_tablecodec::encode_table_row(
        None,
        &[
            Datum::new_enum(
                tidb_datatype::MysqlEnum::new("Y", 2),
                tidb_datatype::Collation::Binary,
            ),
            Datum::new_set(
                tidb_datatype::MysqlSet::new("Select,Grant", 1 | 8),
                tidb_datatype::Collation::Binary,
            ),
        ],
        &[2, 3],
        true,
        None,
    )
    .expect("the seeded account row encodes");
    cluster.committed.lock().expect("committed").insert(
        tidb_tablecodec::table_key::encode_row_key_with_handle(
            104,
            &tidb_tablecodec::table_key::RecordHandle::Int(1),
        ),
        row,
    );

    let selected = rows(&mut session, "SELECT id, select_priv, table_priv FROM acct");
    assert_eq!(selected.len(), 1);
    assert_eq!(selected[0][0], Datum::Int(1));
    match (&selected[0][1], &selected[0][2]) {
        (Datum::Enum(member, _), Datum::Set(members, _)) => {
            assert_eq!(member.name_bytes(), b"Y");
            assert_eq!(member.value(), 2);
            assert_eq!(members.name_bytes(), b"Select,Grant");
            assert_eq!(members.value(), 9);
        }
        other => panic!("the ENUM/SET columns came back as {other:?}"),
    }
}

/// The catalog a session gets is the cluster's, minus exactly the tables
/// this tier cannot lay out -- and those are named, not hidden.
#[test]
fn an_unservable_table_is_refused_by_name() {
    let (session, _) = open_session();
    let skipped = session.skipped_tables();
    assert_eq!(skipped.len(), 1);
    assert_eq!(skipped[0].name, "app.t_pending");
    assert_eq!(
        skipped[0].reason,
        "its schema state is 0 rather than public"
    );
}

/// The wide-SQL surface this node exists for: a join, a subquery, an
/// aggregate with GROUP BY, and a window function, all over cluster
/// storage.
#[test]
fn wide_sql_runs_over_cluster_storage() {
    let (mut session, _) = open_session();
    session
        .execute_write("INSERT INTO t (id, v) VALUES (1, 10), (2, 20), (3, 30)")
        .expect("seed t");
    session
        .execute_write("INSERT INTO g (id, grp) VALUES (1, 100), (2, 100), (3, 200)")
        .expect("seed g");

    let joined = rows(
        &mut session,
        "SELECT t.id, g.grp FROM t JOIN g ON t.id = g.id ORDER BY t.id",
    );
    assert_eq!(joined.len(), 3);
    assert_eq!(joined[2], vec![Datum::Int(3), Datum::Int(200)]);

    let grouped = rows(
        &mut session,
        "SELECT g.grp, SUM(t.v) FROM t JOIN g ON t.id = g.id GROUP BY g.grp ORDER BY g.grp",
    );
    assert_eq!(grouped.len(), 2);

    let subquery = rows(
        &mut session,
        "SELECT id FROM t WHERE id IN (SELECT id FROM g WHERE grp = 200)",
    );
    assert_eq!(subquery, vec![vec![Datum::Int(3)]]);

    let windowed = rows(
        &mut session,
        "SELECT id, ROW_NUMBER() OVER (ORDER BY id) FROM t ORDER BY id",
    );
    assert_eq!(windowed.len(), 3);
    assert_eq!(windowed[0][1], Datum::Int(1));
}
