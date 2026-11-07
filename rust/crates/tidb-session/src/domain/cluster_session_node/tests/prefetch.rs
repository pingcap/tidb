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

//! Go `TestPrefetchPointKeys4Update`, `TestPrefetchPointKeys4Delete`,
//! `TestPrefetchBatchPointGet` and `TestPrefetchPartitionTable`
//! (`pkg/server/conn_test.go`): a multi-statement query's point writes read
//! their keys in the prefetch's batch, so the statements themselves send no
//! point read to the store. Go counts the snapshot cache's hits; the store's
//! RPC client sees the same thing as reads that never leave the session.

use std::sync::{Arc, Mutex};

use crate::domain::cluster_session_node::DomainSessions as _;

use super::node_fixture::{rows, session_context};
use super::unistore_cop::cop_backed_stack;

/// The point reads (`Get` / `BatchGet` keys) the store receives while `run`
/// runs.
fn point_reads(stack: &crate::domain::unistore_node::UnistoreClusterStack, run: impl FnOnce()) -> usize {
    use tikv_client::proto::kvrpcpb;
    let keys = Arc::new(Mutex::new(0));
    let counted = Arc::clone(&keys);
    stack.rpc.set_send_hook(Some(Arc::new(move |request| {
        let any = request.as_any();
        let n = if let Some(get) = any.downcast_ref::<kvrpcpb::GetRequest>() {
            usize::from(!get.key.is_empty())
        } else if let Some(batch) = any.downcast_ref::<kvrpcpb::BatchGetRequest>() {
            batch.keys.len()
        } else {
            0
        };
        *counted.lock().unwrap() += n;
    })));
    run();
    stack.rpc.set_send_hook(None);
    let n = *keys.lock().unwrap();
    n
}

/// Go `handleQuery`: the query splits, the prefetch reads, then each
/// statement runs with its prefetched point plan (`PointPlanKey`), cleared
/// when the query is done.
fn handle_query(session: &mut crate::Session, query: &str) {
    let statements = session.split_statements(query, true).unwrap();
    session.node_conn().prefetch_point_plan_keys(&statements, query).unwrap();
    for (index, statement) in statements.iter().enumerate() {
        session.set_point_plan_value(Some(index));
        rows(session, statement);
    }
    session.set_point_plan_value(None);
}

/// Go `handleQuery` hands each statement of a multi-statement query the
/// point plan its prefetch built (`PointPlanKey`), and `Optimize` takes it
/// instead of `TryFastPlan`: the conversion warning the prefetch's
/// `TryFastPlan` raised went to the context the statement's
/// `ResetContextOfStmt` replaced, so a prefetched point UPDATE or DELETE
/// reports none. Outside a transaction nothing is prefetched and the
/// statement plans (and warns) itself. Captured from a Go tidb-server over
/// the wire (`sql_mode=''`): `[]`, `[]`, then two 1292 warnings; a point
/// SELECT answers the same.
#[test]
fn a_prefetched_point_write_takes_its_prefetched_plan_like_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(91)).expect("session opens");
    rows(&mut session, "use test");
    rows(&mut session, "create table t(id int primary key, v int, u varchar(10), unique key(u))");
    rows(&mut session, "insert into t values (1,1,'a'),(2,2,'b')");
    rows(&mut session, "set sql_mode=''");
    let warnings = |session: &mut crate::Session| text(session, "show warnings");

    rows(&mut session, "begin pessimistic");
    handle_query(&mut session, "update t set v=v+1 where id=2; update t set v=v+1 where id='1x'");
    assert!(warnings(&mut session).is_empty());
    rows(&mut session, "commit");

    rows(&mut session, "begin optimistic");
    handle_query(&mut session, "update t set v=v+1 where id=2; delete from t where id='2x'");
    assert!(warnings(&mut session).is_empty());
    rows(&mut session, "rollback");

    handle_query(&mut session, "update t set v=v+1 where id=2; update t set v=v+1 where id='1x'");
    assert_eq!(
        warnings(&mut session),
        ["Warning 1292 Truncated incorrect DOUBLE value: '1x'"; 2]
    );
    assert_eq!(text(&mut session, "select * from t order by id"), ["1 3 a", "2 4 b"]);

    // A point SELECT takes its prefetched plan too (captured likewise).
    rows(&mut session, "begin pessimistic");
    handle_query(&mut session, "select v from t where id=2; select v from t where id='1x'");
    assert!(warnings(&mut session).is_empty());
    rows(&mut session, "commit");
    handle_query(&mut session, "select v from t where id=2; select v from t where id='1x'");
    assert_eq!(
        warnings(&mut session),
        ["Warning 1292 Truncated incorrect DOUBLE value: '1x'"; 2]
    );
}

fn text(session: &mut crate::Session, sql: &str) -> Vec<String> {
    rows(session, sql)
        .into_iter()
        .map(|row| row.iter().map(crate::user_table::datum_text).collect::<Vec<_>>().join(" "))
        .collect()
}

#[test]
fn a_multi_statement_query_prefetches_its_point_writes_like_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(91)).expect("session opens");
    rows(&mut session, "use test");
    rows(&mut session, "set @@tidb_enable_clustered_index = 'INT_ONLY'");
    rows(&mut session, "create table prefetch (a int, b int, c int, primary key (a, b))");
    rows(&mut session, "insert prefetch values (1, 1, 1), (2, 2, 2), (3, 3, 3)");
    let query = "update prefetch set c = c + 1 where a = 1 and b = 1;\
                 update prefetch set c = c + 1 where a = 2 and b = 2;\
                 update prefetch set c = c + 1 where a = 3 and b = 3;";

    // Outside a transaction Go prefetches nothing.
    let statements = session.split_statements(query, true).unwrap();
    assert_eq!(point_reads(&stack, || session.node_conn().prefetch_point_plan_keys(&statements, query).unwrap()), 0);

    rows(&mut session, "begin optimistic");
    rows(&mut session, "update prefetch set c = c + 1 where a = 2 and b = 2");
    // A `use` statement anywhere in the query stops the prefetch.
    let with_use = format!("use test;{query}");
    let used = session.split_statements(&with_use, true).unwrap();
    assert_eq!(point_reads(&stack, || session.node_conn().prefetch_point_plan_keys(&used, &with_use).unwrap()), 0);
    let statements = session.split_statements(query, true).unwrap();
    // The prefetch reads the unique index keys, then the rows they name.
    let prefetched = point_reads(&stack, || session.node_conn().prefetch_point_plan_keys(&statements, query).unwrap());
    assert!(prefetched > 0);
    // The statements read nothing more from the store.
    assert_eq!(
        point_reads(&stack, || {
            for statement in &statements {
                rows(&mut session, statement);
            }
        }),
        0
    );
    rows(&mut session, "commit");
    assert_eq!(text(&mut session, "select * from prefetch"), ["1 1 2", "2 2 4", "3 3 4"]);

    // Without the prefetch each point write reads its own keys.
    rows(&mut session, "begin optimistic");
    assert!(
        point_reads(&stack, || {
            for statement in &statements {
                rows(&mut session, statement);
            }
        }) > 0
    );
    rows(&mut session, "rollback");

    // Pessimistic: the prefetch locks the keys, and the query's result is
    // Go's.
    rows(&mut session, "begin pessimistic");
    rows(&mut session, "update prefetch set c = c + 1 where a = 2 and b = 2");
    handle_query(&mut session, query);
    rows(&mut session, "commit");
    assert_eq!(text(&mut session, "select * from prefetch"), ["1 1 3", "2 2 6", "3 3 5"]);
}

#[test]
fn a_multi_statement_query_prefetches_batch_and_partitioned_deletes_like_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(92)).expect("session opens");
    rows(&mut session, "use test");
    rows(&mut session, "create table prefetch (a int primary key, b int)");
    rows(&mut session, "insert prefetch values (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)");
    rows(&mut session, "create table prefetch_p (a int primary key, b int) partition by hash(a) partitions 4");
    rows(&mut session, "insert prefetch_p values (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)");
    for (table, query) in [
        ("prefetch", "delete from prefetch where a in (2,3);delete from prefetch where a in (4,5);"),
        (
            "prefetch_p",
            "delete from prefetch_p where a = 2;delete from prefetch_p where a = 3;delete from prefetch_p where a in (4,5);",
        ),
    ] {
        rows(&mut session, "begin optimistic");
        rows(&mut session, &format!("delete from {table} where a = 1"));
        let statements = session.split_statements(query, true).unwrap();
        assert!(point_reads(&stack, || session.node_conn().prefetch_point_plan_keys(&statements, query).unwrap()) > 0);
        assert_eq!(
            point_reads(&stack, || {
                for statement in &statements {
                    rows(&mut session, statement);
                }
            }),
            0,
            "{table}"
        );
        rows(&mut session, "commit");
        assert!(text(&mut session, &format!("select * from {table}")).is_empty(), "{table}");
    }
}
