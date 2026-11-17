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

//! Go master's materialized view DDL on a node with a DDL owner.

use super::*;

/// One statement's answer as the capture script prints it: `OK`, `RS:` and
/// the rows as JSON strings (`null` for NULL), or `ERR code message`.
fn answer(session: &mut Session, sql: &str) -> String {
    use crate::domain::resultset_source::ResultSetSource;
    let mut result = match session.node_conn().execute(sql) {
        Ok(result) => result,
        Err(error) => return format!("ERR {} {}", error.code, error.message),
    };
    let source = result.source();
    // An OK packet's statement has no result columns.
    let is_query = !source.field_types().is_empty();
    let mut rows = Vec::new();
    loop {
        match source.next_batch(8) {
            Ok(batch) if batch.is_empty() => break,
            Ok(batch) => rows.extend(batch),
            Err(error) => return format!("ERR {} {}", error.code, error.message),
        }
    }
    source.finish().expect("finish");
    source.close().expect("close");
    if !is_query {
        return "OK".to_owned();
    }
    let nulls: Vec<Vec<bool>> = rows.iter().map(|row| row.iter().map(Datum::is_null).collect()).collect();
    let cells: Vec<Vec<serde_json::Value>> = displayed(rows)
        .into_iter()
        .zip(nulls)
        .map(|(row, nulls)| {
            row.into_iter()
                .zip(nulls)
                .map(|(cell, null)| if null { serde_json::Value::Null } else { serde_json::Value::String(cell) })
                .collect()
        })
        .collect();
    format!("RS:{}", serde_json::to_string(&cells).unwrap())
}

/// Replays a Go master `eca4674a6f` node's capture (one `[sql, answer]`
/// per line) against a node with a DDL owner. Go master's `upgradeToVer316`
/// creates its materialized view tables; this branch's bootstrap does not, so
/// the replay creates them first.
fn replay_go_master(capture: &str) -> (UnistoreClusterStack, Session) {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(2131)).unwrap();
    for table in tidb_metadef::BOOTSTRAP_TABLES.iter().filter(|table| {
        matches!(
            table.name,
            "tidb_mview_refresh_info" | "tidb_mlog_purge_info" | "tidb_mview_refresh_hist" | "tidb_mview_refresh_alert" | "tidb_mlog_purge_hist"
        )
    }) {
        assert_eq!(answer(&mut session, table.create_sql), "OK", "{}", table.name);
    }
    let mut mismatches = Vec::new();
    for line in capture.lines() {
        let (sql, go): (String, String) = serde_json::from_str(line).expect("a captured statement");
        let rust = answer(&mut session, &sql);
        // The capture script's JSON spacing is Python's.
        let rows = |answer: &str| answer.strip_prefix("RS:").map(|rows| serde_json::from_str::<serde_json::Value>(rows).unwrap());
        let same = match (rows(&rust), rows(&go)) {
            (Some(rust), Some(go)) => rust == go,
            _ => rust == go,
        };
        if !same {
            mismatches.push(format!("{sql}\n  go:   {go}\n  rust: {rust}"));
        }
    }
    assert!(mismatches.is_empty(), "{}", mismatches.join("\n"));
    (stack, session)
}

/// CREATE MATERIALIZED VIEW LOG against a Go master `eca4674a6f` node's
/// answers (`testdata/mview_mlog_ddl_go.jsonl`): the log table Go builds
/// (`SHOW CREATE TABLE`), its purge schedule row, the log rows the base
/// table's DML then writes, and Go's refusals in Go's order.
#[test]
fn create_materialized_view_log_answers_as_go_master() {
    replay_go_master(include_str!("../../../../../testdata/mview_mlog_ddl_go.jsonl"));
}

/// DROP MATERIALIZED VIEW LOG, and Go master's refusal of DROP / TRUNCATE /
/// RENAME TABLE on a log and on its base table, against a Go master node's
/// answers (`testdata/mview_mlog_drop_go.jsonl`).
#[test]
fn drop_materialized_view_log_answers_as_go_master() {
    replay_go_master(include_str!("../../../../../testdata/mview_mlog_drop_go.jsonl"));
}

/// ALTER MATERIALIZED VIEW LOG ... PURGE against a Go master `eca4674a6f`
/// node's answers (`testdata/mview_mlog_alter_go.jsonl`): the next purge
/// time Go stores after the job (START WITH near now defers to NEXT, a NULL
/// schedule stores NULL, a missing row fails after the job), Go's checks in
/// Go's order, and the log's stored schedule as Go's table JSON keeps it.
/// Then Go's lock contention: while another transaction holds the purge
/// info row, the job still runs and the statement warns after waiting 10 s.
#[test]
fn alter_materialized_view_log_purge_answers_as_go_master() {
    let (stack, mut session) = replay_go_master(include_str!("../../../../../testdata/mview_mlog_alter_go.jsonl"));
    let stored = |stack: &UnistoreClusterStack| {
        let (_, mlog) = stack.factory.catalog_snapshot().table_info_by_name("e29", "$mlog$t").unwrap();
        let log = mlog.read().materialized_view_log.clone().expect("a log table");
        let log = log.read();
        (log.purge_method.clone(), log.purge_start_with.clone(), log.purge_next.clone(), log.purge_schedule_sql_mode)
    };
    // Go's `/schema/e29/$mlog$t` after the capture: the last ALTER ran
    // under ANSI_QUOTES.
    assert_eq!(stored(&stack), ("DEFERRED".to_owned(), String::new(), "TIMESTAMP '2040-01-01 00:00:00'".to_owned(), 4));

    let mut holder = stack.factory.open_session(session_context(2132)).unwrap();
    assert_eq!(answer(&mut holder, "use e29"), "OK");
    assert_eq!(answer(&mut holder, "begin pessimistic"), "OK");
    assert_eq!(answer(&mut holder, "select * from mysql.tidb_mlog_purge_info for update").get(..3), Some("RS:"));
    let started = std::time::Instant::now();
    assert_eq!(answer(&mut session, "alter materialized view log on t purge next TIMESTAMP '2041-01-01 00:00:00'"), "OK");
    assert!(started.elapsed() >= std::time::Duration::from_secs(9), "{:?}", started.elapsed());
    assert_eq!(
        answer(&mut session, "show warnings"),
        r#"RS:[["Warning","1105","alter materialized view log purge: metadata updated but failed to update mysql.tidb_mlog_purge_info.NEXT_PURGE_UNIX_SECONDS within 10s due to row lock contention; please retry later if immediate reschedule is needed"]]"#
    );
    assert_eq!(answer(&mut holder, "rollback"), "OK");
    assert_eq!(answer(&mut session, "select NEXT_PURGE_UNIX_SECONDS from mysql.tidb_mlog_purge_info"), r#"RS:[["2208988800"]]"#);
    assert_eq!(stored(&stack).2, "TIMESTAMP '2041-01-01 00:00:00'");
}
