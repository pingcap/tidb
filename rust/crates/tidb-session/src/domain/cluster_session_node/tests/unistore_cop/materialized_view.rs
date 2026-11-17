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

/// ALTER MATERIALIZED VIEW LOG ... ADD COLUMN against a Go master
/// `eca4674a6f` node's answers (`testdata/mview_mlog_add_column_go.jsonl`):
/// each column added after the last tracked one (several as one
/// multi-schema change), typed from its base column, the rows already
/// logged reading Go's origin defaults, and the log's tracked columns.
#[test]
fn alter_materialized_view_log_add_column_answers_as_go_master() {
    let (stack, _session) = replay_go_master(include_str!("../../../../../testdata/mview_mlog_add_column_go.jsonl"));
    let (_, mlog) = stack.factory.catalog_snapshot().table_info_by_name("e29", "$mlog$t").unwrap();
    let mlog = mlog.read();
    let tracked: Vec<String> = mlog
        .materialized_view_log
        .as_ref()
        .expect("a log table")
        .read()
        .columns
        .iter()
        .map(|column| column.original().to_owned())
        .collect();
    assert_eq!(tracked, ["a", "s", "n", "e", "tx", "bn", "d", "ts", "id", "u", "k"]);
    // Go's `/schema/e29/$mlog$t`: name, Tp, Flag, Flen, origin default,
    // state and offset of each column.
    let columns: Vec<String> = mlog
        .columns
        .iter_deref()
        .map(|column| {
            let column = column.read();
            let origin = column.get_origin_default_value();
            let origin = match origin.view() {
                None => "None".to_owned(),
                Some(tidb_model::GoAnyView::String(bytes)) => String::from_utf8_lossy(bytes.as_bytes()).into_owned(),
                Some(other) => format!("{other:?}"),
            };
            format!(
                "{} {} {} {} {} {} {}",
                column.name.original(),
                column.field_type.code().mysql_type(),
                column.field_type.flags(),
                column.field_type.flen(),
                origin,
                column.state.0,
                column.offset
            )
        })
        .collect();
    assert_eq!(
        columns,
        [
            "a 3 0 11 None 5 0",
            "s 15 4097 10   5 1",
            "n 3 4097 11 0 5 2",
            "e 247 4097 1 x 5 3",
            "tx 252 4097 65535   5 4",
            "bn 254 4225 4 \0\0\0\0 5 5",
            "d 246 0 6 None 5 6",
            "ts 7 129 19 0000-00-00 00:00:00 5 7",
            "id 3 1 11 0 5 8",
            "u 3 0 11 None 5 9",
            "k 3 0 11 None 5 10",
            "_MLOG$_DML_TYPE 15 4097 1 None 5 11",
            "_MLOG$_OLD_NEW 1 4097 4 None 5 12",
        ]
    );
}

/// Go master `TestMLogOnlineDDLAddColumn`'s drop race: a base DROP COLUMN
/// that its executor admits while the log's ADD COLUMN of that column is
/// paused at write-reorganization waits for it (the log job involves the
/// base table) and is then cancelled by the owner's
/// `checkDropColumnWithMLogBaseConstraint`.
#[test]
fn base_drop_column_waits_for_and_is_refused_after_the_log_adds_it() {
    let (stack, mut session) = replay_go_master(
        r#"["set tidb_mview_enable = ON", "OK"]
["create database e29", "OK"]
["use e29", "OK"]
["create table t_drop_race (id int primary key, tracked int, added int)", "OK"]
["create materialized view log on t_drop_race (id)", "OK"]"#,
    );
    let (paused_tx, paused_rx) = std::sync::mpsc::channel::<()>();
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let release_rx = std::sync::Mutex::new(Some(release_rx));
    let paused_tx = std::sync::Mutex::new(paused_tx);
    let _pause = tidb_failpoint::enable_call_for_test(
        "github.com/pingcap/tidb/pkg/ddl/onAddColumnStateWriteReorg",
        move |_: &mut dyn std::any::Any| {
            if let Some(release) = release_rx.lock().unwrap().take() {
                paused_tx.lock().unwrap().send(()).unwrap();
                release.recv().unwrap();
            }
        },
    );
    let mut adder = stack.factory.open_session(session_context(2133)).unwrap();
    assert_eq!(answer(&mut adder, "use e29"), "OK");
    let add = std::thread::spawn(move || answer(&mut adder, "alter materialized view log on t_drop_race add column (added)"));
    paused_rx.recv_timeout(std::time::Duration::from_secs(30)).expect("the log's ADD COLUMN reaches write-reorganization");

    let mut dropper = stack.factory.open_session(session_context(2134)).unwrap();
    assert_eq!(answer(&mut dropper, "use e29"), "OK");
    let (dropped_tx, dropped_rx) = std::sync::mpsc::channel();
    let drop = std::thread::spawn(move || {
        dropped_tx.send(answer(&mut dropper, "alter table t_drop_race drop column added")).unwrap();
    });
    assert!(
        dropped_rx.recv_timeout(std::time::Duration::from_millis(500)).is_err(),
        "the base DROP COLUMN finished before the log's ADD COLUMN"
    );
    release_tx.send(()).unwrap();
    assert_eq!(add.join().unwrap(), "OK");
    let dropped = dropped_rx.recv_timeout(std::time::Duration::from_secs(30)).expect("the base DROP COLUMN finishes");
    drop.join().unwrap();
    assert_eq!(
        dropped,
        "ERR 8200 Unsupported ALTER TABLE on base table column added referenced by materialized view log"
    );
    assert_eq!(
        answer(
            &mut session,
            "select column_name from information_schema.columns where table_schema = 'e29' and table_name = '$mlog$t_drop_race' order by ordinal_position"
        ),
        r#"RS:[["id"],["added"],["_MLOG$_DML_TYPE"],["_MLOG$_OLD_NEW"]]"#
    );
}

/// Go master's ALTER TABLE / CREATE / DROP / ALTER INDEX, PARTITION BY and
/// EXCHANGE PARTITION refusals on a materialized view log and on its base
/// table (`checkAlterTableMaterializedViewConstraints` and friends) against
/// a Go master node's answers (`testdata/mview_guards_go.jsonl`).
#[test]
fn alter_table_on_a_log_and_its_base_answers_as_go_master() {
    replay_go_master(include_str!("../../../../../testdata/mview_guards_go.jsonl"));
}

/// MODIFY / CHANGE COLUMN of a base table with a materialized view log
/// against a Go master node's answers
/// (`testdata/mview_modify_column_go.jsonl`): a tracked column's no-reorg
/// change retypes the log's column (keeping the log's own key flags), and
/// Go's refusals (renaming, NULL to NOT NULL, a reorganizing change of a
/// tracked column) in Go's order.
#[test]
fn modify_column_of_a_logged_base_answers_as_go_master() {
    let (stack, _session) = replay_go_master(include_str!("../../../../../testdata/mview_modify_column_go.jsonl"));
    let (_, mlog) = stack.factory.catalog_snapshot().table_info_by_name("e29", "$mlog$t").unwrap();
    let columns: Vec<String> = mlog
        .read()
        .columns
        .iter_deref()
        .map(|column| {
            let column = column.read();
            format!(
                "{} {} {} {} {} {} {}",
                column.name.original(),
                column.field_type.code().mysql_type(),
                column.field_type.flags(),
                column.field_type.flen(),
                column.field_type.decimal(),
                column.field_type.charset_name(),
                column.field_type.collation_name()
            )
        })
        .collect();
    // Go's `/schema/e29/$mlog$t`.
    assert_eq!(
        columns,
        [
            "a 8 0 20 0 binary binary",
            "c 252 0 65535 0 utf8mb4 utf8mb4_bin",
            "n 8 0 20 0 binary binary",
            "_MLOG$_DML_TYPE 15 4097 1 0 utf8mb4 utf8mb4_bin",
            "_MLOG$_OLD_NEW 1 4097 4 0 binary binary",
        ]
    );
}

/// Go master `buildInvolvingSchemaInfoForMaterializedViewBaseModifyColumn`:
/// a MODIFY COLUMN of a logged base table involves the log, so a DROP
/// MATERIALIZED VIEW LOG waits until the change, which rewrites the log's
/// column, has finished.
#[test]
fn drop_log_waits_for_a_base_modify_column() {
    let (stack, mut session) = replay_go_master(
        r#"["set tidb_mview_enable = ON", "OK"]
["create database e29", "OK"]
["use e29", "OK"]
["create table t (id int primary key, a int)", "OK"]
["create materialized view log on t (a)", "OK"]"#,
    );
    let (paused_tx, paused_rx) = std::sync::mpsc::channel::<()>();
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let release_rx = std::sync::Mutex::new(Some(release_rx));
    let paused_tx = std::sync::Mutex::new(paused_tx);
    let _pause = tidb_failpoint::enable_call_for_test(
        "github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep",
        move |job: &mut dyn std::any::Any| {
            let job = job.downcast_mut::<tidb_model::Job>().expect("the step's job");
            if job.type_ != tidb_model::ActionType::ACTION_MODIFY_COLUMN {
                return;
            }
            if let Some(release) = release_rx.lock().unwrap().take() {
                paused_tx.lock().unwrap().send(()).unwrap();
                release.recv().unwrap();
            }
        },
    );
    let mut modifier = stack.factory.open_session(session_context(2135)).unwrap();
    assert_eq!(answer(&mut modifier, "use e29"), "OK");
    let modify = std::thread::spawn(move || answer(&mut modifier, "alter table t modify column a bigint"));
    paused_rx.recv_timeout(std::time::Duration::from_secs(30)).expect("the MODIFY COLUMN job starts");

    let mut dropper = stack.factory.open_session(session_context(2136)).unwrap();
    assert_eq!(answer(&mut dropper, "use e29"), "OK");
    let (dropped_tx, dropped_rx) = std::sync::mpsc::channel();
    let drop = std::thread::spawn(move || {
        dropped_tx.send(answer(&mut dropper, "drop materialized view log on t")).unwrap();
    });
    assert!(
        dropped_rx.recv_timeout(std::time::Duration::from_millis(500)).is_err(),
        "the DROP MATERIALIZED VIEW LOG finished before the base MODIFY COLUMN"
    );
    release_tx.send(()).unwrap();
    assert_eq!(modify.join().unwrap(), "OK");
    assert_eq!(dropped_rx.recv_timeout(std::time::Duration::from_secs(30)).expect("the drop finishes"), "OK");
    drop.join().unwrap();
    assert_eq!(answer(&mut session, "show tables"), r#"RS:[["t"]]"#);
}

/// CREATE MATERIALIZED VIEW's refusals against a Go master `eca4674a6f`
/// node's answers (`testdata/mview_create_checks_go.jsonl`): the planner's
/// SELECT build and column-list count, then the DDL's checks and
/// `validateCreateMaterializedViewQuery`, in Go's order.
#[test]
fn create_materialized_view_refusals_answer_as_go_master() {
    replay_go_master(include_str!("../../../../../testdata/mview_create_checks_go.jsonl"));
}
