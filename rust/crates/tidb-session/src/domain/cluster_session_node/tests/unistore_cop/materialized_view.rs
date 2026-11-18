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
    // Go master bootstrap version 321's refresh-source table.
    assert_eq!(
        answer(&mut session, tidb_metadef::system_tables_def::CREATE_TI_DBMVIEW_FAST_REFRESH_SOURCE_INFO_TABLE),
        "OK"
    );
    let mut mismatches = Vec::new();
    for line in capture.lines() {
        let (sql, go): (String, String) = serde_json::from_str(line).expect("a captured statement");
        let mut rust = answer(&mut session, &sql);
        // A capture taken with `qj_ok.py` records the OK packet's affected
        // rows and info message (`OK <rows> <message>`).
        if rust == "OK" && go.starts_with("OK ") {
            rust = format!("OK {} {}", session.affected_rows(), session.statement_message()).trim_end().to_owned();
        }
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

/// CREATE MATERIALIZED VIEW against a Go master `eca4674a6f` node's answers
/// (`testdata/mview_create_go.jsonl`): the view table Go builds from the
/// SELECT's output types with its group key (`SHOW CREATE TABLE`), its
/// initial rows, the refresh rows, the refusals in Go's order, and the
/// guards on the view and its base table afterwards.
#[test]
fn create_materialized_view_answers_as_go_master() {
    let (stack, mut session) = replay_go_master(include_str!("../../../../../testdata/mview_create_go.jsonl"));
    // Go master `insertMViewRefreshSourceInfo`: each view registers its
    // base table's log; its refresh row has the build's read TS and no
    // schedule.
    assert_eq!(
        answer(
            &mut session,
            "select count(*), count(distinct MLOG_ID), min(MLOG_ID) = max(MLOG_ID) from mysql.tidb_mview_fast_refresh_source_info"
        ),
        r#"RS:[["5","1","1"]]"#
    );
    assert_eq!(
        answer(
            &mut session,
            "select count(*) from mysql.tidb_mview_refresh_info where LAST_SUCCESS_READ_TSO > 0 and LAST_SUCCESS_REFRESH_END_UNIX_SECONDS > 0 and NEXT_REFRESH_UNIX_SECONDS is null"
        ),
        r#"RS:[["5"]]"#
    );
    let catalog = stack.factory.catalog_snapshot();
    let table = |name: &str| catalog.table_info_by_name("e29", name).unwrap().1.read().clone_like_go();
    let (base, mlog) = (table("t"), table("$mlog$t"));
    // Go's `/schema/e29/mv1` and `/schema/e29/mv2` (its time zone is the
    // capturing node's own).
    for (name, sql) in [
        ("mv1", "SELECT `g`,COUNT(1),SUM(`w`) FROM `e29`.`t` GROUP BY `g`"),
        ("mv2", "SELECT `g`,`h`,COUNT(1),SUM(`v`),COUNT(`v`) FROM `e29`.`t` WHERE `v`>0 GROUP BY `g`,`h`"),
    ] {
        let mview = table(name);
        let info = mview.materialized_view.as_ref().expect("a materialized view").read().clone();
        assert_eq!(info.base_table_ids.iter().copied().collect::<Vec<_>>(), [base.id], "{name}");
        assert_eq!(info.sql_content, sql, "{name}");
        assert_eq!(info.refresh_method, "FAST", "{name}");
        assert_eq!(info.definition_sql_mode, 1436549152, "{name}");
        assert_eq!(info.refresh_schedule_sql_mode, 1436549152, "{name}");
        assert_eq!(info.definition_div_precision_increment, 4, "{name}");
        assert_eq!(info.init_build_state, tidb_model::table::MViewInitBuildState::READY, "{name}");
    }
    // The base table lists every view, and its log reads for each.
    let mview_ids: Vec<i64> = ["mv1", "mv2", "mv3", "mv24", "mv27"].iter().map(|name| table(name).id).collect();
    assert_eq!(
        answer(&mut session, &format!("select MLOG_ID = {} from mysql.tidb_mview_fast_refresh_source_info limit 1", mlog.id)),
        r#"RS:[["1"]]"#
    );
    let base_info = base.materialized_view_base.as_ref().expect("a base table").read().clone();
    assert_eq!(base_info.mlog_id, mlog.id);
    assert_eq!(base_info.mview_ids.iter().copied().collect::<Vec<_>>(), mview_ids);
    let log = mlog.materialized_view_log.as_ref().expect("a log").read().clone();
    assert_eq!(log.dependent_mview_ids.iter().copied().collect::<Vec<_>>(), mview_ids);
}

/// Go master `rollbackCreateMaterializedView` after a failed initial build
/// (`mockCreateMaterializedViewBuildErr`): the statement fails with the
/// build's error, and the view, its refresh rows and its entries in the base
/// table and the log are gone.
#[test]
fn create_materialized_view_rolls_back_a_failed_build() {
    let (stack, mut session) = replay_go_master(
        r#"["set tidb_mview_enable = ON", "OK"]
["create database e29", "OK"]
["use e29", "OK"]
["create table t (id int primary key, g int not null, v int)", "OK"]
["create materialized view log on t (g, v)", "OK"]"#,
    );
    {
        let _build_fails = tidb_failpoint::enable_for_test(
            "github.com/pingcap/tidb/pkg/ddl/mockCreateMaterializedViewBuildErr",
            "return(true)",
        );
        assert_eq!(
            answer(&mut session, "create materialized view mv (g, c) as select g, count(*) from t group by g"),
            "ERR 1105 mock create materialized view build error"
        );
    }
    assert_eq!(answer(&mut session, "show tables"), r#"RS:[["$mlog$t"],["t"]]"#);
    assert_eq!(answer(&mut session, "select count(*) from mysql.tidb_mview_refresh_info"), r#"RS:[["0"]]"#);
    assert_eq!(answer(&mut session, "select count(*) from mysql.tidb_mview_fast_refresh_source_info"), r#"RS:[["0"]]"#);
    let catalog = stack.factory.catalog_snapshot();
    let (_, base) = catalog.table_info_by_name("e29", "t").unwrap();
    let (_, mlog) = catalog.table_info_by_name("e29", "$mlog$t").unwrap();
    let base_info = base.read().materialized_view_base.clone().expect("the log's base");
    assert!(base_info.read().mview_ids.is_empty());
    let log = mlog.read().materialized_view_log.clone().expect("a log");
    assert!(log.read().dependent_mview_ids.is_empty());
    assert_eq!(answer(&mut session, "create materialized view mv (g, c) as select g, count(*) from t group by g"), "OK");
    assert_eq!(answer(&mut session, "select count(*) from mysql.tidb_mview_refresh_info"), r#"RS:[["1"]]"#);
}

/// ALTER and DROP MATERIALIZED VIEW against a Go master `eca4674a6f` node's
/// answers (`testdata/mview_drop_alter_go.jsonl`): COMMENT as the view's
/// ALTER TABLE, REFRESH with the next refresh time Go stores, ATTRIBUTES and
/// their refusals, DROP with its notes, the refresh rows going with the view
/// and the log droppable once no view reads it.
#[test]
fn alter_and_drop_materialized_view_answer_as_go_master() {
    replay_go_master(include_str!("../../../../../testdata/mview_drop_alter_go.jsonl"));
}

/// ALTER MATERIALIZED VIEW ... ATTRIBUTES / COMMENT as Go master's
/// `onAlterMaterializedViewAttributes` stores them: the capture up to its
/// combined COMMENT and ATTRIBUTES statement, then the view's metadata.
#[test]
fn alter_materialized_view_attributes_are_stored_as_go_master() {
    let capture = include_str!("../../../../../testdata/mview_drop_alter_go.jsonl");
    let end = capture.find("[\"alter materialized view mv1 comment = 'a'").expect("the combined statement");
    let line_end = end + capture[end..].find('\n').expect("a whole line");
    let (stack, _session) = replay_go_master(&capture[..line_end]);
    let (_, mview) = stack.factory.catalog_snapshot().table_info_by_name("e29", "mv1").unwrap();
    let mview = mview.read();
    assert_eq!(mview.comment, "a");
    let info = mview.materialized_view.as_ref().expect("a materialized view").read().clone();
    // The last ATTRIBUTES replaced the whole set: only the warning is left.
    assert_eq!((info.alert_warning_sec, info.alert_overdue_sec, info.alert_refresh_failed), (5, 0, false));
    // The REFRESH NEXT NULL left the schedule expression and the method.
    assert_eq!(info.refresh_method, "FAST");
    assert_eq!(info.refresh_next, "CAST(NULL AS DATETIME)");
}

/// REFRESH MATERIALIZED VIEW ... COMPLETE IN PLACE against a Go master
/// `eca4674a6f` node's answers (`testdata/mview_refresh_in_place_go.jsonl`):
/// the view recomputed, its history rows (success and failed), its refresh
/// row's read TSO, the refusals, and the refresh-failed alert.
#[test]
fn refresh_materialized_view_complete_in_place_answers_as_go_master() {
    replay_go_master(include_str!("../../../../../testdata/mview_refresh_in_place_go.jsonl"));
}

/// Go master's `onCreateMaterializedViewLog` on a materialized view: the
/// DDL session takes the view's refresh advisory lock (`mv_refresh_<schema
/// ID>_<view ID>`) for the job's delivery, so a session holding it (as a
/// refresh does) cancels the job with Go's message, and the log is created
/// once it is released. Replayed from a Go master node.
#[test]
fn create_materialized_view_log_on_a_view_takes_the_refresh_lock_as_go_master() {
    replay_go_master(include_str!("../../../../../testdata/mview_log_on_mview_lock_go.jsonl"));
}

/// Go `dropTableObject`: only a table object reads `ForeignKeyChecks` into
/// its job's `DropTableArgs.FKCheck`; a view, a sequence, a materialized view
/// and its log drop with it false whatever the session says (Go master and
/// the branch alike). The submitted args are gone once the job finishes, so
/// the job is read as it is delivered.
#[test]
fn drop_jobs_carry_fk_check_only_for_a_table_as_go() {
    let (_stack, mut session) = replay_go_master("");
    let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let _delivered = {
        let seen = std::sync::Arc::clone(&seen);
        tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/beforeDeliveryJob", move |job| {
            use tidb_model::ActionType as A;
            let job = job.downcast_mut::<tidb_model::Job>().unwrap();
            let drop = [
                A::ACTION_DROP_TABLE,
                A::ACTION_DROP_VIEW,
                A::ACTION_DROP_SEQUENCE,
                A::ACTION_DROP_MATERIALIZED_VIEW,
                A::ACTION_DROP_MATERIALIZED_VIEW_LOG,
            ];
            if !drop.contains(&job.type_) {
                return;
            }
            if let Ok(Some(args)) = tidb_model::get_drop_table_args(&mut job.clone()) {
                seen.lock().unwrap().push((job.type_, args.read().fk_check));
            }
        })
    };
    for sql in [
        "set tidb_mview_enable = ON",
        "set foreign_key_checks = ON",
        "create database e29",
        "use e29",
        "create table t (id int primary key, g int not null, v int)",
        "create view vw as select * from t",
        "create sequence sq",
        "create materialized view log on t (g, v)",
        "create materialized view mv (g, c) as select g, count(*) from t group by g",
        "drop view vw",
        "drop sequence sq",
        "drop materialized view mv",
        "drop materialized view log on t",
        "drop table t",
    ] {
        assert_eq!(answer(&mut session, sql), "OK", "{sql}");
    }
    use tidb_model::ActionType as A;
    let mut seen = seen.lock().unwrap().clone();
    seen.dedup();
    assert_eq!(
        seen,
        [
            (A::ACTION_DROP_VIEW, false),
            (A::ACTION_DROP_SEQUENCE, false),
            (A::ACTION_DROP_MATERIALIZED_VIEW, false),
            (A::ACTION_DROP_MATERIALIZED_VIEW_LOG, false),
            (A::ACTION_DROP_TABLE, true),
        ]
    );
}

/// Go master `JobNeedGC` / `insertJobIntoDeleteRangeTable`: DROP
/// MATERIALIZED VIEW and DROP MATERIALIZED VIEW LOG leave the dropped
/// table's key range for GC, as DROP TABLE does (`gc_delete_range`, or
/// `gc_delete_range_done` once deleted). Go's unistore deletes the ranges
/// asynchronously, so its answers race; the rule is Go's source.
#[test]
fn materialized_view_drops_leave_their_ranges_for_gc_as_go_master() {
    let (_stack, mut session) = replay_go_master("");
    let queued = |session: &mut Session, key: &str| {
        answer(
            session,
            &format!(
                "select (select count(*) from mysql.gc_delete_range where start_key = {key}) + (select count(*) from mysql.gc_delete_range_done where start_key = {key})"
            ),
        )
    };
    for sql in [
        "set tidb_mview_enable = ON",
        "create database e29",
        "use e29",
        "create table t (id int primary key, g int not null, v int)",
        "insert into t values (1, 1, 1)",
        "create materialized view log on t (g, v)",
        "create materialized view mv (g, c) as select g, count(*) from t group by g",
        "set @mv = (select lower(concat('74', lpad(hex(tidb_table_id ^ 0x8000000000000000), 16, '0'))) from information_schema.tables where table_schema = 'e29' and table_name = 'mv')",
        "set @mlog = (select lower(concat('74', lpad(hex(tidb_table_id ^ 0x8000000000000000), 16, '0'))) from information_schema.tables where table_schema = 'e29' and table_name = '$mlog$t')",
    ] {
        assert_eq!(answer(&mut session, sql), "OK", "{sql}");
    }
    assert_eq!(queued(&mut session, "@mv"), r#"RS:[["0"]]"#);
    assert_eq!(answer(&mut session, "drop materialized view mv"), "OK");
    assert_eq!(queued(&mut session, "@mv"), r#"RS:[["1"]]"#);
    assert_eq!(answer(&mut session, "drop materialized view log on t"), "OK");
    assert_eq!(queued(&mut session, "@mlog"), r#"RS:[["1"]]"#);
}

/// REFRESH MATERIALIZED VIEW ... COMPLETE OUT OF PLACE against a Go master
/// node (`testdata/mview_refresh_out_of_place_go.jsonl`): the view recomputed
/// into a shadow table that takes its name and ID (the old ID's refresh row
/// and sources move, its history row stays), the shadow and cutover jobs, a
/// second refresh, the refusals (a view with a log), and `CREATE TABLE ...
/// LIKE` a view or a logged base table as a plain table.
#[test]
fn refresh_materialized_view_complete_out_of_place_answers_as_go_master() {
    replay_go_master(include_str!("../../../../../testdata/mview_refresh_out_of_place_go.jsonl"));
}

/// Go `ResetContextOfStmt` classifies a statement before `Compile`: an
/// INSERT or UPDATE that fails in preprocessing (a missing table) leaves
/// `ROW_COUNT()` at 0, a failed SELECT at -1. Replayed from Go master
/// (`testdata/row_count_after_failed_statement_go.jsonl`).
#[test]
fn row_count_after_a_statement_failing_in_preprocess_answers_as_go() {
    replay_go_master(include_str!("../../../../../testdata/row_count_after_failed_statement_go.jsonl"));
}

/// Go master `TestMaterializedViewRefreshRuntime`'s failed attempt, for COMPLETE
/// IN PLACE (`mockRefreshMaterializedViewErrorAfterDataChanges`) and COMPLETE
/// OUT OF PLACE (`mockRefreshMaterializedViewOutOfPlaceBuildErrorAfterLoadShadow`):
/// the view keeps its rows and its success watermark, the history records the
/// failure, no shadow table is left, the advisory lock is free, and the next
/// refresh succeeds.
#[test]
fn a_failed_complete_refresh_rolls_back_as_go_master() {
    for (mode, point) in [
        ("complete in place", "mockRefreshMaterializedViewErrorAfterDataChanges"),
        ("complete out of place", "mockRefreshMaterializedViewOutOfPlaceBuildErrorAfterLoadShadow"),
    ] {
        let (_stack, mut session) = replay_go_master("");
        let mut run = |sql: &str| answer(&mut session, sql);
        for sql in [
            "set tidb_mview_enable = ON",
            "create database t",
            "use t",
            "create table src_runtime (id int primary key, g int not null, v int not null)",
            "insert into src_runtime values (1,1,10), (3,3,5)",
            "create materialized view log on src_runtime (id,g,v)",
            "create materialized view mv_runtime (g,s,cnt) refresh fast as select g,sum(v),count(*) from src_runtime group by g",
            "insert into src_runtime values (4,4,7)",
        ] {
            assert_eq!(run(sql), "OK", "{sql}");
        }
        assert_eq!(run(&format!("refresh materialized view mv_runtime {mode}")), "OK", "{mode}");
        let read = run("select last_success_read_tso from mysql.tidb_mview_refresh_info");
        assert_eq!(run("insert into src_runtime values (5,8,9)"), "OK");
        {
            let _failing =
                tidb_failpoint::enable_for_test(&format!("github.com/pingcap/tidb/pkg/executor/{point}"), r#"return("runtime failure")"#);
            let failed = run(&format!("refresh materialized view mv_runtime {mode}"));
            assert!(failed.starts_with("ERR ") && failed.contains("runtime failure"), "{mode}: {failed}");
        }
        assert_eq!(
            run("select * from mv_runtime order by g"),
            r#"RS:[["1","10","1"],["3","5","1"],["4","7","1"]]"#,
            "{mode}"
        );
        assert_eq!(run("select last_success_read_tso from mysql.tidb_mview_refresh_info"), read, "{mode}");
        assert_eq!(
            run("select refresh_status from mysql.tidb_mview_refresh_hist order by refresh_job_id"),
            r#"RS:[["success"],["failed"]]"#,
            "{mode}"
        );
        assert_eq!(
            run("select count(*) from information_schema.tables where table_schema='t' and table_name like '\\_\\_mv\\_shadow\\_%'"),
            r#"RS:[["0"]]"#,
            "{mode}"
        );
        assert_eq!(
            run("select is_free_lock(concat('mv_refresh_', (select schema_id from information_schema.ddl_jobs where job_type = 'create materialized view' order by job_id desc limit 1), '_', (select tidb_table_id from information_schema.tables where table_schema='t' and table_name='mv_runtime')))"),
            r#"RS:[["1"]]"#,
            "{mode}"
        );
        assert_eq!(run(&format!("refresh materialized view mv_runtime {mode}")), "OK", "{mode}");
        assert_eq!(
            run("select * from mv_runtime order by g"),
            r#"RS:[["1","10","1"],["3","5","1"],["4","7","1"],["8","9","1"]]"#,
            "{mode}"
        );
        assert_eq!(
            run("select refresh_status from mysql.tidb_mview_refresh_hist order by refresh_job_id"),
            r#"RS:[["success"],["failed"],["success"]]"#,
            "{mode}"
        );
    }
}

/// Go master's `_tidb_commit_ts` pseudo column: refused while
/// `tidb_mview_enable` is OFF (Go `preprocessor`), and otherwise each row's
/// commit timestamp (`ChunkDecoder.DecodeToChunk` fills `ExtraCommitTSID`
/// from the scanned version), readable in a SELECT, a WHERE and a DELETE's
/// filter, on handle and clustered tables. Replayed from a Go master node
/// (`testdata/commit_ts_column_go.jsonl`).
#[test]
fn commit_ts_column_answers_as_go_master() {
    replay_go_master(include_str!("../../../../../testdata/commit_ts_column_go.jsonl"));
}
