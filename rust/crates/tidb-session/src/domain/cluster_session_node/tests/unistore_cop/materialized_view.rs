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

mod alter;
mod basic;
mod casetest;
mod create;
mod drop;
mod log_alter;
mod log_basic;
mod log_create;
mod log_drop;
mod log_output;
mod log_purge;
mod mdl;
mod mlog_write;
mod refresh;
mod refresh_entry;
mod refresh_operator;
mod refresh_source;
mod select_lock;
mod show;

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
/// per line) against a node with a DDL owner. An answer a later Go master
/// commit changed carries that commit's text (`27b1b52535f`: CREATE
/// MATERIALIZED VIEW names a "source object", and a view is a valid source). Go master's `upgradeToVer316`
/// creates its materialized view tables; this branch's bootstrap does not, so
/// the replay creates them first.
fn replay_go_master(capture: &str) -> (UnistoreClusterStack, Session) {
    let (stack, _users, session) = go_master_mview_stack();
    replay_on(stack, session, capture)
}

/// A node with Go master's materialized view system tables and a root
/// session on it.
fn go_master_mview_stack() -> (UnistoreClusterStack, Arc<ConfiguredUserStore>, Session) {
    let (stack, users) = cop_backed_stack();
    let session = go_master_mview_tables(&stack);
    (stack, users, session)
}

/// Go master's materialized view system tables on `stack`, and the root
/// session that created them.
fn go_master_mview_tables(stack: &UnistoreClusterStack) -> Session {
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
    session
}

fn replay_on(stack: UnistoreClusterStack, session: Session, capture: &str) -> (UnistoreClusterStack, Session) {
    replay_on_with_ids(stack, session, capture, &[])
}

/// [`replay_on`] where Go's answers carry table IDs: each `(go_id, schema,
/// table)` the node's catalog resolves reads as this node's ID.
fn replay_on_with_ids(
    stack: UnistoreClusterStack,
    session: Session,
    capture: &str,
    ids: &[(i64, &str, &str)],
) -> (UnistoreClusterStack, Session) {
    replay_on_masked(stack, session, capture, ids, &|answer| answer.to_owned())
}

/// [`replay_on_with_ids`] comparing both answers through `mask`, which
/// blanks what differs by wall-clock time alone.
fn replay_on_masked(
    stack: UnistoreClusterStack,
    mut session: Session,
    capture: &str,
    ids: &[(i64, &str, &str)],
    mask: &dyn Fn(&str) -> String,
) -> (UnistoreClusterStack, Session) {
    let mut mismatches = Vec::new();
    for line in capture.lines() {
        let (sql, mut go): (String, String) = serde_json::from_str(line).expect("a captured statement");
        for &(go_id, schema, table) in ids {
            if let Some((_, info)) = stack.factory.catalog_snapshot().table_info_by_name(schema, table) {
                go = go.replace(&format!("\"{go_id}\""), &format!("\"{}\"", info.read().id));
            }
        }
        let mut rust = answer(&mut session, &sql);
        // A capture taken with `qj_ok.py` records the OK packet's affected
        // rows and info message (`OK <rows> <message>`).
        if rust == "OK" && go.starts_with("OK ") {
            rust = format!("OK {} {}", session.affected_rows(), session.statement_message()).trim_end().to_owned();
        }
        // The capture script's JSON spacing is Python's.
        let rows = |answer: &str| answer.strip_prefix("RS:").map(|rows| serde_json::from_str::<serde_json::Value>(rows).unwrap());
        let (rust, go) = (mask(&rust), mask(&go));
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

/// Go master `TestCreateMaterializedViewRefreshInfoRunningAndSuccess`:
/// while the initial build runs, the refresh-info row already holds the
/// build's read TSO, ANALYZE (`buildAnalyzeTable`'s `CheckMViewReadable`)
/// and both refresh forms refuse the view, and no refresh source is
/// registered; once the build finishes, the view reads, its source is
/// registered, ANALYZE runs and the refresh-info TSO moved forward.
#[test]
fn test_create_materialized_view_refresh_info_running_and_success() {
    let (stack, _users, mut root) = go_master_mview_stack();
    mv_setup(&mut root, &[
        "use test",
        "set tidb_mview_enable = ON",
        "create table t (a int not null, b int not null)",
        "insert into t values (1, 10), (1, 5), (2, 7)",
        "create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)",
    ]);
    let pause = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/ddl/pauseCreateMaterializedViewBuild", "pause");
    let mut ddl = stack.factory.open_session(session_context(2140)).unwrap();
    mv_setup(&mut ddl, &["use test", "set tidb_mview_enable = ON"]);
    let ddl_done = std::thread::spawn(move || {
        answer(
            &mut ddl,
            "create materialized view mv_state (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a",
        )
    });

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    let (mview_id, init_ts) = loop {
        let rows = answer(&mut root, "select MVIEW_ID, LAST_SUCCESS_READ_TSO from mysql.tidb_mview_refresh_info");
        let rows: Vec<Vec<Option<String>>> = serde_json::from_str(rows.strip_prefix("RS:").unwrap_or("[]")).unwrap_or_default();
        if let [row] = rows.as_slice() {
            if let (Some(Ok(id)), Some(Ok(ts))) =
                (row[0].as_deref().map(str::parse::<i64>), row[1].as_deref().map(str::parse::<u64>))
            {
                if id != 0 && ts != 0 {
                    break (id, ts);
                }
            }
        }
        assert!(std::time::Instant::now() < deadline, "the initial build never registered its refresh info");
        std::thread::sleep(std::time::Duration::from_millis(100));
    };

    let refused = answer(&mut root, "analyze table mv_state");
    assert!(refused.contains("initial build is in progress"), "{refused}");
    for sql in ["refresh materialized view mv_state fast", "refresh materialized view mv_state complete in place"] {
        let refused = answer(&mut root, sql);
        assert!(refused.contains("initial build is in progress"), "{sql}: {refused}");
    }
    assert_eq!(
        answer(&mut root, &format!("select count(*) from mysql.tidb_mview_fast_refresh_source_info where MVIEW_ID = {mview_id}")),
        r#"RS:[["0"]]"#
    );
    drop(pause);
    assert_eq!(ddl_done.join().unwrap(), "OK");
    assert_eq!(answer(&mut root, "select a, s, cnt from mv_state order by a"), r#"RS:[["1","15","2"],["2","7","1"]]"#);
    assert_eq!(
        answer(&mut root, &format!("select count(*) from mysql.tidb_mview_fast_refresh_source_info where MVIEW_ID = {mview_id}")),
        r#"RS:[["1"]]"#
    );
    assert_eq!(answer(&mut root, "analyze table mv_state"), "OK");
    let final_ts = answer(&mut root, &format!("select LAST_SUCCESS_READ_TSO from mysql.tidb_mview_refresh_info where MVIEW_ID = {mview_id}"));
    let final_ts: u64 = serde_json::from_str::<Vec<Vec<String>>>(final_ts.strip_prefix("RS:").unwrap()).unwrap()[0][0].parse().unwrap();
    assert!(final_ts > init_ts, "{final_ts} > {init_ts}");
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
/// asynchronously, so its answers race; the rule is Go's source. The worker
/// (`delRange.doTask`) moves a range's start key past each batch it deletes
/// (`UpdateDeleteRange`) and records the finished range under that key, so
/// the range is found by the table's prefix, not by its first key.
#[test]
fn materialized_view_drops_leave_their_ranges_for_gc_as_go_master() {
    let (_stack, mut session) = replay_go_master("");
    let queued = |session: &mut Session, key: &str| {
        answer(
            session,
            &format!(
                "select (select count(*) from mysql.gc_delete_range where start_key like concat({key}, '%')) + (select count(*) from mysql.gc_delete_range_done where start_key like concat({key}, '%'))"
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

/// Go master's `PurgeMaterializedViewLogExec`: the target's errors, the
/// purge-info checkpoint, one history row per purge (none when the cutoff
/// fence skips it, a failed one when the dependent views' refresh info is
/// missing), the safe TSO bounded by each dependent view's last read, and
/// the OK packet's deleted count. Replayed from a Go master node
/// (`testdata/mlog_purge_go.jsonl`).
#[test]
fn purge_materialized_view_log_answers_as_go_master() {
    replay_go_master(include_str!("../../../../../testdata/mlog_purge_go.jsonl"));
}

/// Go master's lock conflicts, captured on two connections of a Go master
/// node (`tools/e29/p15/conflict.py`): while another transaction holds the
/// purge-info row, PURGE fails with the CAUSE on the wire (Go
/// `errors.Cause(errors.Annotatef(errMLogPurgeLockConflict, ...))`) and the
/// whole annotated text in its failed history row; while another session
/// holds the view's refresh advisory lock, both COMPLETE refreshes fail with
/// Go master's `ErrMVAdvisoryLockConflict` (8182, `0a914c5b134`), whose
/// `Error()` text the history row keeps.
#[test]
fn purge_and_refresh_lock_conflicts_answer_as_go_master() {
    let (stack, mut session) = replay_go_master(include_str!("../../../../../testdata/mlog_purge_conflict_setup_go.jsonl"));
    let mut holder = stack.factory.open_session(session_context(2132)).unwrap();
    assert_eq!(answer(&mut holder, "use e29"), "OK");
    assert_eq!(answer(&mut holder, "begin"), "OK");
    assert_eq!(answer(&mut holder, "select count(*) from mysql.tidb_mlog_purge_info for update"), r#"RS:[["1"]]"#);
    assert_eq!(answer(&mut session, "purge materialized view log on t"), "ERR 1105 mlog purge lock conflict");
    assert_eq!(answer(&mut holder, "rollback"), "OK");
    assert_eq!(
        answer(&mut session, "select PURGE_STATUS, PURGE_ROWS, PURGE_FAILED_REASON, PURGE_CUTOFF_TSO from mysql.tidb_mlog_purge_hist"),
        r#"RS:[["failed","0","another purge is running for materialized view log on e29.t, please retry later: mlog purge lock conflict",null]]"#
    );
    assert_eq!(answer(&mut session, "select count(*) from `$mlog$t`"), r#"RS:[["1"]]"#);
    assert_eq!(answer(&mut session, "purge materialized view log on t"), "OK");
    assert_eq!(
        answer(&mut session, "select PURGE_STATUS, PURGE_ROWS from mysql.tidb_mlog_purge_hist order by PURGE_JOB_ID"),
        r#"RS:[["failed","0"],["success","1"]]"#
    );

    assert_eq!(answer(&mut session, "create materialized view mv (v, c) as select v, count(*) from t group by v"), "OK");
    let (_, mv) = stack.factory.catalog_snapshot().table_info_by_name("e29", "mv").unwrap();
    let lock = {
        let mv = mv.read();
        format!("mv_refresh_{}_{}", mv.db_id, mv.id)
    };
    assert_eq!(answer(&mut session, &format!("select get_lock('{lock}', 10)")), r#"RS:[["1"]]"#);
    assert_eq!(
        answer(&mut holder, "refresh materialized view mv complete in place"),
        "ERR 8182 materialized view advisory lock conflict: another refresh or materialized view log creation is running for materialized view e29.mv, please retry later"
    );
    assert_eq!(answer(&mut holder, "select row_count()"), r#"RS:[["0"]]"#);
    assert_eq!(
        answer(&mut holder, "refresh materialized view mv complete out of place"),
        "ERR 8182 materialized view advisory lock conflict: another refresh or materialized view log creation is running for materialized view e29.mv, please retry later"
    );
    assert_eq!(answer(&mut session, &format!("select release_lock('{lock}')")), r#"RS:[["1"]]"#);
    let reason = "[executor:8182]materialized view advisory lock conflict: another refresh or materialized view log creation is running for materialized view e29.mv, please retry later";
    assert_eq!(
        answer(&mut session, "select REFRESH_METHOD, REFRESH_STATUS, REFRESH_FAILED_REASON from mysql.tidb_mview_refresh_hist order by REFRESH_JOB_ID"),
        format!(r#"RS:[["complete in place manual","failed","{reason}"],["complete out of place manual","failed","{reason}"]]"#)
    );
}

/// Go master's `CancelMaterializedViewJobExec`: no running job, a running
/// job whose view or log the latest schema cannot resolve, the request
/// recorded once with its requester, and a second request refused.
/// Replayed from a Go master node (`testdata/mview_cancel_job_go.jsonl`).
#[test]
fn cancel_materialized_view_job_answers_as_go_master() {
    replay_go_master(include_str!("../../../../../testdata/mview_cancel_job_go.jsonl"));
}

/// Go master's `TestCancelMaterializedViewLogPurgeJob` /
/// `TestPurgeMaterializedViewLogCancelWatcherUsesHistRequest` /
/// `TestPurgeMaterializedViewLogRunningHistHeartbeat` setup: a purge paused
/// after its running history row, on its own session and thread.
struct PausedPurge {
    stack: UnistoreClusterStack,
    users: Arc<ConfiguredUserStore>,
    root: Session,
    mlog_id: i64,
    job_id: String,
    purge: std::thread::JoinHandle<String>,
    _poll: tidb_failpoint::Guard,
    _heartbeat: Option<tidb_failpoint::Guard>,
}

const PAUSE_PURGE: &str = "github.com/pingcap/tidb/pkg/executor/pausePurgeMaterializedViewLogAfterInsertPurgeHistRunning";

fn paused_purge(table: &str, heartbeat: bool) -> PausedPurge {
    let (stack, users, mut root) = go_master_mview_stack();
    for sql in [
        "use test".to_owned(),
        "set tidb_mview_enable = ON".to_owned(),
        format!("create table {table} (id int primary key, v int not null)"),
        format!("create materialized view log on {table} (id, v) purge next date_add(now(), interval 1 hour)"),
        format!("insert into {table} values (1, 10)"),
    ] {
        assert_eq!(answer(&mut root, &sql).get(..2), Some("OK"), "{sql}");
    }
    let (_, mlog) = stack.factory.catalog_snapshot().table_info_by_name("test", &format!("$mlog${table}")).unwrap();
    let mlog_id = mlog.read().id;
    let poll = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/executor/mockMVTaskMonitorPollInterval", "return(50)");
    let heartbeat = heartbeat.then(|| {
        tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/executor/mockMVTaskHistHeartbeatInterval", "return(50)")
    });
    tidb_failpoint::enable(PAUSE_PURGE, "pause").unwrap();
    let mut purge_session = stack.factory.open_session(session_context(2140)).unwrap();
    let statement = format!("purge materialized view log on {table}");
    let purge = std::thread::spawn(move || {
        assert_eq!(answer(&mut purge_session, "use test"), "OK");
        answer(&mut purge_session, &statement)
    });
    let running =
        format!("select count(*) from mysql.tidb_mlog_purge_hist where MLOG_ID = {mlog_id} and PURGE_STATUS = 'running'");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while answer(&mut root, &running) != r#"RS:[["1"]]"# {
        assert!(std::time::Instant::now() < deadline, "the purge never reached its running history row");
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    let job = answer(
        &mut root,
        &format!(
            "select PURGE_JOB_ID from mysql.tidb_mlog_purge_hist where MLOG_ID = {mlog_id} and PURGE_STATUS = 'running' order by PURGE_JOB_ID desc limit 1"
        ),
    );
    let job_id = serde_json::from_str::<Vec<Vec<String>>>(job.strip_prefix("RS:").unwrap()).unwrap()[0][0].clone();
    PausedPurge { stack, users, root, mlog_id, job_id, purge, _poll: poll, _heartbeat: heartbeat }
}

/// Go master's `waitMVTaskCancelWatcherRequested`: a receiver that fires
/// when a monitor named with `prefix` observes the cancel request.
fn wait_cancel_watcher_requested(prefix: &'static str) -> (std::sync::mpsc::Receiver<()>, tidb_failpoint::Guard) {
    let (sender, receiver) = std::sync::mpsc::channel();
    let sender = std::sync::Mutex::new(Some(sender));
    let guard = tidb_failpoint::enable_call_for_test(
        "github.com/pingcap/tidb/pkg/executor/mvTaskCancelWatcherRequested",
        move |name: &mut dyn std::any::Any| {
            if name.downcast_ref::<String>().is_some_and(|name| name.starts_with(prefix)) {
                if let Some(sender) = sender.lock().unwrap().take() {
                    let _ = sender.send(());
                }
            }
        },
    );
    (receiver, guard)
}

fn last_purge_hist(root: &mut Session, mlog_id: i64) -> String {
    answer(
        root,
        &format!(
            "select PURGE_STATUS, PURGE_METHOD, PURGE_END_TIME is not null, PURGE_FAILED_REASON from mysql.tidb_mlog_purge_hist where MLOG_ID = {mlog_id} order by PURGE_JOB_ID desc limit 1"
        ),
    )
}

/// Go master's `TestCancelMaterializedViewLogPurgeJob`: CANCEL needs
/// OPERATE VIEW on the log; once granted, the monitor sees the request, the
/// purge fails as canceled manually and its history row names who canceled
/// it.
#[test]
fn test_cancel_materialized_view_log_purge_job() {
    let PausedPurge { stack, users, mut root, mlog_id, job_id, purge, _poll, _heartbeat } =
        paused_purge("t_purge_cancel_job", false);
    assert_eq!(answer(&mut root, "create user 'mv_purge_cancel_u'@'%' identified by 'abc'"), "OK");
    let identity = users
        .authenticate_native(
            "mv_purge_cancel_u",
            "127.0.0.1",
            &super::super::node_fixture::SALT,
            &super::super::node_fixture::scramble(b"abc", &super::super::node_fixture::SALT),
        )
        .expect("mv_purge_cancel_u logs in");
    let mut cancel =
        stack.factory.open_session(super::super::node_fixture::session_context_with_identity(2141, identity)).unwrap();
    let denied = answer(&mut cancel, &format!("cancel materialized view log purge job {job_id}"));
    assert!(denied.contains("OPERATE VIEW command denied"), "{denied}");
    assert_eq!(
        answer(&mut root, "grant operate view on test.`$mlog$t_purge_cancel_job` to 'mv_purge_cancel_u'@'%'"),
        "OK"
    );
    let (requested, _watch) = wait_cancel_watcher_requested("mlog-purge-");
    assert_eq!(answer(&mut cancel, &format!("cancel materialized view log purge job {job_id}")), "OK");
    requested.recv_timeout(std::time::Duration::from_secs(10)).expect("the purge's monitor observes the request");
    tidb_failpoint::disable(PAUSE_PURGE);
    let purged = purge.join().unwrap();
    assert!(purged.contains("materialized view task canceled manually"), "{purged}");
    assert_eq!(
        last_purge_hist(&mut root, mlog_id),
        r#"RS:[["failed","manual","1","cancelled manually by 'mv_purge_cancel_u'@'%'"]]"#
    );
}

/// Go master's `TestPurgeMaterializedViewLogCancelWatcherUsesHistRequest`:
/// the monitor reads the history row's request and its requester, whoever
/// wrote it.
#[test]
fn test_purge_materialized_view_log_cancel_watcher_uses_hist_request() {
    let PausedPurge { mut root, mlog_id, purge, _poll, _heartbeat, .. } = paused_purge("t_purge_cancel_watch", false);
    let (requested, _watch) = wait_cancel_watcher_requested("mlog-purge-");
    let request = format!(
        "UPDATE mysql.tidb_mlog_purge_hist SET CANCEL_REQUEST_TIME = NOW(6), CANCEL_REQUESTED_BY = {} WHERE MLOG_ID = {mlog_id} AND PURGE_STATUS = 'running' AND CANCEL_REQUEST_TIME IS NULL",
        "'''purge_watcher_req''@''stage-d'''"
    );
    assert_eq!(answer(&mut root, &request), "OK");
    requested.recv_timeout(std::time::Duration::from_secs(10)).expect("the purge's monitor observes the request");
    tidb_failpoint::disable(PAUSE_PURGE);
    let purged = purge.join().unwrap();
    assert!(purged.contains("materialized view task canceled manually"), "{purged}");
    assert_eq!(
        last_purge_hist(&mut root, mlog_id),
        r#"RS:[["failed","manual","1","cancelled manually by 'purge_watcher_req'@'stage-d'"]]"#
    );
    assert_eq!(
        answer(
            &mut root,
            &format!("select count(*) from mysql.tidb_mlog_purge_hist where MLOG_ID = {mlog_id} and PURGE_STATUS = 'running'")
        ),
        r#"RS:[["0"]]"#
    );
}

/// Go master's `TestPurgeMaterializedViewLogRunningHistHeartbeat`: the
/// monitor moves the running row's heartbeat, and the purge then succeeds.
#[test]
fn test_purge_materialized_view_log_running_hist_heartbeat() {
    let PausedPurge { mut root, mlog_id, purge, _poll, _heartbeat, .. } = paused_purge("t_purge_heartbeat", true);
    let heartbeat = format!(
        "select cast(LAST_HEARTBEAT_TIME as char) from mysql.tidb_mlog_purge_hist where MLOG_ID = {mlog_id} and PURGE_STATUS = 'running' order by PURGE_JOB_ID desc limit 1"
    );
    let first = answer(&mut root, &heartbeat);
    assert!(first.starts_with(r#"RS:[[""#), "{first}");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while answer(&mut root, &heartbeat) == first {
        assert!(std::time::Instant::now() < deadline, "the heartbeat never moved");
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    tidb_failpoint::disable(PAUSE_PURGE);
    assert_eq!(purge.join().unwrap(), "OK");
    assert_eq!(
        answer(
            &mut root,
            &format!(
                "select PURGE_STATUS, LAST_HEARTBEAT_TIME is not null from mysql.tidb_mlog_purge_hist where MLOG_ID = {mlog_id} order by PURGE_JOB_ID desc limit 1"
            )
        ),
        r#"RS:[["success","1"]]"#
    );
}

const PAUSE_REFRESH: &str = "github.com/pingcap/tidb/pkg/executor/pauseRefreshMaterializedViewAfterInsertRefreshHistRunning";

/// Go master's `TestMaterializedViewRefreshCancellation` (its subtests
/// refresh by DELTA APPLY; IN PLACE and OUT OF PLACE share the monitor): a refresh paused after its running history row, canceled by a
/// user once granted OPERATE VIEW on the view, fails as canceled manually,
/// and its history row names who canceled it.
#[test]
fn cancel_materialized_view_refresh_job_as_go_master() {
    for (mode, method) in [
        ("complete delta apply", "complete delta apply manual"),
        ("complete in place", "complete in place manual"),
        ("complete out of place", "complete out of place manual"),
    ] {
        let (stack, users, mut root) = go_master_mview_stack();
        for sql in [
            "use test",
            "set tidb_mview_enable = ON",
            "create table t_refresh_cancel_job (a int not null, b int not null)",
            "insert into t_refresh_cancel_job values (1, 10), (1, 5), (2, 7)",
            "create materialized view log on t_refresh_cancel_job (a, b) purge next date_add(now(), interval 1 hour)",
            "create materialized view mv_refresh_cancel_job (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t_refresh_cancel_job group by a",
            "create user 'mv_refresh_cancel_u'@'%' identified by 'abc'",
        ] {
            assert_eq!(answer(&mut root, sql).get(..2), Some("OK"), "{sql}");
        }
        let (_, mv) = stack.factory.catalog_snapshot().table_info_by_name("test", "mv_refresh_cancel_job").unwrap();
        let mview_id = mv.read().id;
        let _poll =
            tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/executor/mockMVTaskMonitorPollInterval", "return(50)");
        tidb_failpoint::enable(PAUSE_REFRESH, "pause").unwrap();
        let mut refresh_session = stack.factory.open_session(session_context(2150)).unwrap();
        let statement = format!("refresh materialized view mv_refresh_cancel_job {mode}");
        let refresh = std::thread::spawn(move || {
            assert_eq!(answer(&mut refresh_session, "use test"), "OK");
            answer(&mut refresh_session, &statement)
        });
        let running = format!(
            "select count(*) from mysql.tidb_mview_refresh_hist where MVIEW_ID = {mview_id} and REFRESH_STATUS = 'running'"
        );
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while answer(&mut root, &running) != r#"RS:[["1"]]"# {
            assert!(std::time::Instant::now() < deadline, "{mode}: the refresh never reached its running history row");
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
        let job = answer(
            &mut root,
            &format!(
                "select REFRESH_JOB_ID from mysql.tidb_mview_refresh_hist where MVIEW_ID = {mview_id} and REFRESH_STATUS = 'running' order by REFRESH_JOB_ID desc limit 1"
            ),
        );
        let job_id = serde_json::from_str::<Vec<Vec<String>>>(job.strip_prefix("RS:").unwrap()).unwrap()[0][0].clone();
        let identity = users
            .authenticate_native(
                "mv_refresh_cancel_u",
                "127.0.0.1",
                &super::super::node_fixture::SALT,
                &super::super::node_fixture::scramble(b"abc", &super::super::node_fixture::SALT),
            )
            .expect("mv_refresh_cancel_u logs in");
        let mut cancel =
            stack.factory.open_session(super::super::node_fixture::session_context_with_identity(2151, identity)).unwrap();
        let denied = answer(&mut cancel, &format!("cancel materialized view refresh job {job_id}"));
        assert!(denied.contains("OPERATE VIEW command denied"), "{mode}: {denied}");
        assert_eq!(answer(&mut root, "grant operate view on test.mv_refresh_cancel_job to 'mv_refresh_cancel_u'@'%'"), "OK");
        let (requested, _watch) = wait_cancel_watcher_requested("refresh-");
        assert_eq!(answer(&mut cancel, &format!("cancel materialized view refresh job {job_id}")), "OK");
        requested.recv_timeout(std::time::Duration::from_secs(10)).expect("the refresh's monitor observes the request");
        tidb_failpoint::disable(PAUSE_REFRESH);
        let refreshed = refresh.join().unwrap();
        assert!(refreshed.contains("materialized view task canceled manually"), "{mode}: {refreshed}");
        assert_eq!(
            answer(
                &mut root,
                &format!(
                    "select REFRESH_STATUS, REFRESH_METHOD, REFRESH_END_TIME is not null, REFRESH_FAILED_REASON from mysql.tidb_mview_refresh_hist where MVIEW_ID = {mview_id} order by REFRESH_JOB_ID desc limit 1"
                )
            ),
            format!(r#"RS:[["failed","{method}","1","cancelled manually by 'mv_refresh_cancel_u'@'%'"]]"#),
            "{mode}"
        );
        // The view still answers its last refresh, and refreshes again.
        assert_eq!(answer(&mut root, &format!("refresh materialized view mv_refresh_cancel_job {mode}")).get(..2), Some("OK"), "{mode}");
        assert_eq!(answer(&mut root, "select * from mv_refresh_cancel_job order by a"), r#"RS:[["1","15","2"],["2","7","1"]]"#, "{mode}");
    }
}

/// Go master's materialized view SHOW forms (`testdata/mview_show_go.jsonl`,
/// IDs mapped by name): the lists with their LIKE extractor and WHERE over
/// their own columns, SHOW CREATE MATERIALIZED VIEW (LOG), and the
/// REMAIN_LOGS / WAIT_PURGE counts around refreshes and purges, with Go's
/// errors for each wrong target.
#[test]
fn materialized_view_show_forms_answer_as_go_master() {
    let (stack, _users, session) = go_master_mview_stack();
    replay_on_with_ids(
        stack,
        session,
        include_str!("../../../../../testdata/mview_show_go.jsonl"),
        &[(131, "e29", "t"), (133, "e29", "u"), (135, "e29", "$mlog$t"), (137, "e29", "$mlog$u"), (139, "e29", "mv"), (141, "e29", "amv")],
    );
}

/// Go master's privileges on the materialized view SHOW forms, captured with
/// a second user (`testdata/mview_show_privileges_go.jsonl`, `low: ` lines
/// run as that user, IDs mapped by name): the lists keep the objects the
/// user holds any view privilege on, SHOW CREATE MATERIALIZED VIEW needs
/// SELECT then SHOW VIEW, and the log forms need SHOW VIEW (and SELECT) on
/// the log.
#[test]
fn materialized_view_show_privileges_as_go_master() {
    let (stack, users, mut root) = go_master_mview_stack();
    let ids = [("131", "t"), ("133", "u"), ("135", "$mlog$t"), ("137", "$mlog$u"), ("139", "mv"), ("141", "mv2")];
    let mut low: Option<Session> = None;
    let mut mismatches = Vec::new();
    for line in include_str!("../../../../../testdata/mview_show_privileges_go.jsonl").lines() {
        let (sql, mut go): (String, String) = serde_json::from_str(line).unwrap();
        for (go_id, table) in ids {
            if let Some((_, info)) = stack.factory.catalog_snapshot().table_info_by_name("e29", table) {
                go = go.replace(&format!("\"{go_id}\""), &format!("\"{}\"", info.read().id));
            }
        }
        let rust = match sql.strip_prefix("low: ") {
            Some(sql) => {
                let session = low.get_or_insert_with(|| {
                    let identity = users
                        .authenticate_native(
                            "low",
                            "127.0.0.1",
                            &super::super::node_fixture::SALT,
                            &super::super::node_fixture::scramble(b"abc", &super::super::node_fixture::SALT),
                        )
                        .expect("low logs in");
                    let mut session = stack
                        .factory
                        .open_session(super::super::node_fixture::session_context_with_identity(2160, identity))
                        .unwrap();
                    assert_eq!(answer(&mut session, "use e29"), "OK");
                    session
                });
                answer(session, sql)
            }
            None => answer(&mut root, &sql),
        };
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
}

/// Go master's FULL OUTER JOIN (`874bb79fa2f` parser and sysvar,
/// `052084c3034` planner, `65c2a6cd2db` V1 hash join executor), which
/// DELTA APPLY and COMPARE build on: Go master's
/// `executor/jointest/full_outer_join.test` statements, plus the gate, the
/// `FULL JOIN` alias reading, the refused forms, outer-join simplification
/// under WHERE, one-sided ON filters, chained joins, the merge-join hint
/// warning, both build sides, the optimized hash join version, view restore
/// and empty sides. Replayed from a Go master node
/// (`testdata/full_outer_join_go.jsonl`).
#[test]
fn full_outer_join_answers_as_go_master() {
    replay_go_master(include_str!("../../../../../testdata/full_outer_join_go.jsonl"));
}

/// Go master's FULL OUTER JOIN plans (`EXPLAIN FORMAT='brief'`,
/// `testdata/full_outer_join_explain_go.jsonl`): a root hash join on its
/// own; WHERE predicates null-rejecting one side, the other or both reduce
/// it to a left, right or inner join (`simplifyFullOuterJoin`); other
/// filters stay above it; side ON conditions stay inside it; its estimate
/// floors at both children.
#[test]
fn full_outer_join_plans_as_go_master() {
    replay_go_master(include_str!("../../../../../testdata/full_outer_join_explain_go.jsonl"));
}

/// Go master's REFRESH MATERIALIZED VIEW ... COMPLETE DELTA APPLY
/// (`testdata/mview_refresh_delta_apply_go.jsonl`): the view's diff against
/// its recomputed definition (inserts, deletes, updates of the payload
/// columns, a moved group key, NULL group keys matched by `<=>`) applied in
/// place, the OK packet's write counts, the history row and the refresh
/// info, nothing written when nothing changed, and a view's own MV log
/// recording the refresh's row changes.
#[test]
fn refresh_materialized_view_complete_delta_apply_answers_as_go_master() {
    replay_go_master(include_str!("../../../../../testdata/mview_refresh_delta_apply_go.jsonl"));
}

/// Go master `CheckMViewUpdatable`: a user statement's INSERT, REPLACE,
/// UPDATE, DELETE, IMPORT and locking SELECT refuse a materialized view and
/// its log (1288, the statement's own target name), while a read, a locking
/// read of another table and the base table's own writes go on -- a
/// multi-table UPDATE or DELETE of the base table writing its MV log rows
/// (`wrapTableWithMLogIfExists` per target).
#[test]
fn materialized_view_dml_guard_answers_as_go_master() {
    replay_go_master(include_str!("../../../../../testdata/mview_dml_guard_go.jsonl"));
}

/// Go master's test helper `mustExecRefreshInternal`: `sql` on `session`
/// as materialized view maintenance's restricted SQL.
fn exec_refresh_internal(session: &mut Session, sql: &str) -> String {
    let original = (session.vars.in_mview_maintenance, session.vars.restricted_sql);
    (session.vars.in_mview_maintenance, session.vars.restricted_sql) = (true, true);
    let answered = answer(session, sql);
    (session.vars.in_mview_maintenance, session.vars.restricted_sql) = original;
    answered
}

/// Go master `allowMViewMaintenanceBypass` through a session: maintenance's
/// restricted SQL writes the view and its log; maintenance outside
/// restricted SQL is an internal error; a user statement is refused.
#[test]
fn materialized_view_maintenance_bypass_as_go_master() {
    let (_stack, _users, mut root) = go_master_mview_stack();
    for sql in [
        "use test",
        "set tidb_mview_enable = ON",
        "create table t_bypass (a int not null, b int not null)",
        "insert into t_bypass values (1, 10), (2, 20)",
        "create materialized view log on t_bypass (a, b) purge next date_add(now(), interval 1 hour)",
        "create materialized view mv_bypass (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t_bypass group by a",
    ] {
        assert_eq!(answer(&mut root, sql).get(..2), Some("OK"), "{sql}");
    }
    assert!(exec_refresh_internal(&mut root, "update mv_bypass set s = s + 7 where a = 1").starts_with("OK"));
    assert!(exec_refresh_internal(&mut root, "delete from mv_bypass where a = 2").starts_with("OK"));
    assert!(exec_refresh_internal(&mut root, "insert into mv_bypass values (3, 30, 1)").starts_with("OK"));
    assert_eq!(exec_refresh_internal(&mut root, "delete from `$mlog$t_bypass`").get(..2), Some("OK"));
    assert_eq!(answer(&mut root, "select a, s, cnt from mv_bypass order by a"), r#"RS:[["1","17","1"],["3","30","1"]]"#);
    root.vars.in_mview_maintenance = true;
    let refused = answer(&mut root, "insert into mv_bypass values (4, 40, 1)");
    root.vars.in_mview_maintenance = false;
    assert_eq!(refused, "ERR 1815 materialized view maintenance should only run in restricted SQL mode");
    assert_eq!(
        answer(&mut root, "update mv_bypass set s = 1"),
        "ERR 1288 The target table mv_bypass of the UPDATE is not updatable"
    );
}

/// Go master `nextCompareTimestamp`: the GC safe point moved back, then a
/// timestamp after every commit so far (AS OF timestamps have no TSO
/// logical part).
fn next_compare_timestamp(session: &mut Session) -> String {
    assert_eq!(
        answer(
            session,
            "INSERT INTO mysql.tidb VALUES ('tikv_gc_safe_point', '20060102-15:04:05 +0000', 'All versions after safe point can be accessed. (DO NOT EDIT)') ON DUPLICATE KEY UPDATE variable_value = '20060102-15:04:05 +0000'"
        )
        .get(..2),
        Some("OK")
    );
    std::thread::sleep(std::time::Duration::from_millis(10));
    let now = answer(session, "select now(6)");
    std::thread::sleep(std::time::Duration::from_millis(10));
    serde_json::from_str::<Vec<Vec<String>>>(now.strip_prefix("RS:").expect("now(6) answers rows")).unwrap()[0][0].clone()
}

/// The single `compare result` row of a COMPARE.
fn compare_summary(session: &mut Session, sql: &str) -> String {
    let answered = answer(session, sql);
    let rows: Vec<Vec<String>> =
        serde_json::from_str(answered.strip_prefix("RS:").unwrap_or_else(|| panic!("{sql}: {answered}"))).unwrap();
    assert_eq!(rows.len(), 1, "{sql}: {answered}");
    rows[0][0].clone()
}

fn mv_setup(session: &mut Session, statements: &[&str]) {
    for sql in statements {
        assert_eq!(answer(session, sql).get(..2), Some("OK"), "{sql}");
    }
}

fn log_in(users: &ConfiguredUserStore, user: &str) -> crate::domain::configured_user_store::AuthenticatedIdentity {
    users
        .authenticate_native(
            user,
            "127.0.0.1",
            &super::super::node_fixture::SALT,
            &super::super::node_fixture::scramble(b"abc", &super::super::node_fixture::SALT),
        )
        .unwrap_or_else(|_| panic!("{user} logs in"))
}

/// Go master `TestCompareMaterializedView`
/// (`pkg/executor/test/materializedview/materialized_view_compare_test.go`):
/// its subtests in Go's order.
#[test]
fn test_compare_materialized_view() {
    // t.Run("PrivilegeSkeleton")
    test_compare_materialized_view_privilege_skeleton();
    // t.Run("PrivilegeCheckBeforeSnapshotResolution")
    test_compare_materialized_view_privilege_check_before_snapshot_resolution();
    // t.Run("SummaryAndOutput")
    test_compare_materialized_view_summary_and_output();
    // t.Run("StaleReadOnlyTransaction")
    test_compare_materialized_view_stale_read_only_transaction();
    // t.Run("UsesSnapshotMetadataAfterOutOfPlaceCutover")
    test_compare_materialized_view_uses_snapshot_metadata_after_out_of_place_cutover();
    // t.Run("OutputWriterBatchesByTxnSize")
    test_compare_materialized_view_output_writer_batches_by_txn_size();
    // t.Run("DropsOutputTableOnWriteFailure")
    test_compare_materialized_view_drops_output_table_on_write_failure();
    // t.Run("UsesDefinitionTimeZone")
    test_compare_materialized_view_uses_definition_time_zone();
    // t.Run("OutputTimestampUsesDefinitionTimeZone")
    test_compare_materialized_view_output_timestamp_uses_definition_time_zone();
    // t.Run("NullableGroupKeyMinMax")
    test_compare_materialized_view_nullable_group_key_min_max();
    // t.Run("EmptyInputs")
    test_compare_materialized_view_empty_inputs();
    // t.Run("RejectsReadTSOAfterSnapshot")
    test_compare_materialized_view_rejects_read_tso_after_snapshot();
}

/// Go master `testCompareMaterializedViewPrivilegeSkeleton`.
fn test_compare_materialized_view_privilege_skeleton() {
    let (stack, users, mut root) = go_master_mview_stack();
    mv_setup(&mut root, &[
        "use test",
        "set tidb_mview_enable = ON",
        "create table t_compare_priv (a int not null, b int not null)",
        "insert into t_compare_priv values (1, 10), (2, 20)",
        "create materialized view log on t_compare_priv (a, b) purge next date_add(now(), interval 1 hour)",
        "create materialized view mv_compare_priv (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t_compare_priv group by a",
        "create user 'mv_compare_u'@'%' identified by 'abc'",
    ]);
    let mut user = stack.factory.open_session(super::super::node_fixture::session_context_with_identity(2160, log_in(&users, "mv_compare_u"))).unwrap();
    assert_eq!(answer(&mut user, "set tidb_mview_enable = ON"), "OK");
    let compare = format!("compare materialized view test.mv_compare_priv as of timestamp '{}'", next_compare_timestamp(&mut root));
    assert!(answer(&mut user, &compare).contains("OPERATE VIEW command denied"));
    assert_eq!(answer(&mut root, "grant operate view on test.mv_compare_priv to 'mv_compare_u'@'%'"), "OK");
    assert!(answer(&mut user, &compare).contains("SELECT command denied"));
    assert_eq!(answer(&mut root, "grant select on test.t_compare_priv to 'mv_compare_u'@'%'"), "OK");
    assert!(compare_summary(&mut user, &compare).contains("timestamp"));
    let output = format!("{compare} output into table test.mv_compare_priv_diff");
    assert!(answer(&mut user, &output).contains("CREATE command denied"));
    assert_eq!(answer(&mut root, "grant create on test.* to 'mv_compare_u'@'%'"), "OK");
    assert!(answer(&mut user, &output).contains("INSERT command denied"));
    assert_eq!(answer(&mut root, "grant insert on test.* to 'mv_compare_u'@'%'"), "OK");
    assert_eq!(answer(&mut user, &output).get(..2), Some("OK"));
    assert_eq!(answer(&mut root, "select count(*) from test.mv_compare_priv_diff"), r#"RS:[["0"]]"#);
    assert_eq!(answer(&mut root, "drop table mv_compare_priv_diff"), "OK");
    assert_eq!(answer(&mut root, "create table mv_compare_priv_diff (a int)"), "OK");
    assert_eq!(answer(&mut user, &output), "ERR 1050 Table 'test.mv_compare_priv_diff' already exists");
}

/// Go master `testCompareMaterializedViewPrivilegeCheckBeforeSnapshotResolution`:
/// the current schema's privilege check runs before the snapshot schema,
/// which has no such database yet, is read.
fn test_compare_materialized_view_privilege_check_before_snapshot_resolution() {
    let (stack, users, mut root) = go_master_mview_stack();
    let compare_ts = next_compare_timestamp(&mut root);
    mv_setup(&mut root, &[
        "set tidb_mview_enable = ON",
        "create database mv_compare_priv_snapshot",
        "use mv_compare_priv_snapshot",
        "create table t (a int not null, b int not null)",
        "insert into t values (1, 10)",
        "create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)",
        "create materialized view mv (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a",
        "create user 'mv_compare_snapshot_u'@'%' identified by 'abc'",
        "grant operate view on mv_compare_priv_snapshot.mv to 'mv_compare_snapshot_u'@'%'",
    ]);
    let mut user = stack.factory.open_session(super::super::node_fixture::session_context_with_identity(2161, log_in(&users, "mv_compare_snapshot_u"))).unwrap();
    let denied = answer(&mut user, &format!("compare materialized view mv_compare_priv_snapshot.mv as of timestamp '{compare_ts}'"));
    assert!(denied.contains("SELECT command denied"), "{denied}");
    assert!(!denied.contains("Unknown database"), "{denied}");
}

/// Go master `testCompareMaterializedViewSummaryAndOutput`: a view edited
/// by maintenance SQL differs from its definition in one updated, one
/// deleted and one inserted row.
fn test_compare_materialized_view_summary_and_output() {
    let (_stack, _users, mut root) = go_master_mview_stack();
    mv_setup(&mut root, &[
        "use test",
        "set tidb_mview_enable = ON",
        "set time_zone = '+08:00'",
        "create table t_compare_diff (a int not null, b int not null)",
        "insert into t_compare_diff values (1, 10), (2, 20)",
        "create materialized view log on t_compare_diff (a, b) purge next date_add(now(), interval 1 hour)",
        "create materialized view mv_compare_diff (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t_compare_diff group by a",
    ]);
    let read_tso = answer(
        &mut root,
        "select LAST_SUCCESS_READ_TSO from mysql.tidb_mview_refresh_info where MVIEW_ID = (select tidb_table_id from information_schema.tables where table_schema = 'test' and table_name = 'mv_compare_diff')",
    );
    let read_tso: u64 = serde_json::from_str::<Vec<Vec<String>>>(read_tso.strip_prefix("RS:").unwrap()).unwrap()[0][0].parse().unwrap();
    assert!(exec_refresh_internal(&mut root, "update mv_compare_diff set s = s + 7 where a = 1").starts_with("OK"));
    assert!(exec_refresh_internal(&mut root, "delete from mv_compare_diff where a = 2").starts_with("OK"));
    assert!(exec_refresh_internal(&mut root, "insert into mv_compare_diff values (3, 30, 1)").starts_with("OK"));
    assert_eq!(answer(&mut root, "select a, s, cnt from mv_compare_diff order by a"), r#"RS:[["1","17","1"],["3","30","1"]]"#);
    let compare = format!("compare materialized view test.mv_compare_diff as of timestamp '{}'", next_compare_timestamp(&mut root));
    // Go `oracle.GetTimeFromTS(tso).In(+08:00).Format("2006-01-02 15:04:05.000000 -07:00")`.
    let at = chrono::DateTime::from_timestamp_millis((read_tso >> 18) as i64).unwrap().with_timezone(&chrono::FixedOffset::east_opt(8 * 3600).unwrap());
    assert_eq!(
        compare_summary(&mut root, &compare),
        format!(
            "There are differences result in 3 rows compared to source base tables at timestamp '{}' (TSO: {read_tso})",
            at.format("%Y-%m-%d %H:%M:%S%.6f %:z")
        )
    );
    assert_eq!(answer(&mut root, &format!("{compare} output into table test.mv_compare_diff_output")).get(..2), Some("OK"));
    assert_eq!(
        answer(&mut root, "select a, s, cnt, _Differ_type_ from test.mv_compare_diff_output order by a"),
        r#"RS:[["1","17","1","mview_differ"],["2","20","1","mview_vacuum"],["3","30","1","mview_excessive"]]"#
    );
}

/// Go master `testCompareMaterializedViewStaleReadOnlyTransaction`: the
/// summary reads in a read-only staleness transaction, the OUTPUT form is
/// refused there and leaves the transaction as it was.
fn test_compare_materialized_view_stale_read_only_transaction() {
    let (stack, _users, mut root) = go_master_mview_stack();
    mv_setup(&mut root, &[
        "use test",
        "set tidb_mview_enable = ON",
        "create table t_compare_stale (a int not null, b int not null)",
        "insert into t_compare_stale values (1, 10)",
        "create materialized view log on t_compare_stale (a, b) purge next date_add(now(), interval 1 hour)",
        "create materialized view mv_compare_stale (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t_compare_stale group by a",
    ]);
    let compare_ts = next_compare_timestamp(&mut root);
    let compare = format!("compare materialized view test.mv_compare_stale as of timestamp '{compare_ts}'");
    let output = format!("{compare} output into table test.mv_compare_stale_output");
    let mut observer = stack.factory.open_session(session_context(2162)).unwrap();
    assert!(answer(&mut observer, "insert into test.t_compare_stale values (2, 20)").starts_with("OK"));
    assert_eq!(answer(&mut root, &output).get(..2), Some("OK"));
    assert_eq!(answer(&mut observer, "select count(*) from test.mv_compare_stale_output"), r#"RS:[["0"]]"#);
    assert_eq!(answer(&mut root, "drop table mv_compare_stale_output"), "OK");
    for statements in [
        vec![format!("start transaction read only as of timestamp '{compare_ts}'")],
        vec![format!("set transaction read only as of timestamp '{compare_ts}'"), "begin".to_owned()],
    ] {
        let mut stale = stack.factory.open_session(session_context(2163)).unwrap();
        mv_setup(&mut stale, &["use test", "set tidb_mview_enable = ON"]);
        for sql in &statements {
            assert_eq!(answer(&mut stale, sql), "OK", "{sql}");
        }
        assert_eq!(answer(&mut stale, "select a, b from t_compare_stale order by a"), r#"RS:[["1","10"]]"#);
        compare_summary(&mut stale, &compare);
        assert_eq!(answer(&mut stale, &output), "ERR 1105 only support read-only statement during read-only staleness transactions");
        assert_eq!(answer(&mut stale, "select a, b from t_compare_stale order by a"), r#"RS:[["1","10"]]"#);
        assert_eq!(
            answer(&mut observer, "select count(*) from information_schema.tables where table_schema = 'test' and table_name = 'mv_compare_stale_output'"),
            r#"RS:[["0"]]"#
        );
        assert_eq!(answer(&mut stale, "rollback"), "OK");
        assert_eq!(answer(&mut stale, "select a, b from t_compare_stale order by a"), r#"RS:[["1","10"],["2","20"]]"#);
    }
}

/// Go master `testCompareMaterializedViewUsesSnapshotMetadataAfterOutOfPlaceCutover`:
/// the view as of the snapshot is the one compared, and the OUTPUT table
/// takes no index from the current one.
fn test_compare_materialized_view_uses_snapshot_metadata_after_out_of_place_cutover() {
    let (stack, _users, mut root) = go_master_mview_stack();
    mv_setup(&mut root, &[
        "use test",
        "set tidb_mview_enable = ON",
        "create table t_compare_oop_snapshot (a int not null, b int not null)",
        "insert into t_compare_oop_snapshot values (1, 10), (1, 5), (2, 7)",
        "create materialized view log on t_compare_oop_snapshot (a, b) purge next date_add(now(), interval 1 hour)",
        "create materialized view mv_compare_oop_snapshot (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t_compare_oop_snapshot group by a",
    ]);
    let compare_ts = next_compare_timestamp(&mut root);
    let id = |stack: &UnistoreClusterStack| {
        stack.factory.catalog_snapshot().table_info_by_name("test", "mv_compare_oop_snapshot").unwrap().1.read().id
    };
    let old_id = id(&stack);
    assert!(answer(&mut root, "insert into t_compare_oop_snapshot values (2, 3), (3, 4)").starts_with("OK"));
    assert!(answer(&mut root, "refresh materialized view mv_compare_oop_snapshot complete out of place").starts_with("OK"));
    assert_ne!(id(&stack), old_id);
    assert_eq!(answer(&mut root, "create index idx_current_s on mv_compare_oop_snapshot (s)"), "OK");
    let compare = format!("compare materialized view test.mv_compare_oop_snapshot as of timestamp '{compare_ts}'");
    let summary = compare_summary(&mut root, &compare);
    assert!(summary.contains("0 rows"), "{summary}");
    assert_eq!(answer(&mut root, &format!("{compare} output into table test.mv_compare_oop_snapshot_diff")).get(..2), Some("OK"));
    assert_eq!(answer(&mut root, "show index from mv_compare_oop_snapshot_diff"), "RS:[]");
}

/// Go master `testCompareMaterializedViewOutputWriterBatchesByTxnSize`: 900
/// rows of 1.5 KB under a 1 MB transaction limit are written in batches.
fn test_compare_materialized_view_output_writer_batches_by_txn_size() {
    let (_stack, _users, mut root) = go_master_mview_stack();
    mv_setup(&mut root, &[
        "use test",
        "set tidb_mview_enable = ON",
        "create table t_compare_output_batch (a int not null, c varchar(1700) not null)",
        "create index idx_a on t_compare_output_batch (a)",
    ]);
    for start in (0..900).step_by(100) {
        let values: Vec<String> = (start..start + 100).map(|i| format!("({i}, '{i:04}_{}')", "x".repeat(1500))).collect();
        assert!(answer(&mut root, &format!("insert into t_compare_output_batch values {}", values.join(", "))).starts_with("OK"));
    }
    mv_setup(&mut root, &[
        "create materialized view log on t_compare_output_batch (a, c) purge next date_add(now(), interval 1 hour)",
        "create materialized view mv_compare_output_batch (a, mx, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, max(c), count(1) from t_compare_output_batch group by a",
    ]);
    assert!(exec_refresh_internal(&mut root, "delete from mv_compare_output_batch").starts_with("OK"));
    let original = tidb_txnkv::txn_total_size_limit();
    tidb_txnkv::set_txn_total_size_limit(1024 * 1024);
    let compare = format!(
        "compare materialized view test.mv_compare_output_batch as of timestamp '{}' output into table test.mv_compare_output_batch_diff",
        next_compare_timestamp(&mut root)
    );
    let compared = answer(&mut root, &compare);
    tidb_txnkv::set_txn_total_size_limit(original);
    assert_eq!(compared.get(..2), Some("OK"), "{compared}");
    assert_eq!(answer(&mut root, "select count(*) from test.mv_compare_output_batch_diff"), r#"RS:[["900"]]"#);
}

/// Go master `testCompareMaterializedViewDropsOutputTableOnWriteFailure`.
fn test_compare_materialized_view_drops_output_table_on_write_failure() {
    let (_stack, _users, mut root) = go_master_mview_stack();
    mv_setup(&mut root, &[
        "use test",
        "set tidb_mview_enable = ON",
        "create table t_compare_output_cleanup (a int not null, b int not null)",
        "insert into t_compare_output_cleanup values (1, 10)",
        "create materialized view log on t_compare_output_cleanup (a, b) purge next date_add(now(), interval 1 hour)",
        "create materialized view mv_compare_output_cleanup (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t_compare_output_cleanup group by a",
    ]);
    assert!(exec_refresh_internal(&mut root, "update mv_compare_output_cleanup set s = s + 1 where a = 1").starts_with("OK"));
    let _flush = tidb_failpoint::enable_for_test(
        "github.com/pingcap/tidb/pkg/executor/mockCompareMaterializedViewOutputFlushError",
        r#"return("mock compare output flush error")"#,
    );
    let compare = format!(
        "compare materialized view test.mv_compare_output_cleanup as of timestamp '{}' output into table test.mv_compare_output_cleanup_diff",
        next_compare_timestamp(&mut root)
    );
    let failed = answer(&mut root, &compare);
    assert!(failed.contains("mock compare output flush error"), "{failed}");
    assert_eq!(
        answer(&mut root, "select count(*) from information_schema.tables where table_schema = 'test' and table_name = 'mv_compare_output_cleanup_diff'"),
        r#"RS:[["0"]]"#
    );
}

/// Go master `testCompareMaterializedViewUsesDefinitionTimeZone`: the
/// definition's `+00:00` decides which rows the recomputed query reads.
fn test_compare_materialized_view_uses_definition_time_zone() {
    let (_stack, _users, mut root) = go_master_mview_stack();
    mv_setup(&mut root, &[
        "use test",
        "set tidb_mview_enable = ON",
        "set @@session.time_zone = '+00:00'",
        "create table t_compare_tz (a int not null, ts timestamp not null, b int not null)",
        "insert into t_compare_tz values (1, '2025-12-31 23:30:00', 20), (1, '2026-01-01 00:30:00', 10)",
        "create materialized view log on t_compare_tz (a, ts, b) purge next date_add(now(), interval 1 hour)",
        "create materialized view mv_compare_tz (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t_compare_tz where ts >= '2026-01-01 00:00:00' group by a",
    ]);
    assert_eq!(answer(&mut root, "select a, s, cnt from mv_compare_tz"), r#"RS:[["1","10","1"]]"#);
    assert_eq!(answer(&mut root, "set @@session.time_zone = '+08:00'"), "OK");
    let compare = format!("compare materialized view test.mv_compare_tz as of timestamp '{}'", next_compare_timestamp(&mut root));
    let summary = compare_summary(&mut root, &compare);
    assert!(summary.contains("0 rows"), "{summary}");
}

/// Go master `testCompareMaterializedViewOutputTimestampUsesDefinitionTimeZone`:
/// both images keep their TIMESTAMP instants in the OUTPUT table.
fn test_compare_materialized_view_output_timestamp_uses_definition_time_zone() {
    let (_stack, _users, mut root) = go_master_mview_stack();
    mv_setup(&mut root, &[
        "use test",
        "set tidb_mview_enable = ON",
        "set @@session.time_zone = '+08:00'",
        "create table t_compare_ts_output (ts timestamp not null, dt datetime not null, v int not null)",
        "insert into t_compare_ts_output values ('2026-01-01 08:00:00', '2026-01-01 08:00:00', 10), ('2026-01-02 08:00:00', '2026-01-02 08:00:00', 20)",
        "create materialized view log on t_compare_ts_output (ts, dt, v) purge next date_add(now(), interval 1 hour)",
        "create materialized view mv_compare_ts_output (ts, dt, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select ts, dt, sum(v), count(1) from t_compare_ts_output group by ts, dt",
    ]);
    assert!(exec_refresh_internal(&mut root, "update mv_compare_ts_output set s = 99 where ts = '2026-01-01 08:00:00'").starts_with("OK"));
    assert!(exec_refresh_internal(&mut root, "delete from mv_compare_ts_output where ts = '2026-01-02 08:00:00'").starts_with("OK"));
    assert_eq!(answer(&mut root, "set @@session.time_zone = '+00:00'"), "OK");
    let compare = format!("compare materialized view test.mv_compare_ts_output as of timestamp '{}'", next_compare_timestamp(&mut root));
    assert_eq!(answer(&mut root, &format!("{compare} output into table test.mv_compare_ts_output_diff")).get(..2), Some("OK"));
    assert_eq!(
        answer(&mut root, "select ts, dt, s, cnt, _Differ_type_ from mv_compare_ts_output_diff order by ts"),
        r#"RS:[["2026-01-01 00:00:00","2026-01-01 08:00:00","99","1","mview_differ"],["2026-01-02 00:00:00","2026-01-02 08:00:00","20","1","mview_vacuum"]]"#
    );
    let _flush = tidb_failpoint::enable_for_test(
        "github.com/pingcap/tidb/pkg/executor/mockCompareMaterializedViewOutputFlushError",
        r#"return("mock compare output flush error")"#,
    );
    let failed = answer(&mut root, &format!("{compare} output into table test.mv_compare_ts_output_error"));
    assert!(failed.contains("mock compare output flush error"), "{failed}");
    assert_eq!(
        answer(&mut root, "select count(*) from information_schema.tables where table_schema = 'test' and table_name = 'mv_compare_ts_output_error'"),
        r#"RS:[["0"]]"#
    );
}

/// Go master `testCompareMaterializedViewNullableGroupKeyMinMax`: a NULL
/// group key joins its recomputed row (`IsNullEQ`).
fn test_compare_materialized_view_nullable_group_key_min_max() {
    let (_stack, _users, mut root) = go_master_mview_stack();
    mv_setup(&mut root, &[
        "use test",
        "set tidb_mview_enable = ON",
        "create table t_compare_nullable_minmax (a int, b int)",
        "create index idx_a_b on t_compare_nullable_minmax (a, b)",
        "insert into t_compare_nullable_minmax values (null, 1), (null, 3), (1, 5)",
        "create materialized view log on t_compare_nullable_minmax (a, b) purge next date_add(now(), interval 1 hour)",
        "create materialized view mv_compare_nullable_minmax (a, cnt, mx, mn) refresh fast next date_add(now(), interval 1 hour) as select a, count(1), max(b), min(b) from t_compare_nullable_minmax group by a",
    ]);
    assert!(exec_refresh_internal(&mut root, "update mv_compare_nullable_minmax set mx = 99 where a is null").starts_with("OK"));
    let compare = format!("compare materialized view test.mv_compare_nullable_minmax as of timestamp '{}'", next_compare_timestamp(&mut root));
    let summary = compare_summary(&mut root, &compare);
    assert!(summary.contains("1 rows"), "{summary}");
    assert_eq!(answer(&mut root, &format!("{compare} output into table test.mv_compare_nullable_minmax_output")).get(..2), Some("OK"));
    assert_eq!(
        answer(&mut root, "select a, cnt, mx, mn, _Differ_type_ from test.mv_compare_nullable_minmax_output"),
        r#"RS:[[null,"2","99","1","mview_differ"]]"#
    );
}

/// Go master `testCompareMaterializedViewEmptyInputs`.
fn test_compare_materialized_view_empty_inputs() {
    let (_stack, _users, mut root) = go_master_mview_stack();
    mv_setup(&mut root, &[
        "use test",
        "set tidb_mview_enable = ON",
        "create table t_compare_empty (a int not null, b int not null)",
        "create materialized view log on t_compare_empty (a, b) purge next date_add(now(), interval 1 hour)",
        "create materialized view mv_compare_empty (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t_compare_empty group by a",
    ]);
    let compare = format!("compare materialized view test.mv_compare_empty as of timestamp '{}'", next_compare_timestamp(&mut root));
    let summary = compare_summary(&mut root, &compare);
    assert!(summary.starts_with("There are no differences result in 0 rows"), "{summary}");
    assert_eq!(answer(&mut root, &format!("{compare} output into table test.mv_compare_empty_output")).get(..2), Some("OK"));
    assert_eq!(answer(&mut root, "select count(*) from test.mv_compare_empty_output"), r#"RS:[["0"]]"#);
}

/// Go master `testCompareMaterializedViewRejectsReadTSOAfterSnapshot`.
fn test_compare_materialized_view_rejects_read_tso_after_snapshot() {
    let (_stack, _users, mut root) = go_master_mview_stack();
    mv_setup(&mut root, &[
        "use test",
        "set tidb_mview_enable = ON",
        "set @@session.time_zone = '+00:00'",
        "create table t_compare_tso_guard (a int not null, b int not null)",
        "insert into t_compare_tso_guard values (1, 10)",
        "create materialized view log on t_compare_tso_guard (a, b) purge next date_add(now(), interval 1 hour)",
        "create materialized view mv_compare_tso_guard (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t_compare_tso_guard group by a",
    ]);
    // Go `oracle.GoTimeToTS(time.Now().UTC().Add(time.Hour))`.
    let future = u64::try_from(chrono::Utc::now().timestamp_millis() + 3_600_000).unwrap() << 18;
    assert!(answer(
        &mut root,
        &format!("update mysql.tidb_mview_refresh_info set LAST_SUCCESS_READ_TSO = {future} where MVIEW_ID = (select tidb_table_id from information_schema.tables where table_schema = 'test' and table_name = 'mv_compare_tso_guard')")
    )
    .starts_with("OK"));
    let compare = format!("compare materialized view test.mv_compare_tso_guard as of timestamp '{}'", next_compare_timestamp(&mut root));
    let refused = answer(&mut root, &compare);
    assert!(refused.contains("LAST_SUCCESS_READ_TSO"), "{refused}");
}

/// Go master's COMPARE MATERIALIZED VIEW refusals: ErrNoDB, a target that
/// is no materialized view, no table, no database (also the OUTPUT
/// table's), an existing OUTPUT table, a NULL AS OF, and a snapshot whose
/// schema has no such database.
#[test]
fn compare_materialized_view_refusals_answer_as_go_master() {
    replay_go_master(include_str!("../../../../../testdata/mview_compare_errors_go.jsonl"));
}

/// Go master `RefreshMaterializedViewDryRunExec`: the refresh's steps, with
/// the brief EXPLAIN of each data change (COMPLETE IN PLACE's DELETE and
/// INSERT, OUT OF PLACE's load over the view, DELTA APPLY's internal
/// implement plan), nothing executed.
#[test]
fn refresh_materialized_view_dry_run_answers_as_go_master() {
    replay_go_master(include_str!("../../../../../testdata/mview_refresh_dry_run_go.jsonl"));
}

/// A WITH PROFILE row as Go's structure, without the run's own numbers: a
/// step header without its time, and a plan row's id, estimated and actual
/// rows, task and operator info (the execution info, memory and disk being
/// this run's).
fn profile_shape(row: &str) -> String {
    if let Some((header, _)) = row.split_once("] status:") {
        let status = row.rsplit_once("status:").map_or("", |(_, rest)| rest.split(',').next().unwrap_or(""));
        return format!("{header}] status:{status}");
    }
    let parts: Vec<&str> = row.split(" | ").collect();
    let keep: Vec<&str> = parts
        .iter()
        .copied()
        .filter(|part| {
            part.starts_with("  ")
                || part.starts_with("estRows:")
                || part.starts_with("actRows:")
                || part.starts_with("task:")
                || part.starts_with("table:")
        })
        .collect();
    // The operator info is the last field that is no runtime figure.
    let info = parts.iter().rev().find(|part| {
        !part.starts_with("memory:")
            && !part.starts_with("disk:")
            && !part.contains("time:")
            && !part.starts_with("estRows:")
            && !part.starts_with("actRows:")
            && !part.starts_with("task:")
            && !part.starts_with("  ")
            && !part.starts_with("table:")
    });
    format!("{}{}", keep.join(" | "), info.map_or(String::new(), |info| format!(" | {info}")))
}

/// Go master `TestProfileMaterializedViewRefreshStepRuntime`
/// (`step_runtime`, FAST aside): WITH PROFILE runs the refresh and answers
/// each step with its status and time, and each data change's analyzed plan
/// -- compared with a Go master node's answer by its shape.
#[test]
fn refresh_materialized_view_profile_answers_as_go_master() {
    let (_stack, _users, mut session) = go_master_mview_stack();
    let mut mismatches = Vec::new();
    for line in include_str!("../../../../../testdata/mview_refresh_profile_go.jsonl").lines() {
        let (sql, go): (String, String) = serde_json::from_str(line).unwrap();
        let rust = answer(&mut session, &sql);
        if !sql.contains("with profile") {
            if rust.get(..2) != go.get(..2) {
                mismatches.push(format!("{sql}\n  go:   {go}\n  rust: {rust}"));
            }
            continue;
        }
        let shape = |answer: &str| -> Vec<String> {
            answer
                .strip_prefix("RS:")
                .map(|rows| serde_json::from_str::<Vec<Vec<String>>>(rows).unwrap())
                .unwrap_or_default()
                .iter()
                .map(|row| profile_shape(&row[0]))
                .collect()
        };
        if shape(&go) != shape(&rust) || !rust.starts_with("RS:") {
            mismatches.push(format!("{sql}\n  go:   {:#?}\n  rust: {:#?}\n  raw: {rust}", shape(&go), shape(&rust)));
        }
        if sql.contains("delta apply") {
            assert!(rust.contains("mview_complete_delta_apply:{writer:{") && rust.contains("rows:{insert:0, update:1, delete:0}}}"), "{rust}");
        }
    }
    assert!(mismatches.is_empty(), "{}", mismatches.join("\n"));
}

/// Go master FAST refresh WITH PROFILE: the step rows and, under the merge
/// step, the analyzed `MViewDeltaMerge` with Go's `mview_delta_merge_agg`
/// runtime stats, compared by shape. The merge source below it is left out:
/// its log-side estimate is the commit-ts window's wall-clock share of the
/// retained log, so its join choice follows timing (Go's second refresh here
/// hashes at a 24% share); the DRY RUN test compares that plan.
#[test]
fn refresh_materialized_view_fast_profile_answers_as_go_master() {
    let (_stack, _users, mut session) = go_master_mview_stack();
    let mut mismatches = Vec::new();
    let mut profiles = Vec::new();
    for line in include_str!("../../../../../testdata/mview_refresh_profile_fast_go.jsonl").lines() {
        let (sql, go): (String, String) = serde_json::from_str(line).unwrap();
        let rust = answer(&mut session, &sql);
        if !sql.contains("with profile") {
            let rows = |answer: &str| answer.strip_prefix("RS:").map(|rows| serde_json::from_str::<serde_json::Value>(rows).unwrap());
            if rust.get(..2) != go.get(..2) || rows(&rust) != rows(&go) {
                mismatches.push(format!("{sql}\n  go:   {go}\n  rust: {rust}"));
            }
            continue;
        }
        let shape = |answer: &str| -> Vec<String> {
            answer
                .strip_prefix("RS:")
                .map(|rows| serde_json::from_str::<Vec<Vec<String>>>(rows).unwrap())
                .unwrap_or_default()
                .iter()
                .filter(|row| row[0].starts_with("[S") || row[0].starts_with("  MViewDeltaMerge "))
                .map(|row| profile_shape(&row[0]))
                .collect()
        };
        if shape(&go) != shape(&rust) || !rust.starts_with("RS:") {
            mismatches.push(format!("{sql}\n  go:   {:#?}\n  rust: {:#?}\n  raw: {rust}", shape(&go), shape(&rust)));
        }
        profiles.push(rust);
    }
    assert!(mismatches.is_empty(), "{}", mismatches.join("\n"));
    assert!(
        profiles[0].contains("mview_delta_merge_agg:{merge_worker:{")
            && profiles[0].contains("row_ops:3, rows:{insert:1, update:1, delete:1, noop:0}}}"),
        "{}",
        profiles[0]
    );
    assert!(profiles[1].contains("row_ops:0, rows:{insert:0, update:0, delete:0, noop:0}}}"), "{}", profiles[1]);
}

/// Go master `TestProfileMaterializedViewRefreshStepRuntime/redaction`: the
/// profiled plans follow the caller's `tidb_redact_log`, in place and out of
/// place.
#[test]
fn refresh_materialized_view_profile_redacts_as_go_master() {
    let (_stack, _users, mut root) = go_master_mview_stack();
    mv_setup(&mut root, &[
        "use test",
        "set tidb_mview_enable = ON",
        "create table t_profile_redact (a int not null, b int not null)",
        "insert into t_profile_redact values (1, 123456790), (2, 1)",
        "create materialized view log on t_profile_redact (a, b) purge next date_add(now(), interval 1 hour)",
        "create materialized view mv_profile_redact (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t_profile_redact where b > 123456789 group by a",
    ]);
    for (mode, suffix) in [("OFF", ", 123456789)"), ("ON", ", ?)"), ("MARKER", ", ‹123456789›)")] {
        assert_eq!(answer(&mut root, &format!("set global tidb_redact_log = {mode}")), "OK");
        for refresh in ["complete in place", "complete out of place"] {
            let plan = answer(&mut root, &format!("refresh materialized view mv_profile_redact {refresh} with profile"));
            assert!(plan.contains(suffix), "{mode} {refresh}: {plan}");
            if mode != "OFF" {
                assert!(!plan.contains(", 123456789)"), "{mode} {refresh}: {plan}");
            }
            assert_eq!(answer(&mut root, "select a, s, cnt from mv_profile_redact"), r#"RS:[["1","123456790","1"]]"#);
        }
    }
    assert_eq!(answer(&mut root, "set global tidb_redact_log = OFF"), "OK");
}

/// Go master FAST refresh (`mview.Build` and `MViewDeltaMergeAggExec`): the
/// log window's per-group deltas merged into a view of COUNT(*), NOT NULL and
/// nullable SUMs with their COUNTs -- groups inserted, updated and deleted by
/// their new COUNT(*), NULL sums where the COUNT reaches zero -- with the
/// history row's method and scanned log rows, an empty window, and Go's
/// refusals of what FAST cannot merge and of AS OF targets before the last
/// refresh or in the future.
///
/// The DRY RUN's merge source is Go's plan: its log-side cardinality comes
/// from `SplitMLogCommitTSFilterSelectivity` -- the commit-ts filter's share
/// of the retained log window in physical milliseconds -- so those estimates
/// and the window's TSO literal are wall-clock values, masked; a pseudo 1/3
/// estimate in their place means the window never reached the planner.
#[test]
fn refresh_materialized_view_fast_answers_as_go_master() {
    let (stack, _users, session) = go_master_mview_stack();
    let (_stack, mut session) = replay_on_masked(
        stack,
        session,
        include_str!("../../../../../testdata/mview_refresh_fast_go.jsonl"),
        &[],
        &mask_mlog_window_estimates,
    );
    let dry_run = answer(&mut session, "refresh materialized view mv_fast fast dry run");
    let selection = dry_run
        .split("\"],")
        .find(|row| row.contains("Selection"))
        .expect("the log scan's commit-ts selection");
    assert!(!selection.contains("estRows:3333.33"), "{selection}");
}

/// Go master FAST refresh of MIN / MAX (`merge_minmax.go` and the
/// full-update lookup): extremes taken from the added rows, kept when the
/// removed rows do not reach them, recomputed from the base table through
/// the IndexJoin lookup child when a removed extreme leaves no successor in
/// the delta (deleted maximum, changed minimum, emptied nullable column),
/// groups deleted at COUNT(*) zero, the DRY RUN plan with the lookup child
/// and `full_update:index_lookup`, and Go's refusal of a view whose base
/// table has no index covering its group key.
#[test]
fn refresh_materialized_view_fast_min_max_answers_as_go_master() {
    let (stack, _users, session) = go_master_mview_stack();
    replay_on_masked(
        stack,
        session,
        include_str!("../../../../../testdata/mview_refresh_fast_minmax_go.jsonl"),
        &[],
        &mask_mlog_window_estimates,
    );
}

/// Blanks the wall-clock values: every estimate but the full log scan's
/// 10000 pseudo rows, the `_tidb_commit_ts` window literal, and the TSOs the
/// AS OF refusals name.
fn mask_mlog_window_estimates(answer: &str) -> String {
    const MARKERS: [&str; 6] =
        ["estRows:", "_tidb_commit_ts, ", "target tso ", "LAST_SUCCESS_READ_TSO ", "readTS: ", "currentTS: "];
    let mut out = String::with_capacity(answer.len());
    let mut rest = answer;
    while let Some((at, marker)) =
        MARKERS.iter().filter_map(|marker| rest.find(marker).map(|at| (at, *marker))).min_by_key(|(at, _)| *at)
    {
        let start = at + marker.len();
        out.push_str(&rest[..start]);
        let len = rest[start..].find(|c: char| !(c.is_ascii_digit() || c == '.')).unwrap_or(rest.len() - start);
        let value = &rest[start..start + len];
        out.push_str(if value.is_empty() || value == "10000.00" { value } else { "?" });
        rest = &rest[start + len..];
    }
    out.push_str(rest);
    without_window_dependent_join(&out)
}

/// A FAST refresh joins the log's delta to the view, and the planner picks
/// that join from Go master's `estimateMLogCommitTSSelectivity` -- "a
/// physical-time estimate": the time since the last refresh over the age of
/// the retained log. The same statements therefore plan an index join over a
/// range scan or a hash join over a full scan with the wall time between
/// them, in Go as here. The comparison keeps the join's kind and drops what
/// the estimate decides: its algorithm and the operators under it.
fn without_window_dependent_join(answer: &str) -> String {
    let Some(rows) = answer
        .strip_prefix("RS:")
        .and_then(|rows| serde_json::from_str::<Vec<Vec<String>>>(rows).ok())
    else {
        return answer.to_owned();
    };
    // A plan row's depth: where its tree connector stands; none for a stage
    // header or the plan's root.
    let depth = |row: &[String]| {
        row.first()
            .and_then(|cell| cell.chars().position(|c| matches!(c, '├' | '└')))
    };
    let mut kept: Vec<Vec<String>> = Vec::with_capacity(rows.len());
    let mut under_join: Option<usize> = None;
    for mut row in rows {
        if let Some(join_depth) = under_join {
            if depth(&row).is_some_and(|depth| depth > join_depth) {
                continue;
            }
            under_join = None;
        }
        let join = row.first().and_then(|cell| {
            let at = cell.find("IndexJoin | ").or_else(|| cell.find("HashJoin | "))?;
            let kind = cell[at..].split(" | ").nth(3)?.split(',').next()?.to_owned();
            Some((at, kind))
        });
        if let Some((at, kind)) = join {
            under_join = depth(&row);
            let cell = &mut row[0];
            cell.truncate(at);
            cell.push_str(&format!("Join | {kind}"));
        }
        kept.push(row);
    }
    format!("RS:{}", serde_json::to_string(&kept).expect("rows serialize"))
}

#[test]
fn the_window_dependent_join_compares_equal_across_its_two_plans() {
    let plan = |join: &str, probe: &[&str]| {
        let mut rows = vec![
            vec!["[S04 DATA_CHANGE_FAST_MERGE]".to_owned()],
            vec!["  MViewDeltaMerge | estRows:N/A | task:root | agg_deps:[]".to_owned()],
            vec!["  └─Projection | estRows:? | task:root | a".to_owned()],
            vec![format!("    └─{join}")],
            vec!["      ├─HashAgg(Build) | estRows:? | task:root | group by:a".to_owned()],
        ];
        rows.extend(probe.iter().map(|row| vec![(*row).to_owned()]));
        rows.push(vec!["[S05 PERSIST_REFRESH_INFO]".to_owned()]);
        format!("RS:{}", serde_json::to_string(&rows).unwrap())
    };
    let index_join = plan(
        "IndexJoin | estRows:? | task:root | left outer join, inner:TableReader_32, outer key:a",
        &[
            "      └─TableReader(Probe) | estRows:? | task:root | data:TableRangeScan_31",
            "        └─TableRangeScan | estRows:? | task:cop[tikv] | table:mv | range: decided by [a]",
        ],
    );
    let hash_join = plan(
        "HashJoin | estRows:? | task:root | left outer join, left side:HashAgg_26, equal:[eq(a, a)]",
        &[
            "      └─TableReader(Probe) | estRows:10000.00 | task:root | data:TableFullScan_33",
            "        └─TableFullScan | estRows:10000.00 | task:cop[tikv] | table:mv | keep order:false",
        ],
    );
    let normalized = without_window_dependent_join(&index_join);
    assert_eq!(normalized, without_window_dependent_join(&hash_join));
    // The join's kind and the rows around the join stay.
    assert!(normalized.contains("    └─Join | left outer join"), "{normalized}");
    assert!(normalized.contains("[S05 PERSIST_REFRESH_INFO]"), "{normalized}");
    assert!(!normalized.contains("TableReader"), "{normalized}");
    // An inner join is not a left outer join.
    let inner = plan("HashJoin | estRows:? | task:root | inner join, equal:[eq(a, a)]", &[]);
    assert_ne!(without_window_dependent_join(&inner), normalized);
    // Anything but a result set is left alone.
    assert_eq!(without_window_dependent_join("OK"), "OK");
}

/// Polls `sql` until it answers `want` (Go `require.EventuallyWithT`, 20s
/// at 100ms).
fn eventually_answers(session: &mut Session, sql: &str, want: &str) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
    loop {
        let got = answer(session, sql);
        if got == want {
            return;
        }
        assert!(std::time::Instant::now() < deadline, "{sql}: {got}, want {want}");
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
}

/// Go master `TestMaterializedViewServiceAutomaticMaintenance`: its
/// `refresh_and_purge` and `mlog_analyze` subtests.
#[test]
fn test_materialized_view_service_automatic_maintenance() {
    materialized_view_service_automatic_refresh_and_purge();
    materialized_view_service_mlog_analyze();
}

/// Go master `TestMaterializedViewServiceMiscMaintenance`: its `history` and
/// `freshness_and_accumulation` subtests.
#[test]
fn test_materialized_view_service_misc_maintenance() {
    materialized_view_service_history_gc();
    materialized_view_service_freshness_and_accumulation();
}

/// Go master `TestMaterializedViewServiceAutomaticMaintenance/refresh_and_purge`:
/// the MV service refreshes a due view FAST and purges a due log on its
/// own, and the `tidb_mview_task_*` globals reach it.
fn materialized_view_service_automatic_refresh_and_purge() {
    let (stack, _users, mut root) = go_master_mview_stack();
    mv_setup(&mut root, &[
        "use test",
        "set tidb_mview_enable = ON",
        "create table t_mvs (a int primary key, b int not null)",
        "create materialized view log on t_mvs (a, b) purge next date_add(now(), interval 1 hour)",
        "insert into t_mvs values (1, 10)",
        "create materialized view mv_service (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t_mvs group by a",
        "insert into t_mvs values (2, 20)",
    ]);
    let svc = stack.factory.mv_service();
    svc.set_task_backpressure_config(tidb_mvservice::TaskBackpressureConfig::default()).unwrap();
    mv_setup(&mut root, &["set global tidb_mview_task_max=4", "set global tidb_mview_task_refresh_ratio=0.75"]);
    assert_eq!(svc.get_refresh_task_concurrency_ratio(), 0.75);
    mv_setup(&mut root, &["set global tidb_mview_task_threshold_cpu=0.75", "set global tidb_mview_task_threshold_memory=0.65"]);
    assert_eq!(svc.get_task_backpressure_config().cpu_threshold, 0.75);
    assert_eq!(svc.get_task_backpressure_config().mem_threshold, 0.65);
    mv_setup(&mut root, &[
        "set global tidb_mview_task_threshold_cpu=0",
        "set global tidb_mview_task_threshold_memory=0",
        "set global tidb_mview_refresh_hist_time=24",
        "set global tidb_mlog_purge_hist_time=24",
    ]);
    let (_, mv) = stack.factory.catalog_snapshot().table_info_by_name("test", "mv_service").unwrap();
    let mview_id = mv.read().id;
    mv_setup(&mut root, &[&format!("update mysql.tidb_mview_refresh_info set NEXT_REFRESH_UNIX_SECONDS=0 where MVIEW_ID={mview_id}")]);
    let _service = stack.factory.start_mv_service().unwrap();
    eventually_answers(&mut root, "select * from mv_service order by a", r#"RS:[["1","10","1"],["2","20","1"]]"#);

    mv_setup(&mut root, &["update mysql.tidb_mlog_purge_info set NEXT_PURGE_UNIX_SECONDS=0"]);
    svc.notify_ddl_change();
    eventually_answers(&mut root, "select count(*) from `$mlog$t_mvs`", r#"RS:[["0"]]"#);
}

/// Go master `TestMaterializedViewServiceMiscMaintenance/history`: the
/// service's history GC deletes finished rows past the retention the
/// `*_hist_time` globals set, keeps a running row with a live heartbeat, and
/// marks a running row whose heartbeat expired orphaned, in both history
/// tables.
fn materialized_view_service_history_gc() {
    let (stack, _users, mut root) = go_master_mview_stack();
    mv_setup(&mut root, &["use test", "set tidb_mview_enable = ON"]);
    let svc = stack.factory.mv_service();
    // Schedule GC soon, then verify the SQL callbacks replace both retention
    // settings without delaying that already scheduled run.
    svc.set_mview_refresh_hist_retention(1_000_000).unwrap();
    svc.set_mlog_purge_hist_retention(1_000_000).unwrap();
    mv_setup(&mut root, &["set global tidb_mview_refresh_hist_time=48", "set global tidb_mlog_purge_hist_time=48"]);

    let now = chrono::Local::now();
    let at = |hours: i64| now - chrono::Duration::hours(hours);
    // Go `oracle.GoTimeToTS`.
    let tso = |time: chrono::DateTime<chrono::Local>| (time.timestamp_millis() as u64) << 18;
    let text = |time: chrono::DateTime<chrono::Local>| format!("'{}'", time.format("%Y-%m-%d %H:%M:%S"));
    let (old, stale, recent) = (at(49), at(36), at(2));
    for (table, prefix, object) in [
        ("mysql.tidb_mview_refresh_hist", "REFRESH", "MVIEW_ID"),
        ("mysql.tidb_mlog_purge_hist", "PURGE", "MLOG_ID"),
    ] {
        let insert = |job: u64, id: i64, status: &str, start: String, heartbeat: &str| {
            format!(
                "insert into {table} ({prefix}_JOB_ID, {object}, {prefix}_METHOD, {prefix}_STATUS, {prefix}_START_TIME, LAST_HEARTBEAT_TIME, {prefix}_ROWS) values ({job}, {id}, 'manual', {status}, {start}, {heartbeat}, 0)"
            )
        };
        mv_setup(&mut root, &[
            &insert(tso(old), 101, "'success'", text(old), "NULL"),
            &insert(tso(old) + 1, 102, "NULL", text(old), "NULL"),
            &insert(tso(recent), 103, "'success'", text(recent), "NULL"),
            &insert(tso(old) + 2, 104, "'running'", text(old), &text(now)),
            &insert(tso(stale), 105, "'running'", text(stale), "NULL"),
        ]);
    }
    let _service = stack.factory.start_mv_service().unwrap();
    for (table, prefix, object) in [
        ("mysql.tidb_mview_refresh_hist", "REFRESH", "MVIEW_ID"),
        ("mysql.tidb_mlog_purge_hist", "PURGE", "MLOG_ID"),
    ] {
        eventually_answers(
            &mut root,
            &format!("select {object}, {prefix}_STATUS from {table} order by {object}"),
            r#"RS:[["103","success"],["104","running"],["105","orphaned"]]"#,
        );
        assert_eq!(
            answer(
                &mut root,
                &format!("select {prefix}_END_TIME is not null, {prefix}_DURATION_SEC is not null, {prefix}_FAILED_REASON is not null from {table} where {object}=105")
            ),
            r#"RS:[["1","1","1"]]"#
        );
    }
}

/// Go master `TestMaterializedViewServiceMiscMaintenance/freshness_and_accumulation`:
/// the service's refresh alert scan writes warning / overdue levels from
/// each view's last successful read, clears a recovered or disabled view's
/// level while keeping its independent failure flag, removes a deleted
/// view's row, and counts the MV logs past their accumulation threshold.
fn materialized_view_service_freshness_and_accumulation() {
    let (stack, _users, mut root) = go_master_mview_stack();
    mv_setup(&mut root, &[
        "use test",
        "set tidb_mview_enable = ON",
        "create table t_service_alert (a int primary key, b int not null)",
        "create materialized view log on t_service_alert (a, b) purge next date_add(now(), interval 1 hour) alert rows 1",
        "insert into t_service_alert values (1, 10)",
    ]);
    let now = chrono::Local::now();
    // Go `oracle.GoTimeToTS`.
    let tso = |time: chrono::DateTime<chrono::Local>| (time.timestamp_millis() as u64) << 18;
    for (name, age_minutes, disabled) in [
        ("mv_service_warning", 90, false),
        ("mv_service_overdue", 180, false),
        ("mv_service_recovered", 0, false),
        ("mv_service_disabled", 0, true),
    ] {
        let schedule = if disabled { "" } else { "next date_add(now(), interval 1 hour)" };
        mv_setup(&mut root, &[&format!(
            "create materialized view {name} (a, s, cnt) refresh fast {schedule} attributes='mview_alert_warning=3600,mview_alert_overdue=7200,mview_alert_refresh_failed=yes' as select a, sum(b), count(1) from t_service_alert group by a"
        )]);
        let (_, mv) = stack.factory.catalog_snapshot().table_info_by_name("test", name).unwrap();
        let id = mv.read().id;
        mv_setup(&mut root, &[&format!(
            "update mysql.tidb_mview_refresh_info set LAST_SUCCESS_READ_TSO={} where MVIEW_ID={id}",
            tso(now - chrono::Duration::minutes(age_minutes))
        )]);
        if disabled || age_minutes == 0 {
            // Clearing a freshness alert must retain the independent failure flag.
            mv_setup(&mut root, &[&format!(
                "insert into mysql.tidb_mview_refresh_alert (MVIEW_ID, MVIEW_SCHEMA, MVIEW_NAME, ALERT_LEVEL, REFRESH_FAILED) values ({id}, 'test', '{name}', 'overdue', 'yes')"
            )]);
        }
    }
    mv_setup(&mut root, &[
        "insert into mysql.tidb_mview_refresh_alert (MVIEW_ID, MVIEW_NAME, ALERT_LEVEL) values (-1, 'deleted', 'warning')",
        "insert into t_service_alert values (2, 20), (3, 30)",
    ]);

    let _service = stack.factory.start_mv_service().unwrap();
    eventually_answers(
        &mut root,
        "select MVIEW_NAME, ALERT_LEVEL, REFRESH_FAILED from mysql.tidb_mview_refresh_alert order by MVIEW_NAME",
        r#"RS:[["mv_service_disabled",null,"yes"],["mv_service_overdue","overdue",null],["mv_service_recovered",null,"yes"],["mv_service_warning","warning",null]]"#,
    );
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
    loop {
        use tidb_metrics::materialized_view as m;
        let gauges = (
            m::MV_SERVICE_MV_REFRESH_WARNING_GAUGE.get(),
            m::MV_SERVICE_MV_REFRESH_OVERDUE_GAUGE.get(),
            m::MV_SERVICE_MV_LOG_ACCUMULATION_GAUGE.get(),
        );
        if gauges == (1.0, 1.0, 1.0) {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "{gauges:?}");
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
}

/// Go master `TestMaterializedViewServiceAutomaticMaintenance/mlog_analyze`:
/// with auto-analyze on and the minimum row count lowered, the service
/// analyzes an MV log whose statistics were never analyzed.
fn materialized_view_service_mlog_analyze() {
    use std::sync::atomic::Ordering::SeqCst;
    let original_min_cnt = tidb_stats::AUTO_ANALYZE_MIN_CNT.swap(0, SeqCst);
    let (stack, _users, mut root) = go_master_mview_stack();
    mv_setup(&mut root, &[
        "use test",
        "set tidb_mview_enable = ON",
        "set global tidb_enable_auto_analyze=ON",
        "create table t_mvs_analyze (a int)",
    ]);
    super::drain_stats_ddl_events(&stack.factory, &mut root);
    mv_setup(&mut root, &["create materialized view log on t_mvs_analyze (a)"]);
    super::drain_stats_ddl_events(&stack.factory, &mut root);
    mv_setup(&mut root, &["insert into t_mvs_analyze values (1), (2)"]);
    stack.factory.dump_stats_delta_to_kv(true, &[]).expect("the delta flush succeeds");
    root.update_stats_cache().expect("statsHandle.Update");
    stack.factory.mv_service().set_task_backpressure_config(tidb_mvservice::TaskBackpressureConfig::default()).unwrap();
    let _service = stack.factory.start_mv_service().unwrap();
    eventually_answers(
        &mut root,
        "select count(*) > 0 from mysql.analyze_jobs where table_schema='test' and table_name='$mlog$t_mvs_analyze' and state='finished'",
        r#"RS:[["1"]]"#,
    );
    tidb_stats::AUTO_ANALYZE_MIN_CNT.store(original_min_cnt, SeqCst);
}

/// Go master `TestMaterializedViewRefreshSchemaVersion`
/// (`pkg/executor/test/mviewoutputlog/refresh_schema_test.go`).
#[test]
fn test_materialized_view_refresh_schema_version() {
    for scenario in [
        "current", "empty_versions", "unrelated", "unrelated_fast", "unrelated_empty_versions",
        "mlog", "mlog_then_unrelated", "reload_failure", "input_mlog_columns", "output_mlog_columns", "source_oop", "schema_change",
        "schema_cache_miss", "schema_load_failure", "mlog_then_add_column", "related_empty_versions",
    ] {
        refresh_schema_version_scenario(scenario);
    }
}

fn refresh_schema_version_scenario(scenario: &'static str) {
    use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
    let (stack, _users, mut tk) = go_master_mview_stack();
    let must_exec = |tk: &mut Session, sql: &str| assert_eq!(answer(tk, sql), "OK", "{scenario}: {sql}");
    let exec_to_err = |tk: &mut Session, sql: &str| {
        let answer = answer(tk, sql);
        assert!(answer.starts_with("ERR"), "{scenario}: {sql}: {answer}");
        answer
    };
    for sql in [
        "set tidb_mview_enable = on",
        "use test",
        "create table t_schema (a int not null, b int not null)",
        "insert into t_schema values (1, 10)",
        "create materialized view log on t_schema (a, b)",
        "create materialized view mv_schema (a, s, cnt) as select a, sum(b), count(1) from t_schema group by a",
        "insert into t_schema values (2, 20)",
    ] {
        must_exec(&mut tk, sql);
    }
    let mut refresh_sql = "refresh materialized view mv_schema complete in place";
    if scenario == "input_mlog_columns" {
        must_exec(&mut tk, "alter table t_schema add column c int");
    }
    if scenario == "output_mlog_columns" {
        must_exec(&mut tk, "create materialized view log on mv_schema (a)");
    }
    if scenario == "source_oop" {
        must_exec(&mut tk, "create materialized view log on mv_schema (a, s, cnt)");
        must_exec(&mut tk, "create materialized view mv_child (a, s, cnt) as select a, s, count(1) from mv_schema group by a, s");
        refresh_sql = "refresh materialized view mv_child complete in place";
    }
    if matches!(scenario, "unrelated_fast" | "input_mlog_columns" | "output_mlog_columns" | "mlog_then_add_column") {
        refresh_sql = "refresh materialized view mv_schema fast";
    }
    let _no_tick_reload =
        tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/infoschema/issyncer/disableOnTickReload", "return(true)");
    let catalog = Arc::clone(&stack.factory.catalog);
    let old_is = catalog.get_latest();
    let mut intermediate_is = Arc::clone(&old_is);
    match scenario {
        "unrelated" | "unrelated_fast" | "unrelated_empty_versions" | "schema_cache_miss" | "schema_load_failure" => {
            must_exec(&mut tk, "create table schema_bump (a int)");
            must_exec(&mut tk, "alter table schema_bump add column b int");
            must_exec(&mut tk, "drop table schema_bump");
        }
        "mlog" | "mlog_then_unrelated" => {
            must_exec(&mut tk, "create materialized view log on mv_schema (a, s, cnt)");
            if scenario == "mlog_then_unrelated" {
                // "The newest diff alone cannot establish that refresh is unaffected."
                must_exec(&mut tk, "create table schema_bump (a int)");
            }
        }
        "mlog_then_add_column" => {
            must_exec(&mut tk, "create materialized view log on mv_schema (a)");
            intermediate_is = catalog.get_latest();
            must_exec(&mut tk, "alter materialized view log on mv_schema add (s)");
        }
        "reload_failure" | "related_empty_versions" => must_exec(&mut tk, "alter table t_schema add column c int"),
        "input_mlog_columns" => must_exec(&mut tk, "alter materialized view log on t_schema add (c)"),
        "output_mlog_columns" => must_exec(&mut tk, "alter materialized view log on mv_schema add (s)"),
        "source_oop" => must_exec(&mut tk, "refresh materialized view mv_schema complete out of place"),
        "schema_change" => must_exec(&mut tk, "create database schema_bump"),
        _ => {}
    }
    if matches!(scenario, "empty_versions" | "unrelated_empty_versions" | "related_empty_versions") {
        // "Failed DDL steps can leave multiple reserved versions without diffs."
        stack
            .factory
            .meta_store
            .get()
            .unwrap()
            .run_in_new_txn(&tidb_gocontext::Context::background(), tidb_txnkv::INTERNAL_TXN_DDL, false, &mut |txn| {
                tidb_meta::transaction::Mutator::new(txn).gen_schema_versions(2)?;
                Ok(())
            })
            .unwrap();
    }
    if scenario != "current" && scenario != "empty_versions" {
        assert!(catalog.get_latest().schema_meta_version() > old_is.schema_meta_version(), "{scenario}");
        // "Simulate a node whose cache has not caught up with committed DDL."
        catalog.upsert_replacing(Arc::clone(&old_is), 0);
    }
    const LOAD_FAILURE: &str = "github.com/pingcap/tidb/pkg/infoschema/mockLoadTableInfoError";
    let _collect_call = matches!(scenario, "schema_cache_miss" | "schema_load_failure").then(|| {
        assert!(old_is.v2().is_some(), "requires InfoSchema V2");
        let injected = AtomicBool::new(false);
        let old_is = Arc::clone(&old_is);
        tidb_failpoint::enable_call_for_test(
            "github.com/pingcap/tidb/pkg/executor/refreshMaterializedViewBeforeCollectRelatedTables",
            move |_| {
                if injected.compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire).is_ok() {
                    old_is.v2().unwrap().evict_table("test", "t_schema");
                    if scenario == "schema_load_failure" {
                        tidb_failpoint::enable(LOAD_FAILURE, "1*return(true)").unwrap();
                    }
                }
            },
        )
    });
    const RELOAD_FAILURE: &str = "github.com/pingcap/tidb/pkg/infoschema/issyncer/ErrorMockReloadFailed";
    let reload_lock_attempts = Arc::new(AtomicI32::new(0));
    let _lock_call = tidb_failpoint::enable_call_for_test(
        "github.com/pingcap/tidb/pkg/executor/refreshMaterializedViewBeforeSchemaReloadLock",
        {
            let attempts = Arc::clone(&reload_lock_attempts);
            let catalog = Arc::clone(&catalog);
            let intermediate_is = Arc::clone(&intermediate_is);
            move |_| {
                attempts.fetch_add(1, Ordering::AcqRel);
                if scenario == "mlog_then_add_column" {
                    // "Simulate a loader that caught up only to MLog creation.
                    // The original dependency set does not contain the new MLog ID."
                    catalog.upsert_replacing(Arc::clone(&intermediate_is), 0);
                }
            }
        },
    );
    let mut reload_failure = (scenario != "mlog" && scenario != "mlog_then_unrelated")
        .then(|| tidb_failpoint::enable_for_test(RELOAD_FAILURE, "return(true)"));
    match scenario {
        "current" | "empty_versions" | "unrelated" | "unrelated_fast" | "unrelated_empty_versions" | "schema_cache_miss" => {
            // "Unrelated DDL and empty versions do not force a synchronous reload."
            must_exec(&mut tk, refresh_sql);
            assert_eq!(reload_lock_attempts.load(Ordering::Acquire), 0, "{scenario}: unaffected refresh must skip the reload mutex");
            reload_failure = None;
        }
        "mlog" | "mlog_then_unrelated" => {
            let err = exec_to_err(&mut tk, refresh_sql);
            assert!(err.contains("with a materialized view log"), "{scenario}: {err}");
            assert!(catalog.get_latest().schema_meta_version() > old_is.schema_meta_version(), "{scenario}");
            assert_eq!(answer(&mut tk, "select * from mv_schema"), r#"RS:[["1","10","1"]]"#, "{scenario}");
            return;
        }
        "schema_load_failure" => {
            let err = exec_to_err(&mut tk, refresh_sql);
            assert!(err.contains("mockLoadTableInfoError"), "{scenario}: {err}");
            assert_eq!(reload_lock_attempts.load(Ordering::Acquire), 0, "{scenario}: a table load failure must not trigger a schema reload");
            tidb_failpoint::disable(LOAD_FAILURE);
            must_exec(&mut tk, refresh_sql);
            reload_failure = None;
        }
        _ => {
            let err = exec_to_err(&mut tk, refresh_sql);
            assert!(err.contains("mock reload failed"), "{scenario}: {err}");
            reload_failure = None;
            // "Both the reload mutex and the MV advisory lock must be released."
            must_exec(&mut tk, refresh_sql);
            assert!(catalog.get_latest().schema_meta_version() > old_is.schema_meta_version(), "{scenario}");
        }
    }
    drop(reload_failure);
    assert_eq!(answer(&mut tk, "select * from mv_schema order by a"), r#"RS:[["1","10","1"],["2","20","1"]]"#, "{scenario}");
    if scenario == "source_oop" {
        assert_eq!(answer(&mut tk, "select * from mv_child order by a"), r#"RS:[["1","10","1"],["2","20","1"]]"#, "{scenario}");
    }
    if scenario == "mlog_then_add_column" {
        assert_eq!(answer(&mut tk, "select a, s from `$mlog$mv_schema`"), r#"RS:[["2","20"]]"#, "{scenario}");
    }
}

/// The latest schema's table `test.name`, as Go's `dom.InfoSchema().TableByName`.
fn latest_table(stack: &UnistoreClusterStack, name: &str) -> tidb_model::TableInfo {
    stack.factory.catalog.get_latest().table_info_by_name("test", name).expect(name).1.read().clone()
}

fn mview_ids(table: &tidb_model::TableInfo) -> Vec<i64> {
    table.materialized_view_base.as_ref().map(|base| base.read().mview_ids.iter().copied().collect()).unwrap_or_default()
}

fn dependent_mview_ids(mlog: &tidb_model::TableInfo) -> Vec<i64> {
    mlog.materialized_view_log.as_ref().map(|log| log.read().dependent_mview_ids.iter().copied().collect()).unwrap_or_default()
}

fn mlog_id(table: &tidb_model::TableInfo) -> i64 {
    table.materialized_view_base.as_ref().map_or(0, |base| base.read().mlog_id)
}

fn must_exec_all(session: &mut Session, statements: &[&str]) {
    for sql in statements {
        assert_eq!(answer(session, sql), "OK", "{sql}");
    }
}

/// Go `tk.ExecToErr(sql).Error()`: the failure's full `Error()` text,
/// annotations included (the client sees only `errors.Cause`).
fn exec_to_err(session: &mut Session, sql: &str) -> String {
    match session.node_conn().execute(sql) {
        Ok(_) => panic!("{sql}: expected an error"),
        Err(error) => error.error_string(),
    }
}

fn exec_err_contains(session: &mut Session, sql: &str, expected: &str) {
    let answer = answer(session, sql);
    assert!(answer.starts_with("ERR") && answer.contains(expected), "{sql}: {answer}");
}

/// Go master `TestCreateMaterializedViewOnMaterializedView`
/// (`pkg/executor/test/mviewnested/nested_create_test.go`).
#[test]
fn test_create_materialized_view_on_materialized_view() {
    let (stack, _users, mut tk) = go_master_mview_stack();
    must_exec_all(&mut tk, &[
        "set tidb_mview_enable = on",
        "use test",
        "create table mv_on_mv_base (a int not null, b int not null)",
        "insert into mv_on_mv_base values (1, 10), (1, 5), (2, 7)",
        "create materialized view log on mv_on_mv_base (a, b)",
        "create materialized view mv_parent (a, cnt, total) refresh fast as select a, count(1), sum(b) from mv_on_mv_base group by a",
        "create materialized view log on mv_parent (a, cnt)",
        "create materialized view mv_child (a, parent_cnt, child_cnt) refresh fast as select a, sum(cnt), count(1) from mv_parent group by a",
    ]);
    assert_eq!(answer(&mut tk, "select a, parent_cnt, child_cnt from mv_child order by a"), r#"RS:[["1","2","1"],["2","1","1"]]"#);

    let parent = latest_table(&stack, "mv_parent");
    let child = latest_table(&stack, "mv_child");
    let parent_mlog = latest_table(&stack, "$mlog$mv_parent");
    let child_base_ids: Vec<i64> = child.materialized_view.as_ref().unwrap().read().base_table_ids.iter().copied().collect();
    assert_eq!(child_base_ids, vec![parent.id]);
    assert!(parent.materialized_view_base.is_some());
    assert_eq!(mlog_id(&parent), parent_mlog.id);
    assert_eq!(parent_mlog.materialized_view_log.as_ref().unwrap().read().base_table_id, parent.id);
    assert!(mview_ids(&parent).contains(&child.id));
    assert_eq!(dependent_mview_ids(&parent_mlog), vec![child.id]);

    exec_err_contains(&mut tk, "drop materialized view mv_parent", "DROP MATERIALIZED VIEW with materialized view dependencies");
    exec_err_contains(&mut tk, "drop materialized view log on mv_parent", "dependent materialized views exist");

    must_exec_all(&mut tk, &["drop materialized view mv_child"]);
    let parent = latest_table(&stack, "mv_parent");
    let parent_mlog = latest_table(&stack, "$mlog$mv_parent");
    assert!(mview_ids(&parent).is_empty());
    assert!(dependent_mview_ids(&parent_mlog).is_empty());
    assert_eq!(mlog_id(&parent), parent_mlog.id);

    // t.Run("initial_build_rollback")
    {
        let _build_err = tidb_failpoint::enable_for_test(
            "github.com/pingcap/tidb/pkg/ddl/mockCreateMaterializedViewBuildErr",
            r#"return("context-canceled")"#,
        );
        let answer_text = answer(
            &mut tk,
            "create materialized view mv_child_failed (a, parent_cnt, child_cnt) as select a, sum(cnt), count(1) from mv_parent group by a",
        );
        assert!(answer_text.starts_with("ERR"), "{answer_text}");
        assert_eq!(answer(&mut tk, "show tables like 'mv_child_failed'"), "RS:[]");
        let parent = latest_table(&stack, "mv_parent");
        let parent_mlog = latest_table(&stack, "$mlog$mv_parent");
        assert!(mview_ids(&parent).is_empty());
        assert!(dependent_mview_ids(&parent_mlog).is_empty());
        assert_eq!(mlog_id(&parent), parent_mlog.id);
    }

    must_exec_all(&mut tk, &[
        "drop materialized view log on mv_parent",
        "drop materialized view mv_parent",
        "drop materialized view log on mv_on_mv_base",
        "drop table mv_on_mv_base",
    ]);
}

/// Go master `TestMaterializedViewNestedDependencyChain`.
#[test]
fn test_materialized_view_nested_dependency_chain() {
    let (stack, _users, mut tk) = go_master_mview_stack();
    must_exec_all(&mut tk, &[
        "set tidb_mview_enable = on",
        "use test",
        "create table mv_dependency_chain_base (a int not null, b int not null)",
        "insert into mv_dependency_chain_base values (1, 10), (1, 5), (2, 7)",
        "create materialized view log on mv_dependency_chain_base (a, b)",
        "create materialized view mv_dependency_chain_1 (a, cnt, total) refresh fast as select a, count(1), sum(b) from mv_dependency_chain_base group by a",
        "create materialized view log on mv_dependency_chain_1 (a, cnt, total)",
        "create materialized view mv_dependency_chain_2 (a, total, total_cnt, cnt) refresh fast as select a, sum(total), count(total), count(1) from mv_dependency_chain_1 group by a",
        "create materialized view log on mv_dependency_chain_2 (a, total, total_cnt, cnt)",
        "create materialized view mv_dependency_chain_3 (a, total, total_cnt, cnt) refresh fast as select a, sum(total), count(total), count(1) from mv_dependency_chain_2 group by a",
    ]);
    assert_eq!(answer(&mut tk, "select a, total, cnt from mv_dependency_chain_3 order by a"), r#"RS:[["1","15","1"],["2","7","1"]]"#);

    let base = latest_table(&stack, "mv_dependency_chain_base");
    let mv1 = latest_table(&stack, "mv_dependency_chain_1");
    let mv2 = latest_table(&stack, "mv_dependency_chain_2");
    let mv3 = latest_table(&stack, "mv_dependency_chain_3");
    let base_ids = |mv: &tidb_model::TableInfo| -> Vec<i64> {
        mv.materialized_view.as_ref().unwrap().read().base_table_ids.iter().copied().collect()
    };
    assert_eq!(base_ids(&mv1), vec![base.id]);
    assert_eq!(base_ids(&mv2), vec![mv1.id]);
    assert_eq!(base_ids(&mv3), vec![mv2.id]);
    assert!(mview_ids(&base).contains(&mv1.id));
    assert!(mview_ids(&mv1).contains(&mv2.id));
    assert!(mview_ids(&mv2).contains(&mv3.id));

    exec_err_contains(&mut tk, "drop materialized view mv_dependency_chain_1", "DROP MATERIALIZED VIEW with materialized view dependencies");
    exec_err_contains(&mut tk, "drop materialized view mv_dependency_chain_2", "DROP MATERIALIZED VIEW with materialized view dependencies");

    must_exec_all(&mut tk, &[
        "drop materialized view mv_dependency_chain_3",
        "drop materialized view log on mv_dependency_chain_2",
        "drop materialized view mv_dependency_chain_2",
        "drop materialized view log on mv_dependency_chain_1",
        "drop materialized view mv_dependency_chain_1",
        "drop materialized view log on mv_dependency_chain_base",
        "drop table mv_dependency_chain_base",
    ]);
}

/// Go master `TestCreateMaterializedViewOnMaterializedViewRequiresTrackedColumns`.
#[test]
fn test_create_materialized_view_on_materialized_view_requires_tracked_columns() {
    let (_stack, _users, mut tk) = go_master_mview_stack();
    must_exec_all(&mut tk, &[
        "set tidb_mview_enable = on",
        "use test",
        "create table mv_on_mv_missing_log_col_base (a int not null, b int not null)",
        "insert into mv_on_mv_missing_log_col_base values (1, 10)",
        "create materialized view log on mv_on_mv_missing_log_col_base (a, b)",
        "create materialized view mv_parent_missing_log_col (a, cnt) as select a, count(1) from mv_on_mv_missing_log_col_base group by a",
    ]);
    exec_err_contains(
        &mut tk,
        "create materialized view mv_child_missing_log_col (a, parent_cnt, child_cnt) as select a, sum(cnt), count(1) from mv_parent_missing_log_col group by a",
        "materialized view log does not exist for source object test.mv_parent_missing_log_col",
    );
    must_exec_all(&mut tk, &["create materialized view log on mv_parent_missing_log_col (a)"]);
    exec_err_contains(
        &mut tk,
        "create materialized view mv_child_missing_log_col (a, parent_cnt, child_cnt) as select a, sum(cnt), count(1) from mv_parent_missing_log_col group by a",
        "materialized view log does not contain column cnt",
    );
}

fn query_rows(session: &mut Session, sql: &str) -> String {
    let answer = answer(session, sql);
    assert!(answer.starts_with("RS:"), "{sql}: {answer}");
    answer
}

/// Go master `TestMaterializedViewRefreshCompleteOutOfPlaceReplacesMLog`
/// (`pkg/executor/test/mviewnested/nested_oop_test.go`).
#[test]
fn test_materialized_view_refresh_complete_out_of_place_replaces_m_log() {
    let (stack, _users, mut tk) = go_master_mview_stack();
    must_exec_all(&mut tk, &[
        "set tidb_mview_enable = on",
        "create placement policy mv_oop_policy followers=2",
        "alter database test placement policy=mv_oop_policy",
        "use test",
        "create table t (a int not null, b int not null)",
        "insert into t values (1, 10), (2, 20)",
        "create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)",
        "create materialized view mv (a, s, cnt) refresh fast as select a, sum(b), count(1) from t group by a",
        "create materialized view log on mv (a, s, cnt) purge next date_add(now(), interval 1 hour)",
    ]);
    let check_log_placement = |log: &tidb_model::TableInfo| {
        let policy = log.placement_policy_ref.as_ref().expect("the log inherits the database's policy");
        assert_eq!(policy.read().name.lowercase(), "mv_oop_policy");
        let latest = stack.factory.catalog.get_latest();
        let bundle = latest.placement_bundle_by_physical_table_id(log.id).expect("the log's bundle").clone();
        let pd_bundle = tidb_domain_infosync::placement_manager::get_rule_bundle(&bundle.id).unwrap();
        assert_eq!(bundle, pd_bundle);
    };
    let (mut old_mv, mut old_log) = (latest_table(&stack, "mv"), latest_table(&stack, "$mlog$mv"));
    check_log_placement(&old_log);
    must_exec_all(&mut tk, &[&format!("update mysql.tidb_mlog_purge_info set LAST_PURGED_TSO = 1 where MLOG_ID = {}", old_log.id)]);
    for _ in 0..2 {
        must_exec_all(&mut tk, &["refresh materialized view mv complete out of place"]);
        let (new_mv, new_log) = (latest_table(&stack, "mv"), latest_table(&stack, "$mlog$mv"));
        assert_ne!(old_mv.id, new_mv.id);
        assert_ne!(old_log.id, new_log.id);
        assert_eq!(mlog_id(&new_mv), new_log.id);
        let mut expected_log = old_log.materialized_view_log.as_ref().unwrap().read().clone();
        expected_log.base_table_id = new_mv.id;
        assert_eq!(format!("{:?}", *new_log.materialized_view_log.as_ref().unwrap().read()), format!("{expected_log:?}"));
        let names = |table: &tidb_model::TableInfo| -> Vec<String> {
            table.columns.iter_deref().map(|column| column.read().name.original().to_owned()).collect()
        };
        assert_eq!(names(&old_log), names(&new_log));
        let indexes = |table: &tidb_model::TableInfo| -> Vec<String> {
            let mut names: Vec<String> = table.indices.iter_deref().map(|index| index.read().name.original().to_owned()).collect();
            names.sort();
            names
        };
        assert_eq!(indexes(&old_log), indexes(&new_log));
        check_log_placement(&new_log);
        assert_eq!(answer(&mut tk, "select count(*) from `$mlog$mv`"), r#"RS:[["0"]]"#);
        assert_eq!(
            answer(&mut tk, &format!("select count(*) from mysql.tidb_mlog_purge_info where MLOG_ID = {}", old_log.id)),
            r#"RS:[["0"]]"#
        );
        assert_eq!(
            answer(
                &mut tk,
                &format!(
                    "select NEXT_PURGE_UNIX_SECONDS is not null, LAST_PURGED_TSO is null from mysql.tidb_mlog_purge_info where MLOG_ID = {}",
                    new_log.id
                )
            ),
            r#"RS:[["1","1"]]"#
        );
        assert!(stack.factory.catalog.get_latest().table_info_by_id(old_log.id).is_none());
        (old_mv, old_log) = (new_mv, new_log);
    }
    must_exec_all(&mut tk, &["insert into t values (3, 30)", "refresh materialized view mv fast"]);
    assert_eq!(answer(&mut tk, "select a, s, cnt from mv order by a"), r#"RS:[["1","10","1"],["2","20","1"],["3","30","1"]]"#);
    assert_eq!(answer(&mut tk, "select count(*) > 0 from `$mlog$mv`"), r#"RS:[["1"]]"#);
    assert_eq!(answer(&mut tk, r"show tables like '\_\_mv\_shadow\_%'"), "RS:[]");
}

/// Go master `TestMaterializedViewRefreshCompleteOutOfPlaceRebindsDependentMaterializedViews`.
#[test]
fn test_materialized_view_refresh_complete_out_of_place_rebinds_dependent_materialized_views() {
    let (stack, _users, mut tk) = go_master_mview_stack();
    must_exec_all(&mut tk, &[
        "set tidb_mview_enable = on",
        "use test",
        "create table t_oop (a int not null, b int not null)",
        "insert into t_oop values (1, 10), (2, 20)",
        "create materialized view log on t_oop (a, b)",
        "create materialized view mv_oop (a, s, cnt) refresh fast as select a, sum(b), count(*) from t_oop group by a",
        "create materialized view log on mv_oop (a, s, cnt)",
    ]);
    for name in ["child1", "child2"] {
        must_exec_all(&mut tk, &[&format!(
            "create materialized view {name} (a, s, cnt, cnt_s) refresh fast as select a, sum(s), count(*), count(s) from mv_oop group by a"
        )]);
    }
    let (old_mv, old_log) = (latest_table(&stack, "mv_oop"), latest_table(&stack, "$mlog$mv_oop"));
    let (child1, child2) = (latest_table(&stack, "child1"), latest_table(&stack, "child2"));
    let child_ids = format!("{},{}", child1.id, child2.id);
    let child_info_sql = format!("select * from mysql.tidb_mview_refresh_info where MVIEW_ID in ({child_ids}) order by MVIEW_ID");
    let child_sources_sql =
        format!("select * from mysql.tidb_mview_fast_refresh_source_info where MVIEW_ID in ({child_ids}) order by MVIEW_ID, MLOG_ID");
    let (before_info, before_sources) = (query_rows(&mut tk, &child_info_sql), query_rows(&mut tk, &child_sources_sql));
    must_exec_all(&mut tk, &["insert into t_oop values (3, 30)", "refresh materialized view mv_oop complete out of place"]);
    let (new_mv, new_log) = (latest_table(&stack, "mv_oop"), latest_table(&stack, "$mlog$mv_oop"));
    assert_ne!(old_mv.id, new_mv.id);
    assert_ne!(old_log.id, new_log.id);
    let sorted = |mut ids: Vec<i64>| {
        ids.sort_unstable();
        ids
    };
    assert_eq!(sorted(mview_ids(&new_mv)), sorted(vec![child1.id, child2.id]));
    assert_eq!(sorted(dependent_mview_ids(&new_log)), sorted(vec![child1.id, child2.id]));
    for name in ["child1", "child2"] {
        let child = latest_table(&stack, name);
        let base_ids: Vec<i64> = child.materialized_view.as_ref().unwrap().read().base_table_ids.iter().copied().collect();
        assert_eq!(base_ids, vec![new_mv.id]);
        let err = answer(&mut tk, &format!("refresh materialized view {name} fast"));
        assert!(err.starts_with("ERR 8183 "), "{name}: {err}");
        assert_eq!(answer(&mut tk, &format!("select * from {name} order by a")), r#"RS:[["1","10","1","1"],["2","20","1","1"]]"#);
    }
    assert_eq!(query_rows(&mut tk, &child_info_sql), before_info);
    assert_eq!(query_rows(&mut tk, &child_sources_sql), before_sources);
    assert_eq!(answer(&mut tk, "select count(*) from `$mlog$mv_oop`"), r#"RS:[["0"]]"#);

    // "Old physical objects can be collected without deleting child
    // registrations."
    let jobs = answer(&mut tk, "admin show ddl jobs where JOB_TYPE='refresh materialized view complete out-of-place cutover'");
    let jobs: Vec<Vec<Option<String>>> = serde_json::from_str(jobs.strip_prefix("RS:").expect("ddl jobs")).unwrap();
    let job_id = jobs[0][0].clone().unwrap();
    assert_eq!(
        answer(
            &mut tk,
            &format!(
                "select count(*) from (select element_id from mysql.gc_delete_range where job_id={job_id} union all select element_id from mysql.gc_delete_range_done where job_id={job_id}) g"
            )
        ),
        r#"RS:[["2"]]"#
    );

    // "A child with an output log can recover its input with DELTA APPLY
    // while preserving the log consumed by the next level."
    must_exec_all(&mut tk, &[
        "create materialized view log on child1 (a, s, cnt, cnt_s)",
        "create materialized view grandchild (a, s, cnt, cnt_s) refresh fast as select a, sum(s), count(*), count(s) from child1 group by a",
    ]);
    let child_log_id = latest_table(&stack, "$mlog$child1").id;
    must_exec_all(&mut tk, &["refresh materialized view child1 complete delta apply"]);
    assert_eq!(latest_table(&stack, "$mlog$child1").id, child_log_id);
    must_exec_all(&mut tk, &["refresh materialized view grandchild fast"]);
    assert_eq!(
        answer(&mut tk, "select * from grandchild order by a"),
        r#"RS:[["1","10","1","1"],["2","20","1","1"],["3","30","1","1"]]"#
    );
    assert_eq!(
        answer(&mut tk, &format!("select MLOG_ID from mysql.tidb_mview_fast_refresh_source_info where MVIEW_ID={}", child2.id)),
        format!(r#"RS:[["{}"]]"#, old_log.id)
    );

    // "Purge must protect the new log using child2's watermark even though
    // its input registration still names the retired output log."
    must_exec_all(&mut tk, &[
        "insert into t_oop values (1, 5), (4, 40)",
        "update t_oop set b=25 where a=2",
        "delete from t_oop where a=3",
        "refresh materialized view mv_oop fast",
    ]);
    let delta = query_rows(&mut tk, "select * from `$mlog$mv_oop` order by _tidb_rowid");
    assert_ne!(delta, "RS:[]");
    must_exec_all(&mut tk, &["purge materialized view log on mv_oop"]);
    assert_eq!(query_rows(&mut tk, "select * from `$mlog$mv_oop` order by _tidb_rowid"), delta);
    let before_peer = query_rows(
        &mut tk,
        &format!("select LAST_SUCCESS_READ_TSO from mysql.tidb_mview_refresh_info where MVIEW_ID={}", child2.id),
    );
    assert_eq!(
        query_rows(&mut tk, &format!("select LAST_PURGED_TSO from mysql.tidb_mlog_purge_info where MLOG_ID={}", new_log.id)),
        before_peer
    );
    must_exec_all(&mut tk, &["refresh materialized view child1 fast", "refresh materialized view child2 complete in place"]);
    for name in ["child1", "child2"] {
        assert_eq!(
            answer(&mut tk, &format!("select * from {name} order by a")),
            r#"RS:[["1","15","1","1"],["2","25","1","1"],["4","40","1","1"]]"#
        );
        let child = latest_table(&stack, name);
        assert_eq!(
            answer(&mut tk, &format!("select MLOG_ID from mysql.tidb_mview_fast_refresh_source_info where MVIEW_ID={}", child.id)),
            format!(r#"RS:[["{}"]]"#, new_log.id)
        );
    }

    // "OOP of the intermediate view replaces its own output log,
    // invalidating only the next level. The sibling's recovered input
    // remains valid."
    must_exec_all(&mut tk, &["refresh materialized view child1 complete out of place"]);
    assert_ne!(latest_table(&stack, "$mlog$child1").id, child_log_id);
    let err = answer(&mut tk, "refresh materialized view grandchild fast");
    assert!(err.starts_with("ERR 8183 "), "{err}");
    must_exec_all(&mut tk, &["refresh materialized view grandchild complete delta apply", "insert into t_oop values (5, 50)"]);
    for name in ["mv_oop", "child1", "child2", "grandchild"] {
        must_exec_all(&mut tk, &[&format!("refresh materialized view {name} fast")]);
    }
    assert_eq!(
        answer(&mut tk, "select * from grandchild order by a"),
        r#"RS:[["1","15","1","1"],["2","25","1","1"],["4","40","1","1"],["5","50","1","1"]]"#
    );
    must_exec_all(&mut tk, &["drop materialized view grandchild", "drop materialized view log on child1"]);
    for name in ["child1", "child2", "mv_oop"] {
        if name == "mv_oop" {
            must_exec_all(&mut tk, &["drop materialized view log on mv_oop"]);
        }
        must_exec_all(&mut tk, &[&format!("drop materialized view {name}")]);
    }
}

/// Go master `TestAlterMaterializedViewLogAddColumnRefreshConflict`
/// (`pkg/ddl/tests/materializedviewlog/mview_output_log_test.go`): ADD COLUMN
/// on a view's own log while a refresh holds the view's refresh lock fails
/// with 8182 and leaves no intermediate column; after the refresh it runs and
/// the log tracks the new columns.
#[test]
fn test_alter_materialized_view_log_add_column_refresh_conflict() {
    for mode in ["fast", "complete delta apply", "complete out of place"] {
        for columns in ["s", "s, cnt"] {
            alter_mlog_add_column_refresh_conflict(mode, columns);
        }
    }
}

fn alter_mlog_add_column_refresh_conflict(mode: &'static str, columns: &'static str) {
    use std::sync::mpsc;
    let case = format!("{mode}/{columns}");
    let (stack, _users, mut tk) = go_master_mview_stack();
    must_exec_all(&mut tk, &[
        "set tidb_mview_enable = on",
        "use test",
        "create table t_alter_mlog (a int not null, b int not null)",
        "insert into t_alter_mlog values (1, 10)",
        "create materialized view log on t_alter_mlog (a, b)",
        "create materialized view mv_alter_mlog (a, s, cnt) refresh fast as select a, sum(b), count(1) from t_alter_mlog group by a",
        "create materialized view log on mv_alter_mlog (a)",
        "insert into t_alter_mlog values (2, 20)",
    ]);
    let (paused_tx, paused_rx) = mpsc::channel::<()>();
    let (resume_tx, resume_rx) = mpsc::channel::<()>();
    let paused_tx = std::sync::Mutex::new(Some(paused_tx));
    let resume_rx = std::sync::Mutex::new(resume_rx);
    let pause = move || {
        if let Some(paused) = paused_tx.lock().unwrap().take() {
            let _ = paused.send(());
            let _ = resume_rx.lock().unwrap().recv();
        }
    };
    let pause_failpoint = if mode == "complete out of place" {
        "github.com/pingcap/tidb/pkg/executor/refreshMaterializedViewOutOfPlaceAfterBuildDataLoad"
    } else {
        "github.com/pingcap/tidb/pkg/executor/refreshMaterializedViewAfterDataChanges"
    };
    let pause_guard = tidb_failpoint::enable_call_for_test(pause_failpoint, move |_| pause());
    let mut tk_refresh = stack.factory.open_session(session_context(2133)).unwrap();
    must_exec_all(&mut tk_refresh, &["set tidb_mview_enable = on", "use test"]);
    let refresh_done = std::thread::spawn(move || answer(&mut tk_refresh, &format!("refresh materialized view mv_alter_mlog {mode}")));
    paused_rx.recv_timeout(std::time::Duration::from_secs(15)).unwrap_or_else(|_| panic!("{case}: timeout waiting for refresh"));

    let alter_sql = format!("alter materialized view log on mv_alter_mlog add column ({columns})");
    let err = answer(&mut tk, &alter_sql);
    assert!(err.starts_with("ERR 8182 "), "{case}: ADD COLUMN must not complete while refresh holds its lock: {err}");
    assert!(err.contains("materialized view refresh is running"), "{case}: {err}");
    let mlog = latest_table(&stack, "$mlog$mv_alter_mlog");
    let tracked: Vec<String> =
        mlog.materialized_view_log.as_ref().unwrap().read().columns.iter().map(|column| column.original().to_owned()).collect();
    assert_eq!(tracked, vec!["a".to_owned()], "{case}");
    assert_eq!(mlog.columns.len(), 3, "{case}: a rejected ADD COLUMN must leave no intermediate columns");

    let _ = resume_tx.send(());
    assert_eq!(refresh_done.join().unwrap().get(..2), Some("OK"), "{case}: refresh");
    must_exec_all(&mut tk, &[&alter_sql, "insert into t_alter_mlog values (3, 30)"]);
    drop(pause_guard);
    must_exec_all(&mut tk, &["refresh materialized view mv_alter_mlog fast"]);
    assert_eq!(answer(&mut tk, "select a, s from `$mlog$mv_alter_mlog` where a = 3"), r#"RS:[["3","30"]]"#, "{case}");
    must_exec_all(&mut tk, &["update t_alter_mlog set b = 31 where a = 3", "refresh materialized view mv_alter_mlog fast"]);
    assert_eq!(
        answer(&mut tk, "select s from `$mlog$mv_alter_mlog` where a = 3 and `_MLOG$_DML_TYPE` = 'U' order by s"),
        r#"RS:[["30"],["31"]]"#,
        "{case}"
    );
}

/// Go master `6aec16d0234`'s DDL safeguards, replayed from a Go master
/// `2a6eb11aeb9` node (`testdata/mview_ddl_safeguards_go.jsonl`,
/// `tools/e29/p21`): CREATE MATERIALIZED VIEW [LOG] IF NOT EXISTS notes
/// 1050, the view's automatically created group key (a PRIMARY key or the
/// unique key of a nullable group) refuses DROP / RENAME / INVISIBLE, and a
/// plain table named like a shadow drops.
#[test]
fn materialized_view_ddl_safeguards_answer_as_go_master() {
    replay_go_master(include_str!("../../../../../testdata/mview_ddl_safeguards_go.jsonl"));
}

/// Go master's `MAX_COUNT` / `MIN_COUNT` as SQL aggregates and window
/// functions, replayed from a Go master `2a6eb11aeb9` node
/// (`testdata/mview_max_min_count_go.jsonl`, `tools/e29/p22`).
#[test]
fn max_count_and_min_count_answer_as_go_master() {
    replay_go_master(include_str!("../../../../../testdata/mview_max_min_count_go.jsonl"));
}

/// Go master `mustSetMockGCSafePoint`.
fn must_set_mock_gc_safe_point(session: &mut Session, safe_point: chrono::DateTime<chrono::Utc>) {
    let value = safe_point.format("%Y%m%d-%H:%M:%S +0000").to_string();
    let comment = "All versions after safe point can be accessed. (DO NOT EDIT)";
    must_exec_all(session, &[&format!(
        "INSERT INTO mysql.tidb VALUES ('tikv_gc_safe_point', '{value}', '{comment}') ON DUPLICATE KEY UPDATE variable_value = '{value}', comment = '{comment}'"
    )]);
}

/// Go `time.Now().UTC().Add(50 * time.Millisecond).Truncate(time.Millisecond)`,
/// then sleeping past it by 20ms: a target the next writes come after.
fn next_refresh_target() -> chrono::DateTime<chrono::Utc> {
    let now = chrono::Utc::now() + chrono::Duration::milliseconds(50);
    let target = chrono::DateTime::from_timestamp_millis(now.timestamp_millis()).unwrap();
    let wait = (target + chrono::Duration::milliseconds(20)) - chrono::Utc::now();
    if let Ok(wait) = wait.to_std() {
        std::thread::sleep(wait);
    }
    target
}

/// Go `oracle.GoTimeToTS`.
fn go_time_to_ts(time: chrono::DateTime<chrono::Utc>) -> u64 {
    u64::try_from(time.timestamp_millis()).unwrap() << 18
}

fn last_success_read_tso(session: &mut Session, view: &str) -> String {
    answer(
        session,
        &format!(
            "select LAST_SUCCESS_READ_TSO from mysql.tidb_mview_refresh_info where MVIEW_ID = (select tidb_table_id from information_schema.tables where table_schema = 'test' and table_name = '{view}')"
        ),
    )
}

/// Go master `TestMaterializedViewRefreshFastAsOfTimestampMinMaxTargetSnapshot`
/// (`pkg/executor/test/materializedview/materialized_view_refresh_test.go`):
/// an AS OF FAST refresh recomputes MIN / MAX from the base table at the
/// target (`FullUpdateSnapshot`), over the target's schema.
#[test]
fn test_materialized_view_refresh_fast_as_of_timestamp_min_max_target_snapshot() {
    // t.Run("data")
    {
        let (_stack, _users, mut tk) = go_master_mview_stack();
        must_exec_all(&mut tk, &[
            "set tidb_mview_enable = on",
            "use test",
            "set time_zone = '+00:00'",
            "create table t_mv_refresh_asof_minmax_data (a int not null, b int not null, key idx_a(a))",
            "create materialized view log on t_mv_refresh_asof_minmax_data (a, b) purge next date_add(now(), interval 1 hour)",
            "create materialized view mv_refresh_asof_minmax_data (a, cnt, mx, mn) refresh fast as select a, count(1), max(b), min(b) from t_mv_refresh_asof_minmax_data group by a",
            "insert into t_mv_refresh_asof_minmax_data values (1, 10), (1, 20), (1, 30)",
            "refresh materialized view mv_refresh_asof_minmax_data complete delta apply",
        ]);
        assert_eq!(answer(&mut tk, "select * from mv_refresh_asof_minmax_data"), r#"RS:[["1","3","30","10"]]"#);
        must_exec_all(&mut tk, &["delete from t_mv_refresh_asof_minmax_data where a = 1 and b = 30"]);
        let target = next_refresh_target();
        must_exec_all(&mut tk, &["insert into t_mv_refresh_asof_minmax_data values (1, 100)"]);
        must_set_mock_gc_safe_point(&mut tk, target - chrono::Duration::hours(1));
        let refresh = format!(
            "refresh materialized view mv_refresh_asof_minmax_data fast as of timestamp '{}'",
            target.format("%Y-%m-%d %H:%M:%S%.3f")
        );
        assert_eq!(answer(&mut tk, &refresh).get(..2), Some("OK"), "{refresh}");
        assert_eq!(answer(&mut tk, "select * from mv_refresh_asof_minmax_data"), r#"RS:[["1","2","20","10"]]"#);
        assert_eq!(
            last_success_read_tso(&mut tk, "mv_refresh_asof_minmax_data"),
            format!(r#"RS:[["{}"]]"#, go_time_to_ts(target))
        );
    }
    // t.Run("schema")
    {
        let (_stack, _users, mut tk) = go_master_mview_stack();
        must_exec_all(&mut tk, &[
            "set tidb_mview_enable = on",
            "use test",
            "set time_zone = '+00:00'",
            "create table t_mv_refresh_asof_minmax_schema (a int not null, b int not null, key idx_ab(a, b))",
            "create materialized view log on t_mv_refresh_asof_minmax_schema (a, b) purge next date_add(now(), interval 1 hour)",
            "create materialized view mv_refresh_asof_minmax_schema (a, cnt, mx, mn) refresh fast as select a, count(1), max(b), min(b) from t_mv_refresh_asof_minmax_schema group by a",
            "insert into t_mv_refresh_asof_minmax_schema values (1, 10), (1, 20), (1, 30)",
            "refresh materialized view mv_refresh_asof_minmax_schema complete delta apply",
        ]);
        assert_eq!(answer(&mut tk, "select * from mv_refresh_asof_minmax_schema"), r#"RS:[["1","3","30","10"]]"#);
        must_exec_all(&mut tk, &["delete from t_mv_refresh_asof_minmax_schema where a = 1 and b = 30"]);
        let before_invisible = next_refresh_target();
        must_exec_all(&mut tk, &[
            "create index idx_a on t_mv_refresh_asof_minmax_schema (a)",
            "alter table t_mv_refresh_asof_minmax_schema alter index idx_ab invisible",
        ]);
        must_set_mock_gc_safe_point(&mut tk, before_invisible - chrono::Duration::hours(1));
        let refresh = format!(
            "refresh materialized view mv_refresh_asof_minmax_schema fast as of timestamp '{}'",
            before_invisible.format("%Y-%m-%d %H:%M:%S%.3f")
        );
        assert_eq!(answer(&mut tk, &refresh).get(..2), Some("OK"), "{refresh}");
        assert_eq!(answer(&mut tk, "select * from mv_refresh_asof_minmax_schema"), r#"RS:[["1","2","20","10"]]"#);
        assert_eq!(
            last_success_read_tso(&mut tk, "mv_refresh_asof_minmax_schema"),
            format!(r#"RS:[["{}"]]"#, go_time_to_ts(before_invisible))
        );

        must_exec_all(&mut tk, &["delete from t_mv_refresh_asof_minmax_schema where a = 1 and b = 20"]);
        let after_invisible = next_refresh_target();
        must_set_mock_gc_safe_point(&mut tk, after_invisible - chrono::Duration::hours(1));
        let refresh = format!(
            "refresh materialized view mv_refresh_asof_minmax_schema fast as of timestamp '{}'",
            after_invisible.format("%Y-%m-%d %H:%M:%S%.3f")
        );
        assert_eq!(answer(&mut tk, &refresh).get(..2), Some("OK"), "{refresh}");
        assert_eq!(answer(&mut tk, "select * from mv_refresh_asof_minmax_schema"), r#"RS:[["1","1","10","10"]]"#);
        assert_eq!(
            last_success_read_tso(&mut tk, "mv_refresh_asof_minmax_schema"),
            format!(r#"RS:[["{}"]]"#, go_time_to_ts(after_invisible))
        );
    }
}

/// Go master `newMViewRefreshSourceFixture`
/// (`pkg/executor/test/mviewnested/nested_refresh_source_test.go`): the
/// stack, a session on it, and the view's ID.
fn new_mview_refresh_source_fixture() -> (UnistoreClusterStack, Session, i64) {
    let (stack, _users, mut tk) = go_master_mview_stack();
    must_exec_all(&mut tk, &[
        "set tidb_mview_enable = on",
        "use test",
        "create table t_source_init (a int not null, b int not null)",
        "insert into t_source_init values (1, 10), (2, 20)",
        "create materialized view log on t_source_init (a, b)",
        "create materialized view mv_source_init (a, s, cnt) refresh fast as select a, sum(b), count(*) from t_source_init group by a",
    ]);
    let mview_id = latest_table(&stack, "mv_source_init").id;
    (stack, tk, mview_id)
}

/// Go master `TestMaterializedViewRefreshCompleteOutOfPlaceOutputMLogRollbackAndRetry`
/// (`nested_oop_test.go`): a cutover failing after it changed metadata
/// rolls back to the state before the refresh, and an internal retry reuses
/// the persisted job's replacement log ID.
#[test]
fn test_materialized_view_refresh_complete_out_of_place_output_m_log_rollback_and_retry() {
    use std::sync::atomic::{AtomicI64, Ordering};
    for point in [
        "mockMViewRefreshOutOfPlaceCutoverAfterMigrateRefreshInfoError",
        "mockMViewRefreshOutOfPlaceCutoverAfterMetadataError",
    ] {
        let (_stack, mut tk, old_id) = new_mview_refresh_source_fixture();
        must_exec_all(&mut tk, &[
            "create materialized view log on mv_source_init (a, s, cnt)",
            "create materialized view mv_child (a, s, cnt, cnt_s) refresh fast as select a, sum(s), count(*), count(s) from mv_source_init group by a",
        ]);
        let snapshot_sqls = [
            "select * from mysql.tidb_mview_refresh_info order by MVIEW_ID",
            "select * from mysql.tidb_mview_fast_refresh_source_info order by MVIEW_ID, MLOG_ID",
            "select * from mysql.tidb_mlog_purge_info order by MLOG_ID",
            "select TABLE_NAME, TIDB_TABLE_ID from information_schema.tables where TABLE_SCHEMA='test' order by TABLE_NAME",
        ];
        let before: Vec<String> = snapshot_sqls.iter().map(|sql| answer(&mut tk, sql)).collect();
        must_exec_all(&mut tk, &["insert into t_source_init values (3, 30)"]);
        let failpoint = format!("github.com/pingcap/tidb/pkg/ddl/{point}");
        {
            let _fail = tidb_failpoint::enable_for_test(&failpoint, "1*return()");
            let _cancel = tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/afterRunOneJobStep", move |job| {
                let job = job.downcast_mut::<tidb_model::Job>().unwrap();
                if job.type_ == tidb_model::ActionType::ACTION_MVIEW_REFRESH_OUT_OF_PLACE_CUTOVER
                    && job.table_id == old_id
                    && job.error.is_some()
                {
                    job.state = tidb_model::JobState::CANCELLED;
                }
            });
            let refused = answer(&mut tk, "refresh materialized view mv_source_init complete out of place");
            assert!(refused.starts_with("ERR"), "{point}: {refused}");
        }
        let after: Vec<String> = snapshot_sqls.iter().map(|sql| answer(&mut tk, sql)).collect();
        assert_eq!(after, before, "{point}");
        assert_eq!(answer(&mut tk, "select * from mv_source_init order by a"), r#"RS:[["1","10","1"],["2","20","1"]]"#, "{point}");
        must_exec_all(&mut tk, &["refresh materialized view mv_child fast"]);

        // "An internal retry must reuse the log ID allocated in the persisted
        // job."
        let allocated = Arc::new(AtomicI64::new(0));
        let attempts = Arc::new(AtomicI64::new(0));
        let _fail = tidb_failpoint::enable_for_test(&failpoint, "1*return()");
        let _observe = tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep", {
            let (allocated, attempts) = (Arc::clone(&allocated), Arc::clone(&attempts));
            move |job| {
                let job = job.downcast_mut::<tidb_model::Job>().unwrap();
                if job.type_ != tidb_model::ActionType::ACTION_MVIEW_REFRESH_OUT_OF_PLACE_CUTOVER
                    || job.table_id != old_id
                    || job.is_done()
                {
                    return;
                }
                let args = tidb_model::get_refresh_materialized_view_complete_out_of_place_cutover_args(job)
                    .unwrap()
                    .unwrap();
                let new_mlog_id = args.read().new_mlog_id;
                assert!(new_mlog_id > 0);
                let _ = allocated.compare_exchange(0, new_mlog_id, Ordering::AcqRel, Ordering::Acquire);
                assert_eq!(allocated.load(Ordering::Acquire), new_mlog_id);
                attempts.fetch_add(1, Ordering::AcqRel);
            }
        });
        must_exec_all(&mut tk, &["refresh materialized view mv_source_init complete out of place"]);
        assert!(attempts.load(Ordering::Acquire) >= 2, "{point}: attempts {}", attempts.load(Ordering::Acquire));
        assert_eq!(
            answer(
                &mut tk,
                "select TIDB_TABLE_ID from information_schema.tables where TABLE_SCHEMA='test' and TABLE_NAME='$mlog$mv_source_init'"
            ),
            format!(r#"RS:[["{}"]]"#, allocated.load(Ordering::Acquire)),
            "{point}"
        );
        assert_eq!(
            answer(&mut tk, "select * from mv_source_init order by a"),
            r#"RS:[["1","10","1"],["2","20","1"],["3","30","1"]]"#,
            "{point}"
        );
    }
}

/// Go `ddl.MockGC` / `recovertest.mockGC`: the delete-range emulator
/// stopped, so a dropped table keeps its rows, and the GC safe point 48
/// hours back. (Go's `resetGC` restores the emulator; each test here runs
/// in its own process.)
fn mock_gc(session: &mut Session) {
    tidb_ddl_util::emulator_gc_disable();
    let time_before_drop = (chrono::Local::now() - chrono::Duration::hours(48)).format("%Y%m%d-%H:%M:%S %z").to_string();
    must_exec_all(session, &[
        "delete from mysql.tidb where variable_name in ( 'tikv_gc_safe_point','tikv_gc_enable' )",
        &format!(
            "INSERT HIGH_PRIORITY INTO mysql.tidb VALUES ('tikv_gc_safe_point', '{time_before_drop}', '') ON DUPLICATE KEY UPDATE variable_value = '{time_before_drop}'"
        ),
    ]);
}

/// Go `store.GetOracle().GetTimestamp`.
fn oracle_timestamp(session: &mut Session) -> u64 {
    must_exec_all(session, &["begin"]);
    let ts = answer(session, "select @@tidb_current_ts");
    must_exec_all(session, &["rollback"]);
    ts.trim_start_matches("RS:[[\"").trim_end_matches("\"]]").parse().unwrap()
}

/// Go master `TestFlashbackClusterWithMaterializedViews`
/// (`pkg/ddl/cluster_test.go`): a materialized view or log in the current
/// state or at the target refuses FLASHBACK CLUSTER before it prepares any
/// region.
#[test]
fn test_flashback_cluster_with_materialized_views() {
    const CREATE_LOG: &str = "create materialized view log on mv_guard.base (a)";
    const CREATE_MV: &str = "create materialized view mv_guard.mv (a, cnt) as select a, count(*) from mv_guard.base group by a";
    const DROP_LOG: &str = "drop materialized view log on mv_guard.base";
    const DROP_MV: &str = "drop materialized view mv_guard.mv";
    let cases: [(&str, &[&str], &[&str], &str); 7] = [
        (
            "neither snapshot has materialized objects",
            &["create view mv_guard.mv as select a from mv_guard.base", "create table mv_guard.`$mlog$ordinary` (a int)"],
            &[],
            "",
        ),
        ("current materialized view log without dependents", &[], &[CREATE_LOG], "current state"),
        (
            "current materialized views sharing a log",
            &[],
            &[CREATE_LOG, CREATE_MV, "create materialized view mv_guard.mv2 (a, cnt) as select a, count(*) from mv_guard.base group by a"],
            "current state",
        ),
        ("target materialized view log dropped now", &[CREATE_LOG], &[DROP_LOG], "target snapshot"),
        ("target materialized view and log dropped now", &[CREATE_LOG, CREATE_MV], &[DROP_MV, DROP_LOG], "target snapshot"),
        ("target schema dropped now", &[CREATE_LOG, CREATE_MV], &["drop database mv_guard"], "target snapshot"),
        ("materialized objects only between snapshots", &[], &[CREATE_LOG, CREATE_MV, DROP_MV, DROP_LOG], ""),
    ];
    for (name, before, after, snapshot) in cases {
        let (_stack, _users, mut tk) = go_master_mview_stack();
        must_exec_all(&mut tk, &[
            "set tidb_mview_enable=on",
            "create database mv_guard",
            "create table mv_guard.base (a int)",
            "create table test.flashback_guard (a int)",
            "insert into test.flashback_guard values (1)",
        ]);
        must_exec_all(&mut tk, before);
        mock_gc(&mut tk);
        // "Historical DDL-job checks use the physical time of the target TSO."
        std::thread::sleep(std::time::Duration::from_millis(10));
        let ts = oracle_timestamp(&mut tk);
        must_exec_all(&mut tk, after);
        must_exec_all(&mut tk, &["insert into test.flashback_guard values (2)"]);

        let _mock = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/ddl/mockFlashbackTest", "return(true)");
        let safe_ts = u64::try_from((chrono::Utc::now() + chrono::Duration::seconds(10)).timestamp_millis()).unwrap() << 18;
        let _safe = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/ddl/injectSafeTS", &format!("return({safe_ts})"));
        let refused = !snapshot.is_empty();
        let _before = tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep", move |job| {
            let job = job.downcast_mut::<tidb_model::Job>().unwrap();
            if job.type_ == tidb_model::ActionType::ACTION_FLASHBACK_CLUSTER && refused {
                assert_ne!(job.schema_state, tidb_model::SchemaState::WRITE_ONLY, "must reject before preparing flashback regions");
                assert_ne!(job.schema_state, tidb_model::SchemaState::WRITE_REORGANIZATION, "must reject before restoring data");
            }
        });
        let sql = format!("flashback cluster to tso {ts}");
        if !refused {
            assert_eq!(answer(&mut tk, &sql), "OK", "{name}");
            continue;
        }
        let err = answer(&mut tk, &sql);
        for want in ["materialized view", "(ID: ", snapshot] {
            assert!(err.starts_with("ERR") && err.contains(want), "{name}: {err}");
        }
        if snapshot == "target snapshot" {
            assert!(err.contains(&format!("TSO {ts}")), "{name}: {err}");
        }
        assert_eq!(answer(&mut tk, "select * from test.flashback_guard order by a"), r#"RS:[["1"],["2"]]"#, "{name}");
        must_exec_all(&mut tk, &["insert into test.flashback_guard values (3)"]);
    }
}

/// Go master `TestFlashbackTableMaterializedViews` (`pkg/ddl/cluster_test.go`):
/// FLASHBACK TABLE refuses a materialized view, its log and its shadow,
/// current or dropped, and a newer ordinary table of the name still wins.
#[test]
fn test_flashback_table_materialized_views() {
    let (stack, _users, mut tk) = go_master_mview_stack();
    must_exec_all(&mut tk, &["set tidb_mview_enable=on", "use test"]);
    mock_gc(&mut tk);
    must_exec_all(&mut tk, &["create table base (a int not null)", "insert into base values (1)"]);
    // "Older ordinary tables must not be recovered past a newer MV/MLog drop."
    for name in ["mv", "$mlog$base"] {
        must_exec_all(&mut tk, &[
            &format!("create table `{name}` (a int)"),
            &format!("insert into `{name}` values (10)"),
            &format!("drop table `{name}`"),
        ]);
    }
    must_exec_all(&mut tk, &[
        "create materialized view log on base (a)",
        "create materialized view mv (a, cnt) as select a, count(*) from base group by a",
    ]);
    let check_unsupported = |tk: &mut Session, name: &str, object_type: &str| {
        for suffix in ["", " to restored"] {
            assert_eq!(
                answer(tk, &format!("flashback table `{name}`{suffix}")),
                format!("ERR 8200 Unsupported FLASHBACK TABLE on {object_type}"),
                "{name}{suffix}"
            );
        }
    };
    check_unsupported(&mut tk, "mv", "materialized view table");
    check_unsupported(&mut tk, "$mlog$base", "materialized view log table");

    let mv = latest_table(&stack, "mv");
    let mut shadow = mv.clone();
    shadow.id = 0;
    shadow.name = tidb_ast::CiString::new("mv_shadow");
    shadow.materialized_view = None;
    shadow.materialized_view_base = None;
    shadow.materialized_view_shadow = Some(tidb_model::GoShared::new(tidb_model::table::MaterializedViewShadowInfo {
        source_mview_id: mv.id,
        ..Default::default()
    }));
    let schema_id = stack.factory.catalog.get_latest().schema_by_name("test").unwrap().read().id;
    let ddl = std::sync::Arc::clone(&tk.domain_services().unwrap().ddl);
    let context = tk.ddl_statement_context();
    ddl.create_materialized_view_shadow_table(schema_id, "test", shadow, &context).unwrap();
    check_unsupported(&mut tk, "mv_shadow", "materialized view shadow table");
    ddl.drop_materialized_view_shadow_table("test", "mv_shadow", &context).unwrap();
    check_unsupported(&mut tk, "mv_shadow", "materialized view shadow table");

    must_exec_all(&mut tk, &["drop materialized view mv"]);
    check_unsupported(&mut tk, "mv", "materialized view table");
    must_exec_all(&mut tk, &["drop materialized view log on base"]);
    check_unsupported(&mut tk, "$mlog$base", "materialized view log table");
    assert_eq!(answer(&mut tk, "show tables"), r#"RS:[["base"]]"#);

    // "A newer ordinary table must still take precedence over older MV
    // history."
    for name in ["mv", "$mlog$base", "mv_shadow"] {
        must_exec_all(&mut tk, &[
            &format!("create table `{name}` (a int)"),
            &format!("insert into `{name}` values (20)"),
            &format!("drop table `{name}`"),
            &format!("flashback table `{name}` to restored"),
        ]);
        assert_eq!(answer(&mut tk, "select * from restored"), r#"RS:[["20"]]"#, "{name}");
        must_exec_all(&mut tk, &["drop table restored"]);
    }
}

const MV_RECOVER_STATE_QUERIES: [&str; 3] = [
    "select * from mysql.tidb_mview_refresh_info order by mview_id",
    "select * from mysql.tidb_mlog_purge_info order by mlog_id",
    "select * from mysql.tidb_mview_fast_refresh_source_info order by mview_id, mlog_id",
];

fn mv_recover_state(session: &mut Session) -> Vec<String> {
    MV_RECOVER_STATE_QUERIES.iter().map(|sql| answer(session, sql)).collect()
}

/// Go master `TestFlashbackSchemaViews` (`recovertest`): a recovered
/// database's views name it, renamed or not, and keep their other
/// metadata.
#[test]
fn test_flashback_schema_views() {
    for rename in [false, true] {
        let (stack, _users, mut tk) = go_master_mview_stack();
        must_exec_all(&mut tk, &["set tidb_mview_enable=on"]);
        mock_gc(&mut tk);
        must_exec_all(&mut tk, &[
            "create database view_recover",
            "create table view_recover.t (a int not null)",
            "insert into view_recover.t values (1), (2)",
            "create database view_recover_external",
            "create table view_recover_external.t (a int not null)",
            "insert into view_recover_external.t values (10)",
            "set sql_mode = 'ANSI_QUOTES'",
            r#"create algorithm=merge sql security invoker view view_recover.v (a, literal, encoded) as
				select "view_recover"."t"."a", 'view_recover.t', _latin1'view_recover.t' from "view_recover"."t""#,
            "set sql_mode = default",
            "create view view_recover.v_nested as select a from view_recover.v where exists (select 1 from view_recover.t where t.a = v.a)",
            "create view view_recover.v_external as select t.a, e.a as external_a from view_recover.t as t cross join view_recover_external.t as e",
            "create view view_recover.v_star as select view_recover.t.* from view_recover.t",
            "create view view_recover.v_cte as with view_recover as (select a from view_recover.t) select view_recover.a from view_recover union all select a from view_recover_external.t",
        ]);
        let view_of = |db: &str| {
            stack.factory.catalog.get_latest().table_info_by_name(db, "v").unwrap().1.read().view.clone().unwrap().read().clone()
        };
        let original_view_info = view_of("view_recover");
        let check_views = |tk: &mut Session, db: &str, extra_row: bool| {
            let mut rows = vec![r#"["1","view_recover.t","view_recover.t","latin1"]"#, r#"["2","view_recover.t","view_recover.t","latin1"]"#];
            let mut values = vec![r#"["1"]"#, r#"["2"]"#];
            let mut external_rows = vec![r#"["1","10"]"#, r#"["2","10"]"#];
            if extra_row {
                rows.push(r#"["3","view_recover.t","view_recover.t","latin1"]"#);
                values.push(r#"["3"]"#);
                external_rows.push(r#"["3","10"]"#);
            }
            let rs = |rows: &[&str]| format!("RS:[{}]", rows.join(","));
            assert_eq!(answer(tk, &format!("select a, literal, encoded, charset(encoded) from {db}.v order by a")), rs(&rows), "{db}");
            for name in ["v_nested", "v_star"] {
                assert_eq!(answer(tk, &format!("select * from {db}.{name} order by a")), rs(&values), "{db}.{name}");
            }
            assert_eq!(answer(tk, &format!("select * from {db}.v_external order by a")), rs(&external_rows), "{db}");
            let mut cte = values.clone();
            cte.push(r#"["10"]"#);
            assert_eq!(answer(tk, &format!("select * from {db}.v_cte order by a")), rs(&cte), "{db}");
        };
        check_views(&mut tk, "view_recover", false);
        must_exec_all(&mut tk, &["drop database view_recover"]);
        let (db_name, recover_sql) = if rename {
            ("view_recovered", "flashback database view_recover to view_recovered")
        } else {
            ("view_recover", "flashback database view_recover")
        };
        must_exec_all(&mut tk, &[recover_sql]);
        check_views(&mut tk, db_name, false);
        let mut recovered_view_info = view_of(db_name);
        recovered_view_info.select_stmt = original_view_info.select_stmt.clone();
        assert_eq!(serde_json::to_value(&recovered_view_info).unwrap(), serde_json::to_value(&original_view_info).unwrap());
        if rename {
            // "Reusing the old name must not silently redirect recovered
            // views."
            must_exec_all(&mut tk, &[
                "create database view_recover",
                "create table view_recover.t (a int not null)",
                "insert into view_recover.t values (99)",
            ]);
            check_views(&mut tk, db_name, false);
        }
        must_exec_all(&mut tk, &[&format!("insert into {db_name}.t values (3)")]);
        check_views(&mut tk, db_name, true);
        if rename {
            must_exec_all(&mut tk, &[
                &format!("drop database {db_name}"),
                &format!("flashback database {db_name} to view_recovered_again"),
            ]);
            check_views(&mut tk, "view_recovered_again", true);
        }
    }
}

/// Go master `TestFlashbackSchemaMaterializedViews` (`recovertest`): the
/// maintenance rows come back as they stood right before the drop
/// committed, not at its submission, and the views keep refreshing.
#[test]
fn test_flashback_schema_materialized_views() {
    for rename in [false, true] {
        let (stack, _users, mut tk) = go_master_mview_stack();
        must_exec_all(&mut tk, &["set tidb_mview_enable=on"]);
        mock_gc(&mut tk);
        must_exec_all(&mut tk, &[
            "create database mv_recover",
            "use mv_recover",
            "create table t (a int not null)",
            "insert into t values (1), (1), (2)",
            "create materialized view log on t (a)",
            "create materialized view mv1 (a, cnt) as select mv_recover.t.a, count(*) from mv_recover.t group by mv_recover.t.a",
            "create materialized view log on mv1 (a)",
            "create materialized view mv2 (a, cnt) as select a, count(*) from mv1 group by a",
            // "An unrelated database's maintenance state must remain
            // untouched."
            "create database mv_unrelated",
            "create table mv_unrelated.t (a int not null)",
            "create materialized view log on mv_unrelated.t (a)",
            "create materialized view mv_unrelated.mv (a, cnt) as select a, count(*) from mv_unrelated.t group by a",
        ]);
        let old_refresh_state = answer(&mut tk, MV_RECOVER_STATE_QUERIES[0]);
        let db_id = stack.factory.catalog.get_latest().schema_by_name("mv_recover").unwrap().read().id;
        let (started_tx, started_rx) = std::sync::mpsc::channel::<()>();
        let (resume_tx, resume_rx) = std::sync::mpsc::channel::<()>();
        let pause = std::sync::Mutex::new(Some((started_tx, resume_rx)));
        let pause_drop = tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/beforeTransitOneJobStep", move |arg| {
            let job_w = arg.downcast_mut::<tidb_model::JobW>().unwrap();
            let (type_, schema_id) = {
                let job = job_w.job.as_ref().unwrap().read();
                (job.type_, job.schema_id)
            };
            if type_ == tidb_model::ActionType::ACTION_DROP_SCHEMA && schema_id == db_id {
                if let Some((started, resume)) = pause.lock().unwrap().take() {
                    started.send(()).unwrap();
                    resume.recv().unwrap();
                }
            }
        });
        let mut drop_tk = stack_session(&stack);
        let dropping = std::thread::spawn(move || answer(&mut drop_tk, "drop database mv_recover"));
        started_rx.recv_timeout(std::time::Duration::from_secs(10)).expect("DROP DATABASE did not reach its first worker step");
        // "Advance both baselines after the DROP job's StartTS, but before it
        // starts removing the schema. Recovering the submission snapshot is
        // stale."
        must_exec_all(&mut tk, &[
            "insert into t values (2), (3)",
            "refresh materialized view mv1 fast",
            "refresh materialized view mv2 fast",
            "purge materialized view log on t",
            "purge materialized view log on mv1",
            "update mysql.tidb_mview_refresh_info set NEXT_REFRESH_UNIX_SECONDS = 2100000000",
            "update mysql.tidb_mlog_purge_info set NEXT_PURGE_UNIX_SECONDS = 2100000000",
        ]);
        assert_ne!(answer(&mut tk, MV_RECOVER_STATE_QUERIES[0]), old_refresh_state);
        let before = mv_recover_state(&mut tk);
        resume_tx.send(()).unwrap();
        assert_eq!(dropping.join().unwrap(), "OK");
        drop(pause_drop);
        let (db_name, recover_sql) = if rename {
            ("mv_recovered", "flashback database mv_recover to mv_recovered")
        } else {
            ("mv_recover", "flashback database mv_recover")
        };
        must_exec_all(&mut tk, &[recover_sql]);
        assert_eq!(mv_recover_state(&mut tk), before, "rename={rename}");
        must_exec_all(&mut tk, &[&format!("use {db_name}")]);
        assert_eq!(answer(&mut tk, "select * from mv1 order by a"), r#"RS:[["1","2"],["2","2"],["3","1"]]"#);
        assert_eq!(answer(&mut tk, "select * from mv2 order by a"), r#"RS:[["1","1"],["2","1"],["3","1"]]"#);
        must_exec_all(&mut tk, &[
            "insert into t values (1), (4)",
            "refresh materialized view mv1 fast",
            "refresh materialized view mv2 fast",
        ]);
        assert_eq!(answer(&mut tk, "select * from mv1 order by a"), r#"RS:[["1","3"],["2","2"],["3","1"],["4","1"]]"#);
        assert_eq!(answer(&mut tk, "select * from mv2 order by a"), r#"RS:[["1","1"],["2","1"],["3","1"],["4","1"]]"#);
        must_exec_all(&mut tk, &["refresh materialized view mv2 complete in place"]);
        assert_eq!(answer(&mut tk, "select * from mv2 order by a"), r#"RS:[["1","1"],["2","1"],["3","1"],["4","1"]]"#);
        must_exec_all(&mut tk, &["purge materialized view log on t", "purge materialized view log on mv1"]);
    }
}

/// Go master `TestFlashbackSchemaMaterializedViewEscapedDefinition`
/// (`recovertest`): the rewritten definition keeps its literal under the
/// view's SQL mode.
#[test]
fn test_flashback_schema_materialized_view_escaped_definition() {
    for (name, sql_mode, literal) in [
        ("default", "", r"'x\\y'"),
        ("no_backslash_escapes_and_ansi_quotes", "NO_BACKSLASH_ESCAPES,ANSI_QUOTES", r"'x\y'"),
    ] {
        let (stack, _users, mut tk) = go_master_mview_stack();
        must_exec_all(&mut tk, &["set tidb_mview_enable=on"]);
        mock_gc(&mut tk);
        must_exec_all(&mut tk, &[
            "create database mv_recover_escaped",
            "use mv_recover_escaped",
            "create table t (a int not null, s varchar(20) not null)",
            "create materialized view log on t (a, s)",
            &format!("set sql_mode = '{sql_mode}'"),
        ]);
        must_exec_all(&mut tk, &[
            &format!("create materialized view mv (a, cnt) as select a, count(*) from mv_recover_escaped.t where s = {literal} group by a"),
            &format!("create view v as select a from mv_recover_escaped.t where s = {literal}"),
            "set sql_mode = default",
            // "Include a row matching the incorrectly doubled literal as
            // well."
            "insert into t values (1, x'785c79'), (2, x'785c5c79')",
            "refresh materialized view mv fast",
        ]);
        assert_eq!(answer(&mut tk, "select * from mv"), r#"RS:[["1","1"]]"#, "{name}");
        // Go `definitionSQLMode`: the session's mode the view was created
        // under, which the view keeps.
        let created_mode = stack.factory.catalog.get_latest().table_info_by_name("mv_recover_escaped", "mv").unwrap().1.read()
            .materialized_view.as_ref().unwrap().read().definition_sql_mode;
        let mut db_name = "mv_recover_escaped".to_owned();
        for (i, new_name) in ["mv_recovered_escaped", "mv_recovered_escaped_again"].into_iter().enumerate() {
            must_exec_all(&mut tk, &[
                &format!("drop database {db_name}"),
                &format!("flashback database {db_name} to {new_name}"),
            ]);
            db_name = new_name.to_owned();
            must_exec_all(&mut tk, &[&format!("use {db_name}")]);
            let mv = stack.factory.catalog.get_latest().table_info_by_name(&db_name, "mv").unwrap().1.read().clone();
            assert_eq!(mv.materialized_view.as_ref().unwrap().read().definition_sql_mode, created_mode, "{name}");
            assert_eq!(answer(&mut tk, "select * from mv"), format!(r#"RS:[["1","{}"]]"#, i + 1), "{name}");
            must_exec_all(&mut tk, &[
                "insert into t values (1, x'785c79'), (2, x'785c5c79')",
                "refresh materialized view mv fast",
            ]);
            let rows = format!(r#"RS:[["1","{}"]]"#, i + 2);
            assert_eq!(answer(&mut tk, "select * from mv"), rows, "{name}");
            assert_eq!(answer(&mut tk, "select a, count(*) from v group by a"), rows, "{name}");
        }
    }
}

/// Go master `TestFlashbackSchemaMaterializedViewLog` (`recovertest`).
#[test]
fn test_flashback_schema_materialized_view_log() {
    let (_stack, _users, mut tk) = go_master_mview_stack();
    must_exec_all(&mut tk, &["set tidb_mview_enable=on"]);
    mock_gc(&mut tk);
    must_exec_all(&mut tk, &[
        "create database mlog_recover",
        "use mlog_recover",
        "create table t (a int)",
        "create materialized view log on t (a)",
        "insert into t values (1)",
        "purge materialized view log on t",
        "insert into t values (2)",
    ]);
    const STATE_SQL: &str = "select * from mysql.tidb_mlog_purge_info order by mlog_id";
    let before = answer(&mut tk, STATE_SQL);
    must_exec_all(&mut tk, &["drop database mlog_recover", "flashback database mlog_recover"]);
    assert_eq!(answer(&mut tk, STATE_SQL), before);
    must_exec_all(&mut tk, &["use mlog_recover", "insert into t values (3)"]);
    assert_eq!(answer(&mut tk, "select a from `$mlog$t` order by a"), r#"RS:[["2"],["3"]]"#);
    must_exec_all(&mut tk, &["purge materialized view log on t"]);
    assert_eq!(answer(&mut tk, "select count(*) from `$mlog$t`"), r#"RS:[["0"]]"#);
}

/// Go master `TestFlashbackSchemaMaterializedViewAtomicity` (`recovertest`):
/// a recover step failing after it restored the maintenance rows leaves
/// none of them, and its retry restores them once.
#[test]
fn test_flashback_schema_materialized_view_atomicity() {
    use std::sync::atomic::{AtomicBool, Ordering};
    for job_update_error in [false, true] {
        let (stack, _users, mut tk) = go_master_mview_stack();
        must_exec_all(&mut tk, &["set tidb_mview_enable=on"]);
        mock_gc(&mut tk);
        must_exec_all(&mut tk, &[
            "create database mv_recover_atomic",
            "use mv_recover_atomic",
            "create table t (a int not null)",
            "insert into t values (1)",
            "create materialized view log on t (a)",
            "create materialized view mv (a, cnt) as select a, count(*) from t group by a",
        ]);
        let before = mv_recover_state(&mut tk);
        must_exec_all(&mut tk, &["drop database mv_recover_atomic"]);
        let injected = Arc::new(AtomicBool::new(false));
        let _guards: Vec<tidb_failpoint::Guard> = if job_update_error {
            let injected = Arc::clone(&injected);
            vec![tidb_failpoint::enable_call_ref_for_test::<tidb_ddl::cluster_ddl::AfterUpdateJobToTable>(
                "github.com/pingcap/tidb/pkg/ddl/afterUpdateJobToTable",
                Arc::new(move |job, err| {
                    if job.type_ == tidb_model::ActionType::ACTION_RECOVER_SCHEMA && job.is_done() && !injected.swap(true, Ordering::AcqRel) {
                        *err = Some(tidb_ddl_build::plan_error::DdlPlanError::Unsupported(
                            "injected recover schema job update error".to_owned(),
                        ));
                    }
                }),
            )]
        } else {
            let observer = std::sync::Mutex::new(stack_session(&stack));
            let injected = Arc::clone(&injected);
            vec![
                tidb_failpoint::enable_for_test(
                    "github.com/pingcap/tidb/pkg/ddl/mockRecoverSchemaMaterializedViewInfoError",
                    r#"1*return("injected recover schema maintenance error")"#,
                ),
                tidb_failpoint::enable_call_for_test(
                    "github.com/pingcap/tidb/pkg/ddl/afterRollbackTxnForMustRollbackTxnOnError",
                    move |job| {
                        let job = job.downcast_mut::<tidb_model::Job>().unwrap();
                        if job.type_ == tidb_model::ActionType::ACTION_RECOVER_SCHEMA {
                            injected.store(true, Ordering::Release);
                            let mut observer = observer.lock().unwrap();
                            for sql in MV_RECOVER_STATE_QUERIES {
                                assert_eq!(answer(&mut observer, sql), "RS:[]", "{sql}");
                            }
                            assert_eq!(answer(&mut observer, "show databases like 'mv_recover_atomic'"), "RS:[]");
                        }
                    },
                ),
            ]
        };
        must_exec_all(&mut tk, &["flashback database mv_recover_atomic"]);
        assert!(injected.load(Ordering::Acquire), "job_update_error={job_update_error}");
        assert_eq!(mv_recover_state(&mut tk), before, "job_update_error={job_update_error}");
        must_exec_all(&mut tk, &[
            "use mv_recover_atomic",
            "insert into t values (2)",
            "refresh materialized view mv fast",
        ]);
        assert_eq!(answer(&mut tk, "select * from mv order by a"), r#"RS:[["1","1"],["2","1"]]"#);
    }
}

/// Go master `TestFlashbackSchemaMaterializedViewLogBaseline`
/// (`recovertest`): a child's baseline on a replaced log survives recovery,
/// so FAST still refuses it until a COMPLETE refresh.
#[test]
fn test_flashback_schema_materialized_view_log_baseline() {
    let (_stack, _users, mut tk) = go_master_mview_stack();
    must_exec_all(&mut tk, &["set tidb_mview_enable=on"]);
    mock_gc(&mut tk);
    must_exec_all(&mut tk, &[
        "create database mv_recover_baseline",
        "use mv_recover_baseline",
        "create table t (a int not null)",
        "insert into t values (1)",
        "create materialized view log on t (a)",
        "create materialized view mv1 (a, cnt) as select a, count(*) from t group by a",
        "create materialized view log on mv1 (a)",
        "create materialized view mv2 (a, cnt) as select a, count(*) from mv1 group by a",
        // "Cutover replaces the parent's log. The child's old baseline must
        // survive recovery so FAST still rejects it until a COMPLETE refresh
        // establishes a new one."
        "refresh materialized view mv1 complete out of place",
    ]);
    const SOURCES_SQL: &str = "select * from mysql.tidb_mview_fast_refresh_source_info order by mview_id, mlog_id";
    let before = answer(&mut tk, SOURCES_SQL);
    assert!(answer(&mut tk, "refresh materialized view mv2 fast").starts_with("ERR 8183 "));
    must_exec_all(&mut tk, &["drop database mv_recover_baseline", "flashback database mv_recover_baseline"]);
    assert_eq!(answer(&mut tk, SOURCES_SQL), before);
    must_exec_all(&mut tk, &["use mv_recover_baseline"]);
    assert!(answer(&mut tk, "refresh materialized view mv2 fast").starts_with("ERR 8183 "));
    must_exec_all(&mut tk, &[
        "refresh materialized view mv2 complete in place",
        "insert into t values (2)",
        "refresh materialized view mv1 fast",
        "refresh materialized view mv2 fast",
    ]);
    assert_eq!(answer(&mut tk, "select * from mv2 order by a"), r#"RS:[["1","1"],["2","1"]]"#);
}

/// Go master `TestFlashbackSchemaMaterializedViewMissingMaintenance`
/// (`recovertest`): a missing maintenance row cancels the recovery, which
/// leaves no database and no rows.
#[test]
fn test_flashback_schema_materialized_view_missing_maintenance() {
    let tables = ["tidb_mview_refresh_info", "tidb_mlog_purge_info", "tidb_mview_fast_refresh_source_info"];
    for table in tables {
        let (_stack, _users, mut tk) = go_master_mview_stack();
        must_exec_all(&mut tk, &["set tidb_mview_enable=on"]);
        mock_gc(&mut tk);
        must_exec_all(&mut tk, &[
            "create database mv_recover_missing",
            "use mv_recover_missing",
            "create table t (a int not null)",
            "create materialized view log on t (a)",
            "create materialized view mv (a, cnt) as select a, count(*) from t group by a",
            &format!("delete from mysql.{table}"),
            "drop database mv_recover_missing",
        ]);
        exec_err_contains(
            &mut tk,
            "flashback database mv_recover_missing",
            &format!("maintenance records missing from mysql.{table}"),
        );
        assert_eq!(answer(&mut tk, "show databases like 'mv_recover_missing'"), "RS:[]", "{table}");
        for state_table in tables {
            assert_eq!(answer(&mut tk, &format!("select count(*) from mysql.{state_table}")), r#"RS:[["0"]]"#, "{table}");
        }
    }
}

/// Another session on `stack` (Go `testkit.NewTestKit(t, store)`).
fn stack_session(stack: &UnistoreClusterStack) -> Session {
    stack.factory.open_session(session_context(2140)).unwrap()
}

/// Go master `TestMaterializedViewOutputLogModifyColumn`
/// (`pkg/ddl/tests/materializedviewlog/mview_output_column_test.go`):
/// MODIFY COLUMN of a base retypes its log, its dependent view's direct
/// output columns and that view's own log.
#[test]
fn test_materialized_view_output_log_modify_column() {
    let (_stack, _users, mut tk) = go_master_mview_stack();
    must_exec_all(&mut tk, &[
        "set tidb_mview_enable = on",
        "use test",
        "create table t_output_type (a int not null, b int not null)",
        "insert into t_output_type values (1, 10)",
        "create materialized view log on t_output_type (a, b)",
        "create materialized view mv_output_type (k, l, cnt) as select a, b, count(1) from t_output_type group by a, b",
        "create materialized view log on mv_output_type (k, l)",
        "alter table t_output_type modify column a bigint not null, modify column b bigint not null",
    ]);
    assert_eq!(
        answer(
            &mut tk,
            "select table_name, column_name, data_type from information_schema.columns where table_schema='test' and table_name in ('t_output_type', '$mlog$t_output_type', 'mv_output_type', '$mlog$mv_output_type') and column_name in ('a', 'b', 'k', 'l') order by table_name, ordinal_position"
        ),
        r#"RS:[["$mlog$mv_output_type","k","bigint"],["$mlog$mv_output_type","l","bigint"],["$mlog$t_output_type","a","bigint"],["$mlog$t_output_type","b","bigint"],["mv_output_type","k","bigint"],["mv_output_type","l","bigint"],["t_output_type","a","bigint"],["t_output_type","b","bigint"]]"#
    );
    must_exec_all(&mut tk, &[
        "insert into t_output_type values (2147483648, 2147483649)",
        "refresh materialized view mv_output_type fast",
    ]);
    assert_eq!(
        answer(&mut tk, "select k, l, cnt from mv_output_type order by k"),
        r#"RS:[["1","10","1"],["2147483648","2147483649","1"]]"#
    );
    assert_eq!(answer(&mut tk, "select k, l from `$mlog$mv_output_type`"), r#"RS:[["2147483648","2147483649"]]"#);
}

/// Go master `TestMaterializedViewOutputLogModifyColumnBatchRollback`
/// (`mview_output_column_test.go`): a batched MODIFY COLUMN sub-job failing
/// after an earlier one staged the base, view and log changes discards the
/// whole step (`mustRollbackTxnOnError`), and the job rolls back with its
/// restored sub-jobs.
#[test]
fn test_materialized_view_output_log_modify_column_batch_rollback() {
    use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
    let (stack, _users, mut tk) = go_master_mview_stack();
    must_exec_all(&mut tk, &[
        "set tidb_mview_enable = on",
        "use test",
        "create table t_output_batch (a int not null, b int not null)",
        "create materialized view log on t_output_batch (a, b)",
        "create materialized view mv_output_batch (k, l, cnt) as select a, b, count(1) from t_output_batch group by a, b",
        "create materialized view log on mv_output_batch (k, l)",
    ]);
    let base_id = latest_table(&stack, "t_output_batch").id;
    let injected = Arc::new(AtomicBool::new(false));
    let observed = Arc::new(AtomicBool::new(false));
    let failed_job_id = Arc::new(AtomicI64::new(0));
    let fail_guard: Arc<std::sync::Mutex<Option<tidb_failpoint::Guard>>> = Arc::default();
    let _before = tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/beforeBatchedMultiSchemaParentJobUpdate", {
        let (injected, failed_job_id, fail_guard) = (Arc::clone(&injected), Arc::clone(&failed_job_id), Arc::clone(&fail_guard));
        move |jobs| {
            let (parent, proxy) = jobs.downcast_mut::<(tidb_model::Job, tidb_model::Job)>().unwrap();
            if parent.table_id == base_id
                && proxy.type_ == tidb_model::ActionType::ACTION_MODIFY_COLUMN
                && !injected.swap(true, Ordering::AcqRel)
            {
                // "The first sub-job has staged parent/MV/MLog metadata. Fail
                // the second."
                failed_job_id.store(parent.id, Ordering::Release);
                *fail_guard.lock().unwrap() = Some(tidb_failpoint::enable_for_test(
                    "github.com/pingcap/tidb/pkg/ddl/mockUpdateVersionAndTableInfoErr",
                    "1*return(1)",
                ));
            }
        }
    });
    let _after = tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/afterRollbackTxnForMustRollbackTxnOnError", {
        let (observed, failed_job_id) = (Arc::clone(&observed), Arc::clone(&failed_job_id));
        move |job| {
            let job = job.downcast_mut::<tidb_model::Job>().unwrap();
            if job.type_ == tidb_model::ActionType::ACTION_MULTI_SCHEMA_CHANGE && job.id == failed_job_id.load(Ordering::Acquire) {
                observed.store(true, Ordering::Release);
            }
        }
    });
    exec_err_contains(
        &mut tk,
        "alter table t_output_batch modify column a bigint not null, modify column b bigint not null",
        "mock update version and tableInfo error",
    );
    assert!(injected.load(Ordering::Acquire));
    assert!(observed.load(Ordering::Acquire), "the failed batch must discard its whole transaction");
    assert_eq!(
        answer(
            &mut tk,
            "select distinct data_type from information_schema.columns where table_schema='test' and table_name in ('t_output_batch', '$mlog$t_output_batch', 'mv_output_batch', '$mlog$mv_output_batch') and column_name in ('a', 'b', 'k', 'l')"
        ),
        r#"RS:[["int"]]"#
    );
    let job_meta = answer(
        &mut tk,
        &format!("select hex(job_meta) from mysql.tidb_ddl_history where job_id = {}", failed_job_id.load(Ordering::Acquire)),
    );
    let hex = job_meta.trim_start_matches("RS:[[\"").trim_end_matches("\"]]");
    let bytes: Vec<u8> = (0..hex.len()).step_by(2).map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap()).collect();
    let mut history = tidb_model::Job::default();
    history.decode(&bytes).unwrap();
    assert_eq!(history.state, tidb_model::JobState::ROLLBACK_DONE);
    let info = history.multi_schema_info.as_ref().expect("multi-schema info").read().clone();
    assert!(info.revertible);
    assert_eq!(info.sub_jobs.len(), 2);
    // "The restored sub-jobs cancel from SchemaStateNone without publishing
    // changes."
    for (i, sub) in info.sub_jobs.iter_deref().enumerate() {
        let sub = sub.read();
        assert_eq!(sub.state, tidb_model::JobState::CANCELLED, "sub-job {i}");
        assert_eq!(sub.schema_state, tidb_model::SchemaState::NONE, "sub-job {i}");
    }
    let types = "select distinct data_type from information_schema.columns where table_schema='test' and column_name in ('a', 'b', 'k', 'l')";
    assert_eq!(answer(&mut tk, types), r#"RS:[["int"]]"#);
    must_exec_all(&mut tk, &["alter table t_output_batch modify column a bigint not null, modify column b bigint not null"]);
    assert_eq!(answer(&mut tk, types), r#"RS:[["bigint"]]"#);
}

/// The latest finished DDL job (Go `admin show ddl jobs 1` then
/// `GetHistoryJobByID`).
fn latest_history_job(session: &mut Session) -> tidb_model::Job {
    let meta = answer(session, "select hex(job_meta) from mysql.tidb_ddl_history order by job_id desc limit 1");
    let hex = meta.trim_start_matches("RS:[[\"").trim_end_matches("\"]]");
    let bytes: Vec<u8> = (0..hex.len()).step_by(2).map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap()).collect();
    let mut job = tidb_model::Job::default();
    job.decode(&bytes).unwrap();
    job
}

/// Go master `TestNestedMaterializedViewModifyColumn`
/// (`pkg/executor/test/mviewnested/nested_column_test.go`).
#[test]
fn test_nested_materialized_view_modify_column() {
    for multi_column in [false, true] {
        let (_stack, _users, mut tk) = go_master_mview_stack();
        must_exec_all(&mut tk, &[
            "set tidb_mview_enable = on",
            "use test",
            "create table t (a int not null, b int not null)",
            "insert into t values (1, 10)",
            "create materialized view log on t (a, b)",
            "create materialized view mv1 (k, l, cnt) as select a, b, count(1) from t group by a, b",
            "create materialized view log on mv1 (k, l)",
            "create materialized view mv2 (x, y, cnt) as select k, l, count(1) from mv1 group by k, l",
            "create materialized view sibling (q, r, cnt) as select k, l, count(1) from mv1 group by k, l",
            "create materialized view log on mv2 (x, y)",
            "create materialized view mv3 (i, j, cnt) as select x, y, count(1) from mv2 group by x, y",
            "create materialized view log on mv3 (i, j)",
            if multi_column {
                "alter table t modify column a bigint not null, modify column b bigint not null"
            } else {
                "alter table t change column a a bigint not null"
            },
        ]);
        let second = if multi_column { "bigint" } else { "int" };
        let want: Vec<String> = [
            ("$mlog$mv1", "k", "bigint"), ("$mlog$mv1", "l", second),
            ("$mlog$mv2", "x", "bigint"), ("$mlog$mv2", "y", second),
            ("$mlog$mv3", "i", "bigint"), ("$mlog$mv3", "j", second),
            ("$mlog$t", "a", "bigint"), ("$mlog$t", "b", second),
            ("mv1", "k", "bigint"), ("mv1", "l", second),
            ("mv2", "x", "bigint"), ("mv2", "y", second),
            ("mv3", "i", "bigint"), ("mv3", "j", second),
            ("sibling", "q", "bigint"), ("sibling", "r", second),
            ("t", "a", "bigint"), ("t", "b", second),
        ]
        .iter()
        .map(|(table, column, ty)| format!(r#"["{table}","{column}","{ty}"]"#))
        .collect();
        assert_eq!(
            answer(
                &mut tk,
                "select table_name, column_name, data_type from information_schema.columns where table_schema = 'test' and column_name in ('a', 'b', 'k', 'l', 'x', 'y', 'i', 'j', 'q', 'r') order by table_name, ordinal_position"
            ),
            format!("RS:[{}]", want.join(",")),
            "multi_column={multi_column}"
        );
        let job = latest_history_job(&mut tk);
        let mut involving: Vec<String> = job
            .get_involving_schema_info()
            .snapshot()
            .iter()
            .map(|info| format!("{}.{}", info.database, info.table))
            .collect();
        involving.sort();
        involving.dedup();
        assert_eq!(
            involving,
            ["test.$mlog$mv1", "test.$mlog$mv2", "test.$mlog$mv3", "test.$mlog$t", "test.mv1", "test.mv2", "test.mv3", "test.sibling", "test.t"],
            "multi_column={multi_column}"
        );
        must_exec_all(&mut tk, &[
            "insert into t values (2147483648, 20)",
            "refresh materialized view mv1 fast",
            "refresh materialized view mv2 fast",
            "refresh materialized view mv3 fast",
            "refresh materialized view sibling fast",
        ]);
        assert_eq!(answer(&mut tk, "select * from mv3 order by i"), r#"RS:[["1","10","1"],["2147483648","20","1"]]"#);
        assert_eq!(answer(&mut tk, "select * from sibling order by q"), r#"RS:[["1","10","1"],["2147483648","20","1"]]"#);
    }
}

/// Go master `TestNestedMaterializedViewModifyColumnUnreferencedOutput`.
#[test]
fn test_nested_materialized_view_modify_column_unreferenced_output() {
    let (_stack, _users, mut tk) = go_master_mview_stack();
    must_exec_all(&mut tk, &[
        "set tidb_mview_enable = on",
        "use test",
        "create table t (a int not null)",
        "insert into t values (1)",
        "create materialized view log on t (a)",
        "create materialized view mv1 (k, cnt) as select a, count(1) from t group by a",
        "create materialized view log on mv1 (k, cnt)",
        "create materialized view mv2 (x, cnt) as select k, count(1) from mv1 group by k",
        "create materialized view unaffected (c, n) as select cnt, count(1) from mv1 where cnt > 0 group by cnt",
        "create materialized view log on unaffected (c)",
        "create materialized view unaffected_nested (z, n) as select c, count(1) from unaffected where c > 0 group by c",
    ]);
    const UNAFFECTED_SQL: &str = "select table_name, column_name, column_type from information_schema.columns where table_schema = 'test' and table_name in ('unaffected', '$mlog$unaffected', 'unaffected_nested') order by table_name, ordinal_position";
    let before = answer(&mut tk, UNAFFECTED_SQL);
    must_exec_all(&mut tk, &["alter table t modify column a bigint not null"]);
    assert_eq!(
        answer(
            &mut tk,
            "select table_name, column_name, data_type from information_schema.columns where table_schema = 'test' and column_name in ('a', 'k', 'x') order by table_name, ordinal_position"
        ),
        r#"RS:[["$mlog$mv1","k","bigint"],["$mlog$t","a","bigint"],["mv1","k","bigint"],["mv2","x","bigint"],["t","a","bigint"]]"#
    );
    assert_eq!(answer(&mut tk, UNAFFECTED_SQL), before);
}

/// Go master `TestNestedMaterializedViewModifyColumnRejectsDownstreamUsage`,
/// its `preflight` mode (the `worker` modes build the job before the
/// dependency exists through Go's `ExecutorForTest.DoDDLJobWrapper`).
#[test]
fn test_nested_materialized_view_modify_column_rejects_downstream_usage() {
    for (name, column, columns, select_sql, err) in [
        ("where", "a", "(z, cnt)", "select x, count(1) from mv2 where x > 0 group by x", "WHERE clause"),
        ("aggregate", "b", "(z, total, cnt)", "select x, sum(y), count(1) from mv2 group by x", "non-direct SELECT expressions"),
    ] {
        let (_stack, _users, mut tk) = go_master_mview_stack();
        must_exec_all(&mut tk, &[
            "set tidb_mview_enable = on",
            "use test",
            "create table t (a int not null, b int not null)",
            "insert into t values (1, 10)",
            "create materialized view log on t (a, b)",
            "create materialized view mv1 (k, l, cnt) as select a, b, count(1) from t group by a, b",
            "create materialized view log on mv1 (k, l)",
            "create materialized view mv2 (x, y, cnt) as select k, l, count(1) from mv1 group by k, l",
            "create materialized view log on mv2 (x, y)",
            &format!("create materialized view mv3 {columns} as {select_sql}"),
        ]);
        const TYPES_SQL: &str = "select table_name, column_name, column_type from information_schema.columns where table_schema = 'test' order by table_name, ordinal_position";
        let before = answer(&mut tk, TYPES_SQL);
        exec_err_contains(&mut tk, &format!("alter table t modify column {column} bigint not null"), err);
        assert_eq!(answer(&mut tk, TYPES_SQL), before, "{name}");
        assert_eq!(answer(&mut tk, "select * from mv2"), r#"RS:[["1","10","1"]]"#, "{name}");
    }
}

/// Go master `TestMaterializedViewNestingDepthLimit`.
#[test]
fn test_materialized_view_nesting_depth_limit() {
    let (stack, _users, mut tk) = go_master_mview_stack();
    must_exec_all(&mut tk, &[
        "set tidb_mview_enable = on",
        "use test",
        "create table mv_depth_base (a int not null)",
        "insert into mv_depth_base values (1), (1), (2)",
    ]);
    let mut source = "mv_depth_base".to_owned();
    for depth in 1..=16 {
        let name = format!("mv_depth_{depth}");
        must_exec_all(&mut tk, &[
            &format!("create materialized view log on {source} (a)"),
            &format!("create materialized view {name} (a, cnt) refresh fast as select a, count(1) from {source} group by a"),
        ]);
        source = name;
    }
    assert_eq!(answer(&mut tk, "select * from mv_depth_16 order by a"), r#"RS:[["1","1"],["2","1"]]"#);
    must_exec_all(&mut tk, &["create materialized view log on mv_depth_16 (a)"]);
    exec_err_contains(
        &mut tk,
        "create materialized view mv_depth_17 (a, cnt) refresh fast as select a, count(1) from mv_depth_16 group by a",
        "materialized view nesting depth exceeds maximum of 16",
    );
    assert_eq!(answer(&mut tk, "show tables like 'mv_depth_17'"), "RS:[]");
    assert_eq!(answer(&mut tk, "select * from mv_depth_1 order by a"), r#"RS:[["1","2"],["2","1"]]"#);
    assert_eq!(answer(&mut tk, "select * from mv_depth_16 order by a"), r#"RS:[["1","1"],["2","1"]]"#);
    assert!(mview_ids(&latest_table(&stack, "mv_depth_16")).is_empty());
}

/// One cell of a one-row answer.
fn single_value(session: &mut Session, sql: &str) -> String {
    let answer = answer(session, sql);
    let rows: Vec<Vec<serde_json::Value>> =
        serde_json::from_str(answer.strip_prefix("RS:").unwrap_or_else(|| panic!("{sql}: {answer}"))).unwrap();
    assert_eq!(rows.len(), 1, "{sql}: {answer}");
    rows[0][0].as_str().unwrap_or_default().to_owned()
}

/// Go master `TestPurgeMaterializedViewLogPreservesNestedChildDelta`
/// (`nested_purge_test.go`): PURGE of a view's log stops at its child's
/// last read, keeping the delta the child has not consumed.
#[test]
fn test_purge_materialized_view_log_preserves_nested_child_delta() {
    let (stack, _users, mut tk) = go_master_mview_stack();
    must_exec_all(&mut tk, &[
        "set tidb_mview_enable = on",
        "use test",
        "create table t_nested_purge (a int not null, b int not null, v int not null)",
        "insert into t_nested_purge values (1, 1, 10), (2, 1, 20)",
        "create materialized view log on t_nested_purge (a, b, v)",
        "create materialized view mv_nested_purge_parent (a, b, s, cnt) refresh fast as select a, b, sum(v), count(1) from t_nested_purge group by a, b",
        "create materialized view log on mv_nested_purge_parent (a, b, s, cnt)",
        "create materialized view mv_nested_purge_child (a, s, cnt, cnt_s) refresh fast as select a, sum(s), count(1), count(s) from mv_nested_purge_parent group by a",
    ]);
    let parent_mlog_id = latest_table(&stack, "$mlog$mv_nested_purge_parent").id;
    let child_id = latest_table(&stack, "mv_nested_purge_child").id;
    let child_read_tso: u64 =
        single_value(&mut tk, &format!("select LAST_SUCCESS_READ_TSO from mysql.tidb_mview_refresh_info where MVIEW_ID = {child_id}")).parse().unwrap();
    let count = |tk: &mut Session| -> i64 { single_value(tk, "select count(*) from `$mlog$mv_nested_purge_parent`").parse().unwrap() };
    let before_delta = count(&mut tk);
    must_exec_all(&mut tk, &["insert into t_nested_purge values (1, 2, 7)", "refresh materialized view mv_nested_purge_parent fast"]);
    let after_parent_refresh = count(&mut tk);
    assert!(after_parent_refresh > before_delta);
    // "The child has not consumed the parent refresh yet. PURGE must stop at
    // the child's LAST_SUCCESS_READ_TSO and retain the parent delta for the
    // subsequent FAST refresh."
    must_exec_all(&mut tk, &["purge materialized view log on mv_nested_purge_parent"]);
    assert_eq!(count(&mut tk), after_parent_refresh);
    assert_eq!(
        single_value(&mut tk, &format!("select LAST_PURGED_TSO from mysql.tidb_mlog_purge_info where MLOG_ID = {parent_mlog_id}")),
        child_read_tso.to_string()
    );
    must_exec_all(&mut tk, &["refresh materialized view mv_nested_purge_child fast"]);
    assert_eq!(
        answer(&mut tk, "select a, s, cnt, cnt_s from mv_nested_purge_child order by a"),
        r#"RS:[["1","17","2","2"],["2","20","1","1"]]"#
    );
    must_exec_all(&mut tk, &[
        "drop materialized view mv_nested_purge_child",
        "drop materialized view log on mv_nested_purge_parent",
        "drop materialized view mv_nested_purge_parent",
        "drop materialized view log on t_nested_purge",
        "drop table t_nested_purge",
    ]);
}

/// Go master `TestPurgeMaterializedViewLogUsesSlowestNestedChildCheckpoint`
/// (`nested_purge_test.go`).
#[test]
fn test_purge_materialized_view_log_uses_slowest_nested_child_checkpoint() {
    let (stack, _users, mut tk) = go_master_mview_stack();
    must_exec_all(&mut tk, &[
        "set tidb_mview_enable = on",
        "use test",
        "create table t_nested_purge_multi (a int not null, b int not null, v int not null)",
        "insert into t_nested_purge_multi values (1, 1, 10), (2, 1, 20)",
        "create materialized view log on t_nested_purge_multi (a, b, v)",
        "create materialized view mv_nested_purge_parent_multi (a, b, s, cnt) refresh fast as select a, b, sum(v), count(1) from t_nested_purge_multi group by a, b",
        "create materialized view log on mv_nested_purge_parent_multi (a, b, s, cnt)",
    ]);
    let child_names = ["mv_nested_purge_child_1", "mv_nested_purge_child_2"];
    let mut child_ids = Vec::new();
    for name in child_names {
        must_exec_all(&mut tk, &[&format!(
            "create materialized view {name} (a, s, cnt, cnt_s) refresh fast as select a, sum(s), count(1), count(s) from mv_nested_purge_parent_multi group by a"
        )]);
        child_ids.push(latest_table(&stack, name).id);
    }
    let parent_mlog_id = latest_table(&stack, "$mlog$mv_nested_purge_parent_multi").id;
    let read_checkpoint = |tk: &mut Session, child_id: i64| -> u64 {
        let checkpoint: u64 =
            single_value(tk, &format!("select LAST_SUCCESS_READ_TSO from mysql.tidb_mview_refresh_info where MVIEW_ID = {child_id}")).parse().unwrap();
        assert_ne!(checkpoint, 0);
        checkpoint
    };
    let purge_and_check_checkpoint = |tk: &mut Session, expected: u64| {
        must_exec_all(tk, &["purge materialized view log on mv_nested_purge_parent_multi"]);
        assert_eq!(
            single_value(tk, &format!("select LAST_PURGED_TSO from mysql.tidb_mlog_purge_info where MLOG_ID = {parent_mlog_id}")),
            expected.to_string()
        );
    };
    let mut child1_checkpoint = read_checkpoint(&mut tk, child_ids[0]);
    must_exec_all(&mut tk, &[
        "insert into t_nested_purge_multi values (1, 2, 7)",
        "refresh materialized view mv_nested_purge_parent_multi fast",
        "refresh materialized view mv_nested_purge_child_2 fast",
    ]);
    let child2_checkpoint = read_checkpoint(&mut tk, child_ids[1]);
    assert!(child2_checkpoint > child1_checkpoint);
    assert_eq!(answer(&mut tk, "select a, s, cnt, cnt_s from mv_nested_purge_child_1 order by a"), r#"RS:[["1","10","1","1"],["2","20","1","1"]]"#);
    assert_eq!(answer(&mut tk, "select a, s, cnt, cnt_s from mv_nested_purge_child_2 order by a"), r#"RS:[["1","17","2","2"],["2","20","1","1"]]"#);
    // "Put one delta before the faster child's checkpoint and one after it.
    // Neither may be purged yet: child 1 still needs both, even though child
    // 2 has consumed the first."
    must_exec_all(&mut tk, &["insert into t_nested_purge_multi values (3, 1, 8)", "refresh materialized view mv_nested_purge_parent_multi fast"]);
    assert_eq!(
        answer(&mut tk, &format!("select count(*) from `$mlog$mv_nested_purge_parent_multi` where _tidb_commit_ts > {child1_checkpoint} and _tidb_commit_ts <= {child2_checkpoint}")),
        r#"RS:[["1"]]"#
    );
    purge_and_check_checkpoint(&mut tk, child1_checkpoint);
    assert_eq!(answer(&mut tk, "select a, b, s, cnt from `$mlog$mv_nested_purge_parent_multi` order by a, b"), r#"RS:[["1","2","7","1"],["3","1","8","1"]]"#);
    // "Once child 1 catches up, child 2 becomes the slower consumer."
    must_exec_all(&mut tk, &["refresh materialized view mv_nested_purge_child_1 fast"]);
    child1_checkpoint = read_checkpoint(&mut tk, child_ids[0]);
    assert!(child1_checkpoint > child2_checkpoint);
    let final_rows = r#"RS:[["1","17","2","2"],["2","20","1","1"],["3","8","1","1"]]"#;
    assert_eq!(answer(&mut tk, "select a, s, cnt, cnt_s from mv_nested_purge_child_1 order by a"), final_rows);
    purge_and_check_checkpoint(&mut tk, child2_checkpoint);
    assert_eq!(answer(&mut tk, "select a, b, s, cnt from `$mlog$mv_nested_purge_parent_multi` order by a, b"), r#"RS:[["3","1","8","1"]]"#);
    must_exec_all(&mut tk, &["refresh materialized view mv_nested_purge_child_2 fast"]);
    assert!(read_checkpoint(&mut tk, child_ids[1]) > child1_checkpoint);
    assert_eq!(answer(&mut tk, "select a, s, cnt, cnt_s from mv_nested_purge_child_2 order by a"), final_rows);
    purge_and_check_checkpoint(&mut tk, child1_checkpoint);
    assert_eq!(answer(&mut tk, "select count(*) from `$mlog$mv_nested_purge_parent_multi`"), r#"RS:[["0"]]"#);
    for name in child_names {
        must_exec_all(&mut tk, &[&format!("drop materialized view {name}")]);
    }
    must_exec_all(&mut tk, &[
        "drop materialized view log on mv_nested_purge_parent_multi",
        "drop materialized view mv_nested_purge_parent_multi",
        "drop materialized view log on t_nested_purge_multi",
        "drop table t_nested_purge_multi",
    ]);
}

/// Go master `TestNestedMaterializedViewFastRefresh` (`nested_refresh_test.go`).
#[test]
fn test_nested_materialized_view_fast_refresh() {
    for parent_refresh_method in ["fast", "complete delta apply"] {
        let (_stack, _users, mut tk) = go_master_mview_stack();
        must_exec_all(&mut tk, &[
            "set tidb_mview_enable = on",
            "use test",
            "create table t_nested_mv_refresh (a int not null, b int not null, v int)",
            "insert into t_nested_mv_refresh values (1, 1, 10), (1, 1, 5), (1, 2, 7), (2, 1, 4)",
            "create materialized view log on t_nested_mv_refresh (a, b, v)",
            "create materialized view mv_nested_parent (a, b, s, cnt, cnt_v) refresh fast as select a, b, sum(v), count(1), count(v) from t_nested_mv_refresh group by a, b",
            "create materialized view log on mv_nested_parent (a, b, s, cnt)",
            "create materialized view mv_nested_child (a, s, cnt, cnt_s) refresh fast as select a, sum(s), count(1), count(s) from mv_nested_parent group by a",
        ]);
        let child = "select a, s, cnt, cnt_s from mv_nested_child order by a";
        assert_eq!(answer(&mut tk, child), r#"RS:[["1","22","2","2"],["2","4","1","1"]]"#);
        // "These mutations make the parent update one group, delete another,
        // and insert a third."
        must_exec_all(&mut tk, &[
            "update t_nested_mv_refresh set v = 11 where a = 1 and b = 1 and v = 10",
            "delete from t_nested_mv_refresh where a = 1 and b = 2",
            "insert into t_nested_mv_refresh values (3, 1, 8)",
            // "A child refresh only consumes published parent deltas; it does
            // not refresh its parent."
            "refresh materialized view mv_nested_child fast",
        ]);
        assert_eq!(answer(&mut tk, child), r#"RS:[["1","22","2","2"],["2","4","1","1"]]"#, "{parent_refresh_method}");
        must_exec_all(&mut tk, &[
            &format!("refresh materialized view mv_nested_parent {parent_refresh_method}"),
            "refresh materialized view mv_nested_child fast",
        ]);
        assert_eq!(
            answer(&mut tk, "select a, b, s, cnt from mv_nested_parent order by a, b"),
            r#"RS:[["1","1","16","2"],["2","1","4","1"],["3","1","8","1"]]"#,
            "{parent_refresh_method}"
        );
        assert_eq!(answer(&mut tk, child), r#"RS:[["1","16","1","1"],["2","4","1","1"],["3","8","1","1"]]"#, "{parent_refresh_method}");
        // "Moving a parent group and making all SUM inputs NULL exercises both
        // group deletion and nullable aggregate transitions in the child's
        // delta stream."
        must_exec_all(&mut tk, &[
            "update t_nested_mv_refresh set a = 1, b = 3, v = null where a = 3 and b = 1",
            "update t_nested_mv_refresh set v = null where a = 1 and b = 1",
            &format!("refresh materialized view mv_nested_parent {parent_refresh_method}"),
            "refresh materialized view mv_nested_child fast",
        ]);
        assert_eq!(
            answer(&mut tk, "select a, b, s, cnt, cnt_v from mv_nested_parent order by a, b"),
            r#"RS:[["1","1",null,"2","0"],["1","3",null,"1","0"],["2","1","4","1","1"]]"#,
            "{parent_refresh_method}"
        );
        assert_eq!(answer(&mut tk, child), r#"RS:[["1",null,"2","0"],["2","4","1","1"]]"#, "{parent_refresh_method}");
        must_exec_all(&mut tk, &[
            "update t_nested_mv_refresh set v = 9 where a = 1 and b = 3",
            &format!("refresh materialized view mv_nested_parent {parent_refresh_method}"),
            "refresh materialized view mv_nested_child fast",
        ]);
        assert_eq!(answer(&mut tk, child), r#"RS:[["1","9","2","1"],["2","4","1","1"]]"#, "{parent_refresh_method}");
    }
}

/// Go master `TestNestedMaterializedViewFastRefreshMinMax`.
#[test]
fn test_nested_materialized_view_fast_refresh_min_max() {
    let (_stack, _users, mut tk) = go_master_mview_stack();
    must_exec_all(&mut tk, &[
        "set tidb_mview_enable = on",
        "use test",
        "create table t_nested_mv_minmax (a int not null, b int not null, v int not null, key idx_ab (a, b))",
        "insert into t_nested_mv_minmax values (1, 1, 10), (1, 1, 30), (1, 2, 5), (2, 1, 8)",
        "create materialized view log on t_nested_mv_minmax (a, b, v)",
        "create materialized view mv_nested_parent_minmax (a, b, cnt, mx, mn) refresh fast as select a, b, count(1), max(v), min(v) from t_nested_mv_minmax group by a, b",
        "create materialized view log on mv_nested_parent_minmax (a, b, cnt, mx, mn)",
        "create materialized view mv_nested_child_minmax (a, cnt, mx, mn) refresh fast as select a, count(1), max(mx), min(mn) from mv_nested_parent_minmax group by a",
    ]);
    let child = "select a, cnt, mx, mn from mv_nested_child_minmax order by a";
    assert_eq!(answer(&mut tk, child), r#"RS:[["1","2","30","5"],["2","1","8","8"]]"#);
    // "Removing the parent group's MAX forces the child MAX fallback to probe
    // the parent MV by its group-key index."
    must_exec_all(&mut tk, &[
        "delete from t_nested_mv_minmax where a = 1 and b = 1 and v = 30",
        "refresh materialized view mv_nested_parent_minmax fast",
        "refresh materialized view mv_nested_child_minmax fast",
    ]);
    assert_eq!(
        answer(&mut tk, "select a, b, cnt, mx, mn from mv_nested_parent_minmax order by a, b"),
        r#"RS:[["1","1","1","10","10"],["1","2","1","5","5"],["2","1","1","8","8"]]"#
    );
    assert_eq!(answer(&mut tk, child), r#"RS:[["1","2","10","5"],["2","1","8","8"]]"#);
}

/// Go master `TestMaterializedViewRefreshDryRunUsesInternalTxnInfoSchema`.
#[test]
fn test_materialized_view_refresh_dry_run_uses_internal_txn_info_schema() {
    let (_stack, _users, mut tk) = go_master_mview_stack();
    must_exec_all(&mut tk, &[
        "set tidb_mview_enable = on",
        "use test",
        "create table t_mv_dry_run (a int not null, b int not null)",
        "insert into t_mv_dry_run values (1, 10), (2, 20)",
        "create materialized view log on t_mv_dry_run (a, b) purge next date_add(now(), interval 1 hour)",
        "create materialized view mv_dry_run (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t_mv_dry_run group by a",
    ]);
    let last_success_read_tso = answer(&mut tk, "select LAST_SUCCESS_READ_TSO from mysql.tidb_mview_refresh_info");
    must_exec_all(&mut tk, &["insert into t_mv_dry_run values (1, 5)"]);
    for (mode, operator) in [("complete delta apply", "MViewCompleteDeltaApply"), ("fast", "MViewDeltaMerge")] {
        for _ in 0..2 {
            let plan = answer(&mut tk, &format!("refresh materialized view mv_dry_run {mode} dry run"));
            let rows: Vec<Vec<serde_json::Value>> = serde_json::from_str(plan.strip_prefix("RS:").expect(&plan)).unwrap();
            assert!(
                rows.iter().any(|row| row[0].as_str().is_some_and(|id| id.starts_with(&format!("  {operator}")))),
                "{mode}: {plan}"
            );
            assert_eq!(answer(&mut tk, "select * from mv_dry_run order by a"), r#"RS:[["1","10","1"],["2","20","1"]]"#);
            assert_eq!(answer(&mut tk, "select LAST_SUCCESS_READ_TSO from mysql.tidb_mview_refresh_info"), last_success_read_tso);
            assert_eq!(answer(&mut tk, "select count(*) from mysql.tidb_mview_refresh_hist"), r#"RS:[["0"]]"#);
        }
    }
    must_exec_all(&mut tk, &["refresh materialized view mv_dry_run complete delta apply"]);
    assert_eq!(answer(&mut tk, "select * from mv_dry_run order by a"), r#"RS:[["1","15","2"],["2","20","1"]]"#);
}

/// Go master `TestNestedMViewRefreshSourceInfoLifecycle`
/// (`nested_refresh_source_test.go`).
#[test]
fn test_nested_m_view_refresh_source_info_lifecycle() {
    let (stack, _users, mut tk) = go_master_mview_stack();
    must_exec_all(&mut tk, &[
        "set tidb_mview_enable = on",
        "use test",
        "create table t_sources (a int not null, b int not null)",
        "insert into t_sources values (1, 10), (2, 20)",
        "create materialized view log on t_sources (a, b)",
        "create materialized view mv_sources (a, s, cnt) refresh fast as select a, sum(b), count(*) from t_sources group by a",
        "create materialized view log on mv_sources (a, s, cnt)",
        "create materialized view mv_sources_child (a, cnt) refresh fast as select a, count(*) from mv_sources group by a",
    ]);
    let id = |name: &str| latest_table(&stack, name).id;
    let (mv_id, child_id) = (id("mv_sources"), id("mv_sources_child"));
    let (mlog_id, own_log_id) = (id("$mlog$t_sources"), id("$mlog$mv_sources"));
    assert_eq!(
        answer(&mut tk, "select MVIEW_ID, MLOG_ID from mysql.tidb_mview_fast_refresh_source_info order by MVIEW_ID"),
        format!(r#"RS:[["{mv_id}","{mlog_id}"],["{child_id}","{own_log_id}"]]"#)
    );
    must_exec_all(&mut tk, &["drop materialized view mv_sources_child"]);
    assert_eq!(
        answer(&mut tk, "select MVIEW_ID, MLOG_ID from mysql.tidb_mview_fast_refresh_source_info use index(idx_mlog)"),
        format!(r#"RS:[["{mv_id}","{mlog_id}"]]"#)
    );
    must_exec_all(&mut tk, &["drop materialized view log on mv_sources"]);
    assert_eq!(
        answer(&mut tk, "select MVIEW_ID, MLOG_ID from mysql.tidb_mview_fast_refresh_source_info"),
        format!(r#"RS:[["{mv_id}","{mlog_id}"]]"#)
    );
    must_exec_all(&mut tk, &["drop materialized view mv_sources"]);
    assert_eq!(answer(&mut tk, "select count(*) from mysql.tidb_mview_fast_refresh_source_info"), r#"RS:[["0"]]"#);
}

/// Go master `TestNestedMViewRefreshSourceInfoRejectsMissingRecords`.
#[test]
fn test_nested_m_view_refresh_source_info_rejects_missing_records() {
    let (_stack, mut tk, _) = new_mview_refresh_source_fixture();
    must_exec_all(&mut tk, &[
        "create materialized view log on mv_source_init (a, s, cnt)",
        "create materialized view mv_source_child (a, cnt) refresh fast as select a, count(*) from mv_source_init group by a",
    ]);
    const CHILD_ID: &str = "(select TIDB_TABLE_ID from information_schema.tables where table_schema='test' and table_name='mv_source_child')";
    let source_sql = format!("select * from mysql.tidb_mview_fast_refresh_source_info where MVIEW_ID = {CHILD_ID}");
    must_exec_all(&mut tk, &[&format!("delete from mysql.tidb_mview_fast_refresh_source_info where MVIEW_ID = {CHILD_ID}")]);
    let state = |tk: &mut Session| {
        [
            answer(tk, "select * from mysql.tidb_mview_fast_refresh_source_info order by MVIEW_ID, MLOG_ID"),
            answer(tk, "select * from mysql.tidb_mview_refresh_info order by MVIEW_ID"),
            answer(tk, "select * from mv_source_child order by a"),
        ]
    };
    let before = state(&mut tk);
    for method in ["fast", "complete delta apply", "complete in place", "complete out of place"] {
        exec_err_contains(&mut tk, &format!("refresh materialized view mv_source_child {method}"), "input log records are missing");
        assert_eq!(answer(&mut tk, &source_sql), "RS:[]", "{method}");
        assert_eq!(state(&mut tk), before, "{method}");
    }
}

/// Go master `TestNestedMViewRefreshSourceInfoRecoveryAtomicity`.
#[test]
fn test_nested_m_view_refresh_source_info_recovery_atomicity() {
    let (stack, mut tk, mview_id) = new_mview_refresh_source_fixture();
    must_exec_all(&mut tk, &[
        "create materialized view log on mv_source_init (a, s, cnt)",
        "create materialized view mv_source_child (a, cnt) refresh fast as select a, count(*) from mv_source_init group by a",
        "create materialized view mv_source_peer (a, s, cnt) refresh fast as select a, sum(b), count(*) from t_source_init group by a",
        &format!("update mysql.tidb_mview_fast_refresh_source_info set MLOG_ID = -1 where MVIEW_ID = {mview_id}"),
    ]);
    let refresh_info = "select * from mysql.tidb_mview_refresh_info order by MVIEW_ID";
    let sources = "select * from mysql.tidb_mview_fast_refresh_source_info order by MVIEW_ID, MLOG_ID";
    let before = answer(&mut tk, refresh_info);
    let before_sources = answer(&mut tk, sources);
    must_exec_all(&mut tk, &["insert into t_source_init values (1, 5), (3, 7)"]);
    {
        let _fail = tidb_failpoint::enable_for_test(
            "github.com/pingcap/tidb/pkg/executor/mockRefreshMaterializedViewErrorAfterDataChanges",
            r#"return("source recovery rollback")"#,
        );
        exec_err_contains(&mut tk, "refresh materialized view mv_source_init complete delta apply", "source recovery rollback");
    }
    assert_eq!(answer(&mut tk, refresh_info), before);
    assert_eq!(answer(&mut tk, sources), before_sources);
    assert_eq!(answer(&mut tk, "select * from mv_source_init order by a"), r#"RS:[["1","10","1"],["2","20","1"]]"#);
    {
        let _fail = tidb_failpoint::enable_for_test(
            "github.com/pingcap/tidb/pkg/executor/mockRefreshMaterializedViewAfterReplaceSourceInfoError",
            "return()",
        );
        exec_err_contains(&mut tk, "refresh materialized view mv_source_init complete delta apply", "after replacing source info");
    }
    assert_eq!(answer(&mut tk, refresh_info), before);
    assert_eq!(answer(&mut tk, sources), before_sources);
    assert_eq!(answer(&mut tk, "select * from mv_source_init order by a"), r#"RS:[["1","10","1"],["2","20","1"]]"#);
    // "A successful COMPLETE preserves the output log, so the child still
    // FAST refreshes. The peer's input baseline and watermark are unchanged."
    let peer_sql = "select * from mysql.tidb_mview_refresh_info where MVIEW_ID = (select TIDB_TABLE_ID from information_schema.tables where table_schema='test' and table_name='mv_source_peer')";
    let peer_before = answer(&mut tk, peer_sql);
    let computation_ts = Arc::new(std::sync::atomic::AtomicU64::new(0));
    {
        let seen = Arc::clone(&computation_ts);
        let _observe = tidb_failpoint::enable_call_for_test(
            "github.com/pingcap/tidb/pkg/executor/refreshMaterializedViewAfterReplaceSourceInfo",
            move |ts| seen.store(*ts.downcast_mut::<u64>().unwrap(), std::sync::atomic::Ordering::Release),
        );
        must_exec_all(&mut tk, &["refresh materialized view mv_source_init complete delta apply"]);
    }
    let computation_ts = computation_ts.load(std::sync::atomic::Ordering::Acquire);
    assert_ne!(computation_ts, 0);
    assert_eq!(
        answer(&mut tk, &format!("select LAST_SUCCESS_READ_TSO from mysql.tidb_mview_refresh_info where MVIEW_ID = {mview_id}")),
        format!(r#"RS:[["{computation_ts}"]]"#)
    );
    assert_eq!(answer(&mut tk, peer_sql), peer_before);
    must_exec_all(&mut tk, &["refresh materialized view mv_source_child fast"]);
    assert_eq!(answer(&mut tk, "select * from mv_source_child order by a"), r#"RS:[["1","1"],["2","1"],["3","1"]]"#);

    // t.Run("child_baseline_recovery")
    const CHILD_ID: &str = "(select TIDB_TABLE_ID from information_schema.tables where table_schema='test' and table_name='mv_source_child')";
    must_exec_all(&mut tk, &[
        &format!("update mysql.tidb_mview_fast_refresh_source_info set MLOG_ID = -1 where MVIEW_ID = {CHILD_ID}"),
        "insert into t_source_init values (4, 11)",
        "refresh materialized view mv_source_init fast",
    ]);
    let before_child = answer(&mut tk, "select * from mv_source_child order by a");
    let checkpoint_sql = format!("select LAST_SUCCESS_READ_TSO from mysql.tidb_mview_refresh_info where MVIEW_ID = {CHILD_ID}");
    let before_checkpoint = answer(&mut tk, &checkpoint_sql);
    exec_err_contains(&mut tk, "refresh materialized view mv_source_child fast", "run COMPLETE refresh");
    assert_eq!(answer(&mut tk, "select * from mv_source_child order by a"), before_child);
    assert_eq!(answer(&mut tk, &checkpoint_sql), before_checkpoint);
    must_exec_all(&mut tk, &["refresh materialized view mv_source_child complete delta apply"]);
    assert_eq!(
        answer(&mut tk, &format!("select MLOG_ID from mysql.tidb_mview_fast_refresh_source_info where MVIEW_ID = {CHILD_ID}")),
        answer(&mut tk, "select MLOG_ID from information_schema.tidb_mlogs where table_schema='test' and base_table_name='mv_source_init'")
    );
    let _ = &stack;
    assert_eq!(answer(&mut tk, "select * from mv_source_child order by a"), r#"RS:[["1","1"],["2","1"],["3","1"],["4","1"]]"#);
    must_exec_all(&mut tk, &[
        "insert into t_source_init values (5, 13)",
        "refresh materialized view mv_source_init fast",
        "refresh materialized view mv_source_child fast",
    ]);
    assert_eq!(answer(&mut tk, "select * from mv_source_child order by a"), r#"RS:[["1","1"],["2","1"],["3","1"],["4","1"],["5","1"]]"#);
}

/// Go master's `tests/integrationtest/t/executor/infoschema_mv.test`
/// (`bb23c552354`: `TIDB_MVIEWS`, `TIDB_MLOGS` and
/// `TIDB_TABLE_MVIEW_DEPENDENCIES` with their predicate extractors), replayed
/// from a Go master `2a6eb11aeb9` capture (`testdata/mview_infoschema_go.jsonl`).
#[test]
fn materialized_view_infoschema_tables_answer_as_go_master() {
    replay_go_master(include_str!("../../../../../testdata/mview_infoschema_go.jsonl"));
}

/// Go master `TestInformationSchemaMaterializedView`
/// (`pkg/executor/test/materializedview/materialized_view_infoschema_test.go`).
#[test]
fn test_information_schema_materialized_view() {
    // t.Run("TiDBMViews")
    {
        let (stack, _users, mut tk) = go_master_mview_stack();
        must_exec_all(&mut tk, &[
            "set tidb_mview_enable = on",
            "use test",
            "create table t (a int not null, b int not null)",
            "create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)",
            "create materialized view mv_meta (a, s, cnt) comment='mv-meta' refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a",
        ]);
        let mv_id = latest_table(&stack, "mv_meta").id;
        assert_eq!(
            answer(&mut tk, "select table_catalog, table_schema, mview_id, mview_name, mview_comment, refresh_method, refresh_start, refresh_next from information_schema.tidb_mviews where table_schema = 'test' and mview_name = 'mv_meta'"),
            format!(r#"RS:[["def","test","{mv_id}","mv_meta","mv-meta","FAST","","DATE_ADD(NOW(), INTERVAL 1 HOUR)"]]"#)
        );
        assert_eq!(
            answer(&mut tk, "select mview_modify_time is not null from information_schema.tidb_mviews where table_schema = 'test' and mview_name = 'mv_meta'"),
            r#"RS:[["1"]]"#
        );
        assert_eq!(
            answer(&mut tk, "select mview_sql_content like 'SELECT%' from information_schema.tidb_mviews where table_schema = 'test' and mview_name = 'mv_meta'"),
            r#"RS:[["1"]]"#
        );
    }
    // t.Run("TiDBMLogs")
    {
        let (stack, _users, mut tk) = go_master_mview_stack();
        must_exec_all(&mut tk, &[
            "set tidb_mview_enable = on",
            "use test",
            "create table t (a int not null, b int not null)",
            "create materialized view log on t (a, b) purge start with now() next date_add(now(), interval 1 hour)",
        ]);
        let base = latest_table(&stack, "t");
        let (mlog_id, base_id) = (mlog_id(&base), base.id);
        assert_eq!(
            answer(&mut tk, "select table_catalog, table_schema, mlog_id, mlog_name, mlog_columns, base_table_catalog, base_table_schema, base_table_id, base_table_name, purge_method, purge_start, purge_next from information_schema.tidb_mlogs where table_schema = 'test' and mlog_name = '$mlog$t'"),
            format!(r#"RS:[["def","test","{mlog_id}","$mlog$t","a,b","def","test","{base_id}","t","DEFERRED","NOW()","DATE_ADD(NOW(), INTERVAL 1 HOUR)"]]"#)
        );
    }
}

/// The visibility rules of Go master's `setDataFromTiDBMViews` /
/// `setDataFromTiDBMLogs` / `setDataFromTiDBTableMViewDependencies` (a log
/// shown for its own or its base table's SELECT, a dependency row for the
/// base table's, the log's or the view's), replayed from a Go master
/// `2a6eb11aeb9` capture where `low:` statements run as an unprivileged
/// account (`testdata/mview_infoschema_privileges_go.jsonl`).
#[test]
fn materialized_view_infoschema_privileges_as_go_master() {
    let (stack, users, mut root) = go_master_mview_stack();
    let mut low: Option<Session> = None;
    let mut mismatches = Vec::new();
    for line in include_str!("../../../../../testdata/mview_infoschema_privileges_go.jsonl").lines() {
        let (sql, go): (String, String) = serde_json::from_str(line).unwrap();
        let rust = match sql.strip_prefix("low: ") {
            Some(sql) => {
                let session = low.get_or_insert_with(|| {
                    stack
                        .factory
                        .open_session(super::super::node_fixture::session_context_with_identity(2161, log_in(&users, "low")))
                        .unwrap()
                });
                answer(session, sql)
            }
            None => answer(&mut root, &sql),
        };
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
}

/// Go master `TestMaterializedViewRefreshCompleteOutOfPlaceConcurrentChildFast`
/// (`nested_oop_test.go`): a child FAST refresh paused at its data statement
/// fails once its parent's OUT OF PLACE cutover lands, and keeps its old
/// input log registration until a COMPLETE refresh.
///
/// Ignored until register row X05 (3): Go's FAST build takes a fresh
/// for-update timestamp (`updateForUpdateTS`), while this port's merge
/// validates at the transaction's earlier one, so the paused child does not
/// see the cutover and succeeds.
#[test]
fn test_materialized_view_refresh_complete_out_of_place_concurrent_child_fast() {
    use std::sync::atomic::{AtomicBool, Ordering};
    let (stack, mut tk, old_mv) = new_mview_refresh_source_fixture();
    must_exec_all(&mut tk, &[
        "create materialized view log on mv_source_init (a, s, cnt)",
        "create materialized view mv_child (a, s, cnt, cnt_s) refresh fast as select a, sum(s), count(*), count(s) from mv_source_init group by a",
        "insert into t_source_init values (3, 30)",
    ]);
    let child_id = latest_table(&stack, "mv_child").id;
    let before_child_log = single_value(&mut tk, &format!("select MLOG_ID from mysql.tidb_mview_fast_refresh_source_info where MVIEW_ID = {child_id}"));
    let (entered_tx, entered_rx) = std::sync::mpsc::channel::<()>();
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let paused = Arc::new(AtomicBool::new(false));
    let pause = std::sync::Mutex::new(Some((entered_tx, release_rx)));
    let _pause = tidb_failpoint::enable_call_for_test(
        "github.com/pingcap/tidb/pkg/executor/refreshMaterializedViewAfterInsertRefreshHistRunning",
        {
            let paused = Arc::clone(&paused);
            move |_| {
                if !paused.swap(true, Ordering::AcqRel) {
                    if let Some((entered, release)) = pause.lock().unwrap().take() {
                        entered.send(()).unwrap();
                        let _ = release.recv();
                    }
                }
            }
        },
    );
    let mut child = stack.factory.open_session(session_context(2162)).unwrap();
    must_exec_all(&mut child, &["set tidb_mview_enable = on", "use test"]);
    let child_done = std::thread::spawn(move || answer(&mut child, "refresh materialized view mv_child fast"));
    entered_rx.recv_timeout(std::time::Duration::from_secs(30)).expect("child FAST did not reach its data statement");
    let mut parent = stack.factory.open_session(session_context(2163)).unwrap();
    must_exec_all(&mut parent, &["set tidb_mview_enable = on", "use test"]);
    let parent_done = std::thread::spawn(move || answer(&mut parent, "refresh materialized view mv_source_init complete out of place"));
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    let id_sql = "select TIDB_TABLE_ID from information_schema.tables where TABLE_SCHEMA='test' and TABLE_NAME='mv_source_init'";
    while single_value(&mut tk, id_sql) == old_mv.to_string() {
        assert!(std::time::Instant::now() < deadline, "the parent's cutover did not land");
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    release_tx.send(()).unwrap();
    let child_answer = child_done.join().unwrap();
    assert!(child_answer.starts_with("ERR"), "child FAST: {child_answer}");
    assert_eq!(parent_done.join().unwrap(), "OK");
    assert_eq!(answer(&mut tk, "select * from mv_child order by a"), r#"RS:[["1","10","1","1"],["2","20","1","1"]]"#);
    // "The parent's registration moves to its new ID; the child keeps its old
    // input log registration until COMPLETE succeeds."
    assert_eq!(
        single_value(&mut tk, "select MLOG_ID from mysql.tidb_mview_fast_refresh_source_info where MVIEW_ID=(select TIDB_TABLE_ID from information_schema.tables where TABLE_SCHEMA='test' and TABLE_NAME='mv_child')"),
        before_child_log
    );
    must_exec_all(&mut tk, &["refresh materialized view mv_child complete delta apply", "refresh materialized view mv_child fast"]);
    assert_eq!(answer(&mut tk, "select * from mv_child order by a"), r#"RS:[["1","10","1","1"],["2","20","1","1"],["3","30","1","1"]]"#);
}

/// Go master `TestAlterMaterializedViewLogAddColumnRefreshLockLifetime`
/// (`pkg/ddl/tests/materializedviewlog/mview_output_log_test.go`): ADD COLUMN
/// on a view's own log holds the view's refresh advisory lock until its
/// delivery ends (schema sync of the done or rolled-back step included), and
/// the worker releases it before reuse.
#[test]
fn test_alter_materialized_view_log_add_column_refresh_lock_lifetime() {
    use std::sync::atomic::{AtomicBool, Ordering};
    for columns in ["s", "s, cnt"] {
        for rollback in [false, true] {
            let (stack, _users, mut tk) = go_master_mview_stack();
            must_exec_all(&mut tk, &[
                "set tidb_mview_enable = on",
                "use test",
                "create table t_alter_mlog_lock (a int not null, b int not null)",
                "create materialized view log on t_alter_mlog_lock (a, b)",
                "create materialized view mv_alter_mlog_lock (a, s, cnt) refresh fast as select a, sum(b), count(1) from t_alter_mlog_lock group by a",
                "create materialized view log on mv_alter_mlog_lock (a)",
            ]);
            let mv = latest_table(&stack, "mv_alter_mlog_lock");
            let lock_name = format!("mv_refresh_{}_{}", mv.db_id, mv.id);
            let _cancel = rollback.then(|| {
                let cancel_tk = std::sync::Mutex::new(stack.factory.open_session(session_context(2164)).unwrap());
                let cancelled = AtomicBool::new(false);
                tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/afterWaitSchemaSynced", move |job| {
                    let job = job.downcast_mut::<tidb_model::Job>().unwrap();
                    if job.table_name.to_string() != "$mlog$mv_alter_mlog_lock" || cancelled.swap(true, Ordering::AcqRel) {
                        return;
                    }
                    let id = job.id;
                    let answer = answer(&mut cancel_tk.lock().unwrap(), &format!("admin cancel ddl jobs {id}"));
                    assert!(answer.contains("successful"), "{answer}");
                })
            });
            let (paused_tx, paused_rx) = std::sync::mpsc::channel::<()>();
            let (resume_tx, resume_rx) = std::sync::mpsc::channel::<()>();
            let pause = std::sync::Mutex::new(Some((paused_tx, resume_rx)));
            let _before = tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/beforeWaitSchemaSynced", move |arg| {
                let (job, _) = arg.downcast_mut::<(tidb_model::Job, i64)>().unwrap();
                if job.table_name.to_string() == "$mlog$mv_alter_mlog_lock" && (job.is_done() || job.is_rollback_done()) {
                    if let Some((paused, resume)) = pause.lock().unwrap().take() {
                        paused.send(()).unwrap();
                        let _ = resume.recv();
                    }
                }
            });
            let mut alter = stack.factory.open_session(session_context(2165)).unwrap();
            must_exec_all(&mut alter, &["set tidb_mview_enable = on", "use test"]);
            let sql = format!("alter materialized view log on mv_alter_mlog_lock add column ({columns})");
            let ddl = std::thread::spawn(move || answer(&mut alter, &sql));
            paused_rx
                .recv_timeout(std::time::Duration::from_secs(15))
                .unwrap_or_else(|_| panic!("timeout waiting for ADD COLUMN schema sync ({columns}, rollback={rollback})"));
            let mut tk_lock = stack.factory.open_session(session_context(2166)).unwrap();
            must_exec_all(&mut tk_lock, &["set tidb_mview_enable = on", "use test"]);
            let refused = answer(&mut tk_lock, "refresh materialized view mv_alter_mlog_lock fast");
            assert!(refused.starts_with("ERR 8182 "), "{columns}, rollback={rollback}: {refused}");
            resume_tx.send(()).unwrap();
            let done = ddl.join().unwrap();
            if rollback {
                assert!(done.contains("Cancelled DDL job"), "{done}");
            } else {
                assert_eq!(done, "OK");
            }
            // "the worker must release the lock before reuse"
            assert_eq!(answer(&mut tk_lock, &format!("select get_lock('{lock_name}', 1)")), r#"RS:[["1"]]"#);
            assert_eq!(answer(&mut tk_lock, &format!("select release_lock('{lock_name}')")), r#"RS:[["1"]]"#);
            must_exec_all(&mut tk_lock, &["refresh materialized view mv_alter_mlog_lock fast"]);
        }
    }
}

/// Go master `TestAlterMaterializedViewLogAddColumnPausedRejectsOutOfPlaceRefresh`
/// (`mview_output_log_test.go`): an ADD COLUMN on a view's own log paused
/// at an intermediate state releases its worker's lock, an OUT OF PLACE
/// refresh refuses the intermediate log before it creates any shadow, and the
/// resumed job waits for a running refresh without counting failures.
#[test]
fn test_alter_materialized_view_log_add_column_paused_rejects_out_of_place_refresh() {
    use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
    use tidb_model::SchemaState as S;
    let cases = [
        ("delete_only", "s", S::DELETE_ONLY, false, "fast", false),
        ("write_only", "s", S::WRITE_ONLY, false, "complete delta apply", false),
        ("write_reorganization", "s", S::WRITE_REORGANIZATION, false, "fast", false),
        ("multiple_columns", "s, cnt", S::WRITE_REORGANIZATION, false, "fast", false),
        ("stale_schema", "s, cnt", S::WRITE_REORGANIZATION, true, "complete delta apply", false),
        ("between_sub_jobs_fast", "s, cnt", S::WRITE_REORGANIZATION, false, "fast", true),
        ("between_sub_jobs_delta_apply", "s, cnt", S::WRITE_REORGANIZATION, false, "complete delta apply", true),
    ];
    for (name, columns, state, stale, refresh_mode, between_sub_jobs) in cases {
        let (stack, _users, mut tk) = go_master_mview_stack();
        must_exec_all(&mut tk, &[
            "set tidb_mview_enable = on",
            "use test",
            "create table t_paused_mlog (a int not null, b int not null)",
            "insert into t_paused_mlog values (1, 10)",
            "create materialized view log on t_paused_mlog (a, b)",
            "create materialized view mv_paused_mlog (a, s, cnt) refresh fast as select a, sum(b), count(1) from t_paused_mlog group by a",
            "create materialized view log on mv_paused_mlog (a)",
            "insert into t_paused_mlog values (2, 20)",
        ]);
        let catalog = Arc::clone(&stack.factory.catalog);
        let old_is = catalog.get_latest();
        let mv = latest_table(&stack, "mv_paused_mlog");
        let lock_name = format!("mv_refresh_{}_{}", mv.db_id, mv.id);
        let mut tk_ctl = stack.factory.open_session(session_context(2171)).unwrap();
        must_exec_all(&mut tk_ctl, &["set tidb_mview_enable = on", "use test"]);
        let no_reload =
            tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/infoschema/issyncer/disableOnTickReload", "return(true)");

        let job_id = Arc::new(AtomicI64::new(0));
        let pause_requested = Arc::new(AtomicBool::new(false));
        let (pause_result_tx, pause_result_rx) = std::sync::mpsc::channel::<String>();
        let (lock_conflict_tx, lock_conflict_rx) = std::sync::mpsc::channel::<tidb_model::Job>();
        let pauser = std::sync::Mutex::new(stack.factory.open_session(session_context(2172)).unwrap());
        let _after_sync = tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/afterWaitSchemaSynced", {
            let (job_id, pause_requested) = (Arc::clone(&job_id), Arc::clone(&pause_requested));
            let pause_result_tx = std::sync::Mutex::new(pause_result_tx);
            let lock_conflict_tx = std::sync::Mutex::new(lock_conflict_tx);
            move |job| {
                let job = job.downcast_mut::<tidb_model::Job>().unwrap();
                if job.table_name.to_string() != "$mlog$mv_paused_mlog" {
                    return;
                }
                if job.id == job_id.load(Ordering::Acquire)
                    && job.error.as_ref().is_some_and(|error| { let error = error.read().clone(); tidb_exeerrors::ERR_MV_ADVISORY_LOCK_CONFLICT.equal(Some(&error as &(dyn std::error::Error + 'static))) })
                {
                    let _ = lock_conflict_tx.lock().unwrap().send(job.clone());
                }
                if let Some(info) = &job.multi_schema_info {
                    for (i, sub) in info.read().sub_jobs.iter_deref().enumerate() {
                        let want = if between_sub_jobs && i > 0 { S::NONE } else { state };
                        if sub.read().schema_state != want {
                            return;
                        }
                    }
                } else if job.schema_state != state {
                    return;
                }
                // "Multi-column DDL first publishes the analyze-state change.
                // Let that schema version synchronize before pausing."
                if job.multi_schema_info.is_some()
                    && !between_sub_jobs
                    && job.reorg_meta.as_ref().is_some_and(|meta| meta.read().analyze_state == tidb_model::reorg::analyze_state::NONE)
                {
                    return;
                }
                if pause_requested.swap(true, Ordering::AcqRel) {
                    return;
                }
                job_id.store(job.id, Ordering::Release);
                let paused = answer(&mut pauser.lock().unwrap(), &format!("admin pause ddl jobs {}", job.id));
                let _ = pause_result_tx.lock().unwrap().send(paused);
            }
        });
        let (paused_tx, paused_rx) = std::sync::mpsc::channel::<()>();
        let _after_delivery = tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/afterDeliveryJob", {
            let job_id = Arc::clone(&job_id);
            let paused_tx = std::sync::Mutex::new(Some(paused_tx));
            move |job_w| {
                let job_w = job_w.downcast_mut::<tidb_model::JobW>().unwrap();
                let Some(job) = job_w.job.as_ref() else { return };
                let job = job.read();
                if job.id == job_id.load(Ordering::Acquire) && job.is_paused() {
                    if let Some(paused) = paused_tx.lock().unwrap().take() {
                        let _ = paused.send(());
                    }
                }
            }
        });
        let mut alter = stack.factory.open_session(session_context(2173)).unwrap();
        must_exec_all(&mut alter, &["set tidb_mview_enable = on", "use test"]);
        let alter_sql = format!("alter materialized view log on mv_paused_mlog add column ({columns})");
        let ddl = std::thread::spawn(move || answer(&mut alter, &alter_sql));
        let pause = pause_result_rx.recv_timeout(std::time::Duration::from_secs(15)).expect("timeout requesting ADD COLUMN pause");
        assert!(pause.contains("successful"), "{name}: {pause}");
        paused_rx.recv_timeout(std::time::Duration::from_secs(15)).expect("timeout waiting for ADD COLUMN to release its worker");
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while answer(&mut tk_ctl, &format!("select is_used_lock('{lock_name}') is null")) != r#"RS:[["1"]]"# {
            assert!(std::time::Instant::now() < deadline, "{name}: the paused job still holds {lock_name}");
            std::thread::sleep(std::time::Duration::from_millis(10));
        }

        // "An OOP refresh must reject intermediate output-log metadata before
        // submitting any shadow/cutover DDL that could block a resumed ADD
        // COLUMN."
        let shadow_created = Arc::new(AtomicBool::new(false));
        {
            let seen = Arc::clone(&shadow_created);
            let _shadow = tidb_failpoint::enable_call_for_test(
                "github.com/pingcap/tidb/pkg/executor/refreshMaterializedViewOutOfPlaceAfterCreateShadow",
                move |_| seen.store(true, Ordering::Release),
            );
            let _before_hist = tidb_failpoint::enable_for_test(
                "github.com/pingcap/tidb/pkg/executor/mockRefreshMaterializedViewErrorBeforeInsertHist",
                r#"return("OOP passed the intermediate MLog schema")"#,
            );
            if stale {
                catalog.upsert_replacing(Arc::clone(&old_is), 0);
            }
            let refused = answer(&mut tk_ctl, "refresh materialized view mv_paused_mlog complete out of place");
            assert!(refused.starts_with("ERR 8182 "), "{name}: {refused}");
            assert!(refused.contains("materialized view log schema change is in progress"), "{name}: {refused}");
            assert!(!shadow_created.load(Ordering::Acquire), "{name}");
        }
        if stale {
            assert!(catalog.get_latest().schema_meta_version() > old_is.schema_meta_version(), "{name}");
        }
        // "rejected refresh must release its lock"
        assert_eq!(answer(&mut tk_ctl, &format!("select get_lock('{lock_name}', 1)")), r#"RS:[["1"]]"#, "{name}");
        assert_eq!(answer(&mut tk_ctl, &format!("select release_lock('{lock_name}')")), r#"RS:[["1"]]"#, "{name}");
        assert_eq!(answer(&mut tk_ctl, r"show tables like '\_\_mv\_shadow\_%'"), "RS:[]", "{name}");
        drop(no_reload);
        tk_ctl.domain_services().unwrap().domain.upgrade().unwrap().ddl.reload_schema().unwrap();

        must_exec_all(&mut tk_ctl, &["set global tidb_ddl_error_count_limit = 1"]);
        let (refresh_paused_tx, refresh_paused_rx) = std::sync::mpsc::channel::<()>();
        let (allow_tx, allow_rx) = std::sync::mpsc::channel::<()>();
        let hold = std::sync::Mutex::new(Some((refresh_paused_tx, allow_rx)));
        let data_changes = tidb_failpoint::enable_call_for_test(
            "github.com/pingcap/tidb/pkg/executor/refreshMaterializedViewAfterDataChanges",
            move |_| {
                if let Some((paused, allow)) = hold.lock().unwrap().take() {
                    paused.send(()).unwrap();
                    let _ = allow.recv();
                }
            },
        );
        let mut tk_refresh = stack.factory.open_session(session_context(2174)).unwrap();
        must_exec_all(&mut tk_refresh, &["set tidb_mview_enable = on", "use test"]);
        let refresh_sql = format!("refresh materialized view mv_paused_mlog {refresh_mode}");
        let refresh = std::thread::spawn(move || answer(&mut tk_refresh, &refresh_sql));
        refresh_paused_rx.recv_timeout(std::time::Duration::from_secs(15)).expect("timeout waiting for refresh data changes");

        // "Resuming an existing schema change must wait for refresh without
        // exhausting the DDL failure budget or rolling back intermediate
        // columns."
        let resumed = answer(&mut tk_ctl, &format!("admin resume ddl jobs {}", job_id.load(Ordering::Acquire)));
        assert!(resumed.contains("successful"), "{name}: {resumed}");
        for _ in 0..3 {
            let job = lock_conflict_rx
                .recv_timeout(std::time::Duration::from_secs(15))
                .unwrap_or_else(|_| panic!("{name}: timeout waiting for resumed ADD COLUMN lock conflict"));
            assert_eq!(job.state, tidb_model::JobState::RUNNING, "{name}");
            assert_eq!(job.error_count, 0, "{name}: refresh lock waits must not count as DDL failures");
            assert!(!ddl.is_finished(), "{name}: ADD COLUMN finished before refresh released its lock");
        }
        allow_tx.send(()).unwrap();
        assert_eq!(refresh.join().unwrap(), "OK", "{name}");
        drop(data_changes);
        assert_eq!(ddl.join().unwrap(), "OK", "{name}");
        must_exec_all(&mut tk_ctl, &["refresh materialized view mv_paused_mlog complete out of place"]);
        assert_eq!(answer(&mut tk_ctl, "select * from mv_paused_mlog order by a"), r#"RS:[["1","10","1"],["2","20","1"]]"#, "{name}");
    }
}

/// A one-shot latch: `wait` blocks until `open`.
#[derive(Clone, Default)]
struct Latch(Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>);

impl Latch {
    fn open(&self) {
        *self.0 .0.lock().unwrap() = true;
        self.0 .1.notify_all();
    }

    fn wait(&self) {
        let mut opened = self.0 .0.lock().unwrap();
        while !*opened {
            opened = self.0 .1.wait(opened).unwrap();
        }
    }

    fn wait_for(&self, what: &str) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        let mut opened = self.0 .0.lock().unwrap();
        while !*opened {
            let left = deadline.saturating_duration_since(std::time::Instant::now());
            assert!(!left.is_zero(), "timeout waiting for {what}");
            opened = self.0 .1.wait_timeout(opened, left).unwrap().0;
        }
    }
}

/// Go master `TestMaterializedViewRefreshCompleteOutOfPlacePurgeConcurrency`
/// (`nested_oop_test.go`): an OUT OF PLACE cutover retiring a log waits for
/// the log's running purge (cancelling it) without mutating metadata or
/// holding the schema-version lock, a purge holding the old metadata fails
/// fast on the advisory lock, and the cutover's total wait is bounded.
#[test]
fn test_materialized_view_refresh_complete_out_of_place_purge_concurrency() {
    use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
    for stage in ["running", "starting", "resolved", "timeout"] {
        let (stack, mut tk, old_mv) = new_mview_refresh_source_fixture();
        must_exec_all(&mut tk, &[
            "create materialized view log on mv_source_init (a, s, cnt)",
            "insert into t_source_init values (3, 30)",
            "refresh materialized view mv_source_init fast",
        ]);
        let old_log = single_value(
            &mut tk,
            "select TIDB_TABLE_ID from information_schema.tables where TABLE_SCHEMA='test' and TABLE_NAME='$mlog$mv_source_init'",
        );
        let _poll = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/executor/mockMVTaskMonitorPollInterval", "return(10)");
        let state_sqls = [
            "select * from mysql.tidb_mview_refresh_info order by MVIEW_ID",
            "select * from mysql.tidb_mview_fast_refresh_source_info order by MVIEW_ID, MLOG_ID",
            "select * from mysql.tidb_mlog_purge_info order by MLOG_ID",
            "select TABLE_NAME, TIDB_TABLE_ID from information_schema.tables where TABLE_SCHEMA='test' order by TABLE_NAME",
        ];
        let cutover_attempts = Arc::new(AtomicI32::new(0));
        let mut guards = Vec::new();
        let before: Vec<String> = if stage == "timeout" {
            let before = state_sqls.iter().map(|sql| answer(&mut tk, sql)).collect();
            guards.push(tidb_failpoint::enable_for_test(
                "github.com/pingcap/tidb/pkg/ddl/mockMViewOutOfPlacePurgeAdvisoryLockTimeout",
                "return(1000)",
            ));
            let attempts = Arc::clone(&cutover_attempts);
            guards.push(tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/afterRunOneJobStep", move |job| {
                let job = job.downcast_mut::<tidb_model::Job>().unwrap();
                if job.type_ == tidb_model::ActionType::ACTION_MVIEW_REFRESH_OUT_OF_PLACE_CUTOVER && job.table_id == old_mv {
                    attempts.fetch_add(1, Ordering::AcqRel);
                }
            }));
            before
        } else {
            Vec::new()
        };

        let (entered, release, running, release_running) = (Latch::default(), Latch::default(), Latch::default(), Latch::default());
        let paused = Arc::new(AtomicBool::new(false));
        let running_paused = Arc::new(AtomicBool::new(false));
        let pause = {
            let (entered, release, paused) = (entered.clone(), release.clone(), Arc::clone(&paused));
            move || {
                if !paused.swap(true, Ordering::AcqRel) {
                    entered.open();
                    release.wait();
                }
            }
        };
        match stage {
            // "The advisory lock is held, but no running history exists yet."
            "starting" => {
                let pause = pause.clone();
                guards.push(tidb_failpoint::enable_call_for_test(
                    "github.com/pingcap/tidb/pkg/executor/mvMaintainMemQuotaAppliedOnPurgeDeleteSession",
                    move |_| pause(),
                ));
            }
            // "Old metadata/MDL is held before advisory lock acquisition."
            "resolved" => {
                let pause = pause.clone();
                guards.push(tidb_failpoint::enable_call_for_test(
                    "github.com/pingcap/tidb/pkg/executor/mvMaintainMemQuotaAppliedOnPurgeSession",
                    move |_| pause(),
                ));
            }
            _ => {}
        }
        if stage != "resolved" {
            let pause = pause.clone();
            let (running, release_running, running_paused) = (running.clone(), release_running.clone(), Arc::clone(&running_paused));
            guards.push(tidb_failpoint::enable_call_for_test(
                "github.com/pingcap/tidb/pkg/executor/purgeMaterializedViewLogDeleteSQL",
                move |_| {
                    if stage == "running" || stage == "timeout" {
                        pause();
                    } else if !running_paused.swap(true, Ordering::AcqRel) {
                        running.open();
                        release_running.wait();
                    }
                },
            ));
        }
        let (requested, _requested_guard) = wait_cancel_watcher_requested("mlog-purge-");
        let retried = Latch::default();
        {
            let retried = retried.clone();
            guards.push(tidb_failpoint::enable_call_for_test(
                "github.com/pingcap/tidb/pkg/ddl/mViewOutOfPlacePurgeAdvisoryLockRetry",
                move |_| retried.open(),
            ));
        }
        let retiring = Arc::new(AtomicBool::new(false));
        {
            let retiring = Arc::clone(&retiring);
            guards.push(tidb_failpoint::enable_call_for_test(
                "github.com/pingcap/tidb/pkg/ddl/mViewOutOfPlaceBeforeRetireMLog",
                move |_| retiring.store(true, Ordering::Release),
            ));
        }

        let mut purger = stack.factory.open_session(session_context(2175)).unwrap();
        must_exec_all(&mut purger, &["set tidb_mview_enable = on", "use test"]);
        let purge = std::thread::spawn(move || answer(&mut purger, "purge materialized view log on mv_source_init"));
        entered.wait_for("purge pause point");
        let mut refresher = stack.factory.open_session(session_context(2176)).unwrap();
        must_exec_all(&mut refresher, &["set tidb_mview_enable = on", "use test"]);
        let cutover = std::thread::spawn(move || answer(&mut refresher, "refresh materialized view mv_source_init complete out of place"));
        let id_sql = "select TIDB_TABLE_ID from information_schema.tables where TABLE_SCHEMA='test' and TABLE_NAME='mv_source_init'";

        if stage == "resolved" {
            // "Cutover commits, then waits for this statement's old MDL. Purge
            // must fail instead of waiting for the advisory lock."
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
            while single_value(&mut tk, id_sql) == old_mv.to_string() {
                assert!(std::time::Instant::now() < deadline, "the cutover did not land");
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
            release.open();
            let purged = purge.join().unwrap();
            assert!(purged.starts_with("ERR 8182 "), "{stage}: {purged}");
        } else {
            if stage == "starting" {
                retried.wait_for("cutover retry before running history exists");
                assert_eq!(
                    answer(&mut tk, &format!("select count(*) from mysql.tidb_mlog_purge_hist where MLOG_ID={old_log} and PURGE_STATUS='running'")),
                    r#"RS:[["0"]]"#
                );
                release.open();
                running.wait_for("purge running history");
            }
            requested.recv_timeout(std::time::Duration::from_secs(10)).expect("cutover cancellation observed by purge");
            assert!(!retiring.load(Ordering::Acquire), "{stage}: cutover must wait before mutating runtime metadata");
            if stage == "timeout" {
                // "Purge still holds the lock after observing cancellation.
                // The total budget must fail the job instead of restarting DDL
                // retries."
                let cut = cutover.join().unwrap();
                assert!(cut.starts_with("ERR 8182 "), "{cut}");
                assert!(
                    cut.contains("refresh materialized view complete OUT OF PLACE cutover for test.mv_source_init: timed out waiting for materialized view log purge; retry refresh"),
                    "{cut}"
                );
                assert_eq!(cutover_attempts.load(Ordering::Acquire), 1);
                assert!(!retiring.load(Ordering::Acquire));
                let after: Vec<String> = state_sqls.iter().map(|sql| answer(&mut tk, sql)).collect();
                assert_eq!(after, before);
                assert_eq!(answer(&mut tk, "select * from mv_source_init order by a"), r#"RS:[["1","10","1"],["2","20","1"],["3","30","1"]]"#);
                release.open();
                let purged = purge.join().unwrap();
                assert!(purged.contains("materialized view task canceled manually"), "{purged}");
                guards.clear();
                must_exec_all(&mut tk, &[
                    "refresh materialized view mv_source_init complete out of place",
                    "purge materialized view log on mv_source_init",
                ]);
                continue;
            }
            // "A cancelled purge may still be finishing a DELETE batch.
            // Waiting must not hold the shared schema-version lock."
            let mut other = stack.factory.open_session(session_context(2177)).unwrap();
            assert_eq!(answer(&mut other, "use test"), "OK");
            let other = std::thread::spawn(move || answer(&mut other, "create table t_unrelated_to_purge (a int)"));
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
            while !other.is_finished() {
                assert!(std::time::Instant::now() < deadline, "{stage}: unrelated DDL while cutover waits");
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
            assert_eq!(other.join().unwrap(), "OK");
            assert_eq!(single_value(&mut tk, id_sql), old_mv.to_string(), "{stage}");
            release.open();
            release_running.open();
            let purged = purge.join().unwrap();
            assert!(purged.contains("materialized view task canceled manually"), "{stage}: {purged}");
        }
        assert_eq!(cutover.join().unwrap(), "OK", "{stage}");
        assert_eq!(
            answer(&mut tk, &format!("select count(*) from mysql.tidb_mlog_purge_info where MLOG_ID={old_log}")),
            r#"RS:[["0"]]"#,
            "{stage}"
        );
        must_exec_all(&mut tk, &["insert into t_source_init values (4, 40)", "refresh materialized view mv_source_init fast"]);
        assert_eq!(answer(&mut tk, "select count(*) > 0 from `$mlog$mv_source_init`"), r#"RS:[["1"]]"#, "{stage}");
        must_exec_all(&mut tk, &["purge materialized view log on mv_source_init"]);
    }
}

/// Go `tablecodec.EncodeTablePrefix(id)` as Go's `%x`.
fn table_prefix_hex(id: i64) -> String {
    format!("74{:016x}", (id as u64) ^ 0x8000_0000_0000_0000)
}

/// Go master `TestMaterializedViewRefreshCompleteOutOfPlaceCutoverKeepsTiFlashProgressUntilCommit`
/// (`nested_oop_test.go`): a cutover that fails before commit leaves the old
/// view's and log's TiFlash progress and replica, and queues the would-be
/// log's range for GC; the committed cutover gives the replacement log the
/// old replica's settings (unavailable) and a placement rule, and drops the
/// old progress.
#[test]
fn test_materialized_view_refresh_complete_out_of_place_cutover_keeps_tiflash_progress_until_commit() {
    use std::sync::atomic::{AtomicI64, Ordering};
    use tidb_domain_infosync::tiflash_manager as tiflash;
    let _store_count = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/infoschema/mockTiFlashStoreCount", "return(true)");
    let (stack, _users, mut tk) = go_master_mview_stack();
    let mock = Arc::new(tiflash::MockTiFlash::new());
    tiflash::set_mock_tiflash(Arc::clone(&mock));
    must_exec_all(&mut tk, &[
        "set tidb_mview_enable = on",
        "use test",
        "create table t_tiflash_cutover (a int not null, b int not null)",
        "insert into t_tiflash_cutover values (1, 10), (2, 7)",
        "create materialized view log on t_tiflash_cutover (a, b) purge next date_add(now(), interval 1 hour)",
        "create materialized view mv_tiflash_cutover (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t_tiflash_cutover group by a",
        "alter table mv_tiflash_cutover set tiflash replica 1",
        "create materialized view log on mv_tiflash_cutover (a, s, cnt) purge next date_add(now(), interval 1 hour)",
        "alter table `$mlog$mv_tiflash_cutover` set tiflash replica 2 location labels 'zone'",
    ]);
    let mv = latest_table(&stack, "mv_tiflash_cutover");
    let (old_mview_id, old_mlog_id) = (mv.id, mlog_id(&mv));
    stack
        .factory
        .ddl
        .update_table_replica_info(old_mlog_id, true, &tidb_sessionctx::StmtContext::for_query())
        .expect("the log's replica turns available");
    let replica_of = |id: i64| {
        let table = stack.factory.catalog.get_latest().table_info_by_id(id).expect("table").read().clone();
        let replica = table.tiflash_replica.as_ref().expect("a TiFlash replica").read().clone();
        replica
    };
    let old_replica = replica_of(old_mlog_id);
    assert!(old_replica.available);
    tiflash::update_tiflash_progress_cache(old_mview_id, 0.75);
    tiflash::update_tiflash_progress_cache(old_mlog_id, 1.0);
    assert!(tiflash::get_tiflash_progress_from_cache(old_mview_id).is_some());

    must_exec_all(&mut tk, &["set global tidb_ddl_error_count_limit = 0"]);
    let cancelled_job_id = Arc::new(AtomicI64::new(0));
    let cancelled_log_id = Arc::new(AtomicI64::new(0));
    {
        let (job_id, log_id) = (Arc::clone(&cancelled_job_id), Arc::clone(&cancelled_log_id));
        let _observe = tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep", move |job| {
            let job = job.downcast_mut::<tidb_model::Job>().unwrap();
            if job.type_ == tidb_model::ActionType::ACTION_MVIEW_REFRESH_OUT_OF_PLACE_CUTOVER && job.table_id == old_mview_id {
                let args = tidb_model::get_refresh_materialized_view_complete_out_of_place_cutover_args(job).unwrap().unwrap();
                job_id.store(job.id, Ordering::Release);
                log_id.store(args.read().new_mlog_id, Ordering::Release);
            }
        });
        let _fail = tidb_failpoint::enable_for_test(
            "github.com/pingcap/tidb/pkg/ddl/mockMViewRefreshOutOfPlaceCutoverBeforeCommitError",
            "return",
        );
        must_exec_all(&mut tk, &["insert into t_tiflash_cutover values (3, 4)"]);
        exec_err_contains(&mut tk, "refresh materialized view mv_tiflash_cutover complete out of place", "error before commit");
    }
    let (job_id, log_id) = (cancelled_job_id.load(Ordering::Acquire), cancelled_log_id.load(Ordering::Acquire));
    assert!(log_id > 0);
    assert_eq!(
        answer(&mut tk, &format!("select start_key, end_key from mysql.gc_delete_range where job_id = {job_id} union all select start_key, end_key from mysql.gc_delete_range_done where job_id = {job_id}")),
        format!(r#"RS:[["{}","{}"]]"#, table_prefix_hex(log_id), table_prefix_hex(log_id + 1))
    );
    assert!(tiflash::get_tiflash_progress_from_cache(old_mview_id).is_some());
    assert!(tiflash::get_tiflash_progress_from_cache(old_mlog_id).is_some());
    let kept = replica_of(old_mlog_id);
    assert_eq!(serde_json::to_value(&kept).unwrap(), serde_json::to_value(&old_replica).unwrap());

    must_exec_all(&mut tk, &["refresh materialized view mv_tiflash_cutover complete out of place"]);
    assert!(tiflash::get_tiflash_progress_from_cache(old_mview_id).is_none());
    let new_log = latest_table(&stack, "$mlog$mv_tiflash_cutover");
    assert_ne!(new_log.id, old_mlog_id);
    let replica = new_log.tiflash_replica.as_ref().expect("the replacement log's replica").read().clone();
    assert_eq!(replica.count, old_replica.count);
    assert_eq!(replica.location_labels, old_replica.location_labels);
    // t.Run("replica state")
    assert!(!replica.available);
    assert!(replica.available_partition_ids.is_empty());
    // t.Run("placement rule")
    let rule = mock.get_placement_rule(&tiflash::make_rule_id(new_log.id)).expect("the replacement log's rule");
    assert_eq!(rule.count, replica.count as i64);
    assert_eq!(rule.location_labels, replica.location_labels.iter().cloned().collect::<Vec<_>>());
    // t.Run("old log progress")
    assert!(tiflash::get_tiflash_progress_from_cache(old_mlog_id).is_none());
}

/// Go `ddl.ExecutorForTest`: the job `sql` builds now, delivered later by
/// [`deliver_captured_job`].
fn capture_ddl_job(session: &mut Session, sql: &str) -> tidb_ddl::JobWrapper {
    let (answer, job) = tidb_ddl::executor::capture_ddl_job_for_test(|| answer(session, sql));
    assert_eq!(answer, "OK", "{sql}");
    job.expect("the statement built a job")
}

/// Go `ExecutorForTest.DoDDLJobWrapper`.
fn deliver_captured_job(session: &mut Session, job: tidb_ddl::JobWrapper) -> Result<(), String> {
    let ddl = Arc::clone(&session.domain_services().unwrap().ddl);
    ddl.do_ddl_job_wrapper(&session.ddl_statement_context(), job).map(|_| ()).map_err(|error| error.to_string())
}

fn mview_column_chain(tk: &mut Session) {
    must_exec_all(tk, &[
        "set tidb_mview_enable = on",
        "use test",
        "create table t (a int not null, b int not null)",
        "insert into t values (1, 10)",
        "create materialized view log on t (a, b)",
        "create materialized view mv1 (k, l, cnt) as select a, b, count(1) from t group by a, b",
        "create materialized view log on mv1 (k, l)",
        "create materialized view mv2 (x, y, cnt) as select k, l, count(1) from mv1 group by k, l",
        "create materialized view log on mv2 (x, y)",
    ]);
}

/// Go master `TestNestedMaterializedViewModifyColumnRejectsDownstreamUsage`,
/// its `worker` and `multi_worker` modes: a job built before an
/// incompatible dependent view exists is refused by the worker.
#[test]
fn test_nested_materialized_view_modify_column_rejects_downstream_usage_worker() {
    for multi in [false, true] {
        for (name, column, columns, select_sql, err) in [
            ("where", "a", "(z, cnt)", "select x, count(1) from mv2 where x > 0 group by x", "WHERE clause"),
            ("aggregate", "b", "(z, total, cnt)", "select x, sum(y), count(1) from mv2 group by x", "non-direct SELECT expressions"),
        ] {
            let (_stack, _users, mut tk) = go_master_mview_stack();
            mview_column_chain(&mut tk);
            let alter_sql = if multi {
                "alter table t modify column a bigint not null, modify column b bigint not null".to_owned()
            } else {
                format!("alter table t modify column {column} bigint not null")
            };
            // "Build the worker's job before adding the incompatible
            // downstream dependency."
            let job = capture_ddl_job(&mut tk, &alter_sql);
            must_exec_all(&mut tk, &[&format!("create materialized view mv3 {columns} as {select_sql}")]);
            const TYPES_SQL: &str = "select table_name, column_name, column_type from information_schema.columns where table_schema = 'test' order by table_name, ordinal_position";
            let before = answer(&mut tk, TYPES_SQL);
            let refused = deliver_captured_job(&mut tk, job).expect_err("the worker refuses");
            assert!(refused.contains(err), "{name}/multi={multi}: {refused}");
            assert_eq!(answer(&mut tk, TYPES_SQL), before, "{name}/multi={multi}");
            assert_eq!(answer(&mut tk, "select * from mv2"), r#"RS:[["1","10","1"]]"#, "{name}/multi={multi}");
        }
    }
}

/// Go master `TestNestedMaterializedViewModifyColumnNewDescendants`: views
/// created after the job was built are retyped too, and the batch's diff
/// lists every related table, including those the second (version-skipping)
/// sub-job changes.
#[test]
fn test_nested_materialized_view_modify_column_new_descendants() {
    for cache_size in [0_i64, 1 << 30] {
        let (stack, _users, mut tk) = go_master_mview_stack();
        must_exec_all(&mut tk, &[&format!("set @@global.tidb_schema_cache_size = {cache_size}")]);
        mview_column_chain(&mut tk);
        let job = capture_ddl_job(&mut tk, "alter table t modify column a bigint not null, modify column b bigint not null");
        // "These tables are absent from the already constructed job's
        // involving set."
        must_exec_all(&mut tk, &[
            "create materialized view mv3 (i, j, cnt) as select x, y, count(1) from mv2 group by x, y",
            "create materialized view log on mv3 (i, j)",
            // "This branch changes only in the second sub-job, which skips
            // schema version generation."
            "create materialized view mv4 (j, cnt) as select y, count(1) from mv2 group by y",
            "create materialized view log on mv4 (j)",
        ]);
        let involving: Vec<String> =
            job.job.get_involving_schema_info().snapshot().iter().map(|info| info.table.to_string()).collect();
        for name in ["mv3", "$mlog$mv3", "mv4", "$mlog$mv4"] {
            assert!(!involving.iter().any(|table| table == name), "{name} in {involving:?}");
        }
        let affected_names = ["$mlog$t", "mv1", "$mlog$mv1", "mv2", "$mlog$mv2", "mv3", "$mlog$mv3", "mv4", "$mlog$mv4"];
        let mut expected: Vec<i64> = affected_names.iter().map(|name| latest_table(&stack, name).id).collect();
        expected.sort_unstable();
        deliver_captured_job(&mut tk, job).expect("the ALTER runs");
        let history = latest_history_job(&mut tk);
        assert_eq!(history.type_, tidb_model::ActionType::ACTION_MULTI_SCHEMA_CHANGE);
        let version = history.binlog_info.as_ref().unwrap().read().schema_version;
        let domain = tk.domain_services().unwrap().domain.upgrade().unwrap();
        let ts = domain.transactions.fresh_timestamp().unwrap();
        let mut meta = domain.get_snapshot_meta(ts).unwrap();
        let diff = tidb_infoschema::cluster_catalog::with_meta_reader(&mut meta, |reader| reader.schema_diff(version))
            .unwrap()
            .expect("the batch's diff");
        let mut affected: Vec<i64> = diff.affected_options.iter_deref().map(|option| option.read().table_id).collect();
        affected.sort_unstable();
        assert_eq!(affected, expected, "cache_size={cache_size}");
        assert_eq!(
            answer(&mut tk, "select distinct data_type from information_schema.columns where table_schema = 'test' and column_name in ('a', 'b', 'k', 'l', 'x', 'y', 'i', 'j')"),
            r#"RS:[["bigint"]]"#,
            "cache_size={cache_size}"
        );
        must_exec_all(&mut tk, &[
            "insert into t values (2147483648, 2147483649)",
            "refresh materialized view mv1 fast",
            "refresh materialized view mv2 fast",
            "refresh materialized view mv3 fast",
            "refresh materialized view mv4 fast",
        ]);
        assert_eq!(answer(&mut tk, "select * from mv3 order by i"), r#"RS:[["1","10","1"],["2147483648","2147483649","1"]]"#);
        assert_eq!(answer(&mut tk, "select * from mv4 order by j"), r#"RS:[["10","1"],["2147483649","1"]]"#);
        must_exec_all(&mut tk, &["set @@global.tidb_schema_cache_size = default"]);
    }
}

/// Go master `TestNestedMaterializedViewModifyColumnDependencyChangesAfterPrecheck`:
/// a dependent view created while the submitted ALTER waits at its batch
/// boundary is checked by the worker, which refuses it and changes nothing.
#[test]
fn test_nested_materialized_view_modify_column_dependency_changes_after_precheck() {
    let (stack, _users, mut tk) = go_master_mview_stack();
    mview_column_chain(&mut tk);
    let mut tk_alter = stack.factory.open_session(session_context(2178)).unwrap();
    must_exec_all(&mut tk_alter, &["set tidb_mview_enable = on", "use test"]);
    let job = capture_ddl_job(&mut tk_alter, "alter table t modify column a bigint not null, modify column b bigint not null");
    must_exec_all(&mut tk, &[
        "create materialized view mv3 (i, j, cnt) as select x, y, count(1) from mv2 group by x, y",
        "create materialized view log on mv3 (i, j)",
    ]);
    let involving: Vec<String> =
        job.job.get_involving_schema_info().snapshot().iter().map(|info| info.table.to_string()).collect();
    for name in ["mv3", "$mlog$mv3", "blocker"] {
        assert!(!involving.iter().any(|table| table == name), "{name} in {involving:?}");
    }
    let (at_boundary_tx, at_boundary_rx) = std::sync::mpsc::channel::<()>();
    let (resume_tx, resume_rx) = std::sync::mpsc::channel::<()>();
    let pause = std::sync::Mutex::new(Some((at_boundary_tx, resume_rx)));
    let _boundary = tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/beforeTransitOneJobStep", move |arg| {
        let job_w = arg.downcast_mut::<tidb_model::JobW>().unwrap();
        let job = job_w.job.as_ref().unwrap().read();
        if job.type_ != tidb_model::ActionType::ACTION_MULTI_SCHEMA_CHANGE || job.is_rollingback() {
            return;
        }
        let Some(info) = &job.multi_schema_info else { return };
        let info = info.read();
        if !info.revertible || info.sub_jobs.iter_deref().any(|sub| sub.read().revertible || sub.read().is_finished()) {
            return;
        }
        if let Some((at_boundary, resume)) = pause.lock().unwrap().take() {
            at_boundary.send(()).unwrap();
            let _ = resume.recv();
        }
    });
    let alter = std::thread::spawn(move || deliver_captured_job(&mut tk_alter, job));
    at_boundary_rx.recv_timeout(std::time::Duration::from_secs(15)).expect("ALTER did not reach the batch boundary");
    // "blocker CREATE completed while ALTER was paused before its boundary
    // transaction"
    must_exec_all(&mut tk, &["create materialized view blocker (z, total, cnt) as select i, sum(j), count(1) from mv3 group by i"]);
    resume_tx.send(()).unwrap();
    let refused = alter.join().unwrap().expect_err("the worker refuses");
    assert!(refused.contains("non-direct SELECT expressions"), "{refused}");
    assert_eq!(
        answer(&mut tk, "select distinct data_type from information_schema.columns where table_schema = 'test' and column_name in ('a', 'b', 'k', 'l', 'x', 'y', 'i', 'j', 'z')"),
        r#"RS:[["int"]]"#,
        "persisted metadata changed despite rejected ALTER"
    );
}

fn mview_output_log_session() -> (UnistoreClusterStack, Session) {
    let (stack, _users, mut tk) = go_master_mview_stack();
    must_exec_all(&mut tk, &["set tidb_mview_enable = on", "use test"]);
    (stack, tk)
}

/// Go master `testMaterializedViewRefreshWritesMLog`
/// (`pkg/executor/test/mviewoutputlog/refresh_output_log_test.go`).
fn materialized_view_refresh_writes_m_log(refresh_method: &str) {
    let (_stack, mut tk) = mview_output_log_session();
    must_exec_all(&mut tk, &[
        "create table t_mv_refresh_mlog (a int not null, b int not null)",
        "insert into t_mv_refresh_mlog values (1, 10), (1, 5), (2, 7)",
        "create materialized view log on t_mv_refresh_mlog (a, b)",
        "create materialized view mv_refresh_mlog (a, s, cnt) refresh fast as select a, sum(b), count(1) from t_mv_refresh_mlog group by a",
        "create materialized view log on mv_refresh_mlog (a, s, cnt)",
        // "The three base-table mutations make the parent MV update one
        // group, delete one group, and insert one group."
        "update t_mv_refresh_mlog set b = 11 where a = 1 and b = 10",
        "delete from t_mv_refresh_mlog where a = 2",
        "insert into t_mv_refresh_mlog values (3, 4)",
        &format!("refresh materialized view mv_refresh_mlog {refresh_method}"),
    ]);
    assert_eq!(answer(&mut tk, "select a, s, cnt from mv_refresh_mlog order by a"), r#"RS:[["1","16","2"],["3","4","1"]]"#);
    assert_eq!(
        answer(&mut tk, "select a, s, cnt, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$mv_refresh_mlog` order by a, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW`"),
        r#"RS:[["1","15","2","U","-1"],["1","16","2","U","1"],["2","7","1","D","-1"],["3","4","1","I","1"]]"#,
        "{refresh_method}"
    );
    // "A refresh with no source changes must not append a MLog row."
    must_exec_all(&mut tk, &[&format!("refresh materialized view mv_refresh_mlog {refresh_method}")]);
    assert_eq!(answer(&mut tk, "select count(*) from `$mlog$mv_refresh_mlog`"), r#"RS:[["4"]]"#, "{refresh_method}");
}

/// Go master `TestMaterializedViewRefreshFastWritesMLog`.
#[test]
fn test_materialized_view_refresh_fast_writes_m_log() {
    materialized_view_refresh_writes_m_log("fast");
}

/// Go master `TestMaterializedViewRefreshCompleteDeltaApplyWritesMLog`.
#[test]
fn test_materialized_view_refresh_complete_delta_apply_writes_m_log() {
    materialized_view_refresh_writes_m_log("complete delta apply");
}

/// Go master `TestMaterializedViewRefreshSkipsMLogWhenTrackedColumnsAreUnchanged`.
#[test]
fn test_materialized_view_refresh_skips_m_log_when_tracked_columns_are_unchanged() {
    for refresh_method in ["fast", "complete delta apply"] {
        let (_stack, mut tk) = mview_output_log_session();
        must_exec_all(&mut tk, &[
            "create table t_mv_refresh_mlog_tracked (a int not null, b int not null)",
            "insert into t_mv_refresh_mlog_tracked values (1, 10), (1, 5)",
            "create materialized view log on t_mv_refresh_mlog_tracked (a, b)",
            "create materialized view mv_refresh_mlog_tracked (a, s, cnt) refresh fast as select a, sum(b), count(1) from t_mv_refresh_mlog_tracked group by a",
            "create materialized view log on mv_refresh_mlog_tracked (a)",
            "update t_mv_refresh_mlog_tracked set b = 11 where a = 1 and b = 10",
            &format!("refresh materialized view mv_refresh_mlog_tracked {refresh_method}"),
        ]);
        assert_eq!(answer(&mut tk, "select a, s, cnt from mv_refresh_mlog_tracked"), r#"RS:[["1","16","2"]]"#, "{refresh_method}");
        assert_eq!(answer(&mut tk, "select * from `$mlog$mv_refresh_mlog_tracked`"), "RS:[]", "{refresh_method}");
    }
}

/// Go master `TestMaterializedViewRefreshMLogWriteFailureRollsBack`.
#[test]
fn test_materialized_view_refresh_m_log_write_failure_rolls_back() {
    for refresh_method in ["fast", "complete delta apply"] {
        let (_stack, mut tk) = mview_output_log_session();
        must_exec_all(&mut tk, &[
            "create table t_mv_refresh_mlog_rollback (a int not null, b int not null)",
            "insert into t_mv_refresh_mlog_rollback values (1, 10), (1, 5)",
            "create materialized view log on t_mv_refresh_mlog_rollback (a, b)",
            "create materialized view mv_refresh_mlog_rollback (a, s, cnt) refresh fast as select a, sum(b), count(1) from t_mv_refresh_mlog_rollback group by a",
            "create materialized view log on mv_refresh_mlog_rollback (a, s, cnt)",
            "update t_mv_refresh_mlog_rollback set b = 11 where a = 1 and b = 10",
        ]);
        let before = answer(&mut tk, "select LAST_SUCCESS_READ_TSO from mysql.tidb_mview_refresh_info");
        {
            let _fail = tidb_failpoint::enable_for_test(
                "github.com/pingcap/tidb/pkg/table/tables/mockMLogWriteError",
                r#"return("mock mlog write failure")"#,
            );
            exec_err_contains(&mut tk, &format!("refresh materialized view mv_refresh_mlog_rollback {refresh_method}"), "mock mlog write failure");
        }
        assert_eq!(answer(&mut tk, "select a, s, cnt from mv_refresh_mlog_rollback"), r#"RS:[["1","15","2"]]"#, "{refresh_method}");
        assert_eq!(answer(&mut tk, "select * from `$mlog$mv_refresh_mlog_rollback`"), "RS:[]", "{refresh_method}");
        assert_eq!(answer(&mut tk, "select LAST_SUCCESS_READ_TSO from mysql.tidb_mview_refresh_info"), before, "{refresh_method}");
        must_exec_all(&mut tk, &[&format!("refresh materialized view mv_refresh_mlog_rollback {refresh_method}")]);
        assert_eq!(answer(&mut tk, "select a, s, cnt from mv_refresh_mlog_rollback"), r#"RS:[["1","16","2"]]"#, "{refresh_method}");
        assert_eq!(answer(&mut tk, "select count(*) from `$mlog$mv_refresh_mlog_rollback`"), r#"RS:[["2"]]"#, "{refresh_method}");
    }
}

/// Go master `TestMaterializedViewRefreshMLogNullTransitions`.
#[test]
fn test_materialized_view_refresh_m_log_null_transitions() {
    for method in ["fast", "complete delta apply"] {
        let (_stack, mut tk) = mview_output_log_session();
        must_exec_all(&mut tk, &[
            "create table t_null_output (a int not null, b int)",
            "insert into t_null_output values (1, null), (2, 10)",
            "create materialized view log on t_null_output (a, b)",
            "create materialized view mv_null_output (a, s, n, cnt) as select a, sum(b), count(b), count(1) from t_null_output group by a",
            "create materialized view log on mv_null_output (a, s, n, cnt)",
            "update t_null_output set b=10 where a=1",
            "update t_null_output set b=null where a=2",
            &format!("refresh materialized view mv_null_output {method}"),
        ]);
        assert_eq!(
            answer(&mut tk, "select a, ifnull(cast(s as char), 'NULL'), n, cnt, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$mv_null_output` order by a, `_MLOG$_OLD_NEW`"),
            r#"RS:[["1","NULL","0","1","U","-1"],["1","10","1","1","U","1"],["2","10","1","1","U","-1"],["2","NULL","0","1","U","1"]]"#,
            "{method}"
        );
    }
}

/// Go master `TestMaterializedViewRefreshMinMaxWritesMLog`.
#[test]
fn test_materialized_view_refresh_min_max_writes_m_log() {
    for method in ["fast", "complete delta apply"] {
        let (_stack, mut tk) = mview_output_log_session();
        must_exec_all(&mut tk, &[
            "create table t_minmax_output (g int not null, b int, key(g))",
            "insert into t_minmax_output values (1, 10), (1, 20), (2, null)",
            "create materialized view log on t_minmax_output (g, b)",
            "create materialized view mv_minmax_output (g, cnt, mn, mx) as select g, count(1), min(b), max(b) from t_minmax_output group by g",
            "create materialized view log on mv_minmax_output (g, cnt, mn, mx)",
            "delete from t_minmax_output where g=1 and b=20",
            "update t_minmax_output set b=5 where g=2",
            &format!("refresh materialized view mv_minmax_output {method}"),
        ]);
        assert_eq!(
            answer(&mut tk, "select g, cnt, ifnull(cast(mn as char), 'NULL'), ifnull(cast(mx as char), 'NULL'), `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$mv_minmax_output` order by g, `_MLOG$_OLD_NEW`"),
            r#"RS:[["1","2","10","20","U","-1"],["1","1","10","10","U","1"],["2","1","NULL","NULL","U","-1"],["2","1","5","5","U","1"]]"#,
            "{method}"
        );
    }
}

/// Go master `TestMaterializedViewRefreshCompleteInPlaceRejectsMLog`
/// (`pkg/executor/test/mviewoutputlog/refresh_schema_test.go`).
#[test]
fn test_materialized_view_refresh_complete_in_place_rejects_m_log() {
    let (_stack, mut tk) = mview_output_log_session();
    must_exec_all(&mut tk, &[
        "create table t (a int not null, b int not null)",
        "insert into t values (1, 10), (2, 20)",
        "create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)",
        "create materialized view mv (a, s, cnt) refresh fast as select a, sum(b), count(1) from t group by a",
        "create materialized view log on mv (a, s, cnt)",
        "insert into t values (3, 30)",
    ]);
    let refused = answer(&mut tk, "refresh materialized view mv complete in place");
    assert!(refused.contains("complete IN PLACE is not supported") && refused.contains("with a materialized view log"), "{refused}");
    assert_eq!(answer(&mut tk, "select a, s, cnt from mv order by a"), r#"RS:[["1","10","1"],["2","20","1"]]"#);
}

/// Go master `TestMaterializedViewRefreshSchemaReloadConcurrent`: refreshes
/// that see a stale schema share one reload.
#[test]
fn test_materialized_view_refresh_schema_reload_concurrent() {
    use std::sync::atomic::{AtomicI32, Ordering};
    const REFRESH_COUNT: usize = 10;
    let (stack, _users, mut tk) = go_master_mview_stack();
    must_exec_all(&mut tk, &[
        "set tidb_mview_enable = on",
        "use test",
        "create table t_schema (a int not null, b int not null)",
        "insert into t_schema values (1, 10)",
        "create materialized view log on t_schema (a, b)",
    ]);
    let mut refreshers = Vec::new();
    for i in 0..REFRESH_COUNT {
        must_exec_all(&mut tk, &[&format!(
            "create materialized view mv_schema_{i} (a, s, cnt) as select a, sum(b), count(1) from t_schema group by a"
        )]);
        let mut refresher = stack.factory.open_session(session_context(2180 + i as u64)).unwrap();
        must_exec_all(&mut refresher, &["set tidb_mview_enable = on", "use test"]);
        refreshers.push(refresher);
    }
    must_exec_all(&mut tk, &["insert into t_schema values (2, 20)"]);
    let _no_reload =
        tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/infoschema/issyncer/disableOnTickReload", "return(true)");
    let catalog = Arc::clone(&stack.factory.catalog);
    let old_is = catalog.get_latest();
    // "Every refresh uses this source, so the diff requires a shared reload."
    must_exec_all(&mut tk, &["alter table t_schema add column c int"]);
    let required_version = catalog.get_latest().schema_meta_version();
    assert!(required_version > old_is.schema_meta_version());
    catalog.upsert_replacing(Arc::clone(&old_is), 0);

    let (waiting_tx, waiting_rx) = std::sync::mpsc::channel::<i64>();
    let resume = Latch::default();
    let _before_lock = tidb_failpoint::enable_call_for_test(
        "github.com/pingcap/tidb/pkg/executor/refreshMaterializedViewBeforeSchemaReloadLock",
        {
            let waiting_tx = std::sync::Mutex::new(waiting_tx);
            let resume = resume.clone();
            move |version| {
                let version = *version.downcast_mut::<i64>().unwrap();
                waiting_tx.lock().unwrap().send(version).unwrap();
                resume.wait();
            }
        },
    );
    let reloads = Arc::new(AtomicI32::new(0));
    let _before_reload = tidb_failpoint::enable_call_for_test(
        "github.com/pingcap/tidb/pkg/executor/refreshMaterializedViewBeforeSchemaReload",
        {
            let reloads = Arc::clone(&reloads);
            move |_| {
                reloads.fetch_add(1, Ordering::AcqRel);
            }
        },
    );
    let handles: Vec<_> = refreshers
        .into_iter()
        .enumerate()
        .map(|(i, mut refresher)| std::thread::spawn(move || answer(&mut refresher, &format!("refresh materialized view mv_schema_{i} fast"))))
        .collect();
    for _ in 0..REFRESH_COUNT {
        let version = waiting_rx
            .recv_timeout(std::time::Duration::from_secs(15))
            .expect("timeout waiting for every refresh to observe the stale schema");
        assert_eq!(version, required_version);
    }
    resume.open();
    for handle in handles {
        assert_eq!(handle.join().unwrap(), "OK");
    }
    assert_eq!(reloads.load(Ordering::Acquire), 1, "concurrent refreshes must share the first successful schema reload");
    assert!(catalog.get_latest().schema_meta_version() >= required_version);
    for i in 0..REFRESH_COUNT {
        assert_eq!(answer(&mut tk, &format!("select * from mv_schema_{i} order by a")), r#"RS:[["1","10","1"],["2","20","1"]]"#);
    }
}

/// Go master `TestMaterializedViewRefreshCreateMLogConflict`: CREATE
/// MATERIALIZED VIEW LOG on a view a refresh holds is cancelled at once, the
/// refresh finishes, and a retry succeeds.
#[test]
fn test_materialized_view_refresh_create_m_log_conflict() {
    use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
    for (mode, rollback) in [
        ("fast", false),
        ("complete delta apply", false),
        ("complete in place", false),
        ("complete in place", true),
        ("complete out of place", false),
    ] {
        let (stack, _users, mut tk) = go_master_mview_stack();
        must_exec_all(&mut tk, &[
            "set tidb_mview_enable = on",
            "use test",
            "create table t_mlog_refresh (a int not null, b int not null)",
            "insert into t_mlog_refresh values (1, 10)",
            "create materialized view log on t_mlog_refresh (a, b)",
            "create materialized view mv_mlog_refresh (a, s, cnt) refresh fast as select a, sum(b), count(1) from t_mlog_refresh group by a",
            "insert into t_mlog_refresh values (2, 20)",
        ]);
        let (paused, resume) = (Latch::default(), Latch::default());
        let pause = {
            let (paused, resume) = (paused.clone(), resume.clone());
            move |_: &mut dyn std::any::Any| {
                paused.open();
                resume.wait();
            }
        };
        let mut guards = vec![if mode == "complete out of place" {
            tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/executor/refreshMaterializedViewOutOfPlaceAfterBuildDataLoad", pause)
        } else {
            tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/executor/refreshMaterializedViewAfterDataChanges", pause)
        }];
        if rollback {
            guards.push(tidb_failpoint::enable_for_test(
                "github.com/pingcap/tidb/pkg/executor/mockRefreshMaterializedViewErrorAfterDataChanges",
                r#"return("rollback refresh for lock test")"#,
            ));
        }
        let create_attempts = Arc::new(AtomicI32::new(0));
        let cancelled = Arc::new(AtomicBool::new(false));
        {
            let (attempts, cancelled) = (Arc::clone(&create_attempts), Arc::clone(&cancelled));
            guards.push(tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/afterRunOneJobStep", move |job| {
                let job = job.downcast_mut::<tidb_model::Job>().unwrap();
                if job.type_ == tidb_model::ActionType::ACTION_CREATE_MATERIALIZED_VIEW_LOG
                    && job.table_name.to_string() == "$mlog$mv_mlog_refresh"
                {
                    attempts.fetch_add(1, Ordering::AcqRel);
                    cancelled.store(job.is_cancelled(), Ordering::Release);
                }
            }));
        }
        let mut tk_refresh = stack.factory.open_session(session_context(2190)).unwrap();
        must_exec_all(&mut tk_refresh, &["set tidb_mview_enable = on", "use test"]);
        let refresh_sql = format!("refresh materialized view mv_mlog_refresh {mode}");
        let refresh = std::thread::spawn(move || answer(&mut tk_refresh, &refresh_sql));
        paused.wait_for("refresh");
        let created = answer(&mut tk, "create materialized view log on mv_mlog_refresh (a, s, cnt)");
        assert!(created.starts_with("ERR 8182 "), "{mode}/{rollback}: {created}");
        assert!(created.contains("materialized view refresh is running") && created.contains("please retry later"), "{created}");
        assert_eq!(create_attempts.load(Ordering::Acquire), 1, "lock conflict must not retry the same job");
        assert!(cancelled.load(Ordering::Acquire));
        assert_eq!(answer(&mut tk, "show tables like '$mlog$mv_mlog_refresh'"), "RS:[]");
        resume.open();
        let refreshed = refresh.join().unwrap();
        if rollback {
            assert!(refreshed.contains("rollback refresh for lock test"), "{refreshed}");
            assert_eq!(answer(&mut tk, "select * from mv_mlog_refresh order by a"), r#"RS:[["1","10","1"]]"#);
        } else {
            assert_eq!(refreshed, "OK", "{mode}: OOP cutover must remain runnable after CREATE MLog is cancelled");
            assert_eq!(answer(&mut tk, "select * from mv_mlog_refresh order by a"), r#"RS:[["1","10","1"],["2","20","1"]]"#);
        }
        drop(guards);
        // "A new user retry succeeds after refresh releases its lock,
        // including after OOP cutover has replaced the MV's table ID."
        must_exec_all(&mut tk, &["create materialized view log on mv_mlog_refresh (a, s, cnt)"]);
        let mv = latest_table(&stack, "mv_mlog_refresh");
        let mlog = stack.factory.catalog.get_latest().table_info_by_id(mlog_id(&mv)).expect("the view's log").read().clone();
        assert_eq!(mlog.materialized_view_log.as_ref().unwrap().read().base_table_id, mv.id);
        assert_eq!(answer(&mut tk, r"show tables like '\_\_mv\_shadow\_%'"), "RS:[]");
    }
}
