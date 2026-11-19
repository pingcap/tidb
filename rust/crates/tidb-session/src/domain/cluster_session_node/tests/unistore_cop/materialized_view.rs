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
    (stack, users, session)
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

/// Go master `rollbackCreateMaterializedView` after a failed initial build
/// (`mockCreateMaterializedViewBuildErr`): the statement fails with the
/// build's error, and the view, its refresh rows and its entries in the base
/// table and the log are gone.
/// Go master `TestCreateMaterializedViewRefreshInfoRunningAndSuccess`:
/// while the initial build runs, the refresh-info row already holds the
/// build's read TSO, ANALYZE (`buildAnalyzeTable`'s `CheckMViewReadable`)
/// and both refresh forms refuse the view, and no refresh source is
/// registered; once the build finishes, the view reads, its source is
/// registered, ANALYZE runs and the refresh-info TSO moved forward.
#[test]
fn create_materialized_view_refresh_info_running_and_success() {
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
fn cancel_materialized_view_log_purge_job_as_go_master() {
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
fn purge_cancel_watcher_uses_the_hist_request_as_go_master() {
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
fn purge_running_hist_heartbeat_as_go_master() {
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

/// Go master `testCompareMaterializedViewPrivilegeSkeleton`.
#[test]
fn compare_materialized_view_privileges_as_go_master() {
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
#[test]
fn compare_materialized_view_checks_privileges_before_the_snapshot_as_go_master() {
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
#[test]
fn compare_materialized_view_summary_and_output_as_go_master() {
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
#[test]
fn compare_materialized_view_in_a_stale_read_only_transaction_as_go_master() {
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
#[test]
fn compare_materialized_view_reads_the_snapshot_view_after_cutover_as_go_master() {
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
#[test]
fn compare_materialized_view_output_writes_in_batches_as_go_master() {
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
#[test]
fn compare_materialized_view_drops_the_output_table_on_a_write_failure_as_go_master() {
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
#[test]
fn compare_materialized_view_recomputes_in_the_definition_time_zone_as_go_master() {
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
#[test]
fn compare_materialized_view_output_keeps_timestamp_instants_as_go_master() {
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
#[test]
fn compare_materialized_view_joins_a_null_group_key_as_go_master() {
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
#[test]
fn compare_materialized_view_of_empty_inputs_as_go_master() {
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
#[test]
fn compare_materialized_view_refuses_a_read_tso_after_the_snapshot_as_go_master() {
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
    out
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

/// Go master `TestMaterializedViewServiceAutomaticMaintenance/refresh_and_purge`:
/// the MV service refreshes a due view FAST and purges a due log on its
/// own, and the `tidb_mview_task_*` globals reach it.
#[test]
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
#[test]
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
#[test]
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
#[test]
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
