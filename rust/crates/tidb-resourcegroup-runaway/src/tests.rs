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

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use chrono::{Local, TimeZone, Utc};
use tidb_datatype::Datum;
use tidb_proto::resource_manager as rmpb;
use tidb_sqlescape::SqlArg;

use crate::record::{gen_batch_delete_watch_by_id_stmt, gen_runaway_queries_stmt, null_time, Param};
use crate::*;

#[derive(Default)]
struct FakeGroups(Mutex<HashMap<String, rmpb::ResourceGroup>>);

impl FakeGroups {
    fn put(&self, name: &str, settings: Option<rmpb::RunawaySettings>) {
        self.0.lock().unwrap().insert(
            name.to_owned(),
            rmpb::ResourceGroup { name: name.to_owned(), runaway_settings: settings, ..Default::default() },
        );
    }
}

impl ResourceGroupsController for FakeGroups {
    fn get_resource_group(&self, name: &str) -> Result<Option<rmpb::ResourceGroup>, String> {
        Ok(self.0.lock().unwrap().get(name).cloned())
    }
}

/// A session whose executors answer every statement with no rows.
#[derive(Default)]
struct RecordingPool(Arc<NoRows>);

#[derive(Default)]
struct NoRows;

impl tidb_sqlexec::SqlExecutor for NoRows {
    fn execute(
        &self,
        _context: &dyn tidb_sqlexec::ExecutionContext,
        _sql: &str,
    ) -> tidb_sqlexec::Result<Vec<Box<dyn tidb_sqlexec::RecordSet>>> {
        Ok(Vec::new())
    }

    fn execute_internal(
        &self,
        _context: &dyn tidb_sqlexec::ExecutionContext,
        _sql: &str,
        _arguments: &[SqlArg<'_>],
    ) -> tidb_sqlexec::Result<Option<Box<dyn tidb_sqlexec::RecordSet>>> {
        Ok(None)
    }

    fn execute_stmt(
        &self,
        _context: &dyn tidb_sqlexec::ExecutionContext,
        _statement: &tidb_ast::Stmt,
    ) -> tidb_sqlexec::Result<Option<Box<dyn tidb_sqlexec::RecordSet>>> {
        Ok(None)
    }
}

impl tidb_sqlexec::RestrictedSqlExecutor for NoRows {
    fn parse_with_params(
        &self,
        _context: &dyn tidb_sqlexec::ExecutionContext,
        sql: &str,
        _arguments: &[SqlArg<'_>],
    ) -> tidb_sqlexec::Result<tidb_ast::Stmt> {
        Err(format!("unparsed {sql}").into())
    }

    fn exec_restricted_stmt(
        &self,
        _context: &dyn tidb_sqlexec::ExecutionContext,
        _statement: &tidb_ast::Stmt,
        _options: &[tidb_sqlexec::OptionFuncAlias],
    ) -> tidb_sqlexec::Result<(Vec<Vec<Datum>>, Vec<tidb_resolve::ResultFieldRef>)> {
        Ok((Vec::new(), Vec::new()))
    }

    fn exec_restricted_sql(
        &self,
        _context: &dyn tidb_sqlexec::ExecutionContext,
        _options: &[tidb_sqlexec::OptionFuncAlias],
        _sql: &str,
        _arguments: &[SqlArg<'_>],
    ) -> tidb_sqlexec::Result<(Vec<Vec<Datum>>, Vec<tidb_resolve::ResultFieldRef>)> {
        Ok((Vec::new(), Vec::new()))
    }
}

impl SqlSession for RecordingPool {
    fn get_sql_executor(&self) -> Arc<dyn tidb_sqlexec::SqlExecutor> {
        Arc::clone(&self.0) as _
    }
    fn get_restricted_sql_executor(&self) -> Arc<dyn tidb_sqlexec::RestrictedSqlExecutor> {
        Arc::clone(&self.0) as _
    }
}

impl SessionPool for RecordingPool {
    fn with_session(&self, body: &mut dyn FnMut(&dyn SqlSession)) -> Result<(), String> {
        body(self);
        Ok(())
    }
}

struct NoTables;

impl InfoCache for NoTables {
    fn table_exists(&self, _schema: &str, _table: &str) -> bool {
        false
    }
    fn table_by_name(&self, _schema: &str, _table: &str) -> Option<tidb_model::GoShared<tidb_model::TableInfo>> {
        None
    }
}

struct NotOwner;

impl DdlOwner for NotOwner {
    fn is_owner(&self) -> bool {
        false
    }
}

fn manager(groups: Arc<FakeGroups>) -> (Arc<Manager>, Arc<RecordingPool>) {
    let pool = Arc::new(RecordingPool::default());
    let manager = Manager::new(
        groups,
        "127.0.0.1:4000",
        Arc::clone(&pool) as Arc<dyn SessionPool>,
        Arc::new(ExitSignal::default()),
        Arc::new(NoTables),
        Arc::new(NotOwner),
    );
    (manager, pool)
}

fn settings(action: rmpb::RunawayAction, processed_keys: i64) -> rmpb::RunawaySettings {
    rmpb::RunawaySettings {
        rule: Some(rmpb::RunawayRule { exec_elapsed_time_ms: 0, processed_keys, request_unit: 0 }),
        action: action as i32,
        watch: None,
        switch_group_name: String::new(),
    }
}

/// Go `genRunawayQueriesStmt` / `genBatchDeleteWatchByIDStmt` /
/// `genInsertionStmt`: the statement text and the escaped values.
#[test]
fn record_statements_follow_gos_text() {
    let start = Utc.with_ymd_and_hms(2026, 11, 5, 1, 2, 3).unwrap();
    let record = Record {
        resource_group_name: "rg1".to_owned(),
        start_time: start,
        match_type: "identify".to_owned(),
        action: "kill".to_owned(),
        sample_text: "select 'x'".to_owned(),
        sql_digest: "sd".to_owned(),
        plan_digest: "pd".to_owned(),
        source: "127.0.0.1:4000".to_owned(),
        exceed_cause: "ProcessedKeys = 2(1)".to_owned(),
        repeats: 3,
    };
    let (sql, params) = gen_runaway_queries_stmt([&record]);
    assert_eq!(
        tidb_sqlescape::escape_sql_text(&sql, &record::args(&params)).unwrap(),
        "INSERT INTO mysql.tidb_runaway_queries (resource_group_name, start_time, match_type, action, sample_sql, sql_digest, plan_digest, tidb_server, rule, repeats) VALUES ('rg1', '2026-11-05 01:02:03', 'identify', 'kill', 'select \\'x\\'', 'sd', 'pd', '127.0.0.1:4000', 'ProcessedKeys = 2(1)', 3)"
    );
    let (sql, params) = gen_batch_delete_watch_by_id_stmt([7]);
    assert_eq!(
        tidb_sqlescape::escape_sql_text(&sql, &record::args(&params)).unwrap(),
        "delete from mysql.tidb_runaway_watch where id in (7)"
    );
    let watch = QuarantineRecord {
        resource_group_name: "rg1".to_owned(),
        start_time: start,
        watch: rmpb::RunawayWatchType::Similar,
        watch_text: "sd".to_owned(),
        source: MANUAL_SOURCE.to_owned(),
        action: rmpb::RunawayAction::SwitchGroup,
        switch_group_name: "rg2".to_owned(),
        exceed_cause: "None".to_owned(),
        ..QuarantineRecord::default()
    };
    let (sql, params) = watch.gen_insertion_stmt();
    assert_eq!(
        tidb_sqlescape::escape_sql_text(&sql, &record::args(&params)).unwrap(),
        "insert into mysql.tidb_runaway_watch VALUES (null, 'rg1', '2026-11-05 01:02:03', NULL, 2, 'sd', 'manual', 4, 'rg2', 'None')"
    );
    assert_eq!(watch.get_action_string(), "SwitchGroup(rg2)");
    assert_eq!(watch.get_record_key(), "rg1/sd");
    assert!(matches!(Param::Time(null_time()).as_arg(), SqlArg::Time(None)));
}

/// Go `CheckThresholds` / `exceedsThresholds`: processed keys accumulate over
/// the coprocessor responses; a KILL group's threshold interrupts with the
/// cause, records one runaway row and keeps reporting the action.
#[test]
fn processed_keys_threshold_kills_and_marks_once() {
    let groups = Arc::new(FakeGroups::default());
    groups.put("rg1", Some(settings(rmpb::RunawayAction::Kill, 100)));
    let (manager, _) = manager(Arc::clone(&groups));
    let checker = manager.derive_checker("rg1", "select 1", "sd", "pd", Local::now()).expect("settings derive a checker");
    assert!(checker.check_thresholds(None, 60, None).is_none());
    assert_eq!(checker.check_action(), rmpb::RunawayAction::NoneAction);
    let error = checker.check_thresholds(None, 40, None).expect("the threshold is reached");
    assert_eq!(
        error.to_string(),
        "[executor:8253]Query execution was interrupted, identified as runaway query [ProcessedKeys = 100(100)]"
    );
    assert_eq!(checker.check_action(), rmpb::RunawayAction::Kill);
    // The rule matched: a coprocessor request is refused before sending.
    let mut context = tidb_proto::kvrpcpb::Context::default();
    assert!(checker.before_cop_request(&mut context).is_err());
    checker.reset_total_processed_keys();
    // No plan: no checker; no settings and no watch: no checker.
    assert!(manager.derive_checker("rg1", "select 1", "sd", "", Local::now()).is_none());
    groups.put("plain", None);
    assert!(manager.derive_checker("plain", "select 1", "sd", "pd", Local::now()).is_none());
}

/// Go `BeforeExecutor` over the watch list: a manual KILL watch quarantines
/// the statement, a SWITCH_GROUP watch answers its existing group, and the
/// watch counts as the group's active watch.
#[test]
fn watch_list_actions_apply_before_the_executor() {
    let groups = Arc::new(FakeGroups::default());
    groups.put("rg1", None);
    groups.put("rg2", None);
    let (manager, _) = manager(Arc::clone(&groups));
    let watch = |text: &str, action, switch: &str| QuarantineRecord {
        id: 1,
        resource_group_name: "rg1".to_owned(),
        start_time: Utc::now(),
        watch: rmpb::RunawayWatchType::Exact,
        watch_text: text.to_owned(),
        source: MANUAL_SOURCE.to_owned(),
        action,
        switch_group_name: switch.to_owned(),
        ..QuarantineRecord::default()
    };
    manager.add_watch(watch("select 1", rmpb::RunawayAction::Kill, ""));
    assert_eq!(manager.get_active_watch_count("rg1"), 1);
    let checker = manager.derive_checker("rg1", "select 1", "sd", "pd", Local::now()).expect("an active watch");
    let error = checker.before_executor().unwrap_err();
    assert_eq!(
        error.to_string(),
        "[executor:8254]Quarantined and interrupted because of being in runaway watch list"
    );
    assert_eq!(checker.check_action(), rmpb::RunawayAction::Kill);
    // The manual record replaces the old one.
    let mut switch = watch("select 1", rmpb::RunawayAction::SwitchGroup, "rg2");
    switch.id = 2;
    manager.add_watch(switch);
    assert_eq!(manager.get_active_watch_count("rg1"), 1);
    let checker = manager.derive_checker("rg1", "select 1", "sd", "pd", Local::now()).unwrap();
    assert_eq!(checker.before_executor().unwrap(), "rg2");
    // A switch to a group that does not exist keeps the group.
    let mut missing = watch("select 2", rmpb::RunawayAction::SwitchGroup, "nope");
    missing.id = 3;
    manager.add_watch(missing);
    let checker = manager.derive_checker("rg1", "select 2", "sd2", "pd2", Local::now()).unwrap();
    assert_eq!(checker.before_executor().unwrap(), "");
    assert_eq!(manager.get_watch_list().len(), 2);
}

/// Go `BeforeCopRequest`: a COOLDOWN group lowers the priority of the
/// requests sent after the rule matched; a SWITCH_GROUP group renames them.
#[test]
fn cooldown_and_switch_group_rewrite_the_request() {
    let groups = Arc::new(FakeGroups::default());
    groups.put("rg1", Some(settings(rmpb::RunawayAction::CoolDown, 1)));
    let mut switch = settings(rmpb::RunawayAction::SwitchGroup, 1);
    switch.switch_group_name = "rg2".to_owned();
    groups.put("rg_switch", Some(switch));
    groups.put("rg2", None);
    let (manager, _) = manager(Arc::clone(&groups));
    let checker = manager.derive_checker("rg1", "select 1", "sd", "pd", Local::now()).unwrap();
    // Not yet runaway: the request is unchanged.
    let mut context = tidb_proto::kvrpcpb::Context::default();
    checker.before_cop_request(&mut context).unwrap();
    assert!(context.resource_control_context.is_none());
    assert!(checker.check_thresholds(None, 5, None).is_none());
    checker.before_cop_request(&mut context).unwrap();
    assert_eq!(context.resource_control_context.as_ref().unwrap().override_priority, 1);
    let checker = manager.derive_checker("rg_switch", "select 1", "sd", "pd", Local::now()).unwrap();
    assert!(checker.check_thresholds(None, 5, None).is_none());
    let mut context = tidb_proto::kvrpcpb::Context::default();
    checker.before_cop_request(&mut context).unwrap();
    assert_eq!(context.resource_control_context.unwrap().resource_group_name, "rg2");
}
