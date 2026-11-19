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

//! Go `pkg/executor/test/oomtest/oom_test.go`, on the mock store through the
//! session, as Go's testkit runs it: the OOM action's log entry reaches the
//! global logger, whose core (`oomCapture`) records which tracker exceeded
//! its quota or which filtered message was logged.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use crate::support::testkit::*;

/// Go `oomCapture`: a zap core recording the tracker of a `memory exceeds
/// quota` entry, or the message of an entry in its filter.
struct OomCapture {
    level: tidb_log::AtomicLevel,
    state: Arc<Mutex<OomState>>,
}

#[derive(Default)]
struct OomState {
    tracker: String,
    message_filter: HashSet<String>,
}

impl tidb_log::Core for OomCapture {
    fn enabled(&self, level: tidb_log::Level) -> bool {
        level >= self.level.get()
    }
    fn with(&self, _fields: &[tidb_log::Field]) -> Arc<dyn tidb_log::Core> {
        Arc::new(OomCapture {
            level: self.level.clone(),
            state: Arc::clone(&self.state),
        })
    }
    fn write(&self, entry: &tidb_log::Entry, fields: &[tidb_log::Field]) -> Result<(), String> {
        let mut state = self.state.lock().unwrap();
        if entry.message == "memory exceeds quota" {
            let tidb_log::Value::Str(error) = &fields[0].value else {
                panic!("the first field is the error");
            };
            let begin = error.find("8001]").expect("begin not found");
            let end = error.find(" holds").expect("end not found");
            state.tracker = error[begin + "8001]".len()..end].to_owned();
            return Ok(());
        }
        // They are just common background task and not related to the oom.
        if state.message_filter.contains(&entry.message) {
            state.tracker = entry.message.clone();
        }
        Ok(())
    }
    fn level(&self) -> tidb_log::AtomicLevel {
        self.level.clone()
    }
}

/// The handle Go's tests call through the package variable `oom`.
#[derive(Clone)]
struct Oom(Arc<Mutex<OomState>>);

impl Oom {
    fn add_message_filter(&self, values: &[&str]) {
        let mut state = self.0.lock().unwrap();
        for value in values {
            state.message_filter.insert((*value).to_owned());
        }
    }
    fn clear_message_filter(&self) {
        self.0.lock().unwrap().message_filter.clear();
    }
    fn set_tracker(&self, tracker: &str) {
        self.0.lock().unwrap().tracker = tracker.to_owned();
    }
    fn get_tracker(&self) -> String {
        self.0.lock().unwrap().tracker.clone()
    }
}

/// Go `registerHook` (run by `TestMain`): `log.InitLogger` at
/// `$log_level`, then `log.ReplaceGlobals(zap.New(oom), props)`.
fn register_hook() -> Oom {
    let config = tidb_log::Config {
        level: std::env::var("log_level").unwrap_or_default(),
        ..tidb_log::Config::default()
    };
    let (_, props) = tidb_log::init_logger(&config).unwrap();
    let state = Arc::new(Mutex::new(OomState::default()));
    let core = OomCapture {
        level: props.level.clone(),
        state: Arc::clone(&state),
    };
    let _ = tidb_log::replace_globals(tidb_log::Logger::new(Arc::new(core)));
    Oom(state)
}

/// Go `tk.Session().GetSessionVars().MemQuotaQuery = quota`.
fn set_mem_quota_query(tk: &mut TestKit, quota: i64) {
    tk.session().vars_mut().mem_quota_query = quota;
}

/// Go `TestMemTracker4UpdateExec`.
#[test]
fn test_mem_tracker4_update_exec() {
    let oom = register_hook();
    let mut tk = TestKit::new();
    tk.must_exec("use test");
    tk.must_exec("create table t_MemTracker4UpdateExec (id int, a int, b int, index idx_a(`a`))");

    tidb_log::set_level(tidb_log::Level::Info);

    oom.set_tracker("");
    oom.clear_message_filter();
    oom.add_message_filter(&["expensive_query during bootstrap phase"]);

    tk.must_exec("insert into t_MemTracker4UpdateExec values (1,1,1), (2,2,2), (3,3,3)");

    set_mem_quota_query(&mut tk, 244);
    tk.must_exec("update t_MemTracker4UpdateExec set a = 4");
    assert_eq!(oom.get_tracker(), "expensive_query during bootstrap phase");
}

/// Go `TestMemTracker4InsertAndReplaceExec`.
#[test]
fn test_mem_tracker4_insert_and_replace_exec() {
    let oom = register_hook();
    let mut tk = TestKit::new();
    tk.must_exec("use test");
    tk.must_exec("create table t (id int, a int, b int, index idx_a(`a`))");
    tk.must_exec("insert into t values (1,1,1), (2,2,2), (3,3,3)");

    tk.must_exec("create table t_MemTracker4InsertAndReplaceExec (id int, a int, b int, index idx_a(`a`))");

    tidb_log::set_level(tidb_log::Level::Info);

    oom.set_tracker("");
    oom.add_message_filter(&["expensive_query during bootstrap phase"]);

    tk.must_exec("insert into t_MemTracker4InsertAndReplaceExec values (1,1,1), (2,2,2), (3,3,3)");
    set_mem_quota_query(&mut tk, 1);
    oom.clear_message_filter();
    oom.add_message_filter(&["expensive_query during bootstrap phase"]);
    tk.must_exec("insert into t_MemTracker4InsertAndReplaceExec values (1,1,1), (2,2,2), (3,3,3)");
    assert_eq!(oom.get_tracker(), "expensive_query during bootstrap phase");
    set_mem_quota_query(&mut tk, -1);

    oom.set_tracker("");
    oom.clear_message_filter();
    oom.add_message_filter(&["expensive_query during bootstrap phase"]);

    tk.must_exec("replace into t_MemTracker4InsertAndReplaceExec values (1,1,1), (2,2,2), (3,3,3)");
    assert_eq!(oom.get_tracker(), "");
    oom.clear_message_filter();
    oom.add_message_filter(&["expensive_query during bootstrap phase"]);
    set_mem_quota_query(&mut tk, 1);
    tk.must_exec("replace into t_MemTracker4InsertAndReplaceExec values (1,1,1), (2,2,2), (3,3,3)");
    assert_eq!(oom.get_tracker(), "expensive_query during bootstrap phase");
    set_mem_quota_query(&mut tk, -1);

    oom.clear_message_filter();
    oom.add_message_filter(&["background process exited", "initialize slot capacity"]);
    oom.set_tracker("");

    tk.must_exec("insert into t_MemTracker4InsertAndReplaceExec select * from t");
    assert_eq!(oom.get_tracker(), "");
    oom.clear_message_filter();
    oom.add_message_filter(&["expensive_query during bootstrap phase"]);
    set_mem_quota_query(&mut tk, 1);
    tk.must_exec("insert into t_MemTracker4InsertAndReplaceExec select * from t");
    assert_eq!(oom.get_tracker(), "expensive_query during bootstrap phase");
    set_mem_quota_query(&mut tk, -1);

    oom.clear_message_filter();
    oom.set_tracker("");

    tk.must_exec("replace into t_MemTracker4InsertAndReplaceExec select * from t");
    assert_eq!(oom.get_tracker(), "");
    oom.clear_message_filter();
    oom.add_message_filter(&["expensive_query during bootstrap phase"]);
    set_mem_quota_query(&mut tk, 1);
    tk.must_exec("replace into t_MemTracker4InsertAndReplaceExec select * from t");
    assert_eq!(oom.get_tracker(), "expensive_query during bootstrap phase");
    set_mem_quota_query(&mut tk, -1);

    // Go `DMLBatchSize = 1`, `BatchInsert = true`: those variables'
    // `SetSession` fields.
    tk.must_exec("set @@tidb_dml_batch_size = 1");
    tk.must_exec("set @@tidb_batch_insert = 1");

    oom.clear_message_filter();
    oom.set_tracker("");

    tk.must_exec("insert into t_MemTracker4InsertAndReplaceExec values (1,1,1), (2,2,2), (3,3,3)");
    assert_eq!(oom.get_tracker(), "");
    oom.clear_message_filter();
    oom.add_message_filter(&["expensive_query during bootstrap phase"]);
    set_mem_quota_query(&mut tk, 1);
    tk.must_exec("insert into t_MemTracker4InsertAndReplaceExec values (1,1,1), (2,2,2), (3,3,3)");
    assert_eq!(oom.get_tracker(), "expensive_query during bootstrap phase");
    set_mem_quota_query(&mut tk, -1);

    oom.clear_message_filter();
    oom.set_tracker("");

    oom.add_message_filter(&["expensive_query during bootstrap phase"]);

    tk.must_exec("replace into t_MemTracker4InsertAndReplaceExec values (1,1,1), (2,2,2), (3,3,3)");
    assert_eq!(oom.get_tracker(), "");
    oom.clear_message_filter();
    oom.add_message_filter(&["expensive_query during bootstrap phase"]);
    set_mem_quota_query(&mut tk, 1);
    tk.must_exec("replace into t_MemTracker4InsertAndReplaceExec values (1,1,1), (2,2,2), (3,3,3)");
    assert_eq!(oom.get_tracker(), "expensive_query during bootstrap phase");
    set_mem_quota_query(&mut tk, -1);
}

/// Go `TestMemTracker4DeleteExec`.
#[test]
fn test_mem_tracker4_delete_exec() {
    let oom = register_hook();
    let mut tk = TestKit::new();
    tk.must_exec("use test");
    tk.must_exec("create table MemTracker4DeleteExec1 (id int, a int, b int, index idx_a(a), index idx_b(b))");
    tk.must_exec("create table MemTracker4DeleteExec2 (id int, a int, b int, index idx_a(a), index idx_b(b))");

    // delete from single table
    tidb_log::set_level(tidb_log::Level::Info);
    tk.must_exec("insert into MemTracker4DeleteExec1 values(1,1,1), (2,2,2), (3,3,3), (4,4,4), (5,5,5)");

    oom.set_tracker("");
    oom.clear_message_filter();
    oom.add_message_filter(&["expensive_query during bootstrap phase"]);

    tk.must_exec("delete from MemTracker4DeleteExec1");
    assert_eq!(oom.get_tracker(), "");
    tk.must_exec("insert into MemTracker4DeleteExec1 values (1,1,1), (2,2,2), (3,3,3)");
    set_mem_quota_query(&mut tk, 1);
    // Go `MemTracker.SetBytesLimit(1)` on the session tracker, which the
    // next statement's quota (`MemQuotaQuery`) sets again.
    tk.must_exec("delete from MemTracker4DeleteExec1");
    assert_eq!(oom.get_tracker(), "expensive_query during bootstrap phase");

    // delete from multiple table
    set_mem_quota_query(&mut tk, 100000);
    tk.must_exec("insert into MemTracker4DeleteExec1 values(1,1,1)");
    tk.must_exec("insert into MemTracker4DeleteExec2 values(1,1,1)");

    oom.set_tracker("");

    tk.must_exec("delete MemTracker4DeleteExec1, MemTracker4DeleteExec2 from MemTracker4DeleteExec1 join MemTracker4DeleteExec2 on MemTracker4DeleteExec1.a=MemTracker4DeleteExec2.a");
    assert_eq!(oom.get_tracker(), "");
    tk.must_exec("insert into MemTracker4DeleteExec1 values(1,1,1)");
    tk.must_exec("insert into MemTracker4DeleteExec2 values(1,1,1)");
    oom.clear_message_filter();
    oom.set_tracker("");
    oom.add_message_filter(&["memory exceeds quota, rateLimitAction delegate to fallback action"]);

    let _fixed_row_count_hint = tidb_failpoint::enable(
        "github.com/pingcap/tidb/pkg/store/copr/disableFixedRowCountHint",
        "return",
    );
    tk.must_exec("set @@tidb_enable_rate_limit_action = 1");
    // Under cost model v2, a lower quota is needed to trigger rateLimitAction for multi-table DELETE.
    // The query may also trigger spill-to-disk which causes an error; we only verify the OOM action fires.
    set_mem_quota_query(&mut tk, 500);
    let _ = tk.exec_to_err(
        "delete MemTracker4DeleteExec1, MemTracker4DeleteExec2 from MemTracker4DeleteExec1 join MemTracker4DeleteExec2 on MemTracker4DeleteExec1.a=MemTracker4DeleteExec2.a",
    );
    assert_eq!(oom.get_tracker(), "memory exceeds quota, rateLimitAction delegate to fallback action");
}

/// Go `TestOOMActionPriority`.
#[test]
fn test_oom_action_priority() {
    let _oom = register_hook();
    let mut tk = TestKit::new();
    tk.must_exec("use test");
    tk.must_exec("drop table if exists t0");
    tk.must_exec("drop table if exists t1");
    tk.must_exec("drop table if exists t2");
    tk.must_exec("drop table if exists t3");
    tk.must_exec("drop table if exists t4");
    tk.must_exec("create table t0(a int)");
    tk.must_exec("insert into t0 values(1)");
    tk.must_exec("create table t1(a int)");
    tk.must_exec("insert into t1 values(1)");
    tk.must_exec("create table t2(a int)");
    tk.must_exec("insert into t2 values(1)");
    tk.must_exec("create table t3(a int)");
    tk.must_exec("insert into t3 values(1)");
    tk.must_exec("create table t4(a int)");
    tk.must_exec("insert into t4 values(1)");
    tk.must_query("select * from t0 join t1 join t2 join t3 join t4 order by t0.a")
        .check(["1 1 1 1 1"]);
    let (stmt_tracker, _) = tk.session().stmt_ctx_trackers().expect("the query's trackers");
    let action = stmt_tracker.get_fallback_for_test(true).expect("an action");
    // All actions are finished and removed.
    assert_eq!(action.get_priority(), tidb_memory::DEF_LOG_PRIORITY);
}
