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

//! Go `pkg/extworkload/manager_test.go` and `util_test.go`.

use std::sync::{Arc, Mutex};

use super::*;

/// One recorded client call: the method with its arguments, and its context.
#[derive(Default)]
struct FakeClient {
    calls: Mutex<Vec<(String, CallContext)>>,
    fail: Option<&'static str>,
}

struct Fake(Arc<FakeClient>);

impl Fake {
    fn record(&self, call: String, ctx: CallContext) -> Result<(), ClientError> {
        self.0.calls.lock().unwrap().push((call, ctx));
        match self.0.fail {
            Some(message) => Err(ClientError::Other(message.to_owned())),
            None => Ok(()),
        }
    }
}

impl Client for Fake {
    fn close(&self) -> Result<(), ClientError> {
        Ok(())
    }
    fn ping(&self, ctx: CallContext) -> Result<(), ClientError> {
        self.record("Ping".to_owned(), ctx)
    }
    fn register_gcv2(&self, ctx: CallContext, safe_point: u64, gc_life_time: i64) -> Result<(), ClientError> {
        self.record(format!("RegisterGCV2({safe_point}, {gc_life_time})"), ctx)
    }
    fn recycle_gcv2(&self, ctx: CallContext, safe_point: u64) -> Result<(), ClientError> {
        self.record(format!("RecycleGCV2({safe_point})"), ctx)
    }
    fn update_gc_life_time(&self, ctx: CallContext, gc_life_time: i64) -> Result<(), ClientError> {
        self.record(format!("UpdateGCLifeTime({gc_life_time})"), ctx)
    }
    fn register_ttl_task(&self, ctx: CallContext, table_id: i64, ttl_job_enable: bool) -> Result<(), ClientError> {
        self.record(format!("RegisterTTLTask({table_id}, {ttl_job_enable})"), ctx)
    }
    fn delete_ttl_table_info(&self, ctx: CallContext, table_id: i64) -> Result<(), ClientError> {
        self.record(format!("DeleteTTLTableInfo({table_id})"), ctx)
    }
    fn recycle_ttl_task(&self, ctx: CallContext, completed_job_create_time: u64) -> Result<(), ClientError> {
        self.record(format!("RecycleTTLTask({completed_job_create_time})"), ctx)
    }
    fn update_ttl_job_enable(&self, ctx: CallContext, ttl_job_enable: bool) -> Result<(), ClientError> {
        self.record(format!("UpdateTTLJobEnable({ttl_job_enable})"), ctx)
    }
    fn register_auto_analyze(&self, ctx: CallContext, task_id: u64) -> Result<(), ClientError> {
        self.record(format!("RegisterAutoAnalyze({task_id})"), ctx)
    }
    fn recycle_auto_analyze(&self, ctx: CallContext, task_id: u64) -> Result<(), ClientError> {
        self.record(format!("RecycleAutoAnalyze({task_id})"), ctx)
    }
}

fn manager(role: &str, fail: Option<&'static str>) -> (Box<dyn Manager>, Arc<FakeClient>) {
    let fake = Arc::new(FakeClient { fail, ..Default::default() });
    let meta = KeyspaceMeta { name: "ks".to_owned(), ..Default::default() };
    (new_manager_with_client(Box::new(Fake(Arc::clone(&fake))), ExternalWorkloadRole(role.to_owned()), meta), fake)
}

#[test]
fn test_role_predicates_when_disabled() {
    assert!(!is_enabled(None));
    assert!(!is_master(None));
    assert!(!is_gcv2_worker(None));
    assert!(!is_ttl_task_worker(None));
    assert!(!is_auto_analyze_worker(None));
}

#[test]
fn test_role_predicates_dedicated() {
    for (role, expected) in [
        (ROLE_MASTER, [true, false, false, false]),
        (ROLE_GCV2_WORKER, [false, true, false, false]),
        (ROLE_TTL_TASK_WORKER, [false, false, true, false]),
        (ROLE_AUTO_ANALYZE_WORKER, [false, false, false, true]),
    ] {
        let (mgr, _) = manager(role, None);
        let mgr = Some(mgr.as_ref());
        assert!(is_enabled(mgr));
        assert_eq!(
            [is_master(mgr), is_gcv2_worker(mgr), is_ttl_task_worker(mgr), is_auto_analyze_worker(mgr)],
            expected,
            "{role}"
        );
    }
}

#[test]
fn test_abort_gcv2_for_upgrade() {
    assert_eq!(abort_gcv2_for_upgrade(None), Ok(false));
    let (mgr, fake) = manager(ROLE_MASTER, None);
    assert_eq!(abort_gcv2_for_upgrade(Some(mgr.as_ref())), Ok(false));
    assert!(fake.calls.lock().unwrap().is_empty());

    let (mgr, fake) = manager(ROLE_GCV2_WORKER, None);
    assert_eq!(abort_gcv2_for_upgrade(Some(mgr.as_ref())), Ok(true));
    assert_eq!(fake.calls.lock().unwrap()[0].0, format!("RecycleGCV2({})", u64::MAX));

    let (mgr, _) = manager(ROLE_GCV2_WORKER, Some("boom"));
    assert_eq!(abort_gcv2_for_upgrade(Some(mgr.as_ref())).unwrap_err().to_string(), "boom");
}

#[test]
fn test_new_manager_disabled_and_nil_meta() {
    let disabled = ExternalWorkload::default();
    assert!(new_manager(None, &disabled, None).unwrap().is_none());
    let enabled = ExternalWorkload { enable: true, ..Default::default() };
    assert_eq!(
        new_manager(None, &enabled, None).err().unwrap().to_string(),
        "external workload controller requires a non-nil keyspace meta"
    );
}

/// Go `TestManagerMethodsSetDeadlineAndMetrics`: every method runs under the
/// request timeout, and the task methods carry their worker type and action.
#[test]
fn test_manager_methods_set_deadline_and_metrics() {
    let (mgr, fake) = manager(ROLE_MASTER, None);
    assert_eq!(mgr.role().0, ROLE_MASTER);
    assert_eq!(mgr.meta().name, "ks");
    let life = Duration::from_secs(600);
    mgr.initialize_gcv2(life).unwrap();
    mgr.abort_gcv2().unwrap();
    mgr.register_gcv2(42, life).unwrap();
    mgr.recycle_gcv2(42).unwrap();
    mgr.update_gc_life_time(life).unwrap();
    mgr.register_ttl_table_info(9, true).unwrap();
    mgr.delete_ttl_table_info(9).unwrap();
    mgr.recycle_ttl_task(100).unwrap();
    mgr.update_ttl_job_enable(false).unwrap();
    mgr.register_auto_analyze(5).unwrap();
    mgr.recycle_auto_analyze(5).unwrap();
    mgr.close().unwrap();

    let calls = fake.calls.lock().unwrap();
    let labelled = |worker_type, action| Some(CallLabels { worker_type, action });
    let expected = [
        ("RegisterGCV2(0, 600)".to_owned(), labelled("gcv2", "init")),
        (format!("RecycleGCV2({})", u64::MAX), labelled("gcv2", "abort")),
        ("RegisterGCV2(42, 600)".to_owned(), labelled("gcv2", "register")),
        ("RecycleGCV2(42)".to_owned(), labelled("gcv2", "recycle")),
        ("UpdateGCLifeTime(600)".to_owned(), None),
        ("RegisterTTLTask(9, true)".to_owned(), labelled("ttl", "register")),
        ("DeleteTTLTableInfo(9)".to_owned(), None),
        ("RecycleTTLTask(100)".to_owned(), labelled("ttl", "recycle")),
        ("UpdateTTLJobEnable(false)".to_owned(), None),
        ("RegisterAutoAnalyze(5)".to_owned(), labelled("auto-analyze", "register")),
        ("RecycleAutoAnalyze(5)".to_owned(), labelled("auto-analyze", "recycle")),
    ];
    assert_eq!(calls.len(), expected.len());
    for ((call, ctx), (want_call, want_labels)) in calls.iter().zip(&expected) {
        assert_eq!(call, want_call);
        assert_eq!(ctx.timeout, REQUEST_TIMEOUT, "{call}");
        assert_eq!(&ctx.labels, want_labels, "{call}");
    }
}

#[test]
fn test_manager_method_error_propagation() {
    let (mgr, _) = manager(ROLE_MASTER, Some("controller down"));
    let life = Duration::from_secs(1);
    for result in [
        mgr.initialize_gcv2(life),
        mgr.abort_gcv2(),
        mgr.register_gcv2(1, life),
        mgr.recycle_gcv2(1),
        mgr.update_gc_life_time(life),
        mgr.register_ttl_table_info(1, true),
        mgr.delete_ttl_table_info(1),
        mgr.recycle_ttl_task(1),
        mgr.update_ttl_job_enable(true),
        mgr.register_auto_analyze(1),
        mgr.recycle_auto_analyze(1),
    ] {
        assert_eq!(result.unwrap_err().to_string(), "controller down");
    }
}
