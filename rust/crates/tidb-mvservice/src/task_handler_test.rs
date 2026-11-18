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

//! Go master `pkg/mvservice/task_handler_test.go`.

use std::collections::HashMap;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::Arc;

use tidb_timer::go_time::{GoTime, MINUTE, SECOND};

use crate::mvservice_test::{new_service, MockMVServiceHelper};
use crate::service::*;
use crate::service_settings::default_mv_service_config;
use crate::task_handler::{err_mv_task_canceled_manually, MvsError};
use crate::test_helpers_test::*;
use crate::time_proxy::{mvs_now, unix_milli};

/// Go `TestMVServiceMarkFetchFailureRetryCadence`.
#[test]
fn mark_fetch_failure_retry_cadence() {
    let _serial = serial();
    let _clock = install_mock_time_for_test();
    let now = mvs_now();
    let cfg = default_mv_service_config();
    let svc = new_service(Arc::default(), cfg);
    svc.mark_fetch_failure(&now, false);
    assert_eq!(svc.next_fetch_time(&now), now.add(cfg.fetch_interval));

    let svc = new_service(Arc::default(), cfg);
    svc.mark_fetch_failure(&now, true);
    assert_eq!(svc.next_fetch_time(&now), now.add(cfg.basic_interval));

    let mut cfg = default_mv_service_config();
    cfg.basic_interval = 40 * SECOND;
    cfg.fetch_interval = 10 * SECOND;
    let svc = new_service(Arc::default(), cfg);
    svc.mark_fetch_failure(&now, true);
    assert_eq!(svc.next_fetch_time(&now), now.add(cfg.fetch_interval));
}

/// Go `TestMVServiceFetchAllMVMetaReportsDuration` and
/// `TestMVServiceFetchAllMVMetaAvoidsPartialApplyOnFetchError`.
#[test]
fn fetch_all_mv_meta_reports_duration_and_avoids_partial_apply() {
    let _serial = serial();
    let _clock = install_mock_time_for_test();
    let helper = Arc::new(MockMVServiceHelper::default());
    let svc = new_service(helper.clone(), default_mv_service_config());
    svc.fetch_all_mv_meta().unwrap();
    assert_eq!(helper.task_duration_count(MV_FETCH_TYPE_MLOG_PURGE, MV_DURATION_RESULT_SUCCESS), 1);
    assert_eq!(helper.task_duration_count(MV_FETCH_TYPE_MVIEW_REFRESH, MV_DURATION_RESULT_SUCCESS), 1);

    let helper = Arc::new(MockMVServiceHelper::default());
    {
        let mut knobs = helper.knobs.lock().unwrap();
        let next = mvs_now().add(MINUTE);
        knobs.fetch_logs = HashMap::from([(201, MlogPurgeTask::new(201, next.clone(), unix_milli(&next)))]);
        knobs.fetch_views_err = Some(MvsError::Other("fetch views failed".to_owned()));
    }
    let svc = new_service(helper.clone(), default_mv_service_config());
    let old_log_next = mvs_now().add(2 * MINUTE);
    svc.build_mlog_purge_tasks(HashMap::from([(101, MlogPurgeTask::new(101, old_log_next.clone(), unix_milli(&old_log_next)))]));
    let old_mv_next = mvs_now().add(3 * MINUTE);
    let state = MViewTaskState { next_refresh: old_mv_next.clone(), ..MViewTaskState::default() };
    svc.build_mview_refresh_tasks(HashMap::from([(102, MViewTask::new(102, state, unix_milli(&old_mv_next)))]));
    assert!(svc.fetch_all_mv_meta().is_err());
    assert_eq!(svc.last_meta_fetch_millis.load(SeqCst), 0);
    assert_eq!(helper.task_duration_count(MV_FETCH_TYPE_MLOG_PURGE, MV_DURATION_RESULT_SUCCESS), 1);
    assert_eq!(helper.task_duration_count(MV_FETCH_TYPE_MVIEW_REFRESH, MV_DURATION_RESULT_FAILED), 1);
    let logs = svc.mlog_purge_mu.lock().unwrap();
    assert!(logs.pending.contains_key(&101) && !logs.pending.contains_key(&201));
    drop(logs);
    assert!(svc.mview_refresh_mu.lock().unwrap().pending.contains_key(&102));
}

fn running(helper: Arc<MockMVServiceHelper>) -> Arc<MVService> {
    let svc = new_service(helper, default_mv_service_config());
    svc.run_task_executors();
    svc
}

fn wait_finished(svc: &MVService, expected: i64) {
    eventually(|| svc.combined_task_executor_metrics().finished_count == expected, TEST_EVENTUALLY_WAIT, TEST_EVENTUALLY_TICK);
}

fn purge_task(svc: &Arc<MVService>, id: i64, next: GoTime) -> Arc<MlogPurgeTask> {
    let task = MlogPurgeTask::new(id, next.clone(), unix_milli(&next));
    svc.build_mlog_purge_tasks(HashMap::from([(id, Arc::clone(&task))]));
    svc.purge_mlogs(vec![Arc::clone(&task)]);
    task
}

fn refresh_task(svc: &Arc<MVService>, id: i64, next: GoTime) -> Arc<MViewTask> {
    let state = MViewTaskState { next_refresh: next.clone(), ..MViewTaskState::default() };
    let task = MViewTask::new(id, state, unix_milli(&next));
    svc.build_mview_refresh_tasks(HashMap::from([(id, Arc::clone(&task))]));
    svc.refresh_mviews(vec![Arc::clone(&task)]);
    task
}

fn pending_purge(svc: &MVService, id: i64) -> Option<Arc<MlogPurgeTask>> {
    let queue = svc.mlog_purge_mu.lock().unwrap();
    queue.pending.get(&id).and_then(|item| queue.prio.get(*item)).cloned()
}

fn pending_refresh(svc: &MVService, id: i64) -> Option<Arc<MViewTask>> {
    let queue = svc.mview_refresh_mu.lock().unwrap();
    queue.pending.get(&id).and_then(|item| queue.prio.get(*item)).cloned()
}

/// Go `TestMVServiceTaskResult/purge`.
#[test]
fn task_result_purge() {
    let _serial = serial();
    let _clock = install_mock_time_for_test();

    // remove_on_zero_next
    let svc = running(Arc::default());
    let task = purge_task(&svc, 301, mvs_now());
    wait_finished(&svc, 1);
    eventually(|| pending_purge(&svc, 301).is_none(), TEST_EVENTUALLY_WAIT, TEST_EVENTUALLY_TICK);
    assert_eq!(task.retry_count(), 0);
    assert_eq!(svc.metrics.mv_log_count.load(SeqCst), 0);
    svc.close_task_executors();

    // reschedule_on_non_zero_next
    let next = mvs_now().add(MINUTE);
    let helper = Arc::new(MockMVServiceHelper::default());
    helper.knobs.lock().unwrap().purge_next = next.clone();
    let svc = running(helper);
    purge_task(&svc, 302, mvs_now().add(-MINUTE));
    wait_finished(&svc, 1);
    let task = pending_purge(&svc, 302).unwrap();
    assert_eq!(task.next_purge(), next);
    assert_eq!(task.order_ts(), unix_milli(&next));
    svc.close_task_executors();

    // manual_cancel_applied_backoff
    let expected = mvs_now().add(MANUAL_CANCEL_BACKOFF_DELAY + MINUTE);
    let helper = Arc::new(MockMVServiceHelper::default());
    {
        let mut knobs = helper.knobs.lock().unwrap();
        knobs.purge_err = Some(err_mv_task_canceled_manually());
        knobs.purge_manual_cancel_backoff = (true, Some(expected.clone()));
    }
    let svc = running(helper.clone());
    let task = purge_task(&svc, 305, mvs_now().add(-MINUTE));
    wait_finished(&svc, 1);
    assert_eq!(helper.purge_manual_cancel_backoff_calls.load(SeqCst), 1);
    assert_eq!(task.retry_count(), 0);
    let pending = pending_purge(&svc, 305).unwrap();
    assert_eq!((pending.next_purge(), pending.order_ts()), (expected.clone(), unix_milli(&expected)));
    svc.close_task_executors();

    // manual_cancel_applied_clear_schedule
    let helper = Arc::new(MockMVServiceHelper::default());
    {
        let mut knobs = helper.knobs.lock().unwrap();
        knobs.purge_err = Some(err_mv_task_canceled_manually());
        knobs.purge_manual_cancel_backoff = (true, Some(GoTime::zero()));
    }
    let svc = running(helper.clone());
    purge_task(&svc, 307, mvs_now().add(-MINUTE));
    wait_finished(&svc, 1);
    assert_eq!(helper.purge_manual_cancel_backoff_calls.load(SeqCst), 1);
    eventually(|| pending_purge(&svc, 307).is_none(), TEST_EVENTUALLY_WAIT, TEST_EVENTUALLY_TICK);
    svc.close_task_executors();

    // manual_cancel_without_persist_uses_local_backoff
    let helper = Arc::new(MockMVServiceHelper::default());
    helper.knobs.lock().unwrap().purge_err = Some(err_mv_task_canceled_manually());
    let svc = running(helper.clone());
    let now = mvs_now();
    svc.last_meta_fetch_millis.store(unix_milli(&now), SeqCst);
    purge_task(&svc, 306, now.add(-MINUTE));
    wait_finished(&svc, 1);
    assert_eq!(helper.purge_manual_cancel_backoff_calls.load(SeqCst), 1);
    assert_eq!(svc.last_meta_fetch_millis.load(SeqCst), unix_milli(&now));
    assert_eq!(pending_purge(&svc, 306).unwrap().order_ts(), unix_milli(&now.add(MANUAL_CANCEL_BACKOFF_DELAY)));
    svc.close_task_executors();
}

/// Go `TestMVServiceTaskResult/refresh`.
#[test]
fn task_result_refresh() {
    let _serial = serial();
    let _clock = install_mock_time_for_test();

    // remove_on_zero_next
    let svc = running(Arc::default());
    let task = refresh_task(&svc, 401, mvs_now());
    wait_finished(&svc, 1);
    eventually(|| pending_refresh(&svc, 401).is_none(), TEST_EVENTUALLY_WAIT, TEST_EVENTUALLY_TICK);
    assert_eq!(task.retry_count(), 0);
    assert_eq!(svc.metrics.mv_count.load(SeqCst), 0);
    svc.close_task_executors();

    // reschedule_on_non_zero_next
    let next = mvs_now().add(MINUTE);
    let helper = Arc::new(MockMVServiceHelper::default());
    helper.knobs.lock().unwrap().refresh_next = next.clone();
    let svc = running(helper);
    refresh_task(&svc, 402, mvs_now().add(-MINUTE));
    wait_finished(&svc, 1);
    let task = pending_refresh(&svc, 402).unwrap();
    assert_eq!(task.snapshot().next_refresh, next);
    assert_eq!(task.order_ts(), unix_milli(&next));
    svc.close_task_executors();

    // manual_cancel_applied_backoff
    let expected = mvs_now().add(MANUAL_CANCEL_BACKOFF_DELAY + MINUTE);
    let helper = Arc::new(MockMVServiceHelper::default());
    {
        let mut knobs = helper.knobs.lock().unwrap();
        knobs.refresh_err = Some(err_mv_task_canceled_manually());
        knobs.refresh_manual_cancel_backoff = (true, Some(expected.clone()));
    }
    let svc = running(helper.clone());
    let task = refresh_task(&svc, 403, mvs_now().add(-MINUTE));
    wait_finished(&svc, 1);
    assert_eq!(helper.refresh_manual_cancel_backoff_calls.load(SeqCst), 1);
    assert_eq!(task.retry_count(), 0);
    let pending = pending_refresh(&svc, 403).unwrap();
    assert_eq!((pending.snapshot().next_refresh, pending.order_ts()), (expected.clone(), unix_milli(&expected)));
    svc.close_task_executors();

    // manual_cancel_without_persist_uses_local_backoff
    let helper = Arc::new(MockMVServiceHelper::default());
    helper.knobs.lock().unwrap().refresh_err = Some(err_mv_task_canceled_manually());
    let svc = running(helper.clone());
    let now = mvs_now();
    svc.last_meta_fetch_millis.store(unix_milli(&now), SeqCst);
    refresh_task(&svc, 404, now.add(-MINUTE));
    wait_finished(&svc, 1);
    assert_eq!(helper.refresh_manual_cancel_backoff_calls.load(SeqCst), 1);
    assert_eq!(pending_refresh(&svc, 404).unwrap().order_ts(), unix_milli(&now.add(MANUAL_CANCEL_BACKOFF_DELAY)));
    svc.close_task_executors();
}
