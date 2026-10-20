// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Go `request_source_metrics_test.go`.

use std::sync::Arc;
use std::time::Duration;

use prometheus::Counter;
use serial_test::serial;

use super::context::Context;
use super::global_controller::{new_resource_group_controller, DEFAULT_RESOURCE_GROUP_NAME};
use super::group_controller::{
    new_group_cost_controller, GroupCostController, REQUEST_SOURCE_DIRECTION_CONSUME,
    REQUEST_SOURCE_DIRECTION_REFUND, REQUEST_SOURCE_RU_TYPE_RRU, REQUEST_SOURCE_RU_TYPE_WRU,
};
use super::limiter::is_throttled_error;
use super::model::AccessLocationType;
use super::test_support::{
    collector_metric_count, create_test_group_cost_controller, eventually,
    new_mock_resource_group_provider, ru_group,
};
use super::testutil::{TestRequestInfo, TestResponseInfo};
use crate::proto::resource_manager::Consumption;

fn request_source_counter() -> prometheus::CounterVec {
    super::metrics::global_metrics()
        .request_source_ru_counter
        .clone()
}

#[derive(Default)]
struct RequestSourceMetricsSnapshot {
    rru_consume: Option<Counter>,
    rru_refund: Option<Counter>,
    wru_consume: Option<Counter>,
    wru_refund: Option<Counter>,
}

/// Go `requestSourceStateSnapshot`: the cached series of one request source
/// and the number of cached series of the group.
fn request_source_state_snapshot(
    gc: &GroupCostController,
    request_source: &str,
) -> (RequestSourceMetricsSnapshot, usize) {
    let state = gc.metrics.source_state.as_ref().expect("source state");
    let inner = state.mu.read().unwrap();
    let mut snapshot = RequestSourceMetricsSnapshot::default();
    for (key, metric) in &inner.items {
        if key.request_source != request_source {
            continue;
        }
        let slot = match (key.ru_type, key.direction) {
            (REQUEST_SOURCE_RU_TYPE_RRU, REQUEST_SOURCE_DIRECTION_CONSUME) => {
                &mut snapshot.rru_consume
            }
            (REQUEST_SOURCE_RU_TYPE_RRU, REQUEST_SOURCE_DIRECTION_REFUND) => {
                &mut snapshot.rru_refund
            }
            (REQUEST_SOURCE_RU_TYPE_WRU, REQUEST_SOURCE_DIRECTION_CONSUME) => {
                &mut snapshot.wru_consume
            }
            (REQUEST_SOURCE_RU_TYPE_WRU, REQUEST_SOURCE_DIRECTION_REFUND) => {
                &mut snapshot.wru_refund
            }
            _ => continue,
        };
        *slot = Some(metric.clone());
    }
    (snapshot, inner.items.len())
}

/// Whether two counter handles are the same series: Go's `require.Same`.
fn same_counter(a: &Counter, b: &Counter) -> bool {
    let before = b.get();
    a.inc();
    b.get() == before + 1.0
}

fn write_request(request_source: &str) -> TestRequestInfo {
    TestRequestInfo {
        is_write: true,
        write_bytes: 64,
        num_replicas: 1,
        store_id: 1,
        request_source: request_source.to_owned(),
        ..TestRequestInfo::default()
    }
}

fn read_response(read_bytes: u64) -> TestResponseInfo {
    TestResponseInfo {
        read_bytes,
        succeed: true,
        ..TestResponseInfo::default()
    }
}

/// Makes the group look stale and inactive to the next cleanup.
fn mark_inactive(gc: &GroupCostController) {
    let consumption = gc.mu.lock().unwrap().consumption.clone();
    gc.run.lock().unwrap().consumption = consumption;
    gc.inactive.store(true, std::sync::atomic::Ordering::SeqCst);
}

#[tokio::test]
#[serial]
#[allow(non_snake_case)]
async fn source_go_resource_group_controller_request_source_metrics_test_TestRequestSourceMetricsRecordAndCacheAccountingDeltas(
) {
    let gc = create_test_group_cost_controller();
    let req = TestRequestInfo {
        is_write: true,
        write_bytes: 100,
        num_replicas: 1,
        store_id: 1,
        access_type: AccessLocationType::AccessUnknown,
        request_source: "internal_gc_test".to_owned(),
        ..TestRequestInfo::default()
    };
    let resp = read_response(128);

    let before_count = collector_metric_count(&request_source_counter());

    let (req_consumption, _, _, _) = gc
        .on_request_wait_impl(&Context::background(), &req)
        .await
        .unwrap();
    assert_ne!(req_consumption.w_r_u, 0.0);

    let (source_metrics, cache_size) = request_source_state_snapshot(&gc, &req.request_source);
    assert_eq!(cache_size, 1);
    assert!(source_metrics.wru_consume.is_some());
    assert!(source_metrics.wru_refund.is_none());
    assert!(source_metrics.rru_consume.is_none());
    assert_eq!(
        req_consumption.w_r_u,
        source_metrics.wru_consume.as_ref().unwrap().get()
    );
    assert_eq!(
        before_count + 1,
        collector_metric_count(&request_source_counter())
    );

    let resp_consumption = gc.on_response_impl(&req, &resp).unwrap();
    assert_ne!(resp_consumption.r_r_u, 0.0);

    let (source_metrics, cache_size) = request_source_state_snapshot(&gc, &req.request_source);
    assert_eq!(cache_size, 2);
    assert_eq!(
        req_consumption.w_r_u,
        source_metrics.wru_consume.as_ref().unwrap().get()
    );
    assert_eq!(
        resp_consumption.r_r_u,
        source_metrics.rru_consume.as_ref().unwrap().get()
    );
    assert_eq!(
        before_count + 2,
        collector_metric_count(&request_source_counter())
    );

    gc.on_request_wait_impl(&Context::background(), &req)
        .await
        .unwrap();
    let (cached_metrics, cache_size) = request_source_state_snapshot(&gc, &req.request_source);
    assert_eq!(cache_size, 2);
    assert!(same_counter(
        source_metrics.wru_consume.as_ref().unwrap(),
        cached_metrics.wru_consume.as_ref().unwrap()
    ));
    assert!(same_counter(
        source_metrics.rru_consume.as_ref().unwrap(),
        cached_metrics.rru_consume.as_ref().unwrap()
    ));

    gc.metrics.source_state.as_ref().unwrap().cleanup();
}

#[tokio::test]
#[serial]
#[allow(non_snake_case)]
async fn source_go_resource_group_controller_request_source_metrics_test_TestRequestSourceMetricsSkipRejectedAndZeroDeltas(
) {
    let gc = create_test_group_cost_controller();
    let req = TestRequestInfo {
        is_write: true,
        write_bytes: 10000000,
        request_source: "internal_rejected_request".to_owned(),
        ..TestRequestInfo::default()
    };
    let before_count = collector_metric_count(&request_source_counter());

    let error = gc
        .on_request_wait_impl(&Context::background(), &req)
        .await
        .unwrap_err();
    assert!(is_throttled_error(&error));
    let (_, cache_size) = request_source_state_snapshot(&gc, &req.request_source);
    assert_eq!(cache_size, 0);
    assert_eq!(
        before_count,
        collector_metric_count(&request_source_counter())
    );

    gc.metrics
        .add_request_source_ru_delta(&req.request_source, Some(&Consumption::default()));
    let (_, cache_size) = request_source_state_snapshot(&gc, &req.request_source);
    assert_eq!(cache_size, 0);
    assert_eq!(
        before_count,
        collector_metric_count(&request_source_counter())
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
#[allow(non_snake_case)]
async fn source_go_resource_group_controller_request_source_metrics_test_TestRequestSourceMetricsConcurrentLazyCreation(
) {
    let gc = create_test_group_cost_controller();
    const WORKERS: usize = 64;
    let request_source = "internal_concurrent_metric_creation";
    let mut handles = Vec::new();
    for _ in 0..WORKERS {
        let gc = gc.clone();
        handles.push(tokio::spawn(async move {
            gc.metrics.add_request_source_ru_delta(
                request_source,
                Some(&Consumption {
                    r_r_u: 1.0,
                    w_r_u: -1.0,
                    ..Consumption::default()
                }),
            );
        }));
    }
    for handle in handles {
        handle.await.unwrap();
    }

    let (source_metrics, cache_size) = request_source_state_snapshot(&gc, request_source);
    assert_eq!(cache_size, 2);
    assert_eq!(
        WORKERS as f64,
        source_metrics.rru_consume.as_ref().unwrap().get()
    );
    assert_eq!(
        WORKERS as f64,
        source_metrics.wru_refund.as_ref().unwrap().get()
    );

    gc.metrics.source_state.as_ref().unwrap().cleanup();
}

#[tokio::test]
#[serial]
#[allow(non_snake_case)]
async fn source_go_resource_group_controller_request_source_metrics_test_TestCleanupResourceGroupRemovesRequestSourceMetrics(
) {
    let mock_provider = new_mock_resource_group_provider();
    let controller = new_resource_group_controller(1, mock_provider.clone(), None, 0, vec![])
        .await
        .unwrap();

    let group = ru_group("request-source-cleanup", 1000000, 0);
    mock_provider.on_get_resource_group(&group.name, Some(group.clone()));

    let gc = controller
        .try_get_resource_group_controller(&group.name, false)
        .await
        .unwrap();

    let req = write_request("internal_gc_cleanup");
    let resp = read_response(64);
    let before_count = collector_metric_count(&request_source_counter());

    gc.on_request_wait_impl(&Context::background(), &req)
        .await
        .unwrap();
    gc.on_response_impl(&req, &resp).unwrap();

    mark_inactive(&gc);

    assert!(collector_metric_count(&request_source_counter()) > before_count);

    controller.clean_up_resource_group();

    assert!(controller.load_group_controller(&group.name).is_none());
    let (_, cache_size) = request_source_state_snapshot(&gc, &req.request_source);
    assert_eq!(cache_size, 0);
    assert_eq!(
        before_count,
        collector_metric_count(&request_source_counter())
    );
}

#[tokio::test]
#[serial]
#[allow(non_snake_case)]
async fn source_go_resource_group_controller_request_source_metrics_test_TestCleanupDoesNotReexportExistingCachedHandle(
) {
    let mock_provider = new_mock_resource_group_provider();
    let controller = new_resource_group_controller(1, mock_provider.clone(), None, 0, vec![])
        .await
        .unwrap();

    let group = ru_group("request-source-cleanup-cached-handle", 1000000, 0);
    mock_provider.on_get_resource_group(&group.name, Some(group.clone()));

    let gc = controller
        .try_get_resource_group_controller(&group.name, false)
        .await
        .unwrap();

    let req = write_request("internal_gc_cleanup_cached_handle");
    let resp = read_response(64);
    let before_count = collector_metric_count(&request_source_counter());

    gc.on_request_wait_impl(&Context::background(), &req)
        .await
        .unwrap();
    gc.on_response_impl(&req, &resp).unwrap();

    let (source_metrics, cache_size) = request_source_state_snapshot(&gc, &req.request_source);
    assert_eq!(cache_size, 2);
    assert_eq!(
        before_count + 2,
        collector_metric_count(&request_source_counter())
    );

    mark_inactive(&gc);
    controller.clean_up_resource_group();

    assert!(controller.load_group_controller(&group.name).is_none());
    assert_eq!(
        before_count,
        collector_metric_count(&request_source_counter())
    );

    source_metrics.rru_consume.as_ref().unwrap().inc_by(1.0);
    source_metrics.wru_consume.as_ref().unwrap().inc_by(1.0);

    assert_eq!(
        before_count,
        collector_metric_count(&request_source_counter())
    );
}

#[tokio::test]
#[serial]
#[allow(non_snake_case)]
async fn source_go_resource_group_controller_request_source_metrics_test_TestCleanupPreventsRecreateRequestSourceMetrics(
) {
    let mock_provider = new_mock_resource_group_provider();
    let controller = new_resource_group_controller(1, mock_provider.clone(), None, 0, vec![])
        .await
        .unwrap();

    let group = ru_group("request-source-cleanup-prevent-recreate", 1000000, 0);
    mock_provider.on_get_resource_group(&group.name, Some(group.clone()));

    let gc = controller
        .try_get_resource_group_controller(&group.name, false)
        .await
        .unwrap();

    let req = write_request("internal_gc_cleanup_prevent_recreate");
    let resp = read_response(64);
    let before_count = collector_metric_count(&request_source_counter());

    gc.on_request_wait_impl(&Context::background(), &req)
        .await
        .unwrap();
    gc.on_response_impl(&req, &resp).unwrap();
    assert_eq!(
        before_count + 2,
        collector_metric_count(&request_source_counter())
    );

    mark_inactive(&gc);
    controller.clean_up_resource_group();

    assert!(controller.load_group_controller(&group.name).is_none());
    let (_, cache_size) = request_source_state_snapshot(&gc, &req.request_source);
    assert_eq!(cache_size, 0);
    assert_eq!(
        before_count,
        collector_metric_count(&request_source_counter())
    );

    gc.metrics.add_request_source_ru_delta(
        &req.request_source,
        Some(&Consumption {
            r_r_u: 1.0,
            w_r_u: 1.0,
            ..Consumption::default()
        }),
    );

    let (_, cache_size) = request_source_state_snapshot(&gc, &req.request_source);
    assert_eq!(cache_size, 0);
    assert_eq!(
        before_count,
        collector_metric_count(&request_source_counter())
    );
}

#[tokio::test]
#[serial]
#[allow(non_snake_case)]
async fn source_go_resource_group_controller_request_source_metrics_test_TestTombstoneCleanupRemovesExistingRequestSourceMetrics(
) {
    let mock_provider = new_mock_resource_group_provider();
    let controller = new_resource_group_controller(1, mock_provider.clone(), None, 0, vec![])
        .await
        .unwrap();

    let group = ru_group("request-source-tombstone-cleanup", 1000000, 0);
    let default_group = ru_group(DEFAULT_RESOURCE_GROUP_NAME, 1000000, 0);
    mock_provider.on_get_resource_group(&group.name, Some(group.clone()));
    mock_provider.on_get_resource_group(DEFAULT_RESOURCE_GROUP_NAME, Some(default_group));

    let gc = controller
        .try_get_resource_group_controller(&group.name, false)
        .await
        .unwrap();

    let req = write_request("internal_gc_tombstone_cleanup");
    let resp = read_response(64);
    let before_count = collector_metric_count(&request_source_counter());

    gc.on_request_wait_impl(&Context::background(), &req)
        .await
        .unwrap();
    gc.on_response_impl(&req, &resp).unwrap();
    assert_eq!(
        before_count + 2,
        collector_metric_count(&request_source_counter())
    );

    controller
        .tombstone_group_cost_controller(&group.name)
        .await;
    let tombstone_gc = controller
        .try_get_resource_group_controller(&group.name, true)
        .await
        .unwrap();
    assert!(tombstone_gc
        .tombstone
        .load(std::sync::atomic::Ordering::SeqCst));

    controller.clean_up_resource_group();

    assert!(controller.load_group_controller(&group.name).is_none());
    assert_eq!(
        before_count,
        collector_metric_count(&request_source_counter())
    );
}

#[tokio::test]
#[serial]
#[allow(non_snake_case)]
async fn source_go_resource_group_controller_request_source_metrics_test_TestRevivedResourceGroupCleanupRemovesExistingRequestSourceMetrics(
) {
    let mock_provider = new_mock_resource_group_provider();
    let controller = new_resource_group_controller(1, mock_provider.clone(), None, 0, vec![])
        .await
        .unwrap();

    let group = ru_group("request-source-revive-cleanup", 1000000, 0);
    let default_group = ru_group(DEFAULT_RESOURCE_GROUP_NAME, 1000000, 0);
    mock_provider.on_get_resource_group(&group.name, Some(group.clone()));
    mock_provider.on_get_resource_group(DEFAULT_RESOURCE_GROUP_NAME, Some(default_group));

    let gc = controller
        .try_get_resource_group_controller(&group.name, false)
        .await
        .unwrap();

    let req = write_request("internal_gc_revive_cleanup");
    let resp = read_response(64);
    let before_count = collector_metric_count(&request_source_counter());

    gc.on_request_wait_impl(&Context::background(), &req)
        .await
        .unwrap();
    gc.on_response_impl(&req, &resp).unwrap();
    assert_eq!(
        before_count + 2,
        collector_metric_count(&request_source_counter())
    );

    controller
        .tombstone_group_cost_controller(&group.name)
        .await;
    let tombstone_gc = controller
        .try_get_resource_group_controller(&group.name, true)
        .await
        .unwrap();
    assert!(tombstone_gc
        .tombstone
        .load(std::sync::atomic::Ordering::SeqCst));

    let revived_gc = new_group_cost_controller(
        Arc::new(group.clone()),
        controller.ru_config.load_full(),
        controller.low_token_notify_chan.clone(),
        controller.token_bucket_update_chan.clone(),
        Some(controller.get_or_create_request_source_metrics_state(&group.name)),
    )
    .unwrap();
    assert!(controller.compare_and_swap_group_controller(
        &group.name,
        &tombstone_gc,
        revived_gc.clone()
    ));

    mark_inactive(&revived_gc);

    controller.clean_up_resource_group();

    assert!(controller.load_group_controller(&group.name).is_none());
    assert_eq!(
        before_count,
        collector_metric_count(&request_source_counter())
    );
}

/// After the request source state is cleaned up, getting it again answers a
/// fresh, open state, so a new controller records metrics normally.
#[tokio::test]
#[serial]
#[allow(non_snake_case)]
async fn source_go_resource_group_controller_request_source_metrics_test_TestGetOrCreateAfterCleanupReturnsFreshState(
) {
    let mock_provider = new_mock_resource_group_provider();
    let controller = new_resource_group_controller(1, mock_provider.clone(), None, 0, vec![])
        .await
        .unwrap();

    let group = ru_group("request-source-create-after-cleanup", 1000000, 0);
    mock_provider.on_get_resource_group(&group.name, Some(group.clone()));

    // Phase 1: create a controller, record metrics, then clean up.
    let gc = controller
        .try_get_resource_group_controller(&group.name, false)
        .await
        .unwrap();

    let req = write_request("internal_create_after_cleanup");
    let resp = read_response(64);

    gc.on_request_wait_impl(&Context::background(), &req)
        .await
        .unwrap();
    gc.on_response_impl(&req, &resp).unwrap();

    mark_inactive(&gc);
    controller.clean_up_resource_group();
    assert!(controller.load_group_controller(&group.name).is_none());

    // Phase 2: the state after cleanup is fresh and open.
    let state = controller.get_or_create_request_source_metrics_state(&group.name);
    assert!(!state.mu.read().unwrap().closed);

    // Phase 3: a controller with this state records metrics.
    let new_gc = new_group_cost_controller(
        Arc::new(group.clone()),
        controller.ru_config.load_full(),
        controller.low_token_notify_chan.clone(),
        controller.token_bucket_update_chan.clone(),
        Some(state.clone()),
    )
    .unwrap();

    let before_count = collector_metric_count(&request_source_counter());
    new_gc
        .on_request_wait_impl(&Context::background(), &req)
        .await
        .unwrap();
    new_gc.on_response_impl(&req, &resp).unwrap();

    let (source_metrics, cache_size) = request_source_state_snapshot(&new_gc, &req.request_source);
    assert_eq!(cache_size, 2);
    assert!(source_metrics.wru_consume.as_ref().unwrap().get() > 0.0);
    assert!(source_metrics.rru_consume.as_ref().unwrap().get() > 0.0);
    assert_eq!(
        before_count + 2,
        collector_metric_count(&request_source_counter())
    );

    state.cleanup();
}

/// The full path: a cleaned-up group is created again by
/// try_get_resource_group_controller and records metrics.
#[tokio::test]
#[serial]
#[allow(non_snake_case)]
async fn source_go_resource_group_controller_request_source_metrics_test_TestCleanupThenRecreateViaFullPath(
) {
    let mock_provider = new_mock_resource_group_provider();
    let controller = new_resource_group_controller(1, mock_provider.clone(), None, 0, vec![])
        .await
        .unwrap();

    let group = ru_group("request-source-full-recreate", 1000000, 0);
    mock_provider.on_get_resource_group(&group.name, Some(group.clone()));

    let req = write_request("internal_full_recreate");
    let resp = read_response(64);

    // Create, use, and clean up the group.
    let gc = controller
        .try_get_resource_group_controller(&group.name, false)
        .await
        .unwrap();
    gc.on_request_wait_impl(&Context::background(), &req)
        .await
        .unwrap();
    gc.on_response_impl(&req, &resp).unwrap();

    mark_inactive(&gc);
    controller.clean_up_resource_group();
    assert!(controller.load_group_controller(&group.name).is_none());

    // A new request arriving after the cleanup creates the group again.
    let gc2 = controller
        .try_get_resource_group_controller(&group.name, false)
        .await
        .unwrap();

    let before_count = collector_metric_count(&request_source_counter());
    gc2.on_request_wait_impl(&Context::background(), &req)
        .await
        .unwrap();
    gc2.on_response_impl(&req, &resp).unwrap();

    let (source_metrics, cache_size) = request_source_state_snapshot(&gc2, &req.request_source);
    assert_eq!(cache_size, 2);
    assert!(source_metrics.wru_consume.as_ref().unwrap().get() > 0.0);
    assert!(source_metrics.rru_consume.as_ref().unwrap().get() > 0.0);
    assert_eq!(
        before_count + 2,
        collector_metric_count(&request_source_counter())
    );

    gc2.metrics.source_state.as_ref().unwrap().cleanup();
}

/// Stopping closes every group's state, deletes its cached series, and later
/// accounting does not register series again.
#[tokio::test]
#[serial]
#[allow(non_snake_case)]
async fn source_go_resource_group_controller_request_source_metrics_test_TestStopCleansUpRequestSourceMetricsState(
) {
    let mock_provider = new_mock_resource_group_provider();
    let c = new_resource_group_controller(1, mock_provider.clone(), None, 0, vec![])
        .await
        .unwrap();

    let group = ru_group("request-source-stop-cleanup", 1000000, 0);
    mock_provider.on_get_resource_group(&group.name, Some(group.clone()));

    // The main loop must run for its shutdown path to run.
    c.start();

    let gc = c
        .try_get_resource_group_controller(&group.name, false)
        .await
        .unwrap();

    let req = write_request("internal_stop_cleanup");
    let resp = read_response(64);
    gc.on_request_wait_impl(&Context::background(), &req)
        .await
        .unwrap();
    gc.on_response_impl(&req, &resp).unwrap();

    let ms = c
        .request_source_states
        .lock()
        .unwrap()
        .get(&group.name)
        .cloned()
        .expect("request source state");
    let before_count = collector_metric_count(&request_source_counter());

    c.stop().await.unwrap();

    // The cleanup runs on the main loop as it exits.
    assert!(
        eventually(
            || {
                let inner = ms.mu.read().unwrap();
                inner.closed && inner.items.is_empty()
            },
            Duration::from_secs(5),
            Duration::from_millis(10)
        )
        .await
    );

    // The request registered one consume series each for RRU and WRU.
    assert_eq!(
        before_count - 2,
        collector_metric_count(&request_source_counter())
    );

    // Accounting after the stop registers no series.
    gc.metrics.add_request_source_ru_delta(
        "internal_stop_cleanup_later",
        Some(&Consumption {
            r_r_u: 1.0,
            w_r_u: 1.0,
            ..Consumption::default()
        }),
    );
    assert!(ms.mu.read().unwrap().items.is_empty());
    assert_eq!(
        before_count - 2,
        collector_metric_count(&request_source_counter())
    );
}

/// Consume minus refund mirrors the controller's own consumption when a
/// failed write pays back WRU on the response.
#[tokio::test]
#[serial]
#[allow(non_snake_case)]
async fn source_go_resource_group_controller_request_source_metrics_test_TestFailedWriteMatchesControllerConsumption(
) {
    let gc = create_test_group_cost_controller();

    let req = TestRequestInfo {
        is_write: true,
        write_bytes: 100,
        num_replicas: 3,
        store_id: 1,
        access_type: AccessLocationType::AccessUnknown,
        request_source: "internal_failed_write".to_owned(),
        ..TestRequestInfo::default()
    };
    let success_resp = TestResponseInfo {
        read_bytes: 0,
        succeed: true,
        ..TestResponseInfo::default()
    };
    let failed_resp = TestResponseInfo {
        read_bytes: 0,
        succeed: false,
        ..TestResponseInfo::default()
    };

    for (i, resp) in [
        &success_resp,
        &failed_resp,
        &failed_resp,
        &success_resp,
        &failed_resp,
    ]
    .into_iter()
    .enumerate()
    {
        gc.on_request_wait_impl(&Context::background(), &req)
            .await
            .unwrap_or_else(|error| panic!("iteration {i}: {error}"));
        gc.on_response_impl(&req, resp)
            .unwrap_or_else(|error| panic!("iteration {i}: {error}"));
    }

    let (source_metrics, _) = request_source_state_snapshot(&gc, &req.request_source);

    let (controller_wru, controller_rru) = {
        let mu = gc.mu.lock().unwrap();
        (mu.consumption.w_r_u, mu.consumption.r_r_u)
    };

    let net_wru = source_metrics.wru_consume.as_ref().unwrap().get()
        - source_metrics.wru_refund.as_ref().unwrap().get();
    assert!(
        (controller_wru - net_wru).abs() <= 1e-9,
        "per-source WRU diverges from controller consumption: failed-write payback dropped?"
    );
    assert_eq!(controller_rru, 0.0);
    assert!(source_metrics.rru_consume.is_none());
    assert!(source_metrics.rru_refund.is_none());

    // Failed writes still consume WRU: the per-batch and replication terms of
    // the write cost are not paid back.
    assert!(net_wru > 0.0);

    gc.metrics.source_state.as_ref().unwrap().cleanup();
}
