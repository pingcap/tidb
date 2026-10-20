// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Go `group_controller_test.go`.

use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serial_test::serial;
use tokio::sync::mpsc;

use super::config::{
    default_ru_config, DEFAULT_AVG_BATCH_PROPORTION, DEFAULT_TARGET_PERIOD,
    EXTENDED_REPORTING_PERIOD_FACTOR,
};
use super::context::Context;
use super::global_controller::{SelectType, BIG_REQUEST_THRESHOLD};
use super::group_controller::{
    init_counter_notify, init_metrics, new_group_cost_controller, new_request_source_metrics_state,
    GroupCostController,
};
use super::limiter::{is_throttled_error, NotifyMsg, TokenBucketReconfigureArgs};
use super::model::{
    calculate_cross_az_traffic, get_ru_value_from_consumption, new_kv_calculator,
    AccessLocationType, RequestInfo, ResourceCalculator,
};
use super::test_support::{
    closed_channel, counter_value, create_test_group_cost_controller,
    create_test_group_cost_controller_with_config, eventually, histogram_sample_count, ru_group,
};
use super::testutil::{TestRequestInfo, TestResponseInfo};
use crate::proto::resource_manager::{
    Consumption, TokenBucket, TokenBucketResponse, TokenLimitSettings,
};

fn assert_in_delta(expected: f64, actual: f64, delta: f64, message: &str) {
    assert!(
        (expected - actual).abs() <= delta,
        "{message}: expected {expected}, actual {actual}"
    );
}

fn base_read_cost() -> f64 {
    let cfg = default_ru_config();
    cfg.read_base_cost + cfg.read_per_batch_base_cost * DEFAULT_AVG_BATCH_PROPORTION
}

fn reconfigure_tokens(gc: &GroupCostController, tokens: f64) {
    gc.request_unit_tokens.limiter.reconfigure(
        Instant::now(),
        TokenBucketReconfigureArgs {
            new_tokens: tokens,
            new_fill_rate: 0.0,
            new_burst: 0,
            ..TokenBucketReconfigureArgs::default()
        },
        &[],
    );
}

fn available_tokens(gc: &GroupCostController) -> f64 {
    gc.request_unit_tokens
        .limiter
        .available_tokens(Instant::now())
}

#[test]
#[serial]
#[allow(non_snake_case)]
fn source_go_resource_group_controller_group_controller_test_TestGroupControlBurstable() {
    let gc = create_test_group_cost_controller();
    let args = TokenBucketReconfigureArgs {
        new_fill_rate: 1000.0,
        new_burst: -1,
        ..TokenBucketReconfigureArgs::default()
    };
    gc.request_unit_tokens
        .limiter
        .reconfigure(Instant::now(), args, &[]);
    gc.update_avg_request_resource_per_sec();
    assert!(gc.burstable.load(Ordering::SeqCst));
}

#[test]
#[serial]
#[allow(non_snake_case)]
fn source_go_resource_group_controller_group_controller_test_TestCalcAvgSkipsNonPositiveDeltaDuration(
) {
    let gc = create_test_group_cost_controller();
    let counter = gc.request_unit_tokens.clone();
    // A controller published between the two passes of one tick still has
    // `run.now == avg_last_time`: the sample must be skipped, not 0/0.
    assert_eq!(
        gc.run.lock().unwrap().now,
        counter.state.lock().unwrap().avg_last_time
    );
    gc.update_avg_request_resource_per_sec();
    {
        let state = counter.state.lock().unwrap();
        assert!(!state.avg_ru_per_sec.is_nan());
        assert_eq!(state.avg_ru_per_sec, 0.0);
        // The skipped sample does not advance the accounting state.
        assert_eq!(gc.run.lock().unwrap().now, state.avg_last_time);
        assert_eq!(state.avg_ru_per_sec_last_ru, 0.0);
    }

    // Once the run state advances, sampling resumes.
    {
        let mut run = gc.run.lock().unwrap();
        run.consumption.r_r_u = 100.0;
        run.now += Duration::from_secs(1);
    }
    gc.update_avg_request_resource_per_sec();
    let state = counter.state.lock().unwrap();
    assert!(!state.avg_ru_per_sec.is_nan());
    assert!(state.avg_ru_per_sec > 0.0);
}

#[test]
#[serial]
#[allow(non_snake_case)]
fn source_go_resource_group_controller_group_controller_test_TestSmallPositiveConsumptionDoesNotBypassThreshold(
) {
    let gc = create_test_group_cost_controller();
    let now = Instant::now();
    {
        let mut run = gc.run.lock().unwrap();
        run.consumption.r_r_u = 1.0;
        run.last_request_time = now - DEFAULT_TARGET_PERIOD;
        run.now = now;
    }
    gc.initial_request_completed.store(true, Ordering::SeqCst);

    let report = gc.collect_request_and_consumption(SelectType::PeriodicReport);
    assert!(report.is_none());
}

#[test]
#[serial]
#[allow(non_snake_case)]
fn source_go_resource_group_controller_group_controller_test_TestDirectReportConsumptionReportsOnlyConsumption(
) {
    let gc = create_test_group_cost_controller();
    gc.add_ru_consumption(Some(&Consumption {
        r_r_u: 7.0,
        w_r_u: 3.0,
        write_bytes: 1024.0,
        ..Consumption::default()
    }));
    gc.update_run_state();

    let report = gc
        .collect_request_and_consumption(SelectType::PeriodicReport)
        .unwrap();
    let consumption = report.consumption_since_last_request.unwrap();
    assert_eq!(consumption.r_r_u, 7.0);
    assert_eq!(consumption.w_r_u, 3.0);
    assert_eq!(consumption.write_bytes, 1024.0);
}

#[tokio::test]
#[serial]
#[allow(non_snake_case)]
async fn source_go_resource_group_controller_group_controller_test_TestRequestAndResponseConsumption(
) {
    let gc = create_test_group_cost_controller();
    let write = |access_type| TestRequestInfo {
        is_write: true,
        write_bytes: 100,
        num_replicas: 3,
        access_type,
        ..TestRequestInfo::default()
    };
    let read = |access_type| TestRequestInfo {
        is_write: false,
        write_bytes: 0,
        num_replicas: 3,
        access_type,
        ..TestRequestInfo::default()
    };
    let write_resp = TestResponseInfo {
        read_bytes: 100,
        succeed: true,
        ..TestResponseInfo::default()
    };
    let read_resp = TestResponseInfo {
        read_bytes: 100,
        kv_cpu: Duration::from_millis(100),
        succeed: true,
    };
    let test_cases = [
        // Write request.
        (write(AccessLocationType::AccessUnknown), write_resp.clone()),
        // Write request local AZ.
        (
            write(AccessLocationType::AccessLocalZone),
            write_resp.clone(),
        ),
        // Write request cross AZ.
        (
            write(AccessLocationType::AccessCrossZone),
            write_resp.clone(),
        ),
        // Read request.
        (read(AccessLocationType::AccessLocalZone), read_resp.clone()),
        // Read request cross AZ.
        (read(AccessLocationType::AccessCrossZone), read_resp.clone()),
    ];
    let kv_calculator = gc.get_kv_calculator().unwrap().clone();
    for (idx, (req, resp)) in test_cases.iter().enumerate() {
        let case_num = format!("case {idx}");
        let (consumption, _, _, priority) = gc
            .on_request_wait_impl(&Context::todo(), req)
            .await
            .expect(&case_num);
        assert_eq!(priority, gc.get_meta().priority);
        let mut expected_consumption = Consumption::default();
        if req.is_write() {
            kv_calculator.calculate_write_cost(&mut expected_consumption, req);
            assert_eq!(expected_consumption.w_r_u, consumption.w_r_u);
            if req.access_location_type() != AccessLocationType::AccessUnknown {
                assert!(
                    expected_consumption.write_cross_az_traffic_bytes > 0,
                    "{case_num}"
                );
            }
        }
        let consumption = gc.on_response_impl(req, resp).expect(&case_num);
        kv_calculator.calculate_read_cost(&mut expected_consumption, resp);
        kv_calculator.calculate_cpu_cost(&mut expected_consumption, resp);
        calculate_cross_az_traffic(&mut expected_consumption, req, resp);
        assert_eq!(expected_consumption.r_r_u, consumption.r_r_u, "{case_num}");
        assert_eq!(
            expected_consumption.total_cpu_time_ms, consumption.total_cpu_time_ms,
            "{case_num}"
        );
        if req.is_write() && req.access_location_type() != AccessLocationType::AccessUnknown {
            assert!(
                expected_consumption.write_cross_az_traffic_bytes > 0,
                "{case_num}"
            );
        } else if !req.is_write()
            && req.access_location_type() == AccessLocationType::AccessCrossZone
        {
            assert!(
                expected_consumption.read_cross_az_traffic_bytes > 0,
                "{case_num}"
            );
        } else {
            assert_eq!(
                expected_consumption.read_cross_az_traffic_bytes, 0,
                "{case_num}"
            );
            assert_eq!(
                expected_consumption.write_cross_az_traffic_bytes, 0,
                "{case_num}"
            );
        }
    }
}

#[tokio::test]
#[serial]
#[allow(non_snake_case)]
async fn source_go_resource_group_controller_group_controller_test_TestOnResponseWaitConsumption() {
    let gc = create_test_group_cost_controller();

    let req = TestRequestInfo {
        is_write: false,
        ..TestRequestInfo::default()
    };
    let resp = TestResponseInfo {
        // 2000 RU.
        read_bytes: 2000 * 64 * 1024,
        succeed: true,
        ..TestResponseInfo::default()
    };

    let (consumption, wait_time) = gc
        .on_response_wait_impl(&Context::todo(), &req, &resp)
        .await
        .unwrap();
    assert_eq!(wait_time, Duration::ZERO);
    let verify = |consumption: &Consumption| {
        let mut expected_consumption = Consumption::default();
        gc.get_kv_calculator()
            .unwrap()
            .calculate_read_cost(&mut expected_consumption, &resp);
        assert_eq!(expected_consumption.r_r_u, consumption.r_r_u);
    };
    verify(&consumption);

    // After modifying the counter, the response waits.
    gc.modify_token_counter(
        &gc.request_unit_tokens,
        &TokenBucket {
            settings: Some(TokenLimitSettings {
                fill_rate: 1000,
                burst_limit: 1000,
                ..TokenLimitSettings::default()
            }),
            ..TokenBucket::default()
        },
        (Duration::from_secs(5).as_millis()) as i64,
    );

    let (consumption, wait_time) = gc
        .on_response_wait_impl(&Context::todo(), &req, &resp)
        .await
        .unwrap();
    assert_ne!(wait_time, Duration::ZERO);
    verify(&consumption);
}

#[tokio::test]
#[serial]
#[allow(non_snake_case)]
async fn source_go_resource_group_controller_group_controller_test_TestOnResponseWaitZeroSettlementObservesSuccessfulDuration(
) {
    let gc = create_test_group_cost_controller();
    assert!(!gc.burstable.load(Ordering::SeqCst));

    let samples_before = histogram_sample_count(&gc.metrics.successful_request_duration);
    let (_, wait_time) = gc
        .on_response_wait_impl(
            &Context::todo(),
            &TestRequestInfo {
                is_write: false,
                ..TestRequestInfo::default()
            },
            &TestResponseInfo {
                succeed: true,
                ..TestResponseInfo::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(wait_time, Duration::ZERO);
    assert_eq!(
        samples_before + 1,
        histogram_sample_count(&gc.metrics.successful_request_duration)
    );
}

#[test]
#[serial]
#[allow(non_snake_case)]
fn source_go_resource_group_controller_group_controller_test_TestPredictedReadBytesPreCharge() {
    let cfg = Arc::new(default_ru_config());
    let kv_calc = new_kv_calculator(cfg.clone());

    // A PredictedReadBytes hint pre-charges baseCost + predicted * ReadBytesCost.
    let predicted_read_bytes = 256 * 1024u64;
    let req = TestRequestInfo {
        is_write: false,
        is_cop: true,
        predicted_read_bytes,
        ..TestRequestInfo::default()
    };
    let mut precharge = Consumption::default();
    kv_calc.before_kv_request(&mut precharge, &req);

    let base_cost = base_read_cost();
    let hint_cost = cfg.read_bytes_cost * predicted_read_bytes as f64;
    assert_in_delta(
        base_cost + hint_cost,
        precharge.r_r_u,
        1e-6,
        "BeforeKVRequest should pre-charge based on PredictedReadBytes",
    );

    // Settlement subtracts the same hint basis, preserving
    // precharge + settle == baseCost + actualCost.
    let actual_read_bytes = 300 * 1024u64;
    let resp = TestResponseInfo {
        read_bytes: actual_read_bytes,
        kv_cpu: Duration::from_millis(10),
        succeed: true,
    };
    let mut settle = Consumption::default();
    kv_calc.after_kv_request(&mut settle, &req, &resp);

    let actual_read_cost = cfg.read_bytes_cost * actual_read_bytes as f64;
    let cpu_cost = cfg.cpu_ms_cost * 10.0;
    assert_in_delta(
        actual_read_cost + cpu_cost - hint_cost,
        settle.r_r_u,
        1e-6,
        "AfterKVRequest should settle using the same hint basis as BeforeKVRequest",
    );

    let total_rru = precharge.r_r_u + settle.r_r_u;
    assert_in_delta(
        base_cost + actual_read_cost + cpu_cost,
        total_rru,
        1e-6,
        "Total RRU across precharge+settle should equal baseCost + actualCost",
    );
}

#[tokio::test]
#[serial]
#[allow(non_snake_case)]
async fn source_go_resource_group_controller_group_controller_test_TestPagingPrechargeDoesNotEnterReportedRequestConsumption(
) {
    let gc = create_test_group_cost_controller();
    let cfg = default_ru_config();

    let predicted_read_bytes = 4 * 1024 * 1024u64;
    let req = TestRequestInfo {
        is_write: false,
        is_cop: true,
        predicted_read_bytes,
        ..TestRequestInfo::default()
    };

    let (token_delta, _, _, _) = gc
        .on_request_wait_impl(&Context::todo(), &req)
        .await
        .unwrap();

    let base_cost = base_read_cost();
    let precharge_cost = cfg.read_bytes_cost * predicted_read_bytes as f64;
    assert_in_delta(
        base_cost + precharge_cost,
        token_delta.r_r_u,
        1e-6,
        "request token ledger should still include paging pre-charge",
    );

    gc.update_run_state();
    let report = gc
        .collect_request_and_consumption(SelectType::PeriodicReport)
        .unwrap();
    assert_in_delta(
        base_cost,
        report.consumption_since_last_request.unwrap().r_r_u,
        1e-6,
        "reported request consumption must exclude the predicted-read reservation",
    );
}

#[tokio::test]
#[serial]
#[allow(non_snake_case)]
async fn source_go_resource_group_controller_group_controller_test_TestPagingPrechargeRefundDoesNotEnterReportedResponseConsumption(
) {
    let gc = create_test_group_cost_controller();
    let cfg = default_ru_config();

    let predicted_read_bytes = 4 * 1024 * 1024u64;
    let actual_read_bytes = 512 * 1024u64;
    let req = TestRequestInfo {
        is_write: false,
        is_cop: true,
        predicted_read_bytes,
        ..TestRequestInfo::default()
    };
    let resp = TestResponseInfo {
        read_bytes: actual_read_bytes,
        succeed: true,
        ..TestResponseInfo::default()
    };

    gc.on_request_wait_impl(&Context::todo(), &req)
        .await
        .unwrap();
    gc.update_run_state();
    let request_report = gc.collect_request_and_consumption(SelectType::PeriodicReport);
    assert!(request_report.is_some());
    gc.handle_token_bucket_response(&TokenBucketResponse::default());

    let (token_settlement, _) = gc
        .on_response_wait_impl(&Context::todo(), &req, &resp)
        .await
        .unwrap();
    let expected_token_settlement =
        cfg.read_bytes_cost * (actual_read_bytes as f64 - predicted_read_bytes as f64);
    assert_in_delta(
        expected_token_settlement,
        token_settlement.r_r_u,
        1e-6,
        "response token ledger should still settle against the request pre-charge",
    );
    assert!(token_settlement.r_r_u < 0.0);

    gc.run.lock().unwrap().last_request_time =
        Instant::now() - EXTENDED_REPORTING_PERIOD_FACTOR * DEFAULT_TARGET_PERIOD;
    gc.update_run_state();
    let response_report = gc
        .collect_request_and_consumption(SelectType::PeriodicReport)
        .unwrap();
    let reported_rru = response_report
        .consumption_since_last_request
        .unwrap()
        .r_r_u;
    let expected_reported_response_ru = cfg.read_bytes_cost * actual_read_bytes as f64;
    assert_in_delta(
        expected_reported_response_ru,
        reported_rru,
        1e-6,
        "reported response consumption must expose actual read usage, not the paging refund",
    );
    assert!(reported_rru >= 0.0);
}

#[test]
#[serial]
#[allow(non_snake_case)]
fn source_go_resource_group_controller_group_controller_test_TestPredictedReadBytesRequiresCopRead()
{
    let cfg = Arc::new(default_ru_config());
    let kv_calc = new_kv_calculator(cfg.clone());

    let predicted_read_bytes = 256 * 1024u64;
    let req = TestRequestInfo {
        is_write: false,
        is_cop: false,
        predicted_read_bytes,
        ..TestRequestInfo::default()
    };
    let mut precharge = Consumption::default();
    kv_calc.before_kv_request(&mut precharge, &req);
    assert_in_delta(
        base_read_cost(),
        precharge.r_r_u,
        1e-6,
        "non-cop reads must not consume the paging predicted-read hint",
    );

    let actual_read_bytes = 300 * 1024u64;
    let resp = TestResponseInfo {
        read_bytes: actual_read_bytes,
        kv_cpu: Duration::from_millis(10),
        succeed: true,
    };
    let mut settle = Consumption::default();
    kv_calc.after_kv_request(&mut settle, &req, &resp);

    let actual_read_cost = cfg.read_bytes_cost * actual_read_bytes as f64;
    let cpu_cost = cfg.cpu_ms_cost * 10.0;
    assert_in_delta(
        actual_read_cost + cpu_cost,
        settle.r_r_u,
        1e-6,
        "non-cop reads must not settle against the paging predicted-read hint",
    );
}

#[tokio::test]
#[serial]
#[allow(non_snake_case)]
async fn source_go_resource_group_controller_group_controller_test_TestPagingPreChargeTokenRefund()
{
    let gc = create_test_group_cost_controller();
    reconfigure_tokens(&gc, 100000.0);

    // A 4 MB pre-charge and 1 MB actually read.
    let predicted_read_bytes = 4 * 1024 * 1024u64;
    let actual_read_bytes = 1024 * 1024u64;

    let req = TestRequestInfo {
        is_write: false,
        is_cop: true,
        predicted_read_bytes,
        ..TestRequestInfo::default()
    };
    let resp = TestResponseInfo {
        read_bytes: actual_read_bytes,
        succeed: true,
        ..TestResponseInfo::default()
    };

    gc.on_request_wait_impl(&Context::todo(), &req)
        .await
        .unwrap();
    let tokens_after_pre_charge = available_tokens(&gc);

    let samples_before = histogram_sample_count(&gc.metrics.successful_request_duration);
    gc.on_response_wait_impl(&Context::todo(), &req, &resp)
        .await
        .unwrap();
    let tokens_after_settlement = available_tokens(&gc);
    assert_eq!(
        samples_before + 1,
        histogram_sample_count(&gc.metrics.successful_request_duration)
    );

    // The refund (pre-charge - actual) exceeds the actual read cost, so the
    // settlement ends with more tokens than after the pre-charge.
    let cfg = default_ru_config();
    let pre_charge_cost = cfg.read_bytes_cost * predicted_read_bytes as f64;
    let actual_cost = cfg.read_bytes_cost * actual_read_bytes as f64;
    let expected_refund = pre_charge_cost - actual_cost;
    assert!(
        expected_refund > 0.0,
        "sanity: pre-charge should exceed actual cost"
    );
    assert_in_delta(
        tokens_after_pre_charge + expected_refund,
        tokens_after_settlement,
        1.0,
        "limiter should be refunded the excess pre-charged tokens",
    );

    let net_rru = gc.mu.lock().unwrap().consumption.r_r_u;
    assert_in_delta(
        base_read_cost() + actual_cost,
        net_rru,
        1e-6,
        "net consumption should equal baseCost + actualReadCost",
    );
}

#[tokio::test]
#[serial]
#[allow(non_snake_case)]
async fn source_go_resource_group_controller_group_controller_test_TestPagingPreChargeNoRefundWhenActualExceedsEstimate(
) {
    let gc = create_test_group_cost_controller();
    reconfigure_tokens(&gc, 100000.0);

    // A 1 MB pre-charge and 4 MB actually read.
    let req = TestRequestInfo {
        is_write: false,
        is_cop: true,
        predicted_read_bytes: 1024 * 1024,
        ..TestRequestInfo::default()
    };
    let resp = TestResponseInfo {
        read_bytes: 4 * 1024 * 1024,
        succeed: true,
        ..TestResponseInfo::default()
    };

    gc.on_request_wait_impl(&Context::todo(), &req)
        .await
        .unwrap();
    let tokens_after_pre_charge = available_tokens(&gc);

    gc.on_response_wait_impl(&Context::todo(), &req, &resp)
        .await
        .unwrap();
    let tokens_after_settlement = available_tokens(&gc);

    assert!(
        tokens_after_settlement < tokens_after_pre_charge,
        "when actual exceeds pre-charge, settlement should consume tokens"
    );
}

#[tokio::test]
#[serial]
#[allow(non_snake_case)]
async fn source_go_resource_group_controller_group_controller_test_TestOnResponseWaitPrechargedPositiveSettlementDebitsImmediately(
) {
    let mut cfg = default_ru_config();
    cfg.wait_retry_times = 1;
    cfg.wait_retry_interval = Duration::from_millis(1);
    cfg.ltb_max_wait_duration = Duration::from_millis(1);
    let gc = create_test_group_cost_controller_with_config(cfg);

    let limiter = gc.request_unit_tokens.limiter.clone();
    reconfigure_tokens(&gc, 0.0);
    limiter.remove_tokens(Instant::now(), limiter.available_tokens(Instant::now()));
    gc.is_throttled.store(true, Ordering::SeqCst);

    let req = TestRequestInfo {
        is_write: false,
        predicted_read_bytes: 16 * 1024 * 1024,
        is_cop: true,
        ..TestRequestInfo::default()
    };
    let resp = TestResponseInfo {
        read_bytes: 20 * 1024 * 1024,
        succeed: true,
        ..TestResponseInfo::default()
    };
    let mut settlement_delta = Consumption::default();
    for calc in &gc.calculators {
        calc.after_kv_request(&mut settlement_delta, &req, &resp);
    }
    assert!(get_ru_value_from_consumption(Some(&settlement_delta)) > 0.0);
    assert!(
        settlement_delta.read_bytes + settlement_delta.write_bytes >= BIG_REQUEST_THRESHOLD as f64
    );
    assert!(gc.is_throttled.load(Ordering::SeqCst));

    let tokens_before = limiter.available_tokens(Instant::now());
    let samples_before = histogram_sample_count(&gc.metrics.successful_request_duration);
    let (_, wait_duration) = gc
        .on_response_wait_impl(&Context::background(), &req, &resp)
        .await
        .unwrap();
    assert_eq!(wait_duration, Duration::ZERO);

    let tokens_after = limiter.available_tokens(Instant::now());
    assert!(
        tokens_after < tokens_before,
        "precharged positive settlement should be debited immediately"
    );
    assert_eq!(
        samples_before + 1,
        histogram_sample_count(&gc.metrics.successful_request_duration)
    );
}

#[tokio::test]
#[serial]
#[allow(non_snake_case)]
async fn source_go_resource_group_controller_group_controller_test_TestOnResponseImplPagingRefund()
{
    let gc = create_test_group_cost_controller();
    reconfigure_tokens(&gc, 100000.0);

    // A 4 MB pre-charge and 512 KB actually read.
    let req = TestRequestInfo {
        is_write: false,
        is_cop: true,
        predicted_read_bytes: 4 * 1024 * 1024,
        ..TestRequestInfo::default()
    };
    let resp = TestResponseInfo {
        read_bytes: 512 * 1024,
        succeed: true,
        ..TestResponseInfo::default()
    };

    gc.on_request_wait_impl(&Context::todo(), &req)
        .await
        .unwrap();
    let tokens_after_pre_charge = available_tokens(&gc);

    gc.on_response_impl(&req, &resp).unwrap();
    let tokens_after_settlement = available_tokens(&gc);

    assert!(
        tokens_after_settlement > tokens_after_pre_charge,
        "onResponseImpl should refund excess pre-charged tokens"
    );
}

#[tokio::test]
#[serial]
#[allow(non_snake_case)]
async fn source_go_resource_group_controller_group_controller_test_TestNegativePagingSettlementDoesNotForceConsumptionReport(
) {
    let gc = create_test_group_cost_controller();
    reconfigure_tokens(&gc, 100000.0);

    let predicted_read_bytes = 4 * 1024 * 1024u64;
    let actual_read_bytes = 512 * 1024u64;
    let req = TestRequestInfo {
        is_write: false,
        predicted_read_bytes,
        is_cop: true,
        ..TestRequestInfo::default()
    };
    let resp = TestResponseInfo {
        read_bytes: actual_read_bytes,
        succeed: true,
        ..TestResponseInfo::default()
    };

    gc.on_request_wait_impl(&Context::todo(), &req)
        .await
        .unwrap();
    gc.update_run_state();
    let precharge_report = gc.collect_request_and_consumption(SelectType::PeriodicReport);
    assert!(precharge_report.is_some());
    gc.handle_token_bucket_response(&TokenBucketResponse::default());

    let (settlement_delta, _) = gc
        .on_response_wait_impl(&Context::todo(), &req, &resp)
        .await
        .unwrap();
    gc.run.lock().unwrap().last_request_time = Instant::now() - DEFAULT_TARGET_PERIOD;
    gc.update_run_state();
    let report = gc.collect_request_and_consumption(SelectType::PeriodicReport);

    let cfg = default_ru_config();
    let expected_rru =
        cfg.read_bytes_cost * (actual_read_bytes as f64 - predicted_read_bytes as f64);
    assert_in_delta(expected_rru, settlement_delta.r_r_u, 1e-6, "settlement");
    assert!(
        report.is_none(),
        "negative token settlement alone should not force a consumption report"
    );
}

#[tokio::test]
#[serial]
#[allow(non_snake_case)]
async fn source_go_resource_group_controller_group_controller_test_TestPagingPreChargeRefundOnFailedRead(
) {
    let gc = create_test_group_cost_controller();
    reconfigure_tokens(&gc, 100000.0);

    // A 4 MB pre-charge.
    let predicted_read_bytes = 4 * 1024 * 1024u64;
    let req = TestRequestInfo {
        is_write: false,
        is_cop: true,
        predicted_read_bytes,
        ..TestRequestInfo::default()
    };
    // A failed response: nothing read, no CPU, not succeeded.
    let resp = TestResponseInfo {
        read_bytes: 0,
        kv_cpu: Duration::ZERO,
        succeed: false,
    };

    gc.on_request_wait_impl(&Context::todo(), &req)
        .await
        .unwrap();
    let tokens_after_pre_charge = available_tokens(&gc);

    gc.on_response_wait_impl(&Context::todo(), &req, &resp)
        .await
        .unwrap();
    let tokens_after_settlement = available_tokens(&gc);

    // Settlement still runs on a failed read: it subtracts
    // ReadBytesCost*predicted and adds 0 read cost, so the negative delta is
    // refunded. ReadBaseCost is not refunded, as for other failed reads.
    let cfg = default_ru_config();
    let expected_refund = cfg.read_bytes_cost * predicted_read_bytes as f64;
    assert_in_delta(
        tokens_after_pre_charge + expected_refund,
        tokens_after_settlement,
        1.0,
        "failed read with paging hint should refund ReadBytesCost*predicted",
    );
}

#[tokio::test]
#[serial]
#[allow(non_snake_case)]
async fn source_go_resource_group_controller_group_controller_test_TestFailedWriteDoesNotUsePagingRefundPath(
) {
    let gc = create_test_group_cost_controller();
    reconfigure_tokens(&gc, 100000.0);

    let req = TestRequestInfo {
        is_write: true,
        write_bytes: 4 * 1024,
        num_replicas: 3,
        ..TestRequestInfo::default()
    };
    let resp = TestResponseInfo {
        read_bytes: 0,
        succeed: false,
        ..TestResponseInfo::default()
    };

    gc.on_request_wait_impl(&Context::todo(), &req)
        .await
        .unwrap();
    let tokens_after_reservation = available_tokens(&gc);

    let (refund_delta, _) = gc
        .on_response_wait_impl(&Context::todo(), &req, &resp)
        .await
        .unwrap();
    assert!(refund_delta.w_r_u < 0.0);
    let tokens_after_response = available_tokens(&gc);

    assert_in_delta(
        tokens_after_reservation,
        tokens_after_response,
        1.0,
        "only paging over-estimates are refunded, not failed-write reservations",
    );
}

#[tokio::test]
#[serial]
#[allow(non_snake_case)]
async fn source_go_resource_group_controller_group_controller_test_TestFailedWriteOnResponseDoesNotUsePagingRefundPath(
) {
    let gc = create_test_group_cost_controller();
    reconfigure_tokens(&gc, 100000.0);

    let req = TestRequestInfo {
        is_write: true,
        write_bytes: 4 * 1024,
        num_replicas: 3,
        ..TestRequestInfo::default()
    };
    let resp = TestResponseInfo {
        read_bytes: 0,
        succeed: false,
        ..TestResponseInfo::default()
    };

    gc.on_request_wait_impl(&Context::todo(), &req)
        .await
        .unwrap();
    let tokens_after_reservation = available_tokens(&gc);

    let refund_delta = gc.on_response_impl(&req, &resp).unwrap();
    assert!(refund_delta.w_r_u < 0.0);
    let tokens_after_response = available_tokens(&gc);

    assert_in_delta(
        tokens_after_reservation,
        tokens_after_response,
        1.0,
        "only paging over-estimates are refunded, not failed-write reservations",
    );
}

#[tokio::test]
#[serial]
#[allow(non_snake_case)]
async fn source_go_resource_group_controller_group_controller_test_TestNonCopPredictedReadBytesResponseIgnoresPagingAccounting(
) {
    let gc = create_test_group_cost_controller();

    let predicted_read_bytes = 4 * 1024 * 1024u64;
    let actual_read_bytes = 512 * 1024u64;
    let req = TestRequestInfo {
        is_write: false,
        predicted_read_bytes,
        is_cop: false,
        ..TestRequestInfo::default()
    };
    let resp = TestResponseInfo {
        read_bytes: actual_read_bytes,
        kv_cpu: Duration::from_millis(10),
        succeed: true,
    };

    let precharge_before = counter_value(&gc.metrics.precharge_counter);
    let actual_before = counter_value(&gc.metrics.actual_bytes_counter);
    let no_precharge_before = counter_value(&gc.metrics.no_precharge_counter);
    let (delta, _, _, _) = gc
        .on_request_wait_impl(&Context::todo(), &req)
        .await
        .unwrap();
    let cfg = default_ru_config();
    assert_in_delta(
        base_read_cost(),
        delta.r_r_u,
        1e-6,
        "non-cop read hints must not add predicted read bytes to pre-charge",
    );

    let settlement = gc.on_response_impl(&req, &resp).unwrap();
    let actual_read_cost = cfg.read_bytes_cost * actual_read_bytes as f64;
    let cpu_cost = cfg.cpu_ms_cost * 10.0;
    assert_in_delta(
        actual_read_cost + cpu_cost,
        settlement.r_r_u,
        1e-6,
        "non-cop response settlement must bill actual read bytes and CPU",
    );
    assert_in_delta(
        precharge_before,
        counter_value(&gc.metrics.precharge_counter),
        1e-9,
        "precharge",
    );
    assert_in_delta(
        actual_before,
        counter_value(&gc.metrics.actual_bytes_counter),
        1e-9,
        "actual",
    );
    assert_in_delta(
        no_precharge_before,
        counter_value(&gc.metrics.no_precharge_counter),
        1e-9,
        "no precharge",
    );
}

#[test]
#[serial]
#[allow(non_snake_case)]
fn source_go_resource_group_controller_group_controller_test_TestDeletePagingLabelsResetsSeries() {
    // A name no other test uses, so only this test's series are measured.
    let name = "test-paging-cleanup-rg";

    let gmc = init_metrics(name, name, Some(new_request_source_metrics_state(name)));
    // Push a sample through every paging counter and histogram so each
    // label series exists in its vector.
    gmc.observe_paging_request(100);
    gmc.observe_paging_response(100, 80);
    gmc.observe_paging_request(0);
    gmc.observe_paging_response(0, 200);

    // The cached counters are non-zero before the cleanup.
    assert!(counter_value(&gmc.precharge_counter) > 0.0);
    assert!(counter_value(&gmc.actual_bytes_counter) > 0.0);
    assert!(counter_value(&gmc.no_precharge_counter) > 0.0);
    assert!(histogram_sample_count(&gmc.prediction_residual_bytes) > 0);

    gmc.delete_paging_labels(name);

    // Fetching each vector with the same label yields a fresh zero series;
    // a forgotten removal in delete_paging_labels surfaces here.
    let metrics = super::metrics::global_metrics();
    for vec in [
        &metrics.cop_read_precharge_counter,
        &metrics.cop_read_no_precharge_counter,
        &metrics.paging_precharge_bytes_counter,
        &metrics.paging_actual_bytes_counter,
    ] {
        assert_eq!(
            counter_value(&vec.with_label_values(&[name])),
            0.0,
            "paging counter series for {name:?} should be cleared by deletePagingLabels"
        );
    }
    assert_eq!(
        histogram_sample_count(
            &metrics
                .paging_prediction_residual_bytes
                .with_label_values(&[name])
        ),
        0,
        "paging histogram series for {name:?} should be cleared by deletePagingLabels"
    );
}

#[tokio::test]
#[serial]
#[allow(non_snake_case)]
async fn source_go_resource_group_controller_group_controller_test_TestCopReadNoPrechargeGatedByIsCop(
) {
    let gc = create_test_group_cost_controller();

    // Reads without a pre-charge hint reach the interceptor whatever their
    // command; only cop reads count in cop_read_no_precharge_total.
    let resp = TestResponseInfo {
        read_bytes: 1024,
        succeed: true,
        ..TestResponseInfo::default()
    };
    let non_cop = TestRequestInfo {
        is_write: false,
        is_cop: false,
        ..TestRequestInfo::default()
    };
    let cop = TestRequestInfo {
        is_write: false,
        is_cop: true,
        ..TestRequestInfo::default()
    };

    let counter_before = counter_value(&gc.metrics.no_precharge_counter);
    let residual_samples_before = histogram_sample_count(&gc.metrics.prediction_residual_bytes);

    gc.on_request_wait_impl(&Context::todo(), &non_cop)
        .await
        .unwrap();
    gc.on_response_impl(&non_cop, &resp).unwrap();
    assert_in_delta(
        counter_before,
        counter_value(&gc.metrics.no_precharge_counter),
        1e-9,
        "non-cop reads must not increment cop_read_no_precharge_total",
    );
    assert_eq!(
        residual_samples_before,
        histogram_sample_count(&gc.metrics.prediction_residual_bytes),
        "non-cop reads must not be included in paging prediction residuals"
    );

    gc.on_request_wait_impl(&Context::todo(), &cop)
        .await
        .unwrap();
    assert_in_delta(
        counter_before + 1.0,
        counter_value(&gc.metrics.no_precharge_counter),
        1e-9,
        "cop reads without a hint must increment cop_read_no_precharge_total on the request path",
    );
    assert_eq!(
        residual_samples_before,
        histogram_sample_count(&gc.metrics.prediction_residual_bytes),
        "cop reads without a hint must not record prediction residuals before response"
    );

    gc.on_response_impl(&cop, &resp).unwrap();
    assert_in_delta(
        counter_before + 1.0,
        counter_value(&gc.metrics.no_precharge_counter),
        1e-9,
        "cop read responses must not increment cop_read_no_precharge_total again",
    );
    assert_eq!(
        residual_samples_before,
        histogram_sample_count(&gc.metrics.prediction_residual_bytes),
        "cop read responses without a hint must not be included in prediction residuals"
    );
}

#[tokio::test]
#[serial]
#[allow(non_snake_case)]
async fn source_go_resource_group_controller_group_controller_test_TestCopReadNoPrechargeCounterObservedOnRequest(
) {
    let gc = create_test_group_cost_controller();
    let req = TestRequestInfo {
        is_write: false,
        is_cop: true,
        ..TestRequestInfo::default()
    };

    let counter_before = counter_value(&gc.metrics.no_precharge_counter);
    let residual_samples_before = histogram_sample_count(&gc.metrics.prediction_residual_bytes);

    gc.on_request_wait_impl(&Context::todo(), &req)
        .await
        .unwrap();

    assert_in_delta(
        counter_before + 1.0,
        counter_value(&gc.metrics.no_precharge_counter),
        1e-9,
        "cop reads without a prediction count at the request boundary",
    );
    assert_eq!(
        residual_samples_before,
        histogram_sample_count(&gc.metrics.prediction_residual_bytes),
        "prediction residuals are response-only"
    );
}

#[test]
#[serial]
#[allow(non_snake_case)]
fn source_go_resource_group_controller_group_controller_test_TestNoPreChargeWithoutPredictedReadBytes(
) {
    let cfg = Arc::new(default_ru_config());
    let kv_calc = new_kv_calculator(cfg.clone());

    // Without a hint nothing is pre-charged; settlement bills actual bytes.
    let req = TestRequestInfo {
        is_write: false,
        ..TestRequestInfo::default()
    };
    let mut precharge = Consumption::default();
    kv_calc.before_kv_request(&mut precharge, &req);
    assert_in_delta(
        base_read_cost(),
        precharge.r_r_u,
        1e-6,
        "Without a hint, BeforeKVRequest should only charge baseCost",
    );

    let actual_read_bytes = 2 * 1024 * 1024u64;
    let resp = TestResponseInfo {
        read_bytes: actual_read_bytes,
        kv_cpu: Duration::from_millis(10),
        succeed: true,
    };
    let mut settle = Consumption::default();
    kv_calc.after_kv_request(&mut settle, &req, &resp);

    let actual_read_cost = cfg.read_bytes_cost * actual_read_bytes as f64;
    let cpu_cost = cfg.cpu_ms_cost * 10.0;
    assert_in_delta(
        actual_read_cost + cpu_cost,
        settle.r_r_u,
        1e-6,
        "AfterKVRequest should bill actual read cost only when nothing was pre-charged",
    );
}

#[tokio::test]
#[serial]
#[allow(non_snake_case)]
async fn source_go_resource_group_controller_group_controller_test_TestHandleTokenBucketUpdateEventCanceledByInitCounterNotify(
) {
    let gc = create_test_group_cost_controller();
    let counter = gc.request_unit_tokens.clone();
    counter
        .notify
        .lock()
        .unwrap()
        .arm(Instant::now() + Duration::from_secs(3600), 1.0);

    let (ctx, cancel) = Context::with_cancel(&Context::background());
    let done = {
        let gc = gc.clone();
        tokio::spawn(async move { gc.handle_token_bucket_update_event(ctx).await })
    };

    // Never done within 50ms.
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!done.is_finished());

    init_counter_notify(&counter);
    assert!(
        eventually(
            || done.is_finished(),
            Duration::from_secs(1),
            Duration::from_millis(10)
        )
        .await
    );
    init_counter_notify(&counter);
    cancel.cancel();
}

#[tokio::test]
#[serial]
#[allow(non_snake_case)]
async fn source_go_resource_group_controller_group_controller_test_TestHandleTokenBucketUpdateEventCleansNotifyOnTimer(
) {
    let gc = create_test_group_cost_controller();
    let counter = gc.request_unit_tokens.clone();
    let threshold = 42.0;
    counter
        .notify
        .lock()
        .unwrap()
        .arm(Instant::now() + Duration::from_millis(10), threshold);

    let (ctx, cancel) = Context::with_cancel(&Context::background());
    let done = {
        let gc = gc.clone();
        tokio::spawn(async move { gc.handle_token_bucket_update_event(ctx).await })
    };

    assert!(
        eventually(
            || done.is_finished(),
            Duration::from_secs(1),
            Duration::from_millis(10)
        )
        .await
    );

    {
        let notify = counter.notify.lock().unwrap();
        assert!(notify.setup_notification_deadline.is_none());
        assert!(notify.cancel_ch.is_none());
    }
    assert_eq!(
        threshold,
        counter.limiter.mu.lock().unwrap().notify_threshold
    );
    init_counter_notify(&counter);
    cancel.cancel();
}

#[tokio::test]
#[serial]
#[allow(non_snake_case)]
async fn source_go_resource_group_controller_group_controller_test_TestResourceGroupThrottledError()
{
    let gc = create_test_group_cost_controller();
    let req = TestRequestInfo {
        is_write: true,
        write_bytes: 10000000,
        ..TestRequestInfo::default()
    };
    // The group is throttled.
    let error = gc
        .on_request_wait_impl(&Context::todo(), &req)
        .await
        .unwrap_err();
    assert!(is_throttled_error(&error), "{error}");
}

#[tokio::test]
#[serial]
#[allow(non_snake_case)]
async fn source_go_resource_group_controller_group_controller_test_TestPagingPrechargeNotObservedOnThrottle(
) {
    let gc = create_test_group_cost_controller();

    // 4 GiB predicted is about 65536 RU at the default read cost, beyond the
    // max wait budget at the default fill rate, so acquiring throttles.
    let req = TestRequestInfo {
        is_write: false,
        is_cop: true,
        predicted_read_bytes: 4 * 1024 * 1024 * 1024,
        ..TestRequestInfo::default()
    };

    let before = counter_value(&gc.metrics.precharge_counter);
    let error = gc
        .on_request_wait_impl(&Context::todo(), &req)
        .await
        .unwrap_err();
    assert!(is_throttled_error(&error), "{error}");
    let after = counter_value(&gc.metrics.precharge_counter);

    // A throttled request is never settled, so it is not counted either.
    assert_eq!(
        before, after,
        "throttled paging request should not inflate CopReadPrechargeCounter"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
#[allow(non_snake_case)]
async fn source_go_resource_group_controller_group_controller_test_TestAcquireTokensSignalAwareWait(
) {
    // A buffered low-RU channel observes the notification inside reserve_n
    // as a synchronization point.
    let group = ru_group("test", 1000, 0);
    let (notify_ch, mut notified) = mpsc::channel::<NotifyMsg>(1);
    let mut cfg = default_ru_config();
    cfg.wait_retry_interval = Duration::from_secs(5);
    cfg.wait_retry_times = 3;
    let wait_retry_interval = cfg.wait_retry_interval;
    let (update_ch, _update_rx) = mpsc::channel::<Arc<GroupCostController>>(1);
    let gc = new_group_cost_controller(
        Arc::new(group),
        Arc::new(cfg),
        notify_ch,
        update_ch,
        Some(new_request_source_metrics_state("test")),
    )
    .unwrap();

    // A zero fill rate fails every reservation with an infinite wait, the
    // scenario of https://github.com/tikv/pd/issues/10251.
    let counter = gc.request_unit_tokens.clone();
    counter.limiter.reconfigure(
        Instant::now(),
        TokenBucketReconfigureArgs {
            new_tokens: 1000.0,
            new_fill_rate: 0.0,
            new_burst: 0,
            ..TokenBucketReconfigureArgs::default()
        },
        &[],
    );

    let delta = Consumption {
        r_r_u: 5000.0,
        ..Consumption::default()
    };
    let (result_tx, mut result_rx) = mpsc::channel(1);
    {
        let gc = gc.clone();
        tokio::spawn(async move {
            let mut wait_duration = Duration::ZERO;
            let (_, result) = gc
                .acquire_tokens(&Context::background(), &delta, &mut wait_duration, false)
                .await;
            let _ = result_tx.send((result, wait_duration)).await;
        });
    }

    // The notification means the reservation failed and the retry began.
    tokio::time::timeout(Duration::from_secs(2), notified.recv())
        .await
        .expect("timed out waiting for low-RU notification");

    // Reconfiguring closes the reconfigured channel, waking the retry, and
    // provides the tokens for the next reservation.
    counter.limiter.reconfigure(
        Instant::now(),
        TokenBucketReconfigureArgs {
            new_tokens: 100000.0,
            new_fill_rate: 100000.0,
            new_burst: 0,
            ..TokenBucketReconfigureArgs::default()
        },
        &[],
    );

    let (result, wait_duration) = tokio::time::timeout(wait_retry_interval, result_rx.recv())
        .await
        .expect("acquireTokens was not woken up promptly by Reconfigure signal")
        .unwrap();
    assert!(result.is_ok());
    assert!(wait_duration < wait_retry_interval);
}

#[tokio::test]
#[serial]
#[allow(non_snake_case)]
async fn source_go_resource_group_controller_group_controller_test_TestAcquireTokensFallbackToTimer(
) {
    // A short retry interval keeps the test fast.
    let mut cfg = default_ru_config();
    cfg.wait_retry_interval = Duration::from_millis(50);
    cfg.wait_retry_times = 3;
    cfg.ltb_max_wait_duration = Duration::from_millis(100);
    let gc = create_test_group_cost_controller_with_config(cfg);

    // A zero fill rate and no reconfiguration: no signal arrives.
    let counter = gc.request_unit_tokens.clone();
    counter.limiter.reconfigure(
        Instant::now(),
        TokenBucketReconfigureArgs {
            new_tokens: 1000.0,
            new_fill_rate: 0.0,
            new_burst: 0,
            ..TokenBucketReconfigureArgs::default()
        },
        &[],
    );

    let delta = Consumption {
        r_r_u: 5000.0,
        ..Consumption::default()
    };
    let mut wait_duration = Duration::ZERO;
    let (_, result) = gc
        .acquire_tokens(&Context::background(), &delta, &mut wait_duration, false)
        .await;

    // Without a reconfiguration signal every retry is exhausted.
    let error = result.unwrap_err();
    assert!(is_throttled_error(&error), "{error}");
    // The wait is at least retry times * retry interval.
    assert!(wait_duration >= gc.main_cfg.wait_retry_interval * gc.main_cfg.wait_retry_times as u32);
}

/// A stale `cancel_at` must not rewind the limiter's `last`. Two concurrent
/// acquisitions pinned by the waitReservationsBeforeSelect failpoint: A
/// reserves (`last = now_A`) and parks; B advances `last` to `now_B`; A is
/// then cancelled, so `cancel_at` runs with the stale `now_A`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
#[allow(non_snake_case)]
async fn source_go_resource_group_controller_group_controller_test_TestAcquireTokensCancelKeepsLastMonotonic(
) {
    let mut cfg = default_ru_config();
    cfg.ltb_max_wait_duration = Duration::from_secs(30);
    cfg.wait_retry_times = 1;
    let gc = create_test_group_cost_controller_with_config(cfg);
    let counter = gc.request_unit_tokens.clone();

    // A throttled bucket: a 10000-RU request reserves with a multi-second
    // delay and parks in wait_reservations instead of being granted.
    counter.limiter.reconfigure(
        Instant::now(),
        TokenBucketReconfigureArgs {
            new_tokens: 0.0,
            new_fill_rate: 1000.0,
            new_burst: 0,
            ..TokenBucketReconfigureArgs::default()
        },
        &[],
    );

    const FP: &str = "waitReservationsBeforeSelect";
    let (reached_tx, reached_rx) = std::sync::mpsc::channel::<()>();
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let reached_tx = Mutex::new(reached_tx);
    let release_rx = Mutex::new(release_rx);
    fail::cfg_callback(FP, move || {
        let _ = reached_tx.lock().unwrap().send(());
        let _ = release_rx.lock().unwrap().recv();
    })
    .unwrap();
    struct RemoveFailpoint;
    impl Drop for RemoveFailpoint {
        fn drop(&mut self) {
            fail::remove(FP);
        }
    }
    let _remove = RemoveFailpoint;

    // A reserves with a delay and parks at the hook; only A reaches it, the
    // debt-allowed sibling B never enters wait_reservations.
    let (ctx_a, cancel_a) = Context::with_cancel(&Context::background());
    let (result_tx, mut result_rx) = mpsc::channel(1);
    {
        let gc = gc.clone();
        tokio::spawn(async move {
            let mut wd = Duration::ZERO;
            let (_, result) = gc
                .acquire_tokens(
                    &ctx_a,
                    &Consumption {
                        r_r_u: 10000.0,
                        ..Consumption::default()
                    },
                    &mut wd,
                    false,
                )
                .await;
            let _ = result_tx.send(result).await;
        });
    }

    reached_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("timed out waiting for the request to reserve and park");
    // now_A < t_mid <= now_B.
    let t_mid = Instant::now();

    // B advances `last` to now_B through the debt-allowed remove path.
    let mut wd = Duration::ZERO;
    let (_, result) = gc
        .acquire_tokens(
            &Context::background(),
            &Consumption {
                r_r_u: 1.0,
                ..Consumption::default()
            },
            &mut wd,
            true,
        )
        .await;
    assert!(result.is_ok());

    cancel_a.cancel();
    release_tx.send(()).unwrap();
    let result = tokio::time::timeout(Duration::from_secs(5), result_rx.recv())
        .await
        .expect("timed out waiting for acquireTokens to return")
        .unwrap();
    assert!(result.is_err());

    assert!(
        counter.limiter.last() >= t_mid,
        "stale CancelAt rewound lim.last"
    );
}

#[test]
#[serial]
fn closed_notify_channels_drop_notifications() {
    // createTestGroupCostController's channels have no receiver; sends fail
    // without blocking like Go's unbuffered channels nobody reads.
    let sender = closed_channel::<NotifyMsg>();
    assert!(sender.try_send(NotifyMsg::default()).is_err());
}
