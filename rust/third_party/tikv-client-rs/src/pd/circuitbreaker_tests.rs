// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::Error;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::time::Duration;

const SETTINGS: Settings = Settings {
    error_rate_threshold_pct: 50,
    min_qps_for_open: 10,
    error_rate_window: 30_000_000_000,
    cool_down_interval: 10_000_000_000,
    half_open_success_count: 2,
};
const MIN_COUNT: usize = 300;

fn breaker(settings: Settings) -> Arc<CircuitBreaker> {
    static ID: AtomicUsize = AtomicUsize::new(0);
    CircuitBreaker::new(
        format!("source_cb_{}", ID.fetch_add(1, Ordering::Relaxed)),
        settings,
    )
}
fn state(cb: &CircuitBreaker) -> StateHandle {
    cb.inner.lock().unwrap().state.clone()
}
fn kind(cb: &CircuitBreaker) -> StateType {
    state(cb).lock().unwrap().state_type
}
fn expire(cb: &CircuitBreaker) {
    let handle = state(cb);
    let mut s = handle.lock().unwrap();
    s.started = Instant::now();
    s.interval = -1;
}
fn drive(cb: &CircuitBreaker, count: usize, overload: bool) {
    for _ in 0..count {
        cb.execute(|| (overload, Ok(()))).unwrap();
    }
}
fn succeeds(cb: &CircuitBreaker) {
    cb.execute(|| (NO, Ok(()))).unwrap();
}
fn fast_fail(cb: &CircuitBreaker) {
    assert!(matches!(
        cb.execute::<()>(|| panic!("open breaker must not invoke call")),
        Err(Error::Pd(error)) if error.definition() == crate::pd::errs::ERR_CIRCUIT_BREAKER_OPEN
    ));
}
fn open_expired() -> Arc<CircuitBreaker> {
    let cb = breaker(SETTINGS);
    drive(&cb, MIN_COUNT, YES);
    expire(&cb);
    fast_fail(&cb);
    assert_eq!(kind(&cb), StateType::OPEN);
    expire(&cb);
    cb
}
fn counts(cb: &CircuitBreaker) -> [f64; 4] {
    let c = cb.counters.read().unwrap();
    [
        c.success.get(),
        c.error.get(),
        c.overload.get(),
        c.fast_fail.get(),
    ]
}

// Original TestCircuitBreakerExecuteWrapperReturnValues.
#[test]
fn execute_wrapper_return_values() {
    let cb = breaker(SETTINGS);
    for overloaded in [NO, YES] {
        let message = "original operation error".to_owned();
        let pointer = message.as_ptr();
        match cb.execute::<()>(|| (overloaded, Err(Error::StringError(message)))) {
            Err(Error::StringError(returned)) => assert_eq!(returned.as_ptr(), pointer),
            other => panic!("original error was replaced: {other:?}"),
        }
    }
    assert_eq!(counts(&cb), [1., 2., 1., 0.]);
}

#[test]
fn open_state() {
    let cb = breaker(SETTINGS);
    drive(&cb, MIN_COUNT, YES);
    assert_eq!(kind(&cb), StateType::CLOSED);
    succeeds(&cb);
    expire(&cb);
    fast_fail(&cb);
    assert_eq!(kind(&cb), StateType::OPEN);
}

#[test]
fn close_state_not_enough_qps() {
    let cb = breaker(SETTINGS);
    drive(&cb, MIN_COUNT / 2, YES);
    expire(&cb);
    succeeds(&cb);
    assert_eq!(kind(&cb), StateType::CLOSED);
}

#[test]
fn close_state_not_enough_error_rate() {
    let cb = breaker(SETTINGS);
    drive(&cb, MIN_COUNT / 4, YES);
    drive(&cb, MIN_COUNT, NO);
    expire(&cb);
    succeeds(&cb);
    assert_eq!(kind(&cb), StateType::CLOSED);
}

#[test]
fn half_open_to_closed() {
    let cb = open_expired();
    succeeds(&cb);
    assert_eq!(kind(&cb), StateType::HALF_OPEN);
    succeeds(&cb);
    assert_eq!(kind(&cb), StateType::HALF_OPEN);
    succeeds(&cb);
    assert_eq!(kind(&cb), StateType::CLOSED);
}

#[test]
fn half_open_to_open() {
    let cb = open_expired();
    succeeds(&cb);
    cb.execute(|| (YES, Ok(()))).unwrap();
    assert_eq!(kind(&cb), StateType::HALF_OPEN);
    fast_fail(&cb);
    assert_eq!(kind(&cb), StateType::OPEN);
}

#[test]
fn half_open_fail_over_pending_count() {
    let cb = open_expired();
    std::thread::scope(|scope| {
        let (started, received) = mpsc::channel();
        let mut releases = Vec::new();
        let mut handles = Vec::new();
        for _ in 0..SETTINGS.half_open_success_count {
            let (release, wait) = mpsc::channel();
            releases.push(release);
            let cb = cb.clone();
            let started = started.clone();
            handles.push(scope.spawn(move || {
                cb.execute(|| {
                    started.send(()).unwrap();
                    wait.recv().unwrap();
                    (NO, Ok(()))
                })
                .unwrap()
            }));
        }
        for _ in 0..SETTINGS.half_open_success_count {
            received.recv().unwrap();
        }
        fast_fail(&cb);
        assert_eq!(kind(&cb), StateType::HALF_OPEN);
        for release in releases {
            release.send(()).unwrap();
        }
        for handle in handles {
            handle.join().unwrap();
        }
    });
    succeeds(&cb);
    assert_eq!(kind(&cb), StateType::CLOSED);
    assert_eq!(state(&cb).lock().unwrap().success_count, 1);
}

#[test]
fn count_only_requests_in_same_window() {
    let cb = breaker(SETTINGS);
    let old = state(&cb);
    std::thread::scope(|scope| {
        let (started, received) = mpsc::channel();
        let (release, wait) = mpsc::channel();
        let request_cb = cb.clone();
        let handle = scope.spawn(move || {
            request_cb
                .execute(|| {
                    started.send(()).unwrap();
                    wait.recv().unwrap();
                    (NO, Ok(()))
                })
                .unwrap()
        });
        received.recv().unwrap();
        assert_eq!(state(&cb).lock().unwrap().success_count, 0);
        expire(&cb);
        succeeds(&cb);
        assert_eq!(state(&cb).lock().unwrap().success_count, 1);
        release.send(()).unwrap();
        handle.join().unwrap();
        assert_eq!(state(&cb).lock().unwrap().success_count, 1);
    });
    assert_eq!(old.lock().unwrap().success_count, 1);
}

#[test]
fn change_settings() {
    let cb = breaker(*ALWAYS_CLOSED_SETTINGS.read().unwrap());
    drive(&cb, 100, YES);
    expire(&cb);
    succeeds(&cb);
    assert_eq!(kind(&cb), StateType::CLOSED);
    cb.change_settings(|config| {
        config.error_rate_threshold_pct = SETTINGS.error_rate_threshold_pct
    });
    assert_eq!(
        cb.inner.lock().unwrap().settings.error_rate_threshold_pct,
        50
    );
    drive(&cb, MIN_COUNT, YES);
    expire(&cb);
    fast_fail(&cb);
    assert_eq!(kind(&cb), StateType::OPEN);
}

#[test]
fn enabled() {
    let cb = breaker(*ALWAYS_CLOSED_SETTINGS.read().unwrap());
    assert!(!cb.is_enabled());
    cb.change_settings(|config| config.error_rate_threshold_pct = 50);
    assert!(cb.is_enabled());
}

#[test]
fn source_boundaries_disabling_and_zero_probe_count() {
    let cb = breaker(SETTINGS);
    drive(&cb, MIN_COUNT, YES);
    let end = {
        let handle = state(&cb);
        let s = handle.lock().unwrap();
        s.started + Duration::from_nanos(s.interval as u64)
    };
    assert!(
        cb.on_request(|| end).is_ok(),
        "expiration is strictly after end"
    );
    assert!(matches!(
        cb.on_request(|| end + Duration::from_nanos(1)),
        Err(Error::Pd(error)) if error.definition() == crate::pd::errs::ERR_CIRCUIT_BREAKER_OPEN
    ));
    let end = {
        let handle = state(&cb);
        let s = handle.lock().unwrap();
        s.started + Duration::from_nanos(s.interval as u64)
    };
    assert!(matches!(
        cb.on_request(|| end),
        Err(Error::Pd(error)) if error.definition() == crate::pd::errs::ERR_CIRCUIT_BREAKER_OPEN
    ));
    cb.change_settings(|s| s.error_rate_threshold_pct = 0);
    succeeds(&cb);
    assert_eq!(kind(&cb), StateType::CLOSED);
    for probe_count in [0, 2] {
        let cb = open_expired();
        cb.change_settings(|s| s.half_open_success_count = probe_count);
        succeeds(&cb);
        if probe_count == 0 {
            fast_fail(&cb);
        }
        cb.change_settings(|s| s.error_rate_threshold_pct = 0);
        succeeds(&cb);
        assert_eq!(kind(&cb), StateType::CLOSED);
    }
}

#[test]
fn pd_breaker_minimum_qps_uses_source_u32_wrapping() {
    let cb = breaker(Settings {
        error_rate_window: 2_000_000_000,
        min_qps_for_open: 1 << 31,
        ..SETTINGS
    });
    cb.execute(|| (YES, Ok(()))).unwrap();
    expire(&cb);
    fast_fail(&cb);
}

#[test]
fn counts_and_percentage_products_wrap_instead_of_saturating() {
    let cb = breaker(Settings {
        min_qps_for_open: 0,
        ..SETTINGS
    });
    {
        let handle = state(&cb);
        let mut s = handle.lock().unwrap();
        s.success_count = u32::MAX;
    }
    succeeds(&cb);
    assert_eq!(state(&cb).lock().unwrap().success_count, 0);
    {
        let handle = state(&cb);
        let mut s = handle.lock().unwrap();
        s.failure_count = u32::MAX;
    }
    cb.execute(|| (YES, Ok(()))).unwrap();
    assert_eq!(state(&cb).lock().unwrap().failure_count, 0);
    {
        let handle = state(&cb);
        let mut s = handle.lock().unwrap();
        s.failure_count = 1 << 30;
    }
    expire(&cb);
    succeeds(&cb); // failure*100 wraps to zero, so the rate is zero.
    assert_eq!(kind(&cb), StateType::CLOSED);
    {
        let handle = state(&cb);
        let mut s = handle.lock().unwrap();
        s.failure_count = u32::MAX;
        s.success_count = 1;
    }
    expire(&cb);
    succeeds(&cb); // total wraps to zero, so no rate is evaluated.
}

#[test]
fn panic_preserves_payload_and_records_overload_without_error() {
    let cb = breaker(Settings {
        min_qps_for_open: 0,
        ..SETTINGS
    });
    let payload = Arc::new("panic identity");
    let sent = payload.clone();
    let caught = catch_unwind(AssertUnwindSafe(|| {
        cb.execute::<()>(|| std::panic::panic_any(sent))
    }))
    .unwrap_err();
    assert!(Arc::ptr_eq(
        caught.downcast_ref::<Arc<&str>>().unwrap(),
        &payload
    ));
    assert_eq!(counts(&cb), [0., 0., 1., 0.]);
    expire(&cb);
    fast_fail(&cb);
    assert_eq!(counts(&cb), [0., 0., 1., 1.]);
    let caught = catch_unwind(AssertUnwindSafe(|| {
        cb.change_settings(|s| {
            s.error_rate_threshold_pct = 0;
            panic!("settings mutator panicked");
        })
    }));
    assert!(caught.is_err());
    assert!(!cb.is_enabled());
    succeeds(&cb);
}

#[tokio::test]
async fn async_results_panics_and_dropped_probe_complete_the_same_admission() {
    use futures::{poll, FutureExt};
    let cb = breaker(SETTINGS);
    assert_eq!(cb.execute_async(|| async { (NO, Ok(7)) }).await.unwrap(), 7);
    let panic = AssertUnwindSafe(cb.execute_async::<(), _>(|| async { panic!("async panic") }))
        .catch_unwind()
        .await;
    assert!(panic.is_err());
    assert_eq!(counts(&cb), [1., 0., 1., 0.]);
    let cb = open_expired();
    cb.change_settings(|s| s.half_open_success_count = 1);
    let before = counts(&cb);
    // Not polling the future cannot consume a half-open probe.
    drop(cb.execute_async::<(), _>(|| std::future::pending()));
    assert_eq!(kind(&cb), StateType::OPEN);
    let mut request = Box::pin(cb.execute_async::<(), _>(|| std::future::pending()));
    assert!(poll!(&mut request).is_pending());
    fast_fail(&cb);
    drop(request);
    assert_eq!(
        counts(&cb),
        [before[0] + 1., before[1] + 1., before[2], before[3] + 1.]
    );
    assert_eq!(kind(&cb), StateType::HALF_OPEN);
    succeeds(&cb);
    assert_eq!(kind(&cb), StateType::CLOSED);
}

#[test]
fn metric_consumer_rebinds_and_preserves_source_event_classes() {
    use prometheus::core::Collector;
    let mut consumer = None;
    let cb = CircuitBreaker::new_with_consumer("region-meta a.b".into(), SETTINGS, |register| {
        consumer = Some(register)
    });
    let bind = consumer.unwrap();
    let unlabeled = metrics::Metrics::new(Default::default()).unwrap();
    bind(&unlabeled);
    cb.execute::<()>(|| (NO, Err(Error::StringError("ordinary".into()))))
        .unwrap_err();
    cb.execute(|| (YES, Ok(()))).unwrap();
    assert_eq!(counts(&cb), [1., 1., 1., 0.]);
    let labels = std::collections::HashMap::from([("cluster".into(), "new".into())]);
    let labeled = metrics::Metrics::new(labels).unwrap();
    bind(&labeled);
    assert_eq!(counts(&cb), [0.; 4]);
    succeeds(&cb);
    let family = labeled.circuit_breaker_counters.collect().pop().unwrap();
    for metric in family.get_metric() {
        assert!(metric
            .get_label()
            .iter()
            .any(|l| l.get_name() == "name" && l.get_value() == "region_meta_a.b"));
        assert!(metric
            .get_label()
            .iter()
            .any(|l| l.get_name() == "cluster" && l.get_value() == "new"));
    }
    assert_eq!(
        unlabeled
            .circuit_breaker_counters
            .with_label_values(&["region_meta_a.b", "error"])
            .get(),
        1.
    );
}

#[test]
fn contexts_preserve_nil_wrong_type_and_shared_identity() {
    let cb = breaker(SETTINGS);
    let context = TraceContext::new();
    assert!(from_context(None).is_none());
    assert!(from_context(Some(&context)).is_none());
    let wrong = context.with_value::<CircuitBreakerKey, _>(42);
    assert!(from_context(Some(&wrong)).is_none());
    let derived = with_circuit_breaker(&context, Some(cb.clone()));
    assert!(from_context(Some(&context)).is_none());
    assert!(Arc::ptr_eq(&cb, &from_context(Some(&derived)).unwrap()));
    let direct = context.with_value::<CircuitBreakerKey, _>(cb.clone());
    assert!(Arc::ptr_eq(&cb, &from_context(Some(&direct)).unwrap()));
    let cleared = with_circuit_breaker(&derived, None);
    assert!(from_context(Some(&cleared)).is_none());
    assert_eq!(StateType::CLOSED.to_string(), "closed");
    assert_eq!(StateType::OPEN.to_string(), "open");
    assert_eq!(StateType::HALF_OPEN.to_string(), "half-open");
    assert_eq!(StateType(-42).to_string(), "unknown state: -42");
    assert_eq!(Settings::default().half_open_success_count, 0);
}

#[test]
fn signed_windows_and_cooldowns_preserve_source_duration_domain() {
    // Source oracle covers float Seconds conversion before uint32 wrap. Set
    // observations directly so an already-expired window cannot reset them.
    for (window, qps, expected) in [
        (2_000_000_000, 1 << 31, StateType::OPEN),
        (-1_000_000_000, 1, StateType::CLOSED),
        (-500_000_000, 1, StateType::OPEN),
        (i64::MIN, 0, StateType::OPEN),
        (i64::MAX, 0, StateType::CLOSED),
    ] {
        let cb = breaker(Settings {
            error_rate_window: window,
            min_qps_for_open: qps,
            cool_down_interval: i64::MIN,
            ..SETTINGS
        });
        state(&cb).lock().unwrap().failure_count = 1;
        if window == 2_000_000_000 {
            expire(&cb);
        }
        let result = cb.execute(|| (NO, Ok(())));
        assert_eq!(kind(&cb), expected, "window={window}");
        assert_eq!(result.is_err(), expected == StateType::OPEN);
        if expected == StateType::OPEN {
            succeeds(&cb); // Negative cooldown has already expired.
            assert_eq!(kind(&cb), StateType::HALF_OPEN);
        }
    }
    let s = State::new(
        Instant::now(),
        StateType::CLOSED,
        &Settings {
            error_rate_window: -1,
            ..SETTINGS
        },
    );
    assert!(s.expired(s.started));
    assert!(!s.expired(s.started - Duration::from_nanos(1)));
    let mut s = s;
    s.interval = i64::MAX;
    assert!(!s.expired(s.started));
    s.interval = i64::MIN;
    assert!(s.expired(s.started));
}

#[test]
fn source_open_breaker_error_retains_pd_code() {
    let cb = breaker(SETTINGS);
    drive(&cb, MIN_COUNT, YES);
    expire(&cb);
    let error = cb
        .execute::<()>(|| panic!("open breaker must not call RPC"))
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "[PD:client:ErrCircuitBreakerOpen]circuit breaker is open"
    );
}
