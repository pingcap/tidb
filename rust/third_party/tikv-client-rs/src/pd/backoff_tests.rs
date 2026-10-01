// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

use super::*;
use std::future::{pending, ready};
use std::sync::atomic::AtomicUsize;
use std::sync::Mutex as StdMutex;

fn error() -> Error {
    Error::StringError("test".to_owned())
}
fn ms(n: i64) -> i64 {
    n * 1_000_000
}
fn elapsed(start: Instant) -> i64 {
    start.elapsed().as_nanos() as i64
}
async fn sleep(nanos: i64) {
    tokio::time::sleep(Duration::from_nanos(nanos as u64)).await;
}
fn reset(bo: &Backoffer) {
    assert_eq!(bo.next, bo.base);
    assert_eq!(bo.current_total, 0);
    assert_eq!(bo.attempt, 0);
    assert_eq!(bo.next_log_time, 0);
}

#[tokio::test(start_paused = true)]
async fn original_test_backoffer() {
    let bo = Backoffer::new(ms(1000), ms(100), ms(1));
    assert_eq!((bo.base, bo.max, bo.total), (ms(100), ms(100), ms(100)));
    let mut bo = Backoffer::new(ms(100), ms(1000), ms(100));
    assert_eq!((bo.base, bo.total), (ms(100), ms(100)));
    let count = AtomicUsize::new(0);
    let err = bo
        .exec(pending(), || {
            count.fetch_add(1, Ordering::SeqCst);
            ready(Err(error()))
        })
        .await
        .unwrap_err();
    assert_eq!(err.to_string(), "test");
    assert_eq!(count.load(Ordering::SeqCst), 1);
    reset(&bo);
    let mut bo = Backoffer::new(ms(100), ms(1000), ms(1000));
    assert_eq!(bo.next_interval(), ms(100));
    assert_eq!(bo.next_interval(), ms(200));
    for _ in 0..10 {
        assert!(bo.next_interval() <= ms(1000));
    }
    assert_eq!(bo.next_interval(), ms(1000));
    bo.reset();
    reset(&bo);
    count.store(0, Ordering::SeqCst);
    let start = Instant::now();
    let err = bo
        .exec(pending(), || {
            count.fetch_add(1, Ordering::SeqCst);
            ready(Err(error()))
        })
        .await
        .unwrap_err();
    assert_eq!(elapsed(start), ms(1000));
    assert_eq!(err.to_string(), "test");
    assert_eq!(count.load(Ordering::SeqCst), 4);
    reset(&bo);
    count.store(0, Ordering::SeqCst);
    let err = bo
        .exec(pending(), || {
            let n = count.fetch_add(1, Ordering::SeqCst) + 1;
            ready(Err(Error::StringError(format!("test {n}"))))
        })
        .await
        .unwrap_err();
    assert_eq!(err.to_string(), "test 4");
    assert_eq!(count.load(Ordering::SeqCst), 4);
    reset(&bo);
    count.store(0, Ordering::SeqCst);
    bo.exec(pending(), || {
        if count.load(Ordering::SeqCst) == 1 {
            ready(Ok(()))
        } else {
            count.fetch_add(1, Ordering::SeqCst);
            ready(Err(error()))
        }
    })
    .await
    .unwrap();
    assert_eq!(count.load(Ordering::SeqCst), 1);
    reset(&bo);
}

#[tokio::test(start_paused = true)]
async fn original_checker_overwrite_cases() {
    let count = Arc::new(AtomicUsize::new(0));
    let checker = |limit| {
        let count = count.clone();
        Some(Box::new(move |_: &Error| count.load(Ordering::SeqCst) < limit) as RetryableChecker)
    };
    let mut bo = Backoffer::new(ms(100), ms(1000), ms(1000));
    for (limit, overwrite, expected) in [(2, false, 2), (4, false, 2), (4, true, 4)] {
        count.store(0, Ordering::SeqCst);
        bo.set_retryable_checker(checker(limit), overwrite);
        assert_eq!(
            bo.exec(pending(), || {
                count.fetch_add(1, Ordering::SeqCst);
                ready(Err(error()))
            })
            .await
            .unwrap_err()
            .to_string(),
            "test"
        );
        assert_eq!(count.load(Ordering::SeqCst), expected);
        reset(&bo);
    }
    bo.set_retryable_checker(None, true);
    assert!(bo.retryable_checker.is_none());
}

#[derive(Default)]
struct Capture(StdMutex<Vec<String>>);
impl Log for Capture {
    fn enabled(&self, _: &Metadata<'_>) -> bool {
        true
    }
    fn log(&self, record: &Record<'_>) {
        self.0.lock().unwrap().push(record.args().to_string());
    }
    fn flush(&self) {}
}
fn test_fn() -> impl Future<Output = Result<()>> {
    ready(Err(error()))
}

#[tokio::test(start_paused = true)]
async fn original_test_backoffer_with_log() {
    let capture = Capture::default();
    let mut bo =
        Backoffer::new_with_options(ms(10), ms(100), ms(1000), &[with_min_log_interval(ms(100))]);
    for run in 1..=2 {
        assert_eq!(
            bo.exec_with_logger(pending(), test_fn, &capture)
                .await
                .unwrap_err()
                .to_string(),
            "test"
        );
        let messages = capture.0.lock().unwrap();
        assert_eq!(messages.len(), run * 10);
        assert!(messages[(run - 1) * 10].contains("[fn-name=test_fn] [retry-time=4] [error=test]"));
        assert!(messages
            .last()
            .unwrap()
            .contains("[fn-name=test_fn] [retry-time=13] [error=test]"));
        reset(&bo);
    }
}

#[tokio::test(start_paused = true)]
async fn backoff_preserves_context_error_but_runs_once_before_cancellation() {
    let mut bo = Backoffer::new(ms(10), ms(100), ms(1000));
    let count = AtomicUsize::new(0);
    let result = bo
        .exec(ready(Error::ContextCanceled), || {
            count.fetch_add(1, Ordering::SeqCst);
            ready(Err(error()))
        })
        .await;
    assert!(matches!(result, Err(Error::ContextCanceled)));
    assert_eq!(count.load(Ordering::SeqCst), 1);
    reset(&bo);
    bo.exec(ready(Error::ContextCanceled), || ready(Ok(())))
        .await
        .unwrap();
    bo.set_retryable_checker(Some(Box::new(|_| false)), true);
    assert_eq!(
        bo.exec(ready(Error::ContextCanceled), || ready(Err(error())))
            .await
            .unwrap_err()
            .to_string(),
        "test"
    );
    reset(&bo);
    bo.set_retryable_checker(None, true);
    let result = bo
        .exec(
            async {
                sleep(ms(15)).await;
                Error::GrpcAPI(tonic::Status::deadline_exceeded(
                    "context deadline exceeded",
                ))
            },
            || ready(Err(error())),
        )
        .await;
    assert!(
        matches!(result, Err(Error::GrpcAPI(status)) if status.code() == tonic::Code::DeadlineExceeded)
    );
    reset(&bo);
}

#[tokio::test(start_paused = true)]
async fn dropping_execution_resets_state_and_releases_operation() {
    struct DropProbe(Arc<AtomicUsize>);
    impl Drop for DropProbe {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let drops = Arc::new(AtomicUsize::new(0));
    let probe = DropProbe(drops.clone());
    let mut bo = Backoffer::new(ms(10), ms(100), ms(1000));
    {
        let operation = move || {
            let _ = &probe;
            ready(Err(error()))
        };
        let execution = bo.exec(pending(), operation);
        tokio::pin!(execution);
        assert!(futures::poll!(execution.as_mut()).is_pending());
    }
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    reset(&bo);
    bo.exec(pending(), || ready(Ok(()))).await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn operation_time_is_not_charged_to_the_wait_budget() {
    let mut bo = Backoffer::new(ms(10), ms(20), ms(25));
    let count = AtomicUsize::new(0);
    let start = Instant::now();
    bo.exec(pending(), || async {
        count.fetch_add(1, Ordering::SeqCst);
        sleep(ms(100)).await;
        Err(error())
    })
    .await
    .unwrap_err();
    assert_eq!(count.load(Ordering::SeqCst), 2);
    assert_eq!(elapsed(start), ms(225));
    reset(&bo);
}

#[tokio::test(start_paused = true)]
async fn zero_total_retries_until_success_and_caps_normal_intervals() {
    let mut bo = Backoffer::new(ms(10), ms(20), 0);
    let count = AtomicUsize::new(0);
    let start = Instant::now();
    bo.exec(pending(), || {
        ready(if count.fetch_add(1, Ordering::SeqCst) < 5 {
            Err(error())
        } else {
            Ok(())
        })
    })
    .await
    .unwrap();
    assert_eq!(elapsed(start), ms(90));
    reset(&bo);
}

#[test]
fn context_lookup_keeps_identity_and_other_values_without_mutating_parent() {
    let root = TraceContext::new().with_trace_id(b"trace".to_vec());
    assert!(from_context(None).is_none());
    assert!(from_context(Some(&root)).is_none());
    let bo = Arc::new(Mutex::new(Backoffer::new(ms(10), ms(20), ms(30))));
    let child = with_backoffer(&root, Some(bo.clone()));
    assert!(Arc::ptr_eq(&bo, &from_context(Some(&child)).unwrap()));
    assert_eq!(child.trace_id(), root.trace_id());
    assert!(from_context(Some(&with_backoffer(&child, None))).is_none());
    assert!(Arc::ptr_eq(&bo, &from_context(Some(&child)).unwrap()));
    assert!(from_context(Some(&root)).is_none());
}

#[tokio::test(start_paused = true)]
async fn fixed_interval_waits_after_last_failure_and_handles_zero_attempts() {
    let count = AtomicUsize::new(0);
    let start = Instant::now();
    let result = retry(pending(), 3, ms(10), || {
        let n = count.fetch_add(1, Ordering::SeqCst) + 1;
        ready(Err(Error::StringError(format!("test {n}"))))
    })
    .await;
    assert_eq!(result.unwrap_err().to_string(), "test 3");
    assert_eq!(elapsed(start), ms(30));
    retry(pending(), 0, ms(10), || async { panic!("zero attempts") })
        .await
        .unwrap();
    retry(pending(), 1, ms(10), || ready(Ok(()))).await.unwrap();
    assert_eq!(elapsed(start), ms(30));
}

#[tokio::test(start_paused = true)]
async fn fixed_interval_keeps_ticker_phase_after_slow_operations() {
    // Cover both ordinary service intervals and lateness below Tokio's
    // five-millisecond missed-tick threshold. Go drops missed ticks in both.
    for (interval, operation, expected) in [(100, 450, 600), (1, 3, 5)] {
        let count = AtomicUsize::new(0);
        let start = Instant::now();
        retry(pending(), 3, ms(interval), || async {
            if count.fetch_add(1, Ordering::SeqCst) == 0 {
                sleep(ms(operation)).await;
            }
            Err(error())
        })
        .await
        .unwrap_err();
        assert_eq!(elapsed(start), ms(expected), "interval={interval}ms");
    }
}

#[tokio::test(start_paused = true)]
async fn fixed_interval_cancellation_returns_last_operation_error() {
    let count = AtomicUsize::new(0);
    let result = retry(ready(Error::ContextCanceled), 10, ms(100), || {
        count.fetch_add(1, Ordering::SeqCst);
        ready(Err(Error::Unimplemented))
    })
    .await;
    assert!(matches!(result, Err(Error::Unimplemented)));
    assert_eq!(count.load(Ordering::SeqCst), 1);
    retry(ready(Error::ContextCanceled), 10, ms(100), || ready(Ok(())))
        .await
        .unwrap();
}

#[tokio::test(start_paused = true)]
async fn default_microservice_retry_is_ten_attempts_at_half_second_ticks() {
    let count = AtomicUsize::new(0);
    let start = Instant::now();
    with_config(pending(), || {
        count.fetch_add(1, Ordering::SeqCst);
        ready(Err(error()))
    })
    .await
    .unwrap_err();
    assert_eq!(count.load(Ordering::SeqCst), 10);
    assert_eq!(elapsed(start), ms(5000));
}

#[tokio::test]
#[should_panic]
async fn fixed_interval_rejects_zero_interval_even_without_attempts() {
    retry(pending(), 0, 0, || ready(Ok(()))).await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn source_failpoint_marks_a_completed_wait() {
    struct Restore;
    impl Drop for Restore {
        fn drop(&mut self) {
            fail::remove("backOffExecute");
        }
    }
    let _restore = Restore;
    BACKOFF_EXECUTED.store(false, Ordering::Relaxed);
    fail::cfg("backOffExecute", "return").unwrap();
    let mut bo = Backoffer::new(ms(1), ms(1), ms(1));
    bo.exec(pending(), || ready(Err(error())))
        .await
        .unwrap_err();
    assert!(test_backoff_execute());
}

#[test]
fn immediate_completion_does_not_construct_a_retry_timer() {
    // Go only creates a timer after a retryable failure. No Tokio timer
    // driver is needed when the operation succeeds or rejects retry.
    let mut bo = Backoffer::new(ms(10), ms(100), ms(1000));
    futures::executor::block_on(bo.exec(pending(), || ready(Ok(())))).unwrap();
    reset(&bo);
    bo.set_retryable_checker(Some(Box::new(|_| false)), true);
    assert!(matches!(
        futures::executor::block_on(bo.exec(pending(), || ready(Err(Error::Unimplemented)))),
        Err(Error::Unimplemented)
    ));
    reset(&bo);
}

#[test]
fn source_retry_duration_doubling_matches_signed_go() {
    let largest = i64::MAX;
    let mut bo = Backoffer::new(largest, largest, 0);
    let observed = [
        i128::from(bo.next_interval()),
        i128::from(bo.next_interval()),
        i128::from(bo.next_interval()),
    ];
    assert_eq!(observed, [i64::MAX as i128, -2, -4]);
}

#[test]
fn all_interval_values_match_independent_go_oracle() {
    #[derive(serde::Deserialize)]
    struct Case {
        base: i64,
        max: i64,
        total: i64,
        current_total: i64,
        normalized: [i64; 3],
        intervals: Vec<i64>,
    }
    let cases: Vec<Case> = serde_json::from_str(include_str!(
        "../../doc/pd-retry-value-oracle/intervals.json"
    ))
    .unwrap();
    for case in cases {
        let mut bo = Backoffer::new(case.base, case.max, case.total);
        assert_eq!([bo.base, bo.max, bo.total], case.normalized);
        bo.current_total = case.current_total;
        for expected in case.intervals {
            assert_eq!(bo.next_interval(), expected);
        }
    }
}

#[test]
fn copying_preserves_snapshot_and_callback_environment_without_sharing_mutable_state() {
    let calls = Arc::new(AtomicUsize::new(0));
    let captured = calls.clone();
    let mut original = Backoffer::new(ms(1), ms(10), ms(10));
    original.set_retryable_checker(
        Some(Box::new(move |_| {
            captured.fetch_add(1, Ordering::Relaxed);
            false
        })),
        true,
    );
    original.attempt = 5;
    original.next = ms(3);
    original.current_total = ms(4);
    original.next_log_time = ms(7);
    let snapshot = |bo: &Backoffer| {
        [
            bo.attempt as i64,
            bo.next,
            bo.current_total,
            bo.next_log_time,
        ]
    };
    let expected = snapshot(&original);
    let mut first = original.clone();
    let mut second = original.clone();
    assert_eq!(snapshot(&first), expected);
    assert_eq!(snapshot(&second), expected);
    assert!(Arc::ptr_eq(
        first.retryable_checker.as_ref().unwrap(),
        second.retryable_checker.as_ref().unwrap()
    ));
    original.set_retryable_checker(None, true);
    // Immediate checker rejection needs no Tokio timer, just as before.
    for copy in [&mut first, &mut second] {
        assert!(matches!(
            futures::executor::block_on(copy.exec(pending(), || ready(Err(Error::Unimplemented)))),
            Err(Error::Unimplemented)
        ));
        reset(copy);
    }
    assert_eq!(calls.load(Ordering::Relaxed), 2);
    assert_eq!(snapshot(&original), expected);
    first.set_retryable_checker(None, true);
    assert!(second.retryable_checker.is_some());
}

#[tokio::test(start_paused = true)]
async fn context_backoffer_copies_execute_concurrently_without_holding_caller_lock() {
    let checks = Arc::new(AtomicUsize::new(0));
    let observed = checks.clone();
    let mut owner = Backoffer::new(ms(10), ms(20), ms(100));
    owner.set_retryable_checker(
        Some(Box::new(move |_| {
            observed.fetch_add(1, Ordering::Relaxed);
            true
        })),
        true,
    );
    let shared = Arc::new(Mutex::new(owner));
    let context = with_backoffer(&TraceContext::new(), Some(shared.clone()));
    let mut first = from_context(Some(&context)).unwrap().lock().await.clone();
    let mut second = from_context(Some(&context)).unwrap().lock().await.clone();
    let calls1 = AtomicUsize::new(0);
    let calls2 = AtomicUsize::new(0);
    let start = Instant::now();
    let operation = |calls: &AtomicUsize| {
        // A copy must not hold the original's mutex through execution/waits.
        reset(
            &shared
                .try_lock()
                .expect("caller backoffer must stay available"),
        );
        ready(if calls.fetch_add(1, Ordering::Relaxed) < 2 {
            Err(error())
        } else {
            Ok(())
        })
    };
    let (one, two) = tokio::join!(
        first.exec(pending(), || operation(&calls1)),
        second.exec(pending(), || operation(&calls2))
    );
    one.unwrap();
    two.unwrap();
    assert_eq!(elapsed(start), ms(30));
    assert_eq!(checks.load(Ordering::Relaxed), 4);
    reset(&first);
    reset(&second);
    reset(&*shared.lock().await);
}

#[tokio::test(start_paused = true)]
async fn signed_unbounded_totals_and_nonpositive_timers_keep_source_semantics() {
    for total in [0, -1, i64::MIN] {
        for base in [0, -1, i64::MIN] {
            let mut bo = Backoffer::new(base, 100, total);
            let calls = AtomicUsize::new(0);
            let start = Instant::now();
            bo.exec(pending(), || {
                ready(if calls.fetch_add(1, Ordering::Relaxed) < 3 {
                    Err(error())
                } else {
                    Ok(())
                })
            })
            .await
            .unwrap();
            assert_eq!(calls.load(Ordering::Relaxed), 4);
            assert_eq!(
                elapsed(start),
                0,
                "non-positive waits are immediately eligible"
            );
            reset(&bo);
        }
    }
    let mut bo = Backoffer::new(i64::MAX, i64::MAX, 0);
    {
        let mut execution = Box::pin(bo.exec(pending(), || ready(Err(error()))));
        assert!(futures::poll!(&mut execution).is_pending());
    }
    reset(&bo);
}

#[tokio::test(start_paused = true)]
async fn signed_fixed_retry_counts_and_invalid_tickers_match_go() {
    for count in [-1, isize::MIN, 0] {
        retry(pending(), count, ms(1), || async {
            panic!("non-positive attempt count")
        })
        .await
        .unwrap();
    }
    for interval in [0, -1, i64::MIN] {
        use futures::FutureExt;
        let result = std::panic::AssertUnwindSafe(retry(pending(), -1, interval, || ready(Ok(()))))
            .catch_unwind()
            .await;
        assert!(
            result.is_err(),
            "Go creates the ticker before examining count"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn signed_attempts_and_log_accumulators_wrap_and_reset() {
    let capture = Capture::default();
    let mut bo = Backoffer::new_with_options(1, 1, 1, &[with_min_log_interval(1)]);
    bo.attempt = isize::MAX;
    bo.exec_with_logger(
        ready(Error::ContextCanceled),
        || ready(Err(error())),
        &capture,
    )
    .await
    .unwrap_err();
    assert!(capture.0.lock().unwrap()[0].contains(&format!("[retry-time={}]", isize::MIN)));
    reset(&bo);
    capture.0.lock().unwrap().clear();
    bo.next_log_time = i64::MAX;
    bo.exec_with_logger(
        ready(Error::ContextCanceled),
        || ready(Err(error())),
        &capture,
    )
    .await
    .unwrap_err();
    assert!(
        capture.0.lock().unwrap().is_empty(),
        "wrapped negative log time does not meet positive cadence"
    );
    reset(&bo);
    for interval in [0, -1, i64::MIN] {
        let mut bo = Backoffer::new_with_options(1, 1, 1, &[with_min_log_interval(interval)]);
        bo.exec_with_logger(
            ready(Error::ContextCanceled),
            || ready(Err(error())),
            &capture,
        )
        .await
        .unwrap_err();
        assert!(capture.0.lock().unwrap().is_empty());
        reset(&bo);
    }
}

#[test]
fn reusable_constructor_options_run_after_normalization_in_source_order() {
    let calls = Arc::new(AtomicUsize::new(0));
    let captured = calls.clone();
    let first: BackofferOption = Arc::new(move |bo| {
        assert_eq!([bo.base, bo.max, bo.total], [10, 10, 10]);
        assert_eq!(bo.log_interval, 0);
        captured.fetch_add(1, Ordering::Relaxed);
        bo.set_retryable_checker(Some(Box::new(|_| false)), true);
    });
    let second: BackofferOption = Arc::new(|bo| {
        assert!(!bo.retryable_checker.as_ref().unwrap()(&error()));
        bo.set_retryable_checker(Some(Box::new(|_| true)), false);
        assert!(!bo.retryable_checker.as_ref().unwrap()(&error()));
    });
    let options = [
        first,
        with_min_log_interval(7),
        with_min_log_interval(11),
        second,
    ];
    for _ in 0..2 {
        let bo = Backoffer::new_with_options(100, 10, 1, &options);
        assert_eq!(bo.log_interval, 11);
        assert!(!bo.retryable_checker.as_ref().unwrap()(&error()));
        reset(&bo);
    }
    assert_eq!(calls.load(Ordering::Relaxed), 2);
}
