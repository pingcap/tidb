// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

use super::*;
use std::future::{pending, ready};
use std::sync::atomic::AtomicUsize;
use std::sync::Mutex as StdMutex;

fn error() -> Error {
    Error::StringError("test".to_owned())
}
fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}
fn reset(bo: &Backoffer) {
    assert_eq!(bo.next, bo.base);
    assert_eq!(bo.current_total, Duration::ZERO);
    assert_eq!(bo.attempt, 0);
    assert_eq!(bo.next_log_time, Duration::ZERO);
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
    assert_eq!(start.elapsed(), ms(1000));
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
    let mut bo = Backoffer::new(ms(10), ms(100), ms(1000)).with_min_log_interval(ms(100));
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
                tokio::time::sleep(ms(15)).await;
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
        tokio::time::sleep(ms(100)).await;
        Err(error())
    })
    .await
    .unwrap_err();
    assert_eq!(count.load(Ordering::SeqCst), 2);
    assert_eq!(start.elapsed(), ms(225));
    reset(&bo);
}

#[tokio::test(start_paused = true)]
async fn zero_total_retries_until_success_and_saturates_intervals() {
    let mut bo = Backoffer::new(ms(10), ms(20), Duration::ZERO);
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
    assert_eq!(start.elapsed(), ms(90));
    reset(&bo);
    let mut bo = Backoffer::new(Duration::MAX, Duration::MAX, Duration::ZERO);
    assert_eq!(bo.next_interval(), Duration::MAX);
    assert_eq!(bo.next_interval(), Duration::MAX);
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
    assert_eq!(start.elapsed(), ms(30));
    retry(pending(), 0, ms(10), || async { panic!("zero attempts") })
        .await
        .unwrap();
    retry(pending(), 1, ms(10), || ready(Ok(()))).await.unwrap();
    assert_eq!(start.elapsed(), ms(30));
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
                tokio::time::sleep(ms(operation)).await;
            }
            Err(error())
        })
        .await
        .unwrap_err();
        assert_eq!(start.elapsed(), ms(expected), "interval={interval}ms");
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
    assert_eq!(start.elapsed(), ms(5000));
}

#[tokio::test]
#[should_panic]
async fn fixed_interval_rejects_zero_interval_even_without_attempts() {
    retry(pending(), 0, Duration::ZERO, || ready(Ok(())))
        .await
        .unwrap();
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
