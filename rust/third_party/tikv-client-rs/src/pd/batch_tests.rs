// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Mutex;

use super::*;

const TEST_MAX: usize = 20;

#[test]
fn source_go_batch_test_adjust_best_batch_size() {
    let mut controller = Controller::new(TEST_MAX, None, None);
    assert_eq!(controller.best_batch_size, 8);
    controller.adjust_best_batch_size();
    assert_eq!(controller.best_batch_size, 7);
    controller.finish_collected_requests(None, None);
    for i in 0..10 {
        controller.push_request(i);
    }
    controller.adjust_best_batch_size();
    assert_eq!(controller.best_batch_size, 7);
    controller.finish_collected_requests(None, None);
    for i in 0..15 {
        controller.push_request(i);
    }
    controller.adjust_best_batch_size();
    assert_eq!(controller.best_batch_size, 8);
    controller.finish_collected_requests(None, None);
}

#[test]
fn source_go_batch_test_finish_collected_requests() {
    #[derive(Default)]
    struct Request {
        index: usize,
        cancelled: bool,
    }
    let mut controller = Controller::new(TEST_MAX, None, None);
    assert_eq!(controller.get_collected_request_count(), 0);
    controller.finish_collected_requests(None, None);
    assert_eq!(controller.get_collected_request_count(), 0);
    let requests = (0..10)
        .map(|_| Arc::new(Mutex::new(Request::default())))
        .collect::<Vec<_>>();
    for request in &requests {
        controller.push_request(request.clone());
    }
    assert_eq!(controller.get_collected_request_count(), 10);
    controller.finish_collected_requests(None, None);
    assert_eq!(controller.get_collected_request_count(), 0);
    let requests = (0..10)
        .map(|_| Arc::new(Mutex::new(Request::default())))
        .collect::<Vec<_>>();
    for request in &requests {
        controller.push_request(request.clone());
    }
    controller.finish_collected_requests(
        Some(&mut |index, request, error| {
            let mut request = request.lock().unwrap();
            request.index = index;
            request.cancelled = matches!(error, Some(Error::ContextCanceled));
        }),
        Some(&Error::ContextCanceled),
    );
    assert_eq!(controller.get_collected_request_count(), 0);
    for (index, request) in requests.iter().enumerate() {
        let request = request.lock().unwrap();
        assert_eq!(request.index, index);
        assert!(request.cancelled);
    }
}

#[tokio::test]
async fn source_go_batch_test_fetch_pending_requests() {
    let ctx = Cancellation::default();
    let mut controller = Controller::new(TEST_MAX, None, None);
    let (tx, mut rx) = mpsc::channel(TEST_MAX + 1);
    for tokens in [None, Some(Arc::new(Semaphore::new(1)))] {
        for count in [1, TEST_MAX, TEST_MAX + 1] {
            for i in 0..count {
                tx.send(i).await.unwrap();
            }
            let permit = controller
                .fetch_pending_requests(&ctx, &mut rx, tokens.as_ref(), Duration::ZERO)
                .await
                .unwrap();
            assert_eq!(
                controller.get_collected_request_count(),
                count.min(TEST_MAX)
            );
            assert_eq!(rx.len(), count.saturating_sub(TEST_MAX));
            if count > TEST_MAX {
                rx.recv().await.unwrap();
            }
            assert_eq!(permit.is_some(), tokens.is_some());
            if let Some(tokens) = &tokens {
                assert_eq!(tokens.available_permits(), 0);
            }
            drop(permit);
        }
    }
}

#[test]
fn views_iteration_finish_override_and_buffer_reuse() {
    let defaults = Arc::new(Mutex::new(Vec::new()));
    let finished = defaults.clone();
    let mut controller = Controller::new(
        TEST_MAX,
        Some(Box::new(move |index, value, error| {
            finished
                .lock()
                .unwrap()
                .push((index, value, error.is_some()));
        })),
        None,
    );
    let allocation = controller.requests.as_ptr();
    for value in 0..5 {
        controller.push_request(value);
    }
    assert_eq!(controller.get_collected_requests(), &[0, 1, 2, 3, 4]);
    let mut visited = Vec::new();
    controller.iter_collected_requests(|value| {
        visited.push(*value);
        *value < 2
    });
    assert_eq!(visited, [0, 1, 2]);
    let mut overrides = Vec::new();
    controller.finish_collected_requests(
        Some(&mut |index, value, error| {
            assert!(error.is_none());
            overrides.push((index, value));
        }),
        None,
    );
    assert_eq!(overrides, [(0, 0), (1, 1), (2, 2), (3, 3), (4, 4)]);
    assert!(defaults.lock().unwrap().is_empty());
    controller.push_request(99);
    controller.finish_collected_requests(None, Some(&Error::ContextCanceled));
    controller.finish_collected_requests(None, None);
    assert_eq!(*defaults.lock().unwrap(), [(0, 99, true)]);
    assert_eq!(controller.requests.as_ptr(), allocation);
}

#[test]
fn observer_records_before_adjustment_and_target_respects_aiad_boundaries() {
    let histogram =
        Histogram::with_opts(prometheus::HistogramOpts::new("batch_test", "batch test")).unwrap();
    let mut controller = Controller::new(TEST_MAX, None, Some(histogram.clone()));
    for _ in 0..20 {
        controller.adjust_best_batch_size();
    }
    assert_eq!(controller.best_batch_size, 1);
    assert_eq!(histogram.get_sample_count(), 20);
    assert_eq!(histogram.get_sample_sum(), 48.0); // 8..1, then twelve observations of 1.
    controller.best_batch_size = 8;
    for i in 0..12 {
        controller.push_request(i);
    }
    controller.adjust_best_batch_size();
    assert_eq!(controller.best_batch_size, 8);
    controller.push_request(12);
    controller.adjust_best_batch_size();
    assert_eq!(controller.best_batch_size, 9);
    controller.best_batch_size = TEST_MAX;
    controller.adjust_best_batch_size();
    assert_eq!(controller.best_batch_size, TEST_MAX - 1);
    controller.finish_collected_requests(None, None);
    for i in 0..TEST_MAX {
        controller.push_request(i);
    }
    controller.best_batch_size = TEST_MAX;
    controller.adjust_best_batch_size();
    assert_eq!(controller.best_batch_size, TEST_MAX);
}

#[tokio::test]
async fn token_late_full_batch_stops_receiving_and_preserves_zero_start_time() {
    let ctx = Cancellation::default();
    let (tx, mut rx) = mpsc::channel(TEST_MAX + 1);
    for i in 0..TEST_MAX + 1 {
        tx.send(i).await.unwrap();
    }
    let tokens = Arc::new(Semaphore::new(0));
    let mut controller = Controller::new(TEST_MAX, None, None);
    let mut fetching =
        Box::pin(controller.fetch_pending_requests(&ctx, &mut rx, Some(&tokens), Duration::ZERO));
    assert!(futures::poll!(fetching.as_mut()).is_pending());
    assert_eq!(tx.capacity(), TEST_MAX);
    tokens.add_permits(1);
    let permit = fetching.await.unwrap();
    assert!(permit.is_some());
    assert_eq!(controller.get_collected_request_count(), TEST_MAX);
    assert!(controller.get_extra_batching_start_time().is_none());
    assert_eq!(rx.len(), 1);
    drop(permit);
    assert_eq!(tokens.available_permits(), 1);
}

#[tokio::test]
async fn cancellation_before_first_request_returns_acquired_token() {
    let ctx = Cancellation::default();
    let (_tx, mut rx) = mpsc::channel::<usize>(1);
    let tokens = Arc::new(Semaphore::new(1));
    let mut controller = Controller::new(TEST_MAX, None, None);
    let mut fetching =
        Box::pin(controller.fetch_pending_requests(&ctx, &mut rx, Some(&tokens), Duration::ZERO));
    assert!(futures::poll!(fetching.as_mut()).is_pending());
    assert_eq!(tokens.available_permits(), 0);
    ctx.cancel();
    assert!(matches!(fetching.await, Err(Error::ContextCanceled)));
    assert_eq!(tokens.available_permits(), 1);
    assert_eq!(controller.get_collected_request_count(), 0);
}

#[tokio::test]
async fn cancellation_while_waiting_for_token_finishes_prefetched_requests_only() {
    let ctx = Cancellation::default();
    let (tx, mut rx) = mpsc::channel(4);
    for i in 0..4 {
        tx.send(i).await.unwrap();
    }
    let tokens = Arc::new(Semaphore::new(0));
    let done = Arc::new(Mutex::new(Vec::new()));
    let completed = done.clone();
    let mut controller = Controller::new(
        TEST_MAX,
        Some(Box::new(move |index, value, error| {
            assert!(matches!(error, Some(Error::ContextCanceled)));
            completed.lock().unwrap().push((index, value));
        })),
        None,
    );
    let mut fetching =
        Box::pin(controller.fetch_pending_requests(&ctx, &mut rx, Some(&tokens), Duration::ZERO));
    assert!(futures::poll!(fetching.as_mut()).is_pending());
    assert_eq!(tx.capacity(), 4);
    ctx.cancel();
    assert!(matches!(fetching.await, Err(Error::ContextCanceled)));
    assert_eq!(*done.lock().unwrap(), [(0, 0), (1, 1), (2, 2), (3, 3)]);
    assert_eq!(controller.get_collected_request_count(), 0);
    assert_eq!(tokens.available_permits(), 0);
}

#[tokio::test(start_paused = true)]
async fn dropping_extra_wait_returns_token_before_finishing_nonclone_requests() {
    let ctx = Cancellation::default();
    let (tx, mut rx) = mpsc::channel(1);
    struct NonClone(Arc<()>);
    let resource = Arc::new(());
    let weak = Arc::downgrade(&resource);
    tx.send(NonClone(resource)).await.ok().unwrap();
    let tokens = Arc::new(Semaphore::new(1));
    let callback_tokens = tokens.clone();
    let done = Arc::new(Mutex::new(0));
    let completed = done.clone();
    let mut controller = Controller::new(
        TEST_MAX,
        Some(Box::new(move |index, request: NonClone, error| {
            assert_eq!(index, 0);
            assert!(matches!(error, Some(Error::ContextCanceled)));
            assert_eq!(callback_tokens.available_permits(), 1);
            assert_eq!(Arc::strong_count(&request.0), 1);
            *completed.lock().unwrap() += 1;
        })),
        None,
    );
    let mut fetching = Box::pin(controller.fetch_pending_requests(
        &ctx,
        &mut rx,
        Some(&tokens),
        Duration::from_secs(1),
    ));
    assert!(futures::poll!(fetching.as_mut()).is_pending());
    drop(fetching);
    assert_eq!(tokens.available_permits(), 1);
    assert_eq!(*done.lock().unwrap(), 1);
    assert!(weak.upgrade().is_none());
    assert_eq!(controller.get_collected_request_count(), 0);
}

#[tokio::test(start_paused = true)]
async fn optional_extra_wait_uses_one_deadline_and_stops_at_the_target() {
    let ctx = Cancellation::default();
    let (tx, mut rx) = mpsc::channel(TEST_MAX);
    tx.send(0).await.unwrap();
    let mut controller = Controller::new(TEST_MAX, None, None);
    let started = Instant::now();
    controller
        .fetch_pending_requests(&ctx, &mut rx, None, Duration::from_millis(10))
        .await
        .unwrap();
    assert_eq!(Instant::now() - started, Duration::from_millis(10));
    assert_eq!(controller.get_extra_batching_start_time(), Some(started));
    assert_eq!(controller.get_collected_requests(), &[0]);
    tx.send(0).await.unwrap();
    let mut fetching =
        Box::pin(controller.fetch_pending_requests(&ctx, &mut rx, None, Duration::from_secs(1)));
    assert!(futures::poll!(fetching.as_mut()).is_pending());
    for i in 1..12 {
        tx.send(i).await.unwrap();
    }
    fetching.await.unwrap();
    assert_eq!(controller.get_collected_request_count(), 12); // final drain exceeds best size.
    assert_eq!(Instant::now() - started, Duration::from_millis(10));
}

#[tokio::test(start_paused = true)]
async fn timer_only_fetch_keeps_requests_on_cancel_and_drains_after_expiry() {
    let ctx = Cancellation::default();
    let (tx, mut rx) = mpsc::channel(TEST_MAX);
    let mut controller = Controller::new(TEST_MAX, None, None);
    controller.push_request(1);
    let timer = tokio::time::sleep(Duration::from_secs(1));
    tokio::pin!(timer);
    let mut fetching =
        Box::pin(controller.fetch_requests_with_timer(&ctx, &mut rx, timer.as_mut()));
    assert!(futures::poll!(fetching.as_mut()).is_pending());
    ctx.cancel();
    assert!(matches!(fetching.await, Err(Error::ContextCanceled)));
    assert_eq!(controller.get_collected_requests(), &[1]);
    let ctx = Cancellation::default();
    let timer = tokio::time::sleep(Duration::ZERO);
    tokio::pin!(timer);
    for i in 2..10 {
        tx.send(i).await.unwrap();
    }
    controller
        .fetch_requests_with_timer(&ctx, &mut rx, timer.as_mut())
        .await
        .unwrap();
    assert_eq!(
        controller.get_collected_requests(),
        &(1..10).collect::<Vec<_>>()
    );
    assert!(rx.is_empty());
    assert!(controller.get_extra_batching_start_time().is_none());
}

#[tokio::test(start_paused = true)]
async fn timer_only_fetch_reaches_max_before_timer_and_leaves_excess_queued() {
    let ctx = Cancellation::default();
    let (tx, mut rx) = mpsc::channel(TEST_MAX + 1);
    let mut controller = Controller::new(TEST_MAX, None, None);
    for i in 0..TEST_MAX + 1 {
        tx.send(i).await.unwrap();
    }
    let timer = tokio::time::sleep(Duration::from_secs(1));
    tokio::pin!(timer);
    let start = Instant::now();
    controller
        .fetch_requests_with_timer(&ctx, &mut rx, timer.as_mut())
        .await
        .unwrap();
    assert_eq!(Instant::now(), start);
    assert_eq!(controller.get_collected_request_count(), TEST_MAX);
    assert_eq!(rx.len(), 1);
}

#[tokio::test]
async fn closed_native_channel_finishes_collected_work_and_returns_token() {
    let ctx = Cancellation::default();
    let (tx, mut rx) = mpsc::channel(1);
    tx.send(7).await.unwrap();
    drop(tx);
    let tokens = Arc::new(Semaphore::new(1));
    let done = Arc::new(Mutex::new(Vec::new()));
    let completed = done.clone();
    let mut controller = Controller::new(
        TEST_MAX,
        Some(Box::new(move |index, value, error| {
            assert!(error.is_some());
            completed.lock().unwrap().push((index, value));
        })),
        None,
    );
    assert!(controller
        .fetch_pending_requests(&ctx, &mut rx, Some(&tokens), Duration::ZERO)
        .await
        .is_err());
    assert_eq!(*done.lock().unwrap(), [(0, 7)]);
    assert_eq!(tokens.available_permits(), 1);
    assert_eq!(controller.get_collected_request_count(), 0);
}

#[tokio::test]
async fn zero_max_matches_the_source_empty_early_return() {
    let ctx = Cancellation::default();
    let (_tx, mut rx) = mpsc::channel::<u8>(1);
    let mut controller = Controller::new(0, None, None);
    assert!(controller
        .fetch_pending_requests(&ctx, &mut rx, None, Duration::ZERO)
        .await
        .unwrap()
        .is_none());
    assert!(controller.get_collected_requests().is_empty());
    assert!(controller.get_extra_batching_start_time().is_none());
}
