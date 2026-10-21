// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Go `limiter_test.go`.

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serial_test::serial;
use tokio::sync::mpsc;

use super::context::Context;
use super::limiter::{
    wait_reservations, FillRate, Limiter, NotifyMsg, Reservation, TokenBucketReconfigureArgs,
    INF_DURATION,
};

/// One unit of the test clock.
const D: Duration = Duration::from_secs(1);

/// Go's `t0 .. t8`, multiples of `D` after `t0`.
#[derive(Clone, Copy)]
struct Times {
    t: [Instant; 9],
}

impl Times {
    /// Go `resetTime`.
    fn reset() -> Self {
        let t0 = Instant::now();
        let mut t = [t0; 9];
        for (i, slot) in t.iter_mut().enumerate() {
            *slot = t0 + D * i as u32;
        }
        Times { t }
    }

    fn t(&self, i: usize) -> Instant {
        self.t[i]
    }

    /// Go `dSince`: multiples of `D` since `t0`, rounded to the nearest.
    fn d_since(&self, t: Instant) -> i64 {
        let since = t.saturating_duration_since(self.t[0]);
        ((since + D / 2).as_nanos() / D.as_nanos()) as i64
    }
}

struct Request {
    t: Instant,
    n: f64,
    act: Instant,
    ok: bool,
}

fn notify_channel() -> (mpsc::Sender<NotifyMsg>, mpsc::Receiver<NotifyMsg>) {
    mpsc::channel(1)
}

fn new_limiter(
    now: Instant,
    r: FillRate,
    b: i64,
    tokens: f64,
    chan: &mpsc::Sender<NotifyMsg>,
) -> Arc<Limiter> {
    Limiter::new_limiter(now, r, b, tokens, Some(chan.clone()))
}

fn run_reserve_max(times: &Times, lim: &Arc<Limiter>, req: Request) -> Reservation {
    run_reserve(times, lim, req, INF_DURATION)
}

fn run_reserve(
    times: &Times,
    lim: &Arc<Limiter>,
    req: Request,
    max_reserve: Duration,
) -> Reservation {
    let r = lim.reserve_n(req.t, req.n, max_reserve);
    let act = r.time_to_act.map(|t| times.d_since(t)).unwrap_or(0);
    if (r.reserved && act != times.d_since(req.act)) || r.reserved != req.ok {
        panic!(
            "lim.reserve_n(t{}, {}, {:?}) = (t{}, {}) want (t{}, {})",
            times.d_since(req.t),
            req.n,
            max_reserve,
            act,
            r.reserved,
            times.d_since(req.act),
            req.ok
        );
    }
    r
}

fn check_tokens(lim: &Limiter, t: Instant, expected: f64) {
    let available = lim.available_tokens(t);
    assert!(
        (expected - available).abs() <= 1e-7,
        "expected {expected} tokens, got {available}"
    );
}

#[test]
#[serial]
#[allow(non_snake_case)]
fn source_go_resource_group_controller_limiter_test_TestSimpleReserve() {
    let times = Times::reset();
    let t = |i| times.t(i);
    let (nc, _rx) = notify_channel();
    let lim = new_limiter(t(0), 1.0, 0, 2.0, &nc);

    run_reserve_max(
        &times,
        &lim,
        Request {
            t: t(0),
            n: 3.0,
            act: t(1),
            ok: true,
        },
    );
    run_reserve_max(
        &times,
        &lim,
        Request {
            t: t(0),
            n: 3.0,
            act: t(4),
            ok: true,
        },
    );
    run_reserve_max(
        &times,
        &lim,
        Request {
            t: t(3),
            n: 2.0,
            act: t(6),
            ok: true,
        },
    );

    run_reserve(
        &times,
        &lim,
        Request {
            t: t(3),
            n: 2.0,
            act: t(7),
            ok: false,
        },
        D * 4,
    );
    run_reserve(
        &times,
        &lim,
        Request {
            t: t(5),
            n: 2000.0,
            act: t(6),
            ok: false,
        },
        D * 100,
    );

    run_reserve(
        &times,
        &lim,
        Request {
            t: t(3),
            n: 2.0,
            act: t(8),
            ok: true,
        },
        D * 8,
    );
    // Unlimited.
    let args = TokenBucketReconfigureArgs {
        new_burst: -1,
        ..TokenBucketReconfigureArgs::default()
    };
    lim.reconfigure(t(1), args, &[]);
    run_reserve_max(
        &times,
        &lim,
        Request {
            t: t(5),
            n: 2000.0,
            act: t(5),
            ok: true,
        },
    );
}

#[test]
#[serial]
#[allow(non_snake_case)]
fn source_go_resource_group_controller_limiter_test_TestReconfig() {
    let times = Times::reset();
    let t = |i| times.t(i);
    let (nc, _rx) = notify_channel();
    let lim = new_limiter(t(0), 1.0, 0, 2.0, &nc);

    run_reserve_max(
        &times,
        &lim,
        Request {
            t: t(0),
            n: 4.0,
            act: t(2),
            ok: true,
        },
    );
    let args = TokenBucketReconfigureArgs {
        new_tokens: 6.0,
        new_fill_rate: 2.0,
        ..TokenBucketReconfigureArgs::default()
    };
    lim.reconfigure(t(1), args, &[]);
    check_tokens(&lim, t(1), 5.0);
    check_tokens(&lim, t(2), 7.0);

    let args = TokenBucketReconfigureArgs {
        new_tokens: 6.0,
        new_fill_rate: 2.0,
        new_burst: -1,
        ..TokenBucketReconfigureArgs::default()
    };
    lim.reconfigure(t(1), args, &[]);
    check_tokens(&lim, t(1), 6.0);
    check_tokens(&lim, t(2), 6.0);
    assert_eq!(lim.get_burst(), -1);
}

/// A mutator's `now` is taken before it locks the limiter, so a concurrent
/// sibling may already have advanced `last` past it. The stale `now` must not
/// rewind the limiter's clock. In each case the shared reserve plays that
/// sibling, advancing `last` before the mutator runs with an earlier time.
#[test]
#[serial]
#[allow(non_snake_case)]
fn source_go_resource_group_controller_limiter_test_TestLastStaysMonotonicOnStaleNow() {
    fn mutate(name: &str, lim: &Limiter, stale_now: Instant) {
        match name {
            "remove tokens" => lim.remove_tokens(stale_now, 0.0),
            "refund tokens" => lim.refund_tokens(stale_now, 0.0),
            // CancelAt is covered by TestAcquireTokensCancelKeepsLastMonotonic.
            "reconfigure" => lim.reconfigure(
                stale_now,
                TokenBucketReconfigureArgs {
                    new_tokens: 10.0,
                    new_fill_rate: 100.0,
                    ..TokenBucketReconfigureArgs::default()
                },
                &[],
            ),
            "reconfigure unlimited" => lim.reconfigure(
                stale_now,
                TokenBucketReconfigureArgs {
                    new_tokens: 10.0,
                    new_fill_rate: 100.0,
                    new_burst: -1,
                    ..TokenBucketReconfigureArgs::default()
                },
                &[],
            ),
            _ => unreachable!(),
        }
    }
    let tests = [
        ("remove tokens", 1090.0, true),
        ("refund tokens", 1090.0, true),
        ("reconfigure", 1100.0, true),
        ("reconfigure unlimited", 0.0, false),
    ];

    for (name, expected_pool, check_pool) in tests {
        let times = Times::reset();
        let t0 = times.t(0);
        let (nc, _rx) = notify_channel();
        // Fill rate 100/s, burst 0 (no clamp).
        let lim = new_limiter(t0, 100.0, 0, 0.0, &nc);

        let advanced_at = t0 + Duration::from_secs(10);
        let stale_now = t0 + Duration::from_secs(9);
        let check_at = t0 + Duration::from_secs(11);

        // Advance `last` through the public API to t0+10s, leaving a pool of
        // 100*10 - 10 = 990.
        let r = lim.reserve(&Context::background(), INF_DURATION, advanced_at, 10.0);
        assert!(r.reserved, "{name}");
        let (_, pool) = lim.get_tokens(advanced_at);
        assert!((990.0 - pool).abs() <= 1e-6, "{name}");

        mutate(name, &lim, stale_now);
        assert_eq!(advanced_at, lim.last(), "{name}");

        if check_pool {
            // Accrual over t0+10s..t0+11s is 100; a rewound `last` (t0+9s)
            // would over-accrue another 100 tokens.
            let (_, pool) = lim.get_tokens(check_at);
            assert!((expected_pool - pool).abs() <= 1e-6, "{name}: {pool}");
        }
    }
}

#[test]
#[serial]
#[allow(non_snake_case)]
fn source_go_resource_group_controller_limiter_test_TestRefundTokens() {
    let t0 = Instant::now();
    let (nc, _rx) = notify_channel();
    let lim = new_limiter(t0, 0.0, 0, 100.0, &nc);

    // Consume some tokens.
    lim.remove_tokens(t0, 30.0);
    check_tokens(&lim, t0, 70.0);

    // Refund part of them.
    lim.refund_tokens(t0, 20.0);
    check_tokens(&lim, t0, 90.0);

    // Refund beyond the initial amount: allowed, no burst cap at burst 0.
    lim.refund_tokens(t0, 50.0);
    check_tokens(&lim, t0, 140.0);

    // With burst > 0, a refund up to the burst stays.
    let lim_burst = new_limiter(t0, 0.0, 100, 50.0, &nc);
    lim_burst.remove_tokens(t0, 30.0);
    check_tokens(&lim_burst, t0, 20.0);
    lim_burst.refund_tokens(t0, 80.0);
    check_tokens(&lim_burst, t0, 100.0);

    // A refund beyond the burst is capped by get_tokens.
    lim_burst.refund_tokens(t0, 50.0);
    check_tokens(&lim_burst, t0, 100.0);

    // Burstable (burst < 0): refund is a no-op.
    let lim_unlimited = new_limiter(t0, 0.0, -1, 100.0, &nc);
    lim_unlimited.refund_tokens(t0, 50.0);
    check_tokens(&lim_unlimited, t0, 100.0);
}

#[test]
#[serial]
#[allow(non_snake_case)]
fn source_go_resource_group_controller_limiter_test_TestNotify() {
    let times = Times::reset();
    let (nc, mut rx) = notify_channel();
    let lim = new_limiter(times.t(0), 1.0, 0, 0.0, &nc);

    let args = TokenBucketReconfigureArgs {
        new_tokens: 1000.0,
        new_fill_rate: 2.0,
        new_notify_threshold: 400.0,
        ..TokenBucketReconfigureArgs::default()
    };
    lim.reconfigure(times.t(1), args, &[]);
    run_reserve_max(
        &times,
        &lim,
        Request {
            t: times.t(2),
            n: 1000.0,
            act: times.t(2),
            ok: true,
        },
    );
    assert!(rx.try_recv().is_ok(), "no notify");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
#[allow(non_snake_case)]
async fn source_go_resource_group_controller_limiter_test_TestCancel() {
    let times = Times::reset();
    let t = |i| times.t(i);
    let ctx = Context::background();
    let (ctx1, cancel1) = Context::with_deadline(&ctx, t(2));
    let (nc, _rx) = notify_channel();
    let lim1 = new_limiter(t(0), 1.0, 0, 10.0, &nc);
    let lim2 = new_limiter(t(0), 1.0, 0, 0.0, &nc);

    let r1 = run_reserve_max(
        &times,
        &lim1,
        Request {
            t: t(0),
            n: 5.0,
            act: t(0),
            ok: true,
        },
    );
    check_tokens(&lim1, t(0), 5.0);
    r1.cancel_at(t(1));
    check_tokens(&lim1, t(1), 11.0);

    let r1 = lim1.reserve(&ctx, INF_DURATION, t(1), 5.0);
    let r2 = lim2.reserve(&ctx1, INF_DURATION, t(1), 5.0);
    check_tokens(&lim1, t(2), 7.0);
    check_tokens(&lim2, t(2), 2.0);
    let (d, result) = wait_reservations(&ctx, t(2), &[Some(&r1), Some(&r2)]).await;
    let error = result.unwrap_err();
    assert_eq!(d, Duration::from_secs(4));
    assert!(
        error
            .to_string()
            .contains("estimated wait time 4s, ltb state is 1.00:-4.00"),
        "{error}"
    );
    check_tokens(&lim1, t(3), 13.0);
    check_tokens(&lim2, t(3), 3.0);
    cancel1.cancel();

    let (ctx2, cancel2) = Context::with_cancel(&ctx);
    let r1 = lim1.reserve(&ctx, INF_DURATION, t(3), 5.0);
    let r2 = lim2.reserve(&ctx2, INF_DURATION, t(3), 5.0);
    check_tokens(&lim1, t(3), 8.0);
    check_tokens(&lim2, t(3), -2.0);
    let t3 = t(3);
    let waiter = tokio::spawn(async move {
        let (_, result) = wait_reservations(&ctx2, t3, &[Some(&r1), Some(&r2)]).await;
        assert!(result.is_err());
    });
    tokio::time::sleep(Duration::from_secs(1)).await;
    cancel2.cancel();
    waiter.await.unwrap();
    check_tokens(&lim1, t(5), 15.0);
    check_tokens(&lim2, t(5), 5.0);
}

#[tokio::test]
#[serial]
#[allow(non_snake_case)]
async fn source_go_resource_group_controller_limiter_test_TestCancelErrorOfReservation() {
    let t0 = Instant::now();
    let (nc, _rx) = notify_channel();
    let lim = new_limiter(t0, 10.0, 0, 10.0, &nc);
    let (ctx, cancel) = Context::with_cancel(&Context::background());
    cancel.cancel();
    let r = lim.reserve(&ctx, INF_DURATION, t0, 5.0);
    let (d, result) = wait_reservations(&Context::background(), t0, &[Some(&r)]).await;
    assert_eq!(d, Duration::ZERO);
    let error = result.unwrap_err();
    assert!(error.to_string().contains("context canceled"), "{error}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[serial]
#[allow(non_snake_case)]
async fn source_go_resource_group_controller_limiter_test_TestQPS() {
    let cases = [(1000usize, 10i64, 400000i64)];
    for (concurrency, reserve_n, ru_per_sec) in cases {
        let (qps, ru_sec, wait_time) = test_qps_case(concurrency, reserve_n, ru_per_sec).await;
        println!(
            "concurrency={concurrency},reserveN={reserve_n},limit={ru_per_sec}: QPS: {qps:.2}, RU: {ru_sec:.2}, new request need wait {wait_time:?}"
        );
        assert!((ru_per_sec as f64 - ru_sec).abs() <= 100.0 * reserve_n as f64);
        assert!((ru_per_sec as f64 / reserve_n as f64 - qps).abs() <= 100.0);
    }
}

#[test]
#[serial]
#[allow(non_snake_case)]
fn source_go_resource_group_controller_limiter_test_TestReconfiguredCh() {
    let times = Times::reset();
    let (nc, _rx) = notify_channel();
    let lim = new_limiter(times.t(0), 1.0, 0, 0.0, &nc);

    // The channel blocks initially.
    let ch = lim.get_reconfigured_ch();
    assert!(
        !ch.is_cancelled(),
        "reconfiguredCh should not be closed before Reconfigure"
    );

    // After reconfiguring, the old channel is closed.
    let args = TokenBucketReconfigureArgs {
        new_tokens: 10.0,
        new_fill_rate: 1.0,
        ..TokenBucketReconfigureArgs::default()
    };
    lim.reconfigure(times.t(1), args, &[]);
    assert!(
        ch.is_cancelled(),
        "reconfiguredCh should be closed after Reconfigure"
    );

    // A new channel is created and blocks again.
    let ch2 = lim.get_reconfigured_ch();
    assert!(!Arc::ptr_eq(&ch, &ch2));
    assert!(
        !ch2.is_cancelled(),
        "new reconfiguredCh should not be closed yet"
    );

    // Each reconfiguration closes the current channel.
    lim.reconfigure(times.t(2), args, &[]);
    assert!(
        ch2.is_cancelled(),
        "second reconfiguredCh should be closed after second Reconfigure"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
#[allow(non_snake_case)]
async fn source_go_resource_group_controller_limiter_test_TestReconfiguredChWakesMultipleWaiters() {
    let times = Times::reset();
    let (nc, _rx) = notify_channel();
    let lim = new_limiter(times.t(0), 1.0, 0, 0.0, &nc);

    const NUM_WAITERS: usize = 5;
    let ch = lim.get_reconfigured_ch();
    let (woken_up, mut woken) = mpsc::channel(NUM_WAITERS);

    let mut waiters = Vec::new();
    for _ in 0..NUM_WAITERS {
        let ch = ch.clone();
        let woken_up = woken_up.clone();
        waiters.push(tokio::spawn(async move {
            tokio::select! {
                _ = ch.cancelled() => {
                    woken_up.try_send(()).unwrap();
                }
                _ = tokio::time::sleep(Duration::from_secs(2)) => {}
            }
        }));
    }

    // Close by reconfiguring.
    lim.reconfigure(
        times.t(1),
        TokenBucketReconfigureArgs {
            new_tokens: 10.0,
            new_fill_rate: 1.0,
            ..TokenBucketReconfigureArgs::default()
        },
        &[],
    );
    for waiter in waiters {
        waiter.await.unwrap();
    }
    drop(woken_up);
    let mut count = 0;
    while woken.try_recv().is_ok() {
        count += 1;
    }
    assert_eq!(count, NUM_WAITERS);
}

const TEST_CASE_RUN_TIME: Duration = Duration::from_secs(4);

async fn test_qps_case(concurrency: usize, reserve_n: i64, limit: i64) -> (f64, f64, Duration) {
    let (nc, _rx) = notify_channel();
    let lim = new_limiter(Instant::now(), limit as f64, limit, limit as f64, &nc);
    let (ctx, cancel) = Context::with_cancel(&Context::background());

    let total_requests = Arc::new(AtomicI64::new(0));
    let start = Instant::now();

    let mut workers = Vec::new();
    for _ in 0..concurrency {
        let lim = lim.clone();
        let ctx = ctx.clone();
        let total_requests = total_requests.clone();
        workers.push(tokio::spawn(async move {
            loop {
                if ctx.err().is_some() {
                    return;
                }
                let r = lim.reserve(
                    &Context::background(),
                    Duration::from_secs(30),
                    Instant::now(),
                    reserve_n as f64,
                );
                if r.reserved() {
                    let delay = r.delay_from(Instant::now());
                    tokio::time::sleep(delay).await;
                } else {
                    panic!("r not ok");
                }
                total_requests.fetch_add(1, Ordering::SeqCst);
            }
        }));
    }
    let v_qps = Arc::new(Mutex::new(0.0f64));
    let wait = Arc::new(Mutex::new(Duration::ZERO));
    let monitor = {
        let lim = lim.clone();
        let ctx = ctx.clone();
        let total_requests = total_requests.clone();
        let v_qps = v_qps.clone();
        let wait = wait.clone();
        tokio::spawn(async move {
            loop {
                if start.elapsed() >= TEST_CASE_RUN_TIME {
                    break;
                }
                let window_requests = total_requests.swap(0, Ordering::SeqCst);
                *v_qps.lock().unwrap() = window_requests as f64;
                let r = lim.reserve(
                    &ctx,
                    Duration::from_secs(30),
                    Instant::now(),
                    reserve_n as f64,
                );
                *wait.lock().unwrap() = r.delay();
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        })
    };
    monitor.await.unwrap();
    cancel.cancel();
    for worker in workers {
        worker.await.unwrap();
    }
    let qps = *v_qps.lock().unwrap();
    let wait = *wait.lock().unwrap();
    (qps, qps * reserve_n as f64, wait)
}

/// pd-client logs `Reconfigure` under controller trace logging with zap
/// fields, not a formatted message.
#[test]
#[serial]
fn reconfigure_trace_logs_carry_pd_clients_fields() {
    crate::logutil::test_capture::install();
    super::global_controller::set_enable_controller_trace_log_for_test(true);
    let times = Times::reset();
    let (nc, _rx) = notify_channel();
    let lim = new_limiter(times.t(0), 1.0, 0, 2.0, &nc);
    let args = TokenBucketReconfigureArgs {
        new_tokens: 6.0,
        new_fill_rate: 2.5,
        new_notify_threshold: 4321.5,
        ..TokenBucketReconfigureArgs::default()
    };
    lim.reconfigure(times.t(0), args, &[]);
    super::global_controller::set_enable_controller_trace_log_for_test(false);
    let lines = crate::logutil::test_capture::lines();
    assert!(
        lines.iter().any(|line| line
            == "INFO [resource group controller] before reconfigure name= old-tokens=2 old-rate=1 old-notify-threshold=0 old-burst=0"),
        "{lines:?}"
    );
    assert!(
        lines.iter().any(|line| line
            == "INFO [resource group controller] after reconfigure name= tokens=8 rate=2.5 notify-threshold=4321.5 burst=0"),
        "{lines:?}"
    );
}
