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

//! Go `pkg/executor/internal/exec/adaptive_limit_controller_test.go`.

use std::sync::mpsc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tidb_gocontext::{Context, CtxError};

use super::*;

fn reserve_outer_for_test(controller: &AdaptiveLimitController, max_rows: usize) -> (usize, bool) {
    controller.reserve_outer(&Context::background(), max_rows).unwrap()
}

fn reserve_lookup_for_test(controller: &AdaptiveLimitController, max_rows: usize) -> (usize, bool) {
    controller.reserve_lookup(&Context::background(), max_rows).unwrap()
}

fn new_adaptive_limit_controller_for_test(
    demand_rows: u64,
    initial_outer_window: u64,
    max_outer_window: u64,
    initial_lookup_window: u64,
    max_lookup_window: u64,
) -> AdaptiveLimitController {
    AdaptiveLimitController::new(AdaptiveLimitConfig {
        demand_rows,
        initial_outer_window,
        max_outer_window,
        initial_lookup_window,
        max_lookup_window,
        initial_lookup_batch_size: initial_lookup_window,
        max_lookup_batch_size: max_lookup_window,
        ..Default::default()
    })
}

fn recent_outer_totals(controller: &AdaptiveLimitController) -> (u64, u64) {
    controller.lock().recent_outer_yield.totals()
}

fn recent_lookup_totals(controller: &AdaptiveLimitController) -> (u64, u64) {
    controller.lock().recent_lookup_yield.totals()
}

/// Go `require.Eventually` over a minute-scale bound.
fn eventually(mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(1);
    while !condition() {
        assert!(Instant::now() < deadline, "condition never held");
        std::thread::sleep(Duration::from_millis(1));
    }
}

type ReservationResult = Result<(usize, bool), CtxError>;

fn spawn_reserve(
    controller: &Arc<AdaptiveLimitController>,
    ctx: &Context,
    stage: AdaptiveAdmissionStage,
    max_units: usize,
) -> mpsc::Receiver<ReservationResult> {
    let (tx, rx) = mpsc::channel();
    let controller = Arc::clone(controller);
    let ctx = ctx.clone();
    std::thread::spawn(move || {
        let result = match stage {
            AdaptiveAdmissionStage::OuterRows => controller.reserve_outer(&ctx, max_units),
            AdaptiveAdmissionStage::LookupHandles => controller.reserve_lookup(&ctx, max_units),
        };
        let _ = tx.send(result);
    });
    rx
}

/// Go `TestAdaptiveLimitControllerOuterOnly`.
#[test]
fn test_adaptive_limit_controller_outer_only() {
    // zero output with in-flight work
    {
        let controller = AdaptiveLimitController::new_outer_join(AdaptiveLimitConfig {
            demand_rows: 32000,
            initial_outer_window: 32000,
            max_outer_window: 125000,
            ..Default::default()
        });
        let admit = |rows: usize| {
            let (reserved, ok) = reserve_outer_for_test(&controller, 25000);
            assert!(ok);
            assert_eq!(reserved, rows);
            controller.commit_outer(reserved, reserved);
        };

        admit(25000);
        admit(7000);
        controller.observe_join_progress(25000, 0);
        admit(25000);
        controller.observe_join_progress(7000, 0);
        assert_eq!(controller.snapshot().outer_window, 64000);

        admit(25000);
        // With 50,000 rows outstanding, wait for a full batch of room.
        let (ctx, _cancel) = Context::with_timeout(&Context::background(), Duration::from_millis(20));
        assert_eq!(controller.reserve_outer(&ctx, 25000), Err(CtxError::DeadlineExceeded));
        assert!(controller.snapshot().outer_admission_blocked > Duration::ZERO);

        controller.observe_join_progress(25000, 0);
        admit(25000);
        controller.reset();
        admit(25000);
        admit(7000);
    }

    let controller = AdaptiveLimitController::new_outer_join(AdaptiveLimitConfig {
        demand_rows: 3,
        initial_outer_window: 3,
        max_outer_window: 12,
        initial_lookup_window: 4,
        max_lookup_window: 8,
        ..Default::default()
    });
    assert!(!controller.has_lookup_stage());
    assert_eq!(reserve_lookup_for_test(&controller, 1), (0, false));
    let (ctx, cancel) = Context::with_cancel(&Context::background());
    cancel.cancel();
    assert_eq!(controller.reserve_lookup(&ctx, 1), Err(CtxError::Canceled));

    let (reserved, ok) = reserve_outer_for_test(&controller, 12);
    assert!(ok);
    assert_eq!(reserved, 3);
    controller.commit_outer(reserved, reserved);
    controller.observe_join_progress(reserved, 0);
    assert_eq!(controller.snapshot().outer_window, 3);

    let (reserved, ok) = reserve_outer_for_test(&controller, 12);
    assert!(ok);
    assert_eq!(reserved, 3);
    controller.commit_outer(reserved, reserved);
    controller.observe_join_progress(reserved, 3);
    assert!(controller.snapshot().stopped);

    controller.reset();
    let snapshot = controller.snapshot();
    assert!(!snapshot.stopped);
    assert!(!snapshot.has_lookup_stage);
    assert_eq!(snapshot.lookup_window, 0);
    assert_eq!(snapshot.lookup_batch_size, 0);
    assert_eq!(snapshot.outer_fetched, 0);
    assert_eq!(snapshot.outer_window, 3);
}

/// Go `TestAdaptiveLimitControllerUsesCurrentExecutionYield`.
#[test]
fn test_adaptive_limit_controller_uses_current_execution_yield() {
    let controller = new_adaptive_limit_controller_for_test(1000, 32, 100000, 32, 100000);
    let (reserved, ok) = reserve_outer_for_test(&controller, 25000);
    assert!(ok);
    assert_eq!(reserved, 32);
    controller.commit_outer(reserved, reserved);
    controller.observe_join_progress(32, 32);

    let (reserved, ok) = reserve_outer_for_test(&controller, 25000);
    assert!(ok);
    assert_eq!(reserved, 64);
    let snapshot = controller.snapshot();
    assert_eq!(snapshot.outer_fetched, 32);
    assert_eq!(snapshot.outer_consumed, 32);
    assert_eq!(snapshot.output_rows, 32);

    let tail_controller = new_adaptive_limit_controller_for_test(1000, 1024, 100000, 1024, 100000);
    let (reserved, ok) = reserve_outer_for_test(&tail_controller, 25000);
    assert!(ok);
    assert_eq!(reserved, 1000);
    tail_controller.commit_outer(reserved, reserved);
    tail_controller.observe_join_progress(999, 999);
    assert_eq!(tail_controller.snapshot().outer_window, 1);

    let mid_controller = new_adaptive_limit_controller_for_test(1000, 500, 100000, 500, 100000);
    let (reserved, ok) = reserve_outer_for_test(&mid_controller, 25000);
    assert!(ok);
    assert_eq!(reserved, 500);
    mid_controller.commit_outer(reserved, reserved);
    mid_controller.observe_join_progress(500, 500);
    assert_eq!(mid_controller.snapshot().outer_window, 563);

    let phase_controller = new_adaptive_limit_controller_for_test(1000, 1000, 100000, 1000, 100000);
    let (reserved, ok) = reserve_outer_for_test(&phase_controller, 1000);
    assert!(ok);
    phase_controller.commit_outer(reserved, reserved);
    phase_controller.observe_join_progress(900, 900);
    phase_controller.observe_join_progress(100, 10);
    assert_eq!(phase_controller.snapshot().outer_window, 99);
}

/// Go `TestAdaptiveLimitControllerAllowsOneGrowthPerProgressEpoch`.
#[test]
fn test_adaptive_limit_controller_allows_one_growth_per_progress_epoch() {
    let controller = new_adaptive_limit_controller_for_test(1000, 32, 100000, 32, 100000);
    let (reserved, ok) = reserve_outer_for_test(&controller, 32);
    assert!(ok);
    controller.commit_outer(reserved, reserved);
    controller.observe_join_progress(1, 1);
    assert_eq!(controller.snapshot().outer_window, 64);

    // More callbacks from the same fetched batch must not compound the growth.
    controller.observe_join_progress(1, 1);
    controller.observe_join_progress(1, 1);
    assert_eq!(controller.snapshot().outer_window, 64);

    let (reserved, ok) = reserve_outer_for_test(&controller, 32);
    assert!(ok);
    controller.commit_outer(reserved, reserved);
    controller.observe_join_progress(29, 29);
    assert_eq!(controller.snapshot().outer_window, 64);
    controller.observe_join_progress(1, 1);
    assert_eq!(controller.snapshot().outer_window, 128);

    let lookup_controller = new_adaptive_limit_controller_for_test(1000, 32, 100000, 32, 100000);
    let (reserved, ok) = reserve_lookup_for_test(&lookup_controller, 32);
    assert!(ok);
    lookup_controller.complete_lookup(reserved, reserved, 1);
    assert_eq!(lookup_controller.snapshot().lookup_window, 64);

    let (reserved, ok) = reserve_outer_for_test(&lookup_controller, 32);
    assert!(ok);
    lookup_controller.commit_outer(reserved, reserved);
    lookup_controller.observe_join_progress(1, 1);
    assert_eq!(lookup_controller.snapshot().lookup_window, 64);

    let (reserved, ok) = reserve_lookup_for_test(&lookup_controller, 64);
    assert!(ok);
    lookup_controller.complete_lookup(reserved, reserved, 1);
    assert_eq!(lookup_controller.snapshot().lookup_window, 128);
}

/// Go `TestAdaptiveLimitControllerPairsOutputWithCompletedOuterRows`.
#[test]
fn test_adaptive_limit_controller_pairs_output_with_completed_outer_rows() {
    let controller = new_adaptive_limit_controller_for_test(1000, 32, 100000, 32, 100000);
    let (reserved, ok) = reserve_outer_for_test(&controller, 32);
    assert!(ok);
    controller.commit_outer(reserved, reserved);

    // One outer row can fill several result chunks before it is fully
    // consumed.
    for _ in 0..4 {
        controller.observe_join_progress(0, 8);
    }
    let snapshot = controller.snapshot();
    assert_eq!(snapshot.output_rows, 32);
    assert_eq!(snapshot.outer_window, 32);
    assert_eq!(recent_outer_totals(&controller), (0, 0));

    controller.observe_join_progress(1, 0);
    let snapshot = controller.snapshot();
    assert_eq!(snapshot.outer_consumed, 1);
    assert_eq!(snapshot.outer_window, 39);
    assert_eq!(recent_outer_totals(&controller), (1, 32));
}

/// Go `TestAdaptiveLimitControllerGrowsWhenConsumedInputHasNoOutput`.
#[test]
fn test_adaptive_limit_controller_grows_when_consumed_input_has_no_output() {
    // double read recovers after productive input
    {
        let controller = new_adaptive_limit_controller_for_test(128, 16, 128, 16, 64);
        let read_batch = |want: usize| {
            let (rows, ok) = reserve_outer_for_test(&controller, want);
            assert!(ok);
            assert_eq!(rows, want);
            let mut remaining = rows;
            while remaining > 0 {
                let (handles, ok) = reserve_lookup_for_test(&controller, remaining);
                assert!(ok);
                assert!(handles > 0);
                controller.complete_lookup(handles, handles, handles);
                remaining -= handles;
            }
            controller.commit_outer(rows, rows);
        };
        read_batch(16);
        controller.observe_join_progress(16, 1);
        assert_eq!(controller.snapshot().outer_window, 32);

        read_batch(16);
        read_batch(16);
        controller.observe_join_progress(16, 0);
        read_batch(16);
        controller.observe_join_progress(16, 0);
        assert_eq!(controller.snapshot().outer_window, 64);
        assert_eq!(controller.suggested_batch_size(64), 16);

        read_batch(16);
        read_batch(16);
        for _ in 0..4 {
            controller.observe_join_progress(16, 0);
            read_batch(16);
        }
        assert_eq!(controller.snapshot().outer_window, 128);
        assert_eq!(controller.suggested_batch_size(64), 32);

        // A matching outer row can fill a result chunk before it is consumed.
        controller.observe_join_progress(0, 8);
        assert_eq!(controller.suggested_batch_size(64), 16);
        assert_eq!(controller.snapshot().outer_window, 128);
        assert_eq!(recent_outer_totals(&controller), (0, 0));

        controller.observe_join_progress(16, 112);
        assert!(controller.snapshot().outer_window < 128);
        assert_eq!(controller.suggested_batch_size(64), 16);
        assert_eq!(recent_outer_totals(&controller), (16, 120));
        controller.reset();
        assert_eq!(controller.snapshot().outer_window, 16);
        assert_eq!(controller.suggested_batch_size(64), 16);
    }

    let controller = new_adaptive_limit_controller_for_test(1000, 32, 100000, 32, 100000);
    for expected in [32, 64, 128] {
        let (reserved, ok) = reserve_outer_for_test(&controller, 25000);
        assert!(ok);
        assert_eq!(reserved, expected);
        controller.commit_outer(reserved, reserved);
        controller.observe_join_progress(reserved, 0);
    }

    // A sparse phase must recover even after an earlier high-yield phase
    // shrank the window to one row.
    let controller = new_adaptive_limit_controller_for_test(1000, 1000, 100000, 1000, 100000);
    let (reserved, ok) = reserve_outer_for_test(&controller, 1000);
    assert!(ok);
    controller.commit_outer(reserved, reserved);
    controller.observe_join_progress(999, 999);
    assert_eq!(controller.snapshot().outer_window, 1);
    controller.observe_join_progress(1, 0);
    assert_eq!(controller.snapshot().outer_window, 2);
    assert_eq!(recent_outer_totals(&controller), (0, 0));
    for expected in [4, 8] {
        let (reserved, ok) = reserve_outer_for_test(&controller, 1000);
        assert!(ok);
        controller.commit_outer(reserved, reserved);
        controller.observe_join_progress(reserved, 0);
        assert_eq!(controller.snapshot().outer_window, expected);
    }
}

/// Go `TestAdaptiveLimitControllerStopInterruptsReservation`.
#[test]
fn test_adaptive_limit_controller_stop_interrupts_reservation() {
    let controller = Arc::new(new_adaptive_limit_controller_for_test(1000, 32, 100000, 32, 100000));
    let (reserved, ok) = reserve_outer_for_test(&controller, 32);
    assert!(ok);
    controller.commit_outer(reserved, reserved);
    let (lookup_reserved, ok) = reserve_lookup_for_test(&controller, 32);
    assert!(ok);

    let result = spawn_reserve(&controller, &Context::background(), AdaptiveAdmissionStage::OuterRows, 32);
    assert!(
        result.recv_timeout(Duration::from_millis(20)).is_err(),
        "reservation should wait for consumption or stop"
    );

    controller.stop();
    let snapshot = controller.snapshot();
    assert!(snapshot.stopped);
    assert_eq!(snapshot.outer_window, 0);
    assert_eq!(snapshot.lookup_window, 0);
    assert_eq!(snapshot.lookup_reserved, 0);
    assert_eq!(snapshot.outer_outstanding_at_stop, reserved as u64);
    assert_eq!(snapshot.lookup_outstanding_at_stop, lookup_reserved as u64);
    let woken = result
        .recv_timeout(Duration::from_secs(1))
        .expect("stop did not wake the blocked reservation");
    assert_eq!(woken.map(|(_, admitted)| admitted), Ok(false));
    assert!(controller.snapshot().outer_admission_blocked > Duration::ZERO);

    controller.reset();
    let (reserved, ok) = reserve_outer_for_test(&controller, 32);
    assert!(ok);
    assert_eq!(reserved, 32);
    let snapshot = controller.snapshot();
    assert!(!snapshot.stopped);
    assert_eq!(snapshot.outer_fetched, 0);
    assert_eq!(snapshot.outer_outstanding_at_stop, 0);
    assert_eq!(snapshot.lookup_outstanding_at_stop, 0);
    let (canceled_ctx, cancel) = Context::with_cancel(&Context::background());
    cancel.cancel();
    assert_eq!(controller.reserve_outer(&canceled_ctx, 1), Err(CtxError::Canceled));
    controller.stop();
    controller.stop();

    let available_controller = new_adaptive_limit_controller_for_test(1000, 32, 100000, 32, 100000);
    assert_eq!(available_controller.reserve_outer(&canceled_ctx, 1), Err(CtxError::Canceled));
    assert_eq!(available_controller.snapshot().outer_reserved, 0);
    assert_eq!(available_controller.reserve_lookup(&canceled_ctx, 1), Err(CtxError::Canceled));
    assert_eq!(available_controller.snapshot().lookup_reserved, 0);

    let blocked_outer_controller = Arc::new(new_adaptive_limit_controller_for_test(1000, 1, 1, 1, 1));
    let (reserved, ok) = reserve_outer_for_test(&blocked_outer_controller, 1);
    assert!(ok);
    blocked_outer_controller.commit_outer(reserved, reserved);
    let (blocked_outer_ctx, cancel_blocked_outer) = Context::with_cancel(&Context::background());
    let blocked_outer_result =
        spawn_reserve(&blocked_outer_controller, &blocked_outer_ctx, AdaptiveAdmissionStage::OuterRows, 1);
    assert!(
        blocked_outer_result.recv_timeout(Duration::from_millis(20)).is_err(),
        "outer reservation did not block"
    );
    cancel_blocked_outer.cancel();
    let woken = blocked_outer_result
        .recv_timeout(Duration::from_secs(1))
        .expect("cancel did not wake the blocked outer reservation");
    assert_eq!(woken, Err(CtxError::Canceled));
    assert_eq!(blocked_outer_controller.snapshot().outer_reserved, 0);
    assert!(blocked_outer_controller.snapshot().outer_admission_blocked > Duration::ZERO);

    let blocked_lookup_controller = Arc::new(new_adaptive_limit_controller_for_test(1000, 1, 1, 1, 1));
    let (reserved, ok) = reserve_lookup_for_test(&blocked_lookup_controller, 1);
    assert!(ok);
    let (blocked_lookup_ctx, cancel_blocked_lookup) = Context::with_cancel(&Context::background());
    let blocked_lookup_result =
        spawn_reserve(&blocked_lookup_controller, &blocked_lookup_ctx, AdaptiveAdmissionStage::LookupHandles, 1);
    assert!(
        blocked_lookup_result.recv_timeout(Duration::from_millis(20)).is_err(),
        "lookup reservation did not block"
    );
    cancel_blocked_lookup.cancel();
    let woken = blocked_lookup_result
        .recv_timeout(Duration::from_secs(1))
        .expect("cancel did not wake the blocked lookup reservation");
    assert_eq!(woken, Err(CtxError::Canceled));
    assert_eq!(blocked_lookup_controller.snapshot().lookup_reserved, reserved as u64);
    assert!(blocked_lookup_controller.snapshot().lookup_admission_blocked > Duration::ZERO);

    let pending_controller = new_adaptive_limit_controller_for_test(1000, 64, 100000, 32, 100000);
    let (fetched, ok) = reserve_outer_for_test(&pending_controller, 32);
    assert!(ok);
    pending_controller.commit_outer(fetched, fetched);
    let (pending, ok) = reserve_outer_for_test(&pending_controller, 32);
    assert!(ok);
    pending_controller.stop();
    assert_eq!(pending_controller.snapshot().outer_outstanding_at_stop, (fetched + pending) as u64);
}

/// Go `TestAdaptiveLimitControllerBoundsLookupAdmission`.
#[test]
fn test_adaptive_limit_controller_bounds_lookup_admission() {
    let split_controller = AdaptiveLimitController::new(AdaptiveLimitConfig {
        demand_rows: 1,
        initial_outer_window: 1,
        max_outer_window: 100000,
        initial_lookup_window: 1,
        max_lookup_window: 100000,
        initial_lookup_batch_size: 1024,
        max_lookup_batch_size: 20000,
        ..Default::default()
    });
    let snapshot = split_controller.snapshot();
    assert_eq!(snapshot.lookup_window, 1);
    assert_eq!(snapshot.lookup_batch_size, 1024);
    assert_eq!(snapshot.lookup_physical_window, 1024);
    let (reserved, ok) = reserve_lookup_for_test(&split_controller, 20000);
    assert!(ok);
    assert_eq!(reserved, 1024);
    split_controller.complete_lookup(reserved, reserved, 0);
    let snapshot = split_controller.snapshot();
    assert_eq!(snapshot.lookup_window, 2);
    assert_eq!(snapshot.lookup_batch_size, 2048);
    assert_eq!(snapshot.lookup_physical_window, 2048);
    split_controller.stop();
    let snapshot = split_controller.snapshot();
    assert_eq!(snapshot.lookup_batch_size, 0);
    assert_eq!(snapshot.lookup_physical_window, 0);
    split_controller.reset();
    let snapshot = split_controller.snapshot();
    assert_eq!(snapshot.lookup_window, 1);
    assert_eq!(snapshot.lookup_batch_size, 1024);
    assert_eq!(snapshot.lookup_physical_window, 1024);
    let capped_controller = AdaptiveLimitController::new(AdaptiveLimitConfig {
        demand_rows: 1,
        initial_outer_window: 1,
        max_outer_window: 500,
        initial_lookup_window: 1,
        max_lookup_window: 500,
        initial_lookup_batch_size: 1024,
        max_lookup_batch_size: 20000,
        ..Default::default()
    });
    let snapshot = capped_controller.snapshot();
    assert_eq!(snapshot.lookup_batch_size, 500);
    assert_eq!(snapshot.lookup_physical_window, 500);
    let hard_capped_controller = AdaptiveLimitController::new(AdaptiveLimitConfig {
        demand_rows: 1,
        initial_outer_window: 1,
        max_outer_window: 100000,
        initial_lookup_window: 1,
        max_lookup_window: 100000,
        initial_lookup_batch_size: 1024,
        max_lookup_batch_size: 32,
        ..Default::default()
    });
    let snapshot = hard_capped_controller.snapshot();
    assert_eq!(snapshot.lookup_batch_size, 32);
    assert_eq!(snapshot.lookup_physical_window, 32);

    let controller = new_adaptive_limit_controller_for_test(1000, 32, 100000, 32, 100000);
    let (reserved, ok) = reserve_lookup_for_test(&controller, 1000);
    assert!(ok);
    assert_eq!(reserved, 32);
    controller.complete_lookup(reserved, reserved, 1);
    let (reserved, ok) = reserve_outer_for_test(&controller, 32);
    assert!(ok);
    controller.commit_outer(reserved, 32);
    controller.observe_join_progress(32, 32);
    let snapshot = controller.snapshot();
    assert_eq!(snapshot.outer_window, 64);
    assert_eq!(snapshot.lookup_window, 64);
    assert_eq!(snapshot.lookup_handles, 32);
    assert_eq!(snapshot.lookup_rows, 1);
    assert_eq!(controller.suggested_batch_size(1000), 32);
    assert_eq!(snapshot.lookup_physical_window, 64);

    let (reserved, ok) = reserve_lookup_for_test(&controller, 1000);
    assert!(ok);
    assert_eq!(reserved, 32);
    let (second_reserved, ok) = reserve_lookup_for_test(&controller, 1000);
    assert!(ok);
    assert_eq!(second_reserved, 32);
    controller.abort_lookup(reserved + second_reserved);
    let snapshot = controller.snapshot();
    assert_eq!(snapshot.lookup_reserved, 0);
    assert_eq!(snapshot.lookup_handles, 32);
    assert_eq!(snapshot.lookup_rows, 1);

    let phase_controller = new_adaptive_limit_controller_for_test(1000, 64, 100000, 32, 100000);
    let (reserved, ok) = reserve_lookup_for_test(&phase_controller, 1000);
    assert!(ok);
    phase_controller.complete_lookup(reserved, reserved, reserved);
    assert_eq!(phase_controller.snapshot().lookup_window, 32);
    for expected in [64, 128] {
        let (reserved, ok) = reserve_lookup_for_test(&phase_controller, 1000);
        assert!(ok);
        phase_controller.complete_lookup(reserved, reserved, 0);
        let snapshot = phase_controller.snapshot();
        assert_eq!(snapshot.lookup_window, expected);
        assert_eq!(snapshot.lookup_batch_size, expected);
        assert_eq!(snapshot.lookup_physical_window, expected);
    }
    assert_eq!(recent_lookup_totals(&phase_controller), (0, 0));

    let local_yield_controller = new_adaptive_limit_controller_for_test(1000, 64, 100000, 32, 100000);
    let (reserved, ok) = reserve_lookup_for_test(&local_yield_controller, 32);
    assert!(ok);
    local_yield_controller.complete_lookup(reserved, reserved, reserved);
    let (reserved, ok) = reserve_lookup_for_test(&local_yield_controller, 32);
    assert!(ok);
    local_yield_controller.complete_lookup(reserved, reserved, 4);
    let snapshot = local_yield_controller.snapshot();
    assert_eq!(snapshot.lookup_window, 50);
    assert_eq!(snapshot.lookup_batch_size, 32);
    assert_eq!(snapshot.lookup_physical_window, 64);
    assert!(snapshot.lookup_physical_window - snapshot.lookup_window < snapshot.lookup_batch_size);

    let mut recent = AdaptiveYieldWindow::default();
    recent.add(900, 900);
    for _ in 0..3 {
        recent.add(100, 10);
    }
    assert_eq!(recent.totals(), (1200, 930));
    recent.add(100, 10);
    assert_eq!(recent.totals(), (400, 40));

    let partial_controller = new_adaptive_limit_controller_for_test(1000, 32, 100000, 32, 100000);
    let (reserved, ok) = reserve_lookup_for_test(&partial_controller, 1000);
    assert!(ok);
    partial_controller.abort_lookup(reserved - 10);
    partial_controller.complete_lookup(10, 10, 5);
    let snapshot = partial_controller.snapshot();
    assert_eq!(snapshot.lookup_reserved, 0);
    assert_eq!(snapshot.lookup_handles, 10);
    assert_eq!(snapshot.lookup_rows, 5);
}

/// Go `TestAdaptiveLimitControllerDirectLookupUsesLookupYield`.
#[test]
fn test_adaptive_limit_controller_direct_lookup_uses_lookup_yield() {
    let controller = AdaptiveLimitController::new_lookup(AdaptiveLimitConfig {
        demand_rows: 1000,
        initial_lookup_window: 32,
        max_lookup_window: 100000,
        initial_lookup_batch_size: 32,
        max_lookup_batch_size: 100000,
        ..Default::default()
    });
    assert_eq!(controller.snapshot().outer_window, 0);
    assert_eq!(reserve_outer_for_test(&controller, 32), (0, false));

    let (reserved, ok) = reserve_lookup_for_test(&controller, 32);
    assert!(ok);
    assert_eq!(reserved, 32);
    controller.complete_lookup(reserved, reserved, 1);
    let snapshot = controller.snapshot();
    assert_eq!(snapshot.output_rows, 1);
    assert_eq!(snapshot.lookup_handles, 32);
    assert_eq!(snapshot.lookup_rows, 1);
    assert!(snapshot.lookup_window > 32);

    let stop_controller = AdaptiveLimitController::new_lookup(AdaptiveLimitConfig {
        demand_rows: 2,
        initial_lookup_window: 2,
        max_lookup_window: 32,
        initial_lookup_batch_size: 2,
        max_lookup_batch_size: 32,
        ..Default::default()
    });
    let (reserved, ok) = reserve_lookup_for_test(&stop_controller, 2);
    assert!(ok);
    stop_controller.complete_lookup(reserved, reserved, 2);
    let snapshot = stop_controller.snapshot();
    assert_eq!(snapshot.output_rows, 2);
    assert!(snapshot.stopped);
    assert_eq!(snapshot.lookup_window, 0);
    assert_eq!(snapshot.lookup_batch_size, 0);
}

/// Go `TestAdaptiveLimitControllerDirectLookupGrowsOnNoOutput`.
#[test]
fn test_adaptive_limit_controller_direct_lookup_grows_on_no_output() {
    for (name, demand, first_rows) in [
        ("limit_one_empty", 1usize, 0usize),
        ("limit_two_empty", 2, 0),
        ("limit_two_partial", 2, 1),
        ("limit_two_full", 2, 2),
    ] {
        let controller = Arc::new(AdaptiveLimitController::new_lookup(AdaptiveLimitConfig {
            demand_rows: demand as u64,
            initial_lookup_window: demand as u64,
            max_lookup_window: 5000,
            initial_lookup_batch_size: demand as u64,
            max_lookup_batch_size: 1000,
            no_output_lookup_batch_size: 32,
            no_output_lookup_window: 160,
            ..Default::default()
        }));
        let (mut reserved, ok) = reserve_lookup_for_test(&controller, 1000);
        assert!(ok, "{name}");
        assert_eq!(reserved, demand, "{name}");
        if first_rows == demand {
            controller.complete_lookup(reserved, reserved, first_rows);
            assert!(controller.snapshot().stopped, "{name}");
            assert_eq!(controller.snapshot().lookup_reserved, 0, "{name}");
            controller.stop();
            continue;
        }
        let mut pending = vec![reserved];
        if first_rows > 0 {
            controller.complete_lookup(reserved, reserved, first_rows);
            // Productive feedback can grow the window before the next miss.
            let window = controller.lock().lookup_physical_window() as usize;
            pending.clear();
            let mut remaining = window;
            while remaining > 0 {
                let ok;
                (reserved, ok) = reserve_lookup_for_test(&controller, 1000);
                assert!(ok, "{name}");
                assert_eq!(reserved, demand.min(remaining), "{name}");
                pending.push(reserved);
                remaining = remaining.saturating_sub(reserved);
            }
            // Refilling an intermediate slot is valid. Drain the window before
            // testing the next reservation so this case does not race a
            // refill.
            for (i, &reserved) in pending.iter().enumerate() {
                controller.complete_lookup(reserved, reserved, 0);
                if i + 1 < pending.len() {
                    assert_eq!(controller.snapshot().lookup_batch_size, demand as u64, "{name}");
                }
            }
            let (reserved, ok) = reserve_lookup_for_test(&controller, 1000);
            assert!(ok, "{name}");
            assert_eq!(reserved, 32, "{name}");
        } else {
            let (ctx, _cancel) = Context::with_timeout(&Context::background(), Duration::from_secs(5));
            let admitted = spawn_reserve(&controller, &ctx, AdaptiveAdmissionStage::LookupHandles, 1000);
            eventually(|| controller.lock().lookup_admission_blocked.waiters == 1);
            controller.complete_lookup(reserved, reserved, 0);
            let (n, _) = admitted.recv_timeout(Duration::from_secs(5)).unwrap().unwrap();
            assert_eq!(n, 32, "{name}");
        }
        for _ in 0..4 {
            let (reserved, ok) = reserve_lookup_for_test(&controller, 1000);
            assert!(ok, "{name}");
            assert_eq!(reserved, 32, "{name}");
        }
        assert_eq!(controller.snapshot().lookup_reserved, 160, "{name}");
        controller.complete_lookup(32, 32, demand - first_rows);
        assert!(controller.snapshot().stopped, "{name}");
        assert_eq!(controller.snapshot().lookup_reserved, 0, "{name}");
        controller.reset();
        let (reserved, ok) = reserve_lookup_for_test(&controller, 1000);
        assert!(ok, "{name}");
        assert_eq!(reserved, demand, "{name}");
        controller.stop();
    }

    // small_limit_sustained_miss_with_in_flight_tasks
    {
        let controller = AdaptiveLimitController::new_lookup(AdaptiveLimitConfig {
            demand_rows: 2,
            initial_lookup_window: 2,
            max_lookup_window: 5000,
            initial_lookup_batch_size: 2,
            max_lookup_batch_size: 1000,
            no_output_lookup_batch_size: 32,
            no_output_lookup_window: 160,
            ..Default::default()
        });
        let (reserved, ok) = reserve_lookup_for_test(&controller, 1000);
        assert!(ok);
        assert_eq!(reserved, 2);
        controller.complete_lookup(reserved, reserved, 0);
        for _ in 0..5 {
            assert_eq!(reserve_lookup_for_test(&controller, 1000), (32, true));
        }

        // Simulate refilling the window as each empty task completes.
        for _ in 0..4 {
            controller.complete_lookup(32, 32, 0);
            let snapshot = controller.snapshot();
            assert_eq!(snapshot.lookup_batch_size, 32);
            assert_eq!(snapshot.lookup_window, 160);
            assert_eq!(reserve_lookup_for_test(&controller, 1000), (32, true));
        }

        controller.complete_lookup(32, 32, 0);
        let snapshot = controller.snapshot();
        assert_eq!(snapshot.lookup_reserved, 128);
        assert_eq!(snapshot.lookup_window, 320);
        assert_eq!(snapshot.lookup_batch_size, 64);
        assert_eq!(reserve_lookup_for_test(&controller, 1000), (64, true));
        assert_eq!(controller.snapshot().lookup_reserved, 192);
        controller.stop();
    }

    let controller = AdaptiveLimitController::new_lookup(AdaptiveLimitConfig {
        demand_rows: 1000,
        initial_lookup_window: 32,
        max_lookup_window: 100000,
        initial_lookup_batch_size: 32,
        max_lookup_batch_size: 100000,
        ..Default::default()
    });
    for expected in [64, 128, 256] {
        let (reserved, ok) = reserve_lookup_for_test(&controller, 1000);
        assert!(ok);
        controller.complete_lookup(reserved, reserved, 0);
        assert_eq!(controller.snapshot().lookup_window, expected);
    }
}

/// Go `TestAdaptiveLimitControllerMergesConcurrentLookupBlocks`.
#[test]
fn test_adaptive_limit_controller_merges_concurrent_lookup_blocks() {
    let controller = Arc::new(new_adaptive_limit_controller_for_test(1000, 1, 1, 1, 1));
    let (reserved, ok) = reserve_lookup_for_test(&controller, 1);
    assert!(ok);
    let blocked_start = Instant::now();

    let (results_tx, results) = mpsc::channel();
    let spawn_two = |results_tx: &mpsc::Sender<ReservationResult>| {
        for _ in 0..2 {
            let controller = Arc::clone(&controller);
            let results_tx = results_tx.clone();
            std::thread::spawn(move || {
                let _ = results_tx.send(controller.reserve_lookup(&Context::background(), 1));
            });
        }
    };
    spawn_two(&results_tx);
    eventually(|| controller.lock().lookup_admission_blocked.waiters == 2);

    controller.abort_lookup(reserved);
    assert_eq!(results.recv().unwrap().map(|(_, admitted)| admitted), Ok(true));
    controller.abort_lookup(1);
    assert_eq!(results.recv().unwrap().map(|(_, admitted)| admitted), Ok(true));
    controller.abort_lookup(1);

    let snapshot = controller.snapshot();
    let blocked_elapsed = blocked_start.elapsed();
    assert!(snapshot.lookup_admission_blocked > Duration::ZERO);
    assert!(snapshot.lookup_admission_blocked <= blocked_elapsed + Duration::from_millis(50));
    assert_eq!(snapshot.lookup_reserved, 0);

    controller.reset();
    assert_eq!(controller.snapshot().lookup_admission_blocked, Duration::ZERO);
    let (reserved, ok) = reserve_lookup_for_test(&controller, 1);
    assert!(ok);
    spawn_two(&results_tx);
    eventually(|| controller.lock().lookup_admission_blocked.waiters == 2);
    controller.stop();
    assert_eq!(controller.lock().lookup_admission_blocked.waiters, 0);
    for _ in 0..2 {
        assert_eq!(results.recv().unwrap().map(|(_, admitted)| admitted), Ok(false));
    }
    assert_eq!(controller.snapshot().lookup_outstanding_at_stop, reserved as u64);
}
