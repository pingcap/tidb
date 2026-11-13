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

//! Go `MPPGather`'s gather-side pieces: the root tasks' responses
//! (`MppTaskResponses`), `kv.MppCoordinator` as `ExecutorWithRetry` drives it
//! (`MppGatherResponse`), and Go's `ExecutorWithRetry` with its
//! `mppErrRecovery`. The fragments, their dispatch and the coordinator are
//! `local_mpp_coordinator.rs`; the task locations, RPCs and streams are the
//! store's (Go `copr.MPPClient`, `tidb_copr::mpp_client`).

use std::collections::VecDeque;

use tidb_txnkv::{MppStreamResponse, QueryResponseError, QueryResultSubset};

use crate::StatementMemory;
use crate::recovery_handler::{RecoveryHandler, RecoveryInfo};

fn mpp_memory_error(memory: &StatementMemory) -> Result<(), QueryResponseError> {
    memory.check().map_err(|error| {
        let sql = error.to_mysql_error();
        QueryResponseError::Sql {
            code: sql.code,
            message: sql.message,
        }
    })
}

/// One gather's raw task responses, below the shared `SelectResponse`
/// decoder: Go's coordinator over its root tasks' streams.
pub struct MppTaskResponses {
    /// The root tasks' streams, read in order.
    pub streams: VecDeque<Box<dyn MppStreamResponse>>,
    /// How many tasks the gather dispatched (Go `nodeCnt`).
    pub node_count: usize,
}

impl tidb_txnkv::Response for MppTaskResponses {
    fn next(&mut self) -> Result<Option<QueryResultSubset>, QueryResponseError> {
        while let Some(stream) = self.streams.front_mut() {
            match stream.next() {
                Ok(Some(packet)) => {
                    // The stream charged the packet while receiving it; the
                    // holder above charges what it buffers.
                    stream.release_packet_memory();
                    return Ok(Some(packet));
                }
                Ok(None) => {
                    stream.close()?;
                    self.streams.pop_front();
                }
                Err(error) => {
                    let _ = self.close();
                    return Err(error);
                }
            }
        }
        Ok(None)
    }

    fn close(&mut self) -> Result<(), QueryResponseError> {
        let mut result = Ok(());
        for stream in &mut self.streams {
            let closed = stream.close();
            if result.is_ok() {
                result = closed;
            }
        }
        self.streams.clear();
        result
    }
}

impl Drop for MppTaskResponses {
    fn drop(&mut self) {
        let _ = tidb_txnkv::Response::close(self);
    }
}

/// Go `kv.MppCoordinator` as `ExecutorWithRetry` drives it: the gather's
/// response and `GetNodeCnt`.
pub trait MppGatherResponse: tidb_txnkv::Response + Send {
    /// Go `GetNodeCnt`: the nodes the gather's tasks run on.
    fn node_count(&self) -> usize;

    /// Go `localMppCoordinator.closeWithoutReport`, which a recovery closes
    /// the failed coordinator with: only report runtime stats when there is
    /// no error.
    fn close_without_report(&mut self) {
        let _ = self.close();
    }
}

impl MppGatherResponse for MppTaskResponses {
    fn node_count(&self) -> usize {
        self.node_count
    }
}

/// Reopens a gather for a recovery attempt.
pub type MppReopen = Box<dyn FnMut() -> Result<Box<dyn MppGatherResponse>, QueryResponseError> + Send>;
pub use crate::recovery_handler::MppRecover;

/// Go `ExecutorWithRetry`: recovers an mpp error if possible and retries the
/// MPP task. It attempts to hold the results of MPP; while it holds, an
/// error starts a recovery, which discards the held results and re-executes
/// the task. Once results start being returned, error recovery cannot be
/// performed anymore.
pub struct ExecutorWithRetry {
    coord: Box<dyn MppGatherResponse>,
    memory: StatementMemory,
    mpp_err_recovery: RecoveryHandler,
    /// Go `setupMPPCoordinator(ctx, true)`'s new coordinator.
    open: Option<MppReopen>,
    node_cnt: usize,
    closed: bool,
}

impl ExecutorWithRetry {
    /// Go `NewExecutorWithRetry` over its first coordinator.
    /// `disagg_tiflash_with_auto_scaler` is the config's
    /// `DisaggregatedTiFlash && UseAutoScaler`, and `allow_tiflash_fallback`
    /// the session's `AllowFallbackToTiKV[kv.TiFlash]`.
    #[must_use]
    pub fn new(
        coord: Box<dyn MppGatherResponse>,
        disagg_tiflash_with_auto_scaler: bool,
        allow_tiflash_fallback: bool,
        memory: StatementMemory,
        open: MppReopen,
        recover: MppRecover,
    ) -> Self {
        // TODO: After add row info in tipb.DataPacket, we can use row count as capacity.
        // For now, use the number of tipb.DataPacket as capacity.
        const HOLD_CAP: usize = 2;

        // 1. For now, mpp err recovery only support MemLimit, which is only useful when AutoScaler is used.
        // 2. When enable fallback to tikv, the returned mpp err will be ErrTiFlashServerTimeout,
        //    which we cannot handle for now. Also there is no need to recovery because tikv will retry the query.
        // 3. For cached table, will not dispatch tasks to TiFlash, so no need to recovery.
        #[allow(unused_mut)]
        let mut enable_mpp_recovery = disagg_tiflash_with_auto_scaler && !allow_tiflash_fallback;

        if tidb_failpoint::eval("github.com/pingcap/tidb/pkg/executor/internal/mpp/mpp_recovery_test_mock_enable").is_some() && !allow_tiflash_fallback {
            enable_mpp_recovery = true;
        }

        let node_cnt = coord.node_count();
        Self {
            coord,
            mpp_err_recovery: RecoveryHandler::new(
                disagg_tiflash_with_auto_scaler,
                HOLD_CAP,
                enable_mpp_recovery,
                memory.clone(),
                recover,
            ),
            memory,
            open: Some(open),
            node_cnt,
            closed: false,
        }
    }

    /// Go `setupMPPCoordinator(ctx, true)`: the failed coordinator is closed
    /// without its report, then a new one is built and executed.
    fn setup_mpp_coordinator(&mut self) -> Result<(), QueryResponseError> {
        // Only report runtime stats when there is no error.
        self.coord.close_without_report();
        self.coord = (self.open.as_mut().expect("open recovery response"))()?;
        self.node_cnt = self.coord.node_count();
        Ok(())
    }

    /// Go `nextWithRecovery`.
    fn next_with_recovery(&mut self) -> Result<(), QueryResponseError> {
        if !self.mpp_err_recovery.enabled() {
            return Ok(());
        }

        while self.mpp_err_recovery.can_hold_result() {
            #[allow(unused_mut)]
            let mut resp = self.coord.next();

            // Mock recovery n times.
            if tidb_failpoint::eval("github.com/pingcap/tidb/pkg/executor/internal/mpp/mpp_recovery_test_max_err_times")
                .is_some_and(|force_err_cnt| i64::from(self.mpp_err_recovery.recovery_cnt()) < force_err_cnt.int())
            {
                resp = Err(QueryResponseError::Source("mock mpp error".to_owned()));
            }

            match resp {
                Err(mpp_err) => {
                    // The statement's own quota or KILL is terminal.
                    mpp_memory_error(&self.memory)?;
                    let mut recovery_err = self
                        .mpp_err_recovery
                        .recovery(&RecoveryInfo {
                            mpp_err: &mpp_err,
                            node_cnt: self.node_cnt,
                        })
                        .err();

                    // Mock recovery succeed, ignore no recovery handler err.
                    if tidb_failpoint::eval("github.com/pingcap/tidb/pkg/executor/internal/mpp/mpp_recovery_test_ignore_recovery_err").is_some() {
                        let recovery = recovery_err
                            .as_deref()
                            .expect("mocked mpp err should got recovery err");
                        if mpp_err.to_string().contains("mock mpp error")
                            && recovery.contains("no handler to recovery")
                        {
                            recovery_err = None;
                        }
                    }

                    if recovery_err.is_some() {
                        return Err(mpp_err);
                    }

                    if self.setup_mpp_coordinator().is_err() {
                        return Err(mpp_err);
                    }
                    self.mpp_err_recovery.reset_holder();
                }
                Ok(None) => break,
                Ok(Some(resp)) => {
                    self.mpp_err_recovery.hold_result(resp);
                    mpp_memory_error(&self.memory)?;
                }
            }
        }

        if let Some(num) = tidb_failpoint::eval("github.com/pingcap/tidb/pkg/executor/internal/mpp/mpp_recovery_test_hold_size") {
            // Note: this failpoint only execute once.
            let cur_rows = self.mpp_err_recovery.num_hold_resp();
            assert!(cur_rows as i64 == num.int(), "unexpected holding rows, cur: {cur_rows}");
        }
        Ok(())
    }

    /// Go `ExecutorWithRetry.Next`.
    fn pull(&mut self) -> Result<Option<QueryResultSubset>, QueryResponseError> {
        mpp_memory_error(&self.memory)?;
        self.next_with_recovery()?;

        if self.mpp_err_recovery.num_hold_resp() != 0 {
            return self
                .mpp_err_recovery
                .pop_front_resp()
                .map(Some)
                .map_err(QueryResponseError::Source);
        }
        self.coord.next()
    }
}

impl tidb_txnkv::Response for ExecutorWithRetry {
    fn next(&mut self) -> Result<Option<QueryResultSubset>, QueryResponseError> {
        if self.closed {
            return Ok(None);
        }
        let result = self.pull();
        match &result {
            Ok(Some(_)) => {}
            // The drained gather is closed here, so Go `Close`'s error is
            // this pull's.
            Ok(None) => tidb_txnkv::Response::close(self)?,
            Err(_) => {
                let _ = tidb_txnkv::Response::close(self);
            }
        }
        result
    }

    fn close(&mut self) -> Result<(), QueryResponseError> {
        if self.closed {
            return Ok(());
        }
        self.closed = true;
        self.mpp_err_recovery.reset_holder();
        // Go `ExecutorWithRetry.Close`: `err := r.coord.Close()`.
        let result = self.coord.close();
        self.open = None;
        result
    }
}

impl Drop for ExecutorWithRetry {
    fn drop(&mut self) {
        let _ = tidb_txnkv::Response::close(self);
    }
}

#[cfg(test)]
mod recovery_tests {
    use super::*;
    use std::sync::Arc;
    use tidb_txnkv::Response as _;

    fn packet(value: u8) -> QueryResultSubset {
        QueryResultSubset {
            data: vec![value].into(),
            runtime: None,
        }
    }
    fn gather(
        rows: Vec<Result<QueryResultSubset, QueryResponseError>>,
        closed: Arc<std::sync::atomic::AtomicUsize>,
    ) -> Box<dyn MppGatherResponse> {
        Box::new(MppTaskResponses {
            streams: [Box::new(Packets {
                rows: rows.into(),
                closed,
            }) as Box<dyn MppStreamResponse>]
            .into(),
            node_count: 2,
        })
    }
    fn memory_error() -> QueryResponseError {
        QueryResponseError::Source("Memory limit exceeded".into())
    }

    #[test]
    fn mpp_failure_batch_discards_held_packets_and_closes_before_reopening() {
        use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
        let closed = Arc::new(AtomicUsize::new(0));
        let old_closed = closed.clone();
        let calls = Arc::new(AtomicUsize::new(0));
        let recovery_calls = calls.clone();
        let memory = crate::StatementMemory::default();
        let mut response = ExecutorWithRetry::new(
            gather(vec![Ok(packet(99)), Err(memory_error())], closed),
            true,
            false,
            memory.clone(),
            Box::new(move || {
                assert_eq!(old_closed.load(SeqCst), 1);
                Ok(gather(
                    vec![Ok(packet(1)), Ok(packet(2))],
                    old_closed.clone(),
                ))
            }),
            Box::new(move |nodes| {
                assert_eq!(nodes, 2);
                recovery_calls.fetch_add(1, SeqCst);
                Ok(())
            }),
        );
        assert_eq!(response.next().unwrap(), Some(packet(1)));
        assert_eq!(memory.bytes_consumed(), 1);
        assert_eq!(response.next().unwrap(), Some(packet(2)));
        assert!(response.next().unwrap().is_none());
        assert_eq!(calls.load(SeqCst), 1);
        assert_eq!(memory.bytes_consumed(), 0);
    }

    #[test]
    fn mpp_failure_batch_never_replays_delivered_packets_or_disabled_recovery() {
        for enabled in [true, false] {
            let mut response = ExecutorWithRetry::new(
                gather(
                    vec![Ok(packet(1)), Ok(packet(2)), Err(memory_error())],
                    Arc::default(),
                ),
                enabled,
                false,
                crate::StatementMemory::default(),
                Box::new(|| panic!("must not replay delivered packets")),
                Box::new(|_| panic!("must not recover after delivery")),
            );
            assert_eq!(response.next().unwrap(), Some(packet(1)));
            assert_eq!(response.next().unwrap(), Some(packet(2)));
            assert_eq!(response.next().unwrap_err(), memory_error());
        }
    }

    #[test]
    fn mpp_failure_batch_three_attempts_and_original_error() {
        use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
        let calls = Arc::new(AtomicUsize::new(0));
        let recoveries = calls.clone();
        let mut response = ExecutorWithRetry::new(
            gather(vec![Err(memory_error())], Arc::default()),
            true,
            false,
            crate::StatementMemory::default(),
            Box::new(|| Ok(gather(vec![Err(memory_error())], Arc::default()))),
            Box::new(move |_| {
                recoveries.fetch_add(1, SeqCst);
                Ok(())
            }),
        );
        assert_eq!(response.next().unwrap_err(), memory_error());
        assert_eq!(calls.load(SeqCst), 3);
        for recovery_fails in [true, false] {
            let memory = crate::StatementMemory::default();
            let mut response = ExecutorWithRetry::new(
                gather(vec![Ok(packet(5)), Err(memory_error())], Arc::default()),
                true,
                false,
                memory.clone(),
                Box::new(|| Err(QueryResponseError::Source("replacement failed".into()))),
                Box::new(move |_| {
                    if recovery_fails {
                        Err("autoscaler failed".into())
                    } else {
                        Ok(())
                    }
                }),
            );
            assert_eq!(response.next().unwrap_err(), memory_error());
            assert_eq!(memory.bytes_consumed(), 0);
        }
    }

    #[test]
    fn mpp_failure_batch_nonrecoverable_errors_and_short_results() {
        for error in [
            QueryResponseError::Cancelled,
            QueryResponseError::Sql {
                code: 1317,
                message: "Memory limit local kill".into(),
            },
            QueryResponseError::Source("memory limit lowercase is unrelated".into()),
        ] {
            let mut response = ExecutorWithRetry::new(
                gather(vec![Err(error.clone())], Arc::default()),
                true,
                false,
                crate::StatementMemory::default(),
                Box::new(|| panic!("terminal")),
                Box::new(|_| panic!("terminal")),
            );
            assert_eq!(response.next().unwrap_err(), error);
        }
        for count in 0..=2 {
            let memory = crate::StatementMemory::default();
            let mut response = ExecutorWithRetry::new(
                gather((0..count).map(|i| Ok(packet(i))).collect(), Arc::default()),
                true,
                false,
                memory.clone(),
                Box::new(|| panic!("no failure")),
                Box::new(|_| panic!("no failure")),
            );
            for i in 0..count {
                assert_eq!(response.next().unwrap(), Some(packet(i)));
            }
            assert!(response.next().unwrap().is_none());
            assert_eq!(memory.bytes_consumed(), 0);
        }
    }

    #[test]
    fn mpp_failure_batch_close_releases_retry_capabilities() {
        let capability = Arc::new(());
        let observer = Arc::downgrade(&capability);
        let mut response = ExecutorWithRetry::new(
            gather(vec![], Arc::default()),
            true,
            false,
            crate::StatementMemory::default(),
            Box::new(move || {
                let _borrow = &capability;
                panic!("no retry")
            }),
            Box::new(|_| panic!("no failure")),
        );
        response.close().unwrap();
        assert!(observer.upgrade().is_none());
        response.close().unwrap();
    }

    struct Packets {
        rows: std::collections::VecDeque<Result<QueryResultSubset, QueryResponseError>>,
        closed: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    }
    impl MppStreamResponse for Packets {
        fn release_packet_memory(&mut self) -> i64 {
            0
        }

        fn packet_error(&self) -> Option<&str> {
            None
        }
    }
    impl tidb_txnkv::Response for Packets {
        fn next(&mut self) -> Result<Option<QueryResultSubset>, QueryResponseError> {
            self.rows.pop_front().transpose()
        }

        fn close(&mut self) -> Result<(), QueryResponseError> {
            self.closed
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }
    }

    /// A gather whose close answers `error`, as Go's coordinator answers
    /// `handleAllReports`'s.
    struct FailingClose {
        error: QueryResponseError,
        closed: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    }
    impl MppStreamResponse for FailingClose {
        fn release_packet_memory(&mut self) -> i64 {
            0
        }

        fn packet_error(&self) -> Option<&str> {
            None
        }
    }
    impl MppGatherResponse for FailingClose {
        fn node_count(&self) -> usize {
            1
        }
    }
    impl tidb_txnkv::Response for FailingClose {
        fn next(&mut self) -> Result<Option<QueryResultSubset>, QueryResponseError> {
            Ok(None)
        }

        fn close(&mut self) -> Result<(), QueryResponseError> {
            self.closed
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Err(self.error.clone())
        }
    }

    /// Go `ExecutorWithRetry.Close` returns `r.coord.Close()`'s error; here
    /// a drained gather is closed by the pull that drained it.
    #[test]
    fn a_gathers_close_error_is_the_responses() {
        use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
        let error = QueryResponseError::Source("report".into());
        let closed = Arc::new(AtomicUsize::new(0));
        let response = || {
            ExecutorWithRetry::new(
                Box::new(FailingClose {
                    error: error.clone(),
                    closed: closed.clone(),
                }),
                true,
                false,
                crate::StatementMemory::default(),
                Box::new(|| panic!("no retry")),
                Box::new(|_| panic!("no failure")),
            )
        };
        let mut abandoned = response();
        assert_eq!(abandoned.close(), Err(error.clone()));
        assert_eq!(abandoned.close(), Ok(()));
        assert_eq!(closed.load(SeqCst), 1);
        let mut drained = response();
        assert_eq!(drained.next(), Err(error.clone()));
        assert_eq!(drained.close(), Ok(()));
        assert_eq!(closed.load(SeqCst), 2);

        // Every task's stream is closed, and the first error is the close's.
        let failing = |message: &str| {
            Box::new(FailingClose {
                error: QueryResponseError::Source(message.into()),
                closed: closed.clone(),
            }) as Box<dyn MppStreamResponse>
        };
        let mut streams = MppTaskResponses {
            streams: [
                Box::new(Packets {
                    rows: [].into(),
                    closed: closed.clone(),
                }) as Box<dyn MppStreamResponse>,
                failing("first"),
                failing("second"),
            ]
            .into(),
            node_count: 3,
        };
        assert_eq!(streams.close(), Err(QueryResponseError::Source("first".into())));
        assert_eq!(closed.load(SeqCst), 5);
        let mut streams = MppTaskResponses {
            streams: [failing("drained")].into(),
            node_count: 1,
        };
        assert_eq!(streams.next(), Err(QueryResponseError::Source("drained".into())));
    }

    #[test]
    fn compute_topology_batch_fanout_error_closes_all_tasks() {
        let closed = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut streams = MppTaskResponses {
            streams: [
                Box::new(Packets {
                    rows: [Ok(packet(1))].into(),
                    closed: closed.clone(),
                }) as Box<dyn MppStreamResponse>,
                Box::new(Packets {
                    rows: [Err(QueryResponseError::Source("failed".into()))].into(),
                    closed: closed.clone(),
                }),
                Box::new(Packets {
                    rows: [Ok(packet(3))].into(),
                    closed: closed.clone(),
                }),
            ]
            .into(),
            node_count: 3,
        };
        assert_eq!(streams.next().unwrap(), Some(packet(1)));
        assert!(streams.next().is_err());
        assert_eq!(closed.load(std::sync::atomic::Ordering::SeqCst), 3);
        streams.close().unwrap();
        assert_eq!(closed.load(std::sync::atomic::Ordering::SeqCst), 3);
    }
}
