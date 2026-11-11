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

//! Go `pkg/executor/internal/mpp/recovery_handler.go`: the recovery of MPP
//! errors, and the holder of the responses a recovery may still discard.

use std::collections::VecDeque;

use tidb_txnkv::{QueryResponseError, QueryResultSubset};

use crate::StatementMemory;

/// Go `memLimitErrPattern`.
const MEM_LIMIT_ERR_PATTERN: &str = "Memory limit";

/// Go `memLimitHandlerImpl.doRecovery`'s topology call
/// (`RecoveryAndGetTopo(RecoveryTypeMemLimit, info.NodeCnt)`).
pub type MppRecover = Box<dyn FnMut(usize) -> Result<(), String> + Send>;

/// Go `RecoveryHandler`: tries to recovery mpp error.
pub struct RecoveryHandler {
    holder: MppResultHolder,
    handlers: Vec<Box<dyn HandlerImpl>>,
    max_recovery_cnt: u32,
    cur_recovery_cnt: u32,
    enable: bool,
}

/// Go `RecoveryInfo`: info that can help recovery error.
pub struct RecoveryInfo<'a> {
    /// Go `MPPErr`.
    pub mpp_err: &'a QueryResponseError,
    /// Nodes that involved into MPP computation.
    pub node_cnt: usize,
}

impl RecoveryHandler {
    /// Go `NewRecoveryHandler`.
    #[must_use]
    pub fn new(
        use_auto_scaler: bool,
        holder_cap: usize,
        enable: bool,
        parent: StatementMemory,
        recover: MppRecover,
    ) -> Self {
        Self {
            enable,
            handlers: vec![Box::new(MemLimitHandlerImpl {
                use_auto_scaler,
                recover,
            })],
            holder: MppResultHolder::new(holder_cap, parent),
            // Default recovery 3 time.
            max_recovery_cnt: 3,
            cur_recovery_cnt: 0,
        }
    }

    /// Go `Enabled`: true when mpp err recovery enabled.
    #[must_use]
    pub fn enabled(&self) -> bool {
        self.enable
    }

    /// Go `CanHoldResult`: whether we can insert intermediate results.
    #[must_use]
    pub fn can_hold_result(&self) -> bool {
        self.holder.capacity > 0 && !self.holder.cannot_hold
    }

    /// Go `HoldResult`: holds one mpp result. Check [`Self::enabled`] and
    /// [`Self::can_hold_result`] first.
    pub fn hold_result(&mut self, resp: QueryResultSubset) {
        self.holder.insert(resp);
    }

    /// Go `NumHoldResp`: the number of resp holded.
    #[must_use]
    pub fn num_hold_resp(&self) -> usize {
        self.holder.resps.len()
    }

    /// Go `PopFrontResp`: pop one resp.
    pub fn pop_front_resp(&mut self) -> Result<QueryResultSubset, String> {
        let pop_failed = format!(
            "pop resp failed. enable: {}, size: {}",
            self.enable,
            self.holder.resps.len()
        );
        if !self.enable {
            return Err(pop_failed);
        }
        let resp = self.holder.resps.pop_front().ok_or(pop_failed)?;
        self.holder
            .mem_tracker
            .stmt_tracker()
            .consume(-mem_size(&resp));
        self.holder.cannot_hold = true;
        Ok(resp)
    }

    /// Go `ResetHolder`: resets the dynamic data, like resps. Will not touch
    /// other metadata, like enable.
    pub fn reset_holder(&mut self) {
        self.holder.reset();
    }

    /// Go `RecoveryCnt`.
    #[must_use]
    pub fn recovery_cnt(&self) -> u32 {
        self.cur_recovery_cnt
    }

    /// Go `Recovery`: tries to recovery error. Reasons that cannot recovery:
    ///  1. Already return result to client because holder is full.
    ///  2. Recovery method of this kind of error not implemented or error is
    ///     not recoveryable.
    ///  3. Retry time exceeds maxRecoveryCnt.
    pub fn recovery(&mut self, info: &RecoveryInfo<'_>) -> Result<(), String> {
        if !self.enable {
            return Err("mpp err recovery is not enabled".to_owned());
        }

        if self.cur_recovery_cnt >= self.max_recovery_cnt {
            return Err(format!(
                "exceeds max recovery cnt: cur: {}, max: {}",
                self.cur_recovery_cnt, self.max_recovery_cnt
            ));
        }

        self.cur_recovery_cnt += 1;

        for h in &mut self.handlers {
            if h.choose_handler_impl(info.mpp_err) {
                return h.do_recovery(info);
            }
        }
        Err("no handler to recovery this type of mpp err".to_owned())
    }
}

/// Go `handlerImpl`.
trait HandlerImpl: Send {
    fn choose_handler_impl(&self, mpp_err: &QueryResponseError) -> bool;
    fn do_recovery(&mut self, info: &RecoveryInfo<'_>) -> Result<(), String>;
}

/// Go `memLimitHandlerImpl`.
struct MemLimitHandlerImpl {
    use_auto_scaler: bool,
    recover: MppRecover,
}

impl HandlerImpl for MemLimitHandlerImpl {
    fn choose_handler_impl(&self, mpp_err: &QueryResponseError) -> bool {
        // A local quota or KILL error is the statement's own, even when its
        // text holds the pattern: only TiFlash's error is a source error.
        matches!(mpp_err, QueryResponseError::Source(message)
            if message.contains(MEM_LIMIT_ERR_PATTERN))
            && self.use_auto_scaler
    }

    fn do_recovery(&mut self, info: &RecoveryInfo<'_>) -> Result<(), String> {
        // Ignore fetched topo, because AutoScaler will keep the topo for a while.
        // And the new topo will be fetched when dispatch mpp task again.
        (self.recover)(info.node_cnt)
    }
}

/// Go `mppResponse.MemSize` as the holder charges it: the packet's payload.
fn mem_size(resp: &QueryResultSubset) -> i64 {
    resp.data.len() as i64
}

/// Go `mppResultHolder`.
struct MppResultHolder {
    mem_tracker: StatementMemory,
    resps: VecDeque<QueryResultSubset>,
    capacity: usize,
    cannot_hold: bool,
}

impl MppResultHolder {
    /// Go `newMPPResultHolder`.
    fn new(holder_cap: usize, parent: StatementMemory) -> Self {
        Self {
            capacity: holder_cap,
            resps: VecDeque::with_capacity(holder_cap),
            mem_tracker: parent,
            cannot_hold: false,
        }
    }

    /// Go `mppResultHolder.insert`.
    fn insert(&mut self, resp: QueryResultSubset) {
        self.mem_tracker.stmt_tracker().consume(mem_size(&resp));
        self.resps.push_back(resp);

        // TODO: Better use row number as threshold. Need to add row number info in tipb.MPPDataPacket.
        if self.resps.len() >= self.capacity {
            self.cannot_hold = true;
        }
    }

    /// Go `mppResultHolder.reset`: its tracker's detach is the release of
    /// what the held responses charged.
    fn reset(&mut self) {
        self.cannot_hold = false;
        for resp in self.resps.drain(..) {
            self.mem_tracker.stmt_tracker().consume(-mem_size(&resp));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn packet(value: u8) -> QueryResultSubset {
        QueryResultSubset {
            data: vec![value].into(),
            runtime: None,
        }
    }

    fn handler(use_auto_scaler: bool) -> RecoveryHandler {
        RecoveryHandler::new(
            use_auto_scaler,
            4,
            true,
            StatementMemory::default(),
            Box::new(|_| Ok(())),
        )
    }

    #[test]
    fn a_popped_response_ends_holding_until_the_holder_resets_like_go() {
        let mut handler = handler(true);
        handler.hold_result(packet(1));
        handler.hold_result(packet(2));
        assert!(handler.can_hold_result());
        assert_eq!(handler.pop_front_resp().unwrap(), packet(1));
        assert!(!handler.can_hold_result());
        assert_eq!(handler.num_hold_resp(), 1);
        handler.reset_holder();
        assert!(handler.can_hold_result());
        assert_eq!(handler.num_hold_resp(), 0);
        assert_eq!(
            handler.pop_front_resp().unwrap_err(),
            "pop resp failed. enable: true, size: 0"
        );
    }

    #[test]
    fn only_an_auto_scaler_recovers_a_memory_limit_error_like_go() {
        let mpp_err = QueryResponseError::Source("Memory limit exceeded".into());
        let info = RecoveryInfo {
            mpp_err: &mpp_err,
            node_cnt: 2,
        };
        assert_eq!(handler(true).recovery(&info), Ok(()));
        let mut handler = handler(false);
        assert_eq!(
            handler.recovery(&info).unwrap_err(),
            "no handler to recovery this type of mpp err"
        );
        // A refused recovery is counted as one all the same.
        assert_eq!(handler.recovery_cnt(), 1);
    }
}
