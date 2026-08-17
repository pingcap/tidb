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

//! Go `MockPD.GetTS` / the package-level `GetTS`
//! (`tikv/mock_region.go:887-911`): the embedded timestamp oracle.
//!
//! The algorithm is Go's exactly: the physical half is wall-clock
//! milliseconds; a call landing in the same-or-an-earlier millisecond bumps
//! the logical half, a later one resets it. Composition into one `u64` is
//! `physical << 18 | logical` — the same 18-bit split
//! [`crate::mvcc_store`]'s `extract_physical` reads back.
//!
//! Go quirk, kept: nothing guards the logical half overflowing its 18 bits
//! (262,144 timestamps inside one frozen millisecond would bleed into the
//! physical half). Go has no guard, so neither does this.

use std::sync::Mutex;

/// Go `physicalShiftBits`: the TSO layout's logical width.
pub const PHYSICAL_SHIFT_BITS: u32 = 18;

/// Compose a TSO from its halves, client-go's `oracle.ComposeTS`.
#[must_use]
pub const fn compose_ts(physical: i64, logical: i64) -> u64 {
    ((physical as u64) << PHYSICAL_SHIFT_BITS) | (logical as u64)
}

/// Go `tsMu` + `GetTS` (`mock_region.go:887-911`).
#[derive(Debug)]
pub struct Tso {
    state: Mutex<TsoState>,
    /// The clock, injectable exactly as Go's tests freeze time elsewhere;
    /// production construction reads the system clock.
    now_ms: fn() -> i64,
}

#[derive(Debug, Default)]
struct TsoState {
    physical_ts: i64,
    logical_ts: i64,
}

fn system_now_ms() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("the clock sits after the epoch")
            .as_millis(),
    )
    .expect("milliseconds fit i64 for the next 292 million years")
}

impl Default for Tso {
    fn default() -> Self {
        Self::new()
    }
}

impl Tso {
    /// The production oracle over the system clock.
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: Mutex::new(TsoState::default()),
            now_ms: system_now_ms,
        }
    }

    /// A test oracle over a supplied clock.
    #[must_use]
    pub fn with_clock(now_ms: fn() -> i64) -> Self {
        Self {
            state: Mutex::new(TsoState::default()),
            now_ms,
        }
    }

    /// Go `GetTS()`: `(physical, logical)`, strictly monotonic.
    pub fn get_ts(&self) -> (i64, i64) {
        let mut state = self.state.lock().expect("the oracle lock");
        let ts = (self.now_ms)();
        if state.physical_ts >= ts {
            state.logical_ts += 1;
        } else {
            state.physical_ts = ts;
            state.logical_ts = 0;
        }
        (state.physical_ts, state.logical_ts)
    }

    /// The composed form every kvrpc request carries.
    pub fn get_composed_ts(&self) -> u64 {
        let (physical, logical) = self.get_ts();
        compose_ts(physical, logical)
    }
}

/// The [`Tso`] as the lock resolver's timestamp authority: every call a
/// fresh real TSO, which is exactly the trait's admission rule — the oracle
/// can never repeat or synthesize, by construction.
impl tidb_txnkv::lock::TimestampSource for Tso {
    fn current_ts(&self) -> Result<u64, String> {
        Ok(self.get_composed_ts())
    }
}

/// Go `MockPD` as the coordinator's PD surface: the embedded oracle behind
/// [`tidb_txnkv::pd_capability::PdCapability`]. Cluster identity is the
/// bootstrap cluster; a timestamp future is already answered, because the
/// oracle dispatches nothing; and the GC safe point is ZERO — the embedded
/// store never garbage-collects, so no read floor ever rises, which is
/// Go's own unistore behavior for a store whose safe-point loop has no PD
/// to ask.
#[derive(Clone, Debug, Default)]
pub struct InProcessPd {
    tso: std::sync::Arc<Tso>,
}

impl InProcessPd {
    /// A PD surface over a fresh oracle.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The oracle beneath, shared.
    #[must_use]
    pub fn oracle(&self) -> std::sync::Arc<Tso> {
        std::sync::Arc::clone(&self.tso)
    }
}

impl tidb_txnkv::pd_capability::PdCapability for InProcessPd {
    type TsFuture = tidb_txnkv::pd_capability::ReadyTimestamp;

    fn cluster_id(&self) -> u64 {
        crate::region_loader::IN_PROCESS_CLUSTER_ID
    }

    fn timestamp_future(&self) -> Result<Self::TsFuture, String> {
        Ok(tidb_txnkv::pd_capability::ReadyTimestamp(
            self.tso.get_composed_ts(),
        ))
    }

    fn gc_safe_point(&self) -> Result<u64, String> {
        Ok(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // All WRITTEN: Go's oracle is exercised through the store suites.

    #[test]
    fn a_frozen_clock_bumps_the_logical_half() {
        // Go: `tsMu.physicalTS >= ts` → logical++.
        let tso = Tso::with_clock(|| 1_000);
        assert_eq!(tso.get_ts(), (1_000, 0));
        assert_eq!(tso.get_ts(), (1_000, 1));
        assert_eq!(tso.get_ts(), (1_000, 2));
    }

    #[test]
    fn composed_timestamps_are_strictly_monotonic() {
        let tso = Tso::with_clock(|| 7);
        let mut last = 0;
        for _ in 0..1_000 {
            let ts = tso.get_composed_ts();
            assert!(ts > last);
            last = ts;
        }
    }

    #[test]
    fn the_physical_half_reads_back_through_the_stores_extractor() {
        // The 18-bit split is the SAME one `extract_physical` (mvcc_store)
        // and Go's `oracle.ExtractPhysical` read.
        let ts = compose_ts(1_234, 56);
        assert_eq!(ts >> PHYSICAL_SHIFT_BITS, 1_234);
        assert_eq!(ts & ((1 << PHYSICAL_SHIFT_BITS) - 1), 56);
    }

    #[test]
    fn a_moving_clock_resets_the_logical_half() {
        use std::sync::atomic::{AtomicI64, Ordering};
        static CLOCK: AtomicI64 = AtomicI64::new(100);
        let tso = Tso::with_clock(|| CLOCK.load(Ordering::SeqCst));
        assert_eq!(tso.get_ts(), (100, 0));
        assert_eq!(tso.get_ts(), (100, 1));
        CLOCK.store(200, Ordering::SeqCst);
        assert_eq!(tso.get_ts(), (200, 0), "a later millisecond resets");
        CLOCK.store(150, Ordering::SeqCst);
        assert_eq!(tso.get_ts(), (200, 1), "a BACKWARD clock only bumps");
    }

    #[test]
    fn the_oracle_serves_the_resolvers_timestamp_trait() {
        use tidb_txnkv::lock::TimestampSource;
        let tso = Tso::with_clock(|| 9);
        let first = tso.current_ts().expect("a ts");
        let second = tso.current_ts().expect("a ts");
        assert!(second > first, "fresh on every call, never repeated");
    }

    #[test]
    fn the_embedded_pd_serves_the_coordinators_capability() {
        use tidb_txnkv::pd_capability::{PdCapability, TimestampFutureWait};
        let pd = InProcessPd::new();
        assert_eq!(pd.cluster_id(), crate::region_loader::IN_PROCESS_CLUSTER_ID);
        let first = pd
            .timestamp_future()
            .expect("dispatches")
            .wait()
            .expect("ts");
        let second = pd
            .timestamp_future()
            .expect("dispatches")
            .wait()
            .expect("ts");
        assert!(
            second > first,
            "the oracle stays monotonic through the seam"
        );
        assert_eq!(pd.gc_safe_point(), Ok(0), "no read floor ever rises");
    }
}
