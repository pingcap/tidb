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

//! boundary: client-go/v2 `util.RUDetails`.
//!
//! `stmtstats` only ever reads five accessors off the shared `*util.RUDetails`
//! (`RRU`, `WRU`, `RUWaitDuration`, `TiKVRUV2`, `TiflashRU`) and never
//! constructs one in production, so the client-go type is recovered here as a
//! small local struct with the same accessors and the same additive `Merge`.
//! `tidb-exec` carries the identical narrowing as
//! `slow_log_format::RuDetailsSnapshot`; this crate sits below `tidb-exec` and
//! therefore keeps its own copy.
//!
//! client-go keeps the fields in atomics because the details are shared between
//! the executing session and the RPC layer. The same sharing happens here (the
//! session holds an `Arc` while the aggregator samples it from its tick
//! thread), so the fields live behind one `Mutex` — the values are read and
//! written as a group, never individually.

use std::sync::Mutex;
use std::time::Duration;

/// boundary: client-go/v2 `util.RUDetails`.
#[derive(Debug, Default)]
pub struct RuDetails {
    inner: Mutex<RuDetailsValues>,
}

/// The plain-value view of [`RuDetails`], mirroring its five accessors.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
struct RuDetailsValues {
    rru: f64,
    wru: f64,
    ru_wait_duration: Duration,
    tikv_ru_v2: f64,
    tiflash_ru: f64,
}

impl RuDetails {
    /// client-go `NewRUDetails`: an all-zero counter set.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// client-go `NewRUDetailsWith`.
    #[must_use]
    pub fn new_with(rru: f64, wru: f64, ru_wait_duration: Duration) -> Self {
        Self {
            inner: Mutex::new(RuDetailsValues {
                rru,
                wru,
                ru_wait_duration,
                ..RuDetailsValues::default()
            }),
        }
    }

    fn values(&self) -> RuDetailsValues {
        *self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// client-go `RUDetails.RRU`.
    #[must_use]
    pub fn rru(&self) -> f64 {
        self.values().rru
    }

    /// client-go `RUDetails.WRU`.
    #[must_use]
    pub fn wru(&self) -> f64 {
        self.values().wru
    }

    /// client-go `RUDetails.RUWaitDuration`.
    #[must_use]
    pub fn ru_wait_duration(&self) -> Duration {
        self.values().ru_wait_duration
    }

    /// client-go `RUDetails.TiKVRUV2`.
    #[must_use]
    pub fn tikv_ru_v2(&self) -> f64 {
        self.values().tikv_ru_v2
    }

    /// client-go `RUDetails.TiflashRU`.
    #[must_use]
    pub fn tiflash_ru(&self) -> f64 {
        self.values().tiflash_ru
    }

    /// client-go `RUDetails.AddTiKVRUV2`.
    pub fn add_tikv_ru_v2(&self, delta: f64) {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .tikv_ru_v2 += delta;
    }

    /// client-go `RUDetails.AddTiflashRU`.
    pub fn add_tiflash_ru(&self, delta: f64) {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .tiflash_ru += delta;
    }

    /// client-go `RUDetails.Merge`: the additive merge over every field.
    pub fn merge(&self, other: &RuDetails) {
        let src = other.values();
        let mut dst = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        dst.rru += src.rru;
        dst.wru += src.wru;
        dst.ru_wait_duration += src.ru_wait_duration;
        dst.tikv_ru_v2 += src.tikv_ru_v2;
        dst.tiflash_ru += src.tiflash_ru;
    }
}
