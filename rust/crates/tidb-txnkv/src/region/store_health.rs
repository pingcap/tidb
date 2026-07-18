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

use std::time::Duration;

use super::slow_score::SlowScoreStat;

const TIKV_SLOW_THRESHOLD: i64 = 80;
const TIKV_UPDATE_INTERVAL: Duration = Duration::from_millis(100);
const TIKV_ACTIVE_UPDATE_INTERVAL: Duration = Duration::from_secs(15);
const TIKV_DECAY_PER_SECOND: f64 = 20.0 / 60.0;

/// Monotonic instant injected by RegionCache and source tests.
pub type HealthInstant = Duration;

/// Optimistic TiKV queue estimate with linear wall-clock decay.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct StoreLoad {
    estimated_wait: Duration,
    updated_at: HealthInstant,
}

impl StoreLoad {
    /// Publishes a positive `EstimatedWaitMs` observation.
    pub fn update(&mut self, estimated_wait: Duration, now: HealthInstant) {
        self.estimated_wait = estimated_wait;
        self.updated_at = now;
    }

    /// Remaining optimistic wait after subtracting elapsed time.
    #[must_use]
    pub fn estimated_wait(&self, now: HealthInstant) -> Duration {
        self.estimated_wait
            .saturating_sub(now.saturating_sub(self.updated_at))
    }
}

/// Stable, immutable health detail consumed by the pure selector.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct StoreHealthDetail {
    /// Client-observed slow score.
    pub client_side_slow_score: u64,
    /// TiKV-pushed slow score.
    pub tikv_side_slow_score: i64,
}

impl StoreHealthDetail {
    /// Either source reaching 80 makes the store slow.
    #[must_use]
    pub const fn is_slow(self) -> bool {
        self.client_side_slow_score >= 80 || self.tikv_side_slow_score >= TIKV_SLOW_THRESHOLD
    }
}

/// RegionCache-owned client and TiKV health state for one store.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct StoreHealth {
    client: SlowScoreStat,
    has_tikv_feedback: bool,
    tikv_score: i64,
    tikv_updated_at: Option<HealthInstant>,
}

impl StoreHealth {
    /// Current immutable health detail.
    #[must_use]
    pub const fn detail(&self) -> StoreHealthDetail {
        StoreHealthDetail {
            client_side_slow_score: self.client.score(),
            tikv_side_slow_score: self.tikv_score,
        }
    }

    /// Whether either client or TiKV considers this store slow.
    #[must_use]
    pub const fn is_slow(&self) -> bool {
        self.client.is_slow() || self.tikv_score >= TIKV_SLOW_THRESHOLD
    }

    /// Records one client-observed request duration.
    pub fn record_client_duration(&mut self, duration: Duration) {
        self.client.record(duration);
    }

    /// A busy response without an estimate immediately marks the client score.
    pub fn mark_already_slow(&mut self) {
        self.client.mark_already_slow();
    }

    /// Applies one TiKV health feedback observation.
    ///
    /// Different scores are rate-limited to 100 ms. An unchanged score above
    /// one still refreshes its timestamp, exactly as pinned client-go does.
    #[must_use]
    pub fn update_tikv_score(&mut self, score: i64, now: HealthInstant) -> bool {
        if self.tikv_score == score {
            if score > 1 {
                self.tikv_updated_at = Some(now);
                return true;
            }
            return false;
        }
        if self
            .tikv_updated_at
            .is_some_and(|last| now.saturating_sub(last) < TIKV_UPDATE_INTERVAL)
        {
            return false;
        }
        self.has_tikv_feedback = true;
        self.tikv_score = score;
        self.tikv_updated_at = Some(now);
        true
    }

    /// Advances client trend and TiKV's inactive-feedback linear decay.
    pub fn tick(&mut self, now: HealthInstant) {
        self.client.tick();
        if !self.has_tikv_feedback || self.tikv_score <= 1 {
            return;
        }
        let Some(last) = self.tikv_updated_at else {
            return;
        };
        let elapsed = now.saturating_sub(last);
        if elapsed < TIKV_ACTIVE_UPDATE_INTERVAL {
            return;
        }
        self.tikv_score = ((self.tikv_score as f64 - TIKV_DECAY_PER_SECOND * elapsed.as_secs_f64())
            .round() as i64)
            .max(1);
        self.tikv_updated_at = Some(now);
    }
}

/// Mutable routing health kept inside the canonical store authority.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct StoreRoutingHealth {
    /// Decaying server load estimate.
    pub load: StoreLoad,
    /// Client and TiKV slow scores.
    pub health: StoreHealth,
}

impl StoreRoutingHealth {
    /// Applies the store-owned half of one `ServerIsBusy` response.
    pub fn observe_server_busy(&mut self, estimated_wait_ms: u32, now: HealthInstant) {
        if estimated_wait_ms == 0 {
            self.health.mark_already_slow();
        } else {
            self.load
                .update(Duration::from_millis(u64::from(estimated_wait_ms)), now);
        }
    }
}
