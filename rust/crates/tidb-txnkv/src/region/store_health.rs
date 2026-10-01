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

use std::sync::Arc;
use std::time::Duration;

pub use std::time::Instant as HealthInstant;
pub use tikv_client::tikv::{
    HealthStatusDetail as StoreHealthDetail, StoreHealthStatus as StoreHealth,
    StoreLoadStats as StoreLoad,
};

/// Store-owned native health and the current queue-estimate observation.
/// Topology copies retain the same health object, as client-go stores do.
#[derive(Clone, Debug, Default)]
pub struct StoreRoutingHealth {
    /// Decaying server load estimate.
    pub load: StoreLoad,
    /// Client and TiKV slow scores shared with outstanding store views.
    pub health: Arc<StoreHealth>,
}

impl PartialEq for StoreRoutingHealth {
    fn eq(&self, other: &Self) -> bool {
        // Metadata comparisons must not change as asynchronous health samples
        // arrive. Clones preserve identity; a new store gets a new owner.
        self.load == other.load && Arc::ptr_eq(&self.health, &other.health)
    }
}

impl Eq for StoreRoutingHealth {}

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
