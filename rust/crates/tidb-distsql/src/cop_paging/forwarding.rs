// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Request-local unary forwarding and network observations.
//!
//! This leaf never owns proxy preference or store health. It projects one
//! immutable cache-selected route into transport inputs and turns the result
//! back into typed feedback for the canonical region cache.

use tidb_txnkv::region::{LeaderRequest, RouteFeedback, RouteOutcome};

/// Physical/logical addresses projected from one immutable route decision.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct UnaryRouteDispatch<'a> {
    physical_address: &'a str,
    forwarded_host: Option<&'a str>,
}

impl<'a> UnaryRouteDispatch<'a> {
    pub(crate) fn from_request(request: &'a LeaderRequest) -> Self {
        Self {
            physical_address: request.dispatch_address(),
            forwarded_host: request.forwarded_host(),
        }
    }

    pub(crate) const fn physical_address(self) -> &'a str {
        self.physical_address
    }

    pub(crate) const fn forwarded_host(self) -> Option<&'a str> {
        self.forwarded_host
    }

    pub(crate) fn feedback(self, request: &LeaderRequest, outcome: RouteOutcome) -> RouteFeedback {
        debug_assert_eq!(self.physical_address, request.dispatch_address());
        debug_assert_eq!(self.forwarded_host, request.forwarded_host());
        RouteFeedback::from_request(request, outcome)
    }
}

/// Store locality used by client-go's byte counters.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum UnaryTrafficLocation {
    /// No zone comparison was available at the observation boundary.
    #[default]
    Unknown,
    /// Target store matches the configured local zone.
    Local,
    /// Target store differs from the configured local zone.
    CrossZone,
}

impl UnaryTrafficLocation {
    /// Projects the topology label comparison without inventing locality when
    /// no comparison is available.
    pub(crate) const fn from_cross_zone(cross_zone: Option<bool>) -> Self {
        match cross_zone {
            Some(false) => Self::Local,
            Some(true) => Self::CrossZone,
            None => Self::Unknown,
        }
    }
}

/// Request-local equivalent of client-go's network collector counters.
///
/// The values are deliberately plain integers. Publication into a process
/// metrics registry belongs above this retry-local owner, avoiding global
/// mutable counters in deterministic source tests.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct UnaryNetworkMetrics {
    /// All encoded TiKV request bytes.
    pub unpacked_bytes_sent_kv_total: u64,
    /// Encoded request bytes sent across zones.
    pub unpacked_bytes_sent_kv_cross_zone: u64,
    /// All encoded TiKV response bytes.
    pub unpacked_bytes_received_kv_total: u64,
    /// Encoded response bytes received across zones.
    pub unpacked_bytes_received_kv_cross_zone: u64,
    /// Stale-read request bytes sent to local stores.
    pub stale_read_local_out_bytes: u64,
    /// Stale-read request bytes sent across zones.
    pub stale_read_remote_out_bytes: u64,
    /// Stale-read response bytes received from local stores.
    pub stale_read_local_in_bytes: u64,
    /// Stale-read response bytes received across zones.
    pub stale_read_remote_in_bytes: u64,
    /// Stale-read attempts sent to local stores.
    pub stale_read_local_requests: u64,
    /// Stale-read attempts sent across zones.
    pub stale_read_cross_zone_requests: u64,
    /// Successful stale reads.
    pub stale_read_hits: u64,
    /// Stale reads which returned DataIsNotReady before fallback.
    pub stale_read_misses: u64,
    /// Local response plus request bytes served by a leader.
    pub read_leader_local_bytes: u64,
    /// Cross-zone response plus request bytes served by a leader.
    pub read_leader_remote_bytes: u64,
    /// Local response plus request bytes served by a replica read.
    pub read_follower_local_bytes: u64,
    /// Cross-zone response plus request bytes served by a replica read.
    pub read_follower_remote_bytes: u64,
}

impl UnaryNetworkMetrics {
    pub(crate) fn on_request(
        &mut self,
        bytes: usize,
        stale_read: bool,
        location: UnaryTrafficLocation,
    ) {
        let bytes = u64::try_from(bytes).unwrap_or(u64::MAX);
        if bytes == 0 {
            return;
        }
        self.unpacked_bytes_sent_kv_total = self.unpacked_bytes_sent_kv_total.saturating_add(bytes);
        if location == UnaryTrafficLocation::CrossZone {
            self.unpacked_bytes_sent_kv_cross_zone =
                self.unpacked_bytes_sent_kv_cross_zone.saturating_add(bytes);
        }
        if !stale_read {
            return;
        }
        match location {
            UnaryTrafficLocation::Local => {
                self.stale_read_local_out_bytes =
                    self.stale_read_local_out_bytes.saturating_add(bytes);
                self.stale_read_local_requests = self.stale_read_local_requests.saturating_add(1);
            }
            UnaryTrafficLocation::CrossZone => {
                self.stale_read_remote_out_bytes =
                    self.stale_read_remote_out_bytes.saturating_add(bytes);
                self.stale_read_cross_zone_requests =
                    self.stale_read_cross_zone_requests.saturating_add(1);
            }
            UnaryTrafficLocation::Unknown => {}
        }
    }

    pub(crate) fn on_response(
        &mut self,
        request_bytes: usize,
        response_bytes: usize,
        replica_read: bool,
        stale_read: bool,
        location: UnaryTrafficLocation,
    ) {
        let response_bytes = u64::try_from(response_bytes).unwrap_or(u64::MAX);
        if response_bytes == 0 {
            return;
        }
        self.unpacked_bytes_received_kv_total = self
            .unpacked_bytes_received_kv_total
            .saturating_add(response_bytes);
        if location == UnaryTrafficLocation::CrossZone {
            self.unpacked_bytes_received_kv_cross_zone = self
                .unpacked_bytes_received_kv_cross_zone
                .saturating_add(response_bytes);
        }
        if stale_read {
            match location {
                UnaryTrafficLocation::Local => {
                    self.stale_read_local_in_bytes = self
                        .stale_read_local_in_bytes
                        .saturating_add(response_bytes);
                }
                UnaryTrafficLocation::CrossZone => {
                    self.stale_read_remote_in_bytes = self
                        .stale_read_remote_in_bytes
                        .saturating_add(response_bytes);
                }
                UnaryTrafficLocation::Unknown => {}
            }
        }
        let total = response_bytes.saturating_add(u64::try_from(request_bytes).unwrap_or(u64::MAX));
        match (location, replica_read) {
            (UnaryTrafficLocation::Local, false) => {
                self.read_leader_local_bytes = self.read_leader_local_bytes.saturating_add(total);
            }
            (UnaryTrafficLocation::CrossZone, false) => {
                self.read_leader_remote_bytes = self.read_leader_remote_bytes.saturating_add(total);
            }
            (UnaryTrafficLocation::Local, true) => {
                self.read_follower_local_bytes =
                    self.read_follower_local_bytes.saturating_add(total);
            }
            (UnaryTrafficLocation::CrossZone, true) => {
                self.read_follower_remote_bytes =
                    self.read_follower_remote_bytes.saturating_add(total);
            }
            (UnaryTrafficLocation::Unknown, _) => {}
        }
    }

    pub(crate) fn on_stale_read_result(&mut self, hit: bool) {
        if hit {
            self.stale_read_hits = self.stale_read_hits.saturating_add(1);
        } else {
            self.stale_read_misses = self.stale_read_misses.saturating_add(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{UnaryNetworkMetrics, UnaryTrafficLocation};

    #[test]
    fn source_network_request_and_response_rows_accumulate_exact_bytes() {
        let mut metrics = UnaryNetworkMetrics::default();
        metrics.on_request(10, false, UnaryTrafficLocation::Local);
        metrics.on_request(10, true, UnaryTrafficLocation::Local);
        assert_eq!(metrics.unpacked_bytes_sent_kv_total, 20);
        assert_eq!(metrics.unpacked_bytes_sent_kv_cross_zone, 0);
        assert_eq!(metrics.stale_read_local_out_bytes, 10);
        assert_eq!(metrics.stale_read_local_requests, 1);

        metrics.on_response(3, 7, false, false, UnaryTrafficLocation::Local);
        metrics.on_response(7, 13, true, true, UnaryTrafficLocation::Local);
        assert_eq!(metrics.unpacked_bytes_received_kv_total, 20);
        assert_eq!(metrics.unpacked_bytes_received_kv_cross_zone, 0);
        assert_eq!(metrics.stale_read_local_in_bytes, 13);
        assert_eq!(metrics.read_leader_local_bytes, 10);
        assert_eq!(metrics.read_follower_local_bytes, 20);
    }

    #[test]
    fn stale_read_hit_and_local_then_remote_miss_are_distinct() {
        let mut hit = UnaryNetworkMetrics::default();
        hit.on_request(8, true, UnaryTrafficLocation::Local);
        hit.on_response(8, 5, false, true, UnaryTrafficLocation::Local);
        hit.on_stale_read_result(true);
        assert_eq!(hit.stale_read_hits, 1);
        assert_eq!(hit.stale_read_misses, 0);
        assert!(hit.stale_read_local_in_bytes > 0);
        assert_eq!(hit.stale_read_remote_in_bytes, 0);

        let mut miss = UnaryNetworkMetrics::default();
        miss.on_request(8, true, UnaryTrafficLocation::Local);
        miss.on_response(8, 5, false, true, UnaryTrafficLocation::Local);
        miss.on_stale_read_result(false);
        miss.on_request(8, true, UnaryTrafficLocation::CrossZone);
        miss.on_response(8, 5, true, true, UnaryTrafficLocation::CrossZone);
        assert_eq!(miss.stale_read_hits, 0);
        assert_eq!(miss.stale_read_misses, 1);
        assert!(miss.stale_read_local_in_bytes > 0);
        assert!(miss.stale_read_remote_in_bytes > 0);
    }
}
