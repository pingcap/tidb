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

//! The PD capabilities the transaction coordinator actually consumes,
//! as a seam.
//!
//! Go's architecture has ONE `kv.Storage` beneath a store-agnostic stack;
//! this port's transaction coordinator instead named the concrete
//! `PdClient` directly, which is the one divergence blocking a storeless
//! node. This trait is the coordinator's actual PD surface — cluster
//! identity, a timestamp future, and the GC safe point — so the real
//! client and an embedded oracle can stand in the same slot, exactly as
//! Go's unistore hands its `MockPD` to the same client-go machinery.

use tidb_pd_client::{PdClient, PdTimestampFuture};

/// A dispatched timestamp request that can be waited on once.
pub trait TimestampFutureWait {
    /// Waits for the dispatched request's TSO.
    fn wait(self) -> Result<u64, String>;
}

impl TimestampFutureWait for PdTimestampFuture {
    fn wait(self) -> Result<u64, String> {
        PdTimestampFuture::wait(self).map_err(|error| error.to_string())
    }
}

/// An already-answered timestamp: the embedded oracle dispatches nothing,
/// so its future is the value.
#[derive(Debug)]
pub struct ReadyTimestamp(pub u64);

impl TimestampFutureWait for ReadyTimestamp {
    fn wait(self) -> Result<u64, String> {
        Ok(self.0)
    }
}

/// The coordinator's PD surface.
pub trait PdCapability: Clone {
    /// This capability's future type.
    type TsFuture: TimestampFutureWait + Send;

    /// The cluster identity every routed request carries.
    fn cluster_id(&self) -> u64;

    /// Dispatches one timestamp request.
    fn timestamp_future(&self) -> Result<Self::TsFuture, String>;

    /// The current GC safe point — the floor below which no read may start.
    fn gc_safe_point(&self) -> Result<u64, String>;

    /// PD's client address, which is also where it serves its HTTP API.
    ///
    /// Placement rules live behind that HTTP API and nowhere else: Go reaches
    /// them through `pd/client/http`, not through the gRPC surface the rest
    /// of this trait covers. A capability with no real PD behind it — an
    /// embedded store — answers `None`, and delivery is then skipped rather
    /// than attempted against an address that does not exist.
    fn http_endpoint(&self) -> Option<String> {
        None
    }
}

impl PdCapability for PdClient {
    type TsFuture = PdTimestampFuture;

    fn http_endpoint(&self) -> Option<String> {
        Some(PdClient::active_endpoint(self))
    }

    fn cluster_id(&self) -> u64 {
        PdClient::cluster_id(self)
    }

    fn timestamp_future(&self) -> Result<Self::TsFuture, String> {
        self.get_timestamp_async()
            .map_err(|error| error.to_string())
    }

    fn gc_safe_point(&self) -> Result<u64, String> {
        self.get_gc_state(None)
            .map(|state| state.gc_safe_point)
            .map_err(|error| error.to_string())
    }
}

/// [`crate::lock::TimestampSource`] over any [`PdCapability`] — the generic
/// twin of the opener's PD-bound lock timestamp source: dispatch a future,
/// wait it out.
#[derive(Clone)]
pub struct CapabilityTimestampSource<P: PdCapability>(pub P);

impl<P: PdCapability> std::fmt::Debug for CapabilityTimestampSource<P> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CapabilityTimestampSource")
            .finish_non_exhaustive()
    }
}

impl<P: PdCapability> crate::lock::TimestampSource for CapabilityTimestampSource<P> {
    fn current_ts(&self) -> Result<u64, String> {
        self.0.timestamp_future()?.wait()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_ready_timestamp_waits_to_itself() {
        assert_eq!(ReadyTimestamp(42).wait(), Ok(42));
    }
}
