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
    type TsFuture: TimestampFutureWait;

    /// The cluster identity every routed request carries.
    fn cluster_id(&self) -> u64;

    /// Dispatches one timestamp request.
    fn timestamp_future(&self) -> Result<Self::TsFuture, String>;

    /// The current GC safe point — the floor below which no read may start.
    fn gc_safe_point(&self) -> Result<u64, String>;
}

impl PdCapability for PdClient {
    type TsFuture = PdTimestampFuture;

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_ready_timestamp_waits_to_itself() {
        assert_eq!(ReadyTimestamp(42).wait(), Ok(42));
    }
}
