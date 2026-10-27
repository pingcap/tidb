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

//! What one `kv.Response.Next` pull yields (Go `pkg/kv`'s `ResultSubset`,
//! here with the coprocessor runtime data the same pull carries) and how a
//! pull fails.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use tikv_client::util::{ScanDetail, TimeDetail};

/// One response subset returned by a single pull from the transport response.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct QueryResultSubset {
    /// Raw protobuf response bytes, shared with the transport buffer they
    /// arrived in. Decoding belongs to the select consumer.
    pub data: prost::bytes::Bytes,
    /// Runtime data returned by the same response pull, when present.
    pub runtime: Option<ResponseRuntimeStats>,
}

/// Error returned while pulling an already-created response owner.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QueryResponseError {
    /// The deterministic adapter is still open but has no subset available.
    Pending,
    /// The response source returned its first terminal error.
    Source(String),
    /// A registered storage or SQL error whose identity survives row decoding.
    Sql {
        /// MySQL protocol error number.
        code: u16,
        /// Source-formatted error message.
        message: String,
    },
    /// The canonical query cancellation interrupted response consumption.
    Cancelled,
}

impl std::fmt::Display for QueryResponseError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pending => formatter.write_str("DistSQL query response is still pending"),
            Self::Source(message) => formatter.write_str(message),
            Self::Sql { message, .. } => formatter.write_str(message),
            Self::Cancelled => formatter.write_str("query cancelled by caller"),
        }
    }
}

impl std::error::Error for QueryResponseError {}

/// Non-protobuf CopRuntimeStats data attached to one response subset.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ResponseRuntimeStats {
    /// TiKV callee address, which may be empty when request RPC stats exist.
    pub callee_address: String,
    /// Whether request stats reported a nonzero RPCStatsCount, which satisfies
    /// the source empty-callee gate.
    pub request_rpc_stats_present: bool,
    /// Backoff sleep samples owned by this response subset.
    pub backoff_sleep_ns: Vec<(String, u64)>,
    /// Go `CopRuntimeStats.ScanDetail`: the response's `ScanDetailV2` (or the
    /// legacy write-scan counts), `None` when TiKV sent no scan detail.
    pub scan_detail: Option<ScanDetail>,
    /// Go `CopRuntimeStats.TimeDetail`, merged from `TimeDetailV2` or the
    /// legacy `TimeDetail`.
    pub time_detail: TimeDetail,
    /// Go `copResponse.RespTime()`: how long the task's send took
    /// (`handleTaskOnce`'s `costTime`).
    pub resp_time: Duration,
    /// Go `CopRuntimeStats.ReqStats`: the physical RPCs the task's send made.
    pub req_stats: Option<RequestStatsSample>,
    /// Go `CopRuntimeStats.CoprCacheHit`.
    pub copr_cache_hit: bool,
}

/// A shared `*tikv.RegionRequestRuntimeStats`, compared by identity.
#[derive(Clone)]
pub struct RequestStatsSample(pub Arc<tikv_client::RegionRequestRuntimeStats>);

impl fmt::Debug for RequestStatsSample {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RequestStatsSample({})", self.0)
    }
}

impl PartialEq for RequestStatsSample {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for RequestStatsSample {}

/// Aggregated blocking time from the Go coprocessor request limiter.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LimiterWaitStats {
    /// Total nanoseconds spent waiting for request permits.
    pub total_ns: u64,
    /// Longest single permit wait in nanoseconds.
    pub max_ns: u64,
}

impl LimiterWaitStats {
    /// Records one blocking wait, preserving the source total/max contract.
    pub fn record(&mut self, wait_ns: u64) {
        self.total_ns = self.total_ns.saturating_add(wait_ns);
        self.max_ns = self.max_ns.max(wait_ns);
    }

    /// Merges another aggregate into this one.
    pub fn merge(&mut self, other: Self) {
        self.total_ns = self.total_ns.saturating_add(other.total_ns);
        self.max_ns = self.max_ns.max(other.max_ns);
    }

    /// Reports whether no blocking wait was recorded.
    #[must_use]
    pub const fn is_zero(self) -> bool {
        self.total_ns == 0
    }
}

/// Go `copr.CopInfo` (`GetConcurrency`, `GetBuildTaskElapsed`).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CopInfo {
    /// Go `copIterator.concurrency`.
    pub concurrency: usize,
    /// Go `copIterator.smallTaskConcurrency`.
    pub extra_concurrency: usize,
    /// Go `copIterator.buildTaskElapsed`.
    pub build_task_elapsed: std::time::Duration,
}
