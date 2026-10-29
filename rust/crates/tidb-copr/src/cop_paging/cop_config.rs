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

//! The coprocessor read's per-node policy: Go `copr.CopClient`'s request
//! settings that are not part of one `kv.Request`.

use std::time::Duration;

use tidb_txnkv::TraceInfo;

use super::ReadEngineGeneration;
use crate::copr_cache::CoprCache;
use crate::copr_cache::CoprCacheConfig;

/// The coprocessor read's per-node policy.
#[derive(Clone, Debug)]
pub struct CopRuntimeConfig {
    /// Per-RPC timeout used when the task has no request-local read timeout.
    pub default_timeout: Duration,
    /// Storage generation used for paging read-byte accounting.
    pub read_engine_generation: ReadEngineGeneration,
    /// Initial shared read-byte prediction for one request runtime.
    pub seed_read_bytes: u64,
    /// Optional cache configuration. Each returned query response owns one
    /// cache instance for all of its logical tasks and paging attempts.
    pub cache: Option<CoprCacheConfig>,
    /// Process-owned coprocessor cache shared by every query transport.
    ///
    /// Go owns this cache on the TiKV store, outside any one request.
    /// Production supplies this handle so entries survive across statements
    /// and workers.
    pub shared_cache: Option<CoprCache>,
    /// Optional source-statement trace data injected before dispatch.
    pub trace: Option<TraceInfo>,
}

impl Default for CopRuntimeConfig {
    fn default() -> Self {
        Self {
            default_timeout: Duration::from_secs(60),
            read_engine_generation: ReadEngineGeneration::from_kernel_type(),
            seed_read_bytes: 0,
            cache: None,
            shared_cache: None,
            trace: None,
        }
    }
}

/// The coprocessor read's deadline message.
pub(crate) const QUERY_DEADLINE_EXCEEDED: &str = "query deadline exceeded";
