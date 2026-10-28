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

//! Shared Go `pkg/metrics.PanicCounter`, used by session and DDL recovery.

use prometheus::{IntCounterVec, Opts};
use std::sync::LazyLock;

/// One process-wide counter, independent of the caller's transaction outcome.
pub static PANIC_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    let collector = IntCounterVec::new(
        Opts::new("tidb_server_panic_total", "Counter of panic."),
        &["type"],
    )
    .expect("valid panic metric definition");
    if let Err(error) = prometheus::default_registry().register(Box::new(collector.clone())) {
        // Preserve the server's registration policy when this shared metric
        // is initialized by a worker before server metric initialization.
        eprintln!(
            "[[server metric skipped as already registered: {error:?} for tidb_server_panic_total]]"
        );
    }
    collector
});
