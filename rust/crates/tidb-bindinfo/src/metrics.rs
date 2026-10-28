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


//! Go `pkg/bindinfo`'s cache metrics.

use std::sync::LazyLock;

use prometheus::Gauge;

fn register<C: prometheus::core::Collector + Clone + 'static>(
    collector: prometheus::Result<C>,
) -> C {
    let collector = collector.expect("valid metric definition");
    prometheus::default_registry()
        .register(Box::new(collector.clone()))
        .expect("metric registered once");
    collector
}

/// Go BindingCacheMemUsage, published after each binding refresh.
pub static BINDING_CACHE_MEM_USAGE: LazyLock<Gauge> = LazyLock::new(|| {
    register(Gauge::new(
        "tidb_server_binding_cache_mem_usage",
        "Memory usage of binding cache.",
    ))
});

/// Go BindingCacheMemLimit.
pub static BINDING_CACHE_MEM_LIMIT: LazyLock<Gauge> = LazyLock::new(|| {
    register(Gauge::new(
        "tidb_server_binding_cache_mem_limit",
        "Memory limit of binding cache.",
    ))
});

/// Go BindingCacheNumBindings; evicted side-index entries do not count.
pub static BINDING_CACHE_NUM_BINDINGS: LazyLock<Gauge> = LazyLock::new(|| {
    register(Gauge::new(
        "tidb_server_binding_cache_num_bindings",
        "Number of bindings in binding cache.",
    ))
});
