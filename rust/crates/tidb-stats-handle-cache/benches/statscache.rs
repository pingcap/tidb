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

//! Go `pkg/statistics/handle/cache/bench_test.go`.

use std::hint::black_box;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use tidb_config::config_tree::Config;
use tidb_stats_handle_cache::{CacheUpdate, StatsCacheImpl};
use tidb_stats_handle_cache_internal_testutil::new_mock_statistics_table;

const OPERATIONS: usize = 1_000;
const BENCHMARK_QUOTA: i64 = 256 * 1024 * 1024;

struct RestoreCacheConfig {
    config: Config,
    quota: i64,
}

impl Drop for RestoreCacheConfig {
    fn drop(&mut self) {
        tidb_config::config_tree::config::store_global_config(self.config.clone());
        tidb_vardef::STATS_CACHE_MEM_QUOTA.store(self.quota, Ordering::SeqCst);
    }
}

fn table(id: i64) -> Arc<tidb_stats::Table> {
    let source = new_mock_statistics_table(1, 1, true, false, false);
    let mut table = source.as_ref().clone();
    table.hist_coll.physical_id = id;
    Arc::new(table)
}

fn with_cache(enable_quota: bool, operation: impl FnOnce(&Arc<StatsCacheImpl>)) {
    let _restore = RestoreCacheConfig {
        config: tidb_config::config_tree::config::get_global_config(),
        quota: tidb_vardef::STATS_CACHE_MEM_QUOTA.swap(BENCHMARK_QUOTA, Ordering::SeqCst),
    };
    tidb_config::config_tree::config::update_global(|config| {
        config.performance.enable_stats_cache_mem_quota = enable_quota;
    });
    let cache = Arc::new(StatsCacheImpl::new().expect("statistics cache"));
    operation(&cache);
    cache.close();
}

fn benchmark_update(name: &str, enable_quota: bool) {
    with_cache(enable_quota, |cache| {
        let started = Instant::now();
        std::thread::scope(|scope| {
            for id in 0..OPERATIONS as i64 {
                let cache = Arc::clone(cache);
                scope.spawn(move || {
                    cache.update_stats_cache(CacheUpdate {
                        updated: vec![table(id + 1_000_000)],
                        ..CacheUpdate::default()
                    });
                });
            }
        });
        println!("{name}: {:?}", black_box(started.elapsed()));
    });
}

fn benchmark_put_get(name: &str, enable_quota: bool) {
    with_cache(enable_quota, |cache| {
        let started = Instant::now();
        std::thread::scope(|scope| {
            for id in 0..OPERATIONS as i64 {
                let writer = Arc::clone(cache);
                scope.spawn(move || {
                    writer.update_stats_cache(CacheUpdate {
                        updated: vec![table(id + 1_000_000)],
                        ..CacheUpdate::default()
                    });
                });
                let reader = Arc::clone(cache);
                scope.spawn(move || {
                    black_box(reader.get(id));
                });
            }
        });
        println!("{name}: {:?}", black_box(started.elapsed()));
    });
}

fn benchmark_get(name: &str, enable_quota: bool) {
    with_cache(enable_quota, |cache| {
        for id in 0..OPERATIONS as i64 {
            cache.update_stats_cache(CacheUpdate {
                updated: vec![table(id + 1_000_000)],
                ..CacheUpdate::default()
            });
        }
        cache.wait_for_async_updates();
        let started = Instant::now();
        std::thread::scope(|scope| {
            for id in 0..OPERATIONS as i64 {
                let cache = Arc::clone(cache);
                scope.spawn(move || {
                    black_box(cache.get(id));
                });
            }
        });
        println!("{name}: {:?}", black_box(started.elapsed()));
    });
}

fn main() {
    benchmark_update("BenchmarkStatsCacheLFUCopyAndUpdate", true);
    benchmark_update("BenchmarkStatsCacheMapCacheCopyAndUpdate", false);
    benchmark_put_get("BenchmarkLFUCachePutGet", true);
    benchmark_put_get("BenchmarkMapCachePutGet", false);
    benchmark_get("BenchmarkLFUCacheGet", true);
    benchmark_get("BenchmarkMapCacheGet", false);
}
