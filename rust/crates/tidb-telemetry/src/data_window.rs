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

//! Go `data_window.go`: per-minute sub windows of execution counters,
//! aggregated into hour windows for the report.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{LazyLock, Mutex, PoisonError, RwLock};
use std::time::Duration;

use chrono::{DateTime, Local};
use serde::{Serialize, Serializer};

/// Go `CurrentExecuteCount`.
pub static CURRENT_EXECUTE_COUNT: AtomicU64 = AtomicU64::new(0);
/// Go `CurrentTiFlashPushDownCount`.
pub static CURRENT_TIFLASH_PUSH_DOWN_COUNT: AtomicU64 = AtomicU64::new(0);
/// Go `CurrentTiFlashExchangePushDownCount`.
pub static CURRENT_TIFLASH_EXCHANGE_PUSH_DOWN_COUNT: AtomicU64 = AtomicU64::new(0);
/// Go `CurrentCoprCacheHitRatioGTE0Count`.
pub static CURRENT_COPR_CACHE_HIT_RATIO_GTE0_COUNT: AtomicU64 = AtomicU64::new(0);
/// Go `CurrentCoprCacheHitRatioGTE1Count`.
pub static CURRENT_COPR_CACHE_HIT_RATIO_GTE1_COUNT: AtomicU64 = AtomicU64::new(0);
/// Go `CurrentCoprCacheHitRatioGTE10Count`.
pub static CURRENT_COPR_CACHE_HIT_RATIO_GTE10_COUNT: AtomicU64 = AtomicU64::new(0);
/// Go `CurrentCoprCacheHitRatioGTE20Count`.
pub static CURRENT_COPR_CACHE_HIT_RATIO_GTE20_COUNT: AtomicU64 = AtomicU64::new(0);
/// Go `CurrentCoprCacheHitRatioGTE40Count`.
pub static CURRENT_COPR_CACHE_HIT_RATIO_GTE40_COUNT: AtomicU64 = AtomicU64::new(0);
/// Go `CurrentCoprCacheHitRatioGTE80Count`.
pub static CURRENT_COPR_CACHE_HIT_RATIO_GTE80_COUNT: AtomicU64 = AtomicU64::new(0);
/// Go `CurrentCoprCacheHitRatioGTE100Count`.
pub static CURRENT_COPR_CACHE_HIT_RATIO_GTE100_COUNT: AtomicU64 = AtomicU64::new(0);
/// Go `CurrentTiflashTableScanCount`: TiFlash table and partition-table
/// scans.
pub static CURRENT_TIFLASH_TABLE_SCAN_COUNT: AtomicU64 = AtomicU64::new(0);
/// Go `CurrentTiflashTableScanWithFastScanCount`: those scans that use fast
/// scan.
pub static CURRENT_TIFLASH_TABLE_SCAN_WITH_FAST_SCAN_COUNT: AtomicU64 = AtomicU64::new(0);

/// Go `WindowSize`: how long some data is aggregated by.
pub const WINDOW_SIZE: Duration = Duration::from_secs(60 * 60);
/// Go `SubWindowSize`: how often data is rotated.
pub const SUB_WINDOW_SIZE: Duration = Duration::from_secs(60);

/// Go `maxSubWindowLength`.
const MAX_SUB_WINDOW_LENGTH: usize = (crate::REPORT_INTERVAL.as_secs() / SUB_WINDOW_SIZE.as_secs()) as usize;
/// Go `maxSubWindowLengthInWindow`.
const MAX_SUB_WINDOW_LENGTH_IN_WINDOW: usize = (WINDOW_SIZE.as_secs() / SUB_WINDOW_SIZE.as_secs()) as usize;

/// Go `windowData`.
#[derive(Clone, Debug, Serialize)]
pub struct WindowData {
    #[serde(rename = "beginAt", serialize_with = "go_time_json")]
    pub begin_at: DateTime<Local>,
    #[serde(rename = "executeCount")]
    pub execute_count: u64,
    #[serde(rename = "tiFlashUsage")]
    pub tiflash_usage: TiFlashUsageData,
    #[serde(rename = "coprCacheUsage")]
    pub copr_cache_usage: CoprCacheUsageData,
    /// Go `map[string]uint32`, which `encoding/json` writes with sorted keys.
    #[serde(rename = "builtinFunctionsUsage", serialize_with = "sorted_map")]
    pub builtin_functions_usage: BuiltinFunctionsUsage,
}

/// Go `coprCacheUsageData`.
#[derive(Clone, Copy, Debug, Default, Serialize)]
pub struct CoprCacheUsageData {
    #[serde(rename = "gte0")]
    pub gte0: u64,
    #[serde(rename = "gte1")]
    pub gte1: u64,
    #[serde(rename = "gte10")]
    pub gte10: u64,
    #[serde(rename = "gte20")]
    pub gte20: u64,
    #[serde(rename = "gte40")]
    pub gte40: u64,
    #[serde(rename = "gte80")]
    pub gte80: u64,
    #[serde(rename = "gte100")]
    pub gte100: u64,
}

/// Go `tiFlashUsageData`.
#[derive(Clone, Copy, Debug, Default, Serialize)]
pub struct TiFlashUsageData {
    #[serde(rename = "pushDown")]
    pub push_down: u64,
    #[serde(rename = "exchangePushDown")]
    pub exchange_push_down: u64,
    #[serde(rename = "tableScan")]
    pub table_scan: u64,
    #[serde(rename = "tableScanWithFastScan")]
    pub table_scan_with_fast_scan: u64,
}

/// Go `BuiltinFunctionsUsage`: scalar function signature name to usage
/// count.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BuiltinFunctionsUsage(pub HashMap<String, u32>);

impl BuiltinFunctionsUsage {
    /// Go `Inc`.
    pub fn inc(&mut self, scalar_func_sig_name: &str) {
        *self.0.entry(scalar_func_sig_name.to_owned()).or_insert(0) += 1;
    }

    /// Go `Merge`.
    pub fn merge(&mut self, usage_data: &Self) {
        for (name, count) in &usage_data.0 {
            *self.0.entry(name.clone()).or_insert(0) += count;
        }
    }
}

/// Go `builtinFunctionsUsageCollector`.
pub struct BuiltinFunctionsUsageCollector {
    usage_data: Mutex<BuiltinFunctionsUsage>,
}

impl BuiltinFunctionsUsageCollector {
    /// Go `Collect`: merges one session's usage.
    pub fn collect(&self, usage_data: &BuiltinFunctionsUsage) {
        self.usage_data
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .merge(usage_data);
    }

    /// Go `Dump`: takes the collected usage.
    pub fn dump(&self) -> BuiltinFunctionsUsage {
        std::mem::take(&mut *self.usage_data.lock().unwrap_or_else(PoisonError::into_inner))
    }
}

/// Go `GlobalBuiltinFunctionsUsage`.
pub static GLOBAL_BUILTIN_FUNCTIONS_USAGE: LazyLock<BuiltinFunctionsUsageCollector> =
    LazyLock::new(|| BuiltinFunctionsUsageCollector {
        usage_data: Mutex::new(BuiltinFunctionsUsage::default()),
    });

/// Go `rotatedSubWindows` under `subWindowsLock`.
static ROTATED_SUB_WINDOWS: RwLock<Vec<WindowData>> = RwLock::new(Vec::new());

/// Go `RotateSubWindow`.
pub fn rotate_sub_window() {
    let this_sub_window = WindowData {
        begin_at: Local::now(),
        execute_count: CURRENT_EXECUTE_COUNT.swap(0, Ordering::Relaxed),
        tiflash_usage: TiFlashUsageData {
            push_down: CURRENT_TIFLASH_PUSH_DOWN_COUNT.swap(0, Ordering::Relaxed),
            exchange_push_down: CURRENT_TIFLASH_EXCHANGE_PUSH_DOWN_COUNT.swap(0, Ordering::Relaxed),
            table_scan: CURRENT_TIFLASH_TABLE_SCAN_COUNT.swap(0, Ordering::Relaxed),
            table_scan_with_fast_scan: CURRENT_TIFLASH_TABLE_SCAN_WITH_FAST_SCAN_COUNT.swap(0, Ordering::Relaxed),
        },
        copr_cache_usage: CoprCacheUsageData {
            gte0: CURRENT_COPR_CACHE_HIT_RATIO_GTE0_COUNT.swap(0, Ordering::Relaxed),
            gte1: CURRENT_COPR_CACHE_HIT_RATIO_GTE1_COUNT.swap(0, Ordering::Relaxed),
            gte10: CURRENT_COPR_CACHE_HIT_RATIO_GTE10_COUNT.swap(0, Ordering::Relaxed),
            gte20: CURRENT_COPR_CACHE_HIT_RATIO_GTE20_COUNT.swap(0, Ordering::Relaxed),
            gte40: CURRENT_COPR_CACHE_HIT_RATIO_GTE40_COUNT.swap(0, Ordering::Relaxed),
            gte80: CURRENT_COPR_CACHE_HIT_RATIO_GTE80_COUNT.swap(0, Ordering::Relaxed),
            gte100: CURRENT_COPR_CACHE_HIT_RATIO_GTE100_COUNT.swap(0, Ordering::Relaxed),
        },
        builtin_functions_usage: GLOBAL_BUILTIN_FUNCTIONS_USAGE.dump(),
    };
    let mut windows = ROTATED_SUB_WINDOWS.write().unwrap_or_else(PoisonError::into_inner);
    windows.push(this_sub_window);
    if windows.len() > MAX_SUB_WINDOW_LENGTH {
        // Only retain the last N sub windows, according to the report
        // interval.
        let excess = windows.len() - MAX_SUB_WINDOW_LENGTH;
        windows.drain(..excess);
    }
}

/// Go `getWindowData`: the sub windows aggregated by window size.
pub(crate) fn get_window_data() -> Vec<WindowData> {
    let windows = ROTATED_SUB_WINDOWS.read().unwrap_or_else(PoisonError::into_inner);
    let mut results = Vec::new();
    let mut i = 0;
    while i < windows.len() {
        let mut this_window = windows[i].clone();
        let mut aggregated_sub_windows = 1;
        // Aggregate later sub windows.
        i += 1;
        while i < windows.len() && aggregated_sub_windows < MAX_SUB_WINDOW_LENGTH_IN_WINDOW {
            let next = &windows[i];
            this_window.execute_count += next.execute_count;
            this_window.tiflash_usage.push_down += next.tiflash_usage.push_down;
            this_window.tiflash_usage.exchange_push_down += next.tiflash_usage.exchange_push_down;
            this_window.tiflash_usage.table_scan += next.tiflash_usage.table_scan;
            this_window.tiflash_usage.table_scan_with_fast_scan += next.tiflash_usage.table_scan_with_fast_scan;
            this_window.copr_cache_usage.gte0 += next.copr_cache_usage.gte0;
            this_window.copr_cache_usage.gte1 += next.copr_cache_usage.gte1;
            this_window.copr_cache_usage.gte10 += next.copr_cache_usage.gte10;
            this_window.copr_cache_usage.gte20 += next.copr_cache_usage.gte20;
            this_window.copr_cache_usage.gte40 += next.copr_cache_usage.gte40;
            this_window.copr_cache_usage.gte80 += next.copr_cache_usage.gte80;
            this_window.copr_cache_usage.gte100 += next.copr_cache_usage.gte100;
            this_window.builtin_functions_usage.merge(&next.builtin_functions_usage);
            aggregated_sub_windows += 1;
            i += 1;
        }
        results.push(this_window);
    }
    results
}

/// Go `time.Time.MarshalJSON`: RFC 3339 with nanoseconds, trailing zeros
/// trimmed, and `Z` for a zero offset.
fn go_time_json<S: Serializer>(time: &DateTime<Local>, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(&go_rfc3339_nano(time))
}

/// Go `time.RFC3339Nano` formatting.
pub(crate) fn go_rfc3339_nano(time: &DateTime<Local>) -> String {
    use chrono::{Offset, Timelike};
    let mut out = time.format("%Y-%m-%dT%H:%M:%S").to_string();
    let nanos = time.nanosecond() % 1_000_000_000;
    if nanos != 0 {
        let digits = format!("{nanos:09}");
        out.push('.');
        out.push_str(digits.trim_end_matches('0'));
    }
    let offset = time.offset().fix().local_minus_utc();
    if offset == 0 {
        out.push('Z');
    } else {
        out.push_str(&time.format("%:z").to_string());
    }
    out
}

fn sorted_map<S: Serializer>(usage: &BuiltinFunctionsUsage, serializer: S) -> Result<S::Ok, S::Error> {
    usage.0.iter().collect::<BTreeMap<_, _>>().serialize(serializer)
}
