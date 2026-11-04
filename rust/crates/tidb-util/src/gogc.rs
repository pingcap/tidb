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

//! Go `pkg/util/gogc.go`: the process's GOGC value -- 100, or the `GOGC`
//! environment value -- which the GC tuner updates through `SetGOGC`. This
//! port has no Go collector to retune; it keeps Go's value and its
//! `tidb_server_gogc` gauge.

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::LazyLock;

/// Go `gogcValue`, initialized as Go's `init` does.
static GOGC_VALUE: LazyLock<AtomicI64> = LazyLock::new(|| {
    let value = std::env::var("GOGC")
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(100);
    tidb_metrics::server::GOGC.set(value);
    AtomicI64::new(value)
});

/// Go `SetGOGC`: a non-positive value is 100; answers the previous value
/// (Go's `debug.SetGCPercent` result).
pub fn set_gogc(val: i64) -> i64 {
    let val = if val <= 0 { 100 } else { val };
    tidb_metrics::server::GOGC.set(val);
    GOGC_VALUE.swap(val, Ordering::SeqCst)
}

/// Go `GetGOGC`.
pub fn get_gogc() -> i64 {
    GOGC_VALUE.load(Ordering::SeqCst)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_gogc_keeps_gos_floor_and_answers_the_previous_value() {
        let initial = get_gogc();
        assert_eq!(set_gogc(250), initial);
        assert_eq!(get_gogc(), 250);
        assert_eq!(set_gogc(0), 250);
        assert_eq!(get_gogc(), 100);
        assert_eq!(tidb_metrics::server::GOGC.get(), 100);
    }
}
