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

//! Statistics initialization concurrency policy from
//! `pkg/statistics/handle/initstats/load_stats.go`.
//!
//! The Go owner derives a worker count from `GOMAXPROCS`, choosing either
//! half the CPUs or two fewer CPUs, then clamps the result to [2, 16]. This
//! leaf accepts the already-observed CPU count and force flag; runtime probing,
//! config access, worker creation, and stats loading remain external.

/// Computes the source `GetConcurrency` worker limit.
///
/// `cpu_count` is signed to preserve Go's `int` arithmetic for synthetic
/// low/negative inputs before the final clamp. Division truncates toward zero,
/// matching Go integer division.
#[must_use]
pub const fn init_stats_concurrency(cpu_count: i64, force_init_stats: bool) -> i64 {
    let concurrency = if force_init_stats {
        // Go's signed integer arithmetic wraps for overflow; GOMAXPROCS is
        // normally positive, but keeping the operation explicit preserves the
        // source behavior for caller-provided edge values.
        cpu_count.wrapping_sub(2)
    } else {
        cpu_count / 2
    };
    if concurrency < 2 {
        2
    } else if concurrency > 16 {
        16
    } else {
        concurrency
    }
}
