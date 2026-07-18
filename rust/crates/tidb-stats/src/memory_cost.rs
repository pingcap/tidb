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

//! LFU memory-cost transitions from
//! `pkg/statistics/handle/cache/internal/lfu/lfu_cache.go`.
//!
//! The source obtains host memory through `memory.MemTotal` and tracks cache
//! cost with an atomic signed counter. This leaf keeps those boundaries
//! explicit: the caller supplies the memory probe result, while this module
//! owns only the source arithmetic and test-mode capacity override.

use std::fmt;

/// Percentage of host memory used when the configured cache cost is zero.
pub const MEMORY_COST_PERCENT: u64 = 20;

/// Test-mode capacity used by the source to keep the LFU sketch small.
pub const TEST_MODE_MEMORY_COST: i64 = 5_000_000;

/// Failure to obtain the host-memory value required by a zero capacity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MemoryCostError {
    /// The caller did not provide a successful `memory.MemTotal` result.
    SystemMemoryUnavailable,
}

impl fmt::Display for MemoryCostError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SystemMemoryUnavailable => formatter.write_str("system memory unavailable"),
        }
    }
}

impl std::error::Error for MemoryCostError {}

/// Adjusts a configured cost using the source's 20%-of-memory rule.
///
/// A nonzero configured cost is returned unchanged. For zero, `memory_total`
/// models the result of the source `memory.MemTotal()` call. The wrapping
/// multiplication preserves Go's `uint64` arithmetic before conversion to the
/// signed cache-cost type.
pub fn adjust_mem_cost(
    total_mem_cost: i64,
    memory_total: Option<u64>,
) -> Result<i64, MemoryCostError> {
    if total_mem_cost != 0 {
        return Ok(total_mem_cost);
    }
    let memory_total = memory_total.ok_or(MemoryCostError::SystemMemoryUnavailable)?;
    Ok((memory_total.wrapping_mul(MEMORY_COST_PERCENT) / 100) as i64)
}

/// Applies the source `NewLFU` test-mode override after adjustment.
///
/// The source still performs `adjustMemCost` first, so a missing system-memory
/// result remains an error even when test mode would replace the resulting
/// capacity.
pub fn effective_mem_cost(
    total_mem_cost: i64,
    in_test: bool,
    memory_total: Option<u64>,
) -> Result<i64, MemoryCostError> {
    let adjusted = adjust_mem_cost(total_mem_cost, memory_total)?;
    if in_test && total_mem_cost == 0 {
        return Ok(TEST_MODE_MEMORY_COST);
    }
    Ok(adjusted)
}

/// Adds one signed tracking-cost delta using the source atomic-int64 behavior.
#[must_use]
pub const fn add_memory_cost(current: i64, delta: i64) -> i64 {
    current.wrapping_add(delta)
}
