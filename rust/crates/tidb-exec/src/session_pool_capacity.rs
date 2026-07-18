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

//! System-session pool capacity policy from `pkg/session/syssession/pool.go`.
//!
//! TiDB uses a large hard maximum and falls back to it when a requested
//! capacity is non-positive or exceeds that maximum. This leaf ports the
//! deterministic value policy only; factory/channel construction, assertions,
//! context ownership, session transfer, reset, and close lifecycle remain
//! external.

/// Maximum system-session pool capacity from the source.
pub const POOL_MAX_SIZE: usize = 1024 * 1024 * 1024;

/// Normalizes a requested pool capacity using source `NewAdvancedSessionPool`
/// semantics.
#[must_use]
pub const fn normalize_pool_capacity(requested: i64) -> usize {
    if requested <= 0 || requested > POOL_MAX_SIZE as i64 {
        POOL_MAX_SIZE
    } else {
        requested as usize
    }
}
