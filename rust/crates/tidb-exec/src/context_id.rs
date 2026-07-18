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

//! Monotonic statement-context IDs from `pkg/util/context/context.go`.
//!
//! TiDB allocates a fresh non-zero ID whenever a statement context is
//! created or reset. This leaf owns only that process-local atomic sequence;
//! it does not own a statement context, reset locks, timezone state, warning
//! handlers, or any session lifecycle.

use std::sync::atomic::{AtomicU64, Ordering};

static CONTEXT_ID_GENERATOR: AtomicU64 = AtomicU64::new(0);

/// Allocates the next unique, non-zero statement-context ID.
///
/// This mirrors Go's `contextIDGenerator.Add(1)`, including the process-local
/// sequence and atomic increment semantics.
#[must_use]
pub fn gen_context_id() -> u64 {
    CONTEXT_ID_GENERATOR.fetch_add(1, Ordering::SeqCst) + 1
}
