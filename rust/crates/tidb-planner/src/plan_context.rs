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

//! Planner build-context detachment from `pkg/planner/planctx/context.go`.
//!
//! The source's `BuildPBContext.Detach` shallow-copies the build state and
//! swaps only the expression context. Interface-backed expression, client,
//! and warning handles are represented here by opaque copyable IDs so this
//! leaf can preserve identity without inventing session or protobuf owners.

/// Opaque identity for a source-owned planner context interface.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct OpaqueContextHandle(u64);

impl OpaqueContextHandle {
    /// Creates a handle from a stable test/integration identifier.
    #[must_use]
    pub const fn new(raw: u64) -> Self {
        Self(raw)
    }

    /// Returns the stable identifier.
    #[must_use]
    pub const fn raw(self) -> u64 {
        self.0
    }
}

/// Source-shaped state used while building protobuf executors.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BuildPbContext {
    /// Expression context used by the current build.
    pub expr_ctx: OpaqueContextHandle,
    /// KV client handle used by protobuf conversion.
    pub client: OpaqueContextHandle,
    /// TiFlash fast-scan switch.
    pub tiflash_fast_scan: bool,
    /// TiFlash fine-grained shuffle batch size.
    pub tiflash_fine_grained_shuffle_batch_size: u64,
    /// Maximum length for GROUP_CONCAT while building expressions.
    pub group_concat_max_len: u64,
    /// Whether the current statement is EXPLAIN.
    pub in_explain_stmt: bool,
    /// Warning handler identity shared with the source session.
    pub warn_handler: OpaqueContextHandle,
    /// Additional warning handler identity.
    pub extra_warn_handler: OpaqueContextHandle,
}

impl BuildPbContext {
    /// Returns the current expression-context handle.
    #[must_use]
    pub const fn get_expr_ctx(&self) -> OpaqueContextHandle {
        self.expr_ctx
    }

    /// Returns the current client handle.
    #[must_use]
    pub const fn get_client(&self) -> OpaqueContextHandle {
        self.client
    }

    /// Detaches the build state while replacing only its expression context.
    #[must_use]
    pub fn detach(&self, static_expr_ctx: OpaqueContextHandle) -> Self {
        let mut detached = self.clone();
        detached.expr_ctx = static_expr_ctx;
        detached
    }
}
