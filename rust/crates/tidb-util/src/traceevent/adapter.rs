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

//! Go `adapter.go`: the bridge that lets client-go emit trace events through
//! TiDB's recorder and ask TiDB which categories and TiKV trace flags are live.
//!
//! `github.com/tikv/client-go/v2/trace` is not part of this workspace, so its
//! surface is reproduced by the boundary types below: exactly the category
//! enum, the control-flag set, and the three registration hooks that
//! `RegisterWithClientGo` installs. The numeric flag values belong to client-go
//! and are not observable here, so [`TraceControlFlags`] assigns its own bits;
//! only the named flags carry meaning.

use tidb_log::{Field, Value};

use super::{get_flight_recorder, is_enabled, trace_event, Trace, TraceCategory};
use crate::tracing::{self, TraceContext};

// boundary: client-go `trace.Category`.
/// The trace categories client-go emits events under.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClientGoCategory {
    /// client-go `trace.CategoryTxn2PC`.
    Txn2Pc,
    /// client-go `trace.CategoryTxnLockResolve`.
    TxnLockResolve,
    /// client-go `trace.CategoryKVRequest`.
    KvRequest,
    /// client-go `trace.CategoryRegionCache`.
    RegionCache,
    /// A category this build does not map, carrying client-go's raw value.
    Other(u32),
}

impl ClientGoCategory {
    /// The raw `uint32` client-go stores, as logged for unmapped categories.
    #[must_use]
    pub const fn raw(self) -> u32 {
        match self {
            Self::Txn2Pc => 0,
            Self::TxnLockResolve => 1,
            Self::KvRequest => 2,
            Self::RegionCache => 3,
            Self::Other(raw) => raw,
        }
    }
}

// boundary: client-go `trace.TraceControlFlags`.
/// The control flags TiDB hands back to client-go for one request.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TraceControlFlags(u32);

impl TraceControlFlags {
    /// client-go `trace.FlagImmediateLog`.
    pub const IMMEDIATE_LOG: Self = Self(1 << 0);
    /// client-go `trace.FlagTiKVCategoryRequest`.
    pub const TIKV_CATEGORY_REQUEST: Self = Self(1 << 1);
    /// client-go `trace.FlagTiKVCategoryWriteDetails`.
    pub const TIKV_CATEGORY_WRITE_DETAILS: Self = Self(1 << 2);
    /// client-go `trace.FlagTiKVCategoryReadDetails`.
    pub const TIKV_CATEGORY_READ_DETAILS: Self = Self(1 << 3);

    /// client-go `TraceControlFlags.With`.
    #[must_use]
    pub const fn with(self, flag: Self) -> Self {
        Self(self.0 | flag.0)
    }

    /// client-go `TraceControlFlags.Has`.
    #[must_use]
    pub const fn has(self, flag: Self) -> bool {
        self.0 & flag.0 == flag.0
    }
}

/// client-go `trace.SetTraceEventFunc`'s argument.
pub type TraceEventFn = fn(&TraceContext, ClientGoCategory, &str, Vec<Field>);
/// client-go `trace.SetIsCategoryEnabledFunc`'s argument.
pub type IsCategoryEnabledFn = fn(ClientGoCategory) -> bool;
/// client-go `trace.SetTraceControlExtractor`'s argument.
///
/// Go's extractor takes a `context.Context` and asserts its sink to `*Trace`;
/// this port takes the statement's [`Trace`] directly (see the module
/// boundaries).
pub type TraceControlExtractorFn = fn(Option<&Trace>) -> TraceControlFlags;

/// The three client-go globals `RegisterWithClientGo` installs.
pub trait ClientGoTraceRegistry {
    /// client-go `trace.SetTraceEventFunc`.
    fn set_trace_event_func(&self, handler: TraceEventFn);
    /// client-go `trace.SetIsCategoryEnabledFunc`.
    fn set_is_category_enabled_func(&self, handler: IsCategoryEnabledFn);
    /// client-go `trace.SetTraceControlExtractor`.
    fn set_trace_control_extractor(&self, handler: TraceControlExtractorFn);
}

/// Go `RegisterWithClientGo`: registers TiDB's trace event handlers with
/// client-go. Call once during initialization.
pub fn register_with_client_go(registry: &dyn ClientGoTraceRegistry) {
    registry.set_trace_event_func(handle_client_go_trace_event);
    registry.set_is_category_enabled_func(handle_client_go_is_category_enabled);
    registry.set_trace_control_extractor(handle_trace_control_extractor);
}

/// Go `handleClientGoTraceEvent`.
pub fn handle_client_go_trace_event(
    ctx: &TraceContext,
    category: ClientGoCategory,
    name: &str,
    mut fields: Vec<Field>,
) {
    let mapped = map_category(category);
    if !is_enabled(mapped) {
        return;
    }
    // Include the original category value for unknown categories to aid
    // debugging.
    if mapped == TraceCategory::UNKNOWN_CLIENT {
        fields.push(Field::new(
            "client_go_category",
            Value::U64(u64::from(category.raw())),
        ));
    }
    trace_event(ctx, mapped, name, fields);
}

/// Go `handleClientGoIsCategoryEnabled`.
#[must_use]
pub fn handle_client_go_is_category_enabled(category: ClientGoCategory) -> bool {
    is_enabled(map_category(category))
}

/// Go `handleTraceControlExtractor`: maps TiDB's enabled categories onto the
/// TiKV trace flags, and adds the immediate-log flag when the statement's trace
/// is already going to be kept.
#[must_use]
pub fn handle_trace_control_extractor(trace: Option<&Trace>) -> TraceControlFlags {
    let mut flags = TraceControlFlags::default();

    // Map TiDB categories to TiKV categories regardless of whether a Trace sink
    // is present.
    let enabled = tracing::enabled_categories();
    if enabled.0 & TraceCategory::TIKV_REQUEST.0 != 0 {
        flags = flags.with(TraceControlFlags::TIKV_CATEGORY_REQUEST);
    }
    if enabled.0 & TraceCategory::TIKV_WRITE_DETAILS.0 != 0 {
        flags = flags.with(TraceControlFlags::TIKV_CATEGORY_WRITE_DETAILS);
    }
    if enabled.0 & TraceCategory::TIKV_READ_DETAILS.0 != 0 {
        flags = flags.with(TraceControlFlags::TIKV_CATEGORY_READ_DETAILS);
    }

    let Some(trace) = trace else {
        return flags;
    };
    let Some(recorder) = get_flight_recorder() else {
        return flags;
    };
    if recorder.should_keep(trace.bits()) {
        flags = flags.with(TraceControlFlags::IMMEDIATE_LOG);
    }
    flags
}

/// Go `mapCategory`.
#[must_use]
pub fn map_category(category: ClientGoCategory) -> TraceCategory {
    match category {
        ClientGoCategory::Txn2Pc => TraceCategory::TXN_2PC,
        ClientGoCategory::TxnLockResolve => TraceCategory::TXN_LOCK_RESOLVE,
        ClientGoCategory::KvRequest => TraceCategory::KV_REQUEST,
        ClientGoCategory::RegionCache => TraceCategory::REGION_CACHE,
        ClientGoCategory::Other(_) => TraceCategory::UNKNOWN_CLIENT,
    }
}
