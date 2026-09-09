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

//! Go `adapter.go`: the live bridge from the TiKV client's trace hooks to
//! TiDB's trace-event recorder.

use std::sync::Arc;
use tidb_log::{Field, Value};
use tikv_client::trace::{
    self as client_trace, Category, TraceControlFlags, TraceField, TraceValue,
};

use super::{get_flight_recorder, is_enabled, trace_event, Trace, TraceCategory};
use crate::tracing::{self, Sink, TraceContext};

/// Go `RegisterWithClientGo`: install TiDB's three handlers in the real TiKV
/// client trace package.
pub fn register_with_client_go() {
    client_trace::set_trace_event_handler(Some(Arc::new(handle_client_go_trace_event)));
    client_trace::set_category_enabled_handler(Some(Arc::new(
        handle_client_go_is_category_enabled,
    )));
    client_trace::set_trace_control_extractor(Some(Arc::new(handle_trace_control_extractor)));
}

fn local_context(context: &client_trace::TraceContext) -> TraceContext {
    let mut local = TraceContext::background();
    if let Some(trace_id) = context.trace_id() {
        local = local.with_trace_id(trace_id);
    }
    if let Some(trace) = context.value::<Trace, Arc<Trace>>() {
        local = local.with_flight_recorder(Arc::clone(trace) as Arc<dyn Sink>);
    }
    local
}

fn field_value(value: &TraceValue) -> Value {
    match value {
        TraceValue::String(value) => Value::Str(value.clone()),
        TraceValue::Bool(value) => Value::Bool(*value),
        TraceValue::I64(value) => Value::I64(*value),
        TraceValue::U64(value) => Value::U64(*value),
        TraceValue::Duration(value) => Value::Duration(value.as_nanos() as i64),
        TraceValue::Binary(value) => Value::Binary(value.clone()),
        TraceValue::Array(values) => Value::Array(values.iter().map(field_value).collect()),
        TraceValue::Object(fields) => Value::Object(
            fields
                .iter()
                .map(|field| Field::new(&field.name, field_value(field.encoded_value())))
                .collect(),
        ),
        TraceValue::Error(value) => Value::Error {
            basic: value.clone(),
            verbose: None,
        },
    }
}

fn handle_client_go_trace_event(
    context: &client_trace::TraceContext,
    category: Category,
    name: &str,
    fields: &[TraceField],
) {
    let mapped = map_category(category);
    if !is_enabled(mapped) {
        return;
    }
    let mut fields = fields
        .iter()
        .map(|field| Field::new(&field.name, field_value(field.encoded_value())))
        .collect::<Vec<_>>();
    if mapped == TraceCategory::UNKNOWN_CLIENT {
        fields.push(Field::new(
            "client_go_category",
            Value::U64(u64::from(category.as_raw())),
        ));
    }
    trace_event(&local_context(context), mapped, name, fields);
}

fn handle_client_go_is_category_enabled(category: Category) -> bool {
    is_enabled(map_category(category))
}

pub(crate) fn handle_trace_control_extractor(
    context: &client_trace::TraceContext,
) -> TraceControlFlags {
    let mut flags = TraceControlFlags::default();
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

    let Some(trace) = context.value::<Trace, Arc<Trace>>() else {
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

pub(crate) fn map_category(category: Category) -> TraceCategory {
    match category {
        Category::TransactionTwoPhaseCommit => TraceCategory::TXN_2PC,
        Category::TransactionLockResolve => TraceCategory::TXN_LOCK_RESOLVE,
        Category::KvRequest => TraceCategory::KV_REQUEST,
        Category::RegionCache => TraceCategory::REGION_CACHE,
        _ => TraceCategory::UNKNOWN_CLIENT,
    }
}

#[cfg(test)]
pub(crate) mod test_support {
    pub(crate) use super::handle_trace_control_extractor;
}
