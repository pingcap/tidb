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

//! Transcreation of Go `pkg/timer/tablestore/sql.go`: the SQL statement
//! builders that turn a timer record, a condition or an update descriptor into
//! an `ExecuteInternal` statement plus its argument list.
//!
//! Every builder Go keeps unexported is `pub` here so the upstream in-package
//! tests can be ported as integration tests; the doc comment on each names the
//! Go function it came from.

use crate::error::{Result, TimerError};
use crate::go_time::{GoTime, SECOND};
use crate::store::{Cond, OperatorTp, TimerCond, TimerUpdate};
use crate::timer::{EventExtra, ManualRequest, SchedEventStatus, TimerRecord};

use super::json::{parse, write_json_string, JsonValue};

/// `boundary:` Go's `[]any` SQL argument list, whose elements the upstream
/// tests compare with `require.Equal` including their dynamic type.
///
/// Rust has no `any`, so the closed set of types `sql.go` actually appends is
/// spelled as an enum. [`SqlArg::Null`] is Go's untyped `nil` (an unset
/// `WATERMARK`/`EVENT_START`), and [`SqlArg::Json`] is `json.RawMessage`.
///
/// Narrowing: Go distinguishes a nil `[]byte` from an empty one, and the tests
/// assert `[]byte(nil)`. `pkg/timer/api`'s Rust port already stores those
/// columns as `Vec<u8>`, so both collapse to an empty [`SqlArg::Bytes`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlArg {
    /// Go's untyped `nil`.
    Null,
    /// A Go `string`.
    Str(String),
    /// A Go `[]byte`.
    Bytes(Vec<u8>),
    /// A Go `bool`.
    Bool(bool),
    /// A Go `int64`.
    Int64(i64),
    /// A Go `uint64`.
    Uint64(u64),
    /// A Go `json.RawMessage`.
    Json(String),
}

impl SqlArg {
    /// Convenience for the many `string` arguments.
    pub fn str(text: impl Into<String>) -> Self {
        Self::Str(text.into())
    }
}

/// Go `indentString`.
pub fn indent_string(db_name: &str, table_name: &str) -> String {
    format!("`{db_name}`.`{table_name}`")
}

/// Go `timerExt`, the JSON document stored in the `TIMER_EXT` column.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TimerExt {
    /// Go `Tags` (`json:"tags,omitempty"`).
    pub tags: Vec<String>,
    /// Go `Manual` (`json:"manual,omitempty"`).
    pub manual: Option<ManualRequestObj>,
    /// Go `Event` (`json:"event,omitempty"`).
    pub event: Option<EventExtObj>,
}

impl TimerExt {
    /// Go `json.Marshal(ext)`: struct field order, `omitempty` on all three.
    pub fn marshal(&self) -> String {
        let mut out = String::from("{");
        let mut first = true;
        if !self.tags.is_empty() {
            first = false;
            out.push_str("\"tags\":");
            write_string_array(&mut out, &self.tags);
        }
        if let Some(manual) = &self.manual {
            if !first {
                out.push(',');
            }
            first = false;
            out.push_str("\"manual\":");
            manual.write(&mut out);
        }
        if let Some(event) = &self.event {
            if !first {
                out.push(',');
            }
            out.push_str("\"event\":");
            event.write(&mut out);
        }
        out.push('}');
        out
    }

    /// Go `json.Unmarshal(extJSON, &ext)`.
    pub fn unmarshal(text: &str) -> Result<Self> {
        let document = parse(text).map_err(TimerError::message)?;
        let tags = document
            .get("tags")
            .and_then(JsonValue::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(|item| item.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default();
        Ok(Self {
            tags,
            manual: document.get("manual").map(ManualRequestObj::from_json),
            event: document.get("event").map(EventExtObj::from_json),
        })
    }
}

fn write_string_array(out: &mut String, values: &[String]) {
    out.push('[');
    for (index, value) in values.iter().enumerate() {
        if index > 0 {
            out.push(',');
        }
        write_json_string(out, value);
    }
    out.push(']');
}

fn write_optional_string(out: &mut String, value: &Option<String>) {
    match value {
        Some(text) => write_json_string(out, text),
        None => out.push_str("null"),
    }
}

fn write_optional_i64(out: &mut String, value: &Option<i64>) {
    match value {
        Some(number) => out.push_str(&number.to_string()),
        None => out.push_str("null"),
    }
}

/// Go `manualRequestObj`, whose fields are pointers and carry no `omitempty`,
/// so an unset one marshals as an explicit `null`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ManualRequestObj {
    /// Go `RequestID` (`json:"request_id"`).
    pub request_id: Option<String>,
    /// Go `RequestTimeUnix` (`json:"request_time_unix"`).
    pub request_time_unix: Option<i64>,
    /// Go `TimeoutSec` (`json:"timeout_sec"`).
    pub timeout_sec: Option<i64>,
    /// Go `Processed` (`json:"processed"`).
    pub processed: Option<bool>,
    /// Go `EventID` (`json:"event_id"`).
    pub event_id: Option<String>,
}

impl ManualRequestObj {
    fn write(&self, out: &mut String) {
        out.push_str("{\"request_id\":");
        write_optional_string(out, &self.request_id);
        out.push_str(",\"request_time_unix\":");
        write_optional_i64(out, &self.request_time_unix);
        out.push_str(",\"timeout_sec\":");
        write_optional_i64(out, &self.timeout_sec);
        out.push_str(",\"processed\":");
        match self.processed {
            Some(value) => out.push_str(if value { "true" } else { "false" }),
            None => out.push_str("null"),
        }
        out.push_str(",\"event_id\":");
        write_optional_string(out, &self.event_id);
        out.push('}');
    }

    fn from_json(value: &JsonValue) -> Self {
        Self {
            request_id: value
                .get("request_id")
                .and_then(JsonValue::as_str)
                .map(str::to_string),
            request_time_unix: value.get("request_time_unix").and_then(JsonValue::as_i64),
            timeout_sec: value.get("timeout_sec").and_then(JsonValue::as_i64),
            processed: value.get("processed").and_then(JsonValue::as_bool),
            event_id: value
                .get("event_id")
                .and_then(JsonValue::as_str)
                .map(str::to_string),
        }
    }

    /// Go `(*manualRequestObj).ToManualRequest`, including its nil receiver
    /// case (`None` here).
    pub fn to_manual_request(this: Option<&Self>) -> ManualRequest {
        let mut request = ManualRequest::default();
        let Some(this) = this else {
            return request;
        };
        if let Some(value) = &this.request_id {
            request.manual_request_id = value.clone();
        }
        if let Some(value) = this.request_time_unix {
            request.manual_request_time = GoTime::from_unix(value, 0);
        }
        if let Some(value) = this.timeout_sec {
            request.manual_timeout = value * SECOND;
        }
        if let Some(value) = this.processed {
            request.manual_processed = value;
        }
        if let Some(value) = &this.event_id {
            request.manual_event_id = value.clone();
        }
        request
    }
}

/// Go `newManualRequestObj`.
pub fn new_manual_request_obj(manual: &ManualRequest) -> Option<ManualRequestObj> {
    if manual == &ManualRequest::default() {
        return None;
    }

    let mut obj = ManualRequestObj::default();
    if !manual.manual_request_id.is_empty() {
        obj.request_id = Some(manual.manual_request_id.clone());
    }
    if !manual.manual_request_time.is_zero() {
        obj.request_time_unix = Some(manual.manual_request_time.unix());
    }
    if manual.manual_timeout != 0 {
        obj.timeout_sec = Some(manual.manual_timeout / SECOND);
    }
    if manual.manual_processed {
        obj.processed = Some(true);
    }
    if !manual.manual_event_id.is_empty() {
        obj.event_id = Some(manual.manual_event_id.clone());
    }
    Some(obj)
}

/// Go `eventExtObj`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EventExtObj {
    /// Go `ManualRequestID` (`json:"manual_request_id"`).
    pub manual_request_id: Option<String>,
    /// Go `WatermarkUnix` (`json:"watermark_unix"`).
    pub watermark_unix: Option<i64>,
}

impl EventExtObj {
    fn write(&self, out: &mut String) {
        out.push_str("{\"manual_request_id\":");
        write_optional_string(out, &self.manual_request_id);
        out.push_str(",\"watermark_unix\":");
        write_optional_i64(out, &self.watermark_unix);
        out.push('}');
    }

    fn from_json(value: &JsonValue) -> Self {
        Self {
            manual_request_id: value
                .get("manual_request_id")
                .and_then(JsonValue::as_str)
                .map(str::to_string),
            watermark_unix: value.get("watermark_unix").and_then(JsonValue::as_i64),
        }
    }

    /// Go `(*eventExtObj).ToEventExtra`, including its nil receiver case.
    pub fn to_event_extra(this: Option<&Self>) -> EventExtra {
        let mut extra = EventExtra::default();
        let Some(this) = this else {
            return extra;
        };
        if let Some(value) = &this.manual_request_id {
            extra.event_manual_request_id = value.clone();
        }
        if let Some(value) = this.watermark_unix {
            extra.event_watermark = GoTime::from_unix(value, 0);
        }
        extra
    }
}

/// Go `newEventExtObj`.
pub fn new_event_ext_obj(extra: &EventExtra) -> Option<EventExtObj> {
    if extra == &EventExtra::default() {
        return None;
    }

    let mut obj = EventExtObj::default();
    if !extra.event_manual_request_id.is_empty() {
        obj.manual_request_id = Some(extra.event_manual_request_id.clone());
    }
    if !extra.event_watermark.is_zero() {
        obj.watermark_unix = Some(extra.event_watermark.unix());
    }
    Some(obj)
}

/// Go `buildInsertTimerSQL`.
pub fn build_insert_timer_sql(
    db_name: &str,
    table_name: &str,
    record: &TimerRecord,
) -> Result<(String, Vec<SqlArg>)> {
    let (watermark, watermark_format) = if record.spec.watermark.is_zero() {
        (SqlArg::Null, "%?")
    } else {
        (
            SqlArg::Int64(record.spec.watermark.unix()),
            "FROM_UNIXTIME(%?)",
        )
    };

    let (event_start, event_start_format) = if record.event_start.is_zero() {
        (SqlArg::Null, "%?")
    } else {
        (
            SqlArg::Int64(record.event_start.unix()),
            "FROM_UNIXTIME(%?)",
        )
    };

    let event_status = if record.event_status.as_str().is_empty() {
        SchedEventStatus::idle()
    } else {
        record.event_status.clone()
    };

    let ext = TimerExt {
        tags: record.spec.tags.clone(),
        manual: new_manual_request_obj(&record.manual_request),
        event: new_event_ext_obj(&record.event_extra),
    };

    let sql = format!(
        "INSERT INTO {} (\
NAMESPACE, \
TIMER_KEY, \
TIMER_DATA, \
TIMEZONE, \
SCHED_POLICY_TYPE, \
SCHED_POLICY_EXPR, \
HOOK_CLASS, \
WATERMARK, \
ENABLE, \
TIMER_EXT, \
EVENT_ID, \
EVENT_STATUS, \
EVENT_START, \
EVENT_DATA, \
SUMMARY_DATA, \
VERSION) \
VALUES (%?, %?, %?, %?, %?, %?, %?, {watermark_format}, %?, JSON_MERGE_PATCH('{{}}', %?), %?, %?, {event_start_format}, %?, %?, 1)",
        indent_string(db_name, table_name),
    );

    Ok((
        sql,
        vec![
            SqlArg::str(&record.spec.namespace),
            SqlArg::str(&record.spec.key),
            SqlArg::Bytes(record.spec.data.clone()),
            SqlArg::str(&record.spec.time_zone),
            SqlArg::str(record.spec.sched_policy_type.as_str()),
            SqlArg::str(&record.spec.sched_policy_expr),
            SqlArg::str(&record.spec.hook_class),
            watermark,
            SqlArg::Bool(record.spec.enable),
            SqlArg::Json(ext.marshal()),
            SqlArg::str(&record.event_id),
            SqlArg::str(event_status.as_str()),
            event_start,
            SqlArg::Bytes(record.event_data.clone()),
            SqlArg::Bytes(record.summary_data.clone()),
        ],
    ))
}

/// The column list `buildSelectTimerSQL` reads back, in `store.go`'s order.
pub const SELECT_TIMER_COLUMNS: &str = "ID, \
NAMESPACE, \
TIMER_KEY, \
TIMER_DATA, \
TIMEZONE, \
SCHED_POLICY_TYPE, \
SCHED_POLICY_EXPR, \
HOOK_CLASS, \
WATERMARK, \
ENABLE, \
TIMER_EXT, \
EVENT_STATUS, \
EVENT_ID, \
EVENT_DATA, \
EVENT_START, \
SUMMARY_DATA, \
CREATE_TIME, \
UPDATE_TIME, \
VERSION";

/// Go `buildSelectTimerSQL`.
pub fn build_select_timer_sql(
    db_name: &str,
    table_name: &str,
    cond: Option<&dyn Cond>,
) -> Result<(String, Vec<SqlArg>)> {
    let (criteria, args) = build_cond_criteria(cond, Vec::with_capacity(8))?;
    let sql = format!(
        "SELECT {SELECT_TIMER_COLUMNS} FROM {} WHERE {criteria}",
        indent_string(db_name, table_name),
    );
    Ok((sql, args))
}

/// Go `buildCondCriteria`.
///
/// Go dispatches on the dynamic type of the `api.Cond` interface; the Rust
/// `Cond` trait exposes [`Cond::as_timer_cond`]/[`Cond::as_operator`] for the
/// same purpose, and an implementation answering neither reaches Go's
/// `unsupported condition type` arm.
pub fn build_cond_criteria(
    cond: Option<&dyn Cond>,
    args: Vec<SqlArg>,
) -> Result<(String, Vec<SqlArg>)> {
    let Some(cond) = cond else {
        return Ok(("1".to_string(), args));
    };

    if let Some(timer_cond) = cond.as_timer_cond() {
        return build_timer_cond_criteria(timer_cond, args);
    }

    if let Some(operator) = cond.as_operator() {
        return build_operator_criteria(operator, args);
    }

    Err(TimerError::message("unsupported condition type"))
}

/// Go `buildTimerCondCriteria`.
pub fn build_timer_cond_criteria(
    cond: &TimerCond,
    mut args: Vec<SqlArg>,
) -> Result<(String, Vec<SqlArg>)> {
    let mut items: Vec<&str> = Vec::new();

    if let Some(value) = cond.id.get() {
        items.push("ID = %?");
        args.push(SqlArg::str(value));
    }

    if let Some(value) = cond.namespace.get() {
        items.push("NAMESPACE = %?");
        args.push(SqlArg::str(value));
    }

    if let Some(value) = cond.key.get() {
        if cond.key_prefix {
            items.push("TIMER_KEY LIKE %?");
            args.push(SqlArg::Str(format!("{value}%")));
        } else {
            items.push("TIMER_KEY = %?");
            args.push(SqlArg::str(value));
        }
    }

    if let Some(values) = cond.tags.get() {
        if !values.is_empty() {
            let mut encoded = String::new();
            write_string_array(&mut encoded, values);
            items.push("JSON_EXTRACT(TIMER_EXT, '$.tags') IS NOT NULL");
            items.push("JSON_CONTAINS((TIMER_EXT->'$.tags'), %?)");
            args.push(SqlArg::Json(encoded));
        }
    }

    if items.is_empty() {
        return Ok(("1".to_string(), args));
    }

    Ok((items.join(" AND "), args))
}

/// Go `buildOperatorCriteria`.
pub fn build_operator_criteria(
    operator: &crate::store::Operator,
    mut args: Vec<SqlArg>,
) -> Result<(String, Vec<SqlArg>)> {
    if operator.children.is_empty() {
        return Err(TimerError::message("children should not be empty"));
    }

    let op_str = match operator.op {
        OperatorTp::And => "AND",
        OperatorTp::Or => "OR",
    };

    let mut criteria_list = Vec::with_capacity(operator.children.len());
    for child in &operator.children {
        let (mut criteria, next_args) = build_cond_criteria(Some(child.as_ref()), args)?;
        args = next_args;

        if operator.children.len() > 1 && criteria != "1" && criteria != "0" {
            criteria = format!("({criteria})");
        }

        criteria_list.push(criteria);
    }

    let mut criteria = criteria_list.join(&format!(" {op_str} "));
    if operator.not {
        criteria = match criteria.as_str() {
            "0" => "1".to_string(),
            "1" => "0".to_string(),
            other => format!("!({other})"),
        };
    }
    Ok((criteria, args))
}

/// Go `buildUpdateTimerSQL`.
pub fn build_update_timer_sql(
    db_name: &str,
    tbl_name: &str,
    timer_id: &str,
    update: &TimerUpdate,
) -> Result<(String, Vec<SqlArg>)> {
    let (criteria, mut args) = build_update_criteria(update, Vec::with_capacity(6))?;
    let sql = format!(
        "UPDATE {} SET {criteria} WHERE ID = %?",
        indent_string(db_name, tbl_name),
    );
    args.push(SqlArg::str(timer_id));
    Ok((sql, args))
}

/// Go `buildUpdateCriteria`.
pub fn build_update_criteria(
    update: &TimerUpdate,
    mut args: Vec<SqlArg>,
) -> Result<(String, Vec<SqlArg>)> {
    let mut update_fields: Vec<String> = Vec::new();

    if let Some(value) = update.enable.get() {
        update_fields.push("ENABLE = %?".to_string());
        args.push(SqlArg::Bool(*value));
    }

    // Go accumulates these into a `map[string]any`, which `json.Marshal`
    // renders with its keys sorted; the rendered members are therefore
    // collected here and emitted in the same sorted order.
    let mut ext_fields: Vec<(&str, String)> = Vec::new();
    if let Some(value) = update.tags.get() {
        let mut encoded = String::new();
        if value.is_empty() {
            encoded.push_str("null");
        } else {
            write_string_array(&mut encoded, value);
        }
        ext_fields.push(("tags", encoded));
    }

    if let Some(value) = update.manual_request.get() {
        let mut encoded = String::new();
        match new_manual_request_obj(value) {
            Some(obj) => obj.write(&mut encoded),
            None => encoded.push_str("null"),
        }
        ext_fields.push(("manual", encoded));
    }

    if let Some(value) = update.event_extra.get() {
        let mut encoded = String::new();
        match new_event_ext_obj(value) {
            Some(obj) => obj.write(&mut encoded),
            None => encoded.push_str("null"),
        }
        ext_fields.push(("event", encoded));
    }

    if let Some(value) = update.time_zone.get() {
        update_fields.push("TIMEZONE = %?".to_string());
        args.push(SqlArg::str(value));
    }

    if let Some(value) = update.sched_policy_type.get() {
        update_fields.push("SCHED_POLICY_TYPE = %?".to_string());
        args.push(SqlArg::str(value.as_str()));
    }

    if let Some(value) = update.sched_policy_expr.get() {
        update_fields.push("SCHED_POLICY_EXPR = %?".to_string());
        args.push(SqlArg::str(value));
    }

    if let Some(value) = update.event_status.get() {
        update_fields.push("EVENT_STATUS = %?".to_string());
        args.push(SqlArg::str(value.as_str()));
    }

    if let Some(value) = update.event_id.get() {
        update_fields.push("EVENT_ID = %?".to_string());
        args.push(SqlArg::str(value));
    }

    if let Some(value) = update.event_data.get() {
        update_fields.push("EVENT_DATA = %?".to_string());
        args.push(SqlArg::Bytes(value.clone()));
    }

    if let Some(value) = update.event_start.get() {
        if value.is_zero() {
            update_fields.push("EVENT_START = NULL".to_string());
        } else {
            update_fields.push("EVENT_START = FROM_UNIXTIME(%?)".to_string());
            args.push(SqlArg::Int64(value.unix()));
        }
    }

    if let Some(value) = update.watermark.get() {
        if value.is_zero() {
            update_fields.push("WATERMARK = NULL".to_string());
        } else {
            update_fields.push("WATERMARK = FROM_UNIXTIME(%?)".to_string());
            args.push(SqlArg::Int64(value.unix()));
        }
    }

    if let Some(value) = update.summary_data.get() {
        update_fields.push("SUMMARY_DATA = %?".to_string());
        args.push(SqlArg::Bytes(value.clone()));
    }

    if !ext_fields.is_empty() {
        ext_fields.sort_by(|left, right| left.0.cmp(right.0));
        let mut encoded = String::from("{");
        for (index, (key, value)) in ext_fields.iter().enumerate() {
            if index > 0 {
                encoded.push(',');
            }
            write_json_string(&mut encoded, key);
            encoded.push(':');
            encoded.push_str(value);
        }
        encoded.push('}');
        update_fields.push("TIMER_EXT = JSON_MERGE_PATCH(TIMER_EXT, %?)".to_string());
        args.push(SqlArg::Json(encoded));
    }

    update_fields.push("VERSION = VERSION + 1".to_string());
    Ok((update_fields.join(", "), args))
}

/// Go `buildDeleteTimerSQL`.
pub fn build_delete_timer_sql(
    db_name: &str,
    tbl_name: &str,
    timer_id: &str,
) -> (String, Vec<SqlArg>) {
    (
        format!(
            "DELETE FROM {} WHERE ID = %?",
            indent_string(db_name, tbl_name)
        ),
        vec![SqlArg::str(timer_id)],
    )
}
