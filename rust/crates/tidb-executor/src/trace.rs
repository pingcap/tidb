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

//! `TRACE [FORMAT = 'row' | 'json' | 'log'] <statement>` (Go `TraceExec`).
//!
//! Go runs the statement under a recording tracer's `trace` span; every
//! region the session, planner, executors, coprocessor and KV client open
//! while it runs records a span, and the result is the span tree (`row`),
//! appdash's JSON (`json`) or the span starts and logged events (`log`).
//! [`StatementTrace`] is that tracer: the statement runs with its root span
//! as the thread's span (`tidb_util::tracing`), the regions opened under it
//! are the recorded spans, and the formats render what was recorded.

use std::sync::{Arc, Mutex, PoisonError};

use tidb_datatype::{core_time_from_datetime, Datum, FieldType, FieldTypeCode, Time, TimeType};
use tidb_util::tracing::{RawSpan, ThreadSpanGuard, Tracer, TRACE_EVENT_KEY};

/// One recorded span of the tree.
#[derive(Clone, Debug)]
pub struct Span {
    name: String,
    /// Offset from the trace start, in nanoseconds.
    start: i64,
    end: i64,
    children: Vec<Span>,
    /// Appdash's log annotations: `(offset, message)` per log record,
    /// `materializeWithJSON`'s form (JSON form only).
    logs: Vec<(i64, String)>,
    /// `(offset, message)` per `logutil.TraceEventKey` field (log form).
    events: Vec<(i64, String)>,
    /// The span's tags, rendered `key:value` (Go `RawSpan.Tags`).
    tags: std::collections::BTreeMap<String, String>,
    /// The order the recorder received the span in (Go's in-memory
    /// recorder keeps spans in finish order).
    finished: usize,
    /// The span's id, in the order spans were opened.
    opened: u64,
    /// Where appdash's `MemoryStore` put the span among its siblings
    /// ([`appdash_sub_order`]): the JSON form's child order.
    appdash_rank: usize,
}

/// Go `TraceExec`'s tracer over one statement: the recorder, its `trace`
/// root span and the clock the output's times are measured from.
pub struct StatementTrace {
    root: tidb_util::tracing::Span,
    spans: Arc<Mutex<Vec<RawSpan>>>,
}

impl StatementTrace {
    /// Go `basictracer.New(recorder)` and `tracer.StartSpan("trace")`.
    #[must_use]
    pub fn start() -> Self {
        let spans = Arc::new(Mutex::new(Vec::new()));
        let recorded = Arc::clone(&spans);
        let tracer = Arc::new(Tracer::new(Arc::new(move |span| {
            recorded
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(span);
        })));
        Self {
            root: tracer.start_span("trace"),
            spans,
        }
    }

    /// Go `opentracing.ContextWithSpan(ctx, span)`: the statement this
    /// thread runs until the guard drops records its spans under the root.
    #[must_use]
    pub fn enter(&self) -> ThreadSpanGuard {
        tidb_util::tracing::enter_thread_trace(self.root.clone())
    }

    /// Go `logutil.Eventf(ctx, ...)` on the root span.
    pub fn event(&self, message: &str) {
        self.root.log_event(message);
    }

    /// Go `span.Finish()`: the recorded tree, rooted at `trace`.
    #[must_use]
    pub fn finish(self) -> (Span, std::time::SystemTime) {
        self.root.finish();
        let spans = std::mem::take(&mut *self.spans.lock().unwrap_or_else(PoisonError::into_inner));
        build_tree(&spans)
    }
}

/// The recorded spans as a tree under the root (the span without a parent),
/// with times as offsets from the root's start, which is returned too.
#[must_use]
pub fn build_tree(spans: &[RawSpan]) -> (Span, std::time::SystemTime) {
    let started = spans
        .iter()
        .find(|span| span.parent_span_id == 0)
        .map_or(std::time::UNIX_EPOCH, |root| root.start);
    let offset = |at: std::time::SystemTime| match at.duration_since(started) {
        Ok(after) => i64::try_from(after.as_nanos()).unwrap_or(i64::MAX),
        Err(before) => -i64::try_from(before.duration().as_nanos()).unwrap_or(i64::MAX),
    };
    let appdash_rank = appdash_sub_order(spans);
    let node = |(finished, raw): (usize, &RawSpan)| {
        let start = offset(raw.start);
        let mut logs = Vec::new();
        let mut events = Vec::new();
        for record in &raw.logs {
            let at = offset(record.timestamp);
            logs.push((at, materialize_with_json(&record.fields)));
            events.extend(
                record
                    .fields
                    .iter()
                    .filter(|(key, _)| key == TRACE_EVENT_KEY)
                    .map(|(_, value)| (at, value.clone())),
            );
        }
        Span {
            name: raw.operation.clone(),
            start,
            end: start + i64::try_from(raw.duration.as_nanos()).unwrap_or(i64::MAX),
            children: Vec::new(),
            logs,
            events,
            tags: raw.tags.clone(),
            finished,
            opened: raw.context.span_id,
            appdash_rank: appdash_rank
                .get(&raw.context.span_id)
                .copied()
                .unwrap_or(usize::MAX),
        }
    };
    let mut nodes: Vec<Option<Span>> = spans.iter().enumerate().map(node).map(Some).collect();
    let by_id = spans
        .iter()
        .enumerate()
        .map(|(index, span)| (span.context.span_id, index))
        .collect::<std::collections::HashMap<_, _>>();
    let index_of = |span_id: u64| by_id.get(&span_id).copied();
    // Spans do not finish in tree order (a coprocessor send outlives
    // `distsql.Select`), so attach children deepest first: a span moves only
    // once its subtree is whole.
    let depth = |mut index: usize| {
        let mut depth = 0;
        while let Some(parent) = index_of(spans[index].parent_span_id) {
            depth += 1;
            index = parent;
            if depth > spans.len() {
                break;
            }
        }
        depth
    };
    let mut order = (0..spans.len()).collect::<Vec<_>>();
    order.sort_by_cached_key(|index| std::cmp::Reverse(depth(*index)));
    let mut root = None;
    for index in order {
        let Some(span) = nodes[index].take() else {
            continue;
        };
        match index_of(spans[index].parent_span_id) {
            Some(parent) => {
                if let Some(parent) = nodes[parent].as_mut() {
                    parent.children.push(span);
                }
            }
            None if spans[index].parent_span_id == 0 && root.is_none() => root = Some(span),
            None => {}
        }
    }
    let root = root.unwrap_or_else(|| Span {
        name: "trace".to_owned(),
        start: 0,
        end: 0,
        children: Vec::new(),
        logs: Vec::new(),
        events: Vec::new(),
        tags: std::collections::BTreeMap::new(),
        finished: 0,
        opened: 0,
        appdash_rank: 0,
    });
    (root, started)
}

/// appdash `MemoryStore.Collect` over the spans in the order the recorder
/// took them (a span is collected when it finishes): each span's position
/// in its parent's `Sub`, which `json.Marshal` keeps as is. A span whose
/// parent has not been collected yet waits as a temporary child of the
/// (temporary) root, the first span collected until its parent arrives, and
/// moves under its parent, after the children already there, when that
/// parent is collected.
fn appdash_sub_order(spans: &[RawSpan]) -> std::collections::HashMap<u64, usize> {
    struct Node {
        id: u64,
        parent: u64,
        sub: Vec<usize>,
    }
    let mut nodes: Vec<Node> = Vec::new();
    let mut by_id = std::collections::HashMap::new();
    let mut root: Option<usize> = None;
    // appdash `insert`: under the parent when it is collected, else a
    // temporary child of the root.
    fn insert(
        nodes: &mut [Node],
        by_id: &std::collections::HashMap<u64, usize>,
        root: usize,
        t: usize,
    ) {
        let target = by_id.get(&nodes[t].parent).copied().unwrap_or(root);
        nodes[target].sub.push(t);
    }
    // appdash `reattachChildren`: `src`'s children whose parent is `dst`
    // move to `dst`, in order.
    fn reattach(nodes: &mut [Node], dst: usize, src: usize) {
        let dst_id = nodes[dst].id;
        let (moved, kept): (Vec<usize>, Vec<usize>) = std::mem::take(&mut nodes[src].sub)
            .into_iter()
            .partition(|child| nodes[*child].parent == dst_id);
        nodes[src].sub = kept;
        nodes[dst].sub.extend(moved);
    }
    for raw in spans {
        let id = raw.context.span_id;
        if by_id.contains_key(&id) {
            continue;
        }
        let s = nodes.len();
        nodes.push(Node {
            id,
            parent: raw.parent_span_id,
            sub: Vec::new(),
        });
        by_id.insert(id, s);
        let mut current = *root.get_or_insert(s);
        let is_root = raw.parent_span_id == 0;
        let is_temp_root_parent = nodes[current].parent == id;
        if s != current && (is_root || is_temp_root_parent) {
            let old_root = current;
            current = s;
            root = Some(s);
            reattach(&mut nodes, current, old_root);
            insert(&mut nodes, &by_id, current, old_root);
            let old_id = nodes[old_root].id;
            let (moved, kept): (Vec<usize>, Vec<usize>) = std::mem::take(&mut nodes[old_root].sub)
                .into_iter()
                .partition(|child| nodes[*child].parent != old_id);
            nodes[old_root].sub = kept;
            nodes[current].sub.extend(moved);
        }
        if !is_root && s != current {
            insert(&mut nodes, &by_id, current, s);
        }
        if s != current {
            reattach(&mut nodes, s, current);
        }
    }
    let mut rank = std::collections::HashMap::new();
    for node in &nodes {
        for (position, child) in node.sub.iter().enumerate() {
            rank.insert(nodes[*child].id, position);
        }
    }
    rank
}

/// appdash `materializeWithJSON`: a record whose only field is the event
/// is its text, any other record the JSON object of its fields.
fn materialize_with_json(fields: &[(String, String)]) -> String {
    let map = fields
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<std::collections::BTreeMap<_, _>>();
    if map.len() == 1 {
        if let Some(event) = map.get(TRACE_EVENT_KEY) {
            return event.clone();
        }
    }
    tidb_util::stringutil::go_json_marshal(&serde_json::Value::Object(
        map.into_iter()
            .map(|(key, value)| (key, serde_json::Value::String(value)))
            .collect(),
    ))
}

fn json_string(text: &str) -> String {
    tidb_util::stringutil::go_json_marshal(&serde_json::Value::String(text.to_owned()))
}

fn varchar() -> FieldType {
    FieldType::new(FieldTypeCode::VarString)
}

fn clock(started: std::time::SystemTime, offset: i64) -> chrono::DateTime<chrono::Local> {
    let moment =
        started + std::time::Duration::from_nanos(u64::try_from(offset.max(0)).unwrap_or(0));
    moment.into()
}

fn go_duration(nanos: i64) -> String {
    tidb_model::go_duration::format_go_duration(nanos)
}

/// Go `dfsTree`: the `operation | startTS | duration` rows.
#[must_use]
pub fn row_format(
    root: &Span,
    started: std::time::SystemTime,
) -> (Vec<(String, FieldType)>, Vec<Vec<Datum>>) {
    fn walk(
        span: &Span,
        prefix: &str,
        is_last: bool,
        started: std::time::SystemTime,
        rows: &mut Vec<Vec<Datum>>,
    ) {
        let (suffix, new_prefix) = if prefix.is_empty() {
            (String::new(), format!("{prefix}  "))
        } else if is_last {
            ("└─".to_owned(), format!("{prefix}  "))
        } else {
            ("├─".to_owned(), format!("{prefix}│ "))
        };
        rows.push(vec![
            Datum::Bytes(format!("{prefix}{suffix}{}", span.name).into_bytes()),
            Datum::Bytes(
                clock(started, span.start)
                    .format("%H:%M:%S%.6f")
                    .to_string()
                    .into_bytes(),
            ),
            Datum::Bytes(go_duration(span.end - span.start).into_bytes()),
        ]);
        let mut children = span.children.iter().collect::<Vec<_>>();
        // Go sorts siblings by their start (`dfsTree`); its nanosecond
        // monotonic clock never ties, so equal starts keep the order the
        // spans were opened in -- client spans timed from one recorded
        // instant do tie here.
        children.sort_by_key(|child| (child.start, child.opened));
        let last = children.len().saturating_sub(1);
        for (index, child) in children.into_iter().enumerate() {
            walk(child, &new_prefix, index == last, started, rows);
        }
    }
    let mut rows = Vec::new();
    walk(root, "", false, started, &mut rows);
    (
        ["operation", "startTS", "duration"]
            .into_iter()
            .map(|name| (name.to_owned(), varchar()))
            .collect(),
        rows,
    )
}

/// Go `generateLogResult` under the `ORDER BY time` its executor builder
/// adds: per span, in the order the recorder received them, a
/// `start span` row and a row per logged event (with the span's tags), all
/// sorted by time. Go sorts the microsecond `TIMESTAMP(6)` column, leaving
/// rows of one microsecond in no fixed order; these keep their full order,
/// a span opened earlier first on a tie (a detail tree's spans share their
/// start).
#[must_use]
pub fn log_format(
    root: &Span,
    started: std::time::SystemTime,
) -> (Vec<(String, FieldType)>, Vec<Vec<Datum>>) {
    fn collect<'a>(span: &'a Span, spans: &mut Vec<&'a Span>) {
        spans.push(span);
        for child in &span.children {
            collect(child, spans);
        }
    }
    let mut spans = Vec::new();
    collect(root, &mut spans);
    spans.sort_by_key(|span| span.finished);
    let timestamp = |offset: i64| {
        let local = clock(started, offset);
        Time::new(core_time_from_datetime(local), TimeType::Timestamp, 6)
            .map_or(Datum::Null, Datum::new_time)
    };
    let text = |value: &str| Datum::Bytes(value.as_bytes().to_vec());
    let mut rows = Vec::new();
    for span in &spans {
        rows.push((
            (span.start, span.opened),
            vec![
                timestamp(span.start),
                text(&format!("--- start span {} ----", span.name)),
                text(""),
                text(&span.name),
            ],
        ));
        // Go `fmt.Sprintf("%v", span.Tags)`.
        let tags = if span.tags.is_empty() {
            String::new()
        } else {
            let pairs = span
                .tags
                .iter()
                .map(|(key, value)| format!("{key}:{value}"))
                .collect::<Vec<_>>();
            format!("map[{}]", pairs.join(" "))
        };
        for (offset, message) in &span.events {
            rows.push((
                (*offset, span.opened),
                vec![
                    timestamp(*offset),
                    text(message),
                    text(&tags),
                    text(&span.name),
                ],
            ));
        }
    }
    rows.sort_by_key(|(at, _)| *at);
    let rows = rows.into_iter().map(|(_, row)| row).collect();
    let mut timestamp_type = FieldType::new(FieldTypeCode::Timestamp);
    timestamp_type.set_decimal(6);
    (
        vec![
            ("time".to_owned(), timestamp_type),
            ("event".to_owned(), varchar()),
            ("tags".to_owned(), varchar()),
            ("spanName".to_owned(), varchar()),
        ],
        rows,
    )
}

fn base64(bytes: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let value = (u32::from(chunk[0]) << 16)
            | (u32::from(*chunk.get(1).unwrap_or(&0)) << 8)
            | u32::from(*chunk.get(2).unwrap_or(&0));
        out.push(char::from(TABLE[(value >> 18) as usize & 63]));
        out.push(char::from(TABLE[(value >> 12) as usize & 63]));
        out.push(if chunk.len() > 1 {
            char::from(TABLE[(value >> 6) as usize & 63])
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            char::from(TABLE[value as usize & 63])
        } else {
            '='
        });
    }
    out
}

/// Go `json.Marshal(traces)` over appdash's trace: one document split into
/// 4096-byte rows.
#[must_use]
pub fn json_format(
    root: &Span,
    started: std::time::SystemTime,
) -> (Vec<(String, FieldType)>, Vec<Vec<Datum>>) {
    fn rfc3339(started: std::time::SystemTime, offset: i64, nanos: bool) -> String {
        let local = clock(started, offset);
        if nanos {
            local.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, false)
        } else {
            local.to_rfc3339_opts(chrono::SecondsFormat::Micros, false)
        }
    }
    fn annotation(key: &str, value: Option<&str>) -> String {
        let value = value.map_or_else(
            || "null".to_owned(),
            |value| json_string(&base64(value.as_bytes())),
        );
        format!("{{\"Key\":{},\"Value\":{value}}}", json_string(key))
    }
    fn node(
        span: &Span,
        trace_id: &str,
        parent: &str,
        counter: &mut u64,
        started: std::time::SystemTime,
    ) -> String {
        *counter += 1;
        let span_id = format!("{:016x}", 0x5a17_0000_0000_0000_u64 + *counter * 0x1f3d);
        let mut annotations = vec![
            annotation("Name", Some(&span.name)),
            annotation("_schema:name", None),
        ];
        for (offset, message) in &span.logs {
            annotations.push(annotation("Msg", Some(message)));
            annotations.push(annotation("Time", Some(&rfc3339(started, *offset, false))));
            annotations.push(annotation("_schema:log", None));
        }
        // appdash's recorder: one annotation per tag, its `%+v` value.
        for (key, value) in &span.tags {
            annotations.push(annotation(key, Some(value)));
        }
        annotations.push(annotation(
            "Span.End",
            Some(&rfc3339(started, span.end, true)),
        ));
        annotations.push(annotation(
            "Span.Start",
            Some(&rfc3339(started, span.start, true)),
        ));
        annotations.push(annotation("_schema:Timespan", None));
        let mut children = span.children.iter().collect::<Vec<_>>();
        // appdash's `Sub` order; only the row form sorts by start.
        children.sort_by_key(|child| child.appdash_rank);
        let sub = if children.is_empty() {
            "null".to_owned()
        } else {
            format!(
                "[{}]",
                children
                    .into_iter()
                    .map(|child| node(child, trace_id, &span_id, counter, started))
                    .collect::<Vec<_>>()
                    .join(",")
            )
        };
        format!(
            "{{\"ID\":{{\"Trace\":\"{trace_id}\",\"Span\":\"{span_id}\",\"Parent\":\"{parent}\"}},\"Annotations\":[{}],\"Sub\":{sub}}}",
            annotations.join(",")
        )
    }
    let trace_id = format!("{:016x}", 0x0584_bcf9_3725_8368_u64);
    let mut counter = 0;
    let data = format!(
        "[{}]",
        node(root, &trace_id, "0000000000000000", &mut counter, started)
    );
    let mut rows = Vec::new();
    let bytes = data.as_bytes();
    let mut offset = 0;
    while bytes.len() - offset > 4096 {
        rows.push(vec![Datum::Bytes(bytes[offset..offset + 4096].to_vec())]);
        offset += 4096;
    }
    rows.push(vec![Datum::Bytes(bytes[offset..].to_vec())]);
    (vec![("operation".to_owned(), varchar())], rows)
}

/// The output of `TRACE` for `format` (`row` when empty).
///
/// # Errors
/// Go's message for an unknown format.
pub fn render(
    format: &str,
    root: &Span,
    started: std::time::SystemTime,
) -> Result<(Vec<(String, FieldType)>, Vec<Vec<Datum>>), String> {
    match format.to_lowercase().as_str() {
        "" | "row" => Ok(row_format(root, started)),
        "json" => Ok(json_format(root, started)),
        "log" => Ok(log_format(root, started)),
        _ => Err("trace format should be one of 'row', 'log' or 'json'".to_owned()),
    }
}

/// Go `TraceExec` argument checks, before the statement runs.
///
/// # Errors
/// Go's message for an unknown format or plan target.
pub fn check_options(trace: &tidb_ast::TraceStmt) -> Result<(), String> {
    if trace.trace_plan {
        if !trace.trace_plan_target.is_empty()
            && !trace.trace_plan_target.eq_ignore_ascii_case("estimation")
        {
            return Err("trace plan target should only be 'estimation'".to_owned());
        }
        return Err("this feature has been deprecated".to_owned());
    }
    match trace.format.to_lowercase().as_str() {
        "" | "row" | "json" | "log" => Ok(()),
        _ => Err("trace format should be one of 'row', 'log' or 'json'".to_owned()),
    }
}

/// The traced statement's text as written (Go's `QueryString`), falling back
/// to its restored form when the parser recorded none.
#[must_use]
pub fn statement_text(stmt: &tidb_ast::Stmt) -> String {
    let written = String::from_utf8_lossy(stmt.node_text().text())
        .trim()
        .to_owned();
    if written.is_empty() {
        stmt.restore()
    } else {
        written
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The span tree as `name{ child child }`, children in start order.
    fn shape(span: &Span) -> String {
        let mut children = span.children.iter().collect::<Vec<_>>();
        children.sort_by_key(|child| (child.start, child.opened));
        if children.is_empty() {
            return span.name.clone();
        }
        let inner = children
            .into_iter()
            .map(shape)
            .collect::<Vec<_>>()
            .join(" ");
        format!("{}{{ {inner} }}", span.name)
    }

    fn text(datum: &Datum) -> String {
        match datum {
            Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
            other => format!("{other:?}"),
        }
    }

    /// Go `TraceExec`: the statement runs under the `trace` span; regions
    /// opened on the thread nest as Go's ctx nests them, a region whose
    /// ending runs in another frame included, and the root's events and
    /// `runStmt`'s `LogKV` land where Go logs them.
    #[test]
    fn a_statement_trace_records_the_regions_its_run_opens() {
        let trace = StatementTrace::start();
        let entered = trace.enter();
        {
            let _execute = tidb_util::tracing::begin_thread_region_scope("session.ExecuteStmt");
            tidb_util::tracing::begin_thread_region("executor.Compile", Some("SELECT 1".into()));
            tidb_util::tracing::start_thread_region("planner.Preprocess").end();
            tidb_util::tracing::start_thread_region("planner.Optimize").end();
            let text = tidb_util::tracing::end_thread_region("executor.Compile").flatten();
            let run = tidb_util::tracing::begin_thread_region("session.runStmt", None).unwrap();
            run.log_kv("sql", &text.unwrap());
            let select = tidb_util::tracing::start_thread_region_ex("distsql.Select");
            tidb_util::tracing::start_thread_region("copr.buildCopTasks").end();
            select.end();
        }
        for _ in 0..2 {
            let _next = tidb_util::tracing::start_thread_region_ex("*executor.TableDualExec.Next");
        }
        drop(entered);
        assert!(tidb_util::tracing::current_span().is_none());
        trace.event("execute done, ReturnRow: 1, ModifyRow: 0");
        trace.event("execute done, modify row: 0");
        let (root, started) = trace.finish();
        assert_eq!(
            shape(&root),
            "trace{ session.ExecuteStmt{ executor.Compile{ planner.Preprocess \
             planner.Optimize } session.runStmt{ distsql.Select{ copr.buildCopTasks } } } \
             *executor.TableDualExec.Next *executor.TableDualExec.Next }"
        );
        let execute = &root.children[0];
        let run = execute
            .children
            .iter()
            .find(|child| child.name == "session.runStmt")
            .unwrap();
        assert_eq!(run.logs.len(), 1);
        assert_eq!(run.logs[0].1, r#"{"sql":"SELECT 1"}"#);
        assert!(run.events.is_empty());
        assert_eq!(
            root.events
                .iter()
                .map(|(_, event)| event.as_str())
                .collect::<Vec<_>>(),
            [
                "execute done, ReturnRow: 1, ModifyRow: 0",
                "execute done, modify row: 0"
            ]
        );

        // Go `generateLogResult` under `ORDER BY time`: a start row per span
        // and a row per event, the events after the spans they follow.
        let (_, rows) = log_format(&root, started);
        let events = rows.iter().map(|row| text(&row[1])).collect::<Vec<_>>();
        assert_eq!(events.len(), 10 + 2);
        assert_eq!(events[0], "--- start span trace ----");
        assert_eq!(events[10], "execute done, ReturnRow: 1, ModifyRow: 0");
        assert_eq!(events[11], "execute done, modify row: 0");
        assert!(rows.iter().all(|row| text(&row[2]).is_empty()));

        // The row form prints the tree in start order.
        let (_, rows) = row_format(&root, started);
        let names = rows.iter().map(|row| text(&row[0])).collect::<Vec<_>>();
        assert_eq!(
            names,
            [
                "trace",
                "  ├─session.ExecuteStmt",
                "  │ ├─executor.Compile",
                "  │ │ ├─planner.Preprocess",
                "  │ │ └─planner.Optimize",
                "  │ └─session.runStmt",
                "  │   └─distsql.Select",
                "  │     └─copr.buildCopTasks",
                "  ├─*executor.TableDualExec.Next",
                "  └─*executor.TableDualExec.Next",
            ]
        );

        // The JSON form keeps appdash's `Sub` order, built as the spans
        // finish: children waiting for their parent move under it in turn,
        // so `session.runStmt` precedes `executor.Compile` and
        // `planner.Optimize` precedes `planner.Preprocess`, as in Go's.
        let (_, rows) = json_format(&root, started);
        let json = rows.iter().map(|row| text(&row[0])).collect::<String>();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        fn json_names(node: &serde_json::Value, out: &mut Vec<String>) {
            let name = node["Annotations"]
                .as_array()
                .unwrap()
                .iter()
                .find(|annotation| annotation["Key"] == "Name")
                .unwrap()["Value"]
                .as_str()
                .unwrap()
                .to_owned();
            out.push(name);
            for sub in node["Sub"].as_array().into_iter().flatten() {
                json_names(sub, out);
            }
        }
        let mut order = Vec::new();
        json_names(&parsed[0], &mut order);
        let expected = [
            "trace",
            "*executor.TableDualExec.Next",
            "*executor.TableDualExec.Next",
            "session.ExecuteStmt",
            "session.runStmt",
            "distsql.Select",
            "copr.buildCopTasks",
            "executor.Compile",
            "planner.Optimize",
            "planner.Preprocess",
        ]
        .map(|name| base64(name.as_bytes()));
        assert_eq!(order, expected);
    }

    /// A child recorded after its parent finished (a coprocessor send
    /// outliving `distsql.Select`) still lands under it; an async-tagged
    /// span carries Go's `map[async:true]` on its event rows and the tag
    /// annotation in the JSON form.
    #[test]
    fn late_children_and_tags_render_as_go_does() {
        let trace = StatementTrace::start();
        let entered = trace.enter();
        let select = tidb_util::tracing::start_thread_region_ex("distsql.Select");
        let parent = tidb_util::tracing::current_span().unwrap();
        select.end();
        let send = parent
            .tracer()
            .start_span_child_of("regionRequest.SendReqCtx", parent.context());
        send.set_tag("async", "true");
        send.log_event("send Cop request to region 7 at store");
        send.finish();
        drop(entered);
        let (root, started) = trace.finish();
        assert_eq!(
            shape(&root),
            "trace{ distsql.Select{ regionRequest.SendReqCtx } }"
        );
        let (_, rows) = log_format(&root, started);
        let event = rows
            .iter()
            .find(|row| text(&row[1]).starts_with("send Cop"))
            .unwrap();
        assert_eq!(text(&event[2]), "map[async:true]");
        assert_eq!(text(&event[3]), "regionRequest.SendReqCtx");
        let (_, rows) = json_format(&root, started);
        let json = rows.iter().map(|row| text(&row[0])).collect::<String>();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        let send = &parsed[0]["Sub"][0]["Sub"][0];
        let keys = send["Annotations"]
            .as_array()
            .unwrap()
            .iter()
            .map(|annotation| annotation["Key"].as_str().unwrap().to_owned())
            .collect::<Vec<_>>();
        assert!(keys.contains(&"async".to_owned()), "{keys:?}");
        assert!(keys.contains(&"Msg".to_owned()), "{keys:?}");
    }
}
