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
//! Go records every OpenTracing span its session, planner, executors and KV
//! client open while the statement runs, then prints the tree (`row`), the
//! appdash JSON (`json`) or the flat event list (`log`). This tier opens no
//! such spans, so the tree is rebuilt from what it does know -- the kind of
//! statement, the physical plan it executed, the row counts and the wall
//! clock -- using the span names Go's instrumentation gives each step. The
//! shape (names, nesting, the data call and the end-of-data call of a root
//! executor) is Go's; the timestamps are this run's own measurements divided
//! over the phases.
//!
//! The coprocessor client's spans are the exception: when the caller recorded
//! them ([`TraceInput::cop_selects`]), each `distsql.Select` holds Go's
//! `regionRequest.SendReqCtx` per region send, its `rpcClient.SendRequest`
//! per physical RPC and the `tikv.*` tree client-go builds from TiKV's
//! execution details, all at their measured times.

use tidb_datatype::{core_time_from_datetime, Datum, FieldType, FieldTypeCode, Time, TimeType};
use tidb_planner::physical::PhysicalPlan;

/// One span of the tree.
#[derive(Clone, Debug)]
pub struct Span {
    name: String,
    /// Offset from the trace start, in nanoseconds.
    start: i64,
    end: i64,
    children: Vec<Span>,
    /// `(offset, message)` events logged on this span.
    events: Vec<(i64, String)>,
    /// A `Msg` annotation Go attaches to some spans (the JSON form only).
    message: Option<(i64, String)>,
}

impl Span {
    fn new(name: &str, start: i64, end: i64) -> Self {
        Self {
            name: name.to_owned(),
            start,
            end,
            children: Vec::new(),
            events: Vec::new(),
            message: None,
        }
    }

    fn with(mut self, children: Vec<Span>) -> Self {
        self.children = children;
        self
    }
}

/// What the traced statement was, which decides the span skeleton.
#[derive(Clone, Debug)]
pub enum TraceKind {
    /// A statement that produced a result set through an executor tree.
    Query {
        /// The plan it executed, when one was published.
        plan: Option<std::sync::Arc<PhysicalPlan>>,
        /// Rows returned.
        rows: u64,
        /// The executor Go builds for the statement's root when there is no
        /// physical plan (`SHOW`, `EXPLAIN`).
        root: &'static str,
    },
    /// INSERT / REPLACE / UPDATE / DELETE / LOAD DATA.
    Dml {
        /// The executor's type name, e.g. `*executor.InsertExec`.
        executor: &'static str,
        /// Whether the statement reads by point get first.
        point_read: bool,
        /// Whether the statement committed its own transaction.
        commits: bool,
    },
    /// DDL.
    Ddl,
    /// Everything else (`BEGIN`, `SET`, ...): a `SimpleExec`.
    Simple,
}

/// The facts a trace is rebuilt from.
#[derive(Clone, Debug)]
pub struct TraceInput {
    /// What ran.
    pub kind: TraceKind,
    /// The statement text.
    pub sql: String,
    /// Wall-clock start.
    pub started: std::time::SystemTime,
    /// Wall-clock duration of the whole statement.
    pub elapsed: std::time::Duration,
    /// `StmtCtx.AffectedRows` after the run.
    pub affected_rows: u64,
    /// The error code and message when the statement failed.
    pub error: Option<(u16, String)>,
    /// `tidb_init_chunk_size` and `tidb_max_chunk_size`: the chunks a result
    /// is delivered in (Go's chunk size doubles from the first to the second).
    pub chunk_sizes: (u64, u64),
    /// The region sends each coprocessor request (`distsql.Select`) made, in
    /// send order, when the caller recorded them; empty when it did not.
    pub cop_selects: Vec<Vec<SendReqCtxSpan>>,
}

/// One recorded `regionRequest.SendReqCtx`.
#[derive(Clone, Debug)]
pub struct SendReqCtxSpan {
    /// `req.RegionId`.
    pub region_id: u64,
    /// Offset from the trace start, in nanoseconds.
    pub start: i64,
    /// End offset, in nanoseconds.
    pub end: i64,
    /// The physical RPCs of the send.
    pub rpcs: Vec<RpcSpan>,
    /// The index of the earlier send of the same request this one is nested
    /// under (client-go reuses the backoffer whose ctx holds its span).
    pub parent: Option<usize>,
}

/// One recorded `rpcClient.SendRequest` and TiKV's execution details.
#[derive(Clone, Debug)]
pub struct RpcSpan {
    /// Offset from the trace start, in nanoseconds.
    pub start: i64,
    /// End offset, in nanoseconds.
    pub end: i64,
    /// client-go's `buildSpanInfoFromResp` tree.
    pub details: tikv_client::trace::ExecutionDetailSpan,
}

/// client-go `spanInfo.calcDur`: a node without a reported duration lasts as
/// long as its synchronous children.
fn exec_detail_duration(span: &tikv_client::trace::ExecutionDetailSpan) -> i64 {
    if !span.duration.is_zero() {
        return i64::try_from(span.duration.as_nanos()).unwrap_or(i64::MAX);
    }
    span.children
        .iter()
        .filter(|child| !child.asynchronous)
        .map(exec_detail_duration)
        .sum()
}

/// client-go `spanInfo.addTo`: `span` under `parent` from `start`, children
/// laid end to end, a zero-length node left out; returns where the next
/// sibling starts (an asynchronous node does not advance it).
fn add_exec_detail_span(
    span: &tikv_client::trace::ExecutionDetailSpan,
    parent: &mut Span,
    start: i64,
) -> i64 {
    let duration = exec_detail_duration(span);
    if duration == 0 {
        return start;
    }
    let end = start + duration;
    let mut node = Span::new(span.name, start, end);
    let mut cursor = start;
    for child in &span.children {
        cursor = add_exec_detail_span(child, &mut node, cursor);
    }
    parent.children.push(node);
    if span.asynchronous {
        start
    } else {
        end
    }
}

/// One request's `regionRequest.SendReqCtx` spans as Go nests them: a send
/// opened on a backoffer that already carried a send's span is its child.
fn send_req_ctx_spans(sends: &[SendReqCtxSpan]) -> Vec<Span> {
    let mut spans: Vec<Option<Span>> = sends
        .iter()
        .map(|send| Some(send_req_ctx_span(send)))
        .collect();
    // A parent always precedes its child, so attaching from the last send
    // backwards moves every subtree after it is complete.
    for index in (0..sends.len()).rev() {
        if let Some(parent) = sends[index].parent.filter(|parent| *parent < index) {
            let child = spans[index].take();
            if let Some(parent) = spans[parent].as_mut() {
                parent.children.extend(child);
            }
        }
    }
    spans.into_iter().flatten().collect()
}

/// Go's `regionRequest.SendReqCtx` (client-go `region_request.go`) with one
/// `rpcClient.SendRequest, region ID: %d, type: Cop` (`client.go`) per RPC.
fn send_req_ctx_span(send: &SendReqCtxSpan) -> Span {
    let mut span = Span::new("regionRequest.SendReqCtx", send.start, send.end);
    for rpc in &send.rpcs {
        let mut rpc_span = Span::new(
            &format!(
                "rpcClient.SendRequest, region ID: {}, type: Cop",
                send.region_id
            ),
            rpc.start,
            rpc.end,
        );
        add_exec_detail_span(&rpc.details, &mut rpc_span, rpc.start);
        span.children.push(rpc_span);
    }
    span
}

/// Go's name for the executor a physical operator becomes.
fn executor_name(plan: &PhysicalPlan) -> &'static str {
    match plan {
        PhysicalPlan::Projection(_) => "*executor.ProjectionExec",
        PhysicalPlan::Selection(_) => "*executor.SelectionExec",
        PhysicalPlan::TableDual(_) => "*executor.TableDualExec",
        PhysicalPlan::PointGet(_) => "*executor.PointGetExecutor",
        PhysicalPlan::BatchPointGet(_) => "*executor.BatchPointGetExec",
        PhysicalPlan::TableReader(_) => "*executor.TableReaderExecutor",
        PhysicalPlan::IndexReader(_) => "*executor.IndexReaderExecutor",
        PhysicalPlan::IndexLookUpReader(_) | PhysicalPlan::LocalIndexLookUp(_) => {
            "*executor.IndexLookUpExecutor"
        }
        PhysicalPlan::IndexMergeReader(_) => "*executor.IndexMergeReaderExecutor",
        PhysicalPlan::Limit(_) => "*executor.LimitExec",
        PhysicalPlan::TopN(_) => "*sortexec.TopNExec",
        PhysicalPlan::Sort(_) | PhysicalPlan::NominalSort(_) => "*sortexec.SortExec",
        PhysicalPlan::HashAgg(_) => "*aggregate.HashAggExec",
        PhysicalPlan::StreamAgg(_) => "*aggregate.StreamAggExec",
        PhysicalPlan::HashJoin(_) => "*join.HashJoinV1Exec",
        PhysicalPlan::MergeJoin(_) => "*join.MergeJoinExec",
        PhysicalPlan::IndexJoin(_) => "*join.IndexLookUpJoin",
        PhysicalPlan::Apply(_) => "*join.NestedLoopApplyExec",
        PhysicalPlan::Window(_) => "*windowexec.WindowExec",
        PhysicalPlan::UnionAll(_) => "*unionexec.UnionExec",
        PhysicalPlan::MaxOneRow(_) => "*executor.MaxOneRowExec",
        PhysicalPlan::Show(_) => "*executor.ShowExec",
        PhysicalPlan::MemTable(_) => "*executor.MemTableReaderExec",
        PhysicalPlan::Lock(_) => "*executor.SelectLockExec",
        PhysicalPlan::CTE(_) => "*executor.CTEExec",
        PhysicalPlan::CTETable(_) => "*executor.CTETableReaderExec",
        PhysicalPlan::Sequence(_) => "*executor.SequenceExec",
        PhysicalPlan::UnionScan(_) => "*executor.UnionScanExec",
        PhysicalPlan::TableSample(_) => "*executor.TableSampleExecutor",
        PhysicalPlan::Expand(_) => "*executor.ExpandExec",
        PhysicalPlan::Shuffle(_) | PhysicalPlan::ShuffleReceiver(_) => "*executor.ShuffleExec",
        _ => "*executor.Executor",
    }
}

/// Whether the executor drains its children while producing its first batch
/// (so its end-of-data call touches none).
fn is_blocking(plan: &PhysicalPlan) -> bool {
    matches!(
        plan,
        PhysicalPlan::Sort(_)
            | PhysicalPlan::TopN(_)
            | PhysicalPlan::HashAgg(_)
            | PhysicalPlan::HashJoin(_)
            | PhysicalPlan::Window(_)
    )
}

/// Executors that open KV spans inside their data call.
fn kv_get_spans(plan: &PhysicalPlan, start: i64, end: i64) -> Vec<Span> {
    match plan {
        PhysicalPlan::PointGet(_) | PhysicalPlan::BatchPointGet(_) => {
            let width = end - start;
            vec![
                Span::new(
                    "tikvSnapshot.get",
                    start + width / 20,
                    start + width * 3 / 4,
                ),
                Span::new(
                    "regionRequest.SendReqCtx",
                    start + width / 10,
                    start + width * 2 / 3,
                ),
            ]
        }
        _ => Vec::new(),
    }
}

/// One `Next` call of `plan` over `[start, end]`: the data call or the
/// end-of-data call.
fn next_call(plan: &PhysicalPlan, data: bool, start: i64, end: i64) -> Span {
    let name = format!("{}.Next", executor_name(plan));
    let readers = matches!(
        plan,
        PhysicalPlan::TableReader(_)
            | PhysicalPlan::IndexReader(_)
            | PhysicalPlan::IndexLookUpReader(_)
            | PhysicalPlan::IndexMergeReader(_)
            | PhysicalPlan::MemTable(_)
            | PhysicalPlan::TableDual(_)
            | PhysicalPlan::PointGet(_)
            | PhysicalPlan::BatchPointGet(_)
    );
    let mut span = Span::new(&name, start, end);
    if readers {
        if data {
            span.children = kv_get_spans(plan, start, end);
        }
        return span;
    }
    let children = plan.children();
    if children.is_empty() {
        return span;
    }
    // A blocking executor reads all its input inside its first call; a
    // streaming one forwards each call to its children.
    let calls: &[bool] = if is_blocking(plan) {
        if data {
            &[true, false]
        } else {
            &[]
        }
    } else {
        std::slice::from_ref(if data { &true } else { &false })
    };
    let width = end - start;
    let per_child = children.len().max(1) as i64 * calls.len().max(1) as i64;
    let mut cursor = start + width / 8;
    let slice = (width - width / 4).max(per_child) / per_child;
    for child in children {
        for call in calls {
            span.children
                .push(next_call(child, *call, cursor, cursor + slice));
            cursor += slice;
        }
    }
    span
}

/// The `Open` spans of the readers under `plan`, as Go opens them inside
/// `session.runStmt`. `recorded` holds the recorded region sends of each
/// coprocessor request in send order, `None` when none were recorded.
fn open_spans(
    plan: &PhysicalPlan,
    cursor: &mut i64,
    slice: i64,
    recorded: &mut Option<std::slice::Iter<'_, Vec<SendReqCtxSpan>>>,
    out: &mut Vec<Span>,
) {
    let mut select = |start: i64| {
        let mut children = vec![Span::new(
            "copr.buildCopTasks",
            start + slice / 8,
            start + slice / 4,
        )];
        match recorded {
            Some(sends) => {
                children.extend(
                    sends
                        .next()
                        .map_or_else(Vec::new, |sends| send_req_ctx_spans(sends)),
                );
            }
            None => children.push(Span::new(
                "regionRequest.SendReqCtx",
                start + slice / 2,
                start + slice,
            )),
        }
        Span::new("distsql.Select", start, start + slice).with(children)
    };
    match plan {
        PhysicalPlan::TableReader(_) => {
            out.push(
                Span::new("TableReaderExecutor.Open", *cursor, *cursor + slice)
                    .with(vec![select(*cursor)]),
            );
            *cursor += slice;
        }
        PhysicalPlan::IndexReader(_) => {
            out.push(select(*cursor));
            *cursor += slice;
        }
        _ => {}
    }
    for child in plan.children() {
        open_spans(child, cursor, slice, recorded, out);
    }
}

/// The commit subtree Go's autocommit statements run (`session.CommitTxn`).
fn commit_spans(start: i64, end: i64) -> Span {
    let width = end - start;
    let at = |fraction: i64| start + width * fraction / 100;
    Span::new("session.CommitTxn", start, end).with(vec![Span::new(
        "session.doCommitWithRetry",
        at(2),
        at(98),
    )
    .with(vec![Span::new("tikvTxn.Commit", at(5), at(95)).with(vec![
        Span::new("twoPhaseCommitter.prewriteMutations", at(8), at(50)).with(vec![Span::new(
            "regionRequest.SendReqCtx",
            at(15),
            at(45),
        )]),
        Span::new("TiKVStore.getTimestampWithRetry", at(52), at(54)),
        Span::new("twoPhaseCommitter.commitMutations", at(56), at(92)).with(vec![Span::new(
            "regionRequest.SendReqCtx",
            at(62),
            at(88),
        )]),
    ])])])
}

/// How many non-empty chunks a result of `rows` arrives in when each call
/// asks for twice the previous chunk, from `init` up to `max`.
fn data_calls(rows: u64, init: u64, max: u64) -> usize {
    let (mut produced, mut size, mut calls) = (0, init.max(1), 0);
    while produced < rows {
        produced += size;
        calls += 1;
        size = (size * 2).min(max.max(1));
    }
    calls
}

/// Rebuilds the span tree of one traced statement.
#[must_use]
pub fn build_spans(input: &TraceInput) -> Span {
    let total = i64::try_from(input.elapsed.as_nanos())
        .unwrap_or(i64::MAX)
        .max(1_000);
    let at = |fraction: i64| total * fraction / 1000;
    let mut root = Span::new("trace", 0, total);

    let compile = Span::new("executor.Compile", at(10), at(250)).with(vec![
        Span::new("planner.Preprocess", at(12), at(20)),
        Span::new("planner.Optimize", at(30), at(240)),
    ]);
    let mut run_stmt = Span::new("session.runStmt", at(260), at(500));
    run_stmt.message = Some((at(260), format!("{{\"sql\":{}}}", json_string(&input.sql))));
    let mut execute = Span::new("session.ExecuteStmt", at(5), at(500));
    let mut trailing = Vec::new();
    match &input.kind {
        TraceKind::Query {
            plan,
            rows,
            root: root_name,
        } => {
            let _ = rows;
            match plan {
                Some(plan) => {
                    let mut opens = Vec::new();
                    let mut cursor = at(280);
                    let mut recorded =
                        (!input.cop_selects.is_empty()).then(|| input.cop_selects.iter());
                    open_spans(plan, &mut cursor, at(40), &mut recorded, &mut opens);
                    run_stmt.children = opens;
                    let calls = data_calls(*rows, input.chunk_sizes.0, input.chunk_sizes.1);
                    let slot = (at(980) - at(520)) / (calls as i64 + 1);
                    for call in 0..=calls {
                        let begin = at(520) + slot * call as i64;
                        trailing.push(next_call(plan, call < calls, begin, begin + slot * 9 / 10));
                    }
                }
                None => {
                    let name = format!("{root_name}.Next");
                    let calls = data_calls(*rows, input.chunk_sizes.0, input.chunk_sizes.1);
                    let slot = (at(980) - at(520)) / (calls as i64 + 1);
                    for call in 0..=calls {
                        let begin = at(520) + slot * call as i64;
                        trailing.push(Span::new(&name, begin, begin + slot * 9 / 10));
                    }
                }
            }
        }
        TraceKind::Dml {
            executor,
            point_read,
            commits,
        } => {
            let mut executor_span = Span::new(&format!("{executor}.Next"), at(300), at(480));
            let mut children = Vec::new();
            if *executor == "*executor.InsertExec" || *executor == "*executor.ReplaceExec" {
                children.push(Span::new("table.AddRecord", at(320), at(460)));
            } else if *point_read {
                let get =
                    Span::new("*executor.PointGetExecutor.Next", at(310), at(400)).with(vec![
                        Span::new("tikvSnapshot.get", at(315), at(390)).with(vec![Span::new(
                            "regionRequest.SendReqCtx",
                            at(320),
                            at(380),
                        )]),
                    ]);
                children.push(get);
                if *executor == "*executor.UpdateExec" {
                    children.push(Span::new("executor.updateRecord", at(410), at(450)));
                }
                children.push(Span::new(
                    "*executor.PointGetExecutor.Next",
                    at(460),
                    at(465),
                ));
            }
            executor_span.children = children;
            let mut stmt_children =
                vec![
                    Span::new("executor.handleNoDelayExecutor", at(290), at(490))
                        .with(vec![executor_span]),
                ];
            if *commits {
                stmt_children.push(commit_spans(at(500), at(990)));
            }
            run_stmt.children = stmt_children;
            run_stmt.end = at(990);
            execute.end = at(990);
        }
        TraceKind::Ddl => {
            run_stmt.children = vec![
                Span::new("executor.handleNoDelayExecutor", at(270), at(960)).with(vec![
                    Span::new("*executor.DDLExec.Next", at(272), at(958)).with(vec![
                        Span::new("session.CommitTxn", at(274), at(300)).with(vec![Span::new(
                            "session.doCommitWithRetry",
                            at(276),
                            at(296),
                        )]),
                        Span::new("isolation.newOracleFuture", at(310), at(312)),
                    ]),
                ]),
                Span::new("ddl.DoDDLJobWrapper", at(320), at(985)),
                Span::new("session.CommitTxn", at(986), at(990)).with(vec![Span::new(
                    "session.doCommitWithRetry",
                    at(987),
                    at(989),
                )]),
            ];
            run_stmt.end = at(990);
            execute.end = at(990);
        }
        TraceKind::Simple => {
            run_stmt.children = vec![
                Span::new("executor.handleNoDelayExecutor", at(270), at(490)).with(vec![
                    Span::new("*executor.SimpleExec.Next", at(272), at(480)),
                ]),
            ];
        }
    }
    execute.children = vec![compile, run_stmt];
    root.children.push(execute);
    root.children.extend(trailing);

    // The events `executeChild` and `drainRecordSet` log on the root span.
    match &input.error {
        Some((code, message)) => root
            .events
            .push((at(995), format!("execute with error({code}): {message}"))),
        None => {
            if let TraceKind::Query { rows, .. } = &input.kind {
                root.events.push((
                    at(995),
                    format!(
                        "execute done, ReturnRow: {rows}, ModifyRow: {}",
                        input.affected_rows
                    ),
                ));
            }
        }
    }
    root.events.push((
        at(999),
        format!("execute done, modify row: {}", input.affected_rows),
    ));
    root
}

fn json_string(text: &str) -> String {
    crate::go_json_marshal(&serde_json::Value::String(text.to_owned()))
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
        children.sort_by_key(|child| child.start);
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

/// Go `generateLogResult`: a `start span` row per span, then the logged events.
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
    spans.sort_by_key(|span| span.start);
    let timestamp = |offset: i64| {
        let local = clock(started, offset);
        Time::new(core_time_from_datetime(local), TimeType::Timestamp, 6)
            .map_or(Datum::Null, Datum::new_time)
    };
    let text = |value: &str| Datum::Bytes(value.as_bytes().to_vec());
    let mut rows = Vec::new();
    for span in &spans {
        rows.push(vec![
            timestamp(span.start),
            text(&format!("--- start span {} ----", span.name)),
            text(""),
            text(&span.name),
        ]);
    }
    let mut events = Vec::new();
    for span in &spans {
        for (offset, message) in &span.events {
            events.push((*offset, message.clone(), span.name.clone()));
        }
    }
    events.sort_by_key(|(offset, ..)| *offset);
    for (offset, message, name) in events {
        rows.push(vec![
            timestamp(offset),
            text(&message),
            text(""),
            text(&name),
        ]);
    }
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
        let mut logs = Vec::new();
        if let Some((offset, message)) = &span.message {
            logs.push((*offset, message.clone()));
        }
        logs.extend(span.events.iter().cloned());
        for (offset, message) in logs {
            annotations.push(annotation("Msg", Some(&message)));
            annotations.push(annotation("Time", Some(&rfc3339(started, offset, false))));
            annotations.push(annotation("_schema:log", None));
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
        children.sort_by_key(|child| child.start);
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

/// Classifies the traced statement for [`build_spans`].
#[must_use]
pub fn kind_of(
    stmt: &tidb_ast::Stmt,
    plan: Option<std::sync::Arc<PhysicalPlan>>,
    rows: u64,
    in_transaction: bool,
) -> TraceKind {
    use tidb_ast::{AdminStmt, DmlStmt, Stmt};
    match stmt {
        Stmt::Query(_) => TraceKind::Query {
            plan,
            rows,
            root: "*executor.ProjectionExec",
        },
        Stmt::Dml(dml) => {
            let executor = match dml.as_ref() {
                DmlStmt::Insert(insert) if insert.replace => "*executor.ReplaceExec",
                DmlStmt::Insert(_) => "*executor.InsertExec",
                DmlStmt::Update(_) => "*executor.UpdateExec",
                DmlStmt::Delete(_) => "*executor.DeleteExec",
                DmlStmt::LoadData(_) => "*executor.LoadDataExec",
                _ => "*executor.SimpleExec",
            };
            TraceKind::Dml {
                executor,
                point_read: plan.as_ref().is_some_and(|plan| {
                    matches!(
                        plan.as_ref(),
                        PhysicalPlan::PointGet(_) | PhysicalPlan::BatchPointGet(_)
                    )
                }),
                commits: !in_transaction,
            }
        }
        Stmt::Ddl(_) => TraceKind::Ddl,
        Stmt::Admin(admin) => match admin.as_ref() {
            AdminStmt::Explain(_) | AdminStmt::ExplainFor(_) => TraceKind::Query {
                plan: None,
                rows,
                root: "*executor.ExplainExec",
            },
            AdminStmt::ShowTables(_)
            | AdminStmt::ShowDatabases(_)
            | AdminStmt::ShowColumns(_)
            | AdminStmt::ShowVariables(_)
            | AdminStmt::ShowStatus(_)
            | AdminStmt::ShowIndex(_)
            | AdminStmt::ShowTableStatus(_)
            | AdminStmt::ShowCreate { .. } => TraceKind::Query {
                plan: None,
                rows,
                root: "*executor.ShowExec",
            },
            _ => TraceKind::Simple,
        },
        Stmt::Session(_) => TraceKind::Simple,
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
    use tikv_client::proto::kvrpcpb;

    /// The span tree as `name{ child child }`, children in start order.
    fn shape(span: &Span) -> String {
        let mut children = span.children.iter().collect::<Vec<_>>();
        children.sort_by_key(|child| child.start);
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

    fn cop_details(wait: u64, process: u64) -> tikv_client::trace::ExecutionDetailSpan {
        tikv_client::trace::build_execution_detail_span(&kvrpcpb::ExecDetailsV2 {
            time_detail_v2: Some(kvrpcpb::TimeDetailV2 {
                total_rpc_wall_time_ns: wait + process + 100,
                wait_wall_time_ns: wait,
                process_wall_time_ns: process,
                ..Default::default()
            }),
            scan_detail_v2: Some(kvrpcpb::ScanDetailV2 {
                get_snapshot_nanos: wait / 2,
                ..Default::default()
            }),
            ..Default::default()
        })
        .unwrap()
    }

    fn send(start: i64, parent: Option<usize>) -> SendReqCtxSpan {
        SendReqCtxSpan {
            region_id: 136,
            start,
            end: start + 10_000,
            rpcs: vec![RpcSpan {
                start: start + 100,
                end: start + 9_000,
                details: cop_details(1_000, 6_000),
            }],
            parent,
        }
    }

    /// Go's coprocessor RPC spans (`TRACE SELECT COUNT(*) FROM acc.t` on a
    /// Go server): `regionRequest.SendReqCtx` > `rpcClient.SendRequest,
    /// region ID: N, type: Cop` > client-go's `tikv.*` tree, zero-length
    /// nodes (`tikv.RocksDBBlockRead`, `tikv.Suspend`) left out.
    #[test]
    fn a_cop_send_renders_client_gos_spans() {
        let spans = send_req_ctx_spans(&[send(1_000, None)]);
        assert_eq!(spans.len(), 1);
        assert_eq!(
            shape(&spans[0]),
            "regionRequest.SendReqCtx{ rpcClient.SendRequest, region ID: 136, type: Cop{ \
             tikv.RPC{ tikv.Wait{ tikv.GetSnapshot } tikv.Process } } }"
        );
        let rpc = &spans[0].children[0];
        let tikv = &rpc.children[0];
        // client-go `addTo`: the tree starts at the RPC's start, the children
        // laid end to end.
        assert_eq!((tikv.start, tikv.end), (1_100, 1_100 + 7_100));
        let (wait, process) = (&tikv.children[0], &tikv.children[1]);
        assert_eq!((wait.start, wait.end), (1_100, 2_100));
        assert_eq!((process.start, process.end), (2_100, 8_100));
    }

    /// client-go's `SendReqCtx` stores its span in the backoffer's ctx, so
    /// the later pages a backoffer sends nest under its earlier send, as the
    /// Go server traces a paged `TableReader`.
    #[test]
    fn sends_on_one_backoffer_nest() {
        let spans = send_req_ctx_spans(&[
            send(1_000, None),
            send(20_000, None),
            send(40_000, Some(1)),
            send(60_000, Some(2)),
        ]);
        let depths = spans
            .iter()
            .map(|span| {
                let mut nested = 0;
                let mut cursor = span;
                while let Some(child) = cursor
                    .children
                    .iter()
                    .find(|child| child.name == "regionRequest.SendReqCtx")
                {
                    nested += 1;
                    cursor = child;
                }
                (span.start, nested)
            })
            .collect::<Vec<_>>();
        assert_eq!(depths, [(1_000, 0), (20_000, 2)]);
    }
}
