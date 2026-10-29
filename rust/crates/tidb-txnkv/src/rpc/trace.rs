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

//! client-go's opentracing spans under a traced statement.
//!
//! Go TiDB hands client-go a `ctx` whose span is the statement's current
//! one, and client-go opens its spans (`regionRequest.SendReqCtx`,
//! `rpcClient.SendRequest, region ID: N, type: T`, `tikvSnapshot.get`,
//! `tikvTxn.Commit`, `twoPhaseCommitter.*`, `tikv.backoff.*`, the `tikv.*`
//! execution-detail tree, ...) as its children on TiDB's tracer. client-rust
//! reports the same spans to a task-scoped hook; this module is the `ctx`:
//! [`with_span_ctx`] runs a client call with the span as the hook's parent
//! and maps every reported event onto TiDB's recording tracer, and, as
//! `TraceExec` also sets `util.ContextWithTraceExecDetails`, asks for the
//! execution details the client hangs under each RPC's span.

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex, PoisonError};

use tidb_tracing::{Span, TRACE_EVENT_KEY};
use tikv_client::trace::{ClientSpanEvent, ClientSpanHandler};

/// Go's opentracing tracer as client-go reaches it through `ctx`: every
/// client span starts as a child of the span its event names, or of `root`
/// (the span of the `ctx` the client call was made with) when it names
/// none, at the instant the client measured; a log is `logutil.Eventf`'s
/// `event` field on its span and a tag `span.SetTag`. A span stays
/// addressable after it finishes, as a Backoffer's ctx outlives its spans;
/// like Go's recorder, a finished span takes no more logs or tags.
#[must_use]
pub fn client_span_handler(root: Span) -> ClientSpanHandler {
    let spans = Mutex::new(HashMap::<u64, Span>::new());
    Arc::new(move |event| {
        let mut spans = spans.lock().unwrap_or_else(PoisonError::into_inner);
        match event {
            ClientSpanEvent::Start {
                id,
                parent,
                name,
                at,
            } => {
                let parent = parent
                    .and_then(|parent| spans.get(&parent))
                    .unwrap_or(&root);
                let tracer = parent.tracer();
                let span = tracer.start_span_child_of_at(
                    &name,
                    parent.context(),
                    tracer.system_time_of(at),
                );
                spans.insert(id, span);
            }
            ClientSpanEvent::Finish { id, at } => {
                if let Some(span) = spans.get(&id) {
                    span.finish_at(span.tracer().system_time_of(at));
                }
            }
            ClientSpanEvent::Log { id, at, event } => {
                let span = id.and_then(|id| spans.get(&id)).unwrap_or(&root);
                span.log_fields_at(
                    vec![(TRACE_EVENT_KEY.to_owned(), event)],
                    span.tracer().system_time_of(at),
                );
            }
            ClientSpanEvent::Tag { id, key, value } => {
                id.and_then(|id| spans.get(&id))
                    .unwrap_or(&root)
                    .set_tag(&key, &value);
            }
        }
    })
}

/// Go `opentracing.ContextWithSpan(ctx, span)` with
/// `util.ContextWithTraceExecDetails(ctx)` for one client call: `future`
/// reports client-go's spans under `span` and the `tikv.*` tree of every
/// RPC it sends; `None` (an untraced statement) runs it unchanged. Pass the
/// span of the code making the call (`tidb_tracing::current_span()`
/// on the calling thread), taken before the future moves elsewhere.
///
/// The future moves to the heap as soon as it is made: the client's futures
/// (a commit's is the largest) are big, and every frame that hands one on
/// by value -- this scope, the blocking wrapper, the runtime's `block_on` --
/// would otherwise hold another copy on the calling thread's stack.
pub fn with_span_ctx<F: Future>(span: Option<Span>, future: F) -> impl Future<Output = F::Output> {
    let future = Box::pin(future);
    match span {
        Some(span) => {
            // The client places the tree under each `rpcClient.SendRequest`
            // span itself; the handler only turns details on.
            let details: tikv_client::trace::ExecutionDetailsTraceHandler = Arc::new(|_, _| {});
            futures::future::Either::Right(tikv_client::trace::with_client_spans(
                client_span_handler(span),
                tikv_client::trace::with_trace_exec_details(details, future),
            ))
        }
        None => futures::future::Either::Left(future),
    }
}

/// Go `NewBackoffer(ctx)` for a `ctx` carrying `span`: `build` runs where
/// client-rust's `RetryBackoffer` takes the span as its `bo.ctx`, so the
/// client spans of every send on it report under `span` (and, after
/// `bo.SetCtx`, under the spans those sends opened).
pub fn build_in_span_ctx<T>(span: Option<Span>, build: impl FnOnce() -> T) -> T {
    build_in_ctx(span, None, build)
}

/// [`build_in_span_ctx`] for a `ctx` that also carries the statement's
/// values (`context`, client-rust's trace context: Go
/// `util.ExecDetailsKey`, `util.RUDetailsCtxKey`, `retry.TxnStartKey`): a
/// `RetryBackoffer` takes them from the task it is made in, so its sleeps
/// charge the statement's `ExecDetails` and its `retry later` lines report
/// the start ts, as Go's `bo.ctx` does.
pub fn build_in_ctx<'a, T: 'a>(
    span: Option<Span>,
    context: Option<tikv_client::trace::TraceContext>,
    build: impl FnOnce() -> T + 'a,
) -> T {
    if span.is_none() && context.is_none() {
        return build();
    }
    let mut future: std::pin::Pin<Box<dyn Future<Output = T> + 'a>> =
        Box::pin(async move { build() });
    if let Some(span) = span {
        future = Box::pin(tikv_client::trace::with_client_spans(
            client_span_handler(span),
            future,
        ));
    }
    if let Some(context) = context {
        future = Box::pin(tikv_client::trace::with_trace_context(context, future));
    }
    futures::executor::block_on(future)
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use std::time::{Duration, Instant};

    use super::*;
    use tidb_tracing::{RawSpan, Tracer};

    fn recording_root() -> (Span, Arc<Mutex<Vec<RawSpan>>>) {
        let spans = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&spans);
        let tracer = Arc::new(Tracer::new(Arc::new(move |raw| {
            sink.lock().unwrap().push(raw);
        })));
        (tracer.start_span("trace"), spans)
    }

    fn start(id: u64, parent: Option<u64>, name: &'static str, at: Instant) -> ClientSpanEvent {
        ClientSpanEvent::Start {
            id,
            parent,
            name: Cow::Borrowed(name),
            at,
        }
    }

    /// client-go's spans land on TiDB's tracer as Go's do: a span naming no
    /// parent is a child of the call's ctx span, its times are the client's,
    /// a child or log may name a span that already finished (the Backoffer
    /// ctx outlives it) and a tag set while the span is open is recorded.
    #[test]
    fn client_spans_become_children_of_the_ctx_span() {
        let (root, spans) = recording_root();
        let handler = client_span_handler(root.clone());
        let at = Instant::now();
        let micros = |n| at + Duration::from_micros(n);
        handler(start(11, None, "regionRequest.SendReqCtx", micros(1)));
        handler(ClientSpanEvent::Log {
            id: Some(11),
            at: micros(2),
            event: "send Get request to region 7 at store-1:20160".to_owned(),
        });
        handler(start(
            12,
            Some(11),
            "rpcClient.SendRequest, region ID: 7, type: Get",
            micros(3),
        ));
        handler(ClientSpanEvent::Tag {
            id: Some(12),
            key: Cow::Borrowed("async"),
            value: "true".to_owned(),
        });
        handler(ClientSpanEvent::Finish {
            id: 12,
            at: micros(10),
        });
        handler(ClientSpanEvent::Finish {
            id: 11,
            at: micros(11),
        });
        // Under the finished `regionRequest.SendReqCtx`: the child records,
        // the late log does not.
        handler(start(13, Some(11), "tikv.backoff.regionMiss", micros(12)));
        handler(ClientSpanEvent::Finish {
            id: 13,
            at: micros(13),
        });
        handler(ClientSpanEvent::Log {
            id: Some(11),
            at: micros(14),
            event: "late".to_owned(),
        });
        handler(ClientSpanEvent::Tag {
            id: None,
            key: Cow::Borrowed("commitTs"),
            value: "42".to_owned(),
        });
        root.finish();

        let spans = spans.lock().unwrap();
        let named = |name: &str| spans.iter().find(|span| span.operation == name).unwrap();
        let names = spans
            .iter()
            .map(|span| span.operation.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            names,
            [
                "rpcClient.SendRequest, region ID: 7, type: Get",
                "regionRequest.SendReqCtx",
                "tikv.backoff.regionMiss",
                "trace"
            ]
        );
        let send = named("regionRequest.SendReqCtx");
        let rpc = named("rpcClient.SendRequest, region ID: 7, type: Get");
        let backoff = named("tikv.backoff.regionMiss");
        assert_eq!(send.parent_span_id, root.context().span_id);
        assert_eq!(rpc.parent_span_id, send.context.span_id);
        assert_eq!(backoff.parent_span_id, send.context.span_id);
        let offset = |span: &RawSpan| span.start.duration_since(send.start).unwrap();
        assert_eq!(offset(rpc), Duration::from_micros(2));
        assert_eq!(rpc.duration, Duration::from_micros(7));
        assert_eq!(send.duration, Duration::from_micros(10));
        assert_eq!(
            send.logs
                .iter()
                .map(|log| log.fields.clone())
                .collect::<Vec<_>>(),
            [vec![(
                TRACE_EVENT_KEY.to_owned(),
                "send Get request to region 7 at store-1:20160".to_owned()
            )]]
        );
        assert_eq!(
            send.logs[0].timestamp.duration_since(send.start).unwrap(),
            Duration::from_micros(1)
        );
        assert_eq!(rpc.tags.get("async").map(String::as_str), Some("true"));
        assert_eq!(
            named("trace").tags.get("commitTs").map(String::as_str),
            Some("42")
        );
    }

    /// Outside a traced statement the call runs unchanged and opens no
    /// client span scope.
    #[test]
    fn an_untraced_call_runs_without_a_scope() {
        let enabled = futures::executor::block_on(with_span_ctx(None, async {
            tikv_client::trace::client_spans_enabled()
        }));
        assert!(!enabled);
        let (root, _spans) = recording_root();
        let (spans, details) = futures::executor::block_on(with_span_ctx(Some(root), async {
            (
                tikv_client::trace::client_spans_enabled(),
                tikv_client::trace::trace_exec_details_enabled(),
            )
        }));
        assert!(spans && details);
        assert!(!build_in_span_ctx(
            None,
            tikv_client::trace::client_spans_enabled
        ));
    }
}
