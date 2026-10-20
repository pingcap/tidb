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

//! client-go `RPCClient.sendRequest`'s trace span for a traced statement:
//! every physical RPC opens `rpcClient.SendRequest, region ID: N, type: T`
//! under the span its call runs in, and, as `util.TraceExecDetailsEnabled`
//! holds under `TRACE`, hangs the `tikv.*` tree `buildSpanInfoFromResp`
//! builds from the response's execution details under it (`spanInfo.addTo`).
//! The span is recorded whether the RPC succeeds, fails or comes back
//! without execution details.
//!
//! The RPCs run inside client-rust's request plans, so the span is opened
//! by an RPC interceptor around the dispatch, the counterpart of the
//! interceptors client-go runs around `client.SendRequest`, and the
//! execution details reach it through client-rust's per-task
//! `with_trace_exec_details` handler scoped to that one dispatch.

use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Instant, SystemTime};

use futures::future::BoxFuture;
use tidb_util::tracing::Span;
use tikv_client::tikv::Request;
use tikv_client::trace::ExecutionDetailSpan;
use tikv_client::{RpcDispatchResult, RpcNext};

/// The span the RPCs of a call open their `rpcClient.SendRequest` spans
/// under (Go's span in the call's `ctx`), `None` while the call is untraced.
pub type RpcTraceParent = Arc<Mutex<Option<Span>>>;

/// client-go `tikvrpc.CmdType.String()` for a client-rust request label.
#[must_use]
pub fn cmd_type_name(label: &str) -> &'static str {
    match label {
        "kv_get" => "Get",
        "kv_scan" => "Scan",
        "kv_prewrite" => "Prewrite",
        "kv_commit" => "Commit",
        "kv_cleanup" => "Cleanup",
        "kv_batch_get" => "BatchGet",
        "kv_batch_rollback" => "BatchRollback",
        "kv_scan_lock" => "ScanLock",
        "kv_resolve_lock" => "ResolveLock",
        "kv_gc" => "GC",
        "kv_delete_range" => "DeleteRange",
        "kv_pessimistic_lock" => "PessimisticLock",
        "kv_pessimistic_rollback" => "PessimisticRollback",
        "kv_txn_heart_beat" => "TxnHeartBeat",
        "kv_check_txn_status" => "CheckTxnStatus",
        "kv_check_secondary_locks_request" => "CheckSecondaryLocks",
        "kv_flashback_to_version" => "FlashbackToVersion",
        "kv_prepare_flashback_to_version" => "PrepareFlashbackToVersion",
        "kv_flush" => "Flush",
        "kv_buffer_batch_get" => "BufferBatchGet",
        "raw_get" => "RawGet",
        "raw_batch_get" => "RawBatchGet",
        "raw_put" => "RawPut",
        "raw_batch_put" => "RawBatchPut",
        "raw_delete" => "RawDelete",
        "raw_batch_delete" => "RawBatchDelete",
        "raw_delete_range" => "RawDeleteRange",
        "raw_scan" => "RawScan",
        "raw_get_key_ttl" => "RawGetKeyTTL",
        "raw_compare_and_swap" => "RawCompareAndSwap",
        "raw_checksum" => "RawChecksum",
        "unsafe_destroy_range" => "UnsafeDestroyRange",
        "register_lock_observer" => "RegisterLockObserver",
        "check_lock_observer" => "CheckLockObserver",
        "remove_lock_observer" => "RemoveLockObserver",
        "physical_scan_lock" => "PhysicalScanLock",
        "store_safe_ts" => "StoreSafeTS",
        "get_lock_wait_info" => "LockWaitInfo",
        "get_health_feedback" => "GetHealthFeedback",
        "broadcast_txn_status" => "BroadcastTxnStatus",
        "coprocessor" => "Cop",
        "coprocessor_stream" => "CopStream",
        "batch_coprocessor" => "BatchCop",
        "dispatch_mpp_task" => "DispatchMPPTask",
        "establish_mpp_connection" => "EstablishMPPConnection",
        "cancel_mpp_task" => "CancelMPPTask",
        "is_alive" => "MPPAlive",
        "mvcc_get_by_key" => "MvccGetByKey",
        "mvcc_get_by_start_ts" => "MvccGetByStartTS",
        "split_region" => "SplitRegion",
        "debug_get_region_properties" => "DebugGetRegionProperties",
        "compact" => "Compact",
        "get_tiflash_system_table" => "GetTiFlashSystemTable",
        _ => "Unknown",
    }
}

/// client-go `spanInfo.calcDur`: a node without a reported duration lasts
/// as long as its synchronous children.
fn exec_detail_duration(span: &ExecutionDetailSpan) -> std::time::Duration {
    if !span.duration.is_zero() {
        return span.duration;
    }
    span.children
        .iter()
        .filter(|child| !child.asynchronous)
        .map(exec_detail_duration)
        .sum()
}

/// client-go `spanInfo.addTo`: `detail` under `parent` from `start`, its
/// children laid end to end, a zero-length node left out; returns where the
/// next sibling starts (an asynchronous node, tagged `async`, does not
/// advance it).
pub fn add_exec_detail_span(
    detail: &ExecutionDetailSpan,
    parent: &Span,
    start: SystemTime,
) -> SystemTime {
    let duration = exec_detail_duration(detail);
    if duration.is_zero() {
        return start;
    }
    let end = start + duration;
    let span = parent
        .tracer()
        .start_span_child_of_at(detail.name, parent.context(), start);
    let mut cursor = start;
    for child in &detail.children {
        cursor = add_exec_detail_span(child, &span, cursor);
    }
    span.finish_at(end);
    if detail.asynchronous {
        // Go sets the tag after `FinishWithOptions`, when the recorder has
        // already taken the span; a finished span here takes no more tags
        // either.
        span.set_tag("async", "true");
        start
    } else {
        end
    }
}

/// client-go `RPCClient.sendRequest` under a traced call: opens
/// `rpcClient.SendRequest, region ID: N, type: T` under `parent` around the
/// dispatch `next`, and when the response carries execution details adds
/// their tree from the physical send's start. `send_event` also logs Go's
/// `sendReqToRegion` event (`send <type> request to region <id> at <addr>`)
/// on `parent`, which is then the call's `regionRequest.SendReqCtx` span.
pub fn trace_rpc<'a>(
    parent: Span,
    send_event: bool,
    target: &'a str,
    request: &'a dyn Request,
    next: RpcNext<'a>,
) -> BoxFuture<'a, RpcDispatchResult> {
    Box::pin(async move {
        let region_id = request
            .tikv_context()
            .map_or(0, |context| context.region_id);
        let cmd = cmd_type_name(request.label());
        if send_event {
            parent.log_event(&format!(
                "send {cmd} request to region {region_id} at {target}"
            ));
        }
        let span = parent.tracer().start_span_child_of(
            &format!("rpcClient.SendRequest, region ID: {region_id}, type: {cmd}"),
            parent.context(),
        );
        let details = Arc::new(Mutex::new(None::<(Instant, ExecutionDetailSpan)>));
        let recorded = Arc::clone(&details);
        let handler: tikv_client::trace::ExecutionDetailsTraceHandler =
            Arc::new(move |started_at, detail| {
                *recorded.lock().unwrap_or_else(PoisonError::into_inner) =
                    Some((started_at, detail.clone()));
            });
        let result = tikv_client::trace::with_trace_exec_details(handler, next()).await;
        let detail = details
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some((started_at, detail)) = detail {
            add_exec_detail_span(
                &detail,
                &span,
                span.tracer().system_time_of(started_at),
            );
        }
        span.finish();
        result
    })
}

/// The RPC interceptor that opens [`trace_rpc`]'s spans under the span
/// `parent` holds at each RPC; it passes RPCs through while it holds none.
#[must_use]
pub fn rpc_trace_interceptor(
    parent: RpcTraceParent,
    send_event: bool,
) -> tikv_client::RpcInterceptorHandle {
    tikv_client::new_rpc_interceptor("tidb-trace-rpc", move |target, request, next| {
        let span = parent
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        match span {
            Some(span) => trace_rpc(span, send_event, target, request, next),
            None => Box::pin(next()),
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_util::tracing::{RawSpan, Tracer};
    use tikv_client::proto::kvrpcpb;

    fn recording_root() -> (Span, Arc<Mutex<Vec<RawSpan>>>) {
        let spans = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&spans);
        let tracer = Arc::new(Tracer::new(Arc::new(move |raw| {
            sink.lock().unwrap().push(raw);
        })));
        (tracer.start_span("trace"), spans)
    }

    /// client-go `addTo` over a write's details: the asynchronous
    /// `tikv.PersistLog` does not advance its siblings, and
    /// zero-length nodes are left out.
    #[test]
    fn exec_details_become_client_gos_span_tree() {
        let (root, spans) = recording_root();
        let detail = tikv_client::trace::build_execution_detail_span(&kvrpcpb::ExecDetailsV2 {
            time_detail_v2: Some(kvrpcpb::TimeDetailV2 {
                total_rpc_wall_time_ns: 9_000,
                wait_wall_time_ns: 1_000,
                process_wall_time_ns: 2_000,
                ..Default::default()
            }),
            write_detail: Some(kvrpcpb::WriteDetail {
                store_batch_wait_nanos: 100,
                persist_log_nanos: 500,
                commit_log_nanos: 300,
                ..Default::default()
            }),
            ..Default::default()
        })
        .unwrap();
        let start = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(1);
        add_exec_detail_span(&detail, &root, start);
        let spans = spans.lock().unwrap();
        let named = |name: &str| spans.iter().find(|span| span.operation == name).unwrap();
        let names = spans
            .iter()
            .map(|span| span.operation.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            names,
            [
                "tikv.Wait",
                "tikv.Process",
                "tikv.StoreBatchWait",
                "tikv.PersistLog",
                "tikv.CommitLog",
                "tikv.AsyncWrite",
                "tikv.RPC"
            ]
        );
        let at = |span: &RawSpan| span.start.duration_since(start).unwrap().as_nanos();
        assert_eq!(at(named("tikv.Process")), 1_000);
        let (persist, commit) = (named("tikv.PersistLog"), named("tikv.CommitLog"));
        assert_eq!((at(persist), at(commit)), (3_100, 3_100));
        // Go tags the span after finishing it, so appdash's recorder,
        // which reads the tags at the finish, never sees the tag.
        assert!(persist.tags.is_empty());
        assert_eq!(named("tikv.RPC").duration.as_nanos(), 9_000);
        let rpc_id = named("tikv.RPC").context.span_id;
        assert_eq!(named("tikv.AsyncWrite").parent_span_id, rpc_id);
    }

    /// client-go opens `rpcClient.SendRequest` before the send and finishes
    /// it however the RPC ends: a failed RPC, which brings back no execution
    /// details, still records its span, and `sendReqToRegion`'s event lands
    /// on the parent (the `regionRequest.SendReqCtx` span).
    #[test]
    fn a_failed_rpc_still_records_its_span() {
        let (root, spans) = recording_root();
        let request = kvrpcpb::GetRequest {
            context: Some(kvrpcpb::Context {
                region_id: 7,
                ..Default::default()
            }),
            ..Default::default()
        };
        let next: RpcNext<'_> = Arc::new(|| {
            Box::pin(async { Err(tikv_client::Error::StringError("unreachable".to_owned())) })
        });
        let result = futures::executor::block_on(trace_rpc(
            root.clone(),
            true,
            "store-1:20160",
            &request,
            next,
        ));
        assert!(result.is_err());
        root.finish();
        let spans = spans.lock().unwrap();
        let names = spans
            .iter()
            .map(|span| span.operation.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            names,
            ["rpcClient.SendRequest, region ID: 7, type: Get", "trace"]
        );
        assert_eq!(spans[0].parent_span_id, spans[1].context.span_id);
        assert_eq!(
            spans[1].logs[0].fields,
            [(
                tidb_util::tracing::TRACE_EVENT_KEY.to_owned(),
                "send Get request to region 7 at store-1:20160".to_owned()
            )]
        );
    }

    #[test]
    fn labels_render_as_client_go_command_types() {
        assert_eq!(cmd_type_name("kv_get"), "Get");
        assert_eq!(cmd_type_name("kv_batch_get"), "BatchGet");
        assert_eq!(cmd_type_name("coprocessor"), "Cop");
        assert_eq!(cmd_type_name("kv_pessimistic_lock"), "PessimisticLock");
        assert_eq!(cmd_type_name("no_such_label"), "Unknown");
    }
}
