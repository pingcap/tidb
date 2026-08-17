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

//! Go `pkg/store/mockstore/unistore/cophandler/cop_handler.go` — the
//! coprocessor's front door: request dispatch, DAG decode, and the
//! flat-list-to-tree executor conversion.
//!
//! SEED of `cophandler` (~5k lines): the PARSE half lands here; DAG
//! EXECUTION (`closure_exec.go`'s scan-and-evaluate machine), analyze, and
//! checksum are the following courses, refusing by name until they land.
//!
//! # Narrowings, by name
//!
//! * `flagsAndTzToSessionContext` / `globalLocationMap`: the time-zone name
//!   resolves through Go's location cache into a `time.Location`. The parsed
//!   name and offset are CARRIED here ([`DagContext::time_zone`]) and
//!   resolution is the evaluation course's concern — nothing at the parse
//!   layer reads the zone.
//! * `mppCtx` / `HandleMPPDAGReq`: the MPP arm follows the MPP course.

use prost::Message;
use tidb_proto::coprocessor;
use tidb_proto::tipb;

use crate::mvcc_store::MvccStore;

/// Go `kv.ReqTypeDAG` / `ReqTypeAnalyze` / `ReqTypeChecksum`
/// (`pkg/kv/kv.go:375-377`).
pub const REQ_TYPE_DAG: i64 = 103;
/// See [`REQ_TYPE_DAG`].
pub const REQ_TYPE_ANALYZE: i64 = 104;
/// See [`REQ_TYPE_DAG`].
pub const REQ_TYPE_CHECKSUM: i64 = 105;

/// Go `dagContext`, the parse half: what `buildDAG` establishes before any
/// executor runs.
#[derive(Debug)]
pub struct DagContext {
    /// The decoded `tipb.DAGRequest`.
    pub dag_req: tipb::DagRequest,
    /// `keyRanges` from the coprocessor request.
    pub key_ranges: Vec<coprocessor::KeyRange>,
    /// `startTS` — Go reads `req.StartTs`.
    pub start_ts: u64,
    /// The request's time zone, parsed but unresolved (module header).
    pub time_zone: TimeZoneSpec,
}

/// Go `buildDAG`'s three-way time-zone switch, as DATA: empty name is a
/// fixed offset from UTC, "System" is the process zone, anything else is a
/// named location.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TimeZoneSpec {
    /// `time.FixedZone("UTC", offset)`.
    FixedOffset(i64),
    /// `time.Local`.
    System,
    /// `time.LoadLocation(name)`, resolution deferred.
    Named(String),
}

/// Go `HandleCopRequest` (`cop_handler.go`): the type dispatch. The unknown
/// arm's message is Go's exact `fmt.Sprintf`.
pub fn handle_cop_request(
    store: &mut MvccStore,
    req: &coprocessor::Request,
) -> coprocessor::Response {
    match req.tp {
        REQ_TYPE_DAG => handle_cop_dag_request(store, req),
        REQ_TYPE_ANALYZE => other_error(
            "handleCopAnalyzeRequest (cophandler/analyze.go) is a later course of this port",
        ),
        REQ_TYPE_CHECKSUM => other_error(
            "handleCopChecksumRequest (cophandler/cop_handler.go) is a later course of this port",
        ),
        other => other_error(&format!("unsupported request type {other}")),
    }
}

fn other_error(message: &str) -> coprocessor::Response {
    coprocessor::Response {
        other_error: message.to_owned(),
        ..coprocessor::Response::default()
    }
}

/// Go `handleCopDAGRequest`'s parse half: `buildDAG`'s guards and decode,
/// with execution refusing by name.
fn handle_cop_dag_request(
    _store: &mut MvccStore,
    req: &coprocessor::Request,
) -> coprocessor::Response {
    let context = match build_dag(req) {
        Ok(context) => context,
        Err(message) => return other_error(&message),
    };
    let _ = &context;
    // boundary: `buildClosureExecutor` (`closure_exec.go`) — the
    // scan-and-evaluate machine is the next course; a parsed DAG cannot yet
    // run.
    other_error("buildClosureExecutor (cophandler/closure_exec.go) is a later course of this port")
}

/// Go `buildDAG` (`cop_handler.go`), the guards and decode.
pub fn build_dag(req: &coprocessor::Request) -> Result<DagContext, String> {
    if req.ranges.is_empty() {
        // Go's exact message.
        return Err("request range is null".to_owned());
    }
    if req.tp != REQ_TYPE_DAG {
        return Err(format!("unsupported request type {}", req.tp));
    }
    let dag_req = tipb::DagRequest::decode(req.data.as_slice())
        .map_err(|decode_err| format!("invalid dag request: {decode_err}"))?;
    let time_zone = match dag_req.time_zone_name() {
        "" => TimeZoneSpec::FixedOffset(dag_req.time_zone_offset()),
        "System" => TimeZoneSpec::System,
        name => TimeZoneSpec::Named(name.to_owned()),
    };
    Ok(DagContext {
        key_ranges: req.ranges.clone(),
        start_ts: req.start_ts,
        time_zone,
        dag_req,
    })
}

/// Go `ExecutorListsToTree` (`cop_handler.go`) — NAMED BOUNDARY. The
/// legacy tree form it builds hangs children on per-type child fields
/// (`Selection.Child`, `Limit.Child`, ...) which the trimmed `tipb` build
/// does not carry: it kept only the MODERN flat-list DAG, where order and
/// `parent_idx` are the structure. The execution course therefore consumes
/// the list directly, and this validation enforces the same invariants the
/// tree conversion would have panicked on.
///
/// Panics carry Go's exact `invalid parentIdx` message; the leaf check
/// mirrors what `buildClosureExecutor` requires — the first executor is the
/// scan and the only scan.
pub fn validate_executor_list(executors: &[tipb::Executor]) {
    let len = executors.len();
    for (i, executor) in executors.iter().enumerate() {
        let tp = executor.tp();
        let is_scan = tp == tipb::ExecType::TypeTableScan || tp == tipb::ExecType::TypeIndexScan;
        assert!(
            (i == 0) == is_scan,
            "executor {i} has type {tp:?}: the first executor is the scan, and the only scan"
        );
        if i + 1 < len {
            let parent_idx = executor
                .parent_idx
                .map_or(i + 1, |idx| usize::try_from(idx).unwrap_or(usize::MAX));
            assert!(
                parent_idx > i && parent_idx < len,
                "invalid parentIdx: {parent_idx}, for index: {i}"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // All WRITTEN: Go's cop_handler coverage rides the store's RPC suites.

    #[test]
    fn unknown_request_types_answer_gos_exact_message() {
        let mut store = MvccStore::new();
        let resp = handle_cop_request(
            &mut store,
            &coprocessor::Request {
                tp: 999,
                ..coprocessor::Request::default()
            },
        );
        assert_eq!(resp.other_error, "unsupported request type 999");
    }

    #[test]
    fn an_empty_range_list_is_gos_null_range_error() {
        let err = build_dag(&coprocessor::Request {
            tp: REQ_TYPE_DAG,
            ..coprocessor::Request::default()
        })
        .expect_err("no ranges");
        assert_eq!(err, "request range is null");
    }

    #[test]
    fn a_dag_decodes_with_its_zone_split_three_ways() {
        let dag = tipb::DagRequest {
            time_zone_offset: Some(3600),
            ..tipb::DagRequest::default()
        };
        let mut data = Vec::new();
        dag.encode(&mut data).expect("encodes");
        let req = coprocessor::Request {
            tp: REQ_TYPE_DAG,
            data,
            ranges: vec![coprocessor::KeyRange::default()],
            start_ts: 42,
            ..coprocessor::Request::default()
        };
        let context = build_dag(&req).expect("parses");
        assert_eq!(context.start_ts, 42);
        assert_eq!(context.time_zone, TimeZoneSpec::FixedOffset(3600));

        let named = tipb::DagRequest {
            time_zone_name: Some("Asia/Shanghai".to_owned()),
            ..tipb::DagRequest::default()
        };
        let mut data = Vec::new();
        named.encode(&mut data).expect("encodes");
        let context = build_dag(&coprocessor::Request {
            tp: REQ_TYPE_DAG,
            data,
            ranges: vec![coprocessor::KeyRange::default()],
            ..coprocessor::Request::default()
        })
        .expect("parses");
        assert_eq!(
            context.time_zone,
            TimeZoneSpec::Named("Asia/Shanghai".to_owned())
        );
    }

    #[test]
    fn a_leaf_first_list_validates() {
        let scan = tipb::Executor {
            tp: Some(tipb::ExecType::TypeTableScan as i32),
            ..tipb::Executor::default()
        };
        let selection = tipb::Executor {
            tp: Some(tipb::ExecType::TypeSelection as i32),
            ..tipb::Executor::default()
        };
        let limit = tipb::Executor {
            tp: Some(tipb::ExecType::TypeLimit as i32),
            ..tipb::Executor::default()
        };
        validate_executor_list(&[scan, selection, limit]);
    }

    #[test]
    #[should_panic(expected = "invalid parentIdx")]
    fn a_backward_parent_index_panics_with_gos_message() {
        let scan = tipb::Executor {
            tp: Some(tipb::ExecType::TypeTableScan as i32),
            ..tipb::Executor::default()
        };
        let mut selection = tipb::Executor {
            tp: Some(tipb::ExecType::TypeSelection as i32),
            ..tipb::Executor::default()
        };
        selection.parent_idx = Some(0);
        let limit = tipb::Executor {
            tp: Some(tipb::ExecType::TypeLimit as i32),
            ..tipb::Executor::default()
        };
        validate_executor_list(&[scan, selection, limit]);
    }

    #[test]
    #[should_panic(expected = "the first executor is the scan")]
    fn a_scan_in_the_middle_is_refused() {
        let selection = tipb::Executor {
            tp: Some(tipb::ExecType::TypeSelection as i32),
            ..tipb::Executor::default()
        };
        let scan = tipb::Executor {
            tp: Some(tipb::ExecType::TypeTableScan as i32),
            ..tipb::Executor::default()
        };
        validate_executor_list(&[selection, scan]);
    }
}
