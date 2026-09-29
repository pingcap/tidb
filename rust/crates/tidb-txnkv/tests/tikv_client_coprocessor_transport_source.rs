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

//! Differential gate for the coprocessor transport seam (ExecPlan Phase 3).
//!
//! In Go, coprocessor task splitting lives in TiDB's `pkg/store/copr`, and
//! client-go supplies only region resolution plus single-region request
//! sending (`SendReq`); mock stores serve coprocessor requests through an
//! injected handler (`mockstore`'s `CoprocessorHandler`). This gate proves
//! the identical composition works over the vendored crate: an externally
//! implemented `CoprocessorHandler` (the seat `tidb-unistore`'s coprocessor
//! executor takes), a client-side `coprocessor::Request` built from the now
//! public proto module, region resolution through the PD client, and
//! dispatch through the request framework's single-region plan — the exact
//! transport `tidb-distsql`'s task loop rides on after the swap.

use std::sync::Arc;

use tikv_client::mock::mocktikv::{MockPdClient, Session};
use tikv_client::pd::PdClient;
use tikv_client::proto::{coprocessor, kvrpcpb};
use tikv_client::request::{Keyspace, Plan, PlanBuilder};
use tikv_client::testutils::{bootstrap_with_single_store, new_mock_tikv, CoprRpcHandler};
use tikv_client::TimestampExt;

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("current-thread runtime builds")
}

/// Stand-in for `tidb-unistore`'s coprocessor executor: proves the handler
/// seam is externally implementable and observes the request it was sent.
struct EchoCoprocessor;

impl CoprRpcHandler for EchoCoprocessor {
    fn handle(
        &self,
        _context: &kvrpcpb::Context,
        _session: &Session,
        request: &coprocessor::Request,
    ) -> coprocessor::Response {
        let ranges: Vec<String> = request
            .ranges
            .iter()
            .map(|range| {
                format!(
                    "{}..{}",
                    String::from_utf8_lossy(&range.start),
                    String::from_utf8_lossy(&range.end)
                )
            })
            .collect();
        coprocessor::Response {
            data: format!(
                "tp={} start_ts={} payload={} ranges={}",
                request.tp,
                request.start_ts,
                String::from_utf8_lossy(&request.data),
                ranges.join(",")
            )
            .into_bytes(),
            ..Default::default()
        }
    }
}

#[test]
fn coprocessor_requests_dispatch_through_region_resolution_to_the_handler() {
    runtime().block_on(async {
        let (_client, cluster, pd) =
            new_mock_tikv("", Some(Arc::new(EchoCoprocessor))).expect("mock TiKV opens");
        bootstrap_with_single_store(&cluster);
        let pd = Arc::new(pd);

        // The copr layer's per-task flow: resolve the task's region, map it
        // to its store, then send one request pinned to that region.
        let region = pd
            .region_for_key(&tikv_client::Key::from(b"task-key".to_vec()))
            .await
            .expect("PD resolves a region for the task key");
        let store = pd
            .clone()
            .map_region_to_store(region)
            .await
            .expect("PD maps the region to its store");

        let start_ts = pd.clone().get_timestamp().await.unwrap();
        let request = coprocessor::Request {
            // DAG request type, as `pkg/store/copr` sends for executors.
            tp: 103,
            data: b"dag-payload".to_vec(),
            start_ts: start_ts.version(),
            ranges: vec![coprocessor::KeyRange {
                start: b"a".to_vec(),
                end: b"z".to_vec(),
            }],
            ..Default::default()
        };

        let plan = PlanBuilder::new(pd.clone(), Keyspace::Disable, request)
            .single_region_with_store(store)
            .await
            .expect("the plan targets the resolved region")
            .plan();
        let response = plan.execute().await.expect("dispatch reaches the handler");

        assert!(
            response.region_error.is_none() && response.other_error.is_empty(),
            "the mock store accepted the region-pinned context: {response:?}"
        );
        let echoed = String::from_utf8(response.data).unwrap();
        assert_eq!(
            echoed,
            format!(
                "tp=103 start_ts={} payload=dag-payload ranges=a..z",
                start_ts.version()
            )
        );
    });
}

#[test]
fn a_stale_region_context_returns_a_region_error_for_the_copr_layer_to_retry() {
    runtime().block_on(async {
        let (_client, cluster, pd) =
            new_mock_tikv("", Some(Arc::new(EchoCoprocessor))).expect("mock TiKV opens");
        bootstrap_with_single_store(&cluster);
        let pd = Arc::new(pd);

        let region = pd
            .region_for_key(&tikv_client::Key::from(b"task-key".to_vec()))
            .await
            .unwrap();
        let region_id = region.region.id;
        let store = pd.clone().map_region_to_store(region).await.unwrap();

        // Split the region after the store was resolved: the pinned epoch is
        // now stale, and the store must answer with a region error — the
        // signal Go's copr layer uses to rebuild its tasks, deliberately not
        // retried inside the client for a single-region send.
        let new_peer = cluster.alloc_id();
        cluster.split(region_id, cluster.alloc_id(), b"m", &[new_peer], new_peer);

        let request = coprocessor::Request {
            tp: 103,
            data: b"dag-payload".to_vec(),
            ranges: vec![coprocessor::KeyRange {
                start: b"a".to_vec(),
                end: b"z".to_vec(),
            }],
            ..Default::default()
        };
        let plan = PlanBuilder::new(pd.clone(), Keyspace::Disable, request)
            .single_region_with_store(store)
            .await
            .unwrap()
            .plan();
        let response = plan.execute().await.expect("the send itself succeeds");
        assert!(
            response.region_error.is_some(),
            "a stale epoch must surface as a region error, got {response:?}"
        );
    });
}
