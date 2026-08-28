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

//! Local batch admission: a busy local batch falls back without feeding a
//! route failure back to the selector.

#![allow(missing_docs)]

use crate::direct_unary_client_fixture::*;

#[test]
fn local_batch_admission_busy_falls_back_without_route_failure_feedback() {
    let calls = Rc::new(RefCell::new(Vec::new()));
    let events = Rc::new(RefCell::new(Vec::new()));
    let retry_control = Rc::new(RecordingRetryControl::default());
    let transport = DirectUnaryQueryTransport::new_injected_batch_first(
        ScriptedClient {
            calls: Rc::clone(&calls),
            responses: VecDeque::from([Ok(response(b"sync-after-local-admission"))]),
            events: Rc::clone(&events),
            liveness: RefCell::new(VecDeque::new()),
            batch_errors: RefCell::new(VecDeque::from([DirectUnaryClientError::AdmissionBusy {
                address: "tikv-1:20160".to_owned(),
            }])),
            batch_ready_immediately: RefCell::new(VecDeque::new()),
            batch_begin_count: None,
        },
        RegionCache::new(ScriptedLoader {
            cluster_id: 9001,
            calls: Rc::new(RefCell::new(Vec::new())),
            regions: VecDeque::from([location(1, "a", "z", "tikv-1:20160")]),
        }),
        DirectUnaryRuntimeConfig {
            region_retry_waiter: retry_control.clone(),
            ..DirectUnaryRuntimeConfig::default()
        },
        tidb_txnkv::lock::FixedTimestampSource::new(1 << 18),
    )
    .unwrap();
    let mut runtime = InjectedQueryRuntime::new(transport);
    let mut result = select_result(&mut runtime, &transport_request(metadata("a", "z")));

    assert_eq!(
        result.next_raw().unwrap(),
        Some(b"sync-after-local-admission".to_vec())
    );
    assert_eq!(result.next_raw().unwrap(), None);
    assert_eq!(calls.borrow().len(), 1);
    assert_eq!(
        events.borrow().as_slice(),
        [ClientEvent::Send("tikv-1:20160".to_owned())]
    );
    assert!(retry_control.sleeps.borrow().is_empty());
}
