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

//! Local batch admission and the publication observer: a busy local batch
//! falling back without feeding a route failure back to the selector, and the
//! observer running before pending completion and resetting per query.

#![allow(missing_docs)]

use crate::direct_unary_client_fixture::*;

#[test]
fn publication_observer_runs_before_pending_completion_and_resets_per_query() {
    let calls = Rc::new(RefCell::new(Vec::new()));
    let completion_gate = Rc::new(Cell::new(false));
    let transport = DirectUnaryQueryTransport::new_injected_batch_first(
        ScriptedClient {
            calls: Rc::clone(&calls),
            responses: VecDeque::from([
                Ok(response(b"first-published")),
                Ok(response(b"second-published")),
            ]),
            events: Rc::new(RefCell::new(Vec::new())),
            liveness: RefCell::new(VecDeque::new()),
            batch_errors: RefCell::new(VecDeque::new()),
            batch_ready_immediately: RefCell::new(VecDeque::new()),
            batch_completion_gate: Some(Rc::clone(&completion_gate)),
        },
        RegionCache::new(ScriptedLoader {
            cluster_id: 9001,
            calls: Rc::new(RefCell::new(Vec::new())),
            regions: VecDeque::from([location(1, "a", "z", "tikv-1:20160")]),
        }),
        DirectUnaryRuntimeConfig::default(),
        tidb_txnkv::lock::FixedTimestampSource::new(1 << 18),
    )
    .unwrap();
    let evidence = transport.evidence_handle();
    let mut runtime = InjectedQueryRuntime::new(transport);

    let mut first = select_result(&mut runtime, &transport_request(metadata("a", "z")));
    let observed = Rc::new(RefCell::new(Vec::new()));
    let first_observed = Rc::clone(&observed);
    let first_gate = Rc::clone(&completion_gate);
    evidence
        .set_publication_observer(move |publication| {
            first_observed.borrow_mut().push(publication.clone());
            first_gate.set(true);
        })
        .unwrap();
    assert!(evidence.set_publication_observer(|_| {}).is_err());
    assert_eq!(first.next_raw().unwrap(), Some(b"first-published".to_vec()));
    assert_eq!(first.next_raw().unwrap(), None);
    drop(first);

    let first_publication = observed.borrow()[0].clone();
    assert_eq!(first_publication.region_id, 1);
    assert_eq!(
        first_publication.publication.physical_address(),
        "tikv-1:20160"
    );
    assert_eq!(first_publication.publication.physical_channel_version(), 7);
    assert_eq!(first_publication.publication.batch_stream_generation(), 11);
    assert_eq!(first_publication.publication.forwarded_host(), None);
    assert_eq!(evidence.snapshot().published_attempts, [first_publication]);

    completion_gate.set(false);
    let mut second = select_result(&mut runtime, &transport_request(metadata("a", "z")));
    let second_gate = Rc::clone(&completion_gate);
    evidence
        .set_publication_observer(move |_| second_gate.set(true))
        .expect("the next query bind must detach the previous observer");
    assert_eq!(
        second.next_raw().unwrap(),
        Some(b"second-published".to_vec())
    );
    assert_eq!(second.next_raw().unwrap(), None);
    assert_eq!(calls.borrow().len(), 2);
}

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
            batch_completion_gate: None,
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
