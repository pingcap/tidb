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

//! What is refused before anything is sent: a missing cluster loader or an
//! empty PD address failing before client dispatch, and an unsupported
//! operation or request shape failing before PD or TiKV is touched.

#![allow(missing_docs)]

use crate::direct_unary_client_fixture::*;

#[test]
fn missing_cluster_loader_failure_and_empty_pd_address_fail_before_client_dispatch() {
    let calls = Rc::new(RefCell::new(Vec::new()));
    let missing_cluster = DirectUnaryQueryTransport::new_injected(
        ScriptedClient {
            calls: Rc::clone(&calls),
            responses: VecDeque::new(),
            events: Rc::new(RefCell::new(Vec::new())),
            liveness: RefCell::new(VecDeque::new()),
            batch_errors: RefCell::new(VecDeque::new()),
            batch_completion_gate: None,
        },
        RegionCache::new(ScriptedLoader {
            cluster_id: 0,
            calls: Rc::new(RefCell::new(Vec::new())),
            regions: VecDeque::new(),
        }),
        DirectUnaryRuntimeConfig::default(),
        tidb_txnkv::lock::FixedTimestampSource::new(1 << 18),
    )
    .err()
    .unwrap();
    assert_eq!(
        missing_cluster,
        DirectUnaryTransportError::Route(RegionRouteError::MissingClusterId)
    );

    let mut empty = location(2, "a", "z", "ignored");
    empty.stores[0].address.clear();
    let mut runtime =
        InjectedQueryRuntime::new(transport(Rc::clone(&calls), std::iter::empty(), [empty]));
    let mut result = select_result(&mut runtime, &transport_request(metadata("a", "z")));
    let error = result.next_raw().unwrap_err().to_string();
    assert!(error.contains("MissingAddress(202)"), "{error}");
    assert!(calls.borrow().is_empty());

    let mut runtime = InjectedQueryRuntime::new(transport(
        Rc::clone(&calls),
        std::iter::empty(),
        std::iter::empty(),
    ));
    let error = runtime
        .select_with_runtime_stats(
            &transport_request(metadata("a", "z")),
            SelectInput::default(),
            QueryResultContext::new(Vec::new(), WarningCollector::new()),
            Vec::new(),
            0,
            false,
        )
        .err()
        .unwrap()
        .to_string();
    assert!(error.contains("scripted-pd-empty"), "{error}");
    assert!(calls.borrow().is_empty());
}

#[test]
fn unsupported_operation_fails_before_preparing_or_sending() {
    let calls = Rc::new(RefCell::new(Vec::new()));
    let loader_calls = Rc::new(RefCell::new(Vec::new()));
    let mut runtime = InjectedQueryRuntime::new(transport_with_loader_calls(
        Rc::clone(&calls),
        std::iter::empty(),
        [location(1, "a", "z", "one")],
        9001,
        Rc::clone(&loader_calls),
    ));
    let error = runtime
        .select(
            &transport_request(metadata("a", "z")),
            SelectInput::default(),
            QueryResultContext::new(Vec::new(), WarningCollector::new()),
        )
        .err()
        .unwrap()
        .to_string();
    assert!(error.contains("unsupported direct unary operation Select"));
    assert!(calls.borrow().is_empty());
    assert!(loader_calls.borrow().is_empty());
}

#[test]
fn unsupported_request_shape_fails_before_pd_or_tikv() {
    let mut tiflash = metadata("a", "z");
    tiflash.store_type = StoreType::TiFlash;
    let mut analyze = metadata("a", "z");
    analyze.request_type = RequestType::Analyze;
    let mut unordered = metadata("a", "z");
    unordered.keep_order = false;
    let mut batched = metadata("a", "z");
    batched.batch_cop = true;

    for invalid in [tiflash, analyze, unordered, batched] {
        let calls = Rc::new(RefCell::new(Vec::new()));
        let loader_calls = Rc::new(RefCell::new(Vec::new()));
        let mut runtime = InjectedQueryRuntime::new(transport_with_loader_calls(
            Rc::clone(&calls),
            std::iter::empty(),
            [location(1, "a", "z", "one")],
            9001,
            Rc::clone(&loader_calls),
        ));

        assert!(runtime
            .select_with_runtime_stats(
                &transport_request(invalid),
                SelectInput::default(),
                QueryResultContext::new(Vec::new(), WarningCollector::new()),
                Vec::new(),
                0,
                false,
            )
            .is_err());
        assert!(calls.borrow().is_empty());
        assert!(loader_calls.borrow().is_empty());
    }
}
