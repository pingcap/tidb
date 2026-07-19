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

#![allow(missing_docs)]

use tidb_txnkv::{
    BatchRoute, DirectUnaryConnectionError, DirectUnaryResponse, DirectUnaryTransportClass,
};

#[test]
fn successful_and_failed_attempts_expose_the_same_physical_identity_shape() {
    let response = DirectUnaryResponse::new(b"response".to_vec(), "proxy:20160", 17);
    assert_eq!(response.encoded_response, b"response");
    assert_eq!(response.physical_address(), "proxy:20160");
    assert_eq!(response.physical_channel_version(), 17);

    let error = DirectUnaryConnectionError::connection(
        "proxy:20160",
        17,
        "injected channel failure".to_owned(),
    );
    assert_eq!(error.address(), response.physical_address());
    assert_eq!(error.version(), response.physical_channel_version());
    assert_eq!(
        error.transport_class(),
        DirectUnaryTransportClass::Connection
    );
}

#[test]
fn batch_route_keeps_physical_version_distinct_from_stream_generation() {
    let direct = BatchRoute::direct("tikv:20160", 23);
    let forwarded = BatchRoute::forwarded("proxy:20160", "tikv:20160", 29);

    assert_eq!(direct.physical_channel_version(), 1);
    assert_eq!(direct.generation(), 23);
    assert_eq!(forwarded.physical_channel_version(), 1);
    assert_eq!(forwarded.generation(), 29);
    assert_eq!(forwarded.physical_address(), "proxy:20160");
    assert_eq!(forwarded.forwarded_host(), Some("tikv:20160"));
}

#[test]
#[should_panic(expected = "nonzero physical channel version")]
fn a_success_cannot_claim_that_no_physical_channel_was_selected() {
    let _ = DirectUnaryResponse::new(Vec::new(), "tikv:20160", 0);
}

#[test]
fn explicit_shutdown_has_one_fallible_owner_and_one_drop_safety_net() {
    let tonic = include_str!("../src/rpc/tonic_coprocessor.rs");
    let raw = include_str!("../src/rpc/unary.rs");
    let runtime = include_str!("../src/rpc/transport_runtime.rs");

    assert!(!tonic.contains("impl Drop for TonicCoprocessorClient"));
    assert!(!raw.contains("impl Drop for RawTransportClient"));
    assert_eq!(runtime.matches("impl Drop for TransportRuntime").count(), 1);
    assert!(runtime.contains("response.recv().is_err()"));
    assert!(runtime.contains("worker.join()"));
    assert!(runtime.contains("worker panicked during shutdown"));
}
