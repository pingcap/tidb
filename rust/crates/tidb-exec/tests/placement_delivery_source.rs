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

//! The exact HTTP request PD receives for a placement bundle delivery.
//!
//! The contract is PD's own (`client/http/api.go` and `interface.go`):
//! `POST /pd/api/v1/config/placement-rule?partial=true` carrying a JSON array
//! of bundles. Asserting it against a real socket is what makes the wire
//! shape verifiable rather than assumed -- a rule sent in the wrong shape
//! fails SILENTLY, leaving the catalog claiming placement PD never received.

use std::io::{Read, Write};
use std::net::TcpListener;
use std::time::Duration;

use tidb_exec::placement_delivery::{put_rule_bundles, PlacementDeliveryError};
use tidb_placement::new_bundle;

/// Accepts one request, answers `status`, and returns the raw request text.
fn capture_one_request(listener: TcpListener, status: &'static str) -> std::thread::JoinHandle<String> {
    std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("a connection");
        let mut buffer = [0_u8; 8192];
        let read = stream.read(&mut buffer).expect("a request");
        let request = String::from_utf8_lossy(&buffer[..read]).into_owned();
        let response = format!("HTTP/1.1 {status}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
        let _ = stream.write_all(response.as_bytes());
        let _ = stream.flush();
        request
    })
}

#[test]
fn a_bundle_delivery_is_gos_post_with_partial_true() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("a port");
    let endpoint = format!("http://{}", listener.local_addr().expect("an address"));
    let server = capture_one_request(listener, "200 OK");

    let mut bundle = new_bundle(7);
    bundle.index = 3;
    bundle.r#override = true;
    put_rule_bundles(&endpoint, &[bundle], Duration::from_secs(5)).expect("PD accepted");

    let request = server.join().expect("the server thread");

    // Go POSTs to the placement-rule path with `partial=true`
    // (`PlacementRuleBundleWithPartialParameter`, called with true by
    // `PDPlacementManager.PutRuleBundles`). `partial=false` would drop every
    // rule PD holds that this payload does not mention.
    assert!(
        request.starts_with("POST /pd/api/v1/config/placement-rule?partial=true "),
        "unexpected request line: {}",
        request.lines().next().unwrap_or_default()
    );
    assert!(
        request.to_ascii_lowercase().contains("content-type: application/json"),
        "the body is JSON: {request}"
    );

    // The body is a JSON ARRAY of bundles carrying PD's four field names.
    let body = request.split("\r\n\r\n").nth(1).expect("a body");
    let parsed: serde_json::Value = serde_json::from_str(body).expect("valid JSON body");
    let array = parsed.as_array().expect("an array of bundles");
    assert_eq!(array.len(), 1);
    let sent = &array[0];
    assert_eq!(sent["group_id"], "TiDB_DDL_7");
    assert_eq!(sent["group_index"], 3);
    assert_eq!(sent["group_override"], true);
    assert!(sent.get("rules").is_some(), "rules is always present: {sent}");
}

#[test]
fn an_empty_bundle_list_is_not_sent_at_all() {
    // Go returns early for an empty list rather than POSTing `[]`, which PD
    // would read as "these groups now have no rules".
    let listener = TcpListener::bind("127.0.0.1:0").expect("a port");
    let endpoint = format!("http://{}", listener.local_addr().expect("an address"));
    // No server thread: if a request were sent, this would hang or fail.
    put_rule_bundles(&endpoint, &[], Duration::from_secs(5)).expect("nothing to send");
    drop(listener);
}

#[test]
fn a_pd_rejection_is_reported_with_its_status() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("a port");
    let endpoint = format!("http://{}", listener.local_addr().expect("an address"));
    let server = capture_one_request(listener, "400 Bad Request");

    let bundle = new_bundle(9);
    let error = put_rule_bundles(&endpoint, &[bundle], Duration::from_secs(5))
        .expect_err("PD refused");
    let _ = server.join();

    // The statement has to fail when PD refuses: Go fails the DDL job rather
    // than committing a catalog that claims placement PD never accepted.
    match error {
        PlacementDeliveryError::Rejected { status, .. } => assert_eq!(status, 400),
        other => panic!("expected a rejection carrying PD's status, got {other:?}"),
    }
}
