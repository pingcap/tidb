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

#[path = "../src/secure_transport.rs"]
mod secure_transport;

use secure_transport::{
    SecureTransportError, SecureTransportPolicy, TransportDecision, TransportKind,
};

#[test]
fn disabled_policy_allows_plain_tcp() {
    let policy = SecureTransportPolicy::new(false);
    assert!(!policy.require_secure_transport());
    assert_eq!(
        policy.admit(TransportKind::PlainTcp),
        Ok(TransportDecision::Allowed)
    );
}

#[test]
fn enabled_policy_rejects_only_plain_tcp() {
    let policy = SecureTransportPolicy::new(true);
    assert!(policy.require_secure_transport());
    assert_eq!(
        policy.admit(TransportKind::PlainTcp),
        Err(SecureTransportError::Required)
    );
}

#[test]
fn enabled_policy_preserves_unix_and_tls_exemptions() {
    let policy = SecureTransportPolicy::new(true);
    for transport in [
        TransportKind::UnixSocket,
        TransportKind::DirectTls,
        TransportKind::GatewayTls,
    ] {
        assert_eq!(
            policy.admit(transport),
            Ok(TransportDecision::Allowed),
            "{transport:?} should satisfy the secure-transport policy"
        );
    }
}

#[test]
fn policy_does_not_claim_tls_or_authentication() {
    // The type contains only the source admission fact. DirectTls and
    // GatewayTls are supplied by a later transport owner; this test keeps the
    // boundary from growing a fake handshake or password-verification path.
    let policy = SecureTransportPolicy::new(true);
    assert_eq!(
        policy.admit(TransportKind::DirectTls),
        Ok(TransportDecision::Allowed)
    );
    assert_eq!(
        policy.admit(TransportKind::GatewayTls),
        Ok(TransportDecision::Allowed)
    );
}
