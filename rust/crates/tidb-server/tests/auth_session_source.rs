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
#![allow(dead_code)]

#[path = "../src/auth_session.rs"]
mod auth_session;
#[path = "../src/secure_transport.rs"]
mod secure_transport;

use auth_session::{
    AuthChallenge, AuthRejectionReason, AuthSessionAttempt, AuthSessionError, AuthSessionState,
    AUTH_SOCKET_PLUGIN,
};
use secure_transport::{SecureTransportPolicy, TransportKind};

#[test]
fn challenge_retains_opaque_auth_inputs_and_presence_flag() {
    let challenge = AuthChallenge::new(
        "alice",
        "127.0.0.1",
        "mysql_native_password",
        vec![0x10, 0x20],
        vec![0x30, 0x40],
    );
    assert_eq!(challenge.username(), "alice");
    assert_eq!(challenge.host(), "127.0.0.1");
    assert_eq!(challenge.auth_plugin(), "mysql_native_password");
    assert_eq!(challenge.authentication(), [0x10, 0x20]);
    assert_eq!(challenge.salt(), [0x30, 0x40]);
    assert!(challenge.has_password());

    let empty = AuthChallenge::new("alice", "localhost", "mysql_native_password", [], []);
    assert!(!empty.has_password());
}

#[test]
fn attempt_stops_at_external_verification() {
    let challenge = AuthChallenge::new(
        "alice",
        "127.0.0.1",
        "mysql_native_password",
        [1, 2, 3],
        [4, 5],
    );
    let attempt = AuthSessionAttempt::begin(TransportKind::PlainTcp, challenge.clone())
        .expect("normal auth reaches the external verifier");
    assert!(attempt.is_pending_verification());
    assert_eq!(attempt.challenge(), Some(&challenge));
    assert!(matches!(
        attempt.state(),
        AuthSessionState::PendingVerification(_)
    ));
}

#[test]
fn auth_socket_requires_unix_transport_without_claiming_success() {
    let challenge = AuthChallenge::new("root", "localhost", AUTH_SOCKET_PLUGIN, [], []);
    assert_eq!(
        AuthSessionAttempt::begin(TransportKind::DirectTls, challenge.clone()),
        Err(AuthSessionError::SocketPluginRequiresUnixSocket)
    );

    let attempt = AuthSessionAttempt::begin(TransportKind::UnixSocket, challenge)
        .expect("auth_socket is admitted on Unix sockets");
    let rejected = attempt.reject(AuthRejectionReason::ExternalVerifier);
    assert_eq!(
        rejected.state(),
        &AuthSessionState::Rejected(AuthRejectionReason::ExternalVerifier)
    );
    assert!(!rejected.is_pending_verification());
    assert_eq!(rejected.challenge(), None);
}

#[test]
fn policy_admission_precedes_session_and_plugin_admission() {
    let challenge = AuthChallenge::new(
        "alice",
        "127.0.0.1",
        "mysql_native_password",
        [1, 2],
        [3, 4],
    );
    assert_eq!(
        AuthSessionAttempt::begin_with_policy(
            SecureTransportPolicy::new(true),
            TransportKind::PlainTcp,
            challenge.clone(),
        ),
        Err(AuthSessionError::SecureTransport(
            secure_transport::SecureTransportError::Required
        ))
    );

    let attempt = AuthSessionAttempt::begin_with_policy(
        SecureTransportPolicy::new(true),
        TransportKind::GatewayTls,
        challenge.clone(),
    )
    .expect("secure transport reaches external verification");
    assert_eq!(attempt.challenge(), Some(&challenge));
    assert!(attempt.is_pending_verification());
}
