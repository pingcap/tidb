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

use tidb_server::{
    AuthTokenCheck, AuthTokenCheckAction, AuthTokenCheckError, AuthTokenJwksState,
    AuthTokenRetryState, JwtCompactShape, AUTH_TOKEN_INVALID_JWT, AUTH_TOKEN_NO_VALID_JWKS,
    AUTH_TOKEN_RETRY_EXHAUSTED,
};

#[test]
fn compact_shape_keeps_go_three_segment_boundary() {
    // Source: pkg/privilege/privileges/tidb_auth_token.go:91-96 and
    // pkg/privilege/privileges/tidb_auth_token_test.go:386-394. Go checks
    // only strings.Split(token, ".") length; empty segments stay opaque to
    // this leaf and JWT decoding remains external.
    assert_eq!(
        JwtCompactShape::parse("header.payload.signature")
            .unwrap()
            .segments(),
        3
    );
    assert_eq!(JwtCompactShape::parse("..").unwrap().segments(), 3);
    assert_eq!(
        JwtCompactShape::parse("header.payload"),
        Err(AuthTokenCheckError::InvalidJwt { segments: 2 })
    );
    assert_eq!(
        JwtCompactShape::parse("header.payload.signature.extra"),
        Err(AuthTokenCheckError::InvalidJwt { segments: 4 })
    );
    assert_eq!(
        AuthTokenCheckError::InvalidJwt { segments: 2 }.to_string(),
        AUTH_TOKEN_INVALID_JWT
    );
}

#[test]
fn retry_state_matches_initial_attempt_plus_retry_time() {
    // Source: pkg/privilege/privileges/tidb_auth_token.go:98-131. The Go
    // loop executes once for retryTime=0, reloads after every verification
    // failure, and stops only after the initial attempt plus retries.
    let mut retries = AuthTokenRetryState::new(2);
    let first = retries.start_attempt().unwrap();
    assert_eq!(first.number(), 0);
    assert_eq!(first.retries_remaining(), 2);
    let second = retries.start_attempt().unwrap();
    assert_eq!(second.number(), 1);
    assert_eq!(second.retries_remaining(), 1);
    let third = retries.start_attempt().unwrap();
    assert_eq!(third.number(), 2);
    assert_eq!(third.retries_remaining(), 0);
    assert_eq!(
        retries.start_attempt(),
        Err(AuthTokenCheckError::RetryExhausted)
    );
    assert_eq!(retries.retry_time(), 2);
    assert_eq!(retries.attempts_started(), 3);
}

#[test]
fn failed_verification_reloads_before_retry_and_payload_rejection_does_not() {
    // Source: pkg/privilege/privileges/tidb_auth_token.go:98-131. A failed
    // verify calls load and then retries; malformed verified payload/claims
    // continue the loop without another load. The external verifier and JSON
    // claims owner are deliberately not implemented here.
    let mut check = AuthTokenCheck::begin("header.payload.signature", 1).unwrap();
    let first = check.start_attempt().unwrap();
    assert_eq!(
        check.on_verification_failure(first),
        AuthTokenCheckAction::ReloadJwks { failed_attempt: 0 }
    );
    let second = check.after_jwks_reload(Ok(())).unwrap();
    assert_eq!(second.number(), 1);
    assert_eq!(
        check.on_verified_payload(second),
        AuthTokenCheckAction::AwaitExternalClaims { attempt: 1 }
    );
    assert_eq!(
        check.after_payload_rejection(),
        Err(AuthTokenCheckError::RetryExhausted)
    );
}

#[test]
fn missing_jwks_and_load_errors_remain_explicit() {
    // Source: pkg/privilege/privileges/tidb_auth_token.go:48-58, 65-85 and
    // pkg/privilege/privileges/tidb_auth_token_test.go:364-370, 396-400.
    // Missing key state and loader failures are surfaced without filesystem,
    // network refresh, RSA/JWK verification, or claims/authentication logic.
    let mut state = AuthTokenJwksState::Missing;
    assert_eq!(state.verify_gate(), Err(AuthTokenCheckError::MissingJwks));
    assert_eq!(
        AuthTokenCheckError::MissingJwks.to_string(),
        AUTH_TOKEN_NO_VALID_JWKS
    );
    assert_eq!(
        state.record_load(Err(
            "open wrong-jwks-path: no such file or directory".to_owned()
        )),
        Err(AuthTokenCheckError::JwksLoadFailed(
            "open wrong-jwks-path: no such file or directory".to_owned()
        ))
    );
    assert_eq!(state, AuthTokenJwksState::Missing);
    state.record_load(Ok(())).unwrap();
    assert_eq!(state.verify_gate(), Ok(()));
    assert_eq!(
        state.record_load(Err("refresh failed".to_owned())),
        Err(AuthTokenCheckError::JwksLoadFailed(
            "refresh failed".to_owned()
        ))
    );
    assert_eq!(state, AuthTokenJwksState::Loaded);
}

#[test]
fn retry_load_error_and_zero_retry_preserve_go_order() {
    // Source: pkg/privilege/privileges/tidb_auth_token.go:101-108. Even
    // retryTime=0 performs the initial verify, attempts a JWKS load after a
    // failed verification, and only then reaches retry exhaustion.
    let mut check = AuthTokenCheck::begin("header.payload.signature", 0).unwrap();
    let first = check.start_attempt().unwrap();
    assert_eq!(
        check.on_verification_failure(first),
        AuthTokenCheckAction::ReloadJwks { failed_attempt: 0 }
    );
    let load_error = AuthTokenCheckError::JwksLoadFailed("load failed".to_owned());
    assert_eq!(
        check.after_jwks_reload(Err(load_error.clone())),
        Err(load_error)
    );
    assert_eq!(check.retry_state().attempts_started(), 1);
    assert_eq!(
        AUTH_TOKEN_RETRY_EXHAUSTED,
        AuthTokenCheckError::RetryExhausted.to_string()
    );
}
