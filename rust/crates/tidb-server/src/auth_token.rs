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

//! Source-shaped JWT compact-token and JWKS retry admission.
//!
//! Go's `pkg/privilege/privileges/tidb_auth_token.go` checks the compact JWT
//! shape, retries an external verification attempt, and reloads the JWKS after
//! a failed attempt.  This leaf owns only that deterministic control flow. It
//! does not parse or verify RSA/JWK signatures, read files, refresh a network
//! key set, decode claims, or publish an authenticated session.

use std::fmt;

/// The source error text for a token that is not split into three segments.
pub const AUTH_TOKEN_INVALID_JWT: &str = "Invalid JWT";
/// The source error text when no JWKS has been loaded yet.
pub const AUTH_TOKEN_NO_VALID_JWKS: &str = "No valid JWKS yet";
/// The source error text after all verification attempts are spent.
pub const AUTH_TOKEN_RETRY_EXHAUSTED: &str = "Retry time has been spent out";

/// Opaque compact-token shape accepted by Go's `strings.Split(token, ".")`.
///
/// The source checks only that there are exactly three segments. Empty
/// segments are retained as a valid shape here because decoding and signature
/// validation belong to the external JWT owner.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct JwtCompactShape {
    segments: u8,
}

impl JwtCompactShape {
    /// Checks the source's exact three-segment boundary without decoding data.
    pub fn parse(token: &str) -> Result<Self, AuthTokenCheckError> {
        let segments = token.split('.').count();
        if segments != 3 {
            return Err(AuthTokenCheckError::InvalidJwt { segments });
        }
        Ok(Self { segments: 3 })
    }

    /// Returns the fixed number of compact-token segments.
    #[must_use]
    pub const fn segments(self) -> u8 {
        self.segments
    }
}

/// JWKS availability observed by the external key-set owner.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuthTokenJwksState {
    /// No successful key-set load has been observed.
    Missing,
    /// A prior key-set load succeeded; the key bytes remain external.
    Loaded,
}

impl AuthTokenJwksState {
    /// Models Go's `JWKSImpl.verify` precondition without verifying a token.
    pub fn verify_gate(self) -> Result<(), AuthTokenCheckError> {
        match self {
            Self::Missing => Err(AuthTokenCheckError::MissingJwks),
            Self::Loaded => Ok(()),
        }
    }

    /// Records a source-shaped JWKS load result.
    ///
    /// A failed reload does not erase a previously loaded key set, matching
    /// Go's `load`: the atomic pointer changes only after `ReadFile` succeeds.
    pub fn record_load(&mut self, result: Result<(), String>) -> Result<(), AuthTokenCheckError> {
        match result {
            Ok(()) => {
                *self = Self::Loaded;
                Ok(())
            }
            Err(reason) => Err(AuthTokenCheckError::JwksLoadFailed(reason)),
        }
    }
}

/// One external verification attempt and the retries still available after it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AuthTokenAttempt {
    number: u32,
    retries_remaining: u32,
}

impl AuthTokenAttempt {
    /// Zero-based attempt number, where the initial check is attempt zero.
    #[must_use]
    pub const fn number(self) -> u32 {
        self.number
    }

    /// Number of later verification attempts that may still be started.
    #[must_use]
    pub const fn retries_remaining(self) -> u32 {
        self.retries_remaining
    }
}

/// Retry control state matching `for retryTime >= 0` in Go.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthTokenRetryState {
    retry_time: u32,
    attempts_started: u32,
}

impl AuthTokenRetryState {
    /// Creates a state with one initial attempt plus `retry_time` retries.
    #[must_use]
    pub const fn new(retry_time: u32) -> Self {
        Self {
            retry_time,
            attempts_started: 0,
        }
    }

    /// Starts the next external verification attempt.
    pub fn start_attempt(&mut self) -> Result<AuthTokenAttempt, AuthTokenCheckError> {
        let max_attempts = self.retry_time.saturating_add(1);
        if self.attempts_started >= max_attempts {
            return Err(AuthTokenCheckError::RetryExhausted);
        }
        let attempt = AuthTokenAttempt {
            number: self.attempts_started,
            retries_remaining: max_attempts - self.attempts_started - 1,
        };
        self.attempts_started = self.attempts_started.saturating_add(1);
        Ok(attempt)
    }

    /// Number of retry opportunities configured by the source caller.
    #[must_use]
    pub const fn retry_time(&self) -> u32 {
        self.retry_time
    }

    /// Number of attempts already handed to the external verifier.
    #[must_use]
    pub const fn attempts_started(&self) -> u32 {
        self.attempts_started
    }
}

/// Next action after the external owner reports a verification-stage event.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuthTokenCheckAction {
    /// Reload the external JWKS before starting another verification attempt.
    ReloadJwks {
        /// Zero-based attempt whose verification failed.
        failed_attempt: u32,
    },
    /// The opaque verified payload still needs external JSON/claims handling.
    AwaitExternalClaims {
        /// Zero-based attempt that produced the opaque payload.
        attempt: u32,
    },
}

/// Errors from compact-shape and retry/load admission.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AuthTokenCheckError {
    /// The compact token did not contain exactly three dot-separated segments.
    InvalidJwt {
        /// Number of dot-separated segments observed.
        segments: usize,
    },
    /// No successful JWKS load is available to the external verifier.
    MissingJwks,
    /// The external JWKS loader returned an error; its text is preserved.
    JwksLoadFailed(String),
    /// No initial attempt or configured retry remains.
    RetryExhausted,
}

impl fmt::Display for AuthTokenCheckError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidJwt { .. } => formatter.write_str(AUTH_TOKEN_INVALID_JWT),
            Self::MissingJwks => formatter.write_str(AUTH_TOKEN_NO_VALID_JWKS),
            Self::JwksLoadFailed(reason) => formatter.write_str(reason),
            Self::RetryExhausted => formatter.write_str(AUTH_TOKEN_RETRY_EXHAUSTED),
        }
    }
}

impl std::error::Error for AuthTokenCheckError {}

/// Dependency-closed token-check state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthTokenCheck {
    shape: JwtCompactShape,
    retry: AuthTokenRetryState,
}

impl AuthTokenCheck {
    /// Validates compact shape and initializes the source retry loop.
    pub fn begin(token: &str, retry_time: u32) -> Result<Self, AuthTokenCheckError> {
        Ok(Self {
            shape: JwtCompactShape::parse(token)?,
            retry: AuthTokenRetryState::new(retry_time),
        })
    }

    /// Returns the opaque compact-shape result.
    #[must_use]
    pub const fn shape(&self) -> JwtCompactShape {
        self.shape
    }

    /// Starts the initial attempt or the next retry.
    pub fn start_attempt(&mut self) -> Result<AuthTokenAttempt, AuthTokenCheckError> {
        self.retry.start_attempt()
    }

    /// Reports a failed external verification and triggers the source JWKS
    /// reload-before-retry branch, even when no retry remains afterward.
    #[must_use]
    pub const fn on_verification_failure(&self, attempt: AuthTokenAttempt) -> AuthTokenCheckAction {
        AuthTokenCheckAction::ReloadJwks {
            failed_attempt: attempt.number,
        }
    }

    /// Reports an externally verified payload whose JSON/claims handling is
    /// still owned elsewhere. No claims are decoded or authenticated here.
    #[must_use]
    pub const fn on_verified_payload(&self, attempt: AuthTokenAttempt) -> AuthTokenCheckAction {
        AuthTokenCheckAction::AwaitExternalClaims {
            attempt: attempt.number,
        }
    }

    /// Continues after a successful JWKS reload, preserving load errors and
    /// the source retry exhaustion boundary.
    pub fn after_jwks_reload(
        &mut self,
        result: Result<(), AuthTokenCheckError>,
    ) -> Result<AuthTokenAttempt, AuthTokenCheckError> {
        result?;
        self.start_attempt()
    }

    /// Continues after external payload/claims decoding rejected the payload.
    /// Go retries this path without reloading JWKS.
    pub fn after_payload_rejection(&mut self) -> Result<AuthTokenAttempt, AuthTokenCheckError> {
        self.start_attempt()
    }

    /// Returns the retry state for diagnostics and outer orchestration.
    #[must_use]
    pub const fn retry_state(&self) -> &AuthTokenRetryState {
        &self.retry
    }
}
