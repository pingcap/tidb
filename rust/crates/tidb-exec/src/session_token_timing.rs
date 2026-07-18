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

//! Session-migration token timing policy from
//! `pkg/sessionctx/sessionstates/session_token.go`.
//!
//! TiDB uses short classic token/certificate windows and longer Starter
//! windows. This leaf keeps those source constants and mode selection typed;
//! certificate loading/rotation, signing, token serialization, failpoints,
//! and authentication remain external session/server responsibilities.

use std::time::Duration;

/// Deployment mode that selects session-token timing values.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SessionTokenMode {
    /// Regular TiDB deployment.
    Classic,
    /// Starter/zero-backend deployment.
    Starter,
}

/// Timing windows used by session-token creation and certificate rotation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SessionTokenTiming {
    /// Maximum token lifetime.
    pub token_lifetime: Duration,
    /// Certificate reload interval.
    pub load_cert_interval: Duration,
    /// Grace period for an old signing certificate.
    pub old_cert_valid_time: Duration,
}

impl SessionTokenTiming {
    /// Returns source-compatible timings for the selected deployment mode.
    #[must_use]
    pub const fn for_mode(mode: SessionTokenMode) -> Self {
        match mode {
            SessionTokenMode::Classic => Self {
                token_lifetime: Duration::from_secs(60),
                load_cert_interval: Duration::from_secs(10 * 60),
                old_cert_valid_time: Duration::from_secs(15 * 60),
            },
            SessionTokenMode::Starter => Self {
                token_lifetime: Duration::from_secs(8 * 60 * 60),
                load_cert_interval: Duration::from_secs(24 * 60 * 60),
                old_cert_valid_time: Duration::from_secs(36 * 60 * 60),
            },
        }
    }
}
