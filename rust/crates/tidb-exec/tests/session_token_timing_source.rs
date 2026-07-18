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

//! Source-backed tests for session-token timing policy.

use std::time::Duration;

use tidb_exec::session_token_timing::{SessionTokenMode, SessionTokenTiming};

#[test]
fn session_token_timing_preserves_classic_and_starter_windows() {
    // Source: pkg/sessionctx/sessionstates/session_token.go:49-95 and
    // pkg/sessionctx/sessionstates/session_token_test.go:163-191.
    let classic = SessionTokenTiming::for_mode(SessionTokenMode::Classic);
    assert_eq!(classic.token_lifetime, Duration::from_secs(60));
    assert_eq!(classic.load_cert_interval, Duration::from_secs(10 * 60));
    assert_eq!(classic.old_cert_valid_time, Duration::from_secs(15 * 60));

    let starter = SessionTokenTiming::for_mode(SessionTokenMode::Starter);
    assert_eq!(starter.token_lifetime, Duration::from_secs(8 * 60 * 60));
    assert_eq!(
        starter.load_cert_interval,
        Duration::from_secs(24 * 60 * 60)
    );
    assert_eq!(
        starter.old_cert_valid_time,
        Duration::from_secs(36 * 60 * 60)
    );
}
