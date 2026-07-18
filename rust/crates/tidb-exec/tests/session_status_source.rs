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

//! Source-shaped tests for `SessionVars` status-flag operations.

use tidb_exec::session_status::{
    SessionStatus, SERVER_STATUS_AUTOCOMMIT, SERVER_STATUS_CURSOR_EXISTS, SERVER_STATUS_IN_TRANS,
};

#[test]
fn default_status_is_autocommit_and_flag_updates_preserve_unrelated_bits() {
    // Source: pkg/sessionctx/variable/session.go:2505, 2757-2792.
    let status = SessionStatus::new();
    assert!(status.autocommit());
    assert!(!status.in_transaction());

    status.set_flag(SERVER_STATUS_IN_TRANS, true);
    assert!(status.in_transaction());
    status.set_flag(SERVER_STATUS_CURSOR_EXISTS, true);
    assert!(status.in_transaction());
    status.set_flag(SERVER_STATUS_IN_TRANS, false);
    assert!(!status.in_transaction());
    assert!(status.has_flag(SERVER_STATUS_CURSOR_EXISTS));
    assert_eq!(
        status.bits(),
        SERVER_STATUS_AUTOCOMMIT | SERVER_STATUS_CURSOR_EXISTS
    );
}

#[test]
fn clearing_an_absent_flag_is_idempotent_and_multi_bit_queries_match_source_any_bit() {
    // Source: pkg/sessionctx/variable/session.go:2760-2787.
    let status = SessionStatus::default();
    status.set_flag(SERVER_STATUS_IN_TRANS, false);
    assert_eq!(status.bits(), SERVER_STATUS_AUTOCOMMIT);
    assert!(!status.has_flag(SERVER_STATUS_IN_TRANS | SERVER_STATUS_CURSOR_EXISTS));

    status.set_flag(SERVER_STATUS_IN_TRANS, true);
    assert!(status.has_flag(SERVER_STATUS_IN_TRANS | SERVER_STATUS_CURSOR_EXISTS));
    status.set_flag(SERVER_STATUS_CURSOR_EXISTS, true);
    assert!(status.has_flag(SERVER_STATUS_IN_TRANS));
    assert!(status.has_flag(SERVER_STATUS_CURSOR_EXISTS));
    status.set_flag(SERVER_STATUS_IN_TRANS | SERVER_STATUS_CURSOR_EXISTS, false);
    assert!(!status.has_flag(SERVER_STATUS_IN_TRANS));
    assert!(!status.has_flag(SERVER_STATUS_CURSOR_EXISTS));
    assert!(status.autocommit());
}
