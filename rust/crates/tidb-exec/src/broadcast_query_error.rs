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

//! BroadcastQuery compatibility-error classification from
//! `pkg/executor/analyze.go`.
//!
//! During rolling upgrades, an older TiDB peer can reject the
//! `BroadcastQuery` coprocessor executor. The source recognizes that response
//! by two message fragments; RPC, logging, and analyze fallback behavior stay
//! outside this dependency-closed classifier.

/// Returns whether an error message denotes an older peer rejecting a
/// BroadcastQuery executor.
#[must_use]
pub fn is_unsupported_broadcast_query_error(message: Option<&str>) -> bool {
    let Some(message) = message else {
        return false;
    };
    message.contains("exec type") && message.contains("doesn't support yet")
}
