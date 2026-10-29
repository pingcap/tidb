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

//! Go `Session.Status()`: the server status word of a session, from the
//! session's own autocommit and transaction facts
//! (`tidb_variable::wire_status`).

use tidb_variable::wire_status::WireStatus;

use crate::Session;

/// Reads the live status off a session, the way `cc.ctx.Status()` does.
///
/// The session owns both facts already, so this derives rather than
/// duplicates: there is no second copy of "am I in a transaction" that can
/// drift from the one the session acts on.
#[must_use]
pub fn session_status(session: &Session) -> WireStatus {
    WireStatus::of_facts(session.is_autocommit(), session.in_transaction())
}
