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

//! `pkg/sessionctx/sessionstates`: the session state `SHOW SESSION_STATES`
//! encodes and `SET SESSION_STATES` restores, as Go's JSON, and the token a
//! migrated session logs in with.

mod session_states;
mod session_token;

pub use session_states::{
    decode_session_states, Empty, HypoIndexMap, HypoTiFlashReplicaMap, JsonDatum, LastDDLInfo,
    PreparedStmtInfo, QueryInfo, SessionStateType, SessionStates,
};
pub use session_token::{
    create_session_token, get_load_cert_interval, reload_signing_cert, set_cert_path,
    set_key_path, validate_session_token, SessionToken, LOAD_CERT_INTERVAL,
};
pub use tidb_error::catalog::sessionstates::ERR_CANNOT_MIGRATE_SESSION;
