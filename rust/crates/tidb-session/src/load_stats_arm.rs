// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The client half of `LOAD STATS`.
//!
//! Go splits the statement in three: `LoadStatsExec.Next` only records the
//! path in the session, the server's `handleLoadStats` asks the CLIENT for
//! the file's bytes over the protocol's local-file transfer, and
//! `LoadStatsInfo.Update` parses them and hands the table to
//! `LoadStatsFromJSON`, which the session's node runs. A front end that owns
//! a session without a wire connection supplies the transfer through
//! [`Session::set_client_local_file_reader`]; without one the statement is
//! refused, as a server whose client cannot send files refuses it.

use std::sync::Arc;

use tidb_executor::DriverError;

use crate::Session;

/// The client side of Go's local-file transfer: the bytes of the file the
/// statement names, as the connection's client reads them.
pub type ClientLocalFileReader = Arc<dyn Fn(&str) -> std::io::Result<Vec<u8>> + Send + Sync>;

impl Session {
    /// Installs the client half of the protocol's local-file transfer, which
    /// `LOAD STATS` reads its dump through.
    pub fn set_client_local_file_reader(&mut self, reader: ClientLocalFileReader) {
        self.client_local_file_reader = Some(reader);
    }

    /// The client's bytes of the file `path` names, through the installed
    /// local-file reader (Go `clientConn.handleLoadStats`'s transfer).
    pub(crate) fn read_client_local_file(&self, path: &str) -> Result<Vec<u8>, DriverError> {
        if path.is_empty() {
            return Err(DriverError::unsupported("Load Stats: file path is empty"));
        }
        let Some(reader) = self.client_local_file_reader.clone() else {
            return Err(DriverError::unsupported(
                "LOAD STATS requires client-local file transfer",
            ));
        };
        reader(path)
            .map_err(|error| DriverError::unsupported(format!("Load Stats: read {path}: {error}")))
    }
}
