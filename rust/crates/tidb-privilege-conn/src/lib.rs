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

//! Go `pkg/privilege/conn`.

/// Go `AuthConn`: the authentication exchange on this connection.
pub trait AuthConn {
    /// Go `AuthConn.WriteAuthMoreData`: writes a `(u8)1` before `data` (the
    /// MySQL client's `client_mpvio_read_packet` skips it).
    fn write_auth_more_data(&mut self, data: &[u8]) -> Result<(), String>;
    /// Go `AuthConn.ReadPacket`: one more packet, without any prefix.
    fn read_packet(&mut self) -> Result<Vec<u8>, String>;
    /// Go `AuthConn.Flush`: sends every written packet to the client.
    fn flush(&mut self) -> Result<(), String>;
}
