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

use std::io::{self, Cursor, Read, Write};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::time::Instant;

use tidb_protocol::PacketReader;

/// Rust test-only counterpart of Go `testutil.BytesConn`.
///
/// It is intentionally not exported from the production crate: the source
/// package exists only to feed server tests, and Rust's packet reader needs
/// only `Read`. The remaining methods preserve the test double's observable
/// contract without inventing a production socket abstraction.
struct ReadOnlyBytesConn {
    bytes: Cursor<Vec<u8>>,
}

impl ReadOnlyBytesConn {
    fn new(bytes: impl Into<Vec<u8>>) -> Self {
        Self {
            bytes: Cursor::new(bytes.into()),
        }
    }

    fn close(&mut self) -> io::Result<()> {
        Ok(())
    }

    fn local_addr(&self) -> Option<SocketAddr> {
        None
    }

    fn remote_addr(&self) -> Option<SocketAddr> {
        None
    }

    fn set_deadline(&mut self, _deadline: Option<Instant>) -> io::Result<()> {
        Ok(())
    }

    fn set_read_deadline(&mut self, _deadline: Option<Instant>) -> io::Result<()> {
        Ok(())
    }

    fn set_write_deadline(&mut self, _deadline: Option<Instant>) -> io::Result<()> {
        Ok(())
    }
}

impl Read for ReadOnlyBytesConn {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        self.bytes.read(buffer)
    }
}

impl Write for ReadOnlyBytesConn {
    fn write(&mut self, _buffer: &[u8]) -> io::Result<usize> {
        Ok(0)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn read_only_bytes_connection_preserves_the_source_test_contract() {
    let mut reader = PacketReader::new(ReadOnlyBytesConn::new(b"\x03\0\0\0abc"));
    assert_eq!(reader.read_packet().unwrap(), b"abc");

    let mut connection = ReadOnlyBytesConn::new(b"remaining");
    assert_eq!(connection.write(b"discarded").unwrap(), 0);
    assert!(connection.local_addr().is_none());
    assert!(connection.remote_addr().is_none());
    connection.set_deadline(Some(Instant::now())).unwrap();
    connection.set_read_deadline(None).unwrap();
    connection.set_write_deadline(None).unwrap();
    connection.close().unwrap();

    let mut bytes = Vec::new();
    connection.read_to_end(&mut bytes).unwrap();
    assert_eq!(bytes, b"remaining", "no-op methods do not consume bytes");
}

#[test]
fn socket_address_port_is_the_native_get_port_contract() {
    let ipv4 = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 4000);
    let ipv6 = SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 10080);
    assert_eq!(ipv4.port(), 4000);
    assert_eq!(ipv6.port(), 10080);
}
