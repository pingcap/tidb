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

use std::collections::HashMap;

use tidb_protocol::CompressionAlgorithm;
use tidb_server::handshake::CLIENT_ZSTD_COMPRESSION_ALGORITHM;
use tidb_server::{
    AuthHandshakeRequest, HandshakeResponse41, NegotiatedCompression, CLIENT_COMPRESS,
};

fn handshake_request(capability: u32, zstd_level: i32) -> AuthHandshakeRequest {
    AuthHandshakeRequest {
        response: HandshakeResponse41 {
            attrs: HashMap::new(),
            user: "root".to_owned(),
            db_name: String::new(),
            auth_plugin: "mysql_native_password".to_owned(),
            auth: Vec::new(),
            zstd_level,
            capability,
            collation: 45,
        },
        negotiated_capability: capability,
        raw_packet: Vec::new(),
        server_auth_plugin: "mysql_native_password".to_owned(),
    }
}

#[test]
fn handshake_compression_state_preserves_source_selection_and_level() {
    let none = NegotiatedCompression::from_handshake(&handshake_request(0, 0));
    assert_eq!(none.algorithm(), CompressionAlgorithm::None);
    assert_eq!(none.zstd_level(), 0);

    let zstd = NegotiatedCompression::from_handshake(&handshake_request(
        CLIENT_ZSTD_COMPRESSION_ALGORITHM,
        7,
    ));
    assert_eq!(zstd.algorithm(), CompressionAlgorithm::Zstd);
    assert_eq!(zstd.zstd_level(), 7);

    let both = NegotiatedCompression::from_handshake(&handshake_request(
        CLIENT_COMPRESS | CLIENT_ZSTD_COMPRESSION_ALGORITHM,
        11,
    ));
    assert_eq!(both.algorithm(), CompressionAlgorithm::Zlib);
    assert_eq!(both.zstd_level(), 11);
}

