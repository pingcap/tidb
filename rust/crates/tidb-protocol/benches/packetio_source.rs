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

//! Stable all-target workload for Go's `BenchmarkPacketIOWrite` source anchor.

use std::{hint::black_box, io::Cursor};

use tidb_protocol::{CompressionAlgorithm, PacketIoWriter};

fn main() {
    let packet = [
        0x6d, 0x44, 0x42, 0x3a, 0x35, 0x36, 0, 0, 0, 0xfc, 0, 0, 0, 0, 0, 0, 0, 0x68, 0x54, 0x49,
        0x44, 0x3a, 0x31, 0x30, 0x38, 0, 0xfe,
    ];
    for _ in 0..10_000 {
        let mut writer =
            PacketIoWriter::new(Cursor::new(Vec::new()), CompressionAlgorithm::None).unwrap();
        writer.write_packet_buffer(black_box(&packet)).unwrap();
        black_box(writer.into_inner());
    }
}
