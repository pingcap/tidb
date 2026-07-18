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

//! Human-readable byte-size parsing from `pkg/executor/inspection_result.go`.
//!
//! The source accepts decimal integers with optional case-sensitive binary
//! suffixes. Inspection retrieval, warning publication, and configuration
//! semantics remain outside this dependency-closed parser.

const KIB: u64 = 1024;
const MIB: u64 = KIB * 1024;
const GIB: u64 = MIB * 1024;
const TIB: u64 = GIB * 1024;
const PIB: u64 = TIB * 1024;

/// Converts a source-formatted byte size to bytes.
///
/// The parser intentionally preserves TiDB's source boundaries: suffixes are
/// case-sensitive, a recognized three-byte suffix is only stripped when the
/// input has additional characters, and the final multiplication wraps like
/// Go's `uint64` arithmetic.
pub fn readable_size_to_bytes(input: &str) -> Result<u64, std::num::ParseIntError> {
    let rate = if input.ends_with("KiB") {
        KIB
    } else if input.ends_with("MiB") {
        MIB
    } else if input.ends_with("GiB") {
        GIB
    } else if input.ends_with("TiB") {
        TIB
    } else if input.ends_with("PiB") {
        PIB
    } else {
        1
    };

    let mut number = input;
    if rate != 1 && number.len() > 3 {
        number = &number[..number.len() - 3];
    }
    if let Some(stripped) = number.strip_suffix('B') {
        number = stripped;
    }
    let parsed = number.parse::<i64>()?;
    Ok((parsed as u64).wrapping_mul(rate))
}
