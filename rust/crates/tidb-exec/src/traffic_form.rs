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

//! Traffic-capture form encoding from `pkg/executor/traffic.go`.
//!
//! TiDB turns the traffic argument map into `url.Values.Encode()` output
//! before posting to TiProxy. This leaf ports the deterministic key ordering
//! and query-component escaping without owning SQL parsing, HTTP, TiProxy
//! discovery, start-time insertion, or session warning state.

const HEX: &[u8; 16] = b"0123456789ABCDEF";

fn query_escape(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                escaped.push(byte as char);
            }
            b' ' => escaped.push('+'),
            byte => {
                escaped.push('%');
                escaped.push(HEX[(byte >> 4) as usize] as char);
                escaped.push(HEX[(byte & 0x0f) as usize] as char);
            }
        }
    }
    escaped
}

/// Encodes form fields with Go's `url.Values.Encode` ordering and escaping.
///
/// The input is a slice rather than a map so callers can preserve Go's value
/// order if a future source path emits repeated keys. Keys are sorted
/// lexicographically; duplicate-key values retain their input order.
#[must_use]
pub fn encode_form(fields: &[(&str, &str)]) -> String {
    let mut sorted = fields.to_vec();
    sorted.sort_by(|left, right| left.0.cmp(right.0));

    let mut encoded = String::new();
    for (index, (key, value)) in sorted.into_iter().enumerate() {
        if index != 0 {
            encoded.push('&');
        }
        encoded.push_str(&query_escape(key));
        encoded.push('=');
        encoded.push_str(&query_escape(value));
    }
    encoded
}
