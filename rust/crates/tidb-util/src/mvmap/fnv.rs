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

// Copyright 2011 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//! Transcreation of Go `pkg/util/mvmap/fnv.go`.

const OFFSET64: u64 = 14695981039346656037;
const PRIME64: u64 = 1099511628211;

/// FNV-1 64-bit hash, ported from the Go standard library (`hash/fnv`). The
/// multiply wraps on overflow, exactly as Go's `uint64` arithmetic does.
pub(super) fn fnv_hash64(data: &[u8]) -> u64 {
    let mut hash = OFFSET64;
    for &c in data {
        hash = hash.wrapping_mul(PRIME64);
        hash ^= u64::from(c);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::{fnv_hash64, OFFSET64, PRIME64};

    // Go `TestFNVHash` compares `fnvHash64` against the standard library's
    // `hash/fnv` `New64` (FNV-1). Rust has no `hash/fnv`, so the reference value
    // is pinned directly: it is the FNV-1-64 digest of the test bytes (which are
    // the FNV offset basis itself, big-endian).
    #[test]
    fn fnv_hash() {
        let b = [0xcb, 0xf2, 0x9c, 0xe4, 0x84, 0x22, 0x23, 0x25];
        assert_eq!(fnv_hash64(&b), 5886032377557422844);
    }

    #[test]
    fn source_constants_and_boundary_vectors_are_exact() {
        assert_eq!(OFFSET64, 14_695_981_039_346_656_037);
        assert_eq!(PRIME64, 1_099_511_628_211);
        for (data, expected) in [
            (&[][..], 0xcbf29ce484222325),
            (&[0][..], 0xaf63bd4c8601b7df),
            (&[0xff][..], 0xaf63bd4c8601b720),
            (&[0, 0xff, 0x80, 0x7f][..], 0x4a3cb47f9b54e61d),
            (
                &[0xcb, 0xf2, 0x9c, 0xe4, 0x84, 0x22, 0x23, 0x25][..],
                0x51af634308c212fc,
            ),
            (&b"foobar"[..], 0x340d8765a4dda9c2),
        ] {
            assert_eq!(fnv_hash64(data), expected, "{data:02x?}");
        }
    }

    #[test]
    fn source_step_order_and_wrapping_are_exact() {
        let multiplied = OFFSET64.wrapping_mul(PRIME64);
        assert_eq!(multiplied, 0xaf63bd4c8601b7df);
        assert_eq!(fnv_hash64(&[0xff]), multiplied ^ 0xff);
        assert_ne!(fnv_hash64(&[0xff]), (OFFSET64 ^ 0xff).wrapping_mul(PRIME64));
    }
}
