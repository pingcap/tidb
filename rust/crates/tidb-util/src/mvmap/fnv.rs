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
    use super::{OFFSET64, PRIME64, fnv_hash64};
    use sha2::{Digest, Sha256};
    use std::{collections::BTreeMap, fs, path::PathBuf};

    const GO_SOURCE_SHA256: &str =
        "751533a97a8383c70a9d94200838c71061fbea9a1c95d9a40b53b53058098414";
    const INVENTORY_SHA256: &str =
        "05bb922fe1e794e41b6008f83e1f2d220ed107675b5dc184794065656cf32c5d";
    const PRODUCTION_PREFIX_SHA256: &str =
        "5bb6f7da1958615056f1de97332b7902671663c03fd92971468e920ec2408a8b";
    const INVENTORY: &str = include_str!("fnv.inventory.tsv");

    fn repo_root() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../..")
    }

    fn sha256(bytes: impl AsRef<[u8]>) -> String {
        format!("{:x}", Sha256::digest(bytes.as_ref()))
    }

    fn rust_source() -> String {
        fs::read_to_string(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/mvmap/fnv.rs"))
            .unwrap()
    }

    fn production_source() -> String {
        rust_source()
            .split_once("#[cfg(test)]")
            .unwrap()
            .0
            .to_owned()
    }

    #[test]
    fn lockdown_inventory_matches_go_source_and_rust_symbols() {
        let go_source = fs::read(repo_root().join("pkg/util/mvmap/fnv.go")).unwrap();
        assert_eq!(
            sha256(go_source),
            GO_SOURCE_SHA256,
            "owning Go source drifted"
        );
        assert_eq!(sha256(INVENTORY), INVENTORY_SHA256, "FNV inventory drifted");

        let rows: Vec<Vec<&str>> = INVENTORY
            .lines()
            .filter(|line| !line.starts_with('#') && !line.starts_with("id\t"))
            .map(|line| line.split('\t').collect())
            .collect();
        assert!(rows.iter().all(|row| row.len() == 6));
        let actual: Vec<[&str; 5]> = rows
            .iter()
            .map(|row| [row[0], row[1], row[2], row[3], row[4]])
            .collect();
        let expected = [
            [
                "D01",
                "declaration",
                "offset64 uint64 constant",
                "PORTED",
                "OFFSET64",
            ],
            [
                "D02",
                "declaration",
                "prime64 uint64 constant",
                "PORTED",
                "PRIME64",
            ],
            [
                "F01",
                "function",
                "fnvHash64(data []byte) uint64",
                "PORTED",
                "fnv_hash64",
            ],
            [
                "B01",
                "branch",
                "for loop visits every input byte in order",
                "PORTED",
                "fnv_hash64",
            ],
            [
                "B02",
                "branch",
                "nil or empty input executes zero iterations",
                "PORTED",
                "fnv_hash64",
            ],
            [
                "R01",
                "rule",
                "the accumulator starts at offset64",
                "PORTED",
                "fnv_hash64",
            ],
            [
                "R02",
                "rule",
                "each step multiplies by prime64 before XOR",
                "PORTED",
                "fnv_hash64",
            ],
            [
                "R03",
                "rule",
                "uint64 multiplication wraps modulo 2^64",
                "PORTED",
                "fnv_hash64",
            ],
            [
                "R04",
                "rule",
                "each byte is widened to uint64 without sign extension",
                "PORTED",
                "fnv_hash64",
            ],
            [
                "R05",
                "rule",
                "all hash state is call-local and concurrent calls are safe",
                "PORTED",
                "fnv_hash64",
            ],
            [
                "R06",
                "rule",
                "the final accumulator is returned after all bytes",
                "PORTED",
                "fnv_hash64",
            ],
        ];
        assert_eq!(actual, expected, "the exact inventory mapping drifted");

        let mut statuses = BTreeMap::new();
        for row in &rows {
            *statuses.entry(row[3]).or_insert(0usize) += 1;
        }
        assert_eq!(statuses.get("PORTED"), Some(&11));
        assert_eq!(statuses.get("DECLINED"), None);
        assert_eq!(statuses.get("UNREACHABLE"), None);

        let production = production_source();
        for row in &rows {
            let symbol = row[4];
            let present = match symbol {
                "OFFSET64" | "PRIME64" => production.contains(&format!("const {symbol}: u64")),
                "fnv_hash64" => production.contains("pub(super) fn fnv_hash64(data: &[u8]) -> u64"),
                _ => false,
            };
            assert!(present, "{} names missing Rust symbol {symbol}", row[0]);
        }
    }

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

        let production = production_source();
        assert_eq!(
            sha256(&production),
            PRODUCTION_PREFIX_SHA256,
            "the audited FNV production path changed"
        );
        assert!(!production.contains("static mut"));
        assert!(!production.contains("Mutex"));
        assert!(!production.contains("RwLock"));
    }
}
