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

//! Lockdown owner for `pkg/util/size/size.go`.
//!
//! `size.inventory.tsv` classifies every declaration in that Go file. The
//! source fingerprint and Rust symbol gate below make an unreviewed source or
//! inventory drift fail.
//!
//! These values deliberately retain the Go source ABI used by TiDB memory
//! accounting. In particular, they do not claim that an arbitrary Rust
//! container has the same header size as its Go counterpart.

const WORD_SIZE: i64 = std::mem::size_of::<usize>() as i64;

/// One kibibyte, named `KB` to match the source API.
pub const KB: u64 = 1_024;
/// One mebibyte.
pub const MB: u64 = KB * 1_024;
/// One gibibyte.
pub const GB: u64 = MB * 1_024;
/// One tebibyte.
pub const TB: u64 = GB * 1_024;
/// One pebibyte.
pub const PB: u64 = TB * 1_024;

/// Size of a Go slice header, excluding its elements.
pub const SIZE_OF_SLICE: i64 = WORD_SIZE * 3;
/// Size of one Go byte.
pub const SIZE_OF_BYTE: i64 = 1;
/// Size of a Go string header.
pub const SIZE_OF_STRING: i64 = WORD_SIZE * 2;
/// Size of one Go bool.
pub const SIZE_OF_BOOL: i64 = 1;
/// Size of a Go pointer.
pub const SIZE_OF_POINTER: i64 = WORD_SIZE;
/// Size of a Go interface header, excluding its dynamic value.
pub const SIZE_OF_INTERFACE: i64 = WORD_SIZE * 2;
/// Size of one Go `float64`.
pub const SIZE_OF_FLOAT64: i64 = 8;
/// Size of one Go `uint64`.
pub const SIZE_OF_UINT64: i64 = 8;
/// Size of one Go `int32`.
pub const SIZE_OF_INT32: i64 = 4;
/// Size of one architecture-width Go `int`.
pub const SIZE_OF_INT: i64 = WORD_SIZE;
/// Size of one Go `uint8`.
pub const SIZE_OF_UINT8: i64 = 1;
/// Size of one architecture-width Go `uint`.
pub const SIZE_OF_UINT: i64 = WORD_SIZE;
/// Size of a Go function value.
pub const SIZE_OF_FUNC: i64 = WORD_SIZE;
/// Size of one Go `int64`.
pub const SIZE_OF_INT64: i64 = 8;
/// Size of a Go map value, excluding its backing map.
pub const SIZE_OF_MAP: i64 = WORD_SIZE;

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, BTreeSet},
        fmt::Write as _,
    };

    use sha2::{Digest, Sha256};

    use super::*;

    const GO_SOURCE: &[u8] = include_bytes!("../../../../../pkg/util/size/size.go");
    const LOCKDOWN_INVENTORY: &str = include_str!("size.inventory.tsv");
    const EXPECTED_ITEMS: [(&str, (&str, &str)); 20] = [
        ("D01", ("PORTED", "KB")),
        ("D02", ("PORTED", "MB")),
        ("D03", ("PORTED", "GB")),
        ("D04", ("PORTED", "TB")),
        ("D05", ("PORTED", "PB")),
        ("D06", ("PORTED", "SIZE_OF_SLICE")),
        ("D07", ("PORTED", "SIZE_OF_BYTE")),
        ("D08", ("PORTED", "SIZE_OF_STRING")),
        ("D09", ("PORTED", "SIZE_OF_BOOL")),
        ("D10", ("PORTED", "SIZE_OF_POINTER")),
        ("D11", ("PORTED", "SIZE_OF_INTERFACE")),
        ("D12", ("PORTED", "SIZE_OF_FLOAT64")),
        ("D13", ("PORTED", "SIZE_OF_UINT64")),
        ("D14", ("PORTED", "SIZE_OF_INT32")),
        ("D15", ("PORTED", "SIZE_OF_INT")),
        ("D16", ("PORTED", "SIZE_OF_UINT8")),
        ("D17", ("PORTED", "SIZE_OF_UINT")),
        ("D18", ("PORTED", "SIZE_OF_FUNC")),
        ("D19", ("PORTED", "SIZE_OF_INT64")),
        ("D20", ("PORTED", "SIZE_OF_MAP")),
    ];

    #[test]
    fn lockdown_inventory_matches_go_source_and_rust_symbols() {
        let recorded_hash = LOCKDOWN_INVENTORY
            .lines()
            .find_map(|line| line.strip_prefix("# source-sha256\t"))
            .expect("inventory records the owning Go source SHA-256");
        assert_eq!(recorded_hash, sha256_hex(GO_SOURCE), "Go source drifted");

        let mut lines = LOCKDOWN_INVENTORY
            .lines()
            .filter(|line| !line.is_empty() && !line.starts_with('#'));
        assert_eq!(
            lines.next(),
            Some("id\tcategory\tgo_item\tstatus\trust_symbol\tevidence")
        );

        let allowed_statuses = BTreeSet::from(["PORTED", "DECLINED", "UNREACHABLE"]);
        let mut actual = BTreeMap::new();
        for line in lines {
            let columns: Vec<_> = line.split('\t').collect();
            assert_eq!(columns.len(), 6, "invalid inventory row: {line}");
            assert!(
                allowed_statuses.contains(columns[3]),
                "unclassified inventory row: {line}"
            );
            assert!(
                !columns[5].is_empty(),
                "inventory evidence is required: {line}"
            );
            assert!(
                actual
                    .insert(columns[0], (columns[3], columns[4]))
                    .is_none(),
                "duplicate inventory id: {}",
                columns[0]
            );
        }
        assert_eq!(actual, BTreeMap::from(EXPECTED_ITEMS));

        let _: [u64; 5] = [KB, MB, GB, TB, PB];
        let _: [i64; 15] = [
            SIZE_OF_SLICE,
            SIZE_OF_BYTE,
            SIZE_OF_STRING,
            SIZE_OF_BOOL,
            SIZE_OF_POINTER,
            SIZE_OF_INTERFACE,
            SIZE_OF_FLOAT64,
            SIZE_OF_UINT64,
            SIZE_OF_INT32,
            SIZE_OF_INT,
            SIZE_OF_UINT8,
            SIZE_OF_UINT,
            SIZE_OF_FUNC,
            SIZE_OF_INT64,
            SIZE_OF_MAP,
        ];
    }

    #[test]
    fn source_constant_table_is_exact_for_the_target_word_size() {
        assert_eq!(
            (KB, MB, GB, TB, PB),
            (1 << 10, 1 << 20, 1 << 30, 1 << 40, 1 << 50)
        );
        assert_eq!(SIZE_OF_SLICE, WORD_SIZE * 3);
        assert_eq!(SIZE_OF_BYTE, 1);
        assert_eq!(SIZE_OF_STRING, WORD_SIZE * 2);
        assert_eq!(SIZE_OF_BOOL, 1);
        assert_eq!(SIZE_OF_POINTER, WORD_SIZE);
        assert_eq!(SIZE_OF_INTERFACE, WORD_SIZE * 2);
        assert_eq!(SIZE_OF_FLOAT64, 8);
        assert_eq!(SIZE_OF_UINT64, 8);
        assert_eq!(SIZE_OF_INT32, 4);
        assert_eq!(SIZE_OF_INT, WORD_SIZE);
        assert_eq!(SIZE_OF_UINT8, 1);
        assert_eq!(SIZE_OF_UINT, WORD_SIZE);
        assert_eq!(SIZE_OF_FUNC, WORD_SIZE);
        assert_eq!(SIZE_OF_INT64, 8);
        assert_eq!(SIZE_OF_MAP, WORD_SIZE);
    }

    fn sha256_hex(input: &[u8]) -> String {
        Sha256::digest(input)
            .iter()
            .fold(String::with_capacity(64), |mut output, byte| {
                write!(output, "{byte:02x}").expect("write to String");
                output
            })
    }
}
