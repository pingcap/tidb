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

//! Lockdown owner for the complete Go `pkg/util/size` package.
//!
//! `size.artifacts.tsv` hashes both direct package artifacts and
//! `size.inventory.tsv` classifies every generated Go AST obligation. The
//! source fingerprint and Rust symbol gate below make an unreviewed source or
//! inventory drift fail. The package has no Go tests, `TestMain`, benchmarks,
//! fuzz targets, examples, fixtures, generated files, or build-tag variants.
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
    use std::{collections::BTreeMap, fmt::Write as _, fs, path::PathBuf};

    use sha2::{Digest, Sha256};

    use super::*;

    const GO_SOURCE: &[u8] = include_bytes!("../../../../../pkg/util/size/size.go");
    const BUILD_SOURCE: &[u8] = include_bytes!("../../../../../pkg/util/size/BUILD.bazel");
    const ARTIFACTS: &str = include_str!("size.artifacts.tsv");
    const LOCKDOWN_INVENTORY: &str = include_str!("size.inventory.tsv");

    #[test]
    fn lockdown_inventory_matches_go_source_and_rust_symbols() {
        let artifact_rows = data_rows(ARTIFACTS);
        assert_eq!(artifact_rows.len(), 2);
        assert!(artifact_rows.iter().all(|row| row.len() == 3));
        let root = repository_root();
        for row in artifact_rows {
            assert_eq!(
                sha256_hex(&fs::read(root.join(row[0])).expect("read size artifact")),
                row[2],
                "owned artifact drifted: {}",
                row[0]
            );
        }
        assert_eq!(
            sha256_hex(GO_SOURCE),
            artifact_hash(ARTIFACTS, "pkg/util/size/size.go")
        );
        assert_eq!(
            sha256_hex(BUILD_SOURCE),
            artifact_hash(ARTIFACTS, "pkg/util/size/BUILD.bazel")
        );

        let mut lines = LOCKDOWN_INVENTORY
            .lines()
            .filter(|line| !line.is_empty() && !line.starts_with('#'));
        assert_eq!(
            lines.next(),
            Some("obligation_id\tcategory\tsource_path\tast_anchor\tnode_sha256\towner\tstatus\trust_symbol\tevidence\tmutation_policy")
        );

        let expected = BTreeMap::from([
            ("const:GB:0", "GB"),
            ("const:KB:0", "KB"),
            ("const:MB:0", "MB"),
            ("const:PB:0", "PB"),
            ("const:SizeOfBool:0", "SIZE_OF_BOOL"),
            ("const:SizeOfByte:0", "SIZE_OF_BYTE"),
            ("const:SizeOfFloat64:0", "SIZE_OF_FLOAT64"),
            ("const:SizeOfFunc:0", "SIZE_OF_FUNC"),
            ("const:SizeOfInt32:0", "SIZE_OF_INT32"),
            ("const:SizeOfInt64:0", "SIZE_OF_INT64"),
            ("const:SizeOfInt:0", "SIZE_OF_INT"),
            ("const:SizeOfInterface:0", "SIZE_OF_INTERFACE"),
            ("const:SizeOfMap:0", "SIZE_OF_MAP"),
            ("const:SizeOfPointer:0", "SIZE_OF_POINTER"),
            ("const:SizeOfSlice:0", "SIZE_OF_SLICE"),
            ("const:SizeOfString:0", "SIZE_OF_STRING"),
            ("const:SizeOfUint64:0", "SIZE_OF_UINT64"),
            ("const:SizeOfUint8:0", "SIZE_OF_UINT8"),
            ("const:SizeOfUint:0", "SIZE_OF_UINT"),
            ("const:TB:0", "TB"),
        ]);
        let mut actual = BTreeMap::new();
        for line in lines {
            let columns: Vec<_> = line.split('\t').collect();
            assert_eq!(columns.len(), 10, "invalid inventory row: {line}");
            assert_eq!(columns[1], "const");
            assert_eq!(columns[2], "pkg/util/size/size.go");
            assert_eq!(columns[6], "PORTED");
            assert_eq!(
                columns[8],
                "rust-test:source_constant_table_is_exact_for_the_target_word_size"
            );
            assert!(
                actual.insert(columns[3], columns[7]).is_none(),
                "duplicate inventory anchor: {}",
                columns[3]
            );
        }
        assert_eq!(actual, expected);

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

    fn data_rows(contents: &str) -> Vec<Vec<&str>> {
        contents
            .lines()
            .filter(|line| !line.is_empty() && !line.starts_with('#'))
            .skip(1)
            .map(|line| line.split('\t').collect())
            .collect()
    }

    fn repository_root() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../..")
    }

    fn artifact_hash(contents: &str, path: &str) -> String {
        data_rows(contents)
            .into_iter()
            .find(|row| row[0] == path)
            .map(|row| row[2].to_owned())
            .expect("artifact hash row")
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
