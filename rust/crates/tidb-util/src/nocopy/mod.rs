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

//! Lockdown owner for `pkg/util/nocopy/nocopy.go`.
//!
//! `nocopy.artifacts.tsv` hashes both package artifacts and
//! `nocopy.inventory.tsv` classifies every generated Go AST obligation. The
//! source fingerprint and Rust symbol gate below make an unreviewed source or
//! inventory drift fail. The package has no Go tests, `TestMain`, benchmarks,
//! fuzz targets, examples, fixtures, generated files, or build-tag variants.
//!
//! Go's marker relies on `go vet` recognizing its `Lock` method. Rust makes
//! implicit copying impossible directly: this zero-sized marker intentionally
//! implements neither [`Copy`] nor [`Clone`]. Explicit ownership moves remain
//! valid, just as moving an owning Rust value remains valid generally.

/// Zero-sized marker that prevents an embedding Rust type from becoming
/// implicitly copyable.
///
/// ```compile_fail
/// use tidb_util::nocopy::NoCopy;
///
/// let marker = NoCopy::new();
/// let moved = marker;
/// let copied_again = marker;
/// ```
#[derive(Debug, Default)]
pub struct NoCopy;

impl NoCopy {
    /// Constructs the source zero value.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }

    /// Source-compatible no-op `sync.Locker.Lock` method.
    pub const fn lock(&self) {}

    /// Source-compatible no-op `sync.Locker.Unlock` method.
    pub const fn unlock(&self) {}
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, fmt::Write as _, fs, path::PathBuf};

    use sha2::{Digest, Sha256};

    use super::*;

    const GO_SOURCE: &[u8] = include_bytes!("../../../../../pkg/util/nocopy/nocopy.go");
    const ARTIFACTS: &str = include_str!("nocopy.artifacts.tsv");
    const LOCKDOWN_INVENTORY: &str = include_str!("nocopy.inventory.tsv");
    const BUILD_SOURCE: &[u8] = include_bytes!("../../../../../pkg/util/nocopy/BUILD.bazel");

    #[test]
    fn source_zero_value_and_no_op_methods_are_preserved() {
        let marker = NoCopy::new();
        marker.lock();
        marker.unlock();
        assert_eq!(std::mem::size_of_val(&marker), 0);
    }

    #[test]
    fn lockdown_inventory_matches_go_source_and_rust_symbols() {
        let artifact_rows = data_rows(ARTIFACTS);
        assert_eq!(artifact_rows.len(), 2);
        assert!(artifact_rows.iter().all(|row| row.len() == 3));
        let root = repository_root();
        for row in artifact_rows {
            assert_eq!(
                sha256_hex(&fs::read(root.join(row[0])).expect("read nocopy artifact")),
                row[2],
                "owned artifact drifted: {}",
                row[0]
            );
        }
        assert_eq!(
            sha256_hex(GO_SOURCE),
            artifact_hash(ARTIFACTS, "pkg/util/nocopy/nocopy.go")
        );
        assert_eq!(
            sha256_hex(BUILD_SOURCE),
            artifact_hash(ARTIFACTS, "pkg/util/nocopy/BUILD.bazel")
        );

        let mut lines = LOCKDOWN_INVENTORY
            .lines()
            .filter(|line| !line.is_empty() && !line.starts_with('#'));
        assert_eq!(
            lines.next(),
            Some("obligation_id\tcategory\tsource_path\tast_anchor\tnode_sha256\towner\tstatus\trust_symbol\tevidence\tmutation_policy")
        );

        let expected = BTreeMap::from([
            (
                "NoCopy.Lock",
                (
                    "function",
                    "NoCopy::lock",
                    "source_zero_value_and_no_op_methods_are_preserved",
                ),
            ),
            (
                "NoCopy.Unlock",
                (
                    "function",
                    "NoCopy::unlock",
                    "source_zero_value_and_no_op_methods_are_preserved",
                ),
            ),
            (
                "type:NoCopy",
                (
                    "declaration",
                    "NoCopy",
                    "source_zero_value_and_no_op_methods_are_preserved",
                ),
            ),
        ]);
        let mut actual = BTreeMap::new();
        for line in lines {
            let columns: Vec<_> = line.split('\t').collect();
            assert_eq!(columns.len(), 10, "invalid inventory row: {line}");
            assert_eq!(columns[2], "pkg/util/nocopy/nocopy.go");
            assert_eq!(columns[6], "PORTED");
            assert_eq!(
                columns[8],
                "rust-test:source_zero_value_and_no_op_methods_are_preserved"
            );
            assert!(
                actual
                    .insert(
                        columns[3],
                        (
                            columns[1],
                            columns[7],
                            columns[8].strip_prefix("rust-test:").unwrap()
                        )
                    )
                    .is_none(),
                "duplicate inventory id: {}",
                columns[3]
            );
        }
        assert_eq!(actual, expected);

        let _: fn() -> NoCopy = NoCopy::new;
        let _: fn(&NoCopy) = NoCopy::lock;
        let _: fn(&NoCopy) = NoCopy::unlock;
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

    fn sha256_hex(input: &[u8]) -> String {
        Sha256::digest(input)
            .iter()
            .fold(String::with_capacity(64), |mut output, byte| {
                write!(output, "{byte:02x}").expect("write to String");
                output
            })
    }
}
