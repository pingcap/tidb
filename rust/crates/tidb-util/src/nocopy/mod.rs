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
//! `nocopy.inventory.tsv` classifies every declaration, function, and rule in
//! that Go file. The source fingerprint and Rust symbol gate below make an
//! unreviewed source or inventory drift fail. The package has no Go tests,
//! `TestMain`, benchmarks, fuzz targets, examples, fixtures, generated files,
//! or build-tag variants.
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
    use std::{
        collections::{BTreeMap, BTreeSet},
        fmt::Write as _,
    };

    use sha2::{Digest, Sha256};

    use super::*;

    const GO_SOURCE: &[u8] = include_bytes!("../../../../../pkg/util/nocopy/nocopy.go");
    const LOCKDOWN_INVENTORY: &str = include_str!("nocopy.inventory.tsv");
    const EXPECTED_ITEMS: [(&str, (&str, &str)); 7] = [
        ("D01", ("PORTED", "NoCopy")),
        ("R01", ("PORTED", "NoCopy")),
        ("R02", ("PORTED", "NoCopy")),
        ("F01", ("PORTED", "NoCopy::lock")),
        ("R03", ("PORTED", "NoCopy::lock")),
        ("F02", ("PORTED", "NoCopy::unlock")),
        ("R04", ("PORTED", "NoCopy::unlock")),
    ];

    #[test]
    fn source_zero_value_and_no_op_methods_are_preserved() {
        let marker = NoCopy::new();
        marker.lock();
        marker.unlock();
        assert_eq!(std::mem::size_of_val(&marker), 0);
    }

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

        let _: fn() -> NoCopy = NoCopy::new;
        let _: fn(&NoCopy) = NoCopy::lock;
        let _: fn(&NoCopy) = NoCopy::unlock;
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
