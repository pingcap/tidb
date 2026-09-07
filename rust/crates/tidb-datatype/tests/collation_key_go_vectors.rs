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

//! Go-authoritative collation sort-key vectors.
//!
//! Every key in the fixture was produced by
//! `rust/difftests/transaction-tests/fixtures/generate_collation_key_vectors.go`
//! running against this repository's Go tree (`collate.GetCollator(name)
//! .Key(sample)` under the default new-collation configuration). The
//! compare-only suite cannot see the actual sort-key bytes TiDB writes into
//! index values; this file can.

use tidb_datatype::get_collator_by_id;

const FIXTURE: &str =
    include_str!("../../../difftests/transaction-tests/fixtures/collation_key_vectors.tsv");

/// `(collation name, collation id)` pairs, mirroring
/// `generate_collation_key_vectors.go`'s collation list. Go resolves the
/// collator by name; the Rust seam resolves by id.
const COLLATIONS: &[(&str, i32)] = &[
    ("binary", 63),
    ("utf8mb4_bin", 46),
    ("ascii_bin", 65),
    ("latin1_bin", 47),
    ("utf8mb4_general_ci", 45),
    ("utf8mb4_unicode_ci", 192),
    ("utf8mb4_0900_bin", 309),
];

/// The samples, in the fixture's order. Keep this list in sync with
/// `generate_collation_key_vectors.go`.
const SAMPLES: &[&str] = &[
    "", "a", "A", "a ", "a  ", "a\t", "abc", "ABC", "中文", "a中", "ß", "ﬀ",
];

fn fixture_key(collation: &str, index: usize) -> Vec<u8> {
    let prefix = format!("{collation}\t{index:02}\t");
    let hex = FIXTURE
        .lines()
        .find_map(|line| line.strip_prefix(&prefix))
        .unwrap_or_else(|| panic!("fixture has no {collation}/{index:02} entry"));
    hex.as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| {
            let high = (pair[0] as char).to_digit(16).expect("hex digit");
            let low = (pair[1] as char).to_digit(16).expect("hex digit");
            ((high << 4) | low) as u8
        })
        .collect()
}

#[test]
fn collation_sort_keys_match_go_byte_for_byte() {
    // Go's generator ran under the default new-collation configuration,
    // which is this suite's default too. Nothing is toggled here:
    // `set_new_collation_enabled` re-runs the defaults initialization and
    // sibling suites read the same global.
    assert!(tidb_datatype::new_collation_enabled());
    for (collation, id) in COLLATIONS {
        let collator = get_collator_by_id(*id);
        for (index, sample) in SAMPLES.iter().enumerate() {
            let expected = fixture_key(collation, index);
            assert_eq!(
                collator.key(sample.as_bytes()),
                expected,
                "{collation} sort key of {sample:?} diverges from Go"
            );
        }
    }
}
