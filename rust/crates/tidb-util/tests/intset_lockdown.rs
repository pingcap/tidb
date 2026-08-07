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

//! Mutation receipt gate for the complete Go `pkg/util/intset` lockdown.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::PathBuf,
};

use sha2::{Digest, Sha256};

const PLAN: &str = include_str!("../src/intset.mutation-plan.tsv");
const RESULTS: &str = include_str!("../src/intset.mutation-results.tsv");

fn repository_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../..")
}

fn data_rows(contents: &'static str) -> Vec<Vec<&'static str>> {
    contents
        .lines()
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .skip(1)
        .map(|line| line.split('\t').collect())
        .collect()
}

fn sha256(bytes: impl AsRef<[u8]>) -> String {
    format!("{:x}", Sha256::digest(bytes.as_ref()))
}

#[test]
fn intset_mutation_receipt_is_current_and_killed() {
    let plan = data_rows(PLAN);
    let results = data_rows(RESULTS);
    assert_eq!(plan.len(), 7);
    assert_eq!(results.len(), 18);
    assert!(plan.iter().all(|row| row.len() == 6));
    assert!(results.iter().all(|row| row.len() == 9));

    let expected_counts = plan
        .iter()
        .map(|row| (row[0], row[2].parse::<usize>().expect("mutation count")))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(expected_counts.values().sum::<usize>(), 18);

    let mut ids = BTreeSet::new();
    let mut actual_counts = BTreeMap::new();
    let root = repository_root();
    for row in results {
        assert!(ids.insert(row[0]), "duplicate mutation id: {}", row[0]);
        assert_eq!(row[2], "KILLED", "mutation survived: {row:?}");
        assert_eq!(row[3], "8e67f298381c390b12d8642761351cfd6f1a72f6");
        assert!(!row[4].is_empty());
        assert_ne!(row[5], "0");
        assert_eq!(row[8], "PASS", "mutation source was not restored: {row:?}");
        assert_eq!(
            sha256(fs::read(root.join(row[6])).expect("read mutation source")),
            row[7],
            "mutation source drifted: {}",
            row[6]
        );
        *actual_counts.entry(row[1]).or_insert(0usize) += 1;
    }
    assert_eq!(actual_counts, expected_counts);
}
