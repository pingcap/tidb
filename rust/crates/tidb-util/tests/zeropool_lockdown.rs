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

//! Mutation receipt gate for the complete Go `pkg/util/zeropool` lockdown.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::PathBuf,
};

use sha2::{Digest, Sha256};

const PLAN: &str = include_str!("../src/zeropool/zeropool.mutation-plan.tsv");
const RESULTS: &str = include_str!("../src/zeropool/zeropool.mutation-results.tsv");
const BASELINE: &str = "0e633eeff8a9afd0f81a735a66bf2b74a331a8a7";

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
fn zeropool_mutation_receipt_is_current_and_killed() {
    let plan = data_rows(PLAN);
    let results = data_rows(RESULTS);
    assert_eq!(plan.len(), 7);
    assert_eq!(results.len(), 27);
    assert!(plan.iter().all(|row| row.len() == 6));
    assert!(results.iter().all(|row| row.len() == 9));

    let expected_counts = plan
        .iter()
        .map(|row| (row[0], row[2].parse::<usize>().expect("mutation count")))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(expected_counts.values().sum::<usize>(), 27);

    let mut ids = BTreeSet::new();
    let mut actual_counts = BTreeMap::new();
    let mut baselines = BTreeSet::new();
    let root = repository_root();
    for row in results {
        assert!(ids.insert(row[0]), "duplicate mutation id: {}", row[0]);
        assert_eq!(row[2], "KILLED", "mutation survived: {row:?}");
        assert_eq!(row[3], BASELINE);
        baselines.insert(row[3]);
        assert!(!row[4].is_empty());
        assert_ne!(row[5], "0");
        assert_eq!(row[8], "PASS", "mutation source was not restored: {row:?}");

        let sources: Vec<_> = row[6].split('|').collect();
        let hashes: Vec<_> = row[7].split('|').collect();
        assert_eq!(sources.len(), hashes.len());
        for (source, expected_hash) in sources.into_iter().zip(hashes) {
            assert_eq!(
                sha256(fs::read(root.join(source)).expect("read mutation source")),
                expected_hash,
                "mutation source drifted: {source}"
            );
        }
        *actual_counts.entry(row[1]).or_insert(0usize) += 1;
    }
    assert_eq!(actual_counts, expected_counts);
    assert_eq!(baselines, BTreeSet::from([BASELINE]));
}
