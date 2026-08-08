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

//! Independent receipt gate for the `pkg/executor/distsql.go` file-lockdown seed.

use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};

use sha2::{Digest, Sha256};

const ARTIFACTS: &str = include_str!("distsql_lockdown/artifacts.tsv");
const INVENTORY: &str = include_str!("distsql_lockdown/inventory.tsv");
const MUTATION_PLAN: &str = include_str!("distsql_lockdown/mutation-plan.tsv");
const MUTATION_RESULTS: &str = include_str!("distsql_lockdown/mutation-results.tsv");
const RECEIPT: &str = include_str!("distsql_lockdown/receipt.json");

fn repository_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../..")
}

fn data_rows(contents: &str) -> Vec<Vec<&str>> {
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

fn assert_source_evidence(row: &[&str], source_index: usize, hash_index: usize, kind: &str) {
    let sources: Vec<_> = row[source_index].split('|').collect();
    let hashes: Vec<_> = row[hash_index].split('|').collect();
    assert_eq!(
        sources.len(),
        hashes.len(),
        "{kind} source/hash width drift: {row:?}"
    );
    let root = repository_root();
    for (source, expected_hash) in sources.into_iter().zip(hashes) {
        assert!(!source.is_empty(), "{kind} has an empty source path");
        assert!(!expected_hash.is_empty(), "{kind} has an empty source hash");
        let path = root.join(source);
        assert!(path.is_file(), "{kind} source is not a file: {source}");
        let bytes =
            fs::read(&path).unwrap_or_else(|error| panic!("read {kind} source {source}: {error}"));
        assert_eq!(
            sha256(bytes),
            expected_hash,
            "{kind} source drifted: {source}"
        );
    }
}

#[test]
fn distsql_lockdown_source_quotes_and_verdicts_are_exact() {
    let artifacts = data_rows(ARTIFACTS);
    assert_eq!(artifacts.len(), 6);
    assert!(artifacts.iter().all(|row| row.len() == 3));
    for row in artifacts {
        let path = repository_root().join(row[0]);
        assert!(
            path.is_file(),
            "owned Go artifact is not a file: {}",
            row[0]
        );
        assert_eq!(
            sha256(fs::read(path).expect("read owned Go artifact")),
            row[2],
            "owned Go artifact drifted: {}",
            row[0]
        );
    }

    let inventory = data_rows(INVENTORY);
    assert_eq!(inventory.len(), 1_461);
    assert!(inventory.iter().all(|row| row.len() == 11));

    let mut sources = BTreeMap::new();
    let mut categories = BTreeMap::new();
    let mut statuses = BTreeMap::new();
    for row in inventory {
        *sources.entry(row[2]).or_insert(0usize) += 1;
        *categories.entry(row[1]).or_insert(0usize) += 1;
        *statuses.entry(row[6]).or_insert(0usize) += 1;

        match row[6] {
            "PORTED" => panic!(
                "PORTED obligation has no compiled Rust symbol allowlist entry: {}",
                row[0]
            ),
            "DECLINED" | "UNREACHABLE" => {}
            verdict => panic!("invalid or blank verdict {verdict:?}: {}", row[0]),
        }
        assert_eq!(row[7], "-", "declined seed row claims a Rust symbol");
        let evidence = format!("go-ast-quote:{}#{}@sha256:{}", row[2], row[3], row[4]);
        assert_eq!(row[8], evidence);
        assert!(!row[9].is_empty(), "missing verdict reason: {}", row[0]);
        assert!(
            row[10].ends_with("-required-before-PORTED"),
            "missing promotion mutation policy: {}",
            row[0]
        );
    }

    assert_eq!(
        sources,
        BTreeMap::from([
            ("pkg/executor/distsql.go", 904usize),
            ("pkg/executor/distsql_test.go", 231usize),
            ("pkg/executor/table_readers_required_rows_test.go", 192usize),
            ("pkg/executor/test/issuetest/executor_issue_test.go", 8usize,),
            ("pkg/executor/internal/exec/executor_test.go", 116usize),
            ("pkg/executor/test/seqtest/seq_executor_test.go", 10usize),
        ])
    );
    assert_eq!(statuses, BTreeMap::from([("DECLINED", 1_461usize)]));
    assert_eq!(
        categories,
        BTreeMap::from([
            ("branch", 464usize),
            ("closure", 21usize),
            ("const", 2usize),
            ("declaration", 16usize),
            ("field", 153usize),
            ("function", 67usize),
            ("loop", 94usize),
            ("select_case", 11usize),
            ("short_circuit", 74usize),
            ("switch_case", 3usize),
            ("test", 18usize),
            ("test_assertion", 102usize),
            ("test_branch", 34usize),
            ("test_helper", 14usize),
            ("test_helper_closure", 20usize),
            ("test_loop", 64usize),
            ("test_row", 285usize),
            ("test_short_circuit", 4usize),
            ("test_support_declaration", 3usize),
            ("test_support_var", 2usize),
            ("test_switch_case", 6usize),
            ("var", 4usize),
        ])
    );

    assert!(RECEIPT.contains("\"claim_boundary\": \"file-lockdown-seed-not-package-completion\""));
    assert!(RECEIPT.contains("\"obligation_count\": 1461"));
    assert!(RECEIPT.contains("\"ported_symbol_count\": 0"));
    assert!(RECEIPT.contains("\"reachable_ported_rule_count\": 0"));
    assert!(RECEIPT.contains("\"whole_go_package_complete\": false"));
}

#[test]
fn distsql_lockdown_mutation_receipt_is_current_and_killed() {
    let plan = data_rows(MUTATION_PLAN);
    let results = data_rows(MUTATION_RESULTS);
    assert_eq!(plan.len(), 8);
    assert_eq!(results.len(), 9);
    assert!(plan.iter().all(|row| row.len() == 8));
    assert!(results.iter().all(|row| row.len() == 9));

    let expected_counts = plan
        .iter()
        .map(|row| (row[0], row[2].parse::<usize>().expect("mutation count")))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(expected_counts.values().sum::<usize>(), 9);
    let expected_baselines = plan
        .iter()
        .map(|row| (row[0], row[3]))
        .collect::<BTreeMap<_, _>>();
    for row in &plan {
        assert_source_evidence(row, 4, 5, "mutation plan");
    }

    let mut actual_counts = expected_counts
        .keys()
        .map(|suite| (*suite, 0usize))
        .collect::<BTreeMap<_, _>>();
    let mut mutation_ids = std::collections::BTreeSet::new();
    for row in results {
        assert!(
            mutation_ids.insert(row[0]),
            "duplicate mutation id: {}",
            row[0]
        );
        assert_eq!(row[2], "KILLED", "mutation survived: {row:?}");
        assert_eq!(
            row[3], expected_baselines[row[1]],
            "mutation baseline drift: {row:?}"
        );
        assert!(!row[4].is_empty());
        assert_ne!(row[5], "0");
        assert_eq!(row[8], "PASS", "mutation was not restored: {row:?}");
        assert_source_evidence(&row, 6, 7, "mutation result");
        *actual_counts.entry(row[1]).or_insert(0usize) += 1;
    }
    assert_eq!(actual_counts, expected_counts);
}

#[test]
fn distsql_lockdown_has_no_reachable_claim_without_a_compiled_owner() {
    let inventory = data_rows(INVENTORY);
    assert_eq!(
        inventory.iter().filter(|row| row[6] == "PORTED").count(),
        0,
        "a reachable rule must name a real compiled Rust owner and mutation probe"
    );
    assert!(RECEIPT.contains("\"reachable_ported_rule_count\": 0"));
}

#[test]
fn distsql_lockdown_paths_are_repository_relative() {
    for row in data_rows(ARTIFACTS) {
        assert!(!Path::new(row[0]).is_absolute());
        assert!(!row[0].contains(".."));
    }
}
