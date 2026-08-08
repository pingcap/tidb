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

//! Drift, verdict, evidence, symbol, and mutation gates for the source-owned
//! `pkg/planner/core/find_best_task.go` lockdown.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::process::Command;

const INVENTORY: &str = include_str!("find_best_task.inventory.tsv");
const EVIDENCE: &str = include_str!("find_best_task.evidence.tsv");
const MUTATION_PLAN: &str = include_str!("find_best_task.mutation-plan.tsv");
const MUTATION_RESULTS: &str = include_str!("find_best_task.mutation-results.tsv");

fn data_rows(text: &str) -> impl Iterator<Item = Vec<&str>> {
    text.lines()
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .map(|line| line.split('\t').collect())
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(3)
        .expect("tidb-executor is rust/crates/tidb-executor")
        .to_path_buf()
}

#[test]
fn source_ast_test_and_verdict_inventory_is_closed() {
    let root = repo_root();
    let output = Command::new("python3")
        .args(["rust/scripts/find-best-task-lockdown.py", "check"])
        .current_dir(&root)
        .output()
        .expect("run deterministic Go-AST lockdown checker");
    assert!(
        output.status.success(),
        "find_best_task source/AST/test drift:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );

    let rows: Vec<_> = data_rows(INVENTORY).skip(1).collect();
    assert_eq!(rows.len(), 1_728);
    let mut ids = BTreeSet::new();
    let mut anchors = BTreeSet::new();
    let mut production = BTreeMap::<&str, usize>::new();
    let mut support = BTreeMap::<&str, usize>::new();
    for row in &rows {
        assert_eq!(row.len(), 9, "bad inventory row: {row:?}");
        assert!(ids.insert(row[0]), "duplicate obligation id: {}", row[0]);
        assert!(
            anchors.insert((row[2], row[3])),
            "duplicate source anchor: {} {}",
            row[2],
            row[3],
        );
        assert!(
            matches!(row[6], "PORTED" | "DECLINED" | "UNREACHABLE"),
            "invalid or TODO verdict: {row:?}",
        );
        assert_ne!(row[2], "pkg/executor/distsql.go");
        let counts = if row[2].ends_with("_test.go") {
            &mut support
        } else {
            &mut production
        };
        *counts.entry(row[1]).or_default() += 1;
    }
    assert_eq!(production.values().sum::<usize>(), 1_667);
    assert_eq!(support.values().sum::<usize>(), 61);
    assert_eq!(production.get("function"), Some(&68));
    assert_eq!(production.get("branch"), Some(&918));
    assert_eq!(production.get("short_circuit"), Some(&510));
    assert_eq!(support.get("test"), Some(&1));
    assert_eq!(support.get("test_assertion"), Some(&26));
    assert_eq!(support.get("test_row"), Some(&28));
}

#[test]
fn every_verdict_has_evidence_and_every_ported_symbol_compiles() {
    let evidence_rows: Vec<_> = data_rows(EVIDENCE).skip(1).collect();
    let mut evidence_ids = BTreeSet::new();
    for row in &evidence_rows {
        assert_eq!(row.len(), 5, "bad evidence row: {row:?}");
        assert!(
            evidence_ids.insert(row[0]),
            "duplicate evidence id: {}",
            row[0]
        );
        assert!(
            matches!(row[1], "PORTED" | "DECLINED" | "UNREACHABLE"),
            "invalid evidence verdict: {row:?}",
        );
        assert!(
            row[2..].iter().all(|field| !field.is_empty()),
            "empty evidence field: {row:?}"
        );
    }
    let evidence: BTreeMap<_, _> = evidence_rows.iter().map(|row| (row[0], row[1])).collect();
    let inventory: Vec<_> = data_rows(INVENTORY).skip(1).collect();
    for row in &inventory {
        assert_eq!(
            evidence.get(row[8]),
            Some(&row[6]),
            "missing or wrong-status evidence for {row:?}",
        );
    }
    let used_evidence: BTreeSet<_> = inventory.iter().map(|row| row[8]).collect();
    assert_eq!(
        used_evidence, evidence_ids,
        "unused or missing evidence row"
    );

    let ported: BTreeSet<_> = inventory
        .iter()
        .filter(|row| row[6] == "PORTED")
        .map(|row| row[7])
        .collect();
    let mut anchors = BTreeSet::new();
    anchors.extend(crate::skyline::find_best_task_compile_anchors());
    anchors.extend(crate::access_cost::find_best_task_compile_anchors());
    anchors.extend(super::access::find_best_task_compile_anchors());
    anchors.extend(super::from::find_best_task_compile_anchors());
    anchors.extend(super::leaf_access::find_best_task_compile_anchors());
    assert_eq!(ported, anchors, "PORTED/compile-anchor set drift");
}

#[test]
fn every_independent_rule_has_a_killed_and_restored_mutation() {
    let plan: Vec<_> = data_rows(MUTATION_PLAN).skip(1).collect();
    let results: Vec<_> = data_rows(MUTATION_RESULTS).skip(1).collect();
    assert_eq!(plan.len(), 8, "mutation-plan census drift");
    assert_eq!(results.len(), plan.len(), "one result per planned suite");
    let planned: BTreeSet<_> = plan.iter().map(|row| row[0]).collect();
    let mut seen = BTreeSet::new();
    for row in results {
        assert_eq!(row.len(), 8, "bad mutation result: {row:?}");
        assert!(planned.contains(row[1]), "unplanned mutation: {row:?}");
        assert!(seen.insert(row[1]), "duplicate mutation suite: {row:?}");
        assert_eq!(row[4], "KILLED", "mutation survived: {row:?}");
        assert_ne!(row[5], "0", "killed mutation exited zero: {row:?}");
        assert_eq!(row[6], "PASS", "production site not restored: {row:?}");
        assert_eq!(row[7], "PASS", "restored named test is not green: {row:?}");
    }
    assert_eq!(seen, planned);
}
