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

//! Content-addressed receipt for the complete root Go package `pkg/types`.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    mem::size_of,
    path::{Path, PathBuf},
};

use sha2::{Digest, Sha256};
use tidb_datatype::{
    BinaryJSON, BinaryLiteral, ConversionContext, Converted, CoreTime, Datum,
    DatumArithmeticError, EvalType, EXPLAIN_FORMATS, FieldName, FieldType, FieldTypeBuilder,
    FloatOverflow, HackedStr, JSONPathExpression, JSON_TYPE_CODE_OBJECT, MAX_FSP, MyDecimal,
    MysqlEnum, MysqlSet, OverflowError, Time, TimeError, TruncationPolicy, VectorFloat32,
};

const ARTIFACTS: &str = include_str!("pkg_types_lockdown/artifacts.tsv");
const OBLIGATIONS: &str = include_str!("pkg_types_lockdown/obligations.tsv");
const MUTATION_PROBES: &str = include_str!("pkg_types_lockdown/mutation-probes.tsv");
const MUTATION_RESULTS: &str = include_str!("pkg_types_lockdown/mutation-results.tsv");

fn repository_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../..")
}

fn sha256(bytes: impl AsRef<[u8]>) -> String {
    format!("{:x}", Sha256::digest(bytes.as_ref()))
}

fn data_rows(contents: &'static str) -> Vec<Vec<&'static str>> {
    contents
        .lines()
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .skip(1)
        .map(|line| line.split('\t').collect())
        .collect()
}

fn read_rust_sources(path: &Path, output: &mut String) {
    let mut entries = fs::read_dir(path)
        .unwrap_or_else(|error| panic!("read {}: {error}", path.display()))
        .map(|entry| entry.expect("read directory entry").path())
        .collect::<Vec<_>>();
    entries.sort();
    for entry in entries {
        if entry.is_dir() {
            read_rust_sources(&entry, output);
        } else if entry.extension().is_some_and(|extension| extension == "rs") {
            output.push_str(
                &fs::read_to_string(&entry)
                    .unwrap_or_else(|error| panic!("read {}: {error}", entry.display())),
            );
            output.push('\n');
        }
    }
}

#[test]
fn pkg_types_ported_owner_registry_compiles() {
    let _ = size_of::<BinaryLiteral>();
    let _ = size_of::<BinaryJSON>();
    let _ = size_of::<ConversionContext<'static>>();
    let _ = size_of::<Converted<()>>();
    let _ = size_of::<CoreTime>();
    let _ = size_of::<Datum>();
    let _ = size_of::<DatumArithmeticError>();
    let _ = size_of::<EvalType>();
    let _ = EXPLAIN_FORMATS;
    let _ = size_of::<FieldName>();
    let _ = size_of::<FieldType>();
    let _ = size_of::<FieldTypeBuilder>();
    let _ = size_of::<FloatOverflow>();
    let _ = size_of::<HackedStr<'static>>();
    let _ = size_of::<JSONPathExpression>();
    let _ = JSON_TYPE_CODE_OBJECT;
    let _ = MAX_FSP;
    let _ = size_of::<MyDecimal>();
    let _ = size_of::<MysqlEnum>();
    let _ = size_of::<MysqlSet>();
    let _ = size_of::<OverflowError>();
    let _ = size_of::<Time>();
    let _ = size_of::<TimeError>();
    let _ = size_of::<TruncationPolicy>();
    let _ = size_of::<VectorFloat32>();
}

#[test]
fn pkg_types_go_artifact_manifest_is_exact() {
    let root = repository_root();
    let rows = data_rows(ARTIFACTS);
    assert_eq!(rows.len(), 56);
    assert!(rows.iter().all(|row| row.len() == 3));

    let expected = rows
        .iter()
        .map(|row| row[0].to_owned())
        .collect::<BTreeSet<_>>();
    let actual = fs::read_dir(root.join("pkg/types"))
        .expect("read pkg/types")
        .map(|entry| entry.expect("read pkg/types entry").path())
        .filter(|path| path.is_file())
        .map(|path| {
            path.strip_prefix(&root)
                .expect("pkg/types entry under root")
                .to_string_lossy()
                .replace('\\', "/")
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(actual, expected, "root pkg/types artifact set drifted");

    let mut roles = BTreeMap::new();
    for row in rows {
        assert!(matches!(row[1], "production" | "test" | "build"));
        assert_eq!(
            sha256(fs::read(root.join(row[0])).expect("read owned Go artifact")),
            row[2],
            "owned artifact drifted: {}",
            row[0]
        );
        *roles.entry(row[1]).or_insert(0usize) += 1;
    }
    assert_eq!(roles.get("production"), Some(&29));
    assert_eq!(roles.get("test"), Some(&26));
    assert_eq!(roles.get("build"), Some(&1));

    for class in [
        "build_tags",
        "platform_variants",
        "code_generated",
        "go_generate",
        "go_embed",
        "tracked_testdata",
    ] {
        assert!(
            ARTIFACTS.contains(&format!("# zero\t{class}\t0")),
            "missing zero-count ratchet for {class}"
        );
    }
}

#[test]
fn pkg_types_every_obligation_is_classified_once() {
    let rows = data_rows(OBLIGATIONS);
    assert_eq!(rows.len(), 20_140);
    assert!(rows.iter().all(|row| row.len() == 10));

    let allowed_symbols = [
        "BinaryJSON",
        "BinaryLiteral",
        "ConversionContext",
        "Converted",
        "CoreTime",
        "Datum",
        "DatumArithmeticError",
        "EvalType",
        "EXPLAIN_FORMATS",
        "FieldName",
        "FieldType",
        "FieldTypeBuilder",
        "FloatOverflow",
        "HackedStr",
        "JSONPathExpression",
        "JSON_TYPE_CODE_OBJECT",
        "MAX_FSP",
        "MyDecimal",
        "MysqlEnum",
        "MysqlSet",
        "OverflowError",
        "Time",
        "TimeError",
        "TruncationPolicy",
        "VectorFloat32",
    ]
    .into_iter()
    .collect::<BTreeSet<_>>();
    let mut ids = BTreeSet::new();
    let mut categories = BTreeMap::new();
    let mut statuses = BTreeMap::new();
    for row in &rows {
        assert!(ids.insert(row[0]), "duplicate obligation id: {}", row[0]);
        assert!(matches!(row[6], "PORTED" | "DECLINED" | "UNREACHABLE"));
        assert!(!row[8].is_empty(), "missing evidence: {row:?}");
        assert!(!row[9].is_empty(), "missing mutation policy: {row:?}");
        if row[6] == "PORTED" {
            assert!(
                allowed_symbols.contains(row[7]),
                "PORTED row has no compiled owner: {row:?}"
            );
        } else {
            assert_eq!(row[7], "-", "non-PORTED row claims a Rust symbol");
        }
        *categories.entry(row[1]).or_insert(0usize) += 1;
        *statuses.entry(row[6]).or_insert(0usize) += 1;
    }
    assert_eq!(statuses.get("PORTED"), Some(&19_935));
    assert_eq!(statuses.get("DECLINED"), Some(&205));
    assert_eq!(statuses.get("UNREACHABLE"), None);

    let expected_categories = [
        ("benchmark", 23),
        ("branch", 2_830),
        ("closure", 16),
        ("const", 204),
        ("declaration", 49),
        ("field", 71),
        ("function", 761),
        ("fuzz", 1),
        ("loop", 476),
        ("short_circuit", 1_108),
        ("switch_case", 941),
        ("test", 205),
        ("test_assertion", 1_127),
        ("test_branch", 234),
        ("test_helper", 39),
        ("test_helper_closure", 64),
        ("test_loop", 544),
        ("test_main", 1),
        ("test_row", 11_271),
        ("test_short_circuit", 8),
        ("test_support_const", 2),
        ("test_support_declaration", 2),
        ("test_support_var", 3),
        ("test_switch_case", 39),
        ("var", 121),
    ]
    .into_iter()
    .collect::<BTreeMap<_, _>>();
    assert_eq!(categories, expected_categories);
}

#[test]
fn pkg_types_ported_test_evidence_still_exists() {
    let crate_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut rust_sources = String::new();
    for directory in ["src", "tests", "benches", "fuzz"] {
        read_rust_sources(&crate_root.join(directory), &mut rust_sources);
    }
    let evidence = data_rows(OBLIGATIONS)
        .into_iter()
        .filter_map(|row| row[8].strip_prefix("rust-test:"))
        .map(|rest| rest.split(':').next().expect("Rust test evidence name"))
        .collect::<BTreeSet<_>>();
    assert_eq!(evidence.len(), 150);
    for name in evidence {
        assert!(
            rust_sources.contains(&format!("fn {name}(")),
            "Rust test evidence disappeared: {name}"
        );
    }
    assert!(crate_root.join("fuzz/json_extract.rs").is_file());
    assert!(crate_root.join("benches").is_dir());
}

#[test]
fn pkg_types_mutation_plan_and_killed_results_are_current() {
    let planned = data_rows(MUTATION_PROBES);
    let results = data_rows(MUTATION_RESULTS);
    assert!(planned.iter().all(|row| row.len() == 6));
    assert!(results.iter().all(|row| row.len() == 6));

    let plan_ids = planned.iter().map(|row| row[0]).collect::<BTreeSet<_>>();
    let result_ids = results.iter().map(|row| row[0]).collect::<BTreeSet<_>>();
    assert_eq!(plan_ids, result_ids, "mutation plan/result set drifted");
    let required = data_rows(OBLIGATIONS)
        .into_iter()
        .filter_map(|row| row[9].strip_prefix("probe:"))
        .collect::<BTreeSet<_>>();
    assert!(
        required.is_subset(&plan_ids),
        "PORTED rule lacks a mutation suite: required={required:?} plan={plan_ids:?}"
    );
    assert_eq!(plan_ids.len(), 7, "unexpected mutation suite count");

    let root = repository_root();
    for row in results {
        assert_eq!(row[1], "KILLED", "mutation survived: {row:?}");
        assert!(!row[2].is_empty(), "missing baseline commit: {row:?}");
        assert!(!row[3].is_empty(), "missing named failing test: {row:?}");
        assert_eq!(
            sha256(fs::read(root.join(row[4])).expect("read mutation-owned Rust source")),
            row[5],
            "mutation-owned Rust source drifted: {}",
            row[4]
        );
    }
}
