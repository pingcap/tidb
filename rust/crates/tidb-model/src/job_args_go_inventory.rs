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

//! Lockdown gates for Go `pkg/meta/model/job_args.go` and its direct test
//! owner `job_args_test.go`.

use std::collections::{BTreeMap, BTreeSet};

use sha2::{Digest, Sha256};

use crate::{
    index_arg_columnar_index_type, rename_tables_args_from_v1, ColumnarIndexType, IndexOp,
    RenameTableArgs,
};
use tidb_ast::CiString;

type RenameFromV1Fn =
    fn(&[i64], &[CiString], &[CiString], &[i64], &[CiString], &[i64]) -> Vec<RenameTableArgs>;

const GO_OWNER: &[u8] = include_bytes!("../../../../pkg/meta/model/job_args.go");
const GO_TEST_SUPPORT: &[u8] = include_bytes!("../../../../pkg/meta/model/job_args_test.go");
const INVENTORY: &str = include_str!("job_args_go_inventory.tsv");

const GO_OWNER_SHA256: &str = "da1b59226cb2d3a05bbb65c945709088a33fac605237b4e195377c10ebec27bf";
const GO_OWNER_BYTES: usize = 65_331;
const GO_OWNER_LINES: usize = 1_917;
const GO_TEST_SHA256: &str = "7647a990a36ff19f4bc1e56ed915466c6be7c87c32bd0c68a443e24378edb382";
const GO_TEST_BYTES: usize = 39_148;
const GO_TEST_LINES: usize = 1_242;
const ORDERED_AST_IDENTITY_SHA256: &str =
    "8fd015cea480707605e59092382e84518b084d03b5d85a9b13bf965cc2604d8c";

fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

#[test]
fn pinned_go_owner_and_direct_test_support_have_not_drifted() {
    assert_eq!(GO_OWNER.len(), GO_OWNER_BYTES);
    assert_eq!(
        GO_OWNER.iter().filter(|byte| **byte == b'\n').count(),
        GO_OWNER_LINES
    );
    assert_eq!(sha256(GO_OWNER), GO_OWNER_SHA256);

    assert_eq!(GO_TEST_SUPPORT.len(), GO_TEST_BYTES);
    assert_eq!(
        GO_TEST_SUPPORT
            .iter()
            .filter(|byte| **byte == b'\n')
            .count(),
        GO_TEST_LINES
    );
    assert_eq!(sha256(GO_TEST_SUPPORT), GO_TEST_SHA256);
}

#[test]
fn every_ast_obligation_has_exactly_one_concrete_verdict() {
    let expected_categories = BTreeMap::from([
        ("branch", 186_usize),
        ("const", 3),
        ("declaration", 55),
        ("field", 177),
        ("function", 161),
        ("loop", 24),
        ("short_circuit", 18),
        ("switch_case", 13),
        ("test", 44),
        ("test_assertion", 240),
        ("test_branch", 32),
        ("test_helper", 2),
        ("test_helper_closure", 4),
        ("test_loop", 144),
        ("test_row", 509),
    ]);
    let mut categories = BTreeMap::<&str, usize>::new();
    let mut verdicts = BTreeMap::<&str, usize>::new();
    let mut ids = BTreeSet::new();
    let mut ordered_identity = Vec::new();

    for (line_index, line) in INVENTORY.lines().enumerate() {
        if line.starts_with('#') || line.is_empty() {
            continue;
        }
        let fields: Vec<_> = line.split('\t').collect();
        assert_eq!(fields.len(), 9, "inventory line {}", line_index + 1);
        let [id, category, source, anchor, node_hash, owner, verdict, symbol, evidence] =
            <[&str; 9]>::try_from(fields).expect("nine inventory fields");
        assert!(ids.insert(id), "duplicate obligation {id}");
        assert!(matches!(verdict, "PORTED" | "DECLINED" | "UNREACHABLE"));
        assert!(!evidence.is_empty());
        assert!(!evidence.contains("TODO"));
        match verdict {
            "PORTED" => assert_ne!(symbol, "-", "PORTED obligation {id} has no symbol"),
            "DECLINED" => {
                assert_eq!(symbol, "-");
                assert!(evidence.starts_with("Measured boundary:"));
            }
            "UNREACHABLE" => {
                assert_eq!(symbol, "-");
                assert!(evidence.starts_with("Structural proof:"));
            }
            _ => unreachable!(),
        }
        *categories.entry(category).or_default() += 1;
        *verdicts.entry(verdict).or_default() += 1;
        ordered_identity.extend_from_slice(
            format!("{id}\t{category}\t{source}\t{anchor}\t{node_hash}\t{owner}\n").as_bytes(),
        );
    }

    assert_eq!(ids.len(), 1_612);
    assert_eq!(categories, expected_categories);
    assert_eq!(verdicts.get("PORTED"), Some(&24));
    assert_eq!(verdicts.get("DECLINED"), Some(&1_588));
    assert_eq!(verdicts.get("UNREACHABLE"), None);
    assert_eq!(sha256(&ordered_identity), ORDERED_AST_IDENTITY_SHA256);
}

#[test]
fn every_ported_symbol_remains_compile_anchored() {
    let _ = IndexOp::ADD_INDEX;
    let _ = IndexOp::DROP_INDEX;
    let _ = IndexOp::ROLLBACK_ADD_INDEX;
    let rename = RenameTableArgs::default();
    let _ = (
        rename.old_schema_id,
        rename.old_schema_name,
        rename.new_table_name,
        rename.old_table_name,
        rename.new_schema_id,
        rename.table_id,
        rename.old_schema_id_for_schema_diff,
    );
    let _: fn(ColumnarIndexType, bool) -> ColumnarIndexType = index_arg_columnar_index_type;
    let _: RenameFromV1Fn = rename_tables_args_from_v1;

    let symbols: BTreeSet<_> = INVENTORY
        .lines()
        .filter(|line| !line.starts_with('#') && !line.is_empty())
        .filter_map(|line| {
            let fields: Vec<_> = line.split('\t').collect();
            (fields[6] == "PORTED").then_some(fields[7])
        })
        .collect();
    assert_eq!(
        symbols,
        BTreeSet::from([
            "job_args::IndexOp",
            "job_args::RenameTableArgs",
            "job_args::index_arg_columnar_index_type",
            "job_args::rename_tables_args_from_v1",
        ])
    );
}

#[test]
fn deferred_rows_pin_the_measured_job_boundary() {
    let rust_job = include_str!("job.rs");
    for phrase in [
        "DEFERRED (a larger tranche): the `Job` struct itself",
        "version-dependent JSON args (`RawArgs`/`Encode`/`Decode`/`FillArgs`)",
    ] {
        assert!(
            rust_job.contains(phrase),
            "deferred boundary drifted: {phrase}"
        );
    }
    let go_tests = std::str::from_utf8(GO_TEST_SUPPORT).expect("Go test source is UTF-8");
    assert!(go_tests.contains("func getJobBytes("));
    assert!(go_tests.contains("func getFinishedJobBytes("));
    assert!(go_tests.contains("j.FillArgs(inArgs)"));
    assert!(go_tests.contains("j.FillFinishedArgs(inArgs)"));
    assert_eq!(go_tests.matches("func Test").count(), 44);
}
