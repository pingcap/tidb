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

//! File-level LOCKDOWN for Go `pkg/util/ranger/ranger.go` and the Rust index
//! range modules that implement or explicitly decline its obligations.

use std::{
    collections::HashSet,
    fs,
    path::{Path, PathBuf},
};

use sha2::{Digest, Sha256};

const INVENTORY: &str = include_str!("../src/index_range.inventory.tsv");
const INDEX_RANGE_SOURCE: &str = include_str!("../src/index_range.rs");
const INDEX_PREFIX_CUT_SOURCE: &str = include_str!("../src/index_prefix_cut.rs");
const HANDLE_RANGE_SOURCE: &str = include_str!("../src/handle_range.rs");
const LOCKDOWN_SOURCE: &str = include_str!("index_range_source.rs");

const GO_RANGER_SHA256: &str = "7d9203f1e676fd5cb0bd753fff760360661d5370202b29ed590c96d7229261e0";
const GO_RANGER_TEST_SHA256: &str =
    "5be676a3ef5191a419c3a5288bcc20fd5db34637e259ac7ab7537c74e481c9ed";
const RUST_INDEX_RANGE_SHA256: &str =
    "57c86c48cf107170f64a54e5b6fd8efcd3134e1a50a80a7aec5050fae2ee3742";
const RUST_INDEX_PREFIX_CUT_SHA256: &str =
    "aaaf33aa946c3a3d9ec12973deacc03e2a72c523aab154bdfbccaac05f4b6398";
const RUST_HANDLE_RANGE_SHA256: &str =
    "a13c121c264c3a084057194228f790285bd6dbb6994e954f587b67f9f82624a0";
const INVENTORY_SHA256: &str = "26fa5557b6264f33f1187758302e8b3d0acd66525c490595a1d803da8b1bbd64";

fn repository_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../..")
        .canonicalize()
        .expect("repository root")
}

fn sha256_file(path: &Path) -> String {
    format!(
        "{:x}",
        Sha256::digest(fs::read(path).expect("read locked source"))
    )
}

fn inventory_rows() -> Vec<Vec<&'static str>> {
    INVENTORY
        .lines()
        .filter(|line| !line.is_empty() && !line.starts_with('#') && !line.starts_with("id\t"))
        .map(|line| line.split('\t').collect())
        .collect()
}

fn inventory_row(id: &str) -> Vec<&'static str> {
    inventory_rows()
        .into_iter()
        .find(|row| row[0] == id)
        .unwrap_or_else(|| panic!("missing inventory row {id}"))
}

#[test]
fn lockdown_index_range_sources_match_pinned_sha256() {
    let root = repository_root();
    for (path, expected) in [
        ("pkg/util/ranger/ranger.go", GO_RANGER_SHA256),
        ("pkg/util/ranger/ranger_test.go", GO_RANGER_TEST_SHA256),
        (
            "rust/crates/tidb-executor/src/index_range.rs",
            RUST_INDEX_RANGE_SHA256,
        ),
        (
            "rust/crates/tidb-executor/src/index_prefix_cut.rs",
            RUST_INDEX_PREFIX_CUT_SHA256,
        ),
        (
            "rust/crates/tidb-executor/src/handle_range.rs",
            RUST_HANDLE_RANGE_SHA256,
        ),
        (
            "rust/crates/tidb-executor/src/index_range.inventory.tsv",
            INVENTORY_SHA256,
        ),
    ] {
        assert_eq!(sha256_file(&root.join(path)), expected, "SHA drift: {path}");
    }
    assert!(INVENTORY.contains(&format!("# source-sha256\t{GO_RANGER_SHA256}")));
    assert!(INVENTORY.contains(&format!("# test-sha256\t{GO_RANGER_TEST_SHA256}")));
}

#[test]
fn lockdown_index_range_inventory_has_complete_shape_and_allowed_statuses() {
    let rows = inventory_rows();
    assert_eq!(rows.len(), 194);

    let mut ids = HashSet::new();
    let mut category_ordinals = std::collections::HashMap::new();
    for row in &rows {
        assert_eq!(row.len(), 6, "malformed inventory row: {row:?}");
        assert!(ids.insert(row[0]), "duplicate inventory id: {}", row[0]);
        assert!(
            matches!(row[3], "PORTED" | "DECLINED" | "UNREACHABLE"),
            "unsupported status: {row:?}"
        );
        assert!(!row[5].is_empty(), "missing evidence: {row:?}");

        let prefix = match row[1] {
            "declaration" => "D",
            "function" => "F",
            "branch" => "B",
            "loop" => "L",
            "test" => "T",
            category => panic!("unsupported category {category}: {row:?}"),
        };
        let ordinal = category_ordinals.entry(row[1]).or_insert(0_usize);
        *ordinal += 1;
        assert_eq!(row[0], format!("{prefix}{ordinal:03}"));
    }

    for (category, expected) in [
        ("declaration", 1),
        ("function", 28),
        ("branch", 125),
        ("loop", 24),
        ("test", 16),
    ] {
        assert_eq!(
            rows.iter().filter(|row| row[1] == category).count(),
            expected,
            "category count: {category}"
        );
    }
    for (status, expected) in [("PORTED", 72), ("DECLINED", 121), ("UNREACHABLE", 1)] {
        assert_eq!(
            rows.iter().filter(|row| row[3] == status).count(),
            expected,
            "status count: {status}"
        );
    }
}

fn ported_symbol_is_defined(symbol: &str) -> bool {
    let sources = [
        INDEX_RANGE_SOURCE,
        INDEX_PREFIX_CUT_SOURCE,
        HANDLE_RANGE_SOURCE,
        LOCKDOWN_SOURCE,
    ];
    let leaf = symbol.rsplit("::").next().expect("non-empty Rust symbol");
    for definition in [
        format!("fn {leaf}("),
        format!("fn {leaf}<"),
        format!("struct {leaf}"),
        format!("enum {leaf}"),
        format!("const {leaf}:"),
    ] {
        if sources.iter().any(|source| source.contains(&definition)) {
            return true;
        }
    }
    false
}

#[test]
fn lockdown_every_ported_index_range_symbol_still_exists() {
    for row in inventory_rows()
        .into_iter()
        .filter(|row| row[3] == "PORTED")
    {
        assert_ne!(row[4], "-", "PORTED row lacks a Rust symbol: {row:?}");
        assert!(
            ported_symbol_is_defined(row[4]),
            "PORTED symbol disappeared: {} ({})",
            row[4],
            row[2]
        );
        assert_eq!(row[5], "rust_source_and_source_backed_boundary_tests");
    }
}

#[test]
fn lockdown_declined_and_unreachable_index_range_obligations_have_evidence() {
    let rows = inventory_rows();
    let allowed_declined = HashSet::from([
        "go_errctx_and_best_effort_cast_error_identity_not_exposed_by_rust_range_api",
        "rust_handle_range_refuses_unsigned_handle_domain",
        "rust_range_api_has_no_go_range_max_size_byte_budget",
        "go_detacher_tail_range_cartesian_fanout_has_no_rust_runtime_consumer",
        "go_new_field_type_float_string_best_effort_conversion_is_partial_in_rust",
        "rust_owner_retains_original_where_instead_of_rebuilding_eq_or_in_conditions",
        "go_range_to_sql_renderer_has_no_rust_runtime_consumer",
        "go_test_mixes_points_go_or_detacher_go_rules_outside_ranger_go_file_owner",
        "measured_ignored_go_oracle_6_of_19_diverge_in_points_go_domain_fixups",
        "measured_ignored_go_oracle_6_of_10_diverge_in_points_go_is_not_null_rules",
    ]);
    for row in rows.iter().filter(|row| row[3] == "DECLINED") {
        assert_eq!(row[4], "-", "DECLINED row names a Rust symbol: {row:?}");
        assert!(
            allowed_declined.contains(row[5]),
            "unsupported DECLINED evidence: {row:?}"
        );
    }

    assert_eq!(
        &inventory_row("B006")[3..],
        &[
            "DECLINED",
            "-",
            "rust_handle_range_refuses_unsigned_handle_domain"
        ]
    );
    assert_eq!(
        &inventory_row("F015")[3..],
        &[
            "DECLINED",
            "-",
            "rust_handle_range_refuses_unsigned_handle_domain"
        ]
    );
    assert!(
        HANDLE_RANGE_SOURCE.contains("if column.field_type.is_unsigned() {\n        return None;")
    );

    assert_eq!(
        inventory_row("F001")[5],
        "go_errctx_and_best_effort_cast_error_identity_not_exposed_by_rust_range_api"
    );
    assert!(INDEX_RANGE_SOURCE.contains("-> Option<IndexRanges<'a>>"));
    assert_eq!(
        inventory_row("F003")[5],
        "rust_range_api_has_no_go_range_max_size_byte_budget"
    );
    assert!(!INDEX_RANGE_SOURCE.contains("rangeMaxSize"));
    assert!(!HANDLE_RANGE_SOURCE.contains("rangeMaxSize"));

    assert_eq!(
        inventory_row("F012")[5],
        "go_detacher_tail_range_cartesian_fanout_has_no_rust_runtime_consumer"
    );
    assert_eq!(
        inventory_row("F025")[5],
        "go_new_field_type_float_string_best_effort_conversion_is_partial_in_rust"
    );
    assert_eq!(
        inventory_row("F026")[5],
        "rust_owner_retains_original_where_instead_of_rebuilding_eq_or_in_conditions"
    );
    assert!(INDEX_RANGE_SOURCE.contains("The residual half is handled by construction"));
    assert_eq!(
        inventory_row("F027")[5],
        "go_range_to_sql_renderer_has_no_rust_runtime_consumer"
    );
    assert!(!INDEX_RANGE_SOURCE.contains("fn ranges_to_string"));
    assert_eq!(
        inventory_row("T001")[5],
        "go_test_mixes_points_go_or_detacher_go_rules_outside_ranger_go_file_owner"
    );

    assert_eq!(
        inventory_row("T002")[5],
        "measured_ignored_go_oracle_6_of_19_diverge_in_points_go_domain_fixups"
    );
    assert!(INDEX_RANGE_SOURCE.contains("6 of 19 rows still need Go's handleUnsignedCol"));
    assert_eq!(
        inventory_row("T012")[5],
        "measured_ignored_go_oracle_6_of_10_diverge_in_points_go_is_not_null_rules"
    );
    assert!(INDEX_RANGE_SOURCE.contains("6 of 10 rows need IS NOT NULL as an access condition"));

    let unreachable = rows
        .iter()
        .filter(|row| row[3] == "UNREACHABLE")
        .collect::<Vec<_>>();
    assert_eq!(unreachable.len(), 1);
    assert_eq!(unreachable[0][0], "B043");
    assert_eq!(unreachable[0][2], "if err != nil:310");
    assert_eq!(
        unreachable[0][5],
        "go_append_points_2_index_range_returns_nil_error_unconditionally"
    );
    let go_source = fs::read_to_string(repository_root().join("pkg/util/ranger/ranger.go"))
        .expect("read Go ranger source");
    assert!(go_source.contains("func appendPoints2IndexRange"));
    assert!(go_source.contains("return newRanges, nil"));
}
