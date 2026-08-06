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

//! Source-backed LOCKDOWN tests for Go `pkg/sessionctx/vardef/runtime.go`.

use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, fs, path::PathBuf};
use tidb_exec::nextgen_readonly_vars::{is_read_only_var_in_nextgen, NEXTGEN_READ_ONLY_VARIABLES};

const GO_SOURCE_SHA256: &str = "77d37b0f27ee7eef86e81368ceff99fdf9d8297c0672946faa7813deeb63d95a";
const GO_TEST_SHA256: &str = "a9530339c5d9bbb741e978ca8dc029868832f36aa9dedb68de9a69f774141f20";
const INVENTORY_SHA256: &str = "d2ff9cbce1608c6c5cef838ecf0992fb8dded7cf6e5d551138e2f9768d1b8401";
const RUST_MODULE_SHA256: &str = "32577b5cb140cca7a3d475a11b3add537b32f5a3f7daba64720638f968f9dd7f";
const INVENTORY: &str = include_str!("../src/nextgen_readonly_vars.inventory.tsv");

const EXPECTED_IDS: [&str; 37] = [
    "D01", "D02", "D03", "R01", "R02", "R03", "F01", "R04", "F02", "R05", "F03", "R06", "F04",
    "R07", "F05", "R08", "F06", "R09", "F07", "R10", "B01", "B02", "B03", "B04", "B05", "B06",
    "B07", "T01", "TB01", "TC01", "TC02", "TC03", "TC04", "TC05", "TC06", "TC07", "TC08",
];

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../..")
}

fn sha256(bytes: impl AsRef<[u8]>) -> String {
    format!("{:x}", Sha256::digest(bytes.as_ref()))
}

#[test]
fn lockdown_inventory_matches_go_source_test_and_rust_symbols() {
    let root = repo_root();
    assert_eq!(
        sha256(fs::read(root.join("pkg/sessionctx/vardef/runtime.go")).unwrap()),
        GO_SOURCE_SHA256,
        "owning Go source drifted"
    );
    assert_eq!(
        sha256(fs::read(root.join("pkg/sessionctx/vardef/runtime_test.go")).unwrap()),
        GO_TEST_SHA256,
        "owning Go test drifted"
    );
    assert_eq!(
        sha256(INVENTORY),
        INVENTORY_SHA256,
        "runtime.go inventory drifted"
    );
    assert_eq!(
        sha256(
            fs::read(
                PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/nextgen_readonly_vars.rs")
            )
            .unwrap()
        ),
        RUST_MODULE_SHA256,
        "owned Rust module drifted"
    );

    let rows: Vec<Vec<&str>> = INVENTORY
        .lines()
        .filter(|line| !line.is_empty() && !line.starts_with('#') && !line.starts_with("id\t"))
        .map(|line| line.split('\t').collect())
        .collect();
    assert!(rows.iter().all(|row| row.len() == 6));
    assert_eq!(
        rows.iter().map(|row| row[0]).collect::<Vec<_>>(),
        EXPECTED_IDS
    );

    let allowed_statuses = ["PORTED", "DECLINED", "UNREACHABLE"];
    let mut statuses = BTreeMap::new();
    for row in &rows {
        assert!(
            allowed_statuses.contains(&row[3]),
            "invalid status: {row:?}"
        );
        assert!(!row[5].is_empty(), "missing evidence: {row:?}");
        *statuses.entry(row[3]).or_insert(0usize) += 1;
    }
    assert_eq!(statuses.get("PORTED"), Some(&18));
    assert_eq!(statuses.get("DECLINED"), Some(&19));
    assert_eq!(statuses.get("UNREACHABLE"), None);

    let _: fn(&str) -> bool = is_read_only_var_in_nextgen;
    let _: fn() = nextgen_readonly_variable_predicate_matches_source;
    assert_eq!(NEXTGEN_READ_ONLY_VARIABLES.len(), 6);
    for row in rows.iter().filter(|row| row[3] == "PORTED") {
        assert!(
            matches!(
                row[4],
                "is_read_only_var_in_nextgen"
                    | "nextgen_readonly_variable_predicate_matches_source"
            ),
            "PORTED row has no gated Rust symbol: {row:?}"
        );
    }
    for row in rows.iter().filter(|row| row[3] == "DECLINED") {
        assert_eq!(row[4], "-", "DECLINED row claims a Rust symbol: {row:?}");
    }
}

#[test]
fn nextgen_readonly_variable_predicate_matches_source() {
    // Source: pkg/sessionctx/vardef/runtime.go:69-78 and
    // pkg/sessionctx/vardef/runtime_test.go:24-36.
    for name in [
        "tidb_enable_metadata_lock",
        "TIDB_ENABLE_METADATA_LOCK",
        "TiDb_DdL_DiSk_QuOtA",
        "tidb_enable_metadata_loc\u{212a}",
        "tidb_max_dist_task_nodes",
        "tidb_ddl_reorg_max_write_speed",
        "tidb_ddl_disk_quota",
        "tidb_ddl_enable_fast_reorg",
        "tidb_enable_dist_task",
    ] {
        assert!(is_read_only_var_in_nextgen(name), "{name}");
    }
    for name in [
        "",
        "abc",
        "tidb_enable_metadata_lock_suffix",
        " tidb_enable_metadata_lock",
        "TIDB_ENABLE_METADATA_LOC\u{130}",
    ] {
        assert!(!is_read_only_var_in_nextgen(name), "{name}");
    }
}

#[test]
fn unicode_lowering_happens_before_exact_membership() {
    assert!(is_read_only_var_in_nextgen(
        "tidb_enable_metadata_loc\u{212a}"
    ));
    assert!(!is_read_only_var_in_nextgen(
        "tidb_enable_metadata_loc\u{130}"
    ));
    assert!(!is_read_only_var_in_nextgen(
        "tidb_enable_metadata_lock_suffix"
    ));
}

#[test]
fn declined_lease_runtime_seams_are_explicit() {
    let root = repo_root();
    let node_config =
        fs::read_to_string(root.join("rust/crates/tidb-server/src/node_config.rs")).unwrap();
    assert!(node_config.contains("pub schema_lease: Duration"));
    assert!(node_config.contains("let schema_lease = Duration::from_millis"));
    assert!(node_config.contains("None => DEFAULT_SCHEMA_LEASE_MS"));

    let server_boot =
        fs::read_to_string(root.join("rust/crates/tidb-server/src/cluster_session_node/boot.rs"))
            .unwrap();
    assert!(server_boot.contains("spawn_catalog_reloader"));
    assert!(server_boot.contains("config.schema_lease"));

    let declined_items: Vec<&str> = INVENTORY
        .lines()
        .filter(|line| !line.is_empty() && !line.starts_with('#') && !line.starts_with("id\t"))
        .map(|line| line.split('\t').collect::<Vec<_>>())
        .filter(|row| row.get(3) == Some(&"DECLINED"))
        .map(|row| row[2])
        .collect();
    for declined in [
        "SetSchemaLease(lease time.Duration)",
        "GetSchemaLease() time.Duration",
        "SetStatsLease(lease time.Duration)",
        "GetStatsLease() time.Duration",
        "SetPlanReplayerGCLease(lease time.Duration)",
        "GetPlanReplayerGCLease() time.Duration",
    ] {
        assert!(
            declined_items.contains(&declined),
            "missing decline for {declined}"
        );
    }
}
