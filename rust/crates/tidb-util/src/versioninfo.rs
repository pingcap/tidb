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

//! Lockdown owner for the complete Go `pkg/util/versioninfo` package.
//!
//! `versioninfo.artifacts.tsv` hashes both direct package artifacts and
//! `versioninfo.inventory.tsv` classifies every generated Go AST obligation.
//! Go injects five version variables with `-ldflags -X` and may reassign them
//! at runtime. Rust's `option_env!` values are immutable compile-time
//! approximations, so only `CommunityEdition` is classified as fully ported.

/// The default edition for building (Go `CommunityEdition`).
pub const COMMUNITY_EDITION: &str = "Community";

/// Immutable compile-time approximation of Go `TiDBBuildTS`.
pub const TIDB_BUILD_TS: &str = match option_env!("TIDB_BUILD_TS") {
    Some(v) => v,
    None => "None",
};
/// Immutable compile-time approximation of Go `TiDBGitHash`.
pub const TIDB_GIT_HASH: &str = match option_env!("TIDB_GIT_HASH") {
    Some(v) => v,
    None => "None",
};
/// Immutable compile-time approximation of Go `TiDBGitBranch`.
pub const TIDB_GIT_BRANCH: &str = match option_env!("TIDB_GIT_BRANCH") {
    Some(v) => v,
    None => "None",
};
/// Immutable compile-time approximation of Go `TiDBEdition`.
pub const TIDB_EDITION: &str = match option_env!("TIDB_EDITION") {
    Some(v) => v,
    None => COMMUNITY_EDITION,
};
/// Immutable compile-time approximation of Go `TiDBEnterpriseExtensionGitHash`.
pub const TIDB_ENTERPRISE_EXTENSION_GIT_HASH: &str =
    match option_env!("TIDB_ENTERPRISE_EXTENSION_GIT_HASH") {
        Some(v) => v,
        None => "",
    };

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};
    use std::{collections::BTreeMap, fs, path::PathBuf};

    const GO_SOURCE: &[u8] = include_bytes!("../../../../pkg/util/versioninfo/versioninfo.go");
    const BUILD_SOURCE: &[u8] = include_bytes!("../../../../pkg/util/versioninfo/BUILD.bazel");
    const ARTIFACTS: &str = include_str!("versioninfo.artifacts.tsv");
    const INVENTORY: &str = include_str!("versioninfo.inventory.tsv");
    const PRODUCTION_PREFIX_SHA256: &str =
        "e363d4160e7ddbf26f3d1fdf492634e4be024ae120433724863e5ae38c8db65c";

    fn repo_root() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../..")
    }

    fn sha256(bytes: impl AsRef<[u8]>) -> String {
        format!("{:x}", Sha256::digest(bytes.as_ref()))
    }

    fn rust_source() -> String {
        fs::read_to_string(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/versioninfo.rs"))
            .unwrap()
    }

    fn production_source() -> String {
        rust_source()
            .split_once("#[cfg(test)]")
            .unwrap()
            .0
            .to_owned()
    }

    #[test]
    fn lockdown_inventory_matches_go_source_and_rust_symbols() {
        let artifact_rows = data_rows(ARTIFACTS);
        assert_eq!(artifact_rows.len(), 2);
        assert!(artifact_rows.iter().all(|row| row.len() == 3));
        let root = repo_root();
        for row in artifact_rows {
            assert_eq!(
                sha256(fs::read(root.join(row[0])).expect("read versioninfo artifact")),
                row[2],
                "owned artifact drifted: {}",
                row[0]
            );
        }
        assert_eq!(
            sha256(GO_SOURCE),
            artifact_hash(ARTIFACTS, "pkg/util/versioninfo/versioninfo.go")
        );
        assert_eq!(
            sha256(BUILD_SOURCE),
            artifact_hash(ARTIFACTS, "pkg/util/versioninfo/BUILD.bazel")
        );

        let mut lines = INVENTORY
            .lines()
            .filter(|line| !line.is_empty() && !line.starts_with('#'));
        assert_eq!(
            lines.next(),
            Some("obligation_id\tcategory\tsource_path\tast_anchor\tnode_sha256\towner\tstatus\trust_symbol\tevidence\tmutation_policy")
        );
        let expected = BTreeMap::from([
            (
                "const:CommunityEdition:0",
                (
                    "PORTED",
                    "COMMUNITY_EDITION",
                    "rust-test:source_storage_class_and_override_boundaries_are_exact",
                ),
            ),
            (
                "var:TiDBBuildTS:0",
                ("DECLINED", "-", "go-probe:linktime_and_runtime_mutability"),
            ),
            (
                "var:TiDBEdition:0",
                ("DECLINED", "-", "go-probe:linktime_and_runtime_mutability"),
            ),
            (
                "var:TiDBEnterpriseExtensionGitHash:0",
                ("DECLINED", "-", "go-probe:linktime_and_runtime_mutability"),
            ),
            (
                "var:TiDBGitBranch:0",
                ("DECLINED", "-", "go-probe:linktime_and_runtime_mutability"),
            ),
            (
                "var:TiDBGitHash:0",
                ("DECLINED", "-", "go-probe:linktime_and_runtime_mutability"),
            ),
        ]);

        let mut statuses = BTreeMap::new();
        let mut actual = BTreeMap::new();
        for line in lines {
            let columns: Vec<_> = line.split('\t').collect();
            assert_eq!(columns.len(), 10, "invalid inventory row: {line}");
            assert_eq!(columns[2], "pkg/util/versioninfo/versioninfo.go");
            *statuses.entry(columns[6]).or_insert(0usize) += 1;
            assert!(
                actual
                    .insert(columns[3], (columns[6], columns[7], columns[8]))
                    .is_none(),
                "duplicate inventory anchor: {}",
                columns[3]
            );
        }
        assert_eq!(actual, expected, "the exact inventory mapping drifted");
        assert_eq!(statuses.get("PORTED"), Some(&1));
        assert_eq!(statuses.get("DECLINED"), Some(&5));
        assert_eq!(statuses.get("UNREACHABLE"), None);
        let _: &str = COMMUNITY_EDITION;
    }

    fn data_rows(contents: &str) -> Vec<Vec<&str>> {
        contents
            .lines()
            .filter(|line| !line.is_empty() && !line.starts_with('#'))
            .skip(1)
            .map(|line| line.split('\t').collect())
            .collect()
    }

    fn artifact_hash(contents: &str, path: &str) -> String {
        data_rows(contents)
            .into_iter()
            .find(|row| row[0] == path)
            .map(|row| row[2].to_owned())
            .expect("artifact hash row")
    }

    #[test]
    fn source_storage_class_and_override_boundaries_are_exact() {
        assert_eq!(COMMUNITY_EDITION, "Community");
        assert_eq!(
            TIDB_BUILD_TS,
            option_env!("TIDB_BUILD_TS").unwrap_or("None")
        );
        assert_eq!(
            TIDB_GIT_HASH,
            option_env!("TIDB_GIT_HASH").unwrap_or("None")
        );
        assert_eq!(
            TIDB_GIT_BRANCH,
            option_env!("TIDB_GIT_BRANCH").unwrap_or("None")
        );
        assert_eq!(
            TIDB_EDITION,
            option_env!("TIDB_EDITION").unwrap_or(COMMUNITY_EDITION)
        );
        assert_eq!(
            TIDB_ENTERPRISE_EXTENSION_GIT_HASH,
            option_env!("TIDB_ENTERPRISE_EXTENSION_GIT_HASH").unwrap_or("")
        );

        let production = production_source();
        assert_eq!(
            sha256(&production),
            PRODUCTION_PREFIX_SHA256,
            "the audited production mapping changed"
        );
        assert!(!production.contains("pub static"));
        assert!(!production.contains("static mut"));
        assert!(!production.contains("Mutex"));
        assert!(!production.contains("RwLock"));
    }
}
