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

//! Transcreation of Go `pkg/util/versioninfo`: build-time version stamps.
//!
//! Go injects these via `-ldflags -X`; the Rust build injects them via
//! `TIDB_*` environment variables at compile time (`option_env!`), with the
//! same defaults when unset.

/// The default edition for building (Go `CommunityEdition`).
pub const COMMUNITY_EDITION: &str = "Community";

/// Build timestamp (Go `TiDBBuildTS`).
pub const TIDB_BUILD_TS: &str = match option_env!("TIDB_BUILD_TS") {
    Some(v) => v,
    None => "None",
};
/// Git commit hash (Go `TiDBGitHash`).
pub const TIDB_GIT_HASH: &str = match option_env!("TIDB_GIT_HASH") {
    Some(v) => v,
    None => "None",
};
/// Git branch (Go `TiDBGitBranch`).
pub const TIDB_GIT_BRANCH: &str = match option_env!("TIDB_GIT_BRANCH") {
    Some(v) => v,
    None => "None",
};
/// Edition (Go `TiDBEdition`).
pub const TIDB_EDITION: &str = match option_env!("TIDB_EDITION") {
    Some(v) => v,
    None => COMMUNITY_EDITION,
};
/// Enterprise extension commit hash (Go `TiDBEnterpriseExtensionGitHash`).
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

    const GO_SOURCE_SHA256: &str =
        "daa224cf8308f7b9de126919839ed95de7028e67b989d3d1d772d60309603003";
    const INVENTORY_SHA256: &str =
        "850ff36642994f9441579df9389438b672136a549d60a0595e403e5307d3445b";
    const PRODUCTION_PREFIX_SHA256: &str =
        "992c27c08ef48fed1cc0250feb127f33ddf259e2597c32572209f55e14f77a7b";
    const INVENTORY: &str = include_str!("versioninfo.inventory.tsv");

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
        let go_source = fs::read(repo_root().join("pkg/util/versioninfo/versioninfo.go")).unwrap();
        assert_eq!(
            sha256(go_source),
            GO_SOURCE_SHA256,
            "owning Go source drifted"
        );
        assert_eq!(
            sha256(INVENTORY),
            INVENTORY_SHA256,
            "versioninfo inventory drifted"
        );

        let rows: Vec<Vec<&str>> = INVENTORY
            .lines()
            .filter(|line| !line.starts_with('#') && !line.starts_with("id\t"))
            .map(|line| line.split('\t').collect())
            .collect();
        assert!(rows.iter().all(|row| row.len() == 6));
        let actual: Vec<[&str; 5]> = rows
            .iter()
            .map(|row| [row[0], row[1], row[2], row[3], row[4]])
            .collect();
        let expected = [
            [
                "D01",
                "declaration",
                "CommunityEdition constant",
                "PORTED",
                "COMMUNITY_EDITION",
            ],
            [
                "D02",
                "declaration",
                "TiDBBuildTS variable",
                "PORTED",
                "TIDB_BUILD_TS",
            ],
            [
                "D03",
                "declaration",
                "TiDBGitHash variable",
                "PORTED",
                "TIDB_GIT_HASH",
            ],
            [
                "D04",
                "declaration",
                "TiDBGitBranch variable",
                "PORTED",
                "TIDB_GIT_BRANCH",
            ],
            [
                "D05",
                "declaration",
                "TiDBEdition variable",
                "PORTED",
                "TIDB_EDITION",
            ],
            [
                "D06",
                "declaration",
                "TiDBEnterpriseExtensionGitHash variable",
                "PORTED",
                "TIDB_ENTERPRISE_EXTENSION_GIT_HASH",
            ],
            [
                "R01",
                "rule",
                "CommunityEdition is the literal Community",
                "PORTED",
                "COMMUNITY_EDITION",
            ],
            [
                "R02",
                "rule",
                "TiDBBuildTS defaults to None and accepts a verbatim build stamp",
                "PORTED",
                "TIDB_BUILD_TS",
            ],
            [
                "R03",
                "rule",
                "TiDBGitHash defaults to None and accepts a verbatim build stamp",
                "PORTED",
                "TIDB_GIT_HASH",
            ],
            [
                "R04",
                "rule",
                "TiDBGitBranch defaults to None and accepts a verbatim build stamp",
                "PORTED",
                "TIDB_GIT_BRANCH",
            ],
            [
                "R05",
                "rule",
                "TiDBEdition defaults to CommunityEdition and accepts a verbatim build stamp",
                "PORTED",
                "TIDB_EDITION",
            ],
            [
                "R06",
                "rule",
                "TiDBEnterpriseExtensionGitHash defaults to the empty string and accepts a verbatim build stamp",
                "PORTED",
                "TIDB_ENTERPRISE_EXTENSION_GIT_HASH",
            ],
            [
                "R07",
                "rule",
                "The five version fields are mutable process globals after initialization",
                "DECLINED",
                "-",
            ],
        ];
        assert_eq!(actual, expected, "the exact inventory mapping drifted");

        let mut statuses = BTreeMap::new();
        for row in &rows {
            *statuses.entry(row[3]).or_insert(0usize) += 1;
        }
        assert_eq!(statuses.get("PORTED"), Some(&12));
        assert_eq!(statuses.get("DECLINED"), Some(&1));
        assert_eq!(statuses.get("UNREACHABLE"), None);

        let production = production_source();
        for row in rows.iter().filter(|row| row[3] == "PORTED") {
            let declaration = format!("pub const {}: &str", row[4]);
            assert!(
                production.contains(&declaration),
                "{} names missing Rust symbol {}",
                row[0],
                row[4]
            );
        }
        assert_eq!(rows.last().unwrap()[0], "R07");
        assert_eq!(rows.last().unwrap()[3], "DECLINED");
        assert_eq!(rows.last().unwrap()[4], "-");
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
