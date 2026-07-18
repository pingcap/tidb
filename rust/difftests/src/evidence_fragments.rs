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

use std::fs;
use std::path::{Path, PathBuf};

/// Returns every regular `.tsv` evidence fragment in deterministic path order.
///
/// Evidence directories are intentionally closed worlds. A stray README,
/// editor backup, symlink, or nested directory would otherwise make a row
/// invisible to the generated ledgers while looking like owned evidence.
pub(crate) fn sorted_tsv_files(
    root: &Path,
    relative_directory: &str,
) -> Result<Vec<PathBuf>, String> {
    let directory = root.join(relative_directory);
    if !directory.is_dir() {
        return Err(format!(
            "evidence fragment directory {} does not exist",
            directory.display()
        ));
    }

    let entries = fs::read_dir(&directory)
        .map_err(|error| format!("cannot read {}: {error}", directory.display()))?;
    let mut paths = Vec::new();
    for entry in entries {
        let entry =
            entry.map_err(|error| format!("cannot enumerate {}: {error}", directory.display()))?;
        let path = entry.path();
        let file_type = entry
            .file_type()
            .map_err(|error| format!("cannot inspect {}: {error}", path.display()))?;
        if !file_type.is_file() || path.extension().and_then(|value| value.to_str()) != Some("tsv")
        {
            return Err(format!(
                "unknown evidence fragment {}; only regular .tsv files are allowed",
                path.display()
            ));
        }
        validate_header_tabs(&path)?;
        paths.push(path);
    }
    paths.sort();
    if paths.is_empty() {
        return Err(format!(
            "evidence fragment directory {} contains no .tsv files",
            directory.display()
        ));
    }
    Ok(paths)
}

/// Evidence headers are comments, so a literal `\\t` escape would otherwise
/// look harmless while hiding a malformed TSV contract. Data rows must use
/// actual tab separators; reject the common escaped-header typo at discovery
/// time so every ledger consumer fails before silently ignoring the header.
fn validate_header_tabs(path: &Path) -> Result<(), String> {
    let text = fs::read_to_string(path)
        .map_err(|error| format!("cannot read evidence fragment {}: {error}", path.display()))?;
    for (index, line) in text.lines().enumerate() {
        if line.starts_with('#') && line.contains("\\t") {
            return Err(format!(
                "{}:{}: evidence header must contain literal tabs, not \\\\t escapes",
                path.display(),
                index + 1
            ));
        }
    }
    Ok(())
}

/// Ensures a fragment is owned by the wave named in each of its rows.
///
/// `+` is expanded instead of discarded so combined owners remain readable
/// and cannot collide with an owner that already contains a dash.
// This shared source module is compiled into three independent binaries; the
// parser manifest uses source basenames instead of wave-owner filenames.
#[allow(dead_code)]
pub(crate) fn validate_fragment_owner(path: &Path, owner: &str) -> Result<(), String> {
    let mut expected = String::new();
    for character in owner.chars() {
        if character.is_ascii_alphanumeric() || matches!(character, '.' | '-') {
            expected.push(character);
        } else if character == '+' {
            expected.push_str("-plus-");
        } else {
            expected.push('-');
        }
    }
    expected.push_str(".tsv");
    if path.file_name().and_then(|value| value.to_str()) != Some(expected.as_str()) {
        return Err(format!(
            "evidence owner {owner:?} belongs in {expected}, not {}",
            path.display()
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{sorted_tsv_files, validate_fragment_owner};
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn sorts_fragments_and_rejects_unknown_entries() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock must be after the Unix epoch")
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "tidb-rust-evidence-fragments-{}-{nonce}",
            std::process::id()
        ));
        let directory = root.join("evidence");
        fs::create_dir_all(&directory).expect("create evidence fixture directory");
        fs::write(directory.join("z.tsv"), "z\n").expect("write z fragment");
        fs::write(directory.join("a.tsv"), "a\n").expect("write a fragment");

        let paths = sorted_tsv_files(&root, "evidence").expect("valid TSV fragments");
        let names: Vec<_> = paths
            .iter()
            .map(|path| path.file_name().expect("fragment filename"))
            .collect();
        assert_eq!(names, ["a.tsv", "z.tsv"]);

        fs::write(directory.join("README.md"), "not evidence\n").expect("write invalid fragment");
        let error = sorted_tsv_files(&root, "evidence").expect_err("README must be rejected");
        assert!(error.contains("only regular .tsv files are allowed"));

        fs::remove_file(directory.join("README.md")).expect("remove invalid fixture");
        fs::remove_file(directory.join("a.tsv")).expect("remove a fixture");
        fs::remove_file(directory.join("z.tsv")).expect("remove z fixture");
        fs::remove_dir(&directory).expect("remove fixture directory");
        fs::remove_dir(&root).expect("remove fixture root");
    }

    #[test]
    fn rejects_escaped_tabs_in_headers() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock must be after the Unix epoch")
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "tidb-rust-evidence-header-tabs-{}-{nonce}",
            std::process::id()
        ));
        let directory = root.join("evidence");
        fs::create_dir_all(&directory).expect("create evidence fixture directory");
        fs::write(directory.join("bad.tsv"), "# kind\\tsource_path\nrow\n")
            .expect("write malformed header");

        let error = sorted_tsv_files(&root, "evidence").expect_err("escaped header must fail");
        assert!(error.contains("header must contain literal tabs"));

        fs::remove_file(directory.join("bad.tsv")).expect("remove malformed fixture");
        fs::remove_dir(&directory).expect("remove fixture directory");
        fs::remove_dir(&root).expect("remove fixture root");
    }

    #[test]
    fn owner_name_selects_exactly_one_fragment() {
        let valid = std::path::Path::new("table-wave3-plus-wave5.tsv");
        validate_fragment_owner(valid, "table-wave3+wave5").expect("matching owner fragment");
        let error = validate_fragment_owner(valid, "table-wave4+wave5")
            .expect_err("mismatched owner must be rejected");
        assert!(error.contains("belongs in table-wave4-plus-wave5.tsv"));
    }
}
