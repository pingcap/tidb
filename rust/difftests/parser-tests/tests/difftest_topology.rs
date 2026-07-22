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

//! Contract for package-owned Cargo targets and source-owned selector shards.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

fn files_recursive(dir: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    for entry in fs::read_dir(dir).unwrap_or_else(|error| panic!("read {}: {error}", dir.display()))
    {
        let entry =
            entry.unwrap_or_else(|error| panic!("read entry in {}: {error}", dir.display()));
        let path = entry.path();
        if path.is_dir() {
            files.extend(files_recursive(&path));
        } else if path.is_file() {
            files.push(path);
        }
    }
    files.sort();
    files
}

fn selector_paths_from_shard(path: &Path) -> Vec<String> {
    fs::read_to_string(path)
        .unwrap_or_else(|error| panic!("read {}: {error}", path.display()))
        .lines()
        .filter_map(|line| line.trim().strip_prefix("#[path = \""))
        .filter_map(|line| line.strip_suffix("\"]"))
        .filter(|path| path.starts_with("selectors/") && path.ends_with(".rs"))
        .map(str::to_owned)
        .collect()
}

fn root_rust_files(package_root: &Path) -> BTreeSet<String> {
    let tests = package_root.join("tests");
    fs::read_dir(&tests)
        .unwrap_or_else(|error| panic!("read {}: {error}", tests.display()))
        .filter_map(Result::ok)
        .filter_map(|entry| {
            let path = entry.path();
            (path.is_file() && path.extension().is_some_and(|extension| extension == "rs"))
                .then(|| format!("tests/{}", entry.file_name().to_string_lossy()))
        })
        .collect()
}

fn assert_explicit_package_tests(package_root: &Path) {
    let manifest_path = package_root.join("Cargo.toml");
    let manifest = fs::read_to_string(&manifest_path)
        .unwrap_or_else(|error| panic!("read {}: {error}", manifest_path.display()));
    assert!(
        manifest.contains("autotests = false"),
        "{} must disable implicit integration-test discovery",
        manifest_path.display()
    );
    let cargo_paths = cargo_test_paths(&manifest);
    if manifest.contains("build = \"../../scripts/aggregate-tests.rs\"") {
        assert_eq!(
            cargo_paths,
            BTreeSet::from(["tests/all.rs".to_owned()]),
            "{} must expose exactly one aggregate test target",
            package_root.display()
        );
    } else {
        assert_eq!(
            cargo_paths,
            root_rust_files(package_root),
            "every root test in {} must be one explicit Cargo target",
            package_root.display()
        );
    }
}

fn cargo_test_paths(manifest: &str) -> BTreeSet<String> {
    manifest
        .lines()
        .filter_map(|line| line.trim().strip_prefix("path = \""))
        .filter_map(|line| line.strip_suffix('"'))
        .filter(|path| path.starts_with("tests/") && path.ends_with(".rs"))
        .map(str::to_owned)
        .collect()
}

#[test]
fn every_selector_is_owned_once_and_every_root_entrypoint_is_explicit() {
    let parser_package = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let difftest_package = difftest::difftest_root();
    let result_package = difftest_package.join("result-tests");
    let tests = parser_package.join("tests");
    let selectors = tests.join("selectors");
    let source_selectors: BTreeSet<_> = files_recursive(&selectors)
        .into_iter()
        .filter(|path| path.extension().is_some_and(|extension| extension == "rs"))
        .map(|path| {
            path.strip_prefix(&tests)
                .unwrap()
                .to_string_lossy()
                .into_owned()
        })
        .collect();
    assert!(
        !source_selectors.is_empty(),
        "selector tree is unexpectedly empty"
    );

    let root_files = root_rust_files(&parser_package);
    assert!(
        root_files
            .iter()
            .all(|path| !path.ends_with("_selector.rs")),
        "selector modules must live below tests/selectors, never at tests root"
    );

    let non_shard_roots: BTreeSet<_> = root_files
        .iter()
        .filter(|path| !path.starts_with("tests/selector_"))
        .cloned()
        .collect();
    let expected_ring_roots = BTreeSet::from([
        "tests/all.rs".to_owned(),
        "tests/difftest_topology.rs".to_owned(),
        "tests/integration_parser_diff.rs".to_owned(),
        "tests/integration_parser_inventory.rs".to_owned(),
        "tests/lexer_diff.rs".to_owned(),
        "tests/parser_diff.rs".to_owned(),
    ]);
    assert_eq!(
        non_shard_roots, expected_ring_roots,
        "selector modules must live below tests/selectors, not at package test root"
    );

    let shard_paths: Vec<_> = root_files
        .iter()
        .filter(|path| path.starts_with("tests/selector_"))
        .map(|path| tests.join(path.strip_prefix("tests/").unwrap()))
        .collect();
    assert_eq!(shard_paths.len(), 13, "stable selector shard count drifted");

    let mut ownership: BTreeMap<String, usize> = BTreeMap::new();
    for shard in shard_paths {
        for selector in selector_paths_from_shard(&shard) {
            *ownership.entry(selector).or_default() += 1;
        }
    }
    let referenced: BTreeSet<_> = ownership.keys().cloned().collect();
    assert_eq!(
        referenced, source_selectors,
        "selector shard ownership has an orphan or a path that does not exist"
    );
    let duplicates: Vec<_> = ownership
        .into_iter()
        .filter(|(_, count)| *count != 1)
        .collect();
    assert!(
        duplicates.is_empty(),
        "selector is assigned more than once: {duplicates:?}"
    );

    assert_explicit_package_tests(&difftest_package);
    assert_explicit_package_tests(&parser_package);
    assert_explicit_package_tests(&result_package);

    assert!(
        !difftest_package.join("tests/selectors").exists()
            && !result_package.join("tests/selectors").exists(),
        "the parser package must be the sole physical selector owner"
    );
}
