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

//! Guards the `pkg/util/kvcache` semantic receipt's evidence list.
//!
//! `tests/kvcache.semantic.toml` names the files a reviewer must read before
//! accepting the package claim. A later batch removed
//! `rust/crates/tidb-executor/tests/apply_cache_source.rs` (the apply cache was
//! narrowed to executor internals) without updating this receipt, so the
//! recorded evidence pointed at a file that no longer exists. This test fails
//! before that repair and passes after it, and keeps future deletions from
//! silently invalidating the receipt.

use std::path::{Path, PathBuf};

/// The repository root, three levels above this crate.
fn repository_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(3)
        .expect("rust/crates/tidb-kvcache is three levels below the repository root")
        .to_path_buf()
}

/// Every quoted path in the receipt's `evidence_files` array.
fn evidence_files(receipt: &str) -> Vec<&str> {
    let mut files = Vec::new();
    let mut in_evidence = false;
    for line in receipt.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("evidence_files") {
            in_evidence = true;
            continue;
        }
        if !in_evidence {
            continue;
        }
        if trimmed.starts_with(']') {
            break;
        }
        if let Some(path) = trimmed.split('"').nth(1) {
            files.push(path);
        }
    }
    files
}

#[test]
fn every_evidence_file_recorded_by_the_semantic_receipt_exists() {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let receipt = std::fs::read_to_string(manifest_dir.join("tests/kvcache.semantic.toml"))
        .expect("the semantic receipt is checked in");
    let root = repository_root();
    let files = evidence_files(&receipt);
    assert!(!files.is_empty(), "the receipt must record evidence files");
    for file in &files {
        assert!(
            root.join(file).exists(),
            "evidence file {file} recorded by kvcache.semantic.toml does not exist"
        );
    }
    assert!(
        files.contains(&"rust/testport/receipts/executor_internal_applycache.md"),
        "the apply-cache evidence must be its owning receipt, not the removed external test file"
    );
}

/// Every `rust/crates/...` path the owning ExecPlan names must exist.
///
/// The audit ExecPlan is the map a reviewer follows; a renamed or deleted
/// owner or test file leaves it pointing at nothing, which is how the stale
/// `tests/kvcache_source.rs` reference survived until this guard.
#[test]
fn every_crate_path_recorded_by_the_audit_execplan_exists() {
    let root = repository_root();
    let execplan =
        std::fs::read_to_string(root.join("rust/docs/operations/kvcache-audit-execplan.md"))
            .expect("the audit execplan is checked in");
    let is_path_character = |character: char| {
        character.is_ascii_alphanumeric() || matches!(character, '/' | '_' | '.' | '-')
    };
    let mut checked = 0;
    for token in execplan.split(|character: char| !is_path_character(character)) {
        let Some(relative) = token.strip_prefix("rust/crates/") else {
            continue;
        };
        let path = format!("rust/crates/{relative}");
        assert!(
            root.join(&path).exists(),
            "audit execplan path {path} does not exist"
        );
        checked += 1;
    }
    assert!(
        checked >= 4,
        "the execplan must reference the owner, its contract, and the consumers"
    );
}
