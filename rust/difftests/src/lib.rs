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
// See the License for the specific language governing permissions and
// limitations under the License.

//! Helpers shared by the differential tests: loading the SQL corpus and the
//! golden token dump produced by the Go `godump` tool.
//!
//! The parser/expr/table corpora are each a DIRECTORY of per-topic file
//! pairs (`<topic>.txt` statements + `<topic>.golden.txt` expected results)
//! rather than one monolithic file. This is the corpus's actual concurrency
//! boundary: every past increment appended to the tail of one shared file,
//! which is exactly where two contributors' (or agents') edits collide. A
//! new topic is a brand-new file pair — it never touches an existing file,
//! so unrelated increments can land in parallel with zero merge conflicts.
//! Topics are loaded in filename-sorted order so the combined result is
//! deterministic regardless of directory listing order or creation order.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

/// Returns the shared differential harness root (`rust/difftests`).
///
/// Ring test packages live below this directory, but the executable corpora
/// and checked coverage artifacts deliberately remain here as one evidence
/// authority. Deriving the path in the shared library prevents child-package
/// `CARGO_MANIFEST_DIR` values from silently selecting an empty local corpus.
pub fn difftest_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

/// Checked static Go parser oracle shared by the differential replay.
#[path = "bin/integration_parser_golden.rs"]
#[allow(dead_code, missing_docs)]
pub mod parser_oracle;

const EXECUTABLE_CORPUS_NAMESPACES: &[&str] = &["expr", "parser", "table"];

/// Validates the paired-topic contract for every executable corpus namespace.
///
/// The input is the repository root, not `rust/`, so callers and tests use
/// the same stable path that the checked Go inventories use. Coverage is an
/// evidence namespace and is deliberately excluded: Markdown belongs there,
/// never beside runnable corpus topics.
pub fn validate_executable_corpora(repo_root: &Path) -> Result<(), String> {
    let corpus_root = repo_root.join("rust/difftests/corpus");
    for namespace in EXECUTABLE_CORPUS_NAMESPACES {
        let dir = corpus_root.join(namespace);
        let entries = fs::read_dir(&dir)
            .map_err(|error| format!("read executable corpus {}: {error}", dir.display()))?;
        let mut sources = std::collections::BTreeSet::new();
        let mut goldens = std::collections::BTreeSet::new();
        for entry in entries {
            let entry =
                entry.map_err(|error| format!("read entry in {}: {error}", dir.display()))?;
            let path = entry.path();
            if !entry
                .file_type()
                .map_err(|error| format!("inspect {}: {error}", path.display()))?
                .is_file()
            {
                continue;
            }
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.ends_with(".md") {
                return Err(format!(
                    "evidence Markdown {} is inside executable corpus {}; move it under corpus/coverage",
                    path.display(),
                    namespace
                ));
            }
            if let Some(stem) = name.strip_suffix(".golden.txt") {
                if stem.is_empty() {
                    return Err(format!(
                        "invalid empty golden topic name in {}",
                        path.display()
                    ));
                }
                goldens.insert(stem.to_owned());
            } else if let Some(stem) = name.strip_suffix(".txt") {
                if stem.is_empty() {
                    return Err(format!(
                        "invalid empty source topic name in {}",
                        path.display()
                    ));
                }
                sources.insert(stem.to_owned());
            }
        }
        if let Some(stem) = sources.difference(&goldens).next() {
            return Err(format!(
                "executable corpus {namespace}/{stem}.txt has no paired {stem}.golden.txt"
            ));
        }
        if let Some(stem) = goldens.difference(&sources).next() {
            return Err(format!(
                "executable corpus {namespace}/{stem}.golden.txt has no paired {stem}.txt"
            ));
        }
    }
    Ok(())
}

/// Lists the topic stems in a per-topic corpus directory, sorted for
/// deterministic ordering. Each stem `t` has a `t.txt` statements file and a
/// `t.golden.txt` golden file alongside it.
pub fn corpus_topics(dir: &Path) -> Vec<String> {
    let mut stems: Vec<String> = fs::read_dir(dir)
        .unwrap_or_else(|e| panic!("reading corpus dir {}: {e}", dir.display()))
        .filter_map(|e| e.ok())
        .filter_map(|e| e.file_name().into_string().ok())
        .filter_map(|name| name.strip_suffix(".txt").map(str::to_string))
        .filter(|stem| !stem.ends_with(".golden"))
        .collect();
    stems.sort();
    stems
}

/// Reads and concatenates every topic's statements and golden text in a
/// per-topic corpus directory, in filename-sorted order. Preserves the
/// property that the Nth statement's expected result is the Nth line (or
/// `#IDX`/`#END` block) of the combined golden text, exactly as if the
/// topics were one monolithic file — callers that don't need per-topic
/// isolation (parser/expr, whose statements evaluate independently) can use
/// this directly; callers that do (table scripts, which share state across
/// statements within a topic but must NOT leak it across topics) should
/// iterate `corpus_topics` themselves instead.
pub fn load_corpus_dir(dir: &Path) -> (Vec<String>, String) {
    let mut stmts = Vec::new();
    let mut golden = String::new();
    for stem in corpus_topics(dir) {
        stmts.extend(parse_corpus(
            &fs::read_to_string(dir.join(format!("{stem}.txt"))).unwrap(),
        ));
        let g = fs::read_to_string(dir.join(format!("{stem}.golden.txt"))).unwrap();
        golden.push_str(&g);
        if !g.ends_with('\n') {
            golden.push('\n');
        }
    }
    (stmts, golden)
}

/// One statement's expected token labels, keyed by statement index.
pub type Golden = BTreeMap<usize, Vec<(usize, String)>>;

/// Parses the `godump` output format into a per-statement label map.
///
/// Format (see `difftests/godump/main.go`):
/// ```text
/// #IDX <n>
/// <offset> <label>
/// ...
/// #END
/// ```
pub fn parse_golden(text: &str) -> Golden {
    let mut out: Golden = BTreeMap::new();
    let mut cur: Option<usize> = None;
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("#IDX ") {
            cur = rest.trim().parse().ok();
            if let Some(idx) = cur {
                out.entry(idx).or_default();
            }
        } else if line == "#END" {
            cur = None;
        } else if let Some(idx) = cur {
            let mut parts = line.splitn(2, ' ');
            let off: usize = parts.next().unwrap_or("").parse().unwrap_or(0);
            let label = parts.next().unwrap_or("").to_string();
            out.get_mut(&idx).unwrap().push((off, label));
        }
    }
    out
}

/// Splits the corpus file into statements (one per non-empty, non-`##` line).
pub fn parse_corpus(text: &str) -> Vec<String> {
    text.lines()
        .filter(|l| !l.trim().is_empty() && !l.starts_with("##"))
        .map(|l| l.to_string())
        .collect()
}

#[cfg(test)]
mod corpus_contract_tests {
    use super::validate_executable_corpora;
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicUsize, Ordering};

    static NEXT_TEMP_DIR: AtomicUsize = AtomicUsize::new(0);

    fn temp_repo() -> PathBuf {
        let unique = NEXT_TEMP_DIR.fetch_add(1, Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!(
            "tidb-difftest-corpus-contract-{}-{unique}",
            std::process::id()
        ));
        fs::create_dir_all(root.join("rust/difftests/corpus/coverage")).unwrap();
        for namespace in ["expr", "parser", "table"] {
            fs::create_dir_all(root.join("rust/difftests/corpus").join(namespace)).unwrap();
        }
        root
    }

    fn executable(root: &Path, namespace: &str, name: &str, golden: bool) -> PathBuf {
        let suffix = if golden { ".golden.txt" } else { ".txt" };
        root.join("rust/difftests/corpus")
            .join(namespace)
            .join(format!("{name}{suffix}"))
    }

    fn remove(root: &Path) {
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn checked_executable_corpora_have_only_paired_topics() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .ancestors()
            .nth(2)
            .unwrap()
            .to_path_buf();
        validate_executable_corpora(&root).unwrap();
    }

    #[test]
    fn contract_rejects_source_only_topic() {
        let root = temp_repo();
        fs::write(
            executable(&root, "expr", "source_only", false),
            "select 1\n",
        )
        .unwrap();
        assert!(validate_executable_corpora(&root)
            .unwrap_err()
            .contains("source_only.txt has no paired"));
        remove(&root);
    }

    #[test]
    fn contract_rejects_golden_only_topic() {
        let root = temp_repo();
        fs::write(
            executable(&root, "parser", "golden_only", true),
            "#IDX 0\n#END\n",
        )
        .unwrap();
        assert!(validate_executable_corpora(&root)
            .unwrap_err()
            .contains("golden_only.golden.txt has no paired"));
        remove(&root);
    }

    #[test]
    fn contract_rejects_markdown_in_executable_namespace_but_allows_coverage_notes() {
        let root = temp_repo();
        fs::write(
            root.join("rust/difftests/corpus/coverage/note.md"),
            "note\n",
        )
        .unwrap();
        fs::write(
            root.join("rust/difftests/corpus/table/evidence.md"),
            "wrong place\n",
        )
        .unwrap();
        assert!(validate_executable_corpora(&root)
            .unwrap_err()
            .contains("move it under corpus/coverage"));
        remove(&root);
    }
}
