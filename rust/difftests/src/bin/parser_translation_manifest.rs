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

//! Verify the source-to-Rust translation manifest for TiDB's hand parser.
//!
//! The canonical manifest is an ownership and visibility artifact, not a
//! parser-parity metric. Every non-test Go source directly under `pkg/parser/`
//! owns one fragment under `corpus/coverage/evidence/parser/`, with an owning
//! Rust module plus a conservative status: `ported`, `partial`, or
//! `unassigned`. `partial` says the listed Rust module contains a bounded
//! translation and retains work; `unassigned` makes the missing owner
//! explicit. The checked manifest and summary are generated from those source
//! fragments so parallel owners can work without colliding on either output.
//!
//! ```text
//! cd rust
//! cargo run -j 12 -p difftest --bin parser_translation_manifest -- --check-fragments
//! cargo run -j 12 -p difftest --bin parser_translation_manifest -- --check
//! cargo run -j 12 -p difftest --bin parser_translation_manifest -- --write
//! ```

use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

#[path = "../evidence_fragments.rs"]
mod evidence_fragments;

use evidence_fragments::sorted_tsv_files;

const MANIFEST_RELATIVE_PATH: &str =
    "rust/difftests/corpus/coverage/parser_translation_manifest.tsv";
const SUMMARY_RELATIVE_PATH: &str = "rust/difftests/corpus/coverage/parser_translation_summary.tsv";
const FRAGMENT_DIRECTORY_RELATIVE_PATH: &str = "rust/difftests/corpus/coverage/evidence/parser";
const MANIFEST_HEADER: &str = "go_source\trust_module\tstatus";
const SUMMARY_HEADER: &str = "kind\tstatus\trust_module\tcount";
const STATUS_ORDER: [&str; 3] = ["ported", "partial", "unassigned"];

#[derive(Clone, Debug, Eq, PartialEq)]
struct Row {
    source: String,
    rust_module: String,
    status: String,
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("difftest must live at rust/difftests")
        .to_path_buf()
}

fn manifest_path(root: &Path) -> PathBuf {
    root.join(MANIFEST_RELATIVE_PATH)
}

fn summary_path(root: &Path) -> PathBuf {
    root.join(SUMMARY_RELATIVE_PATH)
}

fn go_parser_sources(root: &Path) -> Result<BTreeSet<String>, String> {
    let dir = root.join("pkg/parser");
    let entries = fs::read_dir(&dir).map_err(|error| format!("read {}: {error}", dir.display()))?;
    let mut sources = BTreeSet::new();
    for entry in entries {
        let entry = entry.map_err(|error| format!("read {}: {error}", dir.display()))?;
        let path = entry.path();
        if !entry
            .file_type()
            .map_err(|error| format!("stat {}: {error}", path.display()))?
            .is_file()
        {
            continue;
        }
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if name.ends_with(".go") && !name.ends_with("_test.go") {
            sources.insert(format!("pkg/parser/{name}"));
        }
    }
    Ok(sources)
}

fn validate_source_set(root: &Path, rows: &[Row]) -> Result<(), String> {
    let actual = go_parser_sources(root)?;
    let mut seen = BTreeSet::new();
    for row in rows {
        if !seen.insert(row.source.clone()) {
            return Err(format!("duplicate Go source in manifest: {}", row.source));
        }
        if row.status != "unassigned" && !root.join(&row.rust_module).is_file() {
            return Err(format!(
                "{} maps to missing Rust module {}",
                row.source, row.rust_module
            ));
        }
    }
    let missing: Vec<_> = actual.difference(&seen).cloned().collect();
    let stale: Vec<_> = seen.difference(&actual).cloned().collect();
    if missing.is_empty() && stale.is_empty() {
        return Ok(());
    }
    let mut error =
        String::from("parser translation manifest does not match pkg/parser source set");
    if !missing.is_empty() {
        error.push_str("\nmissing:");
        for source in missing {
            error.push_str("\n  ");
            error.push_str(&source);
        }
    }
    if !stale.is_empty() {
        error.push_str("\nstale:");
        for source in stale {
            error.push_str("\n  ");
            error.push_str(&source);
        }
    }
    Err(error)
}

fn expected_fragment_name(source: &str) -> Result<String, String> {
    let name = source
        .strip_prefix("pkg/parser/")
        .and_then(|value| value.strip_suffix(".go"))
        .filter(|value| !value.is_empty() && !value.contains('/'))
        .ok_or_else(|| format!("parser fragment has invalid top-level Go source {source:?}"))?;
    Ok(format!("{name}.tsv"))
}

fn read_fragment_rows(root: &Path) -> Result<Vec<Row>, String> {
    let mut rows = Vec::new();
    let mut origins = BTreeMap::new();
    for path in sorted_tsv_files(root, FRAGMENT_DIRECTORY_RELATIVE_PATH)? {
        let text = fs::read_to_string(&path)
            .map_err(|error| format!("read {}: {error}", path.display()))?;
        let mut lines = text.lines();
        if lines.next() != Some(MANIFEST_HEADER) {
            return Err(format!("{}: wrong or missing header", path.display()));
        }
        let data: Vec<_> = lines
            .enumerate()
            .filter(|(_, line)| !line.is_empty() && !line.starts_with('#'))
            .collect();
        if data.len() != 1 {
            return Err(format!(
                "{}: expected exactly one parser source row, found {}",
                path.display(),
                data.len()
            ));
        }
        let (index, line) = data[0];
        let fields: Vec<_> = line.split('\t').collect();
        if fields.len() != 3 {
            return Err(format!(
                "{}:{}: expected 3 tab-separated fields",
                path.display(),
                index + 2
            ));
        }
        let row = Row {
            source: fields[0].to_owned(),
            rust_module: fields[1].to_owned(),
            status: fields[2].to_owned(),
        };
        if let Some(first_path) = origins.get(&row.source) {
            return Err(format!(
                "{}:{}: duplicate Go source {} (first declared in {})",
                path.display(),
                index + 2,
                row.source,
                first_path
            ));
        }
        if !STATUS_ORDER.contains(&row.status.as_str()) {
            return Err(format!(
                "{}:{}: unknown status {:?}; expected ported, partial, or unassigned",
                path.display(),
                index + 2,
                row.status
            ));
        }
        if row.source.is_empty() || row.rust_module.is_empty() {
            return Err(format!(
                "{}:{}: empty source or owner",
                path.display(),
                index + 2
            ));
        }
        if row.status == "unassigned" && row.rust_module != "-" {
            return Err(format!(
                "{}:{}: unassigned entries must use '-' as their owner",
                path.display(),
                index + 2
            ));
        }
        if row.status != "unassigned" && row.rust_module == "-" {
            return Err(format!(
                "{}:{}: assigned entries need a Rust module path",
                path.display(),
                index + 2
            ));
        }
        let expected_name = expected_fragment_name(&row.source)
            .map_err(|error| format!("{}:{}: {error}", path.display(), index + 2))?;
        if path.file_name().and_then(|value| value.to_str()) != Some(expected_name.as_str()) {
            return Err(format!(
                "{}:{}: source {} belongs in {expected_name}",
                path.display(),
                index + 2,
                row.source
            ));
        }
        if row.status != "unassigned" && !root.join(&row.rust_module).is_file() {
            return Err(format!(
                "{}:{}: {} maps to missing Rust module {}",
                path.display(),
                index + 2,
                row.source,
                row.rust_module
            ));
        }
        origins.insert(row.source.clone(), path.display().to_string());
        rows.push(row);
    }
    rows.sort_by(|left, right| left.source.cmp(&right.source));
    validate_source_set(root, &rows)?;
    Ok(rows)
}

fn render_manifest(rows: &[Row]) -> String {
    let mut output = String::from(MANIFEST_HEADER);
    output.push('\n');
    for row in rows {
        output.push_str(&row.source);
        output.push('\t');
        output.push_str(&row.rust_module);
        output.push('\t');
        output.push_str(&row.status);
        output.push('\n');
    }
    output
}

fn render_summary(rows: &[Row]) -> String {
    let mut status_counts = BTreeMap::new();
    let mut module_counts = BTreeMap::new();
    for status in STATUS_ORDER {
        status_counts.insert(status, 0usize);
    }
    for row in rows {
        *status_counts
            .get_mut(row.status.as_str())
            .expect("validated manifest status") += 1;
        if row.status != "unassigned" {
            *module_counts
                .entry((&row.status, &row.rust_module))
                .or_insert(0usize) += 1;
        }
    }

    let mut output = String::from(SUMMARY_HEADER);
    output.push('\n');
    for status in STATUS_ORDER {
        output.push_str("status\t");
        output.push_str(status);
        output.push_str("\t-\t");
        output.push_str(&status_counts[status].to_string());
        output.push('\n');
    }
    for ((status, module), count) in module_counts {
        output.push_str("module\t");
        output.push_str(status);
        output.push('\t');
        output.push_str(module);
        output.push('\t');
        output.push_str(&count.to_string());
        output.push('\n');
    }
    output
}

fn run() -> Result<(), String> {
    let mut args = env::args().skip(1);
    let mode = args
        .next()
        .ok_or_else(|| "expected --check-fragments, --check, or --write".to_owned())?;
    if args.next().is_some()
        || !matches!(mode.as_str(), "--check-fragments" | "--check" | "--write")
    {
        return Err(
            "usage: parser_translation_manifest --check-fragments|--check|--write".to_owned(),
        );
    }
    let root = repo_root();
    let rows = read_fragment_rows(&root)?;
    if mode == "--check-fragments" {
        return Ok(());
    }
    let manifest = render_manifest(&rows);
    let summary = render_summary(&rows);
    match mode.as_str() {
        "--write" => {
            let manifest_path = manifest_path(&root);
            fs::write(&manifest_path, manifest)
                .map_err(|error| format!("write {}: {error}", manifest_path.display()))?;
            let summary_path = summary_path(&root);
            fs::write(&summary_path, summary)
                .map_err(|error| format!("write {}: {error}", summary_path.display()))
        }
        "--check" => {
            let mut stale = Vec::new();
            for (path, expected) in [
                (manifest_path(&root), manifest),
                (summary_path(&root), summary),
            ] {
                let actual = fs::read_to_string(&path)
                    .map_err(|error| format!("read {}: {error}", path.display()))?;
                if actual != expected {
                    stale.push(path);
                }
            }
            if stale.is_empty() {
                Ok(())
            } else {
                Err(format!(
                    "{} stale generated parser artifact(s): {}; run cargo run -j 12 -p difftest --bin parser_translation_manifest -- --write",
                    stale.len(),
                    stale
                        .iter()
                        .map(|path| path.display().to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                ))
            }
        }
        _ => unreachable!("validated mode"),
    }
}

fn main() {
    if let Err(error) = run() {
        eprintln!("parser translation manifest: {error}");
        std::process::exit(1);
    }
}
