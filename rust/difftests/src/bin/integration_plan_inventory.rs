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

//! Generates the plan ring's source-backed `EXPLAIN` fixture manifest.
//!
//! The manifest is deliberately an obligation inventory, not a plan-parity
//! claim. It selects exactly the Go-accepted `ast.ExplainStmt` restores from
//! the checked integration parser oracle, keeps their source fixture range and
//! statement ordinal, and records every sibling expected-result artifact. A
//! source-fixture or Go-parser-oracle change therefore fails `--check` until
//! this manifest is deliberately regenerated.
//!
//! ```text
//! cd rust
//! cargo run -p difftest --bin integration_plan_inventory -- --write
//! cargo run -p difftest --bin integration_plan_inventory -- --check
//! cargo run -p difftest --bin integration_plan_inventory -- --summary
//! ```

#![allow(dead_code, missing_docs)]

#[path = "integration_parser_golden.rs"]
mod integration_parser_golden;

use integration_parser_golden::{read_golden, read_inventory, repo_root, GoOutcome, GoldenRecord};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

const MANIFEST_RELATIVE_PATH: &str =
    "rust/difftests/corpus/coverage/integration_plan_inventory.tsv";
const MANIFEST_HEADER: &str = "source_path\tsource_start_line\tsource_end_line\tstatement_ordinal\tplan_statement_kind\texpected_result_artifacts\tgo_restore_hex";

#[derive(Clone, Debug, Eq, PartialEq)]
struct PlanInput {
    path: String,
    start_line: usize,
    end_line: usize,
    ordinal: usize,
    kind: PlanStatementKind,
    result_artifacts: Vec<String>,
    restore: Vec<u8>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PlanStatementKind {
    Explain,
    ExplainAnalyze,
    ExplainForConnection,
    ExplainExplore,
}

impl PlanStatementKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Explain => "explain",
            Self::ExplainAnalyze => "explain_analyze",
            Self::ExplainForConnection => "explain_for_connection",
            Self::ExplainExplore => "explain_explore",
        }
    }

    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "explain" => Ok(Self::Explain),
            "explain_analyze" => Ok(Self::ExplainAnalyze),
            "explain_for_connection" => Ok(Self::ExplainForConnection),
            "explain_explore" => Ok(Self::ExplainExplore),
            _ => Err(format!("unknown plan statement kind {value:?}")),
        }
    }
}

impl PlanInput {
    fn render(&self) -> String {
        format!(
            "{}\t{}\t{}\t{}\t{}\t{}\t{}",
            self.path,
            self.start_line,
            self.end_line,
            self.ordinal,
            self.kind.as_str(),
            self.result_artifacts.join("|"),
            hex_encode(&self.restore),
        )
    }
}

fn manifest_path(root: &Path) -> PathBuf {
    root.join(MANIFEST_RELATIVE_PATH)
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}

fn hex_decode(value: &str) -> Result<Vec<u8>, String> {
    if !value.len().is_multiple_of(2) {
        return Err("hex payload has an odd length".to_owned());
    }
    fn digit(byte: u8) -> Result<u8, String> {
        match byte {
            b'0'..=b'9' => Ok(byte - b'0'),
            b'a'..=b'f' => Ok(byte - b'a' + 10),
            b'A'..=b'F' => Ok(byte - b'A' + 10),
            _ => Err(format!("invalid hex digit {byte:?}")),
        }
    }
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| Ok((digit(pair[0])? << 4) | digit(pair[1])?))
        .collect()
}

/// Resolve all expected result variants belonging to one mysqltest fixture.
/// This mirrors the upstream-test ledger rule: an exact stem wins where it
/// exists, and an underscore suffix is an environment-specific variant.
fn result_artifacts(root: &Path, fixture: &str) -> Result<Vec<String>, String> {
    let source = fixture
        .strip_suffix(".test")
        .and_then(|path| path.split_once("/t/"))
        .ok_or_else(|| format!("invalid integration fixture path {fixture:?}"))?;
    let result_stem = format!("{}/r/{}", source.0, source.1);
    let result_path = root.join(&result_stem);
    let parent = result_path
        .parent()
        .ok_or_else(|| format!("result path has no parent {result_stem:?}"))?;
    let name = result_path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| format!("result path is not UTF-8 {result_stem:?}"))?;
    let mut artifacts = Vec::new();
    for entry in fs::read_dir(parent)
        .map_err(|error| format!("read result directory {}: {error}", parent.display()))?
    {
        let entry = entry.map_err(|error| format!("read result directory entry: {error}"))?;
        if !entry
            .file_type()
            .map_err(|error| format!("read result file type: {error}"))?
            .is_file()
        {
            continue;
        }
        let candidate = entry.file_name();
        let candidate = candidate.to_string_lossy();
        if candidate == format!("{name}.result")
            || (candidate.starts_with(name)
                && candidate.ends_with(".result")
                && candidate
                    .strip_prefix(name)
                    .is_some_and(|suffix| suffix.starts_with('_')))
        {
            artifacts.push(format!("{result_stem}{}", &candidate[name.len()..]));
        }
    }
    artifacts.sort();
    if artifacts.is_empty() {
        return Err(format!(
            "EXPLAIN fixture {fixture} has no expected result artifact under {parent:?}"
        ));
    }
    Ok(artifacts)
}

fn statement_kind(restore: &[u8]) -> Option<PlanStatementKind> {
    if restore.starts_with(b"EXPLAIN ANALYZE ") {
        Some(PlanStatementKind::ExplainAnalyze)
    } else if restore.starts_with(b"EXPLAIN EXPLORE ") {
        Some(PlanStatementKind::ExplainExplore)
    } else if restore.starts_with(b"EXPLAIN ") {
        if restore
            .windows(b" FOR CONNECTION ".len())
            .any(|window| window == b" FOR CONNECTION ")
        {
            Some(PlanStatementKind::ExplainForConnection)
        } else {
            Some(PlanStatementKind::Explain)
        }
    } else {
        None
    }
}

fn collect(root: &Path, records: &[GoldenRecord]) -> Result<Vec<PlanInput>, String> {
    let mut entries = Vec::new();
    for record in records {
        if record.outcome != GoOutcome::Accepted {
            continue;
        }
        let explains: Vec<_> = record
            .restores
            .iter()
            .enumerate()
            .filter_map(|(ordinal, restore)| {
                statement_kind(restore).map(|kind| (ordinal, kind, restore))
            })
            .collect();
        if explains.is_empty() {
            continue;
        }
        let artifacts = result_artifacts(root, &record.input.path)?;
        for (ordinal, kind, restore) in explains {
            entries.push(PlanInput {
                path: record.input.path.clone(),
                start_line: record.input.start_line,
                end_line: record.input.end_line,
                ordinal: ordinal + 1,
                kind,
                result_artifacts: artifacts.clone(),
                restore: restore.clone(),
            });
        }
    }
    Ok(entries)
}

fn render(entries: &[PlanInput]) -> String {
    let mut output = String::from(
        "# Generated by cargo run -p difftest --bin integration_plan_inventory -- --write.\n",
    );
    output.push_str("# This is an EXPLAIN obligation inventory, not plan-parity evidence.\n");
    output.push_str(MANIFEST_HEADER);
    output.push('\n');
    for entry in entries {
        output.push_str(&entry.render());
        output.push('\n');
    }
    output
}

fn read_manifest(root: &Path) -> Result<Vec<PlanInput>, String> {
    let path = manifest_path(root);
    let text =
        fs::read_to_string(&path).map_err(|error| format!("read {}: {error}", path.display()))?;
    let mut lines = text.lines();
    if lines.next()
        != Some("# Generated by cargo run -p difftest --bin integration_plan_inventory -- --write.")
        || lines.next()
            != Some("# This is an EXPLAIN obligation inventory, not plan-parity evidence.")
        || lines.next() != Some(MANIFEST_HEADER)
    {
        return Err(format!(
            "{}: wrong or missing manifest header",
            path.display()
        ));
    }
    lines
        .enumerate()
        .map(|(index, line)| {
            let fields: Vec<_> = line.split('\t').collect();
            let context = format!("{}:{}", path.display(), index + 4);
            if fields.len() != 7 {
                return Err(format!(
                    "{context}: expected 7 TSV fields, got {}",
                    fields.len()
                ));
            }
            let ordinal = fields[3]
                .parse()
                .map_err(|_| format!("{context}: invalid statement ordinal {:?}", fields[3]))?;
            if ordinal == 0 {
                return Err(format!("{context}: statement ordinal must be one-based"));
            }
            let artifacts = fields[5].split('|').map(str::to_owned).collect::<Vec<_>>();
            if artifacts.is_empty() || artifacts.iter().any(|artifact| artifact.is_empty()) {
                return Err(format!("{context}: no expected result artifact"));
            }
            Ok(PlanInput {
                path: fields[0].to_owned(),
                start_line: fields[1]
                    .parse()
                    .map_err(|_| format!("{context}: invalid source start line {:?}", fields[1]))?,
                end_line: fields[2]
                    .parse()
                    .map_err(|_| format!("{context}: invalid source end line {:?}", fields[2]))?,
                ordinal,
                kind: PlanStatementKind::parse(fields[4])?,
                result_artifacts: artifacts,
                restore: hex_decode(fields[6]).map_err(|error| format!("{context}: {error}"))?,
            })
        })
        .collect()
}

fn verify_oracle_inventory(root: &Path, records: &[GoldenRecord]) -> Result<(), String> {
    let inventory = read_inventory(root)?;
    if inventory.len() != records.len() {
        return Err(format!(
            "parser oracle is stale: inventory has {} inputs but oracle has {}; regenerate integration_parser_golden first",
            inventory.len(), records.len()
        ));
    }
    for (index, (input, record)) in inventory.iter().zip(records).enumerate() {
        if input != &record.input {
            return Err(format!(
                "parser oracle is stale at input {index} ({}:{}-{}); regenerate integration_parser_golden first",
                input.path, input.start_line, input.end_line
            ));
        }
    }
    Ok(())
}

fn check(root: &Path) -> Result<(), String> {
    let records = read_golden(root)?;
    verify_oracle_inventory(root, &records)?;
    let expected = collect(root, &records)?;
    let actual = read_manifest(root)?;
    if actual != expected {
        return Err(
            "plan manifest is stale; regenerate with `cd rust && cargo run -p difftest --bin integration_plan_inventory -- --write`"
                .to_owned(),
        );
    }
    let mut counts = [0usize; 4];
    for entry in &actual {
        counts[match entry.kind {
            PlanStatementKind::Explain => 0,
            PlanStatementKind::ExplainAnalyze => 1,
            PlanStatementKind::ExplainForConnection => 2,
            PlanStatementKind::ExplainExplore => 3,
        }] += 1;
    }
    println!(
        "plan inventory current: explain={} explain_analyze={} explain_for_connection={} explain_explore={} total={}",
        counts[0], counts[1], counts[2], counts[3], actual.len()
    );
    Ok(())
}

fn write(root: &Path) -> Result<(), String> {
    let records = read_golden(root)?;
    verify_oracle_inventory(root, &records)?;
    let path = manifest_path(root);
    fs::write(&path, render(&collect(root, &records)?))
        .map_err(|error| format!("write {}: {error}", path.display()))?;
    check(root)
}

fn main() {
    let arguments: Vec<_> = env::args().skip(1).collect();
    let root = repo_root();
    let result = match arguments.as_slice() {
        [command] if command == "--write" => write(&root),
        [command] if command == "--check" || command == "--summary" => check(&root),
        _ => Err("usage: integration_plan_inventory [--write|--check|--summary]".to_owned()),
    };
    if let Err(error) = result {
        eprintln!("error: {error}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::{statement_kind, PlanStatementKind};

    #[test]
    fn classify_canonical_go_explain_restores() {
        assert_eq!(
            statement_kind(b"EXPLAIN FORMAT = 'row' SELECT 1"),
            Some(PlanStatementKind::Explain)
        );
        assert_eq!(
            statement_kind(b"EXPLAIN ANALYZE FORMAT = 'brief' SELECT 1"),
            Some(PlanStatementKind::ExplainAnalyze)
        );
        assert_eq!(
            statement_kind(b"EXPLAIN FORMAT = 'row' FOR CONNECTION 1"),
            Some(PlanStatementKind::ExplainForConnection)
        );
        assert_eq!(
            statement_kind(b"EXPLAIN EXPLORE 'digest'"),
            Some(PlanStatementKind::ExplainExplore)
        );
        assert_eq!(statement_kind(b"PLAN REPLAYER DUMP EXPLAIN SELECT 1"), None);
    }
}
