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

//! Generates and verifies the rewrite's upstream-test obligation ledger.
//!
//! The ledger is deliberately an inventory, not a claim of parity.  Every Go
//! test entry point, AST-discovered fixture/file access (including unresolved
//! helper paths), every Bazel/Make test target, every SQL fixture pair under
//! `tests/**/{t,r}/`, and every support artifact below a repository
//! `test`/`tests` suite must be visible
//! before its port can be scheduled. This includes runner scripts without a
//! `.sh` suffix, configs, certificates, stats, and data consumed by the
//! original core and deferred external-tool suites.
//! A source change that adds or removes an upstream test therefore makes
//! `--check` fail until the checked-in ledger is regenerated and triaged by a
//! human.
//!
//! ```text
//! cd rust
//! cargo run -j 12 -p difftest --bin go_test_ledger -- --write
//! cargo run -j 12 -p difftest --bin go_test_ledger -- --check
//! cargo run -j 12 -p difftest --bin go_test_ledger -- --summary
//! cargo run -j 12 -p difftest --bin go_test_ledger -- --queue result
//! cargo run -j 12 -p difftest --bin go_test_ledger -- --queue result package
//! ```

use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::Command;

#[path = "../evidence_fragments.rs"]
mod evidence_fragments;

use evidence_fragments::{sorted_tsv_files, validate_fragment_owner};

const LEDGER_RELATIVE_PATH: &str = "rust/difftests/corpus/coverage/go_test_inventory.tsv";
const GO_DECLARATION_INVENTORY_RELATIVE_PATH: &str =
    "rust/difftests/corpus/coverage/go_test_declaration_inventory.tsv";
const GO_FIXTURE_ACCESS_INVENTORY_RELATIVE_PATH: &str =
    "rust/difftests/corpus/coverage/go_test_fixture_access_inventory.tsv";
const EVIDENCE_DIRECTORY_RELATIVE_PATH: &str = "rust/difftests/corpus/coverage/evidence/tests";
const TEST_DOMAIN_MANIFEST_RELATIVE_PATH: &str =
    "rust/difftests/corpus/coverage/go_test_domain_manifest.tsv";
const RINGS: [&str; 6] = [
    "parser",
    "plan",
    "result",
    "transaction",
    "deferred-external",
    "unassigned",
];

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct Entry {
    kind: &'static str,
    path: String,
    line: usize,
    name: String,
    ring: &'static str,
}

/// One source-controlled Go AST declaration record. This is intentionally
/// broader than runnable `go test` entry points: every function declaration in
/// every upstream `*_test.go` file is visible, including helpers, methods, and
/// invalid test-like declarations. The ledger only promotes valid runner
/// declarations to `go_test` obligations.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct GoTestDeclaration {
    path: String,
    line: usize,
    column: usize,
    receiver: String,
    name: String,
    category: String,
    valid_runner_signature: bool,
}

/// One source-controlled AST access record from upstream test source. A
/// non-empty `resolved_literal_path` means exactly one direct string literal
/// was lexically resolved relative to the Go source file. Empty does *not*
/// mean absent: it is an explicit helper/join/pattern/escape obligation.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct GoTestFixtureAccess {
    path: String,
    line: usize,
    api: String,
    expression: String,
    resolved_literal_path: String,
}

impl GoTestDeclaration {
    fn is_runner_entrypoint(&self) -> bool {
        self.valid_runner_signature
            && matches!(
                self.category.as_str(),
                "Test" | "Benchmark" | "Fuzz" | "Example" | "TestMain"
            )
    }

    fn is_test_hook(&self) -> bool {
        self.category == "test_hook"
    }
}

/// Evidence attached to one upstream source row. The overlay is deliberately
/// sparse: a missing row means `UNTRIAGED`, never an implicit coverage claim.
#[derive(Clone, Debug)]
struct Evidence {
    status: String,
    owner: String,
    artifact: String,
    note: String,
}

/// A deliberate ownership split inside one otherwise shared Go test file.
///
/// The anchor is the original top-level test declaration, never a guessed
/// range. A file without a manifest row remains one indivisible queue unit.
#[derive(Clone, Debug)]
struct TestDomain {
    name: String,
}

impl Entry {
    fn render(&self, evidence: Option<&Evidence>) -> String {
        let (status, owner, artifact, note) =
            evidence.map_or(("UNTRIAGED", "-", "-", "-"), |value| {
                (
                    value.status.as_str(),
                    value.owner.as_str(),
                    value.artifact.as_str(),
                    value.note.as_str(),
                )
            });
        format!(
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
            self.kind, self.path, self.line, self.name, self.ring, status, owner, artifact, note
        )
    }
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("difftest must live at rust/difftests")
        .to_path_buf()
}

fn is_ignored_dir(name: &str) -> bool {
    matches!(name, ".git" | "target" | "rust" | "vendor" | "node_modules")
}

fn walk(root: &Path, current: &Path, files: &mut Vec<PathBuf>) -> io::Result<()> {
    for item in fs::read_dir(current)? {
        let item = item?;
        let path = item.path();
        let file_type = item.file_type()?;
        if file_type.is_dir() {
            if current == root && is_ignored_dir(&item.file_name().to_string_lossy()) {
                continue;
            }
            if item.file_name() == ".git" || item.file_name() == "target" {
                continue;
            }
            walk(root, &path, files)?;
        } else if file_type.is_file() {
            files.push(path);
        }
    }
    Ok(())
}

fn render_go_test_declaration_inventory(declarations: &[GoTestDeclaration]) -> String {
    let mut text = String::from(
        "# Generated by go run -p=12 ./rust/difftests/tools/go_test_declaration_inventory --root .\n",
    );
    text.push_str("# source_path\tsource_line\tsource_column\treceiver\tfunction_name\tcategory\tvalid_runner_signature\n");
    for declaration in declarations {
        text.push_str(&format!(
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
            declaration.path,
            declaration.line,
            declaration.column,
            declaration.receiver,
            declaration.name,
            declaration.category,
            declaration.valid_runner_signature,
        ));
    }
    text
}

fn render_go_test_fixture_access_inventory(accesses: &[GoTestFixtureAccess]) -> String {
    let mut text = String::from(
        "# Generated by go run -p=12 ./rust/difftests/tools/go_test_fixture_inventory --root .\n",
    );
    text.push_str("# source_path\tsource_line\tapi\texpression\tresolved_literal_path\n");
    for access in accesses {
        text.push_str(&format!(
            "{}\t{}\t{}\t{}\t{}\n",
            access.path, access.line, access.api, access.expression, access.resolved_literal_path,
        ));
    }
    text
}

fn parse_go_test_declaration_inventory(
    path: &Path,
    source: &str,
) -> Result<Vec<GoTestDeclaration>, String> {
    let mut declarations = Vec::new();
    let mut seen = BTreeSet::new();
    for (index, line) in source.lines().enumerate() {
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let fields: Vec<_> = line.split('\t').collect();
        if fields.len() != 7 {
            return Err(format!(
                "{}:{}: expected 7 tab-separated fields",
                path.display(),
                index + 1
            ));
        }
        let line_number = fields[1].parse::<usize>().map_err(|error| {
            format!(
                "{}:{}: invalid source line {:?}: {error}",
                path.display(),
                index + 1,
                fields[1]
            )
        })?;
        let column = fields[2].parse::<usize>().map_err(|error| {
            format!(
                "{}:{}: invalid source column {:?}: {error}",
                path.display(),
                index + 1,
                fields[2]
            )
        })?;
        if fields[0].is_empty() || line_number == 0 || column == 0 {
            return Err(format!(
                "{}:{}: source path, line, and column must be non-empty and positive",
                path.display(),
                index + 1
            ));
        }
        if !matches!(fields[3], "function" | "method") {
            return Err(format!(
                "{}:{}: receiver must be function or method, got {:?}",
                path.display(),
                index + 1,
                fields[3]
            ));
        }
        if !matches!(
            fields[5],
            "Test" | "Benchmark" | "Fuzz" | "Example" | "TestMain" | "test_hook" | "other"
        ) {
            return Err(format!(
                "{}:{}: unknown declaration category {:?}",
                path.display(),
                index + 1,
                fields[5]
            ));
        }
        let valid_runner_signature = fields[6].parse::<bool>().map_err(|error| {
            format!(
                "{}:{}: valid_runner_signature must be true or false, got {:?}: {error}",
                path.display(),
                index + 1,
                fields[6]
            )
        })?;
        let declaration = GoTestDeclaration {
            path: fields[0].to_owned(),
            line: line_number,
            column,
            receiver: fields[3].to_owned(),
            name: fields[4].to_owned(),
            category: fields[5].to_owned(),
            valid_runner_signature,
        };
        if declaration.name.is_empty() || !seen.insert(declaration.clone()) {
            return Err(format!(
                "{}:{}: declaration must have a name and be unique",
                path.display(),
                index + 1
            ));
        }
        declarations.push(declaration);
    }
    if declarations.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(format!(
            "{}: declarations must be strictly sorted by source position",
            path.display()
        ));
    }
    Ok(declarations)
}

fn parse_go_test_fixture_access_inventory(
    path: &Path,
    source: &str,
) -> Result<Vec<GoTestFixtureAccess>, String> {
    let mut accesses = Vec::new();
    let mut seen = BTreeSet::new();
    for (index, line) in source.lines().enumerate() {
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let fields: Vec<_> = line.split('\t').collect();
        if fields.len() != 5 {
            return Err(format!(
                "{}:{}: expected 5 tab-separated fields",
                path.display(),
                index + 1
            ));
        }
        let line_number = fields[1].parse::<usize>().map_err(|error| {
            format!(
                "{}:{}: invalid source line {:?}: {error}",
                path.display(),
                index + 1,
                fields[1]
            )
        })?;
        if fields[0].is_empty() || line_number == 0 || fields[2].is_empty() || fields[3].is_empty()
        {
            return Err(format!(
                "{}:{}: source path, line, API, and expression must be non-empty",
                path.display(),
                index + 1
            ));
        }
        if !matches!(
            fields[2],
            "go:embed" | "os.ReadFile" | "os.Open" | "os.OpenFile" | "os.Stat" | "os.ReadDir"
        ) {
            return Err(format!(
                "{}:{}: unsupported fixture API {:?}",
                path.display(),
                index + 1,
                fields[2]
            ));
        }
        let access = GoTestFixtureAccess {
            path: fields[0].to_owned(),
            line: line_number,
            api: fields[2].to_owned(),
            expression: fields[3].to_owned(),
            resolved_literal_path: fields[4].to_owned(),
        };
        if !seen.insert(access.clone()) {
            return Err(format!(
                "{}:{}: fixture access must be unique",
                path.display(),
                index + 1
            ));
        }
        accesses.push(access);
    }
    if accesses.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(format!(
            "{}: fixture accesses must be strictly sorted by source position",
            path.display()
        ));
    }
    Ok(accesses)
}

/// Runs the source-controlled Go AST extractor. It only parses source: no
/// package loading, dependency resolution, build-tag filtering, or test
/// execution occurs. `-p=12` preserves the repository-wide Go tool parallelism
/// convention for the small compile step performed by `go run`.
fn generate_go_test_declaration_inventory(
    repository_root: &Path,
    scan_root: &Path,
) -> Result<Vec<GoTestDeclaration>, String> {
    let output = Command::new("go")
        .args([
            "run",
            "-p=12",
            "./rust/difftests/tools/go_test_declaration_inventory",
            "--root",
        ])
        .arg(scan_root)
        .current_dir(repository_root)
        .output()
        .map_err(|error| format!("run Go AST declaration extractor: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "Go AST declaration extractor failed:\n{}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    parse_go_test_declaration_inventory(
        &repository_root.join("rust/difftests/tools/go_test_declaration_inventory/main.go"),
        &String::from_utf8_lossy(&output.stdout),
    )
}

fn checked_go_test_declaration_inventory(
    root: &Path,
    mode: &str,
) -> Result<Vec<GoTestDeclaration>, String> {
    let generated = generate_go_test_declaration_inventory(root, root)?;
    let want = render_go_test_declaration_inventory(&generated);
    let inventory = root.join(GO_DECLARATION_INVENTORY_RELATIVE_PATH);
    if mode == "--write" {
        fs::create_dir_all(inventory.parent().expect("inventory path has a parent"))
            .map_err(|error| format!("create declaration inventory directory: {error}"))?;
        fs::write(&inventory, &want)
            .map_err(|error| format!("write {}: {error}", inventory.display()))?;
        return Ok(generated);
    }
    let got = fs::read_to_string(&inventory).map_err(|error| {
        format!(
            "cannot read {} ({error}); generate it with cargo run -p difftest --bin go_test_ledger -- --write",
            inventory.display()
        )
    })?;
    if got != want {
        return Err(format!(
            "{} is stale: the upstream Go AST declaration surface changed. Regenerate the ledger, then triage the new entries before assigning ports.",
            inventory.display()
        ));
    }
    parse_go_test_declaration_inventory(&inventory, &got)
}

/// Runs the source-controlled Go AST fixture-access extractor. Like the
/// declaration inventory, it intentionally does no package loading or test
/// execution, so build-tagged upstream source cannot fall outside the ledger.
fn generate_go_test_fixture_access_inventory(
    repository_root: &Path,
    scan_root: &Path,
) -> Result<Vec<GoTestFixtureAccess>, String> {
    let output = Command::new("go")
        .args([
            "run",
            "-p=12",
            "./rust/difftests/tools/go_test_fixture_inventory",
            "--root",
        ])
        .arg(scan_root)
        .current_dir(repository_root)
        .output()
        .map_err(|error| format!("run Go AST fixture-access extractor: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "Go AST fixture-access extractor failed:\n{}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    parse_go_test_fixture_access_inventory(
        &repository_root.join("rust/difftests/tools/go_test_fixture_inventory/main.go"),
        &String::from_utf8_lossy(&output.stdout),
    )
}

fn checked_go_test_fixture_access_inventory(
    root: &Path,
    mode: &str,
) -> Result<Vec<GoTestFixtureAccess>, String> {
    let generated = generate_go_test_fixture_access_inventory(root, root)?;
    let want = render_go_test_fixture_access_inventory(&generated);
    let inventory = root.join(GO_FIXTURE_ACCESS_INVENTORY_RELATIVE_PATH);
    if mode == "--write" {
        fs::create_dir_all(inventory.parent().expect("inventory path has a parent"))
            .map_err(|error| format!("create fixture-access inventory directory: {error}"))?;
        fs::write(&inventory, &want)
            .map_err(|error| format!("write {}: {error}", inventory.display()))?;
        return Ok(generated);
    }
    let got = fs::read_to_string(&inventory).map_err(|error| {
        format!(
            "cannot read {} ({error}); generate it with cargo run -p difftest --bin go_test_ledger -- --write",
            inventory.display()
        )
    })?;
    if got != want {
        return Err(format!(
            "{} is stale: the upstream Go AST fixture-access surface changed. Regenerate the ledger, then triage the new entries before assigning ports.",
            inventory.display()
        ));
    }
    parse_go_test_fixture_access_inventory(&inventory, &got)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GoLexState {
    Code,
    BlockComment,
    RawString,
}

/// Produces source text with comments removed while retaining string literals.
/// The test-run scanner needs literals to distinguish a stable child name from
/// a table-driven expression, whereas brace accounting must ignore them.
fn uncomment_go_line(line: &str, state: &mut GoLexState) -> String {
    let bytes = line.as_bytes();
    let mut output = String::with_capacity(line.len());
    let mut index = 0;
    while index < bytes.len() {
        match *state {
            GoLexState::BlockComment => {
                if bytes[index..].starts_with(b"*/") {
                    *state = GoLexState::Code;
                    output.push_str("  ");
                    index += 2;
                } else {
                    output.push(' ');
                    index += 1;
                }
            }
            GoLexState::RawString => {
                let byte = bytes[index];
                output.push(byte as char);
                index += 1;
                if byte == b'`' {
                    *state = GoLexState::Code;
                }
            }
            GoLexState::Code => {
                if bytes[index..].starts_with(b"//") {
                    output.extend(std::iter::repeat_n(' ', bytes.len() - index));
                    break;
                }
                if bytes[index..].starts_with(b"/*") {
                    *state = GoLexState::BlockComment;
                    output.push_str("  ");
                    index += 2;
                    continue;
                }
                let byte = bytes[index];
                output.push(byte as char);
                index += 1;
                if byte == b'`' {
                    *state = GoLexState::RawString;
                }
            }
        }
    }
    output
}

/// Counts braces after excluding comments and literals. This is intentionally
/// lexical rather than a partial Go parser: it is only used to keep a `t.Run`
/// call attached to its enclosing top-level test function.
fn go_brace_delta(line: &str, state: &mut GoLexState) -> isize {
    let bytes = line.as_bytes();
    let mut delta = 0;
    let mut index = 0;
    while index < bytes.len() {
        match *state {
            GoLexState::BlockComment => {
                if bytes[index..].starts_with(b"*/") {
                    *state = GoLexState::Code;
                    index += 2;
                } else {
                    index += 1;
                }
            }
            GoLexState::RawString => {
                if bytes[index] == b'`' {
                    *state = GoLexState::Code;
                }
                index += 1;
            }
            GoLexState::Code => {
                if bytes[index..].starts_with(b"//") {
                    break;
                }
                if bytes[index..].starts_with(b"/*") {
                    *state = GoLexState::BlockComment;
                    index += 2;
                    continue;
                }
                match bytes[index] {
                    b'`' => {
                        *state = GoLexState::RawString;
                        index += 1;
                    }
                    b'\'' | b'"' => {
                        let quote = bytes[index];
                        index += 1;
                        while index < bytes.len() {
                            if bytes[index] == b'\\' {
                                index += 2;
                            } else if bytes[index] == quote {
                                index += 1;
                                break;
                            } else {
                                index += 1;
                            }
                        }
                    }
                    b'{' => {
                        delta += 1;
                        index += 1;
                    }
                    b'}' => {
                        delta -= 1;
                        index += 1;
                    }
                    _ => index += 1,
                }
            }
        }
    }
    delta
}

fn decode_go_string(input: &str) -> Option<(String, usize)> {
    let bytes = input.as_bytes();
    let quote = *bytes.first()?;
    if quote == b'`' {
        let end = input[1..].find('`')? + 1;
        return Some((input[1..end].to_owned(), end + 1));
    }
    if quote != b'"' {
        return None;
    }
    let mut decoded = String::new();
    let mut index = 1;
    while index < bytes.len() {
        match bytes[index] {
            b'"' => return Some((decoded, index + 1)),
            b'\\' if index + 1 < bytes.len() => {
                let escaped = bytes[index + 1];
                decoded.push(match escaped {
                    b'n' => '\n',
                    b'r' => '\r',
                    b't' => '\t',
                    b'\\' => '\\',
                    b'"' => '"',
                    value => value as char,
                });
                index += 2;
            }
            value => {
                decoded.push(value as char);
                index += 1;
            }
        }
    }
    None
}

/// Finds every direct `t.Run` invocation on one comment-free source line.
/// A string literal first argument is a static child obligation. Any other
/// expression is an explicit generator obligation; table-driven subtests are
/// never silently folded into their parent test.
fn test_run_names(line: &str) -> Vec<Option<String>> {
    let mut names = Vec::new();
    let bytes = line.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'`' => {
                index += 1;
                while index < bytes.len() && bytes[index] != b'`' {
                    index += 1;
                }
                index += usize::from(index < bytes.len());
            }
            b'\'' | b'"' => {
                let quote = bytes[index];
                index += 1;
                while index < bytes.len() {
                    if bytes[index] == b'\\' {
                        index += 2;
                    } else if bytes[index] == quote {
                        index += 1;
                        break;
                    } else {
                        index += 1;
                    }
                }
            }
            _ if bytes[index..].starts_with(b"t.Run")
                && (index == 0
                    || !(bytes[index - 1].is_ascii_alphanumeric() || bytes[index - 1] == b'_')) =>
            {
                let after_name = &line[index + "t.Run".len()..];
                let after_space = after_name.trim_start();
                if let Some(arguments) = after_space.strip_prefix('(') {
                    let first = arguments.trim_start();
                    names.push(decode_go_string(first).map(|(name, _)| name));
                }
                index += "t.Run".len();
            }
            _ => index += 1,
        }
    }
    names
}

/// Discovers explicit `t.Run` children under executable top-level Go test
/// functions. `go test` may synthesize dynamic names at runtime; those remain
/// visible here as `go_test_generator` rows anchored to the call site.
fn go_test_subtest_entries(
    path: &str,
    source: &str,
    parent_tests: &BTreeMap<usize, String>,
    ring: &'static str,
) -> Vec<Entry> {
    let mut entries = Vec::new();
    let mut comment_state = GoLexState::Code;
    let mut brace_state = GoLexState::Code;
    let mut depth = 0isize;
    let mut active: Option<(String, isize)> = None;

    for (index, raw_line) in source.lines().enumerate() {
        let line_number = index + 1;
        let uncommented = uncomment_go_line(raw_line, &mut comment_state);
        if active.is_none() && depth == 0 {
            if let Some(name) = parent_tests.get(&line_number) {
                active = Some((name.clone(), depth));
            }
        }
        if let Some((parent, _)) = &active {
            for name in test_run_names(&uncommented) {
                entries.push(Entry {
                    kind: if name.is_some() {
                        "go_test_subtest"
                    } else {
                        "go_test_generator"
                    },
                    path: path.to_owned(),
                    line: line_number,
                    name: name.map_or_else(
                        || format!("{parent}/<generated>"),
                        |child| format!("{parent}/{child}"),
                    ),
                    ring,
                });
            }
        }
        depth += go_brace_delta(raw_line, &mut brace_state);
        if let Some((_, base_depth)) = &active {
            if depth <= *base_depth {
                active = None;
            }
        }
    }
    entries
}

const BAZEL_TEST_RULES: [&str; 7] = [
    "go_test",
    "sh_test",
    "py_test",
    "cc_test",
    "java_test",
    "rust_test",
    "test_suite",
];

/// Returns exact Bazel test target definitions as `(source line, rule, name)`.
///
/// TiDB currently declares only `go_test`, but recognizing Bazel's other
/// concrete test rules prevents a new runner language from becoming invisible.
/// Macro definitions and arbitrary names containing "test" are deliberately
/// excluded: only executable test rules are obligations.
fn bazel_test_targets(source: &str) -> Vec<(usize, &'static str, String)> {
    let mut active = None;
    let mut targets = Vec::new();
    for (index, line) in source.lines().enumerate() {
        let trimmed = line.trim();
        if active.is_none() {
            active = BAZEL_TEST_RULES
                .into_iter()
                .find_map(|rule| (trimmed == format!("{rule}(")).then_some((index + 1, rule)));
            continue;
        }
        let Some((line_number, rule)) = active else {
            continue;
        };
        if let Some(name) = quoted_assignment(trimmed, "name") {
            targets.push((line_number, rule, name.to_owned()));
            active = None;
        } else if trimmed == ")" {
            // A computed/missing name is not safe to invent. It remains an
            // audit exclusion until the rule can be resolved structurally.
            active = None;
        }
    }
    targets
}

fn quoted_assignment<'a>(line: &'a str, key: &str) -> Option<&'a str> {
    let rest = line.strip_prefix(key)?.trim_start();
    let rest = rest.strip_prefix('=')?.trim_start().strip_prefix('"')?;
    rest.split_once('"').map(|(value, _)| value)
}

/// A Make target whose name structurally identifies test execution or its
/// lifecycle. Path targets such as `tools/bin/gotestsum` are build artifacts,
/// not test entry mechanisms, and are excluded.
fn is_make_test_target(name: &str) -> bool {
    !name.is_empty()
        && !name.contains('/')
        && !name.contains('$')
        && !name.starts_with('.')
        && name.to_ascii_lowercase().contains("test")
}

/// Returns unique Make test target definitions, anchored at their first
/// declaration. Make commonly splits prerequisites and recipes across two
/// declarations; that remains one executable source unit.
fn make_test_targets(source: &str) -> Vec<(usize, String)> {
    let mut targets = BTreeMap::new();
    for (index, line) in source.lines().enumerate() {
        if line.starts_with(char::is_whitespace) || line.starts_with('#') {
            continue;
        }
        let Some((names, _)) = line.split_once(':') else {
            continue;
        };
        if names.contains('=') {
            continue;
        }
        for name in names
            .split_whitespace()
            .filter(|name| is_make_test_target(name))
        {
            targets.entry(name.to_owned()).or_insert(index + 1);
        }
    }
    targets
        .into_iter()
        .map(|(name, line)| (line, name))
        .collect()
}

fn ring_for(path: &str) -> &'static str {
    if path.starts_with("pkg/parser/") {
        "parser"
    // Bindings affect optimization before executor construction, and ranger
    // produces the ranges consumed by access-path selection. Keeping them in
    // the plan ring makes their Go-test obligations visible to the same team
    // that will compare EXPLAIN output and plan digests.
    } else if path.starts_with("pkg/planner/")
        || path.starts_with("pkg/bindinfo/")
        || path.starts_with("pkg/util/ranger/")
    {
        "plan"
    } else if path.starts_with("pkg/kv/")
        || path.starts_with("pkg/store/")
        || path.starts_with("pkg/meta/")
        || path.starts_with("pkg/tablecodec/")
        || path.starts_with("pkg/util/codec/")
        || path.starts_with("pkg/util/rowcodec/")
        || path.starts_with("tests/realtikvtest/")
    {
        "transaction"
    } else if path.starts_with("tests/")
        || path.starts_with("pkg/expression/")
        || path.starts_with("pkg/executor/")
        || path.starts_with("pkg/session/")
        || path.starts_with("pkg/ddl/")
        || path.starts_with("tests/integrationtest/")
        || path.starts_with("tests/integrationtest2/")
        || path.starts_with("tests/clusterintegrationtest/")
    {
        "result"
    // The design explicitly leaves BR, Lightning, and Dumpling as independent
    // Go binaries during the SQL-node rewrite. Their package-local support
    // code is deferred with those binaries rather than being mixed into the
    // core SQL-node backlog. This is a routing category, not a coverage
    // status: every entry remains in the ledger and starts UNTRIAGED.
    } else if path.starts_with("br/")
        || path.starts_with("lightning/")
        || path.starts_with("dumpling/")
        || path.starts_with("pkg/lightning/")
        || path.starts_with("pkg/importsdk/")
        || path.starts_with("pkg/dumpformat/")
    {
        "deferred-external"
    } else {
        "unassigned"
    }
}

/// Whether a repository path is a SQL test input rather than a recorded
/// result. TiDB has more than one SQL test suite, all following the `t/*.test`
/// convention; do not tie the obligation inventory to just one suite.
fn is_sql_fixture(path: &str) -> bool {
    path.starts_with("tests/") && path.contains("/t/") && path.ends_with(".test")
}

/// Whether a repository path is an expected result for a SQL test input.
///
/// The input and its expected output are one compatibility contract. Keeping
/// only `t/*.test` visible leaves an agent free to port the SQL while silently
/// losing the original assertion. Result files have their own ledger rows so
/// ownership and coverage evidence cannot skip that half of the contract.
fn is_sql_result(path: &str) -> bool {
    path.starts_with("tests/") && path.contains("/r/") && path.ends_with(".result")
}

/// Return the result-file stem belonging to a SQL input. Most fixtures have
/// exactly this stem plus `.result`; environment variants naturally append an
/// underscore suffix (for example `collation_misc_enabled.result`).
fn expected_result_stem(input: &str) -> Option<String> {
    let input = input.strip_suffix(".test")?;
    let (suite, name) = input.split_once("/t/")?;
    Some(format!("{suite}/r/{name}"))
}

fn result_matches_input(result: &str, input: &str) -> bool {
    let Some(result) = result.strip_suffix(".result") else {
        return false;
    };
    let Some(stem) = expected_result_stem(input) else {
        return false;
    };
    result == stem
        || result
            .strip_prefix(&stem)
            .is_some_and(|suffix| suffix.starts_with('_'))
}

/// Resolve one result to its input fixture. Exact stems take precedence over a
/// suffix-family match: if both `foo.test` and `foo_enabled.test` exist, then
/// `foo_enabled.result` belongs to the latter. A result with no exact stem may
/// still be the environment-specific output of one base fixture.
fn result_owners<'a>(result: &Entry, inputs: &[&'a Entry]) -> Vec<&'a Entry> {
    let exact: Vec<_> = inputs
        .iter()
        .copied()
        .filter(|input| {
            let Some(result_stem) = result.path.strip_suffix(".result") else {
                return false;
            };
            expected_result_stem(&input.path).as_deref() == Some(result_stem)
        })
        .collect();
    if !exact.is_empty() {
        return exact;
    }
    inputs
        .iter()
        .copied()
        .filter(|input| result_matches_input(&result.path, &input.path))
        .collect()
}

/// Validate the bidirectional source contract between `t/*.test` and
/// `r/*.result`. A fixture may have several result variants, but neither an
/// input without any expected result nor an orphan expected result is allowed
/// to disappear into the porting queue.
fn validate_sql_fixture_pairs(entries: &BTreeSet<Entry>) -> Result<(), String> {
    let inputs: Vec<_> = entries
        .iter()
        .filter(|entry| entry.kind == "integration_fixture")
        .collect();
    let results: Vec<_> = entries
        .iter()
        .filter(|entry| entry.kind == "integration_result")
        .collect();

    let mut covered_inputs = BTreeSet::new();
    let mut orphan_results = Vec::new();
    let mut ambiguous_results = Vec::new();
    for result in &results {
        let owners = result_owners(result, &inputs);
        match owners.as_slice() {
            [] => orphan_results.push(result.path.as_str()),
            [owner] => {
                covered_inputs.insert(owner.path.as_str());
            }
            _ => ambiguous_results.push(result.path.as_str()),
        }
    }
    let missing_results: Vec<_> = inputs
        .iter()
        .filter(|input| !covered_inputs.contains(input.path.as_str()))
        .map(|input| input.path.as_str())
        .collect();

    if missing_results.is_empty() && orphan_results.is_empty() && ambiguous_results.is_empty() {
        return Ok(());
    }

    let mut error = String::from("SQL fixture input/result inventory is incomplete");
    if !missing_results.is_empty() {
        error.push_str("\ninputs without expected results:");
        for input in missing_results {
            error.push_str("\n  ");
            error.push_str(input);
        }
    }
    if !orphan_results.is_empty() {
        error.push_str("\nresults without SQL inputs:");
        for result in orphan_results {
            error.push_str("\n  ");
            error.push_str(result);
        }
    }
    if !ambiguous_results.is_empty() {
        error.push_str("\nresults matching multiple SQL inputs:");
        for result in ambiguous_results {
            error.push_str("\n  ");
            error.push_str(result);
        }
    }
    Err(error)
}

/// Whether a source path is a checked-in test artifact, rather than test
/// implementation code. `testdata` is intentionally recognized at any
/// nesting depth: Go packages commonly keep it beside the package under
/// test, while integration suites may keep it below their suite directory.
///
/// One artifact gets one ledger entry even if a Go test happens to read it
/// through several helpers. Its ring is still derived from the owning path,
/// so a parser fixture cannot silently appear as an unassigned generic file.
fn is_testdata_artifact(path: &str) -> bool {
    path.split('/').any(|component| component == "testdata")
}

/// Whether a shell program belongs to an original test suite.
///
/// RealTiKV, cluster-upgrade, BR, Lightning, and Dumpling suites are
/// orchestrated by checked-in shell programs. The structural rule covers
/// shell files below a `test`/`tests` directory plus explicitly test-named
/// runners such as `build/jenkins_unit_test.sh`; no per-file exception list
/// is maintained.
fn is_shell_test(path: &str) -> bool {
    if !path.ends_with(".sh") {
        return false;
    }
    let filename = path.rsplit('/').next().unwrap_or(path);
    path.split('/')
        .any(|component| matches!(component, "test" | "tests"))
        || filename.to_ascii_lowercase().contains("test")
}

/// Whether a file is support material owned by an upstream test suite.
///
/// Test runners in this repository consume extensionless shell helpers,
/// Python programs, TOML configs, certificates, statistics JSON, compressed
/// data, and checked binaries. Restricting the inventory to source entrypoints
/// and `t`/`r` pairs made those contracts invisible. BUILD metadata and prose
/// are excluded because they describe or schedule tests rather than serving as
/// an input to the running test itself.
fn is_test_suite_artifact(path: &str) -> bool {
    let first = path.split('/').next().unwrap_or(path);
    if first.starts_with('.')
        || matches!(first, "docs" | "rust" | "vendor" | "node_modules")
        || !path
            .split('/')
            .any(|component| matches!(component, "test" | "tests"))
    {
        return false;
    }
    let filename = path.rsplit('/').next().unwrap_or(path);
    !matches!(filename, ".gitignore" | "BUILD.bazel" | "README.md")
}

fn relative(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .expect("walk only returns paths inside the repository")
        .to_string_lossy()
        .replace('\\', "/")
}

fn collect(
    root: &Path,
    declarations: &[GoTestDeclaration],
    fixture_accesses: &[GoTestFixtureAccess],
) -> io::Result<BTreeSet<Entry>> {
    let mut files = Vec::new();
    walk(root, root, &mut files)?;

    let mut entries = BTreeSet::new();
    for path in files {
        let rel = relative(root, &path);
        let is_go_test = rel.ends_with("_test.go");
        let is_fixture = is_sql_fixture(&rel);
        let is_result = is_sql_result(&rel);
        let is_testdata = is_testdata_artifact(&rel);
        let is_shell = is_shell_test(&rel);

        let filename = rel.rsplit('/').next().unwrap_or(&rel);
        if filename == "BUILD.bazel" || rel.ends_with(".bzl") {
            let source = fs::read_to_string(&path)?;
            for (line, _rule, name) in bazel_test_targets(&source) {
                entries.insert(Entry {
                    kind: "bazel_test_target",
                    path: rel.clone(),
                    line,
                    name,
                    ring: ring_for(&rel),
                });
            }
        }
        if filename == "Makefile" || rel.ends_with(".mk") {
            let source = fs::read_to_string(&path)?;
            for (line, name) in make_test_targets(&source) {
                entries.insert(Entry {
                    kind: "make_test_target",
                    path: rel.clone(),
                    line,
                    name,
                    ring: ring_for(&rel),
                });
            }
        }

        if is_go_test {
            entries.insert(Entry {
                kind: "go_test_file",
                path: rel.clone(),
                line: 0,
                name: rel
                    .rsplit('/')
                    .next()
                    .expect("relative Go test source has a basename")
                    .to_owned(),
                ring: ring_for(&rel),
            });
            let source = fs::read_to_string(&path)?;
            let parent_tests = declarations
                .iter()
                .filter(|declaration| declaration.path == rel && declaration.is_runner_entrypoint())
                .map(|declaration| (declaration.line, declaration.name.clone()))
                .collect::<BTreeMap<_, _>>();
            for declaration in declarations
                .iter()
                .filter(|declaration| declaration.path == rel)
            {
                if declaration.is_runner_entrypoint() {
                    entries.insert(Entry {
                        kind: "go_test",
                        path: rel.clone(),
                        line: declaration.line,
                        name: declaration.name.clone(),
                        ring: ring_for(&rel),
                    });
                }
                if declaration.is_test_hook() {
                    entries.insert(Entry {
                        kind: "go_test_hook",
                        path: rel.clone(),
                        line: declaration.line,
                        name: declaration.name.clone(),
                        ring: ring_for(&rel),
                    });
                }
            }
            for access in fixture_accesses.iter().filter(|access| access.path == rel) {
                if access.resolved_literal_path.is_empty() {
                    entries.insert(Entry {
                        kind: "go_test_fixture_unresolved",
                        path: rel.clone(),
                        line: access.line,
                        name: format!("{}: {}", access.api, access.expression),
                        ring: ring_for(&rel),
                    });
                } else {
                    // This source anchor is a declared local path, not a
                    // filesystem inference. Keep it even when a build tag or
                    // generated source means it is absent in this checkout.
                    entries.insert(Entry {
                        kind: "go_test_fixture",
                        path: rel.clone(),
                        line: access.line,
                        name: access.resolved_literal_path.clone(),
                        ring: ring_for(&rel),
                    });
                }
            }
            entries.extend(go_test_subtest_entries(
                &rel,
                &source,
                &parent_tests,
                ring_for(&rel),
            ));
        }
        // TiDB keeps SQL fixtures in more than one test suite. Restrict this
        // to the conventional `t/` input directory and `.test` sources (not
        // the paired `r/` outputs) so cluster-integration fixtures cannot be
        // silently omitted just because they are not in integrationtest.
        if is_fixture {
            entries.insert(Entry {
                kind: "integration_fixture",
                path: rel.clone(),
                line: 0,
                name: rel
                    .rsplit('/')
                    .next()
                    .expect("relative integration fixture has a basename")
                    .to_owned(),
                ring: "result",
            });
        }
        if is_result {
            entries.insert(Entry {
                kind: "integration_result",
                path: rel.clone(),
                line: 0,
                name: rel
                    .rsplit('/')
                    .next()
                    .expect("relative integration result has a basename")
                    .to_owned(),
                ring: "result",
            });
        }
        if is_testdata {
            entries.insert(Entry {
                kind: "testdata_artifact",
                path: rel.clone(),
                line: 0,
                name: rel
                    .rsplit('/')
                    .next()
                    .expect("testdata artifact has a basename")
                    .to_owned(),
                ring: ring_for(&rel),
            });
        }
        if is_shell {
            entries.insert(Entry {
                kind: "shell_test",
                path: rel.clone(),
                line: 0,
                name: rel
                    .rsplit('/')
                    .next()
                    .expect("relative shell test has a basename")
                    .to_owned(),
                ring: ring_for(&rel),
            });
        }
        if is_test_suite_artifact(&rel)
            && !is_go_test
            && !is_fixture
            && !is_result
            && !is_testdata
            && !is_shell
        {
            entries.insert(Entry {
                kind: "test_suite_artifact",
                path: rel.clone(),
                line: 0,
                name: rel
                    .rsplit('/')
                    .next()
                    .expect("test suite artifact has a basename")
                    .to_owned(),
                ring: ring_for(&rel),
            });
        }
    }
    Ok(entries)
}

fn static_kind(kind: &str) -> Option<&'static str> {
    match kind {
        "go_test_file" => Some("go_test_file"),
        "go_test" => Some("go_test"),
        "go_test_hook" => Some("go_test_hook"),
        "go_test_subtest" => Some("go_test_subtest"),
        "go_test_generator" => Some("go_test_generator"),
        "go_test_fixture" => Some("go_test_fixture"),
        "go_test_fixture_unresolved" => Some("go_test_fixture_unresolved"),
        "bazel_test_target" => Some("bazel_test_target"),
        "make_test_target" => Some("make_test_target"),
        "shell_test" => Some("shell_test"),
        "integration_fixture" => Some("integration_fixture"),
        "integration_result" => Some("integration_result"),
        "testdata_artifact" => Some("testdata_artifact"),
        "test_suite_artifact" => Some("test_suite_artifact"),
        _ => None,
    }
}

fn read_evidence(
    root: &Path,
    entries: &BTreeSet<Entry>,
) -> Result<BTreeMap<Entry, Evidence>, String> {
    let mut overlays = BTreeMap::new();
    let mut origins = BTreeMap::new();
    for path in sorted_tsv_files(root, EVIDENCE_DIRECTORY_RELATIVE_PATH)? {
        let source = fs::read_to_string(&path)
            .map_err(|error| format!("cannot read {}: {error}", path.display()))?;
        for (index, line) in source.lines().enumerate() {
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let fields: Vec<_> = line.split('\t').collect();
            if fields.len() != 8 {
                return Err(format!(
                    "{}:{}: expected 8 tab-separated fields",
                    path.display(),
                    index + 1
                ));
            }
            let line_number = fields[2].parse::<usize>().map_err(|error| {
                format!(
                    "{}:{}: invalid source line {:?}: {error}",
                    path.display(),
                    index + 1,
                    fields[2]
                )
            })?;
            let kind = static_kind(fields[0]).ok_or_else(|| {
                format!(
                    "{}:{}: unknown source kind {:?}",
                    path.display(),
                    index + 1,
                    fields[0]
                )
            })?;
            let key = Entry {
                kind,
                path: fields[1].to_owned(),
                line: line_number,
                name: fields[3].to_owned(),
                ring: "",
            };
            let entry = entries
                .iter()
                .find(|candidate| {
                    candidate.kind == key.kind
                        && candidate.path == key.path
                        && candidate.line == key.line
                        && candidate.name == key.name
                })
                .ok_or_else(|| {
                    format!(
                        "{}:{}: stale source anchor {}:{}:{}",
                        path.display(),
                        index + 1,
                        key.path,
                        key.line,
                        key.name
                    )
                })?
                .clone();
            if !matches!(fields[4], "PARTIAL" | "COVERED" | "BLOCKED") {
                return Err(format!(
                    "{}:{}: status must be PARTIAL, COVERED, or BLOCKED",
                    path.display(),
                    index + 1
                ));
            }
            if fields[5].is_empty() || fields[6].is_empty() || fields[7].is_empty() {
                return Err(format!(
                    "{}:{}: status, owner, artifact, and note must all be present",
                    path.display(),
                    index + 1
                ));
            }
            validate_fragment_owner(&path, fields[5])
                .map_err(|error| format!("{}:{}: {error}", path.display(), index + 1))?;
            let artifact = root.join(fields[6]);
            if !artifact.is_file() {
                return Err(format!(
                    "{}:{}: evidence artifact {} does not exist",
                    path.display(),
                    index + 1,
                    artifact.display()
                ));
            }
            let evidence = Evidence {
                status: fields[4].to_owned(),
                owner: fields[5].to_owned(),
                artifact: fields[6].to_owned(),
                note: fields[7].to_owned(),
            };
            if let Some(first_path) = origins.get(&entry) {
                return Err(format!(
                    "{}:{}: duplicate source anchor (first declared in {})",
                    path.display(),
                    index + 1,
                    first_path
                ));
            }
            origins.insert(entry.clone(), path.display().to_string());
            overlays.insert(entry, evidence);
        }
    }
    Ok(overlays)
}

fn valid_test_domain_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || matches!(character, '-' | '.'))
}

/// Reads the intentionally small manifest that opts a shared Go test file
/// into top-level-test ownership. Its rows are exact source anchors, so a
/// line shift in the original test cannot silently move work between agents.
fn read_test_domains(
    root: &Path,
    entries: &BTreeSet<Entry>,
) -> Result<BTreeMap<Entry, TestDomain>, String> {
    let path = root.join(TEST_DOMAIN_MANIFEST_RELATIVE_PATH);
    let source = fs::read_to_string(&path).map_err(|error| {
        format!(
            "cannot read checked test-domain manifest {}: {error}",
            path.display()
        )
    })?;
    let mut domains = BTreeMap::new();
    for (index, line) in source.lines().enumerate() {
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let fields: Vec<_> = line.split('\t').collect();
        if fields.len() != 4 {
            return Err(format!(
                "{}:{}: expected 4 tab-separated fields: source_path, source_line, test_name, test_domain",
                path.display(),
                index + 1
            ));
        }
        let line_number = fields[1].parse::<usize>().map_err(|error| {
            format!(
                "{}:{}: invalid source line {:?}: {error}",
                path.display(),
                index + 1,
                fields[1]
            )
        })?;
        if !valid_test_domain_name(fields[3]) {
            return Err(format!(
                "{}:{}: invalid test domain {:?}; use letters, digits, '-' or '.'",
                path.display(),
                index + 1,
                fields[3]
            ));
        }
        let anchor = entries
            .iter()
            .find(|entry| {
                entry.kind == "go_test"
                    && entry.path == fields[0]
                    && entry.line == line_number
                    && entry.name == fields[2]
            })
            .ok_or_else(|| {
                format!(
                    "{}:{}: stale test-domain anchor {}:{}:{}; only discovered top-level go_test entries may be split",
                    path.display(),
                    index + 1,
                    fields[0],
                    line_number,
                    fields[2]
                )
            })?
            .clone();
        if domains
            .insert(
                anchor.clone(),
                TestDomain {
                    name: fields[3].to_owned(),
                },
            )
            .is_some()
        {
            return Err(format!(
                "{}:{}: duplicate test-domain anchor {}:{}:{}",
                path.display(),
                index + 1,
                anchor.path,
                anchor.line,
                anchor.name
            ));
        }
    }
    Ok(domains)
}

/// A split file may leave a source test unassigned, but only while that
/// top-level test is visibly `UNTRIAGED`. Once evidence is attached, a domain
/// row is mandatory; otherwise one agent can mark a shared file covered while
/// another agent owns a different anchor in that same file.
fn validate_test_domain_partition(
    entries: &BTreeSet<Entry>,
    evidence: &BTreeMap<Entry, Evidence>,
    domains: &BTreeMap<Entry, TestDomain>,
) -> Result<(), String> {
    let split_paths: BTreeSet<_> = domains.keys().map(|entry| entry.path.as_str()).collect();
    for entry in entries {
        if entry.kind != "go_test" || !split_paths.contains(entry.path.as_str()) {
            continue;
        }
        if domains.contains_key(entry) {
            continue;
        }
        if let Some(value) = evidence.get(entry) {
            return Err(format!(
                "{}:{}:{} has {} evidence but no test-domain claim; add one exact manifest row or remove the evidence claim",
                entry.path, entry.line, entry.name, value.status
            ));
        }
    }
    Ok(())
}

fn rendered(entries: &BTreeSet<Entry>, evidence: &BTreeMap<Entry, Evidence>) -> String {
    let mut text =
        String::from("# Generated by cargo run -p difftest --bin go_test_ledger -- --write.\n");
    text.push_str("# kind\tsource_path\tsource_line\ttest_name\tdifferential_ring\tporting_status\towner\tevidence_artifact\tnote\n");
    for entry in entries {
        text.push_str(&entry.render(evidence.get(entry)));
        text.push('\n');
    }
    text
}

fn print_summary(entries: &BTreeSet<Entry>, evidence: &BTreeMap<Entry, Evidence>) {
    for kind in [
        "go_test_file",
        "go_test",
        "go_test_hook",
        "go_test_subtest",
        "go_test_generator",
        "go_test_fixture",
        "go_test_fixture_unresolved",
        "bazel_test_target",
        "make_test_target",
        "shell_test",
        "integration_fixture",
        "integration_result",
        "testdata_artifact",
        "test_suite_artifact",
    ] {
        let count = entries.iter().filter(|entry| entry.kind == kind).count();
        println!("{kind}: {count}");
    }
    for ring in RINGS {
        let count = entries.iter().filter(|entry| entry.ring == ring).count();
        println!("{ring}: {count}");
    }
    for status in ["UNTRIAGED", "PARTIAL", "COVERED", "BLOCKED"] {
        let count = if status == "UNTRIAGED" {
            entries.len() - evidence.len()
        } else {
            evidence
                .values()
                .filter(|value| value.status == status)
                .count()
        };
        println!("{status}: {count}");
    }
}

/// Coarse package aggregation for backlog summaries. This is deliberately not
/// the default dispatch unit: TiDB packages such as `pkg/util` and
/// `pkg/expression` are far too large for one parallel worker.
fn package_ownership_unit(path: &str) -> &str {
    let mut parts = path.split('/');
    match (parts.next(), parts.next()) {
        (Some("pkg"), Some(package)) => &path[.."pkg/".len() + package.len()],
        (Some("tests"), Some(suite)) => &path[.."tests/".len() + suite.len()],
        (Some(top), _) => top,
        _ => path,
    }
}

/// Exact, non-overlapping source unit assigned to one worker.
///
/// Go tests remain grouped by their source test file so a table-driven test and
/// its lifecycle hooks cannot be split between workers. SQL result variants
/// resolve back to their unique `t/*.test` owner, keeping the input and every
/// checked result in one unit. Testdata artifacts stay individually visible;
/// the worker that consumes one must attach evidence to that exact artifact.
fn split_test_file(domains: &BTreeMap<Entry, TestDomain>, path: &str) -> bool {
    domains.keys().any(|anchor| anchor.path == path)
}

fn top_level_test(entry: &Entry, entries: &BTreeSet<Entry>) -> Option<Entry> {
    let name = entry.name.split('/').next()?;
    entries
        .iter()
        .find(|candidate| {
            candidate.kind == "go_test" && candidate.path == entry.path && candidate.name == name
        })
        .cloned()
}

fn test_anchor_ownership_unit(anchor: &Entry, domains: &BTreeMap<Entry, TestDomain>) -> String {
    domains.get(anchor).map_or_else(
        || format!("{}:{}:{}", anchor.path, anchor.line, anchor.name),
        |domain| format!("domain:{}", domain.name),
    )
}

fn source_ownership_unit(
    entry: &Entry,
    entries: &BTreeSet<Entry>,
    domains: &BTreeMap<Entry, TestDomain>,
) -> String {
    if entry.kind == "integration_result" {
        let inputs: Vec<_> = entries
            .iter()
            .filter(|candidate| candidate.kind == "integration_fixture")
            .collect();
        let owners = result_owners(entry, &inputs);
        debug_assert_eq!(
            owners.len(),
            1,
            "fixture-pair validation must run before queue generation"
        );
        return owners
            .first()
            .map_or_else(|| entry.path.clone(), |owner| owner.path.clone());
    }
    if matches!(entry.kind, "bazel_test_target" | "make_test_target") {
        return format!("{}#{}", entry.path, entry.name);
    }
    if split_test_file(domains, &entry.path) {
        if let Some(anchor) = top_level_test(entry, entries) {
            return test_anchor_ownership_unit(&anchor, domains);
        }
        // File-level source, lifecycle hooks, and external fixtures remain a
        // visible shared-support obligation. They cannot be accidentally
        // claimed by one of the independently dispatched test domains.
        return format!("{}#shared-support", entry.path);
    }
    entry.path.clone()
}

fn is_queue_obligation(kind: &str) -> bool {
    matches!(
        kind,
        "go_test_file"
            | "go_test"
            | "go_test_hook"
            | "go_test_subtest"
            | "go_test_generator"
            | "go_test_fixture"
            | "go_test_fixture_unresolved"
            | "bazel_test_target"
            | "make_test_target"
            | "shell_test"
            | "integration_fixture"
            | "integration_result"
            | "testdata_artifact"
            | "test_suite_artifact"
    )
}

fn print_queue(
    entries: &BTreeSet<Entry>,
    evidence: &BTreeMap<Entry, Evidence>,
    domains: &BTreeMap<Entry, TestDomain>,
    ring: &str,
    granularity: &str,
) {
    let mut units = BTreeMap::<(String, String), usize>::new();
    for entry in entries {
        if entry.ring == ring && is_queue_obligation(entry.kind) {
            let status = evidence
                .get(entry)
                .map_or("UNTRIAGED", |value| value.status.as_str());
            let unit = if granularity == "package" {
                package_ownership_unit(&entry.path).to_owned()
            } else {
                source_ownership_unit(entry, entries, domains)
            };
            *units.entry((unit, status.to_owned())).or_default() += 1;
        }
    }
    if units.is_empty() {
        eprintln!("no work units found for ring {ring:?}");
        return;
    }
    for ((unit, status), count) in units {
        println!("{ring}\t{unit}\t{status}\t{count}");
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = env::args().skip(1);
    let mode = args.next().unwrap_or_else(|| "--check".to_owned());
    let queue_ring = if mode == "--queue" {
        Some(args.next().ok_or("--queue needs a differential ring")?)
    } else {
        None
    };
    let queue_granularity = if mode == "--queue" {
        args.next().unwrap_or_else(|| "source".to_owned())
    } else {
        "source".to_owned()
    };
    if !matches!(
        mode.as_str(),
        "--write" | "--check" | "--summary" | "--queue"
    ) {
        return Err(format!(
            "unknown mode {mode:?}; use --write, --check, --summary, or --queue <ring>"
        )
        .into());
    }
    if !matches!(queue_granularity.as_str(), "source" | "package") {
        return Err(format!(
            "unknown queue granularity {queue_granularity:?}; use source or package"
        )
        .into());
    }
    if let Some(ring) = &queue_ring {
        if !RINGS.contains(&ring.as_str()) {
            return Err(format!(
                "unknown differential ring {ring:?}; use one of {}",
                RINGS.join(", ")
            )
            .into());
        }
    }

    let root = repo_root();
    let declarations = checked_go_test_declaration_inventory(&root, &mode)
        .map_err(|error| format!("Go test declaration inventory invalid: {error}"))?;
    let fixture_accesses = checked_go_test_fixture_access_inventory(&root, &mode)
        .map_err(|error| format!("Go test fixture-access inventory invalid: {error}"))?;
    let entries = collect(&root, &declarations, &fixture_accesses)?;
    validate_sql_fixture_pairs(&entries)
        .map_err(|error| format!("SQL fixture inventory invalid: {error}"))?;
    let evidence = read_evidence(&root, &entries)
        .map_err(|error| format!("evidence overlay invalid: {error}"))?;
    let domains = read_test_domains(&root, &entries)
        .map_err(|error| format!("test-domain manifest invalid: {error}"))?;
    validate_test_domain_partition(&entries, &evidence, &domains)
        .map_err(|error| format!("test-domain partition invalid: {error}"))?;
    if mode == "--summary" {
        print_summary(&entries, &evidence);
        return Ok(());
    }
    if let Some(ring) = queue_ring {
        print_queue(&entries, &evidence, &domains, &ring, &queue_granularity);
        return Ok(());
    }

    let ledger = root.join(LEDGER_RELATIVE_PATH);
    let want = rendered(&entries, &evidence);
    if mode == "--write" {
        fs::create_dir_all(ledger.parent().expect("ledger path has a parent"))?;
        fs::write(&ledger, want)?;
        print_summary(&entries, &evidence);
        return Ok(());
    }

    let got = fs::read_to_string(&ledger).map_err(|error| {
        format!(
            "cannot read {} ({error}); generate it with cargo run -p difftest --bin go_test_ledger -- --write",
            ledger.display()
        )
    })?;
    if got != want {
        return Err(format!(
            "{} is stale: the upstream Go test surface changed. Regenerate the ledger, then triage the new entries before assigning ports.",
            ledger.display()
        )
        .into());
    }
    print_summary(&entries, &evidence);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        bazel_test_targets, collect, expected_result_stem, generate_go_test_declaration_inventory,
        generate_go_test_fixture_access_inventory, go_test_subtest_entries, is_make_test_target,
        is_queue_obligation, is_shell_test, is_sql_fixture, is_sql_result, is_test_suite_artifact,
        is_testdata_artifact, make_test_targets, package_ownership_unit, rendered, repo_root,
        result_matches_input, result_owners, ring_for, source_ownership_unit,
        validate_sql_fixture_pairs, validate_test_domain_partition, Entry, Evidence, TestDomain,
    };
    use std::collections::{BTreeMap, BTreeSet};
    use std::path::Path;

    fn fixture(kind: &'static str, path: &str) -> Entry {
        Entry {
            kind,
            path: path.to_owned(),
            line: 0,
            name: path.rsplit('/').next().unwrap().to_owned(),
            ring: "result",
        }
    }

    #[test]
    fn inventories_test_artifact_kinds() {
        assert!(is_sql_fixture(
            "tests/integrationtest/t/executor/insert.test"
        ));
        assert!(is_sql_fixture("tests/clusterintegrationtest/t/vector.test"));
        assert!(!is_sql_fixture(
            "tests/clusterintegrationtest/r/vector.result"
        ));
        assert!(is_sql_result(
            "tests/clusterintegrationtest/r/vector.result"
        ));
        assert!(!is_sql_result("tests/clusterintegrationtest/t/vector.test"));
        assert!(is_testdata_artifact("pkg/parser/testdata/cases.sql"));
        assert!(is_testdata_artifact(
            "tests/integrationtest/testdata/plan.json"
        ));
        assert!(!is_testdata_artifact("pkg/parser/parser_test.go"));
        assert!(is_shell_test("tests/realtikvtest/scripts/run-tests.sh"));
        assert!(is_shell_test("lightning/tests/lightning_csv/run.sh"));
        assert!(is_shell_test("build/jenkins_unit_test.sh"));
        assert!(!is_shell_test("scripts/build.sh"));
        assert!(is_test_suite_artifact(
            "tests/integrationtest/s/tpch_stats/orders.json"
        ));
        assert!(is_test_suite_artifact("tests/_utils/run_sql"));
        assert!(is_test_suite_artifact(
            "tests/clusterintegrationtest/python_testers/vector_recall.py"
        ));
        assert!(is_test_suite_artifact(
            "lightning/tests/lightning_csv/config.toml"
        ));
        assert!(!is_test_suite_artifact(
            "tests/clusterintegrationtest/README.md"
        ));
        assert!(!is_test_suite_artifact("pkg/parser/testdata/cases.sql"));
        assert!(!is_test_suite_artifact(
            ".agents/skills/reviewer/tests/fixture.json"
        ));
    }

    #[test]
    fn ast_inventory_ignores_comments_and_validates_runner_signatures() {
        let fixture_root =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/go_test_declarations");
        let declarations = generate_go_test_declaration_inventory(&repo_root(), &fixture_root)
            .expect("parse declaration fixture with the Go AST extractor");
        let find = |path: &str, name: &str| {
            declarations
                .iter()
                .find(|declaration| declaration.path == path && declaration.name == name)
                .unwrap_or_else(|| panic!("missing declaration {path}:{name}"))
        };

        for false_name in [
            "TestCommentedLine",
            "TestCommentedString",
            "TestCommentedBlock",
        ] {
            assert!(
                !declarations
                    .iter()
                    .any(|declaration| declaration.name == false_name),
                "comments and strings must not create {false_name}"
            );
        }
        for (name, category) in [
            ("TestLive", "Test"),
            ("BenchmarkLive", "Benchmark"),
            ("FuzzLive", "Fuzz"),
            ("ExampleLive", "Example"),
            ("TestMain", "TestMain"),
            ("TestBuildTagged", "Test"),
        ] {
            let path = if name == "TestBuildTagged" {
                "build_tagged_test.go"
            } else {
                "valid_test.go"
            };
            let declaration = find(path, name);
            assert_eq!(declaration.category, category);
            assert!(declaration.valid_runner_signature);
            assert!(declaration.line > 0 && declaration.column > 0);
        }
        for name in [
            "TestInvalid",
            "BenchmarkInvalid",
            "FuzzInvalid",
            "ExampleInvalid",
            "TestMethod",
        ] {
            assert!(
                !find("valid_test.go", name).valid_runner_signature,
                "invalid runner {name} must remain visible but cannot become a go_test entry"
            );
        }
        let hook = find("valid_test.go", "SetUpTest");
        assert_eq!(hook.category, "test_hook");
        assert_eq!(hook.receiver, "method");

        let fixture_accesses =
            generate_go_test_fixture_access_inventory(&repo_root(), &fixture_root)
                .expect("parse fixture access fixture with the Go AST extractor");
        let ledger_entries = collect(&fixture_root, &declarations, &fixture_accesses)
            .expect("derive ledger entries from AST declaration fixture");
        assert!(ledger_entries.iter().any(|entry| {
            entry.kind == "go_test" && entry.path == "valid_test.go" && entry.name == "TestLive"
        }));
        for name in [
            "TestInvalid",
            "BenchmarkInvalid",
            "FuzzInvalid",
            "ExampleInvalid",
            "TestMethod",
        ] {
            assert!(
                !ledger_entries
                    .iter()
                    .any(|entry| entry.kind == "go_test" && entry.name == name),
                "invalid runner {name} must not enter the Go-test ledger"
            );
        }
    }

    #[test]
    fn current_commented_out_tests_are_not_ast_declarations() {
        let root = repo_root();
        let declarations = generate_go_test_declaration_inventory(&root, &root)
            .expect("parse the repository's Go test declarations");
        for (path, name) in [
            (
                "pkg/expression/integration_test/integration_test.go",
                "TestFTSUnsupportedCases",
            ),
            (
                "pkg/ddl/db_change_test.go",
                "TestParallelColumnModifyingDefinition",
            ),
            (
                "pkg/planner/indexadvisor/indexadvisor_test.go",
                "TestIndexAdvisorCTE",
            ),
        ] {
            assert!(
                !declarations
                    .iter()
                    .any(|declaration| declaration.path == path && declaration.name == name),
                "comment-only declaration {path}:{name} entered the AST inventory"
            );
        }
    }

    #[test]
    fn inventories_only_concrete_bazel_test_rules() {
        let source = r#"
go_library(
    name = "library",
)
go_test(
    name = "package_test",
    srcs = ["package_test.go"],
)
py_test(
    name = "python_contract",
)
some_test_macro(
    name = "not_a_concrete_rule",
)
"#;
        assert_eq!(
            bazel_test_targets(source),
            vec![
                (5, "go_test", "package_test".to_owned()),
                (9, "py_test", "python_contract".to_owned())
            ]
        );
    }

    #[test]
    fn make_test_targets_deduplicate_split_declarations() {
        let source = "\
test: prepare\n\
test: run\n\
\tgo test ./...\n\
build_for_integration_test:\n\
\t./prepare\n\
tools/bin/gotestsum:\n\
\tgo build\n\
ordinary:\n";
        assert_eq!(
            make_test_targets(source),
            vec![
                (4, "build_for_integration_test".to_owned()),
                (1, "test".to_owned())
            ]
        );
        assert!(is_make_test_target("gotest_in_verify_ci"));
        assert!(!is_make_test_target("tools/bin/gotestsum"));
    }

    #[test]
    fn ast_fixture_access_inventory_keeps_literals_and_unresolved_paths_visible() {
        let fixture_root =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/go_test_declarations");
        let accesses = generate_go_test_fixture_access_inventory(&repo_root(), &fixture_root)
            .expect("parse fixture access fixture with the Go AST extractor");
        let find = |api: &str, expression: &str| {
            accesses
                .iter()
                .find(|access| access.api == api && access.expression == expression)
                .unwrap_or_else(|| panic!("missing access {api}:{expression}"))
        };

        assert!(accesses.iter().all(|access| {
            access.expression != "\"comment-only.fixture\""
                && access.expression != "\"string-only.fixture\""
        }));
        assert_eq!(
            find("go:embed", "literal.fixture").resolved_literal_path,
            "literal.fixture"
        );
        assert_eq!(
            find("os.ReadFile", "\"literal.fixture\"").resolved_literal_path,
            "literal.fixture"
        );
        for (api, expression, expected) in [
            ("os.Open", "\"open.fixture\"", "open.fixture"),
            ("os.OpenFile", "\"open-file.fixture\"", "open-file.fixture"),
            ("os.Stat", "\"stat.fixture\"", "stat.fixture"),
            ("os.ReadDir", "\"dir.fixture\"", "dir.fixture"),
            ("os.ReadFile", "\"aliased.fixture\"", "aliased.fixture"),
            (
                "os.ReadFile",
                "\"dot-import.fixture\"",
                "dot-import.fixture",
            ),
        ] {
            assert_eq!(find(api, expression).resolved_literal_path, expected);
        }
        for expression in [
            "testdata/*.fixture",
            "filepath.Join(\"testdata\", \"joined.fixture\")",
            "dynamicFixturePath()",
            "\"../../../outside.fixture\"",
        ] {
            assert!(
                find(
                    if expression == "testdata/*.fixture" {
                        "go:embed"
                    } else {
                        "os.ReadFile"
                    },
                    expression
                )
                .resolved_literal_path
                .is_empty(),
                "{expression} must remain an explicit unresolved obligation"
            );
        }

        let declarations = generate_go_test_declaration_inventory(&repo_root(), &fixture_root)
            .expect("parse declaration fixture");
        let ledger_entries = collect(&fixture_root, &declarations, &accesses)
            .expect("derive ledger entries from fixture access inventory");
        assert!(ledger_entries.iter().any(|entry| {
            entry.kind == "go_test_fixture"
                && entry.path == "fixture_access_test.go"
                && entry.name == "literal.fixture"
        }));
        assert!(ledger_entries.iter().any(|entry| {
            entry.kind == "go_test_fixture_unresolved"
                && entry.path == "fixture_access_test.go"
                && entry.name == "os.ReadFile: dynamicFixturePath()"
        }));
    }

    #[test]
    fn recognizes_normal_and_variant_sql_fixture_result_pairs() {
        let input = "tests/integrationtest/t/collation_misc.test";
        assert_eq!(
            expected_result_stem(input).as_deref(),
            Some("tests/integrationtest/r/collation_misc")
        );
        assert!(result_matches_input(
            "tests/integrationtest/r/collation_misc.result",
            input
        ));
        assert!(result_matches_input(
            "tests/integrationtest/r/collation_misc_enabled.result",
            input
        ));
        assert!(!result_matches_input(
            "tests/integrationtest/r/collation_miscx.result",
            input
        ));
    }

    #[test]
    fn source_queue_units_do_not_serialize_unrelated_test_files() {
        let mut entries = BTreeSet::new();
        let first = Entry {
            kind: "go_test",
            path: "pkg/expression/builtin_string_test.go".to_owned(),
            line: 10,
            name: "TestLength".to_owned(),
            ring: "result",
        };
        let second = Entry {
            kind: "go_test",
            path: "pkg/expression/builtin_time_test.go".to_owned(),
            line: 20,
            name: "TestWeek".to_owned(),
            ring: "result",
        };
        entries.insert(first.clone());
        entries.insert(second.clone());

        assert_eq!(
            package_ownership_unit(&first.path),
            "pkg/expression",
            "package summaries intentionally aggregate both files"
        );
        assert_eq!(
            source_ownership_unit(&first, &entries, &BTreeMap::new()),
            first.path
        );
        assert_eq!(
            source_ownership_unit(&second, &entries, &BTreeMap::new()),
            second.path
        );

        let bazel = Entry {
            kind: "bazel_test_target",
            path: "pkg/expression/BUILD.bazel".to_owned(),
            line: 152,
            name: "expression_test".to_owned(),
            ring: "result",
        };
        assert_eq!(
            source_ownership_unit(&bazel, &entries, &BTreeMap::new()),
            "pkg/expression/BUILD.bazel#expression_test"
        );
        let make = Entry {
            kind: "make_test_target",
            path: "Makefile".to_owned(),
            line: 146,
            name: "test".to_owned(),
            ring: "unassigned",
        };
        assert_eq!(
            source_ownership_unit(&make, &entries, &BTreeMap::new()),
            "Makefile#test"
        );

        let external_fixture = Entry {
            kind: "go_test_fixture",
            path: first.path.clone(),
            line: 30,
            name: "pkg/expression/generated.go".to_owned(),
            ring: "result",
        };
        assert_eq!(
            source_ownership_unit(&external_fixture, &entries, &BTreeMap::new()),
            first.path
        );
    }

    #[test]
    fn discovers_static_and_dynamic_subtest_obligations() {
        let source = r#"
func TestRows(t *testing.T) {
	t.Run("literal child", func(t *testing.T) {})
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {})
	}
	fmt.Println("t.Run(\\\"not a test\\\", nil)")
}
// t.Run("comment", func(t *testing.T) {})
"#;
        let entries = go_test_subtest_entries(
            "pkg/example/rows_test.go",
            source,
            &BTreeMap::from([(2, "TestRows".to_owned())]),
            "result",
        );
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].kind, "go_test_subtest");
        assert_eq!(entries[0].line, 3);
        assert_eq!(entries[0].name, "TestRows/literal child");
        assert_eq!(entries[1].kind, "go_test_generator");
        assert_eq!(entries[1].line, 5);
        assert_eq!(entries[1].name, "TestRows/<generated>");
    }

    #[test]
    fn split_file_routes_claimed_tests_and_keeps_unclaimed_tests_visible() {
        let claimed = Entry {
            kind: "go_test",
            path: "pkg/parser/ast/ddl_test.go".to_owned(),
            line: 10,
            name: "TestClaimed".to_owned(),
            ring: "parser",
        };
        let unclaimed = Entry {
            kind: "go_test",
            path: claimed.path.clone(),
            line: 20,
            name: "TestUnclaimed".to_owned(),
            ring: "parser",
        };
        let child = Entry {
            kind: "go_test_subtest",
            path: claimed.path.clone(),
            line: 12,
            name: "TestClaimed/literal".to_owned(),
            ring: "parser",
        };
        let hook = Entry {
            kind: "go_test_hook",
            path: claimed.path.clone(),
            line: 2,
            name: "SetUpTest".to_owned(),
            ring: "parser",
        };
        let entries = BTreeSet::from([
            claimed.clone(),
            unclaimed.clone(),
            child.clone(),
            hook.clone(),
        ]);
        let domains = BTreeMap::from([(
            claimed.clone(),
            TestDomain {
                name: "parser-ddl".to_owned(),
            },
        )]);
        assert_eq!(
            source_ownership_unit(&claimed, &entries, &domains),
            "domain:parser-ddl"
        );
        assert_eq!(
            source_ownership_unit(&child, &entries, &domains),
            "domain:parser-ddl"
        );
        assert_eq!(
            source_ownership_unit(&unclaimed, &entries, &domains),
            "pkg/parser/ast/ddl_test.go:20:TestUnclaimed"
        );
        assert_eq!(
            source_ownership_unit(&hook, &entries, &domains),
            "pkg/parser/ast/ddl_test.go#shared-support"
        );

        // The generated inventory is the explicit remainder declaration. It
        // cannot be hand-waved away by a sparse manifest row or an owner name.
        let no_evidence = BTreeMap::new();
        validate_test_domain_partition(&entries, &no_evidence, &domains)
            .expect("unclaimed split anchor remains explicitly UNTRIAGED");
        assert!(rendered(&entries, &no_evidence)
            .contains("go_test\tpkg/parser/ast/ddl_test.go\t20\tTestUnclaimed\tparser\tUNTRIAGED"));

        let evidence = BTreeMap::from([(
            unclaimed.clone(),
            Evidence {
                status: "PARTIAL".to_owned(),
                owner: "parser-ddl".to_owned(),
                artifact: "rust/evidence".to_owned(),
                note: "fixture".to_owned(),
            },
        )]);
        let error = validate_test_domain_partition(&entries, &evidence, &domains)
            .expect_err("covered source in a split file needs a domain claim");
        assert!(error.contains("TestUnclaimed"));
    }

    #[test]
    fn source_queue_includes_files_tests_and_lifecycle_hooks() {
        for kind in [
            "go_test_file",
            "go_test",
            "go_test_hook",
            "go_test_fixture",
            "bazel_test_target",
            "make_test_target",
            "shell_test",
            "test_suite_artifact",
        ] {
            assert!(is_queue_obligation(kind), "{kind}");
        }
        assert!(!is_queue_obligation("production_go_file"));
    }

    #[test]
    fn source_queue_pairs_result_variants_with_their_input() {
        let input = fixture(
            "integration_fixture",
            "tests/integrationtest/t/collation_misc.test",
        );
        let result = fixture(
            "integration_result",
            "tests/integrationtest/r/collation_misc_enabled.result",
        );
        let entries = BTreeSet::from([input.clone(), result.clone()]);
        assert_eq!(
            source_ownership_unit(&input, &entries, &BTreeMap::new()),
            input.path
        );
        assert_eq!(
            source_ownership_unit(&result, &entries, &BTreeMap::new()),
            input.path
        );
    }

    #[test]
    fn rejects_missing_and_orphan_sql_fixture_results() {
        let entries = BTreeSet::from([
            fixture(
                "integration_fixture",
                "tests/integrationtest/t/missing.test",
            ),
            fixture(
                "integration_result",
                "tests/integrationtest/r/orphan.result",
            ),
        ]);

        let error = validate_sql_fixture_pairs(&entries).unwrap_err();
        assert!(error.contains("tests/integrationtest/t/missing.test"));
        assert!(error.contains("tests/integrationtest/r/orphan.result"));
    }

    #[test]
    fn exact_input_stem_beats_a_variant_family_match() {
        let base = fixture("integration_fixture", "tests/integrationtest/t/foo.test");
        let exact = fixture(
            "integration_fixture",
            "tests/integrationtest/t/foo_enabled.test",
        );
        let result = fixture(
            "integration_result",
            "tests/integrationtest/r/foo_enabled.result",
        );

        let owners = result_owners(&result, &[&base, &exact]);
        assert_eq!(owners, vec![&exact]);
    }

    #[test]
    fn routes_sql_node_and_deferred_external_obligations_to_distinct_queues() {
        for path in [
            "pkg/planner/core/rule_predicate_push_down_test.go",
            "pkg/bindinfo/binding_plan_evolution_test.go",
            "pkg/util/ranger/ranger_test.go",
        ] {
            assert_eq!(ring_for(path), "plan", "{path}");
        }
        for path in [
            "br/pkg/backup/client_test.go",
            "lightning/pkg/importer/import_test.go",
            "dumpling/export/dump_test.go",
            "pkg/lightning/backend/backend_test.go",
            "pkg/importsdk/sdk_test.go",
            "pkg/dumpformat/parquetfile/parser_test.go",
        ] {
            assert_eq!(ring_for(path), "deferred-external", "{path}");
        }
        assert_eq!(
            ring_for("tests/realtikvtest/scripts/classic/run-tests.sh"),
            "transaction"
        );
        for path in [
            "pkg/tablecodec/tablecodec_test.go",
            "pkg/util/codec/bytes_test.go",
            "pkg/util/rowcodec/encoder_test.go",
        ] {
            assert_eq!(ring_for(path), "transaction", "{path}");
        }
        assert_eq!(
            ring_for("tests/clusterintegrationtest/run_mysql_tester.sh"),
            "result"
        );
        assert_eq!(ring_for("tests/globalkilltest/run-tests.sh"), "result");
        // Do not use the deferred bucket as a catch-all for SQL-node work.
        assert_eq!(ring_for("cmd/tidb-server/main_test.go"), "unassigned");
    }
}
