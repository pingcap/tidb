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

//! Verify the checked source-domain ownership queue.
//!
//! Domains are the parallel rewrite unit: exact Go production owners (a whole
//! file or named declaration) plus the Rust leaves, evidence, and focused
//! commands needed to advance them. Local agent claims are deliberately
//! ignored; they are advisory leases rather than durable queue state.
//!
//! ```text
//! cd rust
//! cargo run --locked -j 12 -p difftest --bin domain_queue -- --check
//! cargo run --locked -j 12 -p difftest --bin domain_queue -- --summary
//! ```

use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;
use std::path::{Component, Path, PathBuf};

const DOMAINS_RELATIVE_PATH: &str = "rust/workstreams/domains";
const SCHEMA: &str = "2";
const STATUSES: [&str; 3] = ["partial", "blocked", "ported"];
const KEYS: [&str; 8] = [
    "schema",
    "domain",
    "owner",
    "status",
    "go_owners",
    "rust_paths",
    "evidence_paths",
    "required_commands",
];

#[derive(Clone, Debug, Eq, PartialEq)]
enum Value {
    String(String),
    Strings(Vec<String>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Domain {
    name: String,
    owner: String,
    status: String,
    go_owners: Vec<String>,
    rust_paths: Vec<String>,
    evidence_paths: Vec<String>,
    required_commands: Vec<String>,
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("difftest must live at rust/difftests")
        .to_path_buf()
}

fn domains_path(root: &Path) -> PathBuf {
    root.join(DOMAINS_RELATIVE_PATH)
}

fn strip_comment(line: &str) -> &str {
    let mut quoted = false;
    for (index, character) in line.char_indices() {
        match character {
            '"' => quoted = !quoted,
            '#' if !quoted => return &line[..index],
            _ => {}
        }
    }
    line
}

fn parse_quoted(value: &str, context: &str) -> Result<String, String> {
    let value = value.trim();
    if value.len() < 2 || !value.starts_with('"') || !value.ends_with('"') {
        return Err(format!("{context}: expected a quoted string"));
    }
    let value = &value[1..value.len() - 1];
    if value.contains('"') {
        return Err(format!(
            "{context}: embedded quotes are not supported in domain records"
        ));
    }
    if value.is_empty() {
        return Err(format!("{context}: empty strings are not allowed"));
    }
    Ok(value.to_owned())
}

fn insert_value(
    values: &mut BTreeMap<String, Value>,
    key: String,
    value: Value,
    context: &str,
) -> Result<(), String> {
    if values.insert(key.clone(), value).is_some() {
        return Err(format!("{context}: duplicate key {key:?}"));
    }
    Ok(())
}

/// Parses the deliberately narrow TOML subset documented beside the records.
///
/// Pulling a general TOML crate into the differential harness solely for two
/// short, reviewable ownership files would make this gate more complex than
/// the records it protects. The parser rejects unsupported syntax instead.
fn parse_record(path: &Path, text: &str) -> Result<Domain, String> {
    let context = path.display().to_string();
    let mut values = BTreeMap::new();
    let mut array: Option<(String, Vec<String>)> = None;

    for (line_number, raw_line) in text.lines().enumerate() {
        let line = strip_comment(raw_line).trim();
        if line.is_empty() {
            continue;
        }
        let line_context = format!("{context}:{}", line_number + 1);
        if array.is_some() {
            if line == "]" {
                let (key, entries) = array.take().expect("array exists");
                if entries.is_empty() {
                    return Err(format!("{line_context}: array {key:?} must not be empty"));
                }
                insert_value(&mut values, key, Value::Strings(entries), &line_context)?;
                continue;
            }
            let item = line.strip_suffix(',').unwrap_or(line);
            let (_, entries) = array.as_mut().expect("array exists");
            entries.push(parse_quoted(item, &line_context)?);
            continue;
        }

        let (key, raw_value) = line
            .split_once('=')
            .ok_or_else(|| format!("{line_context}: expected key = value"))?;
        let key = key.trim();
        if !KEYS.contains(&key) {
            return Err(format!("{line_context}: unknown key {key:?}"));
        }
        if key.is_empty() {
            return Err(format!("{line_context}: empty key"));
        }
        let raw_value = raw_value.trim();
        if raw_value == "[" {
            array = Some((key.to_owned(), Vec::new()));
            continue;
        }
        let value = parse_quoted(raw_value, &line_context)?;
        insert_value(
            &mut values,
            key.to_owned(),
            Value::String(value),
            &line_context,
        )?;
    }

    if let Some((key, _)) = array {
        return Err(format!("{context}: unterminated array {key:?}"));
    }

    let schema = take_scalar(&mut values, "schema", &context)?;
    if schema != SCHEMA {
        return Err(format!(
            "{context}: unsupported schema {schema:?}; expected {SCHEMA:?}"
        ));
    }
    let record = Domain {
        name: take_scalar(&mut values, "domain", &context)?,
        owner: take_scalar(&mut values, "owner", &context)?,
        status: take_scalar(&mut values, "status", &context)?,
        go_owners: take_strings(&mut values, "go_owners", &context)?,
        rust_paths: take_strings(&mut values, "rust_paths", &context)?,
        evidence_paths: take_strings(&mut values, "evidence_paths", &context)?,
        required_commands: take_strings(&mut values, "required_commands", &context)?,
    };
    if !values.is_empty() {
        return Err(format!("{context}: unexpected remaining record values"));
    }
    Ok(record)
}

fn take_scalar(
    values: &mut BTreeMap<String, Value>,
    key: &str,
    context: &str,
) -> Result<String, String> {
    match values.remove(key) {
        Some(Value::String(value)) => Ok(value),
        Some(Value::Strings(_)) => Err(format!("{context}: {key} must be a quoted string")),
        None => Err(format!("{context}: missing required key {key}")),
    }
}

fn take_strings(
    values: &mut BTreeMap<String, Value>,
    key: &str,
    context: &str,
) -> Result<Vec<String>, String> {
    match values.remove(key) {
        Some(Value::Strings(value)) => Ok(value),
        Some(Value::String(_)) => Err(format!("{context}: {key} must be an array")),
        None => Err(format!("{context}: missing required key {key}")),
    }
}

fn domain_name_is_valid(name: &str) -> bool {
    !name.is_empty()
        && name.chars().all(|character| {
            character.is_ascii_lowercase() || character.is_ascii_digit() || character == '_'
        })
}

fn safe_relative_path(path: &str) -> bool {
    let path = Path::new(path);
    !path.is_absolute()
        && path
            .components()
            .all(|component| matches!(component, Component::Normal(_) | Component::CurDir))
}

fn ensure_unique(values: &[String], record_path: &Path, field: &str) -> Result<(), String> {
    let mut seen = BTreeSet::new();
    for value in values {
        if !seen.insert(value) {
            return Err(format!(
                "{}: duplicate {field} entry {value:?}",
                record_path.display()
            ));
        }
    }
    Ok(())
}

fn ensure_file(
    root: &Path,
    record_path: &Path,
    field: &str,
    value: &str,
    expected_prefix: &str,
) -> Result<PathBuf, String> {
    if !safe_relative_path(value) || !value.starts_with(expected_prefix) {
        return Err(format!(
            "{}: {field} {value:?} must be a safe path under {expected_prefix}",
            record_path.display()
        ));
    }
    let path = root.join(value);
    let metadata = fs::metadata(&path).map_err(|error| {
        format!(
            "{}: {field} {value:?} does not exist: {error}",
            record_path.display()
        )
    })?;
    if !metadata.is_file() {
        return Err(format!(
            "{}: {field} {value:?} must name a regular file",
            record_path.display()
        ));
    }
    Ok(path)
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum GoOwnerKind {
    File,
    Function,
    Method,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct GoOwner {
    raw: String,
    source: String,
    kind: GoOwnerKind,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum GoTokenKind {
    Identifier(String),
    Symbol(u8),
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct GoToken {
    kind: GoTokenKind,
    line: usize,
}

fn is_go_identifier_start(byte: u8) -> bool {
    byte.is_ascii_alphabetic() || byte == b'_'
}

fn is_go_identifier_continue(byte: u8) -> bool {
    is_go_identifier_start(byte) || byte.is_ascii_digit()
}

fn is_go_identifier(value: &str) -> bool {
    let bytes = value.as_bytes();
    !bytes.is_empty()
        && is_go_identifier_start(bytes[0])
        && bytes[1..].iter().copied().all(is_go_identifier_continue)
}

/// Lexes only the Go surface required to find package-level `func`
/// declarations. Comments and all string forms are skipped before declaration
/// matching, which makes a source-owner selector insensitive to harmless
/// formatting but immune to text that merely looks like Go in a comment.
fn lex_go(text: &str) -> Result<Vec<GoToken>, String> {
    let bytes = text.as_bytes();
    let mut tokens = Vec::new();
    let mut index = 0;
    let mut line = 1;
    while index < bytes.len() {
        match bytes[index] {
            b' ' | b'\t' | b'\r' => index += 1,
            b'\n' => {
                line += 1;
                index += 1;
            }
            b'/' if bytes.get(index + 1) == Some(&b'/') => {
                index += 2;
                while index < bytes.len() && bytes[index] != b'\n' {
                    index += 1;
                }
            }
            b'/' if bytes.get(index + 1) == Some(&b'*') => {
                index += 2;
                let mut closed = false;
                while index < bytes.len() {
                    if bytes[index] == b'\n' {
                        line += 1;
                    }
                    if bytes[index] == b'*' && bytes.get(index + 1) == Some(&b'/') {
                        index += 2;
                        closed = true;
                        break;
                    }
                    index += 1;
                }
                if !closed {
                    return Err(format!("unterminated block comment at line {line}"));
                }
            }
            b'`' => {
                index += 1;
                while index < bytes.len() && bytes[index] != b'`' {
                    if bytes[index] == b'\n' {
                        line += 1;
                    }
                    index += 1;
                }
                if index == bytes.len() {
                    return Err(format!("unterminated raw string literal at line {line}"));
                }
                index += 1;
            }
            b'"' | b'\'' => {
                let quote = bytes[index];
                index += 1;
                let mut closed = false;
                while index < bytes.len() {
                    if bytes[index] == b'\\' {
                        if index + 1 == bytes.len() {
                            return Err(format!("unterminated escape at line {line}"));
                        }
                        index += 2;
                        continue;
                    }
                    if bytes[index] == b'\n' {
                        line += 1;
                    }
                    if bytes[index] == quote {
                        index += 1;
                        closed = true;
                        break;
                    }
                    index += 1;
                }
                if !closed {
                    return Err(format!("unterminated quoted literal at line {line}"));
                }
            }
            byte if is_go_identifier_start(byte) => {
                let start = index;
                index += 1;
                while index < bytes.len() && is_go_identifier_continue(bytes[index]) {
                    index += 1;
                }
                tokens.push(GoToken {
                    kind: GoTokenKind::Identifier(text[start..index].to_owned()),
                    line,
                });
            }
            byte => {
                tokens.push(GoToken {
                    kind: GoTokenKind::Symbol(byte),
                    line,
                });
                index += 1;
            }
        }
    }
    Ok(tokens)
}

fn symbol(tokens: &[GoToken], index: usize, expected: u8) -> bool {
    matches!(tokens.get(index).map(|token| &token.kind), Some(GoTokenKind::Symbol(found)) if *found == expected)
}

fn identifier(tokens: &[GoToken], index: usize) -> Option<&str> {
    match tokens.get(index).map(|token| &token.kind) {
        Some(GoTokenKind::Identifier(value)) => Some(value),
        Some(GoTokenKind::Symbol(_)) | None => None,
    }
}

fn matching_delimiter(tokens: &[GoToken], open: usize, start: u8, end: u8) -> Option<usize> {
    let mut depth = 0usize;
    for (index, token) in tokens.iter().enumerate().skip(open) {
        match token.kind {
            GoTokenKind::Symbol(found) if found == start => depth += 1,
            GoTokenKind::Symbol(found) if found == end => {
                depth = depth.checked_sub(1)?;
                if depth == 0 {
                    return Some(index);
                }
            }
            GoTokenKind::Identifier(_) | GoTokenKind::Symbol(_) => {}
        }
    }
    None
}

fn receiver_base(tokens: &[GoToken]) -> Option<&str> {
    let first_pointer = tokens
        .iter()
        .position(|token| matches!(token.kind, GoTokenKind::Symbol(b'*')));
    if let Some(pointer) = first_pointer {
        return tokens[pointer + 1..]
            .iter()
            .find_map(|token| match &token.kind {
                GoTokenKind::Identifier(value) => Some(value.as_str()),
                GoTokenKind::Symbol(_) => None,
            });
    }
    tokens
        .iter()
        .filter_map(|token| match &token.kind {
            GoTokenKind::Identifier(value) => Some(value.as_str()),
            GoTokenKind::Symbol(_) => None,
        })
        .nth(1)
}

/// Produces canonical `func:` / `method:` selectors for every top-level Go
/// declaration in one source file. It deliberately recognizes only the
/// declaration prefix; function bodies remain opaque and cannot create false
/// ownership entries.
fn go_declarations(source_path: &str, source: &str) -> Result<BTreeMap<String, usize>, String> {
    let tokens = lex_go(source)?;
    let mut declarations = BTreeMap::new();
    let mut brace_depth = 0usize;
    for index in 0..tokens.len() {
        match tokens[index].kind {
            GoTokenKind::Symbol(b'{') => brace_depth += 1,
            GoTokenKind::Symbol(b'}') => brace_depth = brace_depth.saturating_sub(1),
            GoTokenKind::Identifier(ref value) if brace_depth == 0 && value == "func" => {
                let mut cursor = index + 1;
                let receiver = if symbol(&tokens, cursor, b'(') {
                    let close = matching_delimiter(&tokens, cursor, b'(', b')');
                    let receiver =
                        close.and_then(|close| receiver_base(&tokens[cursor + 1..close]));
                    cursor = close.map_or(cursor, |close| close + 1);
                    receiver
                } else {
                    None
                };
                let Some(name) = identifier(&tokens, cursor) else {
                    continue;
                };
                cursor += 1;
                if symbol(&tokens, cursor, b'[') {
                    let Some(close) = matching_delimiter(&tokens, cursor, b'[', b']') else {
                        continue;
                    };
                    cursor = close + 1;
                }
                if !symbol(&tokens, cursor, b'(') {
                    continue;
                }
                let selector = match receiver {
                    Some(receiver) => format!("method:{source_path}#{receiver}.{name}"),
                    None => format!("func:{source_path}#{name}"),
                };
                declarations.insert(selector, tokens[index].line);
            }
            GoTokenKind::Identifier(_) | GoTokenKind::Symbol(_) => {}
        }
    }
    Ok(declarations)
}

fn parse_go_owner(value: &str, record_path: &Path) -> Result<GoOwner, String> {
    let (kind, target) = value.split_once(':').ok_or_else(|| {
        format!(
            "{}: go_owners entry {value:?} must start with file:, func:, or method:",
            record_path.display()
        )
    })?;
    let (source, member) = match kind {
        "file" => (target, None),
        "func" | "method" => {
            let (source, member) = target.split_once('#').ok_or_else(|| {
                format!(
                    "{}: {kind}: owner {value:?} must include path#symbol",
                    record_path.display()
                )
            })?;
            (source, Some(member))
        }
        _ => {
            return Err(format!(
                "{}: unsupported go_owners kind {kind:?}; expected file, func, or method",
                record_path.display()
            ));
        }
    };
    if !source.starts_with("pkg/")
        || !source.ends_with(".go")
        || source.ends_with("_test.go")
        || !safe_relative_path(source)
    {
        return Err(format!(
            "{}: Go owner source {source:?} must be a safe non-test .go file under pkg/",
            record_path.display()
        ));
    }
    match (kind, member) {
        ("file", None) if !target.contains('#') => {}
        ("func", Some(name)) if is_go_identifier(name) => {}
        ("method", Some(method))
            if method.split_once('.').is_some_and(|(receiver, name)| {
                is_go_identifier(receiver) && is_go_identifier(name)
            }) => {}
        _ => {
            return Err(format!(
                "{}: malformed go_owners entry {value:?}",
                record_path.display()
            ));
        }
    }
    Ok(GoOwner {
        raw: value.to_owned(),
        source: source.to_owned(),
        kind: match kind {
            "file" => GoOwnerKind::File,
            "func" => GoOwnerKind::Function,
            "method" => GoOwnerKind::Method,
            _ => unreachable!("validated owner kind"),
        },
    })
}

fn uses_twelve_jobs(command: &str) -> bool {
    command.contains("CARGO_BUILD_JOBS=12")
        || command.contains("-j12")
        || command.contains("-j 12")
        || command.contains("-p=12")
        || command.contains("-p 12")
}

fn read_go_declarations(root: &Path, source: &str) -> Result<BTreeMap<String, usize>, String> {
    let path = root.join(source);
    let text =
        fs::read_to_string(&path).map_err(|error| format!("read {}: {error}", path.display()))?;
    go_declarations(source, &text)
}

fn evidence_owners(path: &Path) -> Result<BTreeSet<String>, String> {
    let text =
        fs::read_to_string(path).map_err(|error| format!("read {}: {error}", path.display()))?;
    let mut lines = text.lines().filter(|line| !line.trim().is_empty());
    let header = lines
        .next()
        .ok_or_else(|| format!("{}: evidence file is empty", path.display()))?;
    let owner_column = header
        .split('\t')
        .position(|column| column == "go_owner")
        .ok_or_else(|| format!("{}: evidence header must contain go_owner", path.display()))?;
    let mut owners = BTreeSet::new();
    for (row, line) in lines.enumerate() {
        let value = line.split('\t').nth(owner_column).ok_or_else(|| {
            format!(
                "{}:{}: evidence row has no go_owner column",
                path.display(),
                row + 2
            )
        })?;
        if value.is_empty() {
            return Err(format!(
                "{}:{}: evidence go_owner must not be empty",
                path.display(),
                row + 2
            ));
        }
        owners.insert(value.to_owned());
    }
    Ok(owners)
}

fn validate_record(root: &Path, record_path: &Path, record: &Domain) -> Result<(), String> {
    let expected_name = record_path
        .file_stem()
        .and_then(|name| name.to_str())
        .expect("domain record filenames are UTF-8 .toml names");
    if record.name != expected_name || !domain_name_is_valid(&record.name) {
        return Err(format!(
            "{}: domain {:?} must match safe filename {:?}",
            record_path.display(),
            record.name,
            expected_name
        ));
    }
    if record.owner.trim().is_empty() {
        return Err(format!(
            "{}: owner must not be empty",
            record_path.display()
        ));
    }
    if !STATUSES.contains(&record.status.as_str()) {
        return Err(format!(
            "{}: unknown status {:?}; expected partial, blocked, or ported",
            record_path.display(),
            record.status
        ));
    }

    ensure_unique(&record.go_owners, record_path, "go_owners")?;
    ensure_unique(&record.rust_paths, record_path, "rust_paths")?;
    ensure_unique(&record.evidence_paths, record_path, "evidence_paths")?;
    ensure_unique(&record.required_commands, record_path, "required_commands")?;

    for raw_owner in &record.go_owners {
        let owner = parse_go_owner(raw_owner, record_path)?;
        ensure_file(root, record_path, "go_owners", &owner.source, "pkg/")?;
        if owner.kind != GoOwnerKind::File {
            let declarations = read_go_declarations(root, &owner.source)?;
            if !declarations.contains_key(&owner.raw) {
                return Err(format!(
                    "{}: go_owners selector {:?} does not name a top-level Go declaration",
                    record_path.display(),
                    owner.raw
                ));
            }
        }
    }
    for path in &record.rust_paths {
        ensure_file(root, record_path, "rust_paths", path, "rust/")?;
    }

    let mut evidence = BTreeSet::new();
    for path in &record.evidence_paths {
        let path = ensure_file(
            root,
            record_path,
            "evidence_paths",
            path,
            "rust/difftests/corpus/coverage/",
        )?;
        evidence.extend(evidence_owners(&path)?);
    }
    for owner in &record.go_owners {
        if !evidence.contains(owner) {
            return Err(format!(
                "{}: evidence_paths do not name owned Go selector {owner:?}",
                record_path.display(),
            ));
        }
    }

    for command in &record.required_commands {
        if command.trim().is_empty() || !uses_twelve_jobs(command) {
            return Err(format!(
                "{}: required command must be nonempty and use 12 jobs: {command:?}",
                record_path.display()
            ));
        }
    }
    Ok(())
}

fn validate_owner_sets(root: &Path, domains: &[(PathBuf, Domain)]) -> Result<(), String> {
    let mut claims: BTreeMap<String, Vec<(String, String, GoOwnerKind)>> = BTreeMap::new();
    for (record_path, domain) in domains {
        for raw_owner in &domain.go_owners {
            let owner = parse_go_owner(raw_owner, record_path)?;
            claims.entry(owner.source).or_default().push((
                owner.raw,
                domain.name.clone(),
                owner.kind,
            ));
        }
    }
    for (source, claims) in claims {
        let mut unique = BTreeMap::new();
        for (selector, domain, _) in &claims {
            if let Some(previous) = unique.insert(selector, domain) {
                return Err(format!(
                    "Go owner {selector:?} is claimed by both domains {previous:?} and {domain:?}"
                ));
            }
        }
        let file_claims: Vec<_> = claims
            .iter()
            .filter(|(_, _, kind)| *kind == GoOwnerKind::File)
            .collect();
        if !file_claims.is_empty() {
            if claims.len() != 1 {
                return Err(format!(
                    "Go file {source:?} has a whole-file owner and symbol owners; use exactly one ownership model"
                ));
            }
            continue;
        }
        let declarations = read_go_declarations(root, &source)?;
        let claimed: BTreeSet<_> = claims
            .iter()
            .map(|(selector, _, _)| selector.as_str())
            .collect();
        let declared: BTreeSet<_> = declarations.keys().map(String::as_str).collect();
        if claimed != declared {
            let missing: Vec<_> = declared.difference(&claimed).copied().collect();
            let extra: Vec<_> = claimed.difference(&declared).copied().collect();
            return Err(format!(
                "Go symbol families for {source:?} must cover every top-level function/method; missing={missing:?}, extra={extra:?}"
            ));
        }
    }
    Ok(())
}

fn read_domains(root: &Path) -> Result<Vec<Domain>, String> {
    let directory = domains_path(root);
    let entries = fs::read_dir(&directory)
        .map_err(|error| format!("read {}: {error}", directory.display()))?;
    let mut records = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|error| format!("read {}: {error}", directory.display()))?;
        let path = entry.path();
        let file_type = entry
            .file_type()
            .map_err(|error| format!("inspect {}: {error}", path.display()))?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name == "README.md" && file_type.is_file() {
            continue;
        }
        if !file_type.is_file() || path.extension().and_then(|value| value.to_str()) != Some("toml")
        {
            return Err(format!(
                "unknown source-domain entry {}; only regular .toml records and README.md are allowed",
                path.display()
            ));
        }
        let text = fs::read_to_string(&path)
            .map_err(|error| format!("read {}: {error}", path.display()))?;
        records.push((path, text));
    }
    if records.is_empty() {
        return Err(format!(
            "{} contains no source-domain records",
            directory.display()
        ));
    }
    records.sort_by(|left, right| left.0.cmp(&right.0));

    let mut domains = Vec::with_capacity(records.len());
    for (path, text) in records {
        let domain = parse_record(&path, &text)?;
        validate_record(root, &path, &domain)?;
        domains.push((path, domain));
    }
    validate_owner_sets(root, &domains)?;
    Ok(domains.into_iter().map(|(_, domain)| domain).collect())
}

fn render_summary(domains: &[Domain]) -> String {
    let mut status_counts = BTreeMap::new();
    for status in STATUSES {
        status_counts.insert(status, 0usize);
    }
    for domain in domains {
        *status_counts
            .get_mut(domain.status.as_str())
            .expect("validated status") += 1;
    }

    let mut output = String::new();
    output.push_str(
        "kind\tname\tstatus\towner\tgo_owners\trust_paths\tevidence_paths\trequired_commands\n",
    );
    for (status, count) in status_counts {
        output.push_str(&format!("status\t{status}\t-\t-\t{count}\t-\t-\t-\n"));
    }
    for domain in domains {
        output.push_str("domain\t");
        output.push_str(&domain.name);
        output.push('\t');
        output.push_str(&domain.status);
        output.push('\t');
        output.push_str(&domain.owner);
        output.push('\t');
        output.push_str(&domain.go_owners.join(","));
        output.push('\t');
        output.push_str(&domain.rust_paths.join(","));
        output.push('\t');
        output.push_str(&domain.evidence_paths.join(","));
        output.push('\t');
        output.push_str(&domain.required_commands.join(" | "));
        output.push('\n');
    }
    output
}

fn run() -> Result<(), String> {
    let mut args = env::args().skip(1);
    let mode = args
        .next()
        .ok_or_else(|| "expected --check or --summary".to_owned())?;
    if args.next().is_some() || !matches!(mode.as_str(), "--check" | "--summary") {
        return Err("usage: domain_queue --check|--summary".to_owned());
    }
    let domains = read_domains(&repo_root())?;
    if mode == "--summary" {
        print!("{}", render_summary(&domains));
    }
    Ok(())
}

fn main() {
    if let Err(error) = run() {
        eprintln!("domain queue: {error}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::{evidence_owners, go_declarations, parse_record, render_summary, Domain};
    use std::fs;
    use std::path::Path;

    const VALID: &str = r#"
schema = "2"
domain = "ddl_index"
owner = "parser-index"
status = "partial"
go_owners = [
  "file:pkg/parser/ddl_index_parser.go",
]
rust_paths = [
  "rust/crates/tidb-parser/src/ddl/index.rs",
]
evidence_paths = [
  "rust/difftests/corpus/coverage/evidence/parser/ddl_index_parser.tsv",
]
required_commands = [
  "CARGO_BUILD_JOBS=12 cargo test --locked -j12 -p tidb-parser index_source",
]
"#;

    #[test]
    fn parses_documented_record_shape() {
        let parsed = parse_record(Path::new("ddl_index.toml"), VALID).expect("valid record");
        assert_eq!(parsed.name, "ddl_index");
        assert_eq!(parsed.go_owners, ["file:pkg/parser/ddl_index_parser.go"]);
    }

    #[test]
    fn rejects_unknown_keys_and_wrong_shapes() {
        let unknown = VALID.replace("status = \"partial\"", "state = \"partial\"");
        assert!(parse_record(Path::new("ddl_index.toml"), &unknown)
            .expect_err("unknown key")
            .contains("unknown key"));

        let wrong_shape = VALID.replace(
            "owner = \"parser-index\"",
            "owner = [\n  \"parser-index\",\n]",
        );
        assert!(parse_record(Path::new("ddl_index.toml"), &wrong_shape)
            .expect_err("owner array")
            .contains("owner must be a quoted string"));
    }

    #[test]
    fn summary_includes_status_and_required_commands() {
        let domain = Domain {
            name: "ddl_index".to_owned(),
            owner: "parser-index".to_owned(),
            status: "partial".to_owned(),
            go_owners: vec!["file:pkg/parser/ddl_index_parser.go".to_owned()],
            rust_paths: vec!["rust/crates/tidb-parser/src/ddl/index.rs".to_owned()],
            evidence_paths: vec![
                "rust/difftests/corpus/coverage/evidence/parser/ddl_index_parser.tsv".to_owned(),
            ],
            required_commands: vec![
                "CARGO_BUILD_JOBS=12 cargo test --locked -j12 -p tidb-parser index_source"
                    .to_owned(),
            ],
        };
        let summary = render_summary(&[domain]);
        assert!(summary.contains("partial"));
        assert!(summary.contains("index_source"));
    }

    #[test]
    fn declaration_lexer_ignores_comments_literals_and_closures() {
        let source = r#"
// func Fake() {}
/* func (f *Fake) Method() {} */
var quoted = "func Quoted() {}"
var raw = `func Raw() {}
func (r *Raw) Method() {}`
var closure = func() {}
func /* comment */ Free[T any](value T) {}
func (
    p *HandParser
) Parse() {}
func (p Parser[K, V]) Pair() {}
"#;
        let declarations =
            go_declarations("pkg/parser/fixture.go", source).expect("valid Go declaration fixture");
        assert_eq!(
            declarations.keys().map(String::as_str).collect::<Vec<_>>(),
            [
                "func:pkg/parser/fixture.go#Free",
                "method:pkg/parser/fixture.go#HandParser.Parse",
                "method:pkg/parser/fixture.go#Parser.Pair",
            ]
        );
    }

    #[test]
    fn declaration_lexer_rejects_unclosed_comment_and_literal() {
        assert!(
            go_declarations("pkg/parser/fixture.go", "/* func Fake() {}")
                .expect_err("unterminated comment")
                .contains("block comment")
        );
        assert!(
            go_declarations("pkg/parser/fixture.go", "var x = `func Fake() {}")
                .expect_err("unterminated raw literal")
                .contains("raw string")
        );
    }

    #[test]
    fn evidence_requires_an_exact_go_owner_cell() {
        let directory =
            std::env::temp_dir().join(format!("tidb-domain-queue-evidence-{}", std::process::id()));
        fs::create_dir_all(&directory).expect("create temporary directory");
        let path = directory.join("owners.tsv");
        fs::write(
            &path,
            "go_owner\trust_module\nfunc:pkg/parser/a.go#Parse\trust/a.rs\n",
        )
        .expect("write evidence");
        let owners = evidence_owners(&path).expect("parse exact owner cell");
        assert!(owners.contains("func:pkg/parser/a.go#Parse"));
        fs::write(
            &path,
            "go_source\trust_module\npkg/parser/a.go\trust/a.rs\n",
        )
        .expect("write invalid header");
        assert!(evidence_owners(&path)
            .expect_err("legacy source header must not be accepted")
            .contains("go_owner"));
        fs::remove_dir_all(directory).expect("remove temporary directory");
    }
}
