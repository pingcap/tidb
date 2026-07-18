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

//! Generate and validate the static Go parser oracle for every integration
//! parser-inventory input.
//!
//! `--write` is deliberately the only mode that starts Go. It uses
//! `godump framed-restore`, whose byte-counted protocol preserves multiline
//! mysqltest inputs and control bytes. `--check` compares the checked oracle
//! with the checked source inventory only, so the regular Rust test suite has
//! no Go subprocess dependency.
//!
//! ```text
//! cd rust
//! cargo run -p difftest --bin integration_parser_inventory -- --check
//! cargo run -p difftest --bin integration_parser_golden -- --write
//! cargo run -p difftest --bin integration_parser_golden -- --check
//! ```

use std::env;
use std::fs;
use std::io::{self, BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, OnceLock};
use std::thread;

#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};

const INVENTORY_RELATIVE_PATH: &str =
    "rust/difftests/corpus/coverage/integration_parser_inventory.tsv";
const GOLDEN_RELATIVE_PATH: &str = "rust/difftests/corpus/coverage/integration_parser_golden.tsv";
const INVENTORY_HEADER: &str =
    "source_path\tsource_start_line\tsource_end_line\tdelimiter\tboundary\tsql";
const GOLDEN_HEADER: &str = "source_path\tsource_start_line\tsource_end_line\tdelimiter\tboundary\tsql\tgo_outcome\tgo_statement_count\tgo_restores_hex";

static SHARED_GOLDEN: OnceLock<Result<Arc<[GoldenRecord]>, String>> = OnceLock::new();
#[cfg(test)]
static SHARED_GOLDEN_LOADS: AtomicUsize = AtomicUsize::new(0);

/// One SQL input extracted from the checked integration-test inventory.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Input {
    /// Repository-relative source fixture path.
    pub path: String,
    /// First source line contributing this input.
    pub start_line: usize,
    /// Last source line contributing this input.
    pub end_line: usize,
    /// mysqltest statement delimiter recorded by the inventory.
    pub delimiter: String,
    /// Inventory extraction boundary kind.
    pub boundary: String,
    /// Exact SQL input bytes represented as UTF-8 text.
    pub sql: String,
}

/// The Go parser result class stored in the checked oracle.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GoOutcome {
    /// Go parsed and restored the input.
    Accepted,
    /// Go rejected the input during parsing.
    Rejected,
    /// Go parsed the input but could not restore it.
    RestoreFailure,
}

impl GoOutcome {
    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "accepted" => Ok(Self::Accepted),
            "rejected" => Ok(Self::Rejected),
            "restore_failure" => Ok(Self::RestoreFailure),
            _ => Err(format!("unknown Go parser outcome {value:?}")),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::Rejected => "rejected",
            Self::RestoreFailure => "restore_failure",
        }
    }
}

/// One checked Go parser result corresponding to an [`Input`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GoldenRecord {
    /// Source inventory input.
    pub input: Input,
    /// Go parser result class.
    pub outcome: GoOutcome,
    /// Number of Go statements produced for this input.
    pub statement_count: usize,
    /// Byte-exact restored SQL for each parsed statement.
    pub restores: Vec<Vec<u8>>,
}

/// Returns the repository root inferred from the difftest package location.
pub fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("difftest must live at rust/difftests")
        .to_path_buf()
}

fn inventory_path(root: &Path) -> PathBuf {
    root.join(INVENTORY_RELATIVE_PATH)
}

fn golden_path(root: &Path) -> PathBuf {
    root.join(GOLDEN_RELATIVE_PATH)
}

fn escape_tsv(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        match character {
            '\\' => escaped.push_str("\\\\"),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            _ if character.is_control() => {
                use std::fmt::Write as _;
                write!(&mut escaped, "\\u{{{:04X}}}", character as u32)
                    .expect("write to String cannot fail");
            }
            _ => escaped.push(character),
        }
    }
    escaped
}

fn unescape_tsv(value: &str) -> Result<String, String> {
    let mut output = String::with_capacity(value.len());
    let mut chars = value.chars();
    while let Some(character) = chars.next() {
        if character != '\\' {
            output.push(character);
            continue;
        }
        let escaped = chars
            .next()
            .ok_or_else(|| "truncated TSV escape".to_owned())?;
        match escaped {
            '\\' => output.push('\\'),
            'n' => output.push('\n'),
            'r' => output.push('\r'),
            't' => output.push('\t'),
            'u' => {
                if chars.next() != Some('{') {
                    return Err("invalid Unicode TSV escape".to_owned());
                }
                let mut hex = String::new();
                loop {
                    let digit = chars
                        .next()
                        .ok_or_else(|| "unterminated Unicode TSV escape".to_owned())?;
                    if digit == '}' {
                        break;
                    }
                    hex.push(digit);
                }
                let code_point = u32::from_str_radix(&hex, 16)
                    .map_err(|_| format!("invalid Unicode TSV escape {hex:?}"))?;
                output.push(
                    char::from_u32(code_point)
                        .ok_or_else(|| format!("invalid Unicode code point {code_point:X}"))?,
                );
            }
            _ => return Err(format!("unknown TSV escape \\{escaped}")),
        }
    }
    Ok(output)
}

fn split_row<'a>(line: &'a str, count: usize, context: &str) -> Result<Vec<&'a str>, String> {
    let fields: Vec<_> = line.split('\t').collect();
    if fields.len() != count {
        return Err(format!(
            "{context}: expected {count} TSV fields, got {}",
            fields.len()
        ));
    }
    Ok(fields)
}

fn parse_input(fields: &[&str], context: &str) -> Result<Input, String> {
    let boundary = fields[4];
    if !matches!(
        boundary,
        "lexical" | "runner_raw_fallback" | "directive_query"
    ) {
        return Err(format!(
            "{context}: unknown inventory boundary {boundary:?}"
        ));
    }
    Ok(Input {
        path: fields[0].to_owned(),
        start_line: fields[1]
            .parse()
            .map_err(|_| format!("{context}: invalid source start line {:?}", fields[1]))?,
        end_line: fields[2]
            .parse()
            .map_err(|_| format!("{context}: invalid source end line {:?}", fields[2]))?,
        delimiter: unescape_tsv(fields[3]).map_err(|error| format!("{context}: {error}"))?,
        boundary: boundary.to_owned(),
        sql: unescape_tsv(fields[5]).map_err(|error| format!("{context}: {error}"))?,
    })
}

/// Read the checked source fixture inventory used to build the Go oracle.
///
/// Other differential rings use this to reject a derived manifest whenever
/// its source inventory and the parser oracle have drifted apart.
pub(crate) fn read_inventory(root: &Path) -> Result<Vec<Input>, String> {
    let path = inventory_path(root);
    let text =
        fs::read_to_string(&path).map_err(|error| format!("read {}: {error}", path.display()))?;
    let mut lines = text.lines();
    if lines.next() != Some(INVENTORY_HEADER) {
        return Err(format!(
            "{}: wrong or missing inventory header",
            path.display()
        ));
    }
    lines
        .enumerate()
        .map(|(index, line)| {
            let context = format!("{}:{}", path.display(), index + 2);
            let fields = split_row(line, 6, &context)?;
            parse_input(&fields, &context)
        })
        .collect()
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

fn encode_restores(restores: &[Vec<u8>]) -> Vec<u8> {
    let mut output = Vec::new();
    for restore in restores {
        output.extend_from_slice(&(restore.len() as u64).to_be_bytes());
        output.extend_from_slice(restore);
    }
    output
}

fn decode_restores(mut payload: &[u8], count: usize) -> Result<Vec<Vec<u8>>, String> {
    let mut restores = Vec::with_capacity(count);
    for _ in 0..count {
        let length = payload
            .get(..8)
            .ok_or_else(|| "truncated restore length".to_owned())?;
        let length = u64::from_be_bytes(length.try_into().expect("eight byte slice"));
        let length: usize = length
            .try_into()
            .map_err(|_| "restore length cannot fit in usize".to_owned())?;
        payload = &payload[8..];
        let text = payload
            .get(..length)
            .ok_or_else(|| "truncated restored SQL".to_owned())?;
        restores.push(text.to_vec());
        payload = &payload[length..];
    }
    if !payload.is_empty() {
        return Err("trailing bytes after restored SQL records".to_owned());
    }
    Ok(restores)
}

fn load_golden(path: &Path) -> Result<Arc<[GoldenRecord]>, String> {
    let text =
        fs::read_to_string(path).map_err(|error| format!("read {}: {error}", path.display()))?;
    let mut lines = text.lines();
    if lines.next() != Some(GOLDEN_HEADER) {
        return Err(format!(
            "{}: wrong or missing golden header",
            path.display()
        ));
    }
    let records: Vec<_> = lines
        .enumerate()
        .map(|(index, line)| {
            let context = format!("{}:{}", path.display(), index + 2);
            let fields = split_row(line, 9, &context)?;
            let input = parse_input(&fields[..6], &context)?;
            let outcome =
                GoOutcome::parse(fields[6]).map_err(|error| format!("{context}: {error}"))?;
            let statement_count = fields[7]
                .parse()
                .map_err(|_| format!("{context}: invalid Go statement count {:?}", fields[7]))?;
            let payload = hex_decode(fields[8]).map_err(|error| format!("{context}: {error}"))?;
            let restores = decode_restores(&payload, statement_count)
                .map_err(|error| format!("{context}: {error}"))?;
            if outcome != GoOutcome::Accepted && (!restores.is_empty() || statement_count != 0) {
                return Err(format!(
                    "{context}: rejected/restore-failure record carries restores"
                ));
            }
            Ok(GoldenRecord {
                input,
                outcome,
                statement_count,
                restores,
            })
        })
        .collect::<Result<_, _>>()?;
    Ok(records.into())
}

fn shared_golden_result() -> &'static Result<Arc<[GoldenRecord]>, String> {
    SHARED_GOLDEN.get_or_init(|| {
        #[cfg(test)]
        SHARED_GOLDEN_LOADS.fetch_add(1, Ordering::Relaxed);
        load_golden(&golden_path(&repo_root()))
    })
}

/// Borrows the process-wide checked Go parser oracle.
///
/// Selector modules in one Cargo shard share this immutable allocation, so
/// the 51k-row TSV is read and decoded once per test process instead of once
/// per selector test.
pub fn shared_golden() -> Result<&'static [GoldenRecord], &'static str> {
    match shared_golden_result() {
        Ok(records) => Ok(records),
        Err(error) => Err(error),
    }
}

/// Reads the checked static Go parser oracle below `root`.
///
/// The repository oracle uses the process-wide shared allocation. Explicit
/// alternate roots remain uncached so inventory/golden tooling keeps its
/// path-specific behavior and error messages.
pub fn read_golden(root: &Path) -> Result<Arc<[GoldenRecord]>, String> {
    let path = golden_path(root);
    if path == golden_path(&repo_root()) {
        return shared_golden_result()
            .as_ref()
            .map(Arc::clone)
            .map_err(Clone::clone);
    }
    load_golden(&path)
}

fn render(records: &[GoldenRecord]) -> String {
    let mut output = String::from(GOLDEN_HEADER);
    output.push('\n');
    for record in records {
        let input = &record.input;
        let restore_hex = hex_encode(&encode_restores(&record.restores));
        output.push_str(&format!(
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
            input.path,
            input.start_line,
            input.end_line,
            escape_tsv(&input.delimiter),
            input.boundary,
            escape_tsv(&input.sql),
            record.outcome.as_str(),
            record.statement_count,
            restore_hex,
        ));
    }
    output
}

fn frame_request(index: usize, sql: &[u8]) -> Vec<u8> {
    let mut frame = format!("@{index} {}\n", sql.len()).into_bytes();
    frame.extend_from_slice(sql);
    frame
}

fn read_response(reader: &mut impl BufRead) -> Result<(usize, GoOutcome, usize, Vec<u8>), String> {
    let mut header = Vec::new();
    reader
        .read_until(b'\n', &mut header)
        .map_err(|error| format!("read Go response header: {error}"))?;
    let header = std::str::from_utf8(&header)
        .map_err(|error| format!("Go response header is not UTF-8: {error}"))?;
    let fields: Vec<_> = header.trim_end_matches('\n').split(' ').collect();
    if fields.len() != 4 || !fields[0].starts_with('@') {
        return Err(format!("invalid Go response header {header:?}"));
    }
    let index = fields[0][1..]
        .parse()
        .map_err(|_| format!("invalid Go response index {:?}", fields[0]))?;
    let outcome = match fields[1] {
        "A" => GoOutcome::Accepted,
        "P" => GoOutcome::Rejected,
        "R" => GoOutcome::RestoreFailure,
        _ => return Err(format!("invalid Go response status {:?}", fields[1])),
    };
    let statement_count = fields[2]
        .parse()
        .map_err(|_| format!("invalid Go response statement count {:?}", fields[2]))?;
    let payload_len: usize = fields[3]
        .parse()
        .map_err(|_| format!("invalid Go response payload length {:?}", fields[3]))?;
    let mut payload = vec![0; payload_len];
    reader
        .read_exact(&mut payload)
        .map_err(|error| format!("read Go response payload: {error}"))?;
    Ok((index, outcome, statement_count, payload))
}

fn build_godump(root: &Path) -> Result<PathBuf, String> {
    let path = root.join("rust/target/godump-framed-restore");
    let needs_build = match (
        fs::metadata(root.join("rust/difftests/godump/main.go")),
        fs::metadata(&path),
    ) {
        (Ok(source), Ok(binary)) => match (source.modified(), binary.modified()) {
            (Ok(source), Ok(binary)) => source > binary,
            _ => true,
        },
        _ => true,
    };
    if !needs_build {
        return Ok(path);
    }
    let status = Command::new("go")
        .args([
            "build",
            "-p",
            "12",
            "-o",
            path.to_str().expect("workspace path is UTF-8"),
            "./rust/difftests/godump",
        ])
        .current_dir(root)
        .status()
        .map_err(|error| format!("start go build for godump: {error}"))?;
    if !status.success() {
        return Err(format!("go build -p 12 failed with {status}"));
    }
    Ok(path)
}

fn capture_go(root: &Path, inputs: &[Input]) -> Result<Vec<GoldenRecord>, String> {
    let godump = build_godump(root)?;
    let mut child = Command::new(godump)
        .arg("framed-restore")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .map_err(|error| format!("start framed Go parser helper: {error}"))?;
    let mut stdin = child.stdin.take().expect("child stdin is piped");
    let requests: Vec<_> = inputs
        .iter()
        .enumerate()
        .map(|(index, input)| frame_request(index, input.sql.as_bytes()))
        .collect();
    let writer = thread::spawn(move || -> io::Result<()> {
        for request in requests {
            stdin.write_all(&request)?;
        }
        stdin.flush()
    });

    let stdout = child.stdout.take().expect("child stdout is piped");
    let mut reader = BufReader::new(stdout);
    let mut records = Vec::with_capacity(inputs.len());
    for (expected_index, input) in inputs.iter().enumerate() {
        let (index, outcome, statement_count, payload) = read_response(&mut reader)?;
        if index != expected_index {
            return Err(format!(
                "Go response order changed: expected {expected_index}, got {index}"
            ));
        }
        let restores = decode_restores(&payload, statement_count)?;
        if outcome != GoOutcome::Accepted && (!payload.is_empty() || statement_count != 0) {
            return Err(format!(
                "Go response {index} carries payload for non-accepted input"
            ));
        }
        records.push(GoldenRecord {
            input: input.clone(),
            outcome,
            statement_count,
            restores,
        });
    }
    writer
        .join()
        .map_err(|_| "Go parser input writer panicked".to_owned())?
        .map_err(|error| format!("write Go parser frames: {error}"))?;
    let status = child
        .wait()
        .map_err(|error| format!("wait for framed Go parser helper: {error}"))?;
    if !status.success() {
        return Err(format!("framed Go parser helper failed with {status}"));
    }
    Ok(records)
}

fn check(root: &Path) -> Result<(), String> {
    let inventory = read_inventory(root)?;
    let golden = read_golden(root)?;
    if inventory.len() != golden.len() {
        return Err(format!(
            "parser golden is stale: inventory has {} inputs but golden has {}; regenerate with `cd rust && cargo run -p difftest --bin integration_parser_golden -- --write`",
            inventory.len(),
            golden.len()
        ));
    }
    for (index, (input, record)) in inventory.iter().zip(golden.iter()).enumerate() {
        if input != &record.input {
            return Err(format!(
                "parser golden is stale at input {index} ({}:{}-{}); regenerate with `cd rust && cargo run -p difftest --bin integration_parser_golden -- --write`",
                input.path, input.start_line, input.end_line
            ));
        }
    }
    let accepted = golden
        .iter()
        .filter(|record| record.outcome == GoOutcome::Accepted)
        .count();
    let rejected = golden
        .iter()
        .filter(|record| record.outcome == GoOutcome::Rejected)
        .count();
    let restore_failure = golden.len() - accepted - rejected;
    println!(
        "parser golden current: inputs={} go_accepted={} go_rejected={} go_restore_failure={}",
        golden.len(),
        accepted,
        rejected,
        restore_failure
    );
    Ok(())
}

fn write(root: &Path) -> Result<(), String> {
    let inputs = read_inventory(root)?;
    let records = capture_go(root, &inputs)?;
    let path = golden_path(root);
    fs::create_dir_all(path.parent().expect("golden has a parent directory"))
        .map_err(|error| format!("create golden directory: {error}"))?;
    fs::write(&path, render(&records))
        .map_err(|error| format!("write {}: {error}", path.display()))?;
    check(root)
}

fn main() {
    let arguments: Vec<_> = env::args().skip(1).collect();
    let root = repo_root();
    let result = match arguments.as_slice() {
        [command] if command == "--write" => write(&root),
        [command] if command == "--check" => check(&root),
        _ => Err("usage: integration_parser_golden [--write|--check]".to_owned()),
    };
    if let Err(error) = result {
        eprintln!("error: {error}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::{
        decode_restores, encode_restores, escape_tsv, frame_request, hex_decode, hex_encode,
        read_golden, read_response, repo_root, shared_golden, unescape_tsv, GoOutcome,
        SHARED_GOLDEN_LOADS,
    };
    use std::io::Cursor;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;

    #[test]
    fn framed_protocol_keeps_newlines_tabs_and_controls_byte_exact() {
        let sql = b"SELECT 'first\nsecond\t\x01;'; -- @7 3\n";
        let request = frame_request(7, sql);
        let header = format!("@7 {}\n", sql.len());
        assert_eq!(&request[..header.len()], header.as_bytes());
        assert_eq!(&request[header.len()..], sql);

        let restores = vec![b"SELECT _UTF8MB4'first\nsecond\t\x01;'".to_vec()];
        let payload = encode_restores(&restores);
        let mut response = format!("@7 A 1 {}\n", payload.len()).into_bytes();
        response.extend_from_slice(&payload);
        let (index, outcome, count, actual) = read_response(&mut Cursor::new(response)).unwrap();
        assert_eq!(index, 7);
        assert_eq!(outcome, GoOutcome::Accepted);
        assert_eq!(count, 1);
        assert_eq!(decode_restores(&actual, count).unwrap(), restores);
    }

    #[test]
    fn tsv_and_hex_transport_controls_without_line_splitting() {
        let source = "line\nwith\tcontrol\u{1}\\slash";
        assert_eq!(unescape_tsv(&escape_tsv(source)).unwrap(), source);
        assert_eq!(
            hex_decode(&hex_encode(source.as_bytes())).unwrap(),
            source.as_bytes()
        );
    }

    #[test]
    fn repository_oracle_reads_share_one_immutable_allocation() {
        let first = read_golden(&repo_root()).expect("read repository parser oracle");
        let second = read_golden(&repo_root()).expect("read repository parser oracle again");
        assert!(Arc::ptr_eq(&first, &second));
        let borrowed = shared_golden().expect("borrow shared repository parser oracle");
        assert!(std::ptr::eq(first.as_ref(), borrowed));
        assert_eq!(SHARED_GOLDEN_LOADS.load(Ordering::Relaxed), 1);
    }
}
