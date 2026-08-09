// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Rust counterpart of `pkg/parser/generate_keyword/genkeyword.go`.

use std::{env, fs, process::ExitCode};

const RESERVED_KEYWORD_START: &str = "The following tokens belong to ReservedKeyword";
const UNRESERVED_KEYWORD_START: &str = "The following tokens belong to UnReservedKeyword";
const NOT_KEYWORD_START: &str = "The following tokens belong to NotKeywordToken";
const TIDB_KEYWORD_START: &str = "The following tokens belong to TiDBKeyword";

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum Section {
    #[default]
    None,
    Reserved,
    Unreserved,
    TiDb,
}

impl Section {
    fn attributes(self) -> Option<(bool, &'static str)> {
        match self {
            Self::None => None,
            Self::Reserved => Some((true, "reserved")),
            Self::Unreserved => Some((false, "unreserved")),
            Self::TiDb => Some((false, "tidb")),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Keyword<'a> {
    word: &'a str,
    reserved: bool,
    section: &'static str,
}

/// Extracts a quoted keyword from one parser.y token declaration. This is the
/// exact contract of Go's `parseLine`: leading whitespace, one identifier,
/// whitespace, then one quoted word with no trailing characters. MariaDB-only
/// `MONITOR` is deliberately filtered from the public TiDB catalog.
fn parse_line(line: &str) -> Option<&str> {
    if !line.as_bytes().first().is_some_and(u8::is_ascii_whitespace)
        || line.as_bytes().last().is_some_and(u8::is_ascii_whitespace)
    {
        return None;
    }

    let mut fields = line.split_ascii_whitespace();
    let token = fields.next()?;
    let quoted = fields.next()?;
    if fields.next().is_some()
        || !token
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        || quoted.len() < 3
        || !quoted.starts_with('"')
        || !quoted.ends_with('"')
    {
        return None;
    }
    let word = &quoted[1..quoted.len() - 1];
    if !word
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        || word == "MONITOR"
    {
        return None;
    }
    Some(word)
}

fn parse_catalog(parser_y: &str) -> Vec<Keyword<'_>> {
    let mut section = Section::None;
    let mut keywords = Vec::new();
    for line in parser_y.lines() {
        if line.is_empty() {
            section = Section::None;
        } else if line.contains(RESERVED_KEYWORD_START) {
            section = Section::Reserved;
        } else if line.contains(UNRESERVED_KEYWORD_START) {
            section = Section::Unreserved;
        } else if line.contains(TIDB_KEYWORD_START) {
            section = Section::TiDb;
        } else if line.contains(NOT_KEYWORD_START) {
            section = Section::None;
        }

        if let (Some(word), Some((reserved, section_name))) =
            (parse_line(line), section.attributes())
        {
            keywords.push(Keyword {
                word,
                reserved,
                section: section_name,
            });
        }
    }
    keywords
}

fn check_catalog(generated: &[Keyword<'_>]) -> Result<(), String> {
    if generated.len() != tidb_lexer::KEYWORDS.len() {
        return Err(format!(
            "keyword count differs: parser.y has {}, Rust has {}",
            generated.len(),
            tidb_lexer::KEYWORDS.len()
        ));
    }
    for (index, (generated, rust)) in generated
        .iter()
        .zip(tidb_lexer::KEYWORDS.iter())
        .enumerate()
    {
        if generated.word != rust.word
            || generated.reserved != rust.reserved
            || generated.section != rust.section
        {
            return Err(format!(
                "keyword {index} differs: parser.y={generated:?}, Rust={rust:?}"
            ));
        }
    }
    Ok(())
}

fn print_rust_catalog(keywords: &[Keyword<'_>]) {
    println!("use super::Keyword;\n");
    println!("pub static KEYWORDS: &[Keyword] = &[");
    for keyword in keywords {
        println!(
            "    Keyword {{ word: {:?}, reserved: {}, section: {:?} }},",
            keyword.word, keyword.reserved, keyword.section
        );
    }
    println!("];");
}

fn run() -> Result<(), String> {
    let mut args = env::args().skip(1);
    let parser_y = args
        .next()
        .ok_or_else(|| "usage: generate_keyword <parser.y> [--check]".to_owned())?;
    let mode = args.next();
    if args.next().is_some() || mode.as_deref().is_some_and(|value| value != "--check") {
        return Err("usage: generate_keyword <parser.y> [--check]".to_owned());
    }
    let source = fs::read_to_string(&parser_y)
        .map_err(|error| format!("failed to read {parser_y}: {error}"))?;
    let keywords = parse_catalog(&source);
    if mode.is_some() {
        check_catalog(&keywords)
    } else {
        print_rust_catalog(&keywords);
        Ok(())
    }
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Direct transcreation of
    /// `pkg/parser/generate_keyword/genkeyword_test.go::TestParseLine`.
    #[test]
    fn test_parse_line() {
        assert_eq!(parse_line("\tadd               \"ADD\""), Some("ADD"));
        assert_eq!(
            parse_line("\ttidbCurrentTSO    \"TIDB_CURRENT_TSO\""),
            Some("TIDB_CURRENT_TSO")
        );
    }
}
