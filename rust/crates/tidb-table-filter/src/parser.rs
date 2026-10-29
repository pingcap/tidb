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

//! Transcreation of Go `pkg/util/table-filter/parser.go`.
//!
//! Parses serialized filter rules into [`Matcher`]s. Wildcard patterns
//! (`*`, `?`, `[...]`) are compiled into anchored `(?s)^...$` regexes, exactly
//! as Go does. The `@file` directive imports rules from a file.

use super::matchers::{new_regexp_matcher, ColumnRule, Matcher, TableRule};
use super::FilterError;
use regex::Regex;
use std::sync::LazyLock;

static REGEXP_RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^/(?:\\.|[^/])+/").unwrap());
static DOUBLE_QUOTED_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#"^"(?:""|[^"])+""#).unwrap());
static BACKQUOTED_RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^`(?:``|[^`])+`").unwrap());
static WILDCARD_RANGE_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^\[!?(?:\\[^0-9a-zA-Z]|[^\\\]])+\]").unwrap());

/// Tracks the file name and line number for error reporting.
pub(crate) struct MatcherParser {
    pub(crate) file_name: String,
    pub(crate) line_num: i64,
}

impl MatcherParser {
    pub(crate) fn new() -> Self {
        MatcherParser {
            file_name: "<cmdline>".to_string(),
            line_num: 1,
        }
    }

    pub(crate) fn errorf(&self, msg: &str) -> FilterError {
        FilterError::new(format!("at {}:{}: {}", self.file_name, self.line_num, msg))
    }

    pub(crate) fn annotate(&self, err: &FilterError, msg: &str) -> FilterError {
        FilterError::new(format!(
            "at {}:{}: {}: {}",
            self.file_name, self.line_num, msg, err
        ))
    }

    fn new_regexp_matcher(&self, pat: &str) -> Result<Matcher, FilterError> {
        new_regexp_matcher(pat).map_err(|e| self.annotate(&e, "invalid pattern"))
    }

    pub(crate) fn parse_pattern<'a>(
        &self,
        line: &'a str,
        needs_dot_separator: bool,
    ) -> Result<(Matcher, &'a str), FilterError> {
        if line.is_empty() {
            return Err(self.errorf("syntax error: missing pattern"));
        }

        match line.as_bytes()[0] {
            b'/' => {
                // a regexp pattern
                let end = match REGEXP_RE.find(line) {
                    Some(m) => m.end(),
                    None => return Err(self.errorf("syntax error: incomplete regexp")),
                };
                let m = self.new_regexp_matcher(&line[1..end - 1])?;
                Ok((m, &line[end..]))
            }
            b'"' => {
                // a double-quoted pattern
                let end = match DOUBLE_QUOTED_RE.find(line) {
                    Some(m) => m.end(),
                    None => return Err(self.errorf("syntax error: incomplete quoted identifier")),
                };
                let name = line[1..end - 1].replace("\"\"", "\"");
                Ok((Matcher::Str(name), &line[end..]))
            }
            b'`' => {
                // a backquoted pattern
                let end = match BACKQUOTED_RE.find(line) {
                    Some(m) => m.end(),
                    None => return Err(self.errorf("syntax error: incomplete quoted identifier")),
                };
                let name = line[1..end - 1].replace("``", "`");
                Ok((Matcher::Str(name), &line[end..]))
            }
            _ => self.parse_wildcard_pattern(line, needs_dot_separator),
        }
    }

    fn parse_wildcard_pattern<'a>(
        &self,
        line: &'a str,
        needs_dot_separator: bool,
    ) -> Result<(Matcher, &'a str), FilterError> {
        let bytes = line.as_bytes();
        let mut literal: Vec<u8> = Vec::with_capacity(bytes.len());
        let mut wildcard: Vec<u8> = Vec::with_capacity(bytes.len() + 6);
        let mut is_literal_string = true;
        wildcard.extend_from_slice(b"(?s)^");

        let mut i = 0;
        while i < bytes.len() {
            let c = bytes[i];
            match c {
                b'\\' => {
                    // escape character
                    if i == bytes.len() - 1 {
                        return Err(self.errorf("syntax error: cannot place \\ at end of line"));
                    }
                    let esc = bytes[i + 1];
                    if esc.is_ascii_alphanumeric() {
                        return Err(self.errorf(&format!(
                            "cannot escape a letter or number (\\{}), it is reserved for future extension",
                            esc as char
                        )));
                    }
                    if is_literal_string {
                        literal.push(esc);
                    }
                    if esc < 0x80 {
                        wildcard.push(b'\\');
                    }
                    wildcard.push(esc);
                    i += 2;
                }
                b'.' => {
                    if needs_dot_separator {
                        // table separator, end now.
                        break;
                    }
                    return Err(
                        self.errorf(&format!("unexpected special character '{}'", c as char))
                    );
                }
                b'*' => {
                    // wildcard
                    is_literal_string = false;
                    wildcard.extend_from_slice(b".*");
                    i += 1;
                }
                b'?' => {
                    is_literal_string = false;
                    wildcard.push(b'.');
                    i += 1;
                }
                b'[' => {
                    // range of characters
                    is_literal_string = false;
                    let end = match WILDCARD_RANGE_RE.find(&line[i..]) {
                        Some(m) => i + m.end(),
                        None => {
                            return Err(self.errorf("syntax error: failed to parse character class"))
                        }
                    };
                    match bytes[i + 1] {
                        b'!' => {
                            wildcard.extend_from_slice(b"[^");
                            wildcard.extend_from_slice(&bytes[i + 2..end]);
                        }
                        // `[^` is not special in a glob pattern. escape it.
                        b'^' => {
                            wildcard.extend_from_slice(br"[\^");
                            wildcard.extend_from_slice(&bytes[i + 2..end]);
                        }
                        _ => {
                            wildcard.extend_from_slice(&bytes[i..end]);
                        }
                    }
                    i = end;
                }
                _ => {
                    if !(c == b'$' || c == b'_' || c.is_ascii_alphanumeric() || c >= 0x80) {
                        return Err(
                            self.errorf(&format!("unexpected special character '{}'", c as char))
                        );
                    }
                    literal.push(c);
                    wildcard.push(c);
                    i += 1;
                }
            }
        }

        let remaining = &line[i..];
        if is_literal_string {
            return Ok((
                Matcher::Str(String::from_utf8(literal).expect("input is valid UTF-8")),
                remaining,
            ));
        }
        wildcard.push(b'$');
        let pat = String::from_utf8(wildcard).expect("wildcard regex is valid UTF-8");
        let m = self.new_regexp_matcher(&pat)?;
        Ok((m, remaining))
    }
}

/// Shared behavior for the table- and column-rule parsers, including the `@file`
/// import.
pub(crate) trait RuleParser {
    fn mp(&self) -> &MatcherParser;
    fn mp_mut(&mut self) -> &mut MatcherParser;
    fn parse(&mut self, line: &str, can_import: bool) -> Result<(), FilterError>;

    fn import_file(&mut self, file_name: &str) -> Result<(), FilterError> {
        let content = match std::fs::read_to_string(file_name) {
            Ok(c) => c,
            Err(e) => {
                // Mirror Go's `open <path>: <reason>` phrasing so the path is
                // reported; the trailing reason is the Rust io message.
                let inner = FilterError::new(format!("open {file_name}: {e}"));
                return Err(self.mp().annotate(&inner, "cannot open filter file"));
            }
        };

        let old_file_name = self.mp().file_name.clone();
        let old_line_num = self.mp().line_num;
        self.mp_mut().file_name = file_name.to_string();
        self.mp_mut().line_num = 1;

        for line in content.lines() {
            self.parse(line, false)?;
            self.mp_mut().line_num += 1;
        }

        self.mp_mut().file_name = old_file_name;
        self.mp_mut().line_num = old_line_num;
        Ok(())
    }
}

/// Parses table-filter rules.
pub(crate) struct TableRulesParser {
    pub(crate) rules: Vec<TableRule>,
    pub(crate) mp: MatcherParser,
}

impl RuleParser for TableRulesParser {
    fn mp(&self) -> &MatcherParser {
        &self.mp
    }

    fn mp_mut(&mut self) -> &mut MatcherParser {
        &mut self.mp
    }

    fn parse(&mut self, line: &str, can_import: bool) -> Result<(), FilterError> {
        let line = line.trim_matches(|c| c == ' ' || c == '\t');
        if line.is_empty() {
            return Ok(());
        }

        let mut positive = true;
        let mut rest = line;
        match line.as_bytes()[0] {
            b'#' => return Ok(()),
            b'!' => {
                positive = false;
                rest = &line[1..];
            }
            b'@' => {
                if !can_import {
                    return Err(self
                        .mp
                        .errorf("importing filter files recursively is not allowed"));
                }
                return self.import_file(&line[1..]);
            }
            _ => {}
        }

        let (sm, rest) = self.mp.parse_pattern(rest, true)?;
        if rest.is_empty() {
            return Err(self.mp.errorf("wrong table pattern"));
        }
        if rest.as_bytes()[0] != b'.' {
            return Err(self
                .mp
                .errorf("syntax error: missing '.' between schema and table patterns"));
        }

        let (tm, rest) = self.mp.parse_pattern(&rest[1..], true)?;
        if !rest.is_empty() {
            return Err(self
                .mp
                .errorf("syntax error: stray characters after table pattern"));
        }

        self.rules.push(TableRule {
            schema: sm,
            table: tm,
            positive,
        });
        Ok(())
    }
}

/// Parses column-filter rules.
pub(crate) struct ColumnRulesParser {
    pub(crate) rules: Vec<ColumnRule>,
    pub(crate) mp: MatcherParser,
}

impl RuleParser for ColumnRulesParser {
    fn mp(&self) -> &MatcherParser {
        &self.mp
    }

    fn mp_mut(&mut self) -> &mut MatcherParser {
        &mut self.mp
    }

    fn parse(&mut self, line: &str, can_import: bool) -> Result<(), FilterError> {
        let line = line.trim_matches(|c| c == ' ' || c == '\t');
        if line.is_empty() {
            return Ok(());
        }

        let mut positive = true;
        let mut rest = line;
        match line.as_bytes()[0] {
            b'#' => return Ok(()),
            b'!' => {
                positive = false;
                rest = &line[1..];
            }
            b'@' => {
                if !can_import {
                    return Err(self
                        .mp
                        .errorf("importing filter files recursively is not allowed"));
                }
                return self.import_file(&line[1..]);
            }
            _ => {}
        }

        let (cm, rest) = self.mp.parse_pattern(rest, false)?;
        if !rest.is_empty() {
            return Err(self
                .mp
                .errorf("syntax error: stray characters after column pattern"));
        }

        self.rules.push(ColumnRule {
            // Column names and aliases are not case-sensitive on any platform,
            // so always match in lowercase.
            column: cm.to_lower(),
            positive,
        });
        Ok(())
    }
}
