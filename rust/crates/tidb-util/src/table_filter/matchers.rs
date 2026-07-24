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

//! Transcreation of Go `pkg/util/table-filter/matchers.go`.
//!
//! Go's `matcher` interface (`stringMatcher`, `trueMatcher`, `regexpMatcher`)
//! collapses into the [`Matcher`] enum. Go's `regexp` and Rust's `regex` are
//! both RE2-lineage, so the compiled patterns match identically; only the
//! compiler's error *text* differs (see [`new_regexp_matcher`]).

use super::FilterError;
use regex::Regex;

/// Matches a name against a pattern (Go's `matcher` interface).
#[derive(Clone)]
pub(crate) enum Matcher {
    /// A literal string (`stringMatcher`).
    Str(String),
    /// Matches everything, the `*` pattern (`trueMatcher`).
    True,
    /// A regular-expression matcher (`regexpMatcher`).
    Regexp(Regex),
}

impl Matcher {
    pub(crate) fn match_string(&self, name: &str) -> bool {
        match self {
            Matcher::Str(s) => s == name,
            Matcher::True => true,
            Matcher::Regexp(r) => r.is_match(name),
        }
    }

    pub(crate) fn match_all_strings(&self) -> bool {
        matches!(self, Matcher::True)
    }

    pub(crate) fn to_lower(&self) -> Matcher {
        match self {
            Matcher::Str(s) => Matcher::Str(s.to_lowercase()),
            Matcher::True => Matcher::True,
            Matcher::Regexp(r) => Matcher::Regexp(
                Regex::new(&format!("(?i){}", r.as_str()))
                    .expect("adding (?i) to a compiled pattern always compiles"),
            ),
        }
    }
}

/// A rule of a table filter: a schema and table pattern, positive (accept) or
/// negative (deny).
pub(crate) struct TableRule {
    pub(crate) schema: Matcher,
    pub(crate) table: Matcher,
    pub(crate) positive: bool,
}

/// A rule of a column filter.
pub(crate) struct ColumnRule {
    pub(crate) column: Matcher,
    pub(crate) positive: bool,
}

/// Compiles a regular-expression matcher, with the `(?s)^.*$` special case for
/// `*` returning [`Matcher::True`].
///
/// Go returns `regexp`'s `error parsing regexp: ...` text on failure. Rust's
/// `regex` rejects exactly the same patterns (both are RE2 — no look-around, no
/// unclosed classes) but words the error differently; the message is prefixed
/// with the same `error parsing regexp:` and carries the underlying `regex`
/// text.
pub(crate) fn new_regexp_matcher(pat: &str) -> Result<Matcher, FilterError> {
    if pat == "(?s)^.*$" {
        // special case for '*'
        return Ok(Matcher::True);
    }
    match Regex::new(pat) {
        Ok(re) => Ok(Matcher::Regexp(re)),
        Err(e) => Err(FilterError::new(format!(
            "error parsing regexp: {}",
            flatten(&e.to_string())
        ))),
    }
}

/// Collapses the multi-line `regex::Error` display into one line.
fn flatten(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}
