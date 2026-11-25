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
//! collapses into the [`Matcher`] enum. Rust's `regex` supports the accepted
//! package's RE2-style pattern surface; compiler-owned diagnostics differ (see
//! [`new_regexp_matcher`]).

use super::{go_simple_lowercase, FilterError};
use std::borrow::Cow;
use std::ops::Range;
use std::sync::Arc;
use tidb_go_regexp::RegexpPredicate;
use tidb_mysql::simple_fold;

/// Matches a name against a pattern (Go's `matcher` interface).
#[derive(Clone)]
pub(crate) enum Matcher {
    /// A literal string (`stringMatcher`).
    Str(String),
    /// Matches everything, the `*` pattern (`trueMatcher`).
    True,
    /// A glob that can be matched without a regular-expression engine.
    Glob(SimpleGlob),
    /// A regular-expression matcher (`regexpMatcher`).
    Regexp(Box<RegexpMatcher>),
}

#[derive(Clone)]
pub(crate) struct RegexpMatcher {
    pattern: Arc<str>,
    predicate: RegexpPredicate,
    case_sensitive: bool,
}

impl RegexpMatcher {
    fn compile(pattern: Arc<str>, case_sensitive: bool) -> Result<Self, String> {
        let predicate = tidb_go_regexp::compile_predicate(&pattern, !case_sensitive, false, false)?;
        Ok(Self { pattern, predicate, case_sensitive })
    }

    fn to_lower(&self) -> Self {
        if !self.case_sensitive {
            return self.clone();
        }
        Self::compile(Arc::clone(&self.pattern), false)
            .expect("case-insensitive Go regexp compilation")
    }
}

impl Matcher {
    pub(crate) fn literal(&self) -> Option<&str> {
        match self {
            Matcher::Str(value) => Some(value),
            _ => None,
        }
    }

    pub(crate) fn match_string(&self, name: &str) -> bool {
        match self {
            Matcher::Str(s) => s == name,
            Matcher::True => true,
            Matcher::Glob(glob) => glob.matches(name),
            Matcher::Regexp(r) => r.predicate.is_match(name),
        }
    }

    pub(crate) fn match_all_strings(&self) -> bool {
        matches!(self, Matcher::True)
    }

    pub(crate) fn to_lower(&self) -> Matcher {
        match self {
            Matcher::Str(s) => Matcher::Str(go_simple_lowercase(s)),
            Matcher::True => Matcher::True,
            Matcher::Glob(glob) => glob.to_lower(),
            Matcher::Regexp(r) => Matcher::Regexp(Box::new(r.to_lower())),
        }
    }
}

#[derive(Clone)]
pub(crate) struct SimpleGlob {
    pattern: String,
    fixed: Option<Box<FixedGlob>>,
    folded: bool,
}

impl SimpleGlob {
    pub(crate) fn new(pattern: &str) -> Option<Self> {
        let fixed = if pattern
            .bytes()
            .all(|byte| byte == b'*' || is_glob_literal(byte))
        {
            None
        } else {
            Some(Box::new(FixedGlob::parse(pattern)?))
        };
        Some(Self {
            pattern: pattern.to_owned(),
            fixed,
            folded: false,
        })
    }

    fn matches(&self, name: &str) -> bool {
        let name = if self.folded {
            fold_glob_text(name)
        } else {
            Cow::Borrowed(name)
        };
        let name = name.as_ref();
        if let Some(fixed) = &self.fixed {
            return fixed.matches(&self.pattern, name, self.folded);
        }

        // The literal pieces between stars are disjoint. The earliest match
        // for each piece leaves the most room for the remaining pieces.
        let mut pieces = self.pattern.split('*');
        let prefix = pieces.next().expect("glob has a prefix");
        let Some(suffix) = pieces.next_back() else {
            return prefix == name;
        };
        let Some(mut rest) = name
            .strip_prefix(prefix)
            .and_then(|rest| rest.strip_suffix(suffix))
        else {
            return false;
        };
        for literal in pieces.filter(|piece| !piece.is_empty()) {
            let Some((_, after)) = rest.split_once(literal) else {
                return false;
            };
            rest = after;
        }
        true
    }

    fn to_lower(&self) -> Matcher {
        // Folding ASCII literals preserves the stored byte ranges. A fixed
        // pattern with Unicode literals still goes through the regex engine.
        if self.fixed.is_none() || self.pattern.is_ascii() {
            return Matcher::Glob(Self {
                pattern: fold_glob_text(&self.pattern).into_owned(),
                fixed: self.fixed.clone(),
                folded: true,
            });
        }
        // Case folding can add characters outside a printed range. Let the
        // regex engine expand classes for case-insensitive fixed patterns.
        let pattern = self.pattern.replace('*', ".*").replace('?', ".");
        Matcher::Regexp(Box::new(
            RegexpMatcher::compile(Arc::from(format!("(?s)^{pattern}$")), false)
                .expect("validated simple glob"),
        ))
    }
}

fn fold_glob_text(value: &str) -> Cow<'_, str> {
    if value.is_ascii() {
        return if value.bytes().any(|byte| byte.is_ascii_uppercase()) {
            Cow::Owned(value.to_ascii_lowercase())
        } else {
            Cow::Borrowed(value)
        };
    }

    let mut folded = String::with_capacity(value.len());
    folded.extend(value.chars().map(|mut ch| loop {
        if ch.is_ascii() {
            break ch.to_ascii_lowercase();
        }
        let next = simple_fold(ch);
        // Go visits the orbit in ascending order before wrapping to its
        // smallest rune. Share the ASCII fast path's spelling.
        if next <= ch {
            break next.to_ascii_lowercase();
        }
        ch = next;
    }));
    Cow::Owned(folded)
}

fn is_glob_literal(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_' || !byte.is_ascii()
}

#[derive(Clone)]
struct FixedGlob {
    pieces: Vec<FixedPiece>,
    leading_star: bool,
    trailing_star: bool,
}

#[derive(Clone)]
enum FixedPiece {
    Literal(Range<usize>),
    Any,
    AsciiRange(u8, u8),
}

impl FixedGlob {
    fn parse(pattern: &str) -> Option<Self> {
        let start = pattern.len() - pattern.trim_start_matches('*').len();
        let end = pattern.trim_end_matches('*').len();
        let leading_star = start != 0;
        let trailing_star = end != pattern.len();
        if leading_star && trailing_star {
            return None;
        }

        let bytes = pattern.as_bytes();
        let mut pieces = Vec::new();
        let mut literal_start = start;
        let mut index = start;
        while index < end {
            let (piece, width) = match &bytes[index..end] {
                [b'?', ..] => (FixedPiece::Any, 1),
                [b'[', first, b'-', last, b']', ..]
                    if first.is_ascii_alphanumeric()
                        && last.is_ascii_alphanumeric()
                        && first <= last =>
                {
                    (FixedPiece::AsciiRange(*first, *last), 5)
                }
                [byte, ..] if is_glob_literal(*byte) => {
                    index += 1;
                    continue;
                }
                _ => return None,
            };
            if literal_start < index {
                pieces.push(FixedPiece::Literal(literal_start..index));
            }
            pieces.push(piece);
            index += width;
            literal_start = index;
        }
        if literal_start < end {
            pieces.push(FixedPiece::Literal(literal_start..end));
        }
        Some(Self {
            pieces,
            leading_star,
            trailing_star,
        })
    }

    fn matches(&self, pattern: &str, mut name: &str, folded: bool) -> bool {
        if self.leading_star {
            for piece in self.pieces.iter().rev() {
                let Some(rest) = piece.strip(pattern, name, true, folded) else {
                    return false;
                };
                name = rest;
            }
            return true;
        }

        for piece in &self.pieces {
            let Some(rest) = piece.strip(pattern, name, false, folded) else {
                return false;
            };
            name = rest;
        }
        self.trailing_star || name.is_empty()
    }
}

impl FixedPiece {
    fn strip<'a>(
        &self,
        pattern: &str,
        name: &'a str,
        from_end: bool,
        folded: bool,
    ) -> Option<&'a str> {
        if let Self::Literal(range) = self {
            let literal = &pattern[range.clone()];
            return if from_end {
                name.strip_suffix(literal)
            } else {
                name.strip_prefix(literal)
            };
        }
        let mut chars = name.chars();
        let ch = if from_end {
            chars.next_back()?
        } else {
            chars.next()?
        };
        if let Self::AsciiRange(first, last) = self {
            // A folded rune with any ASCII member is its lowercase ASCII
            // representative. The range may contain either spelling.
            let range = char::from(*first)..=char::from(*last);
            if !(range.contains(&ch) || folded && range.contains(&ch.to_ascii_uppercase())) {
                return None;
            }
        }
        Some(chars.as_str())
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
    match RegexpMatcher::compile(Arc::from(pat), true) {
        Ok(re) => Ok(Matcher::Regexp(Box::new(re))),
        Err(e) => Err(FilterError::new(format!(
            "error parsing regexp: {}",
            flatten(&e)
        ))),
    }
}

/// Collapses the multi-line `regex::Error` display into one line.
fn flatten(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

#[cfg(test)]
mod tests {
    use super::{new_regexp_matcher, Matcher, SimpleGlob};

    #[test]
    fn go_perl_character_classes_are_ascii() {
        let digits = new_regexp_matcher(r"^\d+$").unwrap();
        assert!(digits.match_string("123"));
        assert!(!digits.match_string("١٢٣"));

        let words = new_regexp_matcher(r"^\w+$").unwrap();
        assert!(words.match_string("tenant_1"));
        assert!(!words.match_string("ténant"));
    }

    #[test]
    fn simple_globs_keep_go_case_fold_orbits_and_range_endpoints() {
        let pattern = Matcher::Glob(SimpleGlob::new("prefix[A-Z]*").unwrap());
        let folded = pattern.to_lower();
        assert!(folded.match_string("PREFIXſ_tail"));
        assert!(folded.match_string("prefixK_tail"));
        assert!(!folded.match_string("prefixé_tail"));

        let cross_case = Matcher::Glob(SimpleGlob::new("*[A-z]").unwrap()).to_lower();
        assert!(cross_case.match_string("name_"));
        assert!(!cross_case.match_string("name@"));
    }
}
