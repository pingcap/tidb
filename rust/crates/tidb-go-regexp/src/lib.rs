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

//! Shared compatibility with Go's `regexp` character-class semantics.

use memchr::memchr2_iter;
use regex::{Regex, RegexBuilder};
use regex_syntax::ast::parse::ParserBuilder;
use regex_syntax::ast::{self, Ast, Visitor};
use std::borrow::Cow;
use std::cell::RefCell;

mod fallback;
mod literal;
mod unicode_class;

const MAX_CACHED_PATTERN_BYTES: usize = 256;
const MAX_DIRECT_PATTERN_BYTES: usize = 256;

#[derive(Clone, Copy, PartialEq, Eq)]
struct CompileFlags {
    case_insensitive: bool,
    multi_line: bool,
    dot_matches_new_line: bool,
}

struct RecentRegexp {
    pattern: String,
    flags: Option<CompileFlags>,
    regexp: Option<Regex>,
}

impl RecentRegexp {
    const EMPTY: Self = Self {
        pattern: String::new(),
        flags: None,
        regexp: None,
    };
}

thread_local! {
    // Repeated dynamic patterns commonly come from the same scan. A small
    // per-thread cache also handles two interleaved regexes without locking.
    static RECENT_REGEXPS: RefCell<[RecentRegexp; 2]> = const {
        RefCell::new([RecentRegexp::EMPTY, RecentRegexp::EMPTY])
    };
}

fn recent_entry<'a>(
    entries: &'a mut [RecentRegexp; 2],
    pattern: &str,
    flags: CompileFlags,
) -> (&'a mut RecentRegexp, bool) {
    if let Some(position) = entries
        .iter()
        .position(|entry| entry.flags == Some(flags) && entry.pattern == pattern)
    {
        entries.swap(0, position);
        return (&mut entries[0], true);
    }
    entries.swap(0, 1);
    let entry = &mut entries[0];
    entry.pattern.clear();
    entry.pattern.push_str(pattern);
    entry.flags = Some(flags);
    entry.regexp = None;
    (entry, false)
}

#[derive(Clone, Copy)]
struct RegexReplacement {
    start: usize,
    end: usize,
    value: &'static str,
}

#[derive(Default)]
struct GoRegexpVisitor {
    replacements: Vec<RegexReplacement>,
}

impl GoRegexpVisitor {
    fn perl_class(class: &ast::ClassPerl) -> RegexReplacement {
        let value = match (&class.kind, class.negated) {
            (ast::ClassPerlKind::Digit, false) => "[0-9]",
            (ast::ClassPerlKind::Digit, true) => "[^0-9]",
            (ast::ClassPerlKind::Space, false) => "[\\t\\n\\f\\r ]",
            (ast::ClassPerlKind::Space, true) => "[^\\t\\n\\f\\r ]",
            (ast::ClassPerlKind::Word, false) => "[0-9A-Za-z_]",
            (ast::ClassPerlKind::Word, true) => "[^0-9A-Za-z_]",
        };
        RegexReplacement {
            start: class.span.start.offset,
            end: class.span.end.offset,
            value,
        }
    }
}

impl Visitor for GoRegexpVisitor {
    type Output = Vec<RegexReplacement>;
    type Err = String;

    fn finish(self) -> Result<Self::Output, Self::Err> {
        Ok(self.replacements)
    }

    fn visit_pre(&mut self, node: &Ast) -> Result<(), Self::Err> {
        match node {
            Ast::Flags(flags) => validate_flags(&flags.flags)?,
            Ast::Group(group) => {
                if let ast::GroupKind::NonCapturing(flags) = &group.kind {
                    validate_flags(flags)?;
                }
            }
            Ast::Repetition(repetition) => {
                // Go rejects adjacent repeat operators, but permits grouped
                // repeats. The AST preserves that distinction.
                if matches!(repetition.ast.as_ref(), Ast::Repetition(_)) {
                    return Err("invalid nested repetition operator".to_owned());
                }
                if counted_repeat_limit(repetition).is_some_and(|count| count >= 2) {
                    validate_repeats(node)?;
                }
            }
            Ast::ClassPerl(class) => self.replacements.push(Self::perl_class(class)),
            Ast::Assertion(assertion) => {
                let value = match assertion.kind {
                    ast::AssertionKind::WordBoundary => Some("(?-u:\\b)"),
                    ast::AssertionKind::NotWordBoundary => Some("(?-u:\\B)"),
                    _ => None,
                };
                if let Some(value) = value {
                    self.replacements.push(RegexReplacement {
                        start: assertion.span.start.offset,
                        end: assertion.span.end.offset,
                        value,
                    });
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn visit_class_set_item_pre(&mut self, item: &ast::ClassSetItem) -> Result<(), Self::Err> {
        if let ast::ClassSetItem::Perl(class) = item {
            self.replacements.push(Self::perl_class(class));
        }
        Ok(())
    }
}

// Go's regexp package defines Perl character classes and word boundaries over
// ASCII. Rust regex deliberately makes the same spellings Unicode-aware.
// Rewrite only those constructs; Unicode literals, `.`, and `\p{...}` retain
// their normal rune semantics.
fn rewrite_pattern(pattern: &str) -> Result<Cow<'_, str>, String> {
    // The regex compiler can parse ordinary literals, groups, anchors and
    // standalone * or + operators directly. The other constructs can need
    // Go-specific normalization or repetition and flag validation.
    let mut previous_repeat = false;
    let needs_compatibility_parse = pattern.bytes().any(|byte| {
        let repeat = matches!(byte, b'*' | b'+');
        let needs_parse = matches!(byte, b'\\' | b'[' | b'{' | b'?')
            || (repeat && previous_repeat);
        previous_repeat = repeat;
        needs_parse
    });
    if !needs_compatibility_parse {
        return Ok(Cow::Borrowed(pattern));
    }
    let normalized = normalize_pattern(pattern)?;
    let ast = parse_ast(&normalized)?;
    rewrite_ast(normalized, &ast)
}

fn parse_ast(pattern: &str) -> Result<Ast, String> {
    ParserBuilder::new()
        .octal(true)
        .nest_limit(1000)
        .build()
        .parse(pattern)
        .map_err(|error| error.to_string())
}

fn rewrite_ast<'a>(normalized: Cow<'a, str>, ast: &Ast) -> Result<Cow<'a, str>, String> {
    let mut replacements = ast::visit(ast, GoRegexpVisitor::default())?;
    if replacements.is_empty() {
        return Ok(normalized);
    }
    replacements.sort_unstable_by_key(|replacement| std::cmp::Reverse(replacement.start));
    let mut result = normalized.into_owned();
    for replacement in replacements {
        result.replace_range(replacement.start..replacement.end, replacement.value);
    }
    Ok(Cow::Owned(result))
}

fn validate_flags(flags: &ast::Flags) -> Result<(), String> {
    for item in &flags.items {
        if let ast::FlagsItemKind::Flag(flag) = &item.kind {
            if !matches!(
                flag,
                ast::Flag::CaseInsensitive
                    | ast::Flag::MultiLine
                    | ast::Flag::DotMatchesNewLine
                    | ast::Flag::SwapGreed
            ) {
                return Err("invalid or unsupported Perl syntax".to_owned());
            }
        }
    }
    Ok(())
}

fn counted_repeat_limit(repetition: &ast::Repetition) -> Option<u32> {
    match &repetition.op.kind {
        ast::RepetitionKind::Range(
            ast::RepetitionRange::Exactly(n)
            | ast::RepetitionRange::AtLeast(n)
            | ast::RepetitionRange::Bounded(_, n),
        ) => Some(*n),
        _ => None,
    }
}

// Go's parser checks the repeat budget only when an enclosing count is at
// least two. Every nested repeat is also visited and checked independently.
fn validate_repeats(root: &Ast) -> Result<(), String> {
    let mut pending = vec![(root, 1000u32)];
    while let Some((node, mut budget)) = pending.pop() {
        match node {
            Ast::Repetition(repetition) => {
                if let Some(count) = counted_repeat_limit(repetition) {
                    if count == 0 {
                        continue;
                    }
                    if count > budget {
                        return Err("invalid repeat count".to_owned());
                    }
                    budget /= count;
                }
                pending.push((&repetition.ast, budget));
            }
            Ast::Group(group) => pending.push((&group.ast, budget)),
            Ast::Alternation(parts) => pending.extend(parts.asts.iter().map(|node| (node, budget))),
            Ast::Concat(parts) => pending.extend(parts.asts.iter().map(|node| (node, budget))),
            _ => {}
        }
    }
    Ok(())
}

// Normalize Go's bracket grammar before the backend AST parser: '&', '~',
// and nested '[' are ordinary class literals, never Rust set operations.
// Preserve POSIX classes and escapes as syntactic units. Go's quoted literal
// regions are escaped once here, before any class/flag rewriting.
fn normalize_pattern(pattern: &str) -> Result<Cow<'_, str>, String> {
    if !pattern.bytes().any(|byte| matches!(byte, b'\\' | b'[' | b'{')) {
        return Ok(Cow::Borrowed(pattern));
    }
    let mut output = String::with_capacity(pattern.len());
    let mut chars = pattern.chars().peekable();
    let mut in_class = false;
    let mut first = false;
    let mut negated = false;
    while let Some(ch) = chars.next() {
        if ch == '\\' {
            let escaped = chars
                .next()
                .ok_or_else(|| "trailing backslash".to_owned())?;
            if escaped == 'Q' && !in_class {
                let mut quoted = String::new();
                while let Some(ch) = chars.next() {
                    if ch == '\\' && chars.peek() == Some(&'E') {
                        chars.next();
                        break;
                    }
                    quoted.push(ch);
                }
                output.push_str(&regex::escape(&quoted));
            } else {
                if matches!(escaped, 'u' | 'U')
                    || (('1'..='7').contains(&escaped)
                        && !chars.peek().is_some_and(|ch| ('0'..='7').contains(ch)))
                {
                    return Err("invalid escape sequence".to_owned());
                }
                output.push('\\');
                output.push(escaped);
                // `\p{Greek}`, `\x{61}`: the braces belong to the escape.
                if matches!(escaped, 'p' | 'P' | 'x') && chars.peek() == Some(&'{') {
                    for part in chars.by_ref() {
                        output.push(part);
                        if part == '}' {
                            break;
                        }
                    }
                }
            }
            first = false;
        } else if in_class {
            match ch {
                ']' if !first => {
                    output.push(ch);
                    in_class = false;
                }
                '^' if first && !negated => {
                    negated = true;
                    output.push(ch);
                    continue;
                }
                '[' if chars.peek() == Some(&':') => {
                    output.push(ch);
                    for part in chars.by_ref() {
                        output.push(part);
                        if part == ']' {
                            break;
                        }
                    }
                }
                '-' if first => output.push_str(r"\x2D"),
                '-' if chars.peek() == Some(&'-') => {
                    // The first hyphen is Go's range operator; the second
                    // is its literal endpoint, never set subtraction.
                    chars.next();
                    output.push_str(r"-\x2D");
                }
                '[' | ']' | '&' | '~' => {
                    output.push('\\');
                    output.push(ch);
                }
                _ => output.push(ch),
            }
            first = false;
        } else {
            if ch == '[' {
                in_class = true;
                first = true;
                negated = false;
            }
            // Go `regexp/syntax.parseRepeat`: a `{` that does not open a valid
            // `{n}`, `{n,}` or `{n,m}` is an ordinary literal, where the Rust
            // `regex` crate rejects it.
            if ch == '{' && !is_counted_repeat(chars.clone()) {
                output.push('\\');
            }
            output.push(ch);
        }
    }
    Ok(Cow::Owned(output))
}

/// Whether the text after a `{` is `digits [, [digits]] }`, the only shape
/// Go reads as a counted repetition.
fn is_counted_repeat(mut rest: std::iter::Peekable<std::str::Chars<'_>>) -> bool {
    let take_digits = |rest: &mut std::iter::Peekable<std::str::Chars<'_>>| {
        let leading_zero = rest.peek() == Some(&'0');
        let mut digits = 0;
        while rest.peek().is_some_and(char::is_ascii_digit) {
            rest.next();
            digits += 1;
        }
        (!leading_zero || digits == 1).then_some(digits)
    };
    if !take_digits(&mut rest).is_some_and(|digits| digits != 0) {
        return false;
    }
    if rest.peek() == Some(&',') {
        rest.next();
        if take_digits(&mut rest).is_none() {
            return false;
        }
    }
    rest.next() == Some('}')
}

/// Compile using Go's character classes and syntax boundary, with options
/// supplied by the caller's signature. All utility and SQL users share this.
pub fn compile_with_flags(
    pattern: &str,
    case_insensitive: bool,
    multi_line: bool,
    dot_matches_new_line: bool,
) -> Result<Regex, String> {
    let flags = CompileFlags {
        case_insensitive,
        multi_line,
        dot_matches_new_line,
    };
    if pattern.len() > MAX_CACHED_PATTERN_BYTES {
        return compile_uncached(pattern, flags);
    }

    let (seen, cached) = RECENT_REGEXPS
        .try_with(|entries| {
            let mut entries = entries.borrow_mut();
            let (entry, seen) = recent_entry(&mut entries, pattern, flags);
            (seen, entry.regexp.clone())
        })
        .unwrap_or((false, None));
    if let Some(cached) = cached {
        return Ok(cached);
    }

    let regexp = compile_uncached(pattern, flags)?;
    // Cache compiled programs only once the same pattern has recurred.
    if seen {
        let _ = RECENT_REGEXPS.try_with(|entries| {
            entries.borrow_mut()[0].regexp = Some(regexp.clone());
        });
    }
    Ok(regexp)
}

fn compile_uncached(pattern: &str, flags: CompileFlags) -> Result<Regex, String> {
    build_regex(&rewrite_pattern(pattern)?, flags)
}

fn build_regex(pattern: &str, flags: CompileFlags) -> Result<Regex, String> {
    RegexBuilder::new(pattern)
        .octal(true)
        .case_insensitive(flags.case_insensitive)
        .multi_line(flags.multi_line)
        .dot_matches_new_line(flags.dot_matches_new_line)
        .build()
        .map_err(|error| error.to_string())
}

/// A compiled Go regexp for callers that only need a boolean result.
#[derive(Clone, Debug)]
pub struct RegexpPredicate {
    minimum_len: usize,
    matcher: Matcher,
}

#[derive(Clone, Debug)]
enum Matcher {
    Unicode(unicode_class::Predicate),
    PrefixedClass(unicode_class::PrefixedClass),
    DotRun(literal::DotRun<Box<str>>),
    Regex {
        regexp: Regex,
        check_utf8_boundaries: bool,
    },
}

impl RegexpPredicate {
    /// Returns whether the pattern matches the text.
    #[inline]
    pub fn is_match(&self, text: &str) -> bool {
        if text.len() < self.minimum_len {
            return false;
        }
        match &self.matcher {
            Matcher::Unicode(predicate) => predicate.is_match(text),
            Matcher::PrefixedClass(predicate) => predicate.is_match(text),
            Matcher::DotRun(run) => run.is_match(text),
            Matcher::Regex {
                regexp,
                check_utf8_boundaries,
            } => match_regex(regexp, text, *check_utf8_boundaries),
        }
    }
}

/// Compiles a predicate with Go's regexp syntax and the supplied flags.
pub fn compile_predicate(
    pattern: &str,
    case_insensitive: bool,
    multi_line: bool,
    dot_matches_new_line: bool,
) -> Result<RegexpPredicate, String> {
    if pattern.len() <= MAX_DIRECT_PATTERN_BYTES {
        let (body, folded, multiline) = leading_match_flags(pattern, case_insensitive, multi_line);
        let (body, left, right) = strip_anchors(body, multiline);
        if let Some(predicate) = unicode_class::compile(body, left, right, folded) {
            return predicate.map(|predicate| RegexpPredicate {
                minimum_len: 0,
                matcher: Matcher::Unicode(predicate),
            });
        }
        if left == Boundary::Text && right == Boundary::Text {
            if let Some(predicate) = unicode_class::PrefixedClass::compile(body, folded) {
                return Ok(RegexpPredicate {
                    minimum_len: 0,
                    matcher: Matcher::PrefixedClass(predicate),
                });
            }
        }
        if let Some(run) = literal::compile_dot_run(body, left, right, folded, dot_matches_new_line)
        {
            return Ok(RegexpPredicate {
                minimum_len: run.minimum_len(),
                matcher: Matcher::DotRun(run),
            });
        }
    }
    compile_with_flags(pattern, case_insensitive, multi_line, dot_matches_new_line).map(|regexp| {
        RegexpPredicate {
            minimum_len: 0,
            matcher: Matcher::Regex {
                check_utf8_boundaries: requires_utf8_boundaries(&regexp),
                regexp,
            },
        }
    })
}

/// Tests for a match using Go's regexp syntax and the supplied flags.
///
/// Short literals and simple character-class patterns can be matched directly.
/// Other patterns use the shared syntax normalization and a boolean matcher.
pub fn is_match_with_flags(
    text: &str,
    pattern: &str,
    case_insensitive: bool,
    multi_line: bool,
    dot_matches_new_line: bool,
) -> Result<bool, String> {
    if pattern.len() <= MAX_DIRECT_PATTERN_BYTES {
        let (body, folded, multiline) = leading_match_flags(pattern, case_insensitive, multi_line);
        if let Some(matched) = match_literal(text, body, folded)
            .or_else(|| match_perl_class(text, body, folded))
        {
            return Ok(matched);
        }
        let (body, left, right) = strip_anchors(body, multiline);
        if let Some(predicate) = unicode_class::compile(body, left, right, folded) {
            return predicate.map(|predicate| predicate.is_match(text));
        }
        if let Some(matched) = literal::match_extended(text, body, left, right, folded)
            .or_else(|| unicode_class::match_pair(text, body, left, right, folded))
            .or_else(|| unicode_class::match_ascii_class(text, body, left, right, folded))
            .or_else(|| literal::match_dot_run(text, body, left, right, folded, dot_matches_new_line))
        {
            return Ok(matched);
        }
    }
    match_general(
        text,
        pattern,
        CompileFlags {
            case_insensitive,
            multi_line,
            dot_matches_new_line,
        },
    )
}

fn match_general(text: &str, pattern: &str, flags: CompileFlags) -> Result<bool, String> {
    let compile_and_match = || compile_uncached(pattern, flags).map(|re| regex_is_match(&re, text));
    if pattern.len() > MAX_CACHED_PATTERN_BYTES {
        return compile_and_match();
    }
    RECENT_REGEXPS
        .try_with(|entries| {
            let mut entries = entries.borrow_mut();
            let (entry, seen) = recent_entry(&mut entries, pattern, flags);
            if let Some(regexp) = &entry.regexp {
                return Ok(regex_is_match(regexp, text));
            }
            let pattern = if !seen && unicode_class::compound::eligible(text, pattern) {
                let normalized = normalize_pattern(pattern)?;
                let ast = parse_ast(&normalized)?;
                if let Some(matched) =
                    unicode_class::compound::try_match(text, &normalized, &ast, flags)
                {
                    return Ok(matched);
                }
                rewrite_ast(normalized, &ast)?
            } else {
                rewrite_pattern(pattern)?
            };
            if !seen {
                if let Some(matched) = fallback::try_match(text, &pattern, flags) {
                    return Ok(matched);
                }
            }
            let regexp = build_regex(&pattern, flags)?;
            if seen {
                // Keep the engine's search cache warm as well as its program.
                Ok(regex_is_match(entry.regexp.insert(regexp), text))
            } else {
                Ok(regex_is_match(&regexp, text))
            }
        })
        .unwrap_or_else(|_| compile_and_match())
}

fn regex_is_match(regexp: &Regex, text: &str) -> bool {
    match_regex(regexp, text, requires_utf8_boundaries(regexp))
}

fn requires_utf8_boundaries(regexp: &Regex) -> bool {
    regexp.as_str().contains(r"(?-u:\B)")
}

fn match_regex(regexp: &Regex, text: &str, check_utf8_boundaries: bool) -> bool {
    // Earliest-mode search can stop at an ASCII non-boundary inside a
    // multibyte rune and miss a consuming alternative. Span search honors
    // the UTF-8 match boundaries while considering those alternatives.
    if check_utf8_boundaries && !text.is_ascii() {
        regexp.find(text).is_some()
    } else {
        regexp.is_match(text)
    }
}

fn leading_match_flags(
    mut pattern: &str,
    mut case_insensitive: bool,
    mut multi_line: bool,
) -> (&str, bool, bool) {
    loop {
        if let Some(rest) = pattern.strip_prefix("(?i)") {
            case_insensitive = true;
            pattern = rest;
        } else if let Some(rest) = pattern.strip_prefix("(?-i)") {
            case_insensitive = false;
            pattern = rest;
        } else if let Some(rest) = pattern.strip_prefix("(?m)") {
            multi_line = true;
            pattern = rest;
        } else if let Some(rest) = pattern.strip_prefix("(?-m)") {
            multi_line = false;
            pattern = rest;
        } else {
            return (pattern, case_insensitive, multi_line);
        }
    }
}

fn match_literal(text: &str, literal: &str, case_insensitive: bool) -> Option<bool> {
    if literal.chars().any(regex_syntax::is_meta_character) {
        return None;
    }
    match_unanchored_literal(text, literal, case_insensitive)
}

fn match_unanchored_literal(text: &str, literal: &str, case_insensitive: bool) -> Option<bool> {
    if !case_insensitive || literal.is_empty() {
        return Some(text.contains(literal));
    }
    if !literal.is_ascii() {
        return None;
    }
    if contains_ascii_folded(text.as_bytes(), literal.as_bytes()) {
        return Some(true);
    }

    // Go's only folds from ASCII letters to non-ASCII are K to K and S to ſ.
    if literal.bytes().any(|b| matches!(b, b'k' | b'K' | b's' | b'S')) && !text.is_ascii() {
        if let Some(first_fold) = text.find(['K', 'ſ']) {
            // A match using this fold can start at most one pattern length
            // earlier. All matching runes before the first fold are ASCII.
            let start = text.ceil_char_boundary(first_fold.saturating_sub(literal.len() - 1));
            return Some(contains_unicode_folded_ascii(&text[start..], literal.as_bytes()));
        }
    }
    Some(false)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Boundary {
    Anywhere,
    Line,
    Text,
}

fn strip_anchors(mut pattern: &str, multi_line: bool) -> (&str, Boundary, Boundary) {
    let mut left = Boundary::Anywhere;
    while let Some(inner) = pattern
        .strip_prefix(r"\A")
        .or_else(|| pattern.strip_prefix('^'))
    {
        left = if multi_line && pattern.starts_with('^') && left != Boundary::Text {
            Boundary::Line
        } else {
            Boundary::Text
        };
        pattern = inner;
    }
    let mut right = Boundary::Anywhere;
    while let Some((inner, line_anchor)) = pattern
        .strip_suffix(r"\z")
        .map(|inner| (inner, false))
        .or_else(|| pattern.strip_suffix('$').map(|inner| (inner, true)))
    {
        let escapes = inner.bytes().rev().take_while(|&byte| byte == b'\\').count();
        if escapes % 2 != 0 {
            break;
        }
        right = if multi_line && line_anchor && right != Boundary::Text {
            Boundary::Line
        } else {
            Boundary::Text
        };
        pattern = inner;
    }
    (pattern, left, right)
}

fn contains_ascii_folded(text: &[u8], literal: &[u8]) -> bool {
    let Some(last_start) = text.len().checked_sub(literal.len()) else {
        return false;
    };
    let mut anchor = 0;
    // A repeated leading byte often makes a poor search anchor.
    if text[0].eq_ignore_ascii_case(&literal[0]) {
        let sample = &text[..text.len().min(16)];
        if let Some(index) = literal
            .iter()
            .position(|byte| !sample.iter().any(|candidate| byte.eq_ignore_ascii_case(candidate)))
        {
            anchor = index;
        }
    }
    let byte = literal[anchor];
    memchr2_iter(
        byte.to_ascii_lowercase(),
        byte.to_ascii_uppercase(),
        &text[anchor..=anchor + last_start],
    )
    .any(|start| text[start..start + literal.len()].eq_ignore_ascii_case(literal))
}

fn contains_unicode_folded_ascii(text: &str, literal: &[u8]) -> bool {
    // Knuth–Morris–Pratt over folded runes keeps the scan linear and needs
    // only a prefix table for the short ASCII pattern.
    let mut prefixes = [0_u8; MAX_DIRECT_PATTERN_BYTES];
    let mut count = 0;
    for (index, byte) in literal.iter().copied().enumerate().skip(1) {
        while count != 0 && !byte.eq_ignore_ascii_case(&literal[count]) {
            count = usize::from(prefixes[count - 1]);
        }
        count += usize::from(byte.eq_ignore_ascii_case(&literal[count]));
        prefixes[index] = count as u8;
    }

    count = 0;
    for ch in text.chars() {
        let byte = match ch {
            'K' => b'k',
            'ſ' => b's',
            ch if ch.is_ascii() => ch as u8,
            _ => {
                count = 0;
                continue;
            }
        };
        while count != 0 && !byte.eq_ignore_ascii_case(&literal[count]) {
            count = usize::from(prefixes[count - 1]);
        }
        count += usize::from(byte.eq_ignore_ascii_case(&literal[count]));
        if count == literal.len() {
            return true;
        }
    }
    false
}

fn match_perl_class(text: &str, pattern: &str, case_insensitive: bool) -> Option<bool> {
    // A single character class and its one-or-more repetition have the same
    // boolean result. Limit this path to patterns with a single class.
    let atom = pattern.strip_suffix('+').unwrap_or(pattern);
    let [b'\\', class] = atom.as_bytes() else {
        return None;
    };
    let is_space = |byte: u8| matches!(byte, b'\t' | b'\n' | 0x0c | b'\r' | b' ');
    Some(match class {
        b'd' => text.bytes().any(|byte| byte.is_ascii_digit()),
        b'D' => text.bytes().any(|byte| !byte.is_ascii_digit()),
        b's' => text.bytes().any(is_space),
        b'S' => text.bytes().any(|byte| !is_space(byte)),
        b'w' | b'W' => {
            let want_word = *class == b'w';
            if case_insensitive && !text.is_ascii() {
                text.chars().any(|ch| {
                    (ch.is_ascii_alphanumeric() || matches!(ch, '_' | 'ſ' | 'K')) == want_word
                })
            } else {
                let matches = |byte: u8| (byte.is_ascii_alphanumeric() || byte == b'_') == want_word;
                let (chunks, tail) = text.as_bytes().as_chunks::<16>();
                chunks.iter().any(|chunk| {
                    chunk.iter().copied().fold(false, |found, byte| found | matches(byte))
                }) || tail.iter().copied().any(matches)
            }
        }
        _ => return None,
    })
}

/// Compiles `pattern` with Go `regexp` syntax, optionally case-insensitive.
pub fn compile(pattern: &str, case_sensitive: bool) -> Result<Regex, String> {
    compile_with_flags(pattern, !case_sensitive, false, false)
}

#[cfg(test)]
mod tests {
    use super::{compile, compile_predicate, compile_with_flags, is_match_with_flags};

    #[test]
    fn compound_boolean_patterns_preserve_rune_boundaries_and_folding() {
        for (pattern, text, expected) in [
            (r"^(?:\p{Greek}+[-])?\p{L}+$", "ΩΣ-abc雅", true),
            (r"^(?:\p{Greek}+[-])?\p{L}+$", "ΩΣ-123", false),
            (r"(?i)foo\p{Greek}+(?:0|[a-z])", "pre-FOOΣK", true),
            (r"(?i)foo\p{Greek}+(?:0|[a-z])", "fooΣ猫", false),
            (r"(\p{Greek}|[A-Za-z]+)\d", "猫α4", true),
            (r"(\p{Greek}|[A-Za-z]+)\d", "猫α４", false),
            (r"\bfoo\d{2}\b", "Éfoo23Ω", true),
            (r"(?i)^s[abc]+k.\d$", "ſaBCK猫8", true),
            (r"^\p{Greek}{2,3}[0-9]$", "ω0", false),
            (r"^\p{Greek}{2,3}[0-9]$", "ωΩ0", true),
        ] {
            for _ in 0..3 {
                assert_eq!(
                    is_match_with_flags(text, pattern, false, false, false),
                    Ok(expected),
                    "{pattern:?} on {text:?}"
                );
            }
        }
    }

    #[test]
    fn bracketed_class_runs_match_go_character_sets_and_splits() {
        for (pattern, text, folded, expected) in [
            (r"^[a-zA-Z_][0-9a-zA-Z_]*$", "sys_created_ts", false, true),
            (r"^[a-zA-Z_][0-9a-zA-Z_]*$", "Key", false, false),
            (r"\A[a-j]{1,6}?[b-z]{2,8}\z", "fibbzz", false, true),
            (r"\A[a-j]{1,6}?[b-z]{2,8}\z", "abc/def", false, false),
            (r"^[ab]{0,2}[bc]{0,2}$", "abc", false, true),
            (r"^[ab]{2}[ab]{2}$", "aba", false, false),
            (r"^[[:alpha:]]+:[[:digit:]]+$", "K:42", true, true),
            (r"^[[:alpha:]]+:[[:digit:]]+$", "K:42", false, false),
            (r"^[[:space:]]*=[[:xdigit:]]+$", "\x0b\t=abcF4", false, true),
            (r"^[🌽-🟝]+[^a-z]*x$", "🌽🟝ñx", false, true),
            (r"^[🌽-🟝]+[^a-z]*x$", "🌼🟝ñx", false, false),
        ] {
            for _ in 0..3 {
                assert_eq!(
                    is_match_with_flags(text, pattern, folded, false, false),
                    Ok(expected),
                    "{pattern:?} on {text:?}"
                );
            }
        }
    }

    #[test]
    fn literal_dot_run_preserves_go_anchors_and_newlines() {
        for (pattern, text, multiline, dot_all, expected) in [
            ("^a.*b$", "a\nb", false, false, false),
            ("^a.*b$", "a\nb", false, true, true),
            ("^a.+b$", "ab", false, false, false),
            ("^a.+b$", "a🐹b", false, false, true),
            ("^a.*?b$", "before\na🐹b\nlater", true, false, true),
            (r"\Aa.*b$", "before\na🐹b\nlater", true, false, false),
            ("a.*b", "a\nb\na🙂b", false, false, true),
            ("a.*b", "ax\nay\nab", false, false, true),
            ("a.*b", "ax\nay\nb", false, false, false),
            (r"^a\.b.*c\?$", "a.b🐹c?", false, false, true),
            ("^(?:a.*b)$", "axb", false, false, true),
        ] {
            assert_eq!(
                is_match_with_flags(text, pattern, false, multiline, dot_all),
                Ok(expected),
                "{pattern:?} on {text:?}"
            );
            let predicate = compile_predicate(pattern, false, multiline, dot_all).unwrap();
            assert_eq!(
                predicate.is_match(text),
                expected,
                "{pattern:?} on {text:?}"
            );
        }
    }

    #[test]
    fn repeated_literal_alternatives_preserve_possible_splits() {
        for (pattern, text, expected) in [
            (r"\A(?:a|ab)+?bc\z", "abababc", true),
            (r"\A(?:a|ab)+?bc\z", "abababac", false),
            ("^(é|é世)+ok$", "é世éok", true),
            ("(cat|dash)+dog", "dogcatdog", true),
            ("(cat|dash)+dog", "dogcat", false),
            ("(cat|dash)*dog", "dog", true),
            ("^(cat|dash)+$", "catdash\n", false),
        ] {
            assert_eq!(
                is_match_with_flags(text, pattern, false, false, false),
                Ok(expected),
                "{pattern:?} on {text:?}"
            );
        }
    }

    #[test]
    fn compound_matching_preserves_general_compiler_diagnostics() {
        let large = format!("^{}[a-z]$", r"\p{L}{63}".repeat(4));
        for pattern in [r"^\p{^Greek}+[a-z]$", large.as_str()] {
            let actual = is_match_with_flags("abc", pattern, false, false, false);
            let expected = compile_with_flags(pattern, false, false, false)
                .map(|regexp| regexp.is_match("abc"));
            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn counted_repeats_with_leading_zeroes_are_literals() {
        for count in ["{01}", "{1,02}", "{00,}"] {
            let pattern = format!(r"^\pL{count}$");
            let text = format!("α{count}");
            assert!(is_match_with_flags(&text, &pattern, false, false, false).unwrap());
            let regex = compile_with_flags(&pattern, false, false, false).unwrap();
            assert!(regex.is_match(&text));
            assert!(!regex.is_match("α"));
        }
        let regex = compile_with_flags(r"^\pL{0}$", false, false, false).unwrap();
        assert!(regex.is_match(""));
    }

    #[test]
    fn general_boolean_matching_keeps_go_flags_and_alternatives() {
        let long = "z".repeat(4096);
        let cases = [
            ("a\nb", "a.*b", false, false, false, false),
            ("a\nb", "a.*b", false, false, true, true),
            ("ſxK0", r"(?mi)^s.k[0-9]$", false, false, false, true),
            ("kxa", r"^\x{212a}.[a]$", true, false, false, true),
            ("Éfoo23Ω", r"\bfoo\d{2}\b", false, false, false, true),
            (
                "a\nb",
                r"(?i-m)(?s:[[:^lower:]]{2}(?i:\B)*)$",
                false,
                false,
                false,
                false,
            ),
            (&long, "c.*d", false, false, false, false),
            (&long, "c.*d|z+", false, false, false, true),
            (&long, "(?:hello)?z+", false, false, false, true),
        ];
        for (text, pattern, folded, multiline, dot_all, expected) in cases {
            // Assert the first-use result and the result after the pattern's
            // regular engine and search cache have been retained.
            for _ in 0..3 {
                assert_eq!(
                    is_match_with_flags(text, pattern, folded, multiline, dot_all),
                    Ok(expected),
                    "{pattern:?} with flags ({folded}, {multiline}, {dot_all})"
                );
            }
        }
    }

    #[test]
    fn ascii_non_boundary_keeps_consuming_alternatives_on_utf8() {
        let pattern = r"(?:\B|..)";
        let text = "a¢b🌽3";
        for _ in 0..3 {
            assert!(is_match_with_flags(text, pattern, false, false, false).unwrap());
        }
        let predicate = compile_predicate(pattern, false, false, false).unwrap();
        assert!(predicate.is_match(text));
        let compiled = compile_predicate(pattern, false, false, false).unwrap();
        assert!(compiled.is_match(text));
    }

    #[test]
    fn anchored_predicates_follow_go_boundaries_and_repeat_splits() {
        let cases = [
            (r"^\p{Tai_Yo}{2}\p{Nd}+$", "\u{1e6c0}\u{1e6c1}42", true),
            (r"^\p{Tai_Yo}+\$$", "\u{1e6c0}$", true),
            (r"(?i)^\p{Tai_Yo}+sky$", "\u{1e6c0}ſKy", true),
            (r"^\p{L}{1,3}\p{Greek}{1,2}$", "αβγδ", true),
            (r"^\p{L}{1,3}\p{Greek}{1,2}$", "αβγδφζ", false),
            (r"^\p{Any}+0\p{Nd}+$", "猫0012", true),
            (r"^\p{Any}+0\p{Nd}+$", "猫012x", false),
            (r"^\p{Greek}+\\d$", "Ω\\d", true),
            (r"(?i)^sky$", "ſKy", true),
            (r"(?m)\Asky$", "sky\nother", true),
            (r"(?m)^sky\z", "sky\nother", false),
            (r"(?m)^(?:sky|sea)$", "other\nsea\n", true),
            (r"^\d+$", "12١", false),
            (r"[a-z]+", "猫x", true),
        ];
        for (pattern, text, expected) in cases {
            assert_eq!(
                is_match_with_flags(text, pattern, false, false, false),
                Ok(expected),
                "{pattern}: {text:?}"
            );
        }
    }

    #[test]
    fn unicode_properties_use_go_aliases_and_unicode_version() {
        for (pattern, text, folded, matched) in [
            (r"\p{Tai_Yo}", "\u{1e6c0}", false, true),
            (r"\p{^ g-r_e eK }+", "猫", false, true),
            (r"\p{^Greek}+", "α", false, false),
            (r"\p{ASCII}", "K", true, true),
            (r"\P{ASCII}", "K", true, false),
            (r"\P{^ASCII}", "K", false, false),
            (r"\A\p{Letter}{2,3}\z", "É猫α", false, true),
            (r"\A\p{Letter}{2,3}\z", "É猫αx", false, false),
            (r"\pL{2,}", "α1É猫", false, true),
            (r"^\pL{2}", "α12É猫", false, false),
            (r"\pL{2}$", "α12É猫", false, true),
            (r"^\pL+$", "hello\n", false, false),
            (r"^\p{Assigned}*$", "", false, true),
            (r"\p{Assigned}", "\u{0378}", false, false),
        ] {
            assert_eq!(
                is_match_with_flags(text, pattern, folded, false, false).unwrap(),
                matched,
                "{pattern:?} against {text:?}"
            );
        }
        assert_eq!(
            is_match_with_flags("😀", r"\p{Emoji}", false, false, false),
            Err("invalid character class range: `\\p{Emoji}`".to_owned())
        );
        assert!(is_match_with_flags("junk!\nhello\n1", r"^\pL+$", false, true, false).unwrap());
    }

    #[test]
    fn literal_boolean_matching_keeps_go_folds_and_inline_flags() {
        for (pattern, text, folded, expected) in [
            ("needle", "É猫NEEDLE", true, true),
            ("needle", "É猫NEEDLE", false, false),
            ("(?i)needle", "NEEDLE", false, true),
            ("(?i)(?-i)Ab", "AB", true, false),
            ("(?i)sk", "prefixſKtail", false, true),
            ("(?i)sk", "prefixſKtail", true, true),
            ("(?i)kk", "KXK", false, false),
            ("i", "İ", true, false),
            ("(?i)Σ", "ς", false, true),
            ("(?i)", "", false, true),
            ("猫🦀", "猫猫🦀", false, true),
            (r"\d", "123", false, true),
            (r"\W", "é", false, true),
            (r"\w+", "ſ", true, true),
            (r"\W+", "ſ", true, false),
            (r"\s+", "\x0b", false, false),
            (r"\p{Greek}+", "猫Σ", false, true),
            (r"[a-z]+", "É", true, false),
            (r"[a-z]+", "Éz", true, true),
        ] {
            assert_eq!(
                is_match_with_flags(text, pattern, folded, false, false).unwrap(),
                expected,
                "{pattern:?} against {text:?}"
            );
        }
        assert!(is_match_with_flags("text", "(?x)a", true, false, false).is_err());
    }

    #[test]
    fn repeated_compilation_keeps_flags_distinct() {
        for _ in 0..3 {
            let plain = compile_with_flags("^a.b$", false, false, false).unwrap();
            let insensitive = compile_with_flags("^a.b$", true, false, false).unwrap();
            assert!(plain.is_match("axb"));
            assert!(!plain.is_match("AxB"));
            assert!(insensitive.is_match("AxB"));
            assert!(!insensitive.is_match("A\nB"));
        }
        let multiline = compile_with_flags("^a.b$", true, true, false).unwrap();
        assert!(multiline.is_match("head\nAxB\ntail"));
        let dot_all = compile_with_flags("^a.b$", true, true, true).unwrap();
        assert!(dot_all.is_match("head\nA\nB\ntail"));
        assert!(!multiline.is_match("head\nA\nB\ntail"));
    }

    #[test]
    fn go_syntax_and_character_contracts() {
        for (pattern, text, expected) in [
            (r"\d", "١", false),
            (r"\w", "é", false),
            (r"\s", "\u{a0}", false),
            (r"\bé\b", "é", false),
            (r"[a-z&&b]", "a", true),
            (r"[[a]", "[", true),
            (r"[--a]", "0", true),
            (r"[^^]", "a", true),
            (r"[^^]", "^", false),
            (r"\Q(a)+\E", "(a)+", true),
            (r"\141", "a", true),
            (r"(?i:a)", "A", true),
            (r"^(猫+|犬*)$", "猫猫", true),
            (r"\p{Greek}", "Σ", true),
            (r#"{"a": [1, 2]}"#, r#"{"a": 1}"#, true),
            (r#"{"a": [1, 2]}"#, r#"{"a": 1"#, false),
            (r"a{,2}", "a{,2}", true),
            (r"a{2", "a{2", true),
        ] {
            assert_eq!(
                compile(pattern, true).unwrap().is_match(text),
                expected,
                "{pattern}"
            );
        }
        for pattern in [
            r"[a--b]",
            r"(?u)a",
            r"(?x)a",
            r"\u{61}",
            r"a++",
            r"a**",
            r"a*+",
            r"a+*",
            r"a{1001}",
            r"(a{20}){51}",
            r"\1",
        ] {
            assert!(compile(pattern, true).is_err(), "{pattern}");
        }
        assert!(compile(r"(a{20}){50}", true).is_ok());
    }
}
