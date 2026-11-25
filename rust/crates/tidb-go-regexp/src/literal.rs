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

//! Direct boolean matching of anchored literals and literal alternatives.

use super::{match_unanchored_literal, Boundary, MAX_DIRECT_PATTERN_BYTES};
use memchr::{memchr, memchr_iter, memmem, memrchr, memrchr_iter};

pub(super) fn match_extended(
    text: &str,
    body: &str,
    left: Boundary,
    right: Boundary,
    folded: bool,
) -> Option<bool> {
    let supported = |literal: &str| {
        (!folded || literal.is_ascii())
            && (left != Boundary::Line && right != Boundary::Line || !literal.contains('\n'))
    };

    if let Some(group) = strip_group(body) {
        let mut matched = false;
        for alternative in group.split('|') {
            if alternative.chars().any(regex_syntax::is_meta_character) || !supported(alternative) {
                return None;
            }
            matched = matched || match_literal(text, alternative, left, right, folded);
        }
        return Some(matched);
    }

    if let Some(matched) = match_repeated_alternatives(text, body, left, right, folded) {
        return Some(matched);
    }

    if left == Boundary::Anywhere && right == Boundary::Anywhere {
        return None;
    }
    let mut decoded = [0; MAX_DIRECT_PATTERN_BYTES];
    let literal = decode(body, &mut decoded)?;
    supported(literal).then(|| match_literal(text, literal, left, right, folded))
}

fn match_repeated_alternatives(
    text: &str,
    body: &str,
    left: Boundary,
    right: Boundary,
    folded: bool,
) -> Option<bool> {
    if folded || left == Boundary::Line || right == Boundary::Line || text.len() >= 64 {
        return None;
    }
    let body = body
        .strip_prefix("(?:")
        .or_else(|| body.strip_prefix('('))?;
    let (group, tail) = body.split_once(')')?;
    let operator = *tail.as_bytes().first()?;
    if !matches!(operator, b'+' | b'*') || !group.contains('|') {
        return None;
    }
    let tail = &tail[1..];
    let suffix = tail.strip_prefix('?').unwrap_or(tail);
    if suffix.chars().any(regex_syntax::is_meta_character) {
        return None;
    }
    let mut arms = [&[][..]; 8];
    let mut len = 0;
    for arm in group.split('|') {
        let slot = arms.get_mut(len)?;
        if arm.is_empty() || arm.chars().any(regex_syntax::is_meta_character) {
            return None;
        }
        *slot = arm.as_bytes();
        len += 1;
    }

    let text = text.as_bytes();
    let suffix = suffix.as_bytes();
    // Each bit marks a byte position that can reach the suffix with zero or
    // more arms. Nonempty arms only advance to positions already checked.
    let mut reachable = 0u64;
    for start in (0..=text.len()).rev() {
        let rest = &text[start..];
        let repeat = arms[..len]
            .iter()
            .any(|arm| rest.starts_with(arm) && reachable & (1 << (start + arm.len())) != 0);
        let end = if right == Boundary::Text {
            rest == suffix
        } else {
            rest.starts_with(suffix)
        };
        if (repeat || (operator == b'*' && end)) && (left == Boundary::Anywhere || start == 0) {
            return Some(true);
        }
        if repeat || end {
            reachable |= 1 << start;
        }
    }
    Some(false)
}

/// A single dot repetition between two literals needs no compiled program.
pub(super) fn match_dot_run(
    text: &str,
    body: &str,
    left: Boundary,
    right: Boundary,
    folded: bool,
    dot_all: bool,
) -> Option<bool> {
    let mut prefix_buffer = [0; MAX_DIRECT_PATTERN_BYTES];
    let mut suffix_buffer = [0; MAX_DIRECT_PATTERN_BYTES];
    parse_dot_run(
        body,
        left,
        right,
        folded,
        dot_all,
        &mut prefix_buffer,
        &mut suffix_buffer,
    )
    .map(|run| run.is_match(text))
}

pub(super) fn compile_dot_run(
    body: &str,
    left: Boundary,
    right: Boundary,
    folded: bool,
    dot_all: bool,
) -> Option<DotRun<Box<str>>> {
    let mut prefix_buffer = [0; MAX_DIRECT_PATTERN_BYTES];
    let mut suffix_buffer = [0; MAX_DIRECT_PATTERN_BYTES];
    let run = parse_dot_run(
        body,
        left,
        right,
        folded,
        dot_all,
        &mut prefix_buffer,
        &mut suffix_buffer,
    )?;
    Some(DotRun {
        prefix: run.prefix.into(),
        suffix: run.suffix.into(),
        left,
        right,
        plus: run.plus,
        dot_all,
    })
}

#[derive(Clone, Debug)]
pub(super) struct DotRun<T> {
    prefix: T,
    suffix: T,
    left: Boundary,
    right: Boundary,
    plus: bool,
    dot_all: bool,
}

fn parse_dot_run<'a>(
    body: &'a str,
    left: Boundary,
    right: Boundary,
    folded: bool,
    dot_all: bool,
    prefix_buffer: &'a mut [u8; MAX_DIRECT_PATTERN_BYTES],
    suffix_buffer: &'a mut [u8; MAX_DIRECT_PATTERN_BYTES],
) -> Option<DotRun<&'a str>> {
    if folded || dot_all && (left == Boundary::Line || right == Boundary::Line) {
        return None;
    }
    let body = strip_group(body).unwrap_or(body);
    let (dot, operator) = body.match_indices('.').find_map(|(index, _)| {
        let operator = *body.as_bytes().get(index + 1)?;
        matches!(operator, b'*' | b'+').then_some((index, operator))
    })?;
    let (prefix, suffix) = (&body[..dot], &body[dot + 2..]);
    let prefix = decode(prefix, prefix_buffer)?;
    let suffix = decode(suffix.strip_prefix('?').unwrap_or(suffix), suffix_buffer)?;
    if !dot_all && (prefix.contains('\n') || suffix.contains('\n')) {
        return None;
    }

    Some(DotRun {
        prefix,
        suffix,
        left,
        right,
        plus: operator == b'+',
        dot_all,
    })
}

impl<T: AsRef<str>> DotRun<T> {
    pub(super) fn minimum_len(&self) -> usize {
        self.prefix.as_ref().len() + self.suffix.as_ref().len() + usize::from(self.plus)
    }

    pub(super) fn is_match(&self, text: &str) -> bool {
        let (prefix, suffix) = (self.prefix.as_ref(), self.suffix.as_ref());
        let matches = |text: &str| {
            let rest = if self.left == Boundary::Anywhere {
                text.find(prefix).map(|start| &text[start + prefix.len()..])
            } else {
                text.strip_prefix(prefix)
            };
            let Some(mut rest) = rest else { return false };
            if self.plus {
                let Some(rune) = rest.chars().next() else {
                    return false;
                };
                rest = &rest[rune.len_utf8()..];
            }
            if self.right == Boundary::Anywhere {
                rest.contains(suffix)
            } else {
                rest.ends_with(suffix)
            }
        };

        if self.dot_all {
            matches(text)
        } else {
            // A non-dot-all wildcard stays on the line containing the literals.
            match (self.left, self.right) {
                (Boundary::Anywhere, Boundary::Anywhere) => {
                    match_unanchored_dot_run(text, prefix, suffix, self.plus)
                }
                (Boundary::Text, Boundary::Text) => {
                    matches(text) && memchr(b'\n', text.as_bytes()).is_none()
                }
                (Boundary::Text, _) => matches(text.split('\n').next().unwrap()),
                (_, Boundary::Text) => matches(text.rsplit('\n').next().unwrap()),
                _ => text.split('\n').any(matches),
            }
        }
    }
}

fn match_unanchored_dot_run(mut text: &str, prefix: &str, suffix: &str, plus: bool) -> bool {
    while let Some(start) = memmem::find(text.as_bytes(), prefix.as_bytes()) {
        // A nonempty UTF-8 literal starts at a rune boundary.
        let rest = &text[start + prefix.len()..];
        let skip = if plus {
            let Some(first) = rest.chars().next() else {
                return false;
            };
            first.len_utf8()
        } else {
            0
        };
        let Some(end) = memmem::find(&rest.as_bytes()[skip..], suffix.as_bytes()) else {
            return false;
        };
        let Some(newline) = memrchr(b'\n', &rest.as_bytes()[..skip + end]) else {
            return true;
        };
        // No suffix occurs on the intervening lines. Resume at the start
        // of the line that contains this suffix.
        text = &rest[newline + 1..];
    }
    false
}

pub(super) fn decode<'a>(
    body: &'a str,
    decoded: &'a mut [u8; MAX_DIRECT_PATTERN_BYTES],
) -> Option<&'a str> {
    if !body.contains('\\') {
        return (!body.chars().any(regex_syntax::is_meta_character)).then_some(body);
    }
    let mut len = 0;
    let mut chars = body.chars();
    while let Some(ch) = chars.next() {
        let literal = if ch == '\\' {
            let escaped = chars.next()?;
            if !regex_syntax::is_meta_character(escaped) {
                return None;
            }
            escaped
        } else {
            if regex_syntax::is_meta_character(ch) {
                return None;
            }
            ch
        };
        len += literal.encode_utf8(&mut decoded[len..]).len();
    }
    std::str::from_utf8(&decoded[..len]).ok()
}

pub(super) fn strip_group(pattern: &str) -> Option<&str> {
    pattern
        .strip_prefix("(?:")
        .or_else(|| pattern.strip_prefix('('))?
        .strip_suffix(')')
}

fn match_literal(text: &str, literal: &str, left: Boundary, right: Boundary, folded: bool) -> bool {
    if left == Boundary::Anywhere && right == Boundary::Anywhere {
        return match_unanchored_literal(text, literal, folded).unwrap();
    }
    if !folded && left == Boundary::Text && right == Boundary::Text {
        return text == literal;
    }
    let prefix = |text: &str| {
        let width = if folded {
            folded_width(text.chars(), literal.bytes())
        } else {
            text.starts_with(literal).then_some(literal.len())
        };
        width.is_some_and(|width| match right {
            Boundary::Anywhere => true,
            Boundary::Text => text.len() == width,
            Boundary::Line => matches!(text.as_bytes().get(width), None | Some(b'\n')),
        })
    };
    let suffix = |text: &str| {
        let width = if folded {
            folded_width(text.chars().rev(), literal.bytes().rev())
        } else {
            text.ends_with(literal).then_some(literal.len())
        };
        width.is_some_and(|width| {
            left == Boundary::Anywhere
                || width == text.len()
                || text.as_bytes()[text.len() - width - 1] == b'\n'
        })
    };
    match (left, right) {
        (Boundary::Text, _) => prefix(text),
        (_, Boundary::Text) => suffix(text),
        (Boundary::Line, _) => std::iter::once(text)
            .chain(memchr_iter(b'\n', text.as_bytes()).map(|index| &text[index + 1..]))
            .any(prefix),
        _ => std::iter::once(text)
            .chain(memrchr_iter(b'\n', text.as_bytes()).map(|index| &text[..index]))
            .any(suffix),
    }
}

pub(super) fn folded_width(
    mut text: impl Iterator<Item = char>,
    literal: impl Iterator<Item = u8>,
) -> Option<usize> {
    let mut width = 0;
    for expected in literal {
        let actual = text.next()?;
        let byte = match actual {
            'K' => b'k',
            'ſ' => b's',
            ch if ch.is_ascii() => ch as u8,
            _ => return None,
        };
        if !expected.eq_ignore_ascii_case(&byte) {
            return None;
        }
        width += actual.len_utf8();
    }
    Some(width)
}
