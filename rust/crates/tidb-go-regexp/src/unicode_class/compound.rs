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

//! First-use boolean matching of compound expressions on short Unicode text.

use regex_syntax::ast::{self, Ast, ClassAsciiKind, ClassSet, ClassSetItem};

use super::{class, repeat, split_property, split_repeat};
use crate::{counted_repeat_limit, leading_match_flags, CompileFlags};

const MAX_TEXT_BYTES: usize = 63;

pub(super) fn match_ascii_pair(text: &str, body: &str) -> Option<bool> {
    let (first, tail) = split_ascii_bracket(body)?;
    let (quantifier, tail) = split_repeat(tail)?;
    let (second, tail) = split_ascii_bracket(tail)?;
    let (first_min, first_max) = repeat(quantifier)?;
    let (second_min, second_max) = repeat(tail)?;
    let prefix = text
        .bytes()
        .take(first_max)
        .take_while(|&byte| in_ascii_set(first, byte))
        .count();
    let suffix = text
        .bytes()
        .rev()
        .take(second_max)
        .take_while(|&byte| in_ascii_set(second, byte))
        .count();
    // The allowed prefix and suffix lengths each form an interval. Any
    // compatible pair of lengths must cover exactly the entire text.
    Some(
        prefix >= first_min
            && suffix >= second_min
            && prefix + suffix >= text.len()
            && first_min + second_min <= text.len(),
    )
}

fn split_ascii_bracket(pattern: &str) -> Option<(u128, &str)> {
    let (body, tail) = pattern.strip_prefix('[')?.split_once(']')?;
    let literal = |byte: u8| byte.is_ascii_alphanumeric() || byte == b'_';
    let mut bytes = body.as_bytes();
    let mut mask = 0;
    // Recognize positive classes of ordinary ASCII letters, digits, or
    // underscores. The general parser handles escapes and other syntax.
    while let [start, rest @ ..] = bytes {
        if !literal(*start) {
            return None;
        }
        if let [b'-', end, tail @ ..] = rest {
            if !literal(*end) || end < start {
                return None;
            }
            mask |= ascii_range(*start, *end);
            bytes = tail;
        } else {
            mask |= 1 << start;
            bytes = rest;
        }
    }
    (mask != 0).then_some((mask, tail))
}

fn in_ascii_set(mask: u128, byte: u8) -> bool {
    byte.is_ascii() && mask & (1 << byte) != 0
}

pub(crate) fn eligible(text: &str, pattern: &str) -> bool {
    text.len() <= MAX_TEXT_BYTES && memchr::memchr2(b'\\', b'[', pattern.as_bytes()).is_some()
}

pub(crate) fn try_match(text: &str, pattern: &str, ast: &Ast, flags: CompileFlags) -> Option<bool> {
    let (body, folded, multiline) =
        leading_match_flags(pattern, flags.case_insensitive, flags.multi_line);
    let mut chars = ['\0'; MAX_TEXT_BYTES];
    let mut len = 0;
    for ch in text.chars() {
        chars[len] = ch;
        len += 1;
    }
    let mut evaluator = Evaluator {
        pattern,
        chars: &chars[..len],
        folded,
        multiline,
        dot_all: flags.dot_matches_new_line,
        leading_flags_end: pattern.len() - body.len(),
        remaining_counted_repeats: MAX_TEXT_BYTES as u32,
    };
    // Bit i represents a boundary immediately before rune i. Every boundary
    // is a possible start until an assertion restricts it.
    let starts = u64::MAX >> (MAX_TEXT_BYTES - len);
    Some(evaluator.advance(ast, starts)? != 0)
}

struct Evaluator<'a> {
    pattern: &'a str,
    chars: &'a [char],
    folded: bool,
    multiline: bool,
    dot_all: bool,
    leading_flags_end: usize,
    remaining_counted_repeats: u32,
}

impl Evaluator<'_> {
    fn advance(&mut self, ast: &Ast, starts: u64) -> Option<u64> {
        Some(match ast {
            Ast::Empty(_) => starts,
            Ast::Flags(flags) if flags.span.end.offset <= self.leading_flags_end => starts,
            Ast::Concat(parts) => {
                let mut positions = starts;
                for part in &parts.asts {
                    positions = self.advance(part, positions)?;
                }
                positions
            }
            Ast::Alternation(parts) => {
                let mut positions = 0;
                for part in &parts.asts {
                    positions |= self.advance(part, starts)?;
                }
                positions
            }
            Ast::Group(group) if group.flags().is_none_or(|flags| flags.items.is_empty()) => {
                self.advance(&group.ast, starts)?
            }
            Ast::Assertion(assertion) => starts & self.assertion(&assertion.kind)?,
            Ast::Repetition(rep) => {
                // Leave larger counted programs to the general engine so
                // its compiled size checks apply even on short haystacks.
                if let Some(count) = counted_repeat_limit(rep) {
                    self.remaining_counted_repeats =
                        self.remaining_counted_repeats.checked_sub(count)?;
                }
                let (min, max) = repeat(self.fragment(&rep.op.span))?;
                if min == 0 && max == 1 && matches!(*rep.ast, Ast::Group(_)) {
                    starts | self.advance(&rep.ast, starts)?
                } else {
                    let mask = self.atom(&rep.ast)?;
                    self.repeat_atom(starts, mask, min, max)
                }
            }
            _ => (starts & self.atom(ast)?) << 1,
        })
    }

    fn atom(&self, ast: &Ast) -> Option<u64> {
        Some(match ast {
            Ast::Literal(literal) => {
                let expected = literal.c;
                if self.folded {
                    if !expected.is_ascii() {
                        return None;
                    }
                    let expected = expected.to_ascii_lowercase() as u8;
                    self.mask(|ch| folded_ascii(ch) == Some(expected))
                } else {
                    self.mask(|ch| ch == expected)
                }
            }
            Ast::ClassUnicode(property) => {
                let property = self.fragment(&property.span);
                // The regular Rust compiler rejects Go's caret spelling in
                // a compound property. Leave its diagnostics to that path.
                if property.as_bytes().get(2..4) == Some(b"{^") {
                    return None;
                }
                let (name, _, negated) = split_property(property)?;
                if !shared_property(name) {
                    return None;
                }
                let class = class(name, self.folded)?;
                self.mask(|ch| class.contains(ch) != negated)
            }
            Ast::ClassBracketed(bracket) => {
                let ClassSet::Item(item) = &bracket.kind else {
                    return None;
                };
                let mask = self.class_mask(item)?;
                if bracket.negated {
                    mask ^ ((1 << self.chars.len()) - 1)
                } else {
                    mask
                }
            }
            Ast::ClassPerl(perl) => self.mask(|ch| {
                let member = match perl.kind {
                    ast::ClassPerlKind::Digit => ch.is_ascii_digit(),
                    ast::ClassPerlKind::Space => matches!(ch, '\t' | '\n' | '\x0c' | '\r' | ' '),
                    ast::ClassPerlKind::Word => {
                        is_ascii_word(ch) || self.folded && matches!(ch, 'K' | 'ſ')
                    }
                };
                member != perl.negated
            }),
            Ast::Dot(_) => self.mask(|ch| self.dot_all || ch != '\n'),
            _ => return None,
        })
    }

    fn class_mask(&self, item: &ClassSetItem) -> Option<u64> {
        if let Some(members) = ascii_members(item) {
            let contains = |byte: u8| members & (1 << byte) != 0;
            return Some(if self.folded {
                self.mask(|ch| {
                    folded_ascii(ch)
                        .is_some_and(|byte| contains(byte) || contains(byte.to_ascii_uppercase()))
                })
            } else {
                self.mask(|ch| ch.is_ascii() && contains(ch as u8))
            });
        }
        if self.folded {
            return None;
        }
        Some(match item {
            ClassSetItem::Literal(literal) => self.mask(|ch| ch == literal.c),
            ClassSetItem::Range(range) => {
                self.mask(|ch| (range.start.c..=range.end.c).contains(&ch))
            }
            ClassSetItem::Union(union) => union
                .items
                .iter()
                .try_fold(0, |mask, item| Some(mask | self.class_mask(item)?))?,
            _ => return None,
        })
    }

    fn assertion(&self, kind: &ast::AssertionKind) -> Option<u64> {
        use ast::AssertionKind::*;

        let end = 1 << self.chars.len();
        Some(match *kind {
            StartLine if self.multiline => (self.mask(|ch| ch == '\n') << 1) | 1,
            EndLine if self.multiline => self.mask(|ch| ch == '\n') | end,
            StartText | StartLine => 1,
            EndText | EndLine => end,
            WordBoundary | NotWordBoundary => {
                let words = self.mask(is_ascii_word);
                let boundaries = words ^ (words << 1);
                if *kind == WordBoundary {
                    boundaries
                } else {
                    !boundaries
                }
            }
            _ => return None,
        })
    }

    fn repeat_atom(&self, mut positions: u64, mask: u64, min: usize, max: usize) -> u64 {
        if min > self.chars.len() {
            return 0;
        }
        for _ in 0..min {
            positions = (positions & mask) << 1;
        }
        let mut reachable = positions;
        for _ in min..max.min(self.chars.len()) {
            positions = (positions & mask) << 1;
            if positions & !reachable == 0 {
                break;
            }
            reachable |= positions;
        }
        reachable
    }

    fn mask(&self, mut contains: impl FnMut(char) -> bool) -> u64 {
        self.chars
            .iter()
            .copied()
            .enumerate()
            .fold(0, |bits, (i, ch)| bits | (u64::from(contains(ch)) << i))
    }

    fn fragment(&self, span: &ast::Span) -> &str {
        &self.pattern[span.start.offset..span.end.offset]
    }
}

fn is_ascii_word(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || ch == '_'
}

fn shared_property(name: &str) -> bool {
    // The Go tables include aliases and newer scripts that the general Rust
    // engine can reject. Keep compound matching on properties both accept.
    matches!(
        name,
        "ASCII"
            | "Any"
            | "Arabic"
            | "Cyrillic"
            | "Greek"
            | "Han"
            | "Hebrew"
            | "Hiragana"
            | "Katakana"
            | "Latin"
    ) || (name.len() <= 2 && name.bytes().all(|byte| byte.is_ascii_alphabetic()))
}

fn folded_ascii(ch: char) -> Option<u8> {
    match ch {
        'K' => Some(b'k'),
        'ſ' => Some(b's'),
        ch if ch.is_ascii() => Some((ch as u8).to_ascii_lowercase()),
        _ => None,
    }
}

fn ascii_members(item: &ClassSetItem) -> Option<u128> {
    Some(match item {
        ClassSetItem::Literal(literal) if literal.c.is_ascii() => 1 << (literal.c as u8),
        ClassSetItem::Range(range) if range.start.c.is_ascii() && range.end.c.is_ascii() => {
            ascii_range(range.start.c as u8, range.end.c as u8)
        }
        ClassSetItem::Ascii(class) if !class.negated => posix_members(&class.kind),
        ClassSetItem::Union(union) => union
            .items
            .iter()
            .try_fold(0, |members, item| Some(members | ascii_members(item)?))?,
        _ => return None,
    })
}

const fn ascii_range(start: u8, end: u8) -> u128 {
    (u128::MAX << start) & (u128::MAX >> (127 - end))
}

fn posix_members(kind: &ClassAsciiKind) -> u128 {
    const LOWER: u128 = ascii_range(b'a', b'z');
    const UPPER: u128 = ascii_range(b'A', b'Z');
    const DIGIT: u128 = ascii_range(b'0', b'9');
    match kind {
        ClassAsciiKind::Alnum => LOWER | UPPER | DIGIT,
        ClassAsciiKind::Alpha => LOWER | UPPER,
        ClassAsciiKind::Ascii => u128::MAX,
        ClassAsciiKind::Blank => (1 << b'\t') | (1 << b' '),
        ClassAsciiKind::Cntrl => ascii_range(0, 0x1f) | (1 << 0x7f),
        ClassAsciiKind::Digit => DIGIT,
        ClassAsciiKind::Graph => ascii_range(b'!', b'~'),
        ClassAsciiKind::Lower => LOWER,
        ClassAsciiKind::Print => ascii_range(b' ', b'~'),
        ClassAsciiKind::Punct => {
            ascii_range(b'!', b'/')
                | ascii_range(b':', b'@')
                | ascii_range(b'[', b'`')
                | ascii_range(b'{', b'~')
        }
        ClassAsciiKind::Space => ascii_range(b'\t', b'\r') | (1 << b' '),
        ClassAsciiKind::Upper => UPPER,
        ClassAsciiKind::Word => LOWER | UPPER | DIGIT | (1 << b'_'),
        ClassAsciiKind::Xdigit => DIGIT | ascii_range(b'A', b'F') | ascii_range(b'a', b'f'),
    }
}
