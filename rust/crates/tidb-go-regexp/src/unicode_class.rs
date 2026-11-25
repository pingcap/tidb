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

//! Direct boolean matching for Go regexp Unicode properties.

use std::ops::RangeInclusive;

use memchr::memchr;

use super::{literal, Boundary, MAX_DIRECT_PATTERN_BYTES};

pub(super) mod compound;

#[derive(Debug)]
struct Class {
    start: usize,
    count: usize,
    latin1: [u64; 4],
    full_bmp: [u64; 4],
    pages: u64,
    utf8_leads: u64,
}

include!("unicode_class_data.rs");

const RANGES: &[[u8; 8]] = include_bytes!("unicode_class_ranges.bin")
    .as_chunks::<8>()
    .0;
const ASCII_ALPHABET: u64 = (1 << 26) - 1;
const ASCII_LETTERS: [u64; 2] = [0, (ASCII_ALPHABET << 1) | (ASCII_ALPHABET << 33)];

#[derive(Clone, Copy, PartialEq, Eq)]
enum Anchor {
    Anywhere,
    Start,
    End,
    Both,
}

#[derive(Clone, Debug)]
pub(super) struct Predicate {
    left: Boundary,
    right: Boundary,
    run: ClassRun,
}

#[derive(Clone, Debug)]
pub(super) struct PrefixedClass {
    prefix: Box<str>,
    run: ClassRun,
    folded: bool,
    folds_non_ascii: bool,
}

impl PrefixedClass {
    pub(super) fn compile(body: &str, folded: bool) -> Option<Self> {
        let (prefix, atom) = body.split_at(body.find('[')?);
        if !prefix.is_ascii() || prefix.chars().any(regex_syntax::is_meta_character) {
            return None;
        }
        let (class, suffix, folds_non_ascii) = if let Some(suffix) = atom.strip_prefix("[0-9]") {
            (&DIGITS, suffix, false)
        } else if let Some(suffix) = atom.strip_prefix("[a-z]") {
            (if folded { &LETTERS } else { &LOWERCASE }, suffix, folded)
        } else {
            let suffix = atom.strip_prefix("[A-Z]")?;
            (if folded { &LETTERS } else { &UPPERCASE }, suffix, folded)
        };
        let (minimum, maximum) = repeat(suffix)?;
        Some(Self {
            prefix: prefix.into(),
            run: ClassRun { minimum, maximum, class, negated: false },
            folded,
            folds_non_ascii,
        })
    }

    pub(super) fn is_match(&self, text: &str) -> bool {
        let remainder = if self.folded {
            literal::folded_width(text.chars(), self.prefix.bytes()).map(|width| &text[width..])
        } else {
            text.strip_prefix(self.prefix.as_ref())
        };
        let Some(remainder) = remainder else {
            return false;
        };
        if self.folds_non_ascii && !remainder.is_ascii() {
            return matches_extent(
                remainder.chars().map(|ch| matches!(ch, 'K' | 'ſ') || self.run.contains(ch)),
                Anchor::Both,
                self.run.minimum,
                self.run.maximum,
            );
        }
        self.run.matches_all(remainder)
    }
}

#[derive(Clone, Copy, Debug)]
struct ClassRun {
    minimum: usize,
    maximum: usize,
    class: &'static Class,
    negated: bool,
}

pub(super) fn compile(
    pattern: &str,
    left: Boundary,
    right: Boundary,
    case_insensitive: bool,
) -> Option<Result<Predicate, String>> {
    Some(compile_property(pattern, case_insensitive)?.map(|run| Predicate { left, right, run }))
}

fn split_property(pattern: &str) -> Option<(&str, &str, bool)> {
    let (body, mut negated) = if let Some(body) = pattern.strip_prefix(r"\p") {
        (body, false)
    } else {
        (pattern.strip_prefix(r"\P")?, true)
    };
    let (name, suffix) = if let Some(braced) = body.strip_prefix('{') {
        braced.split_once('}')?
    } else {
        body.split_at(body.chars().next()?.len_utf8())
    };
    let name = if let Some(name) = name.strip_prefix('^') {
        negated = !negated;
        name
    } else {
        name
    };
    Some((name, suffix, negated))
}

fn compile_property(pattern: &str, case_insensitive: bool) -> Option<Result<ClassRun, String>> {
    let (name, suffix, negated) = split_property(pattern)?;
    let (minimum, maximum) = repeat(suffix)?;
    let Some(class) = class(name, case_insensitive) else {
        let property = &pattern[..pattern.len() - suffix.len()];
        return Some(Err(format!("invalid character class range: `{property}`")));
    };
    Some(Ok(ClassRun {
        minimum,
        maximum,
        class,
        negated,
    }))
}

impl Predicate {
    pub(super) fn is_match(&self, text: &str) -> bool {
        let Self { left, right, run } = *self;
        let ClassRun {
            minimum,
            maximum,
            class,
            negated,
        } = run;
        let anchor = match (left != Boundary::Anywhere, right != Boundary::Anywhere) {
            (false, false) => Anchor::Anywhere,
            (true, false) => Anchor::Start,
            (false, true) => Anchor::End,
            (true, true) => Anchor::Both,
        };
        if text.is_empty() || (minimum == 0 && anchor != Anchor::Both) {
            return minimum == 0;
        }
        let ascii = [class.latin1[0], class.latin1[1]];
        let ascii = if negated {
            ascii.map(|word| !word)
        } else {
            ascii
        };
        if left != Boundary::Line && right != Boundary::Line && text.is_ascii() {
            return match ascii {
                [0, 0] => false,
                [u64::MAX, u64::MAX] => {
                    text.len() >= minimum && (anchor != Anchor::Both || text.len() <= maximum)
                }
                ASCII_LETTERS if anchor == Anchor::Both => {
                    (minimum..=maximum).contains(&text.len()) && all_ascii_letters(text)
                }
                _ => {
                    let bytes = text.bytes();
                    let contains = |byte| contains_bit(&class.latin1, byte) != negated;
                    if anchor == Anchor::End {
                        matches_extent(bytes.rev().map(contains), anchor, minimum, maximum)
                    } else {
                        matches_extent(bytes.map(contains), anchor, minimum, maximum)
                    }
                }
            };
        }
        let contains = |ch: char| class.contains(ch) != negated;
        if left == Boundary::Line || right == Boundary::Line {
            return matches_lines(text, contains, left, right, minimum, maximum);
        }
        if anchor == Anchor::Anywhere && minimum == 1 && ascii == [0, 0] && !negated {
            return any_unicode_member(text, class.utf8_leads, contains);
        }
        let chars = text.chars();
        if anchor == Anchor::End {
            matches_extent(chars.rev().map(contains), anchor, minimum, maximum)
        } else {
            matches_extent(chars.map(contains), anchor, minimum, maximum)
        }
    }
}

impl Class {
    #[inline]
    fn contains(&self, ch: char) -> bool {
        let ch = ch as u32;
        if ch <= u32::from(u8::MAX) {
            return contains_bit(&self.latin1, ch as u8);
        }
        // An absent page rules out the rune before the range lookup.
        if self.pages & (1 << ((ch >> 10) & 63)) == 0 {
            return false;
        }
        if ch <= u32::from(u16::MAX) && contains_bit(&self.full_bmp, (ch >> 8) as u8) {
            return true;
        }
        let ranges = &RANGES[self.start..self.start + self.count];
        let index = ranges.partition_point(|entry| word(&entry[4..]) < ch);
        ranges
            .get(index)
            .is_some_and(|entry| word(&entry[..4]) <= ch)
    }
}

/// Fully anchored class pairs and property runs with a literal suffix.
pub(super) fn match_pair(
    text: &str,
    body: &str,
    left: Boundary,
    right: Boundary,
    folded: bool,
) -> Option<bool> {
    if left != Boundary::Text || right != Boundary::Text {
        return None;
    }
    let body = literal::strip_group(body).unwrap_or(body);
    if !folded && body.starts_with('[') {
        return compound::match_ascii_pair(text, body);
    }
    let (_, suffix, _) = split_property(body)?;
    let (_, tail) = split_repeat(suffix)?;
    let first = compile_property(&body[..body.len() - tail.len()], folded)?.ok()?;
    if !tail.is_empty() {
        let mut decoded = [0; MAX_DIRECT_PATTERN_BYTES];
        if let Some(literal) = literal::decode(tail, &mut decoded) {
            let predicate = Predicate {
                left,
                right,
                run: first,
            };
            if !folded {
                return Some(
                    text.strip_suffix(literal)
                        .is_some_and(|head| predicate.is_match(head)),
                );
            }
            if literal.is_ascii() {
                let width = literal::folded_width(text.chars().rev(), literal.bytes().rev());
                return Some(
                    width.is_some_and(|width| predicate.is_match(&text[..text.len() - width])),
                );
            }
        }
    }
    let (separator, tail) = if tail.starts_with('\\') {
        (None, tail)
    } else {
        let byte = *tail.as_bytes().first()?;
        if !byte.is_ascii()
            || byte.is_ascii_alphabetic()
            || regex_syntax::is_meta_character(byte.into())
        {
            return None;
        }
        (Some(byte), &tail[1..])
    };
    let second = if let Some(suffix) = tail.strip_prefix(r"\d") {
        let (minimum, maximum) = repeat(suffix)?;
        ClassRun {
            minimum,
            maximum,
            class: &DIGITS,
            negated: false,
        }
    } else {
        compile_property(tail, folded)?.ok()?
    };
    Some(match_runs(text, &first, &second, separator))
}

static DIGITS: Class = ascii_class(0x3ff << b'0', 0);
static LETTERS: Class = ascii_class(0, ASCII_LETTERS[1]);
static LOWERCASE: Class = ascii_class(0, ASCII_ALPHABET << (b'a' - 64));
static UPPERCASE: Class = ascii_class(0, ASCII_ALPHABET << (b'A' - 64));
static WORD: Class = ascii_class(0x3ff << b'0', ASCII_LETTERS[1] | (1 << (b'_' - 64)));

const fn ascii_class(low: u64, high: u64) -> Class {
    Class {
        start: 0,
        count: 0,
        latin1: [low, high, 0, 0],
        full_bmp: [0; 4],
        pages: 0,
        utf8_leads: 0,
    }
}

pub(super) fn match_ascii_class(
    text: &str,
    body: &str,
    left: Boundary,
    right: Boundary,
    folded: bool,
) -> Option<bool> {
    let (class, suffix) = if !folded {
        [
            ("[a-z]", &LOWERCASE),
            ("[A-Z]", &UPPERCASE),
            ("[A-Za-z0-9_]", &WORD),
            (r"\w", &WORD),
        ]
        .into_iter()
        .find_map(|(atom, class)| body.strip_prefix(atom).map(|suffix| (class, suffix)))
    } else {
        None
    }
    .or_else(|| {
        body.strip_prefix("[0-9]")
            .or_else(|| body.strip_prefix(r"\d"))
            .map(|suffix| (&DIGITS, suffix))
    })?;
    let (minimum, maximum) = repeat(suffix)?;
    if minimum == 1 && left == Boundary::Anywhere && right == Boundary::Anywhere {
        return Some(text.bytes().any(|byte| contains_bit(&class.latin1, byte)));
    }
    let predicate = Predicate {
        left,
        right,
        run: ClassRun {
            minimum,
            maximum,
            class,
            negated: false,
        },
    };
    Some(predicate.is_match(text))
}

fn match_runs(text: &str, first: &ClassRun, second: &ClassRun, separator: Option<u8>) -> bool {
    if let Some(byte) = separator {
        if !first.contains(byte.into()) && !second.contains(byte.into()) {
            let Some(index) = memchr(byte, text.as_bytes()) else {
                return false;
            };
            return first.matches_all(&text[..index]) && second.matches_all(&text[index + 1..]);
        }
    }
    let Some(prefix) = first.edge(text.chars()) else {
        return false;
    };
    if separator.is_none() && *prefix.end() == text.len() {
        // The prefix can reach the end of the string. Leave the shortest
        // allowed suffix so the prefix has the greatest chance of fitting.
        let mut suffix = text.chars().rev();
        let mut width = 0;
        for _ in 0..second.minimum {
            let Some(ch) = suffix.next().filter(|&ch| second.contains(ch)) else {
                return false;
            };
            width += ch.len_utf8();
        }
        return text.len() - width >= *prefix.start();
    }
    let Some(suffix) = second.edge(text.chars().rev()) else {
        return false;
    };
    let separator_len = usize::from(separator.is_some());
    // A valid split belongs to both runs. Every rune boundary between a
    // run's minimum and maximum width is reachable by a valid repeat count.
    let start = (*prefix.start()).max((text.len() - suffix.end()).saturating_sub(separator_len));
    let Some(end) = (text.len() - suffix.start()).checked_sub(separator_len) else {
        return false;
    };
    let end = (*prefix.end()).min(end);
    start <= end && separator.is_none_or(|byte| text.as_bytes()[start..=end].contains(&byte))
}

impl ClassRun {
    fn contains(&self, ch: char) -> bool {
        self.class.contains(ch) != self.negated
    }

    fn matches_all(&self, text: &str) -> bool {
        if text.is_ascii() {
            return Predicate {
                left: Boundary::Text,
                right: Boundary::Text,
                run: *self,
            }
            .is_match(text);
        }
        self.edge(text.chars())
            .is_some_and(|width| *width.end() == text.len())
    }

    fn edge(&self, chars: impl Iterator<Item = char>) -> Option<RangeInclusive<usize>> {
        let mut minimum_bytes = 0;
        let mut bytes = 0;
        let mut previous = None;
        for (index, ch) in chars.take(self.maximum).enumerate() {
            if previous != Some(ch) && !self.contains(ch) {
                break;
            }
            previous = Some(ch);
            bytes += ch.len_utf8();
            if index + 1 == self.minimum {
                minimum_bytes = bytes;
            }
        }
        (self.minimum == 0 || minimum_bytes != 0).then_some(minimum_bytes..=bytes)
    }
}

fn all_ascii_letters(text: &str) -> bool {
    let (blocks, tail) = text.as_bytes().as_chunks::<16>();
    blocks.iter().all(|block| {
        block.map(|byte| ((byte | 0x20).wrapping_sub(b'a') <= b'z' - b'a') as u8) == [1; 16]
    }) && tail.iter().all(u8::is_ascii_alphabetic)
}

fn any_unicode_member(mut text: &str, leads: u64, contains: impl Fn(char) -> bool) -> bool {
    while let Some(index) = text
        .bytes()
        .position(|byte| byte >= 0xc2 && leads & (1 << (byte & 63)) != 0)
    {
        // A valid UTF-8 continuation byte is below 0xc2.
        let current = text[index..].chars().next().unwrap();
        if contains(current) {
            return true;
        }
        text = &text[index + current.len_utf8()..];
    }
    false
}

fn matches_lines(
    text: &str,
    contains: impl Fn(char) -> bool,
    left: Boundary,
    right: Boundary,
    minimum: usize,
    maximum: usize,
) -> bool {
    let mut starts = std::iter::once(0)
        .chain(
            text.chars()
                .enumerate()
                .filter_map(|(index, ch)| (ch == '\n').then_some(index + 1)),
        )
        .peekable();
    let mut run_start = 0;
    for (end, ch) in text
        .chars()
        .map(Some)
        .chain(std::iter::once(None))
        .enumerate()
    {
        let can_end = right == Boundary::Anywhere
            || ch.is_none()
            || (right == Boundary::Line && ch == Some('\n'));
        if can_end && end >= minimum {
            // Candidate starts must lie in the current run and meet the
            // count bounds. Both endpoints of this range move forward.
            let first = run_start.max(end.saturating_sub(maximum));
            let last = end - minimum;
            if first <= last {
                let can_start = match left {
                    Boundary::Anywhere => true,
                    Boundary::Text => first == 0,
                    Boundary::Line => {
                        while starts.peek().is_some_and(|&start| start < first) {
                            starts.next();
                        }
                        starts.peek().is_some_and(|&start| start <= last)
                    }
                };
                if can_start {
                    return true;
                }
            }
        }
        if ch.is_some_and(|ch| !contains(ch)) {
            run_start = end + 1;
        }
    }
    false
}

fn split_repeat(tail: &str) -> Option<(&str, &str)> {
    let mut end = match tail.as_bytes().first() {
        Some(b'+' | b'*' | b'?') => 1,
        Some(b'{') => tail.find('}')? + 1,
        _ => 0,
    };
    if end != 0 && tail.as_bytes().get(end) == Some(&b'?') {
        end += 1;
    }
    Some(tail.split_at(end))
}

fn repeat(suffix: &str) -> Option<(usize, usize)> {
    let suffix = if suffix.len() > 1 {
        suffix.strip_suffix('?').unwrap_or(suffix)
    } else {
        suffix
    };
    Some(match suffix {
        "" => (1, 1),
        "+" => (1, usize::MAX),
        "*" => (0, usize::MAX),
        "?" => (0, 1),
        _ => {
            let bounds = suffix.strip_prefix('{')?.strip_suffix('}')?;
            let number = |digits: &str| {
                if digits.len() > 1 && digits.starts_with('0')
                    || !digits.bytes().all(|byte| byte.is_ascii_digit())
                {
                    return None;
                }
                digits.parse::<usize>().ok().filter(|&value| value <= 1000)
            };
            if let Some((min, max)) = bounds.split_once(',') {
                let min = number(min)?;
                let max = if max.is_empty() {
                    usize::MAX
                } else {
                    number(max)?
                };
                if min > max {
                    return None;
                }
                (min, max)
            } else {
                let count = number(bounds)?;
                (count, count)
            }
        }
    })
}

fn matches_extent(
    mut units: impl Iterator<Item = bool>,
    anchor: Anchor,
    minimum: usize,
    maximum: usize,
) -> bool {
    match anchor {
        Anchor::Anywhere if minimum == 1 => units.any(|matched| matched),
        Anchor::Anywhere => {
            let mut run = 0;
            units.any(|matched| {
                run = if matched { run + 1 } else { 0 };
                run >= minimum
            })
        }
        Anchor::Start | Anchor::End => {
            units.take(minimum).take_while(|&matched| matched).count() == minimum
        }
        Anchor::Both if maximum == usize::MAX && minimum <= 1 => units.all(|matched| matched),
        Anchor::Both => {
            let mut count = 0;
            units.all(|matched| {
                count += 1;
                matched && count <= maximum
            }) && count >= minimum
        }
    }
}

fn class(name: &str, folded: bool) -> Option<&'static Class> {
    let mut canonical = [0; 64];
    let mut len = 0;
    for byte in name.bytes().filter(|b| !matches!(b, b'_' | b'-' | b' ')) {
        *canonical.get_mut(len)? = byte.to_ascii_lowercase();
        len += 1;
    }
    let index = NAMES
        .binary_search_by(|(key, _)| key.as_bytes().cmp(&canonical[..len]))
        .ok()?;
    Some(&CLASSES[NAMES[index].1[usize::from(folded)]])
}

fn contains_bit(mask: &[u64; 4], byte: u8) -> bool {
    mask[usize::from(byte >> 6)] & (1 << (byte & 63)) != 0
}

fn word(value: &[u8]) -> u32 {
    u32::from_le_bytes(value.try_into().unwrap())
}
