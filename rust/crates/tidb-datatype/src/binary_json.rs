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

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, OnceLock};

use bytes::Bytes;

use serde_json::{Map, Number, Value};

use crate::{CoreTime, MySqlDuration, Time, TimeType};

/// BinaryJSON object type code.
pub const JSON_TYPE_CODE_OBJECT: u8 = 0x01;
/// BinaryJSON array type code.
pub const JSON_TYPE_CODE_ARRAY: u8 = 0x03;
/// BinaryJSON literal type code.
pub const JSON_TYPE_CODE_LITERAL: u8 = 0x04;
/// BinaryJSON signed integer type code.
pub const JSON_TYPE_CODE_INT64: u8 = 0x09;
/// BinaryJSON unsigned integer type code.
pub const JSON_TYPE_CODE_UINT64: u8 = 0x0a;
/// BinaryJSON double type code.
pub const JSON_TYPE_CODE_FLOAT64: u8 = 0x0b;
/// BinaryJSON string type code.
pub const JSON_TYPE_CODE_STRING: u8 = 0x0c;
/// BinaryJSON opaque type code.
pub const JSON_TYPE_CODE_OPAQUE: u8 = 0x0d;
/// BinaryJSON DATE type code.
pub const JSON_TYPE_CODE_DATE: u8 = 0x0e;
/// BinaryJSON DATETIME type code.
pub const JSON_TYPE_CODE_DATETIME: u8 = 0x0f;
/// BinaryJSON TIMESTAMP type code.
pub const JSON_TYPE_CODE_TIMESTAMP: u8 = 0x10;
/// BinaryJSON TIME/duration type code.
pub const JSON_TYPE_CODE_DURATION: u8 = 0x11;

/// BinaryJSON null literal.
pub const JSON_LITERAL_NULL: u8 = 0;
/// BinaryJSON true literal.
pub const JSON_LITERAL_TRUE: u8 = 1;
/// BinaryJSON false literal.
pub const JSON_LITERAL_FALSE: u8 = 2;

const HEADER_SIZE: usize = 8;
const KEY_ENTRY_SIZE: usize = 6;
const VALUE_ENTRY_SIZE: usize = 5;
pub(crate) const MAX_JSON_DEPTH: usize = 100;
pub(crate) const EMPTY_JSON_CONTAINER: &[u8; HEADER_SIZE] = &[0, 0, 0, 0, 8, 0, 0, 0];
/// Source `floatEpsilon`: the precision loss tolerated when a JSON double is
/// compared against a JSON integer.
const FLOAT_EPSILON: f64 = 1e-8;

/// Source-compatible `type code + value bytes` BinaryJSON representation.
#[derive(Clone)]
pub struct BinaryJSON {
    type_code: u8,
    value: Bytes,
    layout: OnceLock<BinaryLayout>,
}

#[derive(Clone, Copy)]
enum BinaryLayout {
    // The writer produced a canonical layout without recording its depth.
    Canonical,
    Validated(ValidatedLayout),
    Invalid,
    TooDeep,
}

/// The layout result for one exact binary JSON value.
#[doc(hidden)]
#[derive(Clone, Copy)]
pub struct EncodedJSONLayout(BinaryLayout);

impl EncodedJSONLayout {
    pub(crate) fn canonical() -> Self {
        Self(BinaryLayout::Canonical)
    }

    /// Whether the payload passed the binary JSON layout checks.
    #[inline]
    pub fn is_valid(self) -> bool {
        matches!(self.0, BinaryLayout::Canonical | BinaryLayout::Validated(_))
    }

    /// Whether the checked value has canonical key order throughout.
    pub fn is_canonical(self) -> bool {
        matches!(
            self.0,
            BinaryLayout::Canonical
                | BinaryLayout::Validated(ValidatedLayout { canonical: true, .. })
        )
    }

    fn from_validation(result: Result<ValidatedLayout, BinaryJSONError>) -> Self {
        Self(match result {
            Ok(layout) => BinaryLayout::Validated(layout),
            Err(BinaryJSONError::TooDeep) => BinaryLayout::TooDeep,
            Err(_) => BinaryLayout::Invalid,
        })
    }
}

/// A checked prefix containing a packed object's directories and keys.
/// Values with validation that depends on their payload are checked on every read.
#[doc(hidden)]
pub struct CheckedJSONKeys {
    prefix: Vec<u8>,
    packed_keys: Option<PackedObjectKeys>,
    value_checks: PayloadChecks,
}

enum PayloadChecks {
    Fields(Vec<PayloadCheck>),
    Run(Box<CheckedChildRun>),
    General,
}

#[cfg(test)]
impl PayloadChecks {
    fn as_deref(&self) -> Option<&[PayloadCheck]> {
        match self {
            Self::Fields(checks) => Some(checks),
            _ => None,
        }
    }
}

struct PackedObjectKeys {
    data_start: usize,
    last_start: usize,
    previous_start: usize,
    fixed_width: Option<usize>,
}

impl PackedObjectKeys {
    fn new(entries: &[[u8; KEY_ENTRY_SIZE]], data_start: usize, end: usize) -> Option<Self> {
        let mut next = data_start;
        let mut fixed_width = None;
        let mut last_start = next;
        let mut previous_start = next;
        for (index, &[a, b, c, d, e, f]) in entries.iter().enumerate() {
            let start = 1 + u32::from_le_bytes([a, b, c, d]) as usize;
            let width = usize::from(u16::from_le_bytes([e, f]));
            if start != next || width == 0 {
                return None;
            }
            if index == 0 {
                fixed_width = Some(width);
            } else if fixed_width != Some(width) {
                fixed_width = None;
            }
            previous_start = last_start;
            last_start = start;
            next = start + width;
        }
        (next == end).then_some(Self { data_start, last_start, previous_start, fixed_width })
    }

    fn matches(&self, prefix: &[u8], cell: &[u8]) -> bool {
        let Some(current) = cell.get(..prefix.len()) else {
            return false;
        };
        if current.starts_with(&prefix[..self.last_start]) {
            let key = &current[self.last_start..];
            return key == &prefix[self.last_start..]
                || (is_valid_json_text(key) && &current[self.previous_start..self.last_start] < key);
        }
        if !current.starts_with(&prefix[..self.data_start]) {
            return false;
        }

        let current = &current[self.data_start..];
        let previous = &prefix[self.data_start..];
        let (chunks, _) = current.as_chunks::<32>();
        let (old_chunks, _) = previous.as_chunks::<32>();
        let matched = chunks.iter().zip(old_chunks).take_while(|(a, b)| a == b).count() * 32;
        let Some(difference) = current[matched..].iter().zip(&previous[matched..])
            .position(|(a, b)| a != b) else {
            return true;
        };
        let difference = matched + difference;
        let count = (self.data_start - 1 - HEADER_SIZE) / (KEY_ENTRY_SIZE + VALUE_ENTRY_SIZE);
        let entries = prefix[1 + HEADER_SIZE..1 + HEADER_SIZE + count * KEY_ENTRY_SIZE]
            .as_chunks::<KEY_ENTRY_SIZE>().0;
        let offset = |&[a, b, c, d, ..]: &[u8; KEY_ENTRY_SIZE]| {
            1 + u32::from_le_bytes([a, b, c, d]) as usize - self.data_start
        };
        let index = entries.partition_point(|entry| offset(entry) <= difference) - 1;
        let start = offset(&entries[index]);
        let end = entries.get(index + 1).map_or(current.len(), offset);
        if current[end..] == previous[end..] {
            let key = &current[start..end];
            return is_valid_json_text(key)
                && (index == 0 || &current[offset(&entries[index - 1])..start] < key)
                && entries.get(index + 1).is_none_or(|_| {
                    key < &current[end..entries.get(index + 2).map_or(current.len(), offset)]
                });
        }

        if let Some(width @ 1..=16) = self.fixed_width {
            let padding = 16 - width;
            if let Some(words) = cell[self.data_start..].get(..current.len() + padding) {
                // Each window starts at a key boundary. Ignore bytes past the key.
                let mask = u128::MAX << (padding * 8);
                return current.is_ascii()
                    && words
                        .array_windows::<16>()
                        .step_by(width)
                        .map(|word| u128::from_be_bytes(*word) & mask)
                        .is_sorted_by(|a, b| a < b);
            }
            return check_short_ascii_keys(&cell[1..], entries, prefix.len() - 1).is_some();
        }
        // The directories prove the key boundaries. One ASCII check covers
        // all the keys, so only their ordering remains to be verified.
        if !current.is_ascii() {
            return false;
        }
        if let Some(width) = self.fixed_width {
            return current.chunks_exact(width).is_sorted_by(|a, b| a < b);
        }
        let mut remaining = current;
        entries.iter().map(|&[_, _, _, _, low, high]| {
            let (key, rest) = remaining.split_at(usize::from(u16::from_le_bytes([low, high])));
            remaining = rest;
            key
        }).is_sorted_by(|a, b| a < b)
    }
}

enum PayloadCheck {
    Value(usize),
    Strings(PackedStringRun),
    Child(CheckedChildLayout),
    ChildWithValues(Box<CheckedChildValues>),
}

struct CheckedChildLayout {
    index: usize,
    offset: usize,
    prefix: Box<[u8]>,
    depth: u8,
    last_key: Option<CheckedChildLastKey>,
    last_key_changed: bool,
}

struct CheckedChildLastKey {
    start: u8,
    previous: u8,
    order_prefix_end: u8,
}

impl CheckedChildLayout {
    #[inline]
    fn matches(&mut self, value: &[u8]) -> bool {
        (!self.last_key_changed && value.starts_with(&self.prefix))
            || self.matches_changed_key(value)
    }

    #[cold]
    fn matches_changed_key(&mut self, value: &[u8]) -> bool {
        let Some(key) = &self.last_key else {
            return false;
        };
        let start = usize::from(key.start);
        let Some(last) = value.get(start..self.prefix.len()) else {
            return false;
        };
        let order_end = usize::from(key.order_prefix_end);
        let order_prefix = &self.prefix[..order_end];
        let order_matches = if (48..=64).contains(&order_end) {
            let current = &value[..order_end];
            current.first_chunk::<48>() == order_prefix.first_chunk::<48>()
                && current.last_chunk::<16>() == order_prefix.last_chunk::<16>()
        } else {
            value.starts_with(order_prefix)
        };
        let ordered = order_matches
            || (value.starts_with(&self.prefix[..start])
                && &self.prefix[usize::from(key.previous)..start] < last);
        let matches = ordered && is_valid_child_json_text(last);
        self.last_key_changed |= matches;
        matches
    }
}

struct CheckedChildValues {
    layout: CheckedChildLayout,
    size: usize,
    checks: ChildValueChecks,
}

enum ChildValueChecks {
    // Keep this common pair inline so each row can check both offsets directly.
    FloatAndString { number: usize, text: usize },
    General(Box<[ChildValueCheck]>),
}

struct CheckedChildRun {
    prefix: Box<[u8]>,
    depth: u8,
    offset: usize,
    size: usize,
    count: usize,
    number: usize,
    text: usize,
}

impl CheckedChildRun {
    #[cold]
    fn find(parent: JSONContainer<'_>) -> Option<Box<Self>> {
        let end = parent.value_entries + parent.count * VALUE_ENTRY_SIZE;
        let entries = parent.value[parent.value_entries..end].as_chunks::<VALUE_ENTRY_SIZE>().0;
        let first = entries.first()?;
        if !matches!(first[0], JSON_TYPE_CODE_OBJECT | JSON_TYPE_CODE_ARRAY)
            || entries.last()?[0] != first[0]
        {
            return None;
        }
        let (PayloadCheck::ChildWithValues(proof), _) = PayloadCheck::for_child(parent, 0, first, 128)? else {
            return None;
        };
        let ChildValueChecks::FloatAndString { number, text } = proof.checks else {
            return None;
        };
        let mut remaining = &parent.value[proof.layout.offset..];
        for entry in entries {
            let offset = parent.value.len() - remaining.len();
            if entry[0] != first[0] || value_entry_offset(entry) != offset {
                return None;
            }
            let (child, rest) = remaining.split_at_checked(proof.size)?;
            if !child.starts_with(&proof.layout.prefix) {
                return None;
            }
            remaining = rest;
        }
        Some(Box::new(Self {
            prefix: proof.layout.prefix,
            depth: proof.layout.depth,
            offset: proof.layout.offset,
            size: proof.size,
            count: entries.len(),
            number,
            text,
        }))
    }

    #[inline(never)]
    fn validate(&self, parent: JSONContainer<'_>) -> Result<ValidatedLayout, BinaryJSONError> {
        let first = self.offset;
        let last = first + (self.count - 1) * self.size;
        let sample = &parent.value[first + self.text..first + self.size];
        // Sample once per row so distinct strings avoid a comparison per child.
        // Only a fully validated string can be reused inside the loop.
        if sample[0] < 128 && sample == &parent.value[last + self.text..last + self.size] {
            self.validate_with_reuse::<true>(parent)
        } else {
            self.validate_with_reuse::<false>(parent)
        }
    }

    #[inline]
    fn validate_with_reuse<const REUSE_TEXT: bool>(
        &self,
        parent: JSONContainer<'_>,
    ) -> Result<ValidatedLayout, BinaryJSONError> {
        let prefix = &self.prefix;
        if (48..=64).contains(&prefix.len()) {
            let front = prefix.first_chunk::<48>().unwrap();
            let tail = prefix.last_chunk::<16>().unwrap();
            let tail_at = prefix.len() - tail.len();
            return self.validate_children::<REUSE_TEXT>(parent, |child| {
                child.first_chunk::<48>() == Some(front)
                    && child[tail_at..].first_chunk::<16>() == Some(tail)
            });
        }
        self.validate_children::<REUSE_TEXT>(parent, |child| child.starts_with(prefix))
    }

    #[inline]
    fn validate_children<const REUSE_TEXT: bool>(
        &self,
        parent: JSONContainer<'_>,
        same_layout: impl Fn(&[u8]) -> bool,
    ) -> Result<ValidatedLayout, BinaryJSONError> {
        let start = self.offset;
        let size = self.size;
        let values = &parent.value[start..start + size * self.count];
        let mut checked_text = None::<&[u8]>;
        let mut matched = self.count;
        let mut layout = ValidatedLayout { canonical: true, depth: 1 };
        for (index, child) in values.chunks_exact(size).enumerate() {
            if !same_layout(child) {
                let current = Self::validate_changed_child(parent, index)?;
                layout.canonical &= current.canonical;
                layout.depth = layout.depth.max(current.depth);
                matched -= 1;
                continue;
            }
            let number = child[self.number..self.number + 8].try_into().unwrap();
            if !f64::from_le_bytes(number).is_finite() {
                return Err(BinaryJSONError::InvalidBinary);
            }
            let payload = &child[self.text..];
            if REUSE_TEXT && checked_text.is_some_and(|text| payload.starts_with(text)) {
                continue;
            }
            validate_string_payload::<false>(payload)?;
            if REUSE_TEXT && payload[0] < 128 {
                // Include the checked length byte so a prefix of another
                // string cannot reuse the previous validation.
                checked_text = Some(&payload[..1 + usize::from(payload[0])]);
            }
        }
        if matched != 0 {
            layout.depth = layout.depth.max(self.depth);
        }
        // The parent above the deepest child adds one nesting level.
        layout.depth += 1;
        Ok(layout)
    }

    #[cold]
    fn validate_changed_child(
        parent: JSONContainer<'_>,
        index: usize,
    ) -> Result<ValidatedLayout, BinaryJSONError> {
        parent.child_payload(index)?.validate_layout_at::<true>(1, false)
    }
}

enum ChildValueCheck {
    Float(usize),
    String(usize),
}

impl PayloadCheck {
    fn for_child(
        parent: JSONContainer<'_>,
        index: usize,
        entry: &[u8; VALUE_ENTRY_SIZE],
        budget: usize,
    ) -> Option<(Self, usize)> {
        let child = parent.child_from_entry(entry).ok()?.container_payload(false).ok()?;
        let end = child.value_entries + child.count * VALUE_ENTRY_SIZE;
        let entries = child.value[child.value_entries..end].as_chunks::<VALUE_ENTRY_SIZE>().0;
        let mut values = Vec::new();
        for entry in entries {
            let offset = value_entry_offset(entry);
            match entry[0] {
                JSON_TYPE_CODE_INT64 | JSON_TYPE_CODE_UINT64
                | JSON_TYPE_CODE_DATE | JSON_TYPE_CODE_DATETIME | JSON_TYPE_CODE_TIMESTAMP
                    if offset <= child.value.len() - 8 => {}
                JSON_TYPE_CODE_LITERAL
                    if matches!(entry[1], JSON_LITERAL_NULL | JSON_LITERAL_FALSE | JSON_LITERAL_TRUE) => {}
                JSON_TYPE_CODE_FLOAT64 if offset <= child.value.len() - 8 => {
                    values.push(ChildValueCheck::Float(offset));
                }
                JSON_TYPE_CODE_STRING if offset < child.value.len() => {
                    values.push(ChildValueCheck::String(offset));
                }
                _ => return None,
            }
        }
        let prefix_end = child.checked_key_prefix(budget.min(128))?;
        let keys = child.value[HEADER_SIZE..child.value_entries].as_chunks::<KEY_ENTRY_SIZE>().0;
        let last_key = if keys.is_empty() {
            None
        } else {
            // Packed keys follow the directories and do not overlap. The
            // parent's packed-key offsets include the type byte.
            PackedObjectKeys::new(keys, 1 + end, 1 + prefix_end).map(|packed| {
                let start = packed.last_start - 1;
                let previous = packed.previous_start - 1;
                let prior_key = &child.value[previous..start];
                // Keep enough of the last key to prove it still sorts after
                // its predecessor, even when the rest of the key changes.
                let order_len = prior_key
                    .iter()
                    .zip(&child.value[start..prefix_end])
                    .take_while(|(a, b)| a == b)
                    .count()
                    + usize::from(!prior_key.is_empty());
                CheckedChildLastKey {
                    start: start as u8,
                    previous: previous as u8,
                    order_prefix_end: (start + order_len) as u8,
                }
            })
        };
        let layout = CheckedChildLayout {
            index,
            offset: value_entry_offset(entry),
            prefix: child.value[..prefix_end].into(),
            depth: 1 + u8::from(child.count != 0),
            last_key,
            last_key_changed: false,
        };
        let check = if values.is_empty() {
            Self::Child(layout)
        } else {
            Self::ChildWithValues(Box::new(CheckedChildValues {
                layout,
                size: child.value.len(),
                checks: match values.as_slice() {
                    [ChildValueCheck::Float(number), ChildValueCheck::String(text)] => {
                        ChildValueChecks::FloatAndString { number: *number, text: *text }
                    }
                    _ => ChildValueChecks::General(values.into_boxed_slice()),
                },
            }))
        };
        Some((check, prefix_end))
    }
}

impl CheckedChildValues {
    #[inline]
    fn validate_values(&self, value: &[u8]) -> Result<(), BinaryJSONError> {
        // The matching parent directory and child header prove this range.
        let value = &value[..self.size];
        let checks = match &self.checks {
            ChildValueChecks::FloatAndString { number, text } => {
                let word = value[*number..*number + 8].try_into().unwrap();
                if !f64::from_le_bytes(word).is_finite() {
                    return Err(BinaryJSONError::InvalidBinary);
                }
                return validate_string_payload::<false>(&value[*text..]);
            }
            ChildValueChecks::General(checks) => checks,
        };
        for check in checks {
            match *check {
                ChildValueCheck::Float(offset) => {
                    let word = value[offset..offset + 8].try_into().unwrap();
                    if !f64::from_le_bytes(word).is_finite() {
                        return Err(BinaryJSONError::InvalidBinary);
                    }
                }
                ChildValueCheck::String(offset) => validate_string_payload::<false>(&value[offset..])?,
            }
        }
        Ok(())
    }
}

struct PackedStringRun {
    start: usize,
    end: usize,
    lengths: PackedStringLengths,
}

enum PackedStringLengths {
    Fixed(u8),
    Varying(Box<[u8]>),
    Wide(Box<PackedWideStringLengths>),
}

struct PackedWideStringLengths {
    lengths: WideStringLengths,
    text: WideStringText,
    adaptive_prefix: Option<WidePrefixProbe>,
}

enum WideStringText {
    General,
    SharedPrefix(Box<[u8]>),
    #[cfg(target_arch = "aarch64")]
    ThreeBytePrefix,
}

struct WidePrefixProbe {
    length: WideStringLength,
    prefix: Option<Box<[u8]>>,
    rows_until_probe: u8,
    interval: u8,
}

impl WidePrefixProbe {
    fn matching_prefix(&mut self, encoded: &[u8]) -> Option<&[u8]> {
        if self.prefix.is_none() {
            if self.rows_until_probe == 0 {
                self.learn(encoded);
            } else {
                self.rows_until_probe -= 1;
            }
        }
        self.prefix
            .as_deref()
            .filter(|prefix| encoded[2..self.length.width()].starts_with(prefix))
    }

    #[cold]
    fn learn(&mut self, encoded: &[u8]) {
        self.rows_until_probe = self.interval - 1;
        // Odd intervals keep probes from locking onto one of two alternating rows.
        self.interval = (2 * self.interval + 1).min(31);
        let width = self.length.width();
        let first = &encoded[..width];
        let middle_start = (encoded.len() / width / 2) * width;
        let middle = &encoded[middle_start..middle_start + width];
        let last = &encoded[encoded.len() - width..];
        if [first, middle, last].iter().any(|cell| !cell.starts_with(&self.length.encoded)) {
            return;
        }
        let Some(prefix) = long_text_prefix(&first[2..]).map(str::as_bytes) else {
            return;
        };
        if middle[2..].starts_with(prefix) && last[2..].starts_with(prefix) {
            self.prefix = Some(prefix.into());
        }
    }
}

enum WideStringLengths {
    Fixed(WideStringLength),
    Varying(Box<[WideStringCell]>),
}

#[derive(Clone, Copy)]
struct WideStringCell {
    length: WideStringLength,
    prefix_len: u16,
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct WideStringLength {
    length: u16,
    encoded: [u8; 2],
}

impl WideStringLength {
    fn read(value: &[u8]) -> Option<Self> {
        let &[low @ 0x80..=0xff, high @ 1..=0x7f] = value.first_chunk::<2>()? else {
            return None;
        };
        Some(Self {
            length: u16::from(low & 0x7f) | (u16::from(high) << 7),
            encoded: [low, high],
        })
    }

    fn width(self) -> usize {
        usize::from(self.length) + self.encoded.len()
    }

    #[inline(always)]
    fn validate(
        self,
        cell: &[u8],
        prefix_len: usize,
        valid_text: &impl Fn(&[u8], usize) -> bool,
    ) -> Result<bool, BinaryJSONError> {
        let (length, text) = cell.split_at(self.encoded.len());
        if length != self.encoded {
            return Ok(false);
        }
        if !valid_text(text, prefix_len) {
            return Err(BinaryJSONError::InvalidBinary);
        }
        Ok(true)
    }
}

impl PackedWideStringLengths {
    fn validate(&mut self, encoded: &[u8]) -> Result<bool, BinaryJSONError> {
        if let Some(prefix) = self
            .adaptive_prefix
            .as_mut()
            .and_then(|probe| probe.matching_prefix(encoded))
        {
            #[cfg(target_arch = "aarch64")]
            if matches!(self.text, WideStringText::ThreeBytePrefix) {
                return self.lengths.validate_cells(encoded, 0, |text, _| {
                    is_valid_json_text(
                        text.strip_prefix(prefix).unwrap_or_else(|| after_three_byte_prefix(text)),
                    )
                });
            }
            return self.lengths.validate_with_prefix(encoded, prefix);
        }
        #[cfg(target_arch = "aarch64")]
        if !matches!(self.text, WideStringText::SharedPrefix(_)) {
            if let WideStringLengths::Fixed(length) = self.lengths {
                let sample = &encoded[2..length.width()];
                let validated = match sample[0] {
                    0xc2..=0xdf => self.validate_mixed_width::<false>(encoded, length.width()),
                    0xf0..=0xf4 => self.validate_mixed_width::<true>(encoded, length.width()),
                    _ => None,
                };
                if let Some(result) = validated {
                    return result;
                }
            }
        }
        match &self.text {
            WideStringText::SharedPrefix(prefix) => {
                self.lengths.validate_with_prefix(encoded, prefix)
            }
            #[cfg(target_arch = "aarch64")]
            WideStringText::ThreeBytePrefix => self.lengths.validate_cells(encoded, 0, |text, _| {
                is_valid_json_text(after_three_byte_prefix(text))
            }),
            WideStringText::General => self.lengths.validate_cells(encoded, 0, |text, _| {
                is_valid_wide_json_text(text)
            }),
        }
    }

    #[cfg(target_arch = "aarch64")]
    #[inline(never)]
    fn validate_mixed_width<const FOUR_FIRST: bool>(
        &self,
        encoded: &[u8],
        width: usize,
    ) -> Option<Result<bool, BinaryJSONError>> {
        let middle = (encoded.len() / width / 2) * width;
        let samples = [
            &encoded[2..width],
            &encoded[middle + 2..middle + width],
            &encoded[encoded.len() - width + 2..],
        ];
        if !samples.iter().all(|text| valid_mixed_pair::<FOUR_FIRST>(text.first_chunk().unwrap())) {
            return None;
        }
        if samples.iter().all(|text| text[..6] == text[6..12]) {
            return Some(self.lengths.validate_cells(encoded, 0, |text, _| {
                validate_mixed_text::<false>(text, after_repeated_pair::<FOUR_FIRST>)
            }));
        }
        // Use the aligned scanner when at least two 192-byte batches fit.
        if width >= 2 + usize::from(FOUR_FIRST) * 4 + 2 * MIXED_PAIR_BATCH_BYTES {
            self.validate_distinct_mixed_width::<FOUR_FIRST, true>(encoded, samples)
        } else {
            self.validate_distinct_mixed_width::<FOUR_FIRST, false>(encoded, samples)
        }
    }

    #[cfg(target_arch = "aarch64")]
    #[inline]
    fn validate_distinct_mixed_width<const FOUR_FIRST: bool, const BATCHED: bool>(
        &self,
        encoded: &[u8],
        samples: [&[u8]; 3],
    ) -> Option<Result<bool, BinaryJSONError>> {
        if samples.iter().all(|text| {
            if BATCHED {
                // Only half the field needs to fit the pattern for this
                // probe. Stop at a whole pair in the normalized order.
                let lead = usize::from(FOUR_FIRST) * 4;
                let end = lead + (text.len().div_ceil(2) - lead).next_multiple_of(6);
                after_mixed_pairs::<FOUR_FIRST, true>(&text[..end]).is_empty()
            } else {
                after_mixed_pairs::<FOUR_FIRST, false>(text).len() <= text.len() / 2
            }
        }) {
            return Some(self.lengths.validate_cells(encoded, 0, |text, _| {
                validate_mixed_text::<BATCHED>(text, after_mixed_pairs::<FOUR_FIRST, BATCHED>)
            }));
        }
        None
    }
}

#[inline]
fn is_valid_wide_json_text(text: &[u8]) -> bool {
    is_wide_ascii(text) || simdutf8::basic::from_utf8(text).is_ok()
}

#[inline]
fn is_wide_ascii(text: &[u8]) -> bool {
    // Two-byte string lengths always encode at least 128 bytes of text.
    let (blocks, tail) = text.as_chunks::<128>();
    if !blocks.iter().all(|block| block.is_ascii()) {
        return false;
    }
    // Check a fixed-width window over the tail so the compiler can unroll
    // the ASCII test. The window may overlap the already checked blocks.
    match tail.len() {
        0 => true,
        1..=32 => text.last_chunk::<32>().unwrap().is_ascii(),
        33..=64 => text.last_chunk::<64>().unwrap().is_ascii(),
        _ => text.last_chunk::<128>().unwrap().is_ascii(),
    }
}

impl WideStringLengths {
    fn validate_with_prefix(&self, encoded: &[u8], prefix: &[u8]) -> Result<bool, BinaryJSONError> {
        self.validate_cells(encoded, prefix.len(), |text, len| {
            is_valid_json_text(text.strip_prefix(&prefix[..len]).unwrap_or(text))
        })
    }

    fn validate_cells(
        &self,
        mut encoded: &[u8],
        fixed_prefix_len: usize,
        valid_text: impl Fn(&[u8], usize) -> bool,
    ) -> Result<bool, BinaryJSONError> {
        match self {
            Self::Fixed(length) => {
                for cell in encoded.chunks_exact(length.width()) {
                    if !length.validate(cell, fixed_prefix_len, &valid_text)? {
                        return Ok(false);
                    }
                }
            }
            Self::Varying(lengths) => {
                for field in lengths {
                    let (cell, remaining) = encoded.split_at(field.length.width());
                    if !field.length.validate(cell, usize::from(field.prefix_len), &valid_text)? {
                        return Ok(false);
                    }
                    encoded = remaining;
                }
            }
        }
        Ok(true)
    }
}

#[inline]
fn three_byte_mismatch(lead: u8, first: u8, second: u8) -> u8 {
    let lead_class = (lead & 0xf0) ^ 0xe0;
    let continuations = ((first ^ 0x80) | (second ^ 0x80)) & 0xc0;
    // Bits 3 and 2 differ for E4 through EB, whose continuation bytes have no
    // restrictions beyond the usual 80 through BF range.
    let lead_range = ((lead ^ (lead >> 1)) & 4) ^ 4;
    lead_class | continuations | lead_range
}

#[cfg(target_arch = "aarch64")]
#[inline]
fn after_three_byte_prefix(mut text: &[u8]) -> &[u8] {
    if text.first().is_some_and(|b| b & 0xf0 == 0xe0) {
        while let Some((group, rest)) = text.split_first_chunk::<48>() {
            // Fixed array size lets the compiler vectorize 16 complete characters.
            let mismatches = std::array::from_fn(|i| {
                three_byte_mismatch(group[3 * i], group[3 * i + 1], group[3 * i + 2])
            });
            if u128::from_ne_bytes(mismatches) != 0 {
                break;
            }
            text = rest;
        }
        if text.len() <= 16 {
            while let Some((&[lead, first, second], rest)) = text.split_first_chunk::<3>() {
                if three_byte_mismatch(lead, first, second) != 0 {
                    break;
                }
                text = rest;
            }
        }
    }
    text
}

#[cfg(target_arch = "aarch64")]
#[inline]
fn validate_mixed_text<const RESUME: bool>(text: &[u8], peel_pairs: impl Fn(&[u8]) -> &[u8]) -> bool {
    let tail = if text[0] & 0xf0 == 0xe0 {
        after_three_byte_prefix(text)
    } else {
        peel_pairs(text)
    };
    if RESUME && tail.len() >= 6 + MIXED_PAIR_BATCH_BYTES && tail.len() != text.len() {
        let (block, rest) = tail.split_first_chunk::<6>().unwrap();
        // Verify a complete window before resuming. If the window ends
        // inside a character, validate the original suffix instead.
        if rest.first_chunk().is_some_and(valid_mixed_pair::<false>)
            && simdutf8::basic::from_utf8(block).is_ok()
        {
            return is_valid_json_text(after_mixed_pairs::<false, true>(rest));
        }
    }
    is_valid_json_text(tail)
}

#[cfg(target_arch = "aarch64")]
#[inline]
fn mixed_pair<const FOUR_FIRST: bool>(pair: &[u8; 6]) -> [u8; 6] {
    if FOUR_FIRST {
        [pair[4], pair[5], pair[0], pair[1], pair[2], pair[3]]
    } else {
        *pair
    }
}

#[cfg(target_arch = "aarch64")]
#[inline]
fn valid_mixed_pair<const FOUR_FIRST: bool>(pair: &[u8; 6]) -> bool {
    let [a, b, c, d, e, f] = mixed_pair::<FOUR_FIRST>(pair);
    let continuations = ((b ^ 0x80) | (d ^ 0x80) | (e ^ 0x80) | (f ^ 0x80)) & 0xc0;
    a.wrapping_sub(0xc2) < 30 && c.wrapping_sub(0xf0) < 5 && continuations == 0
        && (c != 0xf0 || d >= 0x90) && (c != 0xf4 || d < 0x90)
}

#[cfg(target_arch = "aarch64")]
#[inline]
fn mixed_pair_mismatch(words: &[[u8; 2]], start: usize) -> u16 {
    let two = u16::from_be_bytes(words[start]);
    let four = u16::from_be_bytes(words[start + 1]);
    let tail = u16::from_be_bytes(words[start + 2]);
    let two_outside_range = u16::from(two.wrapping_sub(0xc280) > 0x1d3f);
    // F090..=F48F is exactly 1,024 words. The continuation mask
    // excludes gaps between lead bytes within that range.
    let four_outside_range = four.wrapping_sub(0xf090) & 0xfc00;
    let second_bytes = ((two | four) & 0x0040) | ((!two | !four) & 0x0080);
    let remaining_bytes = (tail & 0xc0c0) ^ 0x8080;
    two_outside_range | four_outside_range | second_bytes | remaining_bytes
}

#[cfg(target_arch = "aarch64")]
#[inline]
fn after_mixed_pairs<const FOUR_FIRST: bool, const BATCHED: bool>(mut text: &[u8]) -> &[u8] {
    if FOUR_FIRST {
        let Some(pair) = text.first_chunk::<6>() else { return text };
        if !valid_mixed_pair::<true>(pair) {
            return text;
        }
        // Remove the proven leading character, so the rest uses the
        // same two-then-four order as the forward scan.
        text = &text[4..];
    }
    if BATCHED {
        text = after_mixed_pair_batches(text);
    }
    while let Some((group, rest)) = text.split_first_chunk::<48>() {
        let words = group.as_chunks::<2>().0;
        let invalid: [u16; 8] =
            std::array::from_fn(|index| mixed_pair_mismatch(words, 3 * index));
        if invalid.iter().fold(0, |bits, error| bits | error) != 0 {
            if BATCHED {
                // The earlier lanes already prove complete pairs.
                let verified_pairs = invalid.iter().position(|&error| error != 0).unwrap();
                return &text[6 * verified_pairs..];
            }
            return text;
        }
        text = rest;
    }
    while let Some((pair, rest)) = text.split_first_chunk::<6>() {
        if mixed_pair_mismatch(pair.as_chunks::<2>().0, 0) != 0 {
            break;
        }
        text = rest;
    }
    if FOUR_FIRST {
        if let Some((&[lead, continuation], rest)) = text.split_first_chunk::<2>() {
            if (0xc2..=0xdf).contains(&lead) && continuation & 0xc0 == 0x80 {
                return rest;
            }
        }
    }
    text
}

#[cfg(target_arch = "aarch64")]
const MIXED_PAIR_BATCH_BYTES: usize = 192;

#[cfg(target_arch = "aarch64")]
#[inline]
fn after_mixed_pair_batches(text: &[u8]) -> &[u8] {
    use zerocopy::FromBytes;

    // Load aligned words directly. On a failed batch the 48-byte scanner
    // can still prove a shorter prefix.
    let even_end = text.len() & !1;
    let Ok(words) = <[u16]>::ref_from_bytes(&text[..even_end]) else {
        return after_shifted_mixed_pair_batches(text);
    };
    let mut consumed = 0;
    for batch in words.as_chunks::<{ MIXED_PAIR_BATCH_BYTES / 2 }>().0 {
        let valid = batch.as_chunks::<3>().0.iter().fold(true, |valid, &[two, four, tail]| {
            let two = two.to_le();
            let four = four.to_le();
            let tail = tail.to_le();
            // Each lead selects four ranges of the second byte. F0 owns
            // ranges 1–3, F1 through F3 own 4–15, and F4 owns 16.
            let four_range = ((four & 7) << 2) | ((four >> 12) & 3);
            // A bitwise reduction allows every pair in the batch to be
            // checked without a branch per character.
            valid & (two & 0xc0e0 == 0x80c0)
                & (two & 0x001e != 0)
                & (four & 0xc0f8 == 0x80f0)
                & (four_range.wrapping_sub(1) < 16)
                & (tail & 0xc0c0 == 0x8080)
        });
        if !valid {
            break;
        }
        consumed += MIXED_PAIR_BATCH_BYTES;
    }
    &text[consumed..]
}

#[cfg(target_arch = "aarch64")]
#[inline(never)]
fn after_shifted_mixed_pair_batches(text: &[u8]) -> &[u8] {
    use zerocopy::FromBytes;

    if text.len() <= MIXED_PAIR_BATCH_BYTES || !(0xc2..=0xdf).contains(&text[0]) {
        return text;
    }
    let end = 1 + ((text.len() - 1) & !1);
    let Ok(words) = <[u16]>::ref_from_bytes(&text[1..end]) else {
        return text;
    };
    let mut consumed = 0;
    for batch in words.as_chunks::<{ MIXED_PAIR_BATCH_BYTES / 2 }>().0 {
        let pairs = batch.as_chunks::<3>().0;
        let valid = pairs.iter().enumerate().fold(
            true,
            |valid, (index, &[first, middle, last])| {
                // After skipping the first lead, the words contain the
                // bytes [b,c], [d,e], [f,next_a] of a,b,c,d,e,f pairs.
                let first = first.to_le();
                let middle = middle.to_le();
                let last = last.to_le();
                let four = (first & 0xff00) | (middle & 0x00ff);
                valid & (four.wrapping_sub(0xf090) <= 0x03ff)
                    & (first & 0x00c0 == 0x0080)
                    & (middle & 0xc0c0 == 0x8080)
                    & (last & 0x00c0 == 0x0080)
                    & ((index + 1 == pairs.len()) | (last.wrapping_sub(0xc200) <= 0x1dff))
            },
        );
        if !valid {
            break;
        }
        consumed += MIXED_PAIR_BATCH_BYTES;
        // The last word includes this byte, but it belongs to the next
        // pair and may instead begin an ASCII suffix.
        if !(0xc2..=0xdf).contains(&text[consumed]) {
            break;
        }
    }
    &text[consumed..]
}

#[cfg(target_arch = "aarch64")]
#[inline]
fn after_repeated_pair<const FOUR_FIRST: bool>(mut text: &[u8]) -> &[u8] {
    let Some(sample @ &[a, b, c, d, e, f]) = text.first_chunk::<6>() else {
        return text;
    };
    if !valid_mixed_pair::<FOUR_FIRST>(sample) {
        return text;
    }
    let pair = u128::from(u64::from_le_bytes([a, b, c, d, e, f, 0, 0]));
    // Six bytes against 16-byte words gives starting phases 0, 4 and 2.
    let first = pair | (pair << 48) | (pair << 96);
    let second = (pair >> 32) | (pair << 16) | (pair << 64) | (pair << 112);
    let third = (pair >> 16) | (pair << 32) | (pair << 80);
    while let Some((group, rest)) = text.split_first_chunk::<48>() {
        let words = group.as_chunks::<16>().0;
        if (u128::from_le_bytes(words[0]) ^ first) | (u128::from_le_bytes(words[1]) ^ second)
            | (u128::from_le_bytes(words[2]) ^ third) != 0
        {
            return text;
        }
        text = rest;
    }
    while let Some((pair, rest)) = text.split_first_chunk::<6>() {
        if pair != sample {
            break;
        }
        text = rest;
    }
    text
}

fn long_text_prefix(text: &[u8]) -> Option<&str> {
    let end = text.iter().rposition(|b| !b.is_ascii())? + 1;
    if end > 256 {
        return None;
    }
    simdutf8::basic::from_utf8(&text[..end]).ok()
}

impl PackedStringRun {
    fn find(value: &[u8], entries: &[[u8; VALUE_ENTRY_SIZE]]) -> Option<Self> {
        let start = value_entry_offset(entries.first()?);
        let first = *value.get(start)?;
        if first >= 0x80 {
            return Self::find_wide(value, entries, start);
        }
        let mut end = start;
        let mut count = 0;
        let mut fixed_length = Some(first);
        for entry in entries {
            if entry[0] != JSON_TYPE_CODE_STRING || value_entry_offset(entry) != end {
                break;
            }
            let Some(&length @ 0..=127) = value.get(end) else {
                break;
            };
            let next = end.checked_add(usize::from(length) + 1)?;
            if next > value.len() {
                break;
            }
            if fixed_length != Some(length) {
                fixed_length = None;
            }
            end = next;
            count += 1;
        }
        if count < 4 {
            return None;
        }
        let lengths = match fixed_length {
            Some(length) => PackedStringLengths::Fixed(length),
            None => PackedStringLengths::Varying(
                entries[..count].iter().map(|entry| value[value_entry_offset(entry)]).collect(),
            ),
        };
        Some(Self { start, end, lengths })
    }

    #[cold]
    fn find_wide(value: &[u8], entries: &[[u8; VALUE_ENTRY_SIZE]], start: usize) -> Option<Self> {
        let first = WideStringLength::read(value.get(start..)?)?;
        let mut end = start;
        let mut count = 0;
        let mut varying = Vec::new();
        let (mut longest_start, mut longest) = (start, first);
        for entry in entries {
            if entry[0] != JSON_TYPE_CODE_STRING || value_entry_offset(entry) != end {
                break;
            }
            let Some(length) = WideStringLength::read(&value[end..]) else {
                break;
            };
            let next = end.checked_add(length.width())?;
            if next > value.len() {
                break;
            }
            if length != first && varying.is_empty() {
                varying.resize(count, WideStringCell { length: first, prefix_len: 0 });
            }
            if !varying.is_empty() {
                varying.push(WideStringCell { length, prefix_len: 0 });
            }
            if length.length > longest.length {
                (longest_start, longest) = (end, length);
            }
            count += 1;
            end = next;
        }
        if count < 4 {
            return None;
        }
        let prefix = long_text_prefix(&value[longest_start + 2..longest_start + longest.width()]);
        let (lengths, shared_prefix) = if varying.is_empty() {
            let second = &value[start + first.width() + 2..start + 2 * first.width()];
            (
                WideStringLengths::Fixed(first),
                prefix.filter(|p| second.starts_with(p.as_bytes())),
            )
        } else {
            let shared = prefix.filter(|prefix| {
                let mut offset = start;
                let mut matches = 0;
                for field in &mut varying {
                    let next = offset + field.length.width();
                    let text = &value[offset + 2..next];
                    // `str::get` checks the code-point boundary for shorter fields.
                    let candidate = text.iter().rposition(|b| !b.is_ascii())
                        .and_then(|last| prefix.get(..=last));
                    if let Some(candidate) = candidate.filter(|p| text.starts_with(p.as_bytes())) {
                        field.prefix_len = candidate.len() as u16;
                        matches += 1;
                    }
                    offset = next;
                }
                matches >= 2
            });
            (WideStringLengths::Varying(varying.into_boxed_slice()), shared)
        };
        let adaptive_prefix = match &lengths {
            WideStringLengths::Fixed(length) if shared_prefix.is_none() => {
                let width = length.width();
                (!value[start + 2..start + width].is_ascii()
                    && !value[start + width + 2..start + 2 * width].is_ascii())
                    .then_some(WidePrefixProbe {
                        length: *length,
                        prefix: None,
                        rows_until_probe: 0,
                        interval: 1,
                    })
            }
            _ => None,
        };
        let text = match shared_prefix {
            Some(prefix) => WideStringText::SharedPrefix(prefix.as_bytes().into()),
            None => WideStringText::General,
        };
        #[cfg(target_arch = "aarch64")]
        let text = if matches!(text, WideStringText::General) {
            let second = match &lengths {
                WideStringLengths::Fixed(length) => *length,
                WideStringLengths::Varying(fields) => fields[1].length,
            };
            let second_start = start + first.width();
            let samples = [
                &value[start + 2..second_start],
                &value[second_start + 2..second_start + second.width()],
            ];
            // Select the faster prefix scan when it covers most of both samples.
            if samples
                .iter()
                .all(|sample| after_three_byte_prefix(sample).len() <= sample.len() / 2)
            {
                WideStringText::ThreeBytePrefix
            } else {
                text
            }
        } else {
            text
        };
        Some(Self {
            start,
            end,
            lengths: PackedStringLengths::Wide(Box::new(PackedWideStringLengths {
                lengths,
                text,
                adaptive_prefix,
            })),
        })
    }

    #[inline]
    fn count(&self) -> usize {
        match &self.lengths {
            PackedStringLengths::Fixed(length) => (self.end - self.start) / (usize::from(*length) + 1),
            PackedStringLengths::Varying(lengths) => lengths.len(),
            PackedStringLengths::Wide(wide) => match &wide.lengths {
                WideStringLengths::Fixed(length) => (self.end - self.start) / length.width(),
                WideStringLengths::Varying(lengths) => lengths.len(),
            },
        }
    }

    fn validate(&mut self, value: &[u8]) -> Result<bool, BinaryJSONError> {
        let encoded = &value[self.start..self.end];
        match &mut self.lengths {
            PackedStringLengths::Fixed(length) => {
                if !encoded.chunks_exact(usize::from(*length) + 1).all(|cell| cell[0] == *length) {
                    return Ok(false);
                }
            }
            PackedStringLengths::Varying(lengths) => {
                let mut offset = 0;
                for &length in lengths.iter() {
                    if encoded.get(offset) != Some(&length) {
                        return Ok(false);
                    }
                    offset += usize::from(length) + 1;
                }
            }
            PackedStringLengths::Wide(width) => return width.validate(encoded),
        }
        // The single-byte lengths are ASCII. They separate the strings, so
        // an incomplete UTF-8 sequence cannot cross a field boundary.
        simdutf8::basic::from_utf8(encoded).map_err(|_| BinaryJSONError::InvalidBinary)?;
        Ok(true)
    }
}

impl CheckedJSONKeys {
    /// Saves a bounded prefix for an object with valid, sorted keys.
    pub fn new(cell: &[u8]) -> Option<Self> {
        let [JSON_TYPE_CODE_OBJECT, value @ ..] = cell else {
            return None;
        };
        let object = BinaryJSONRef { type_code: JSON_TYPE_CODE_OBJECT, value }.container().ok()?;
        Self::from_object(cell, object)
    }

    fn from_object(cell: &[u8], object: JSONContainer<'_>) -> Option<Self> {
        if object.count < 16 {
            return None;
        }
        let value = object.value;
        let end = object.checked_key_prefix(8191)?;
        let keys = value[HEADER_SIZE..object.value_entries].as_chunks::<KEY_ENTRY_SIZE>().0;
        let value_checks = match CheckedChildRun::find(object) {
            Some(run) => PayloadChecks::Run(run),
            None => object.payload_checks().ok()?.map_or(PayloadChecks::General, PayloadChecks::Fields),
        };
        Some(Self {
            packed_keys: PackedObjectKeys::new(
                keys,
                1 + object.value_entries + object.count * VALUE_ENTRY_SIZE,
                1 + end,
            ),
            value_checks,
            prefix: cell[..1 + end].to_vec(),
        })
    }

    /// Saves the layout only when it can check a run of wide strings together.
    #[doc(hidden)]
    pub fn for_wide_string_run(cell: &[u8]) -> Option<Self> {
        let [JSON_TYPE_CODE_OBJECT, value @ ..] = cell else {
            return None;
        };
        let object = BinaryJSONRef { type_code: JSON_TYPE_CODE_OBJECT, value }.container().ok()?;
        let end = object.value_entries + object.count * VALUE_ENTRY_SIZE;
        let entries = value[object.value_entries..end].as_chunks::<VALUE_ENTRY_SIZE>().0;
        if !entries.iter().any(|entry| {
            entry[0] == JSON_TYPE_CODE_STRING
                && value.get(value_entry_offset(entry)).is_some_and(|&length| length >= 128)
        }) {
            return None;
        }
        let keys = Self::from_object(cell, object)?;
        let PayloadChecks::Fields(checks) = &keys.value_checks else {
            return None;
        };
        checks.iter().any(|check| matches!(
            check,
            PayloadCheck::Strings(PackedStringRun { lengths: PackedStringLengths::Wide(_), .. })
        )).then_some(keys)
    }

    /// Returns the validated prefix length.
    pub fn prefix_len(&self) -> usize {
        self.prefix.len()
    }

    /// Checks the values whose payload can affect layout validity when the
    /// directories match and the keys are known to be valid and ordered.
    /// A child whose layout changes uses general validation on later rows.
    #[inline]
    pub fn check_layout(&mut self, cell: &[u8]) -> Option<EncodedJSONLayout> {
        let keys_valid = match &self.packed_keys {
            Some(keys) => keys.matches(&self.prefix, cell),
            None => cell.starts_with(&self.prefix),
        };
        if !keys_valid {
            return None;
        }
        let view = BinaryJSONRef { type_code: JSON_TYPE_CODE_OBJECT, value: &cell[1..] };
        Some(EncodedJSONLayout::from_validation(view.container().and_then(|container| {
            match &mut self.value_checks {
                PayloadChecks::Fields(indices) => container.validate_payloads(indices),
                PayloadChecks::Run(run) => run.validate(container),
                PayloadChecks::General => view.validate_container_layout::<true>(container, 0, true),
            }
        })))
    }
}

#[derive(Clone, Copy)]
struct ValidatedLayout {
    canonical: bool,
    // The root counts as one in addition to MAX_JSON_DEPTH nested levels.
    depth: u8,
}

impl PartialEq for BinaryJSON {
    fn eq(&self, other: &Self) -> bool {
        self.type_code == other.type_code && self.value == other.value
    }
}

impl Eq for BinaryJSON {}

impl Hash for BinaryJSON {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.type_code.hash(state);
        self.value.as_ref().hash(state);
    }
}

impl fmt::Debug for BinaryJSON {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("BinaryJSON")
            .field("type_code", &self.type_code)
            .field("value", &self.value.as_ref())
            .finish()
    }
}

/// Lossless logical tree used by binary JSON operations.
///
/// Container structure is decoded, while every scalar remains an exact
/// `type code + payload` value. This preserves opaque and temporal tags across
/// extract/modify/merge operations without inventing a serde sentinel format.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum JSONNode {
    Scalar(BinaryJSON),
    Array(Vec<JSONNode>),
    Object(Vec<(String, JSONNode)>),
}

/// Raw MySQL value embedded in binary JSON.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Opaque {
    /// MySQL field type code.
    pub type_code: u8,
    /// Uninterpreted field payload.
    pub bytes: Vec<u8>,
}

/// The complete set of source values accepted by `CreateBinaryJSON`.
///
/// A typed enum replaces Go's runtime `any` switch, making unsupported inputs
/// unrepresentable while retaining `json.Number`'s signed-integer, unsigned-
/// integer, then double conversion rule.
#[derive(Clone, Debug, PartialEq)]
pub enum BinaryJSONValue {
    /// JSON null.
    Null,
    /// JSON boolean.
    Bool(bool),
    /// Signed integer.
    Int64(i64),
    /// Unsigned integer.
    Uint64(u64),
    /// Double-precision number.
    Float64(f64),
    /// Source `json.Number` text.
    Number(String),
    /// UTF-8 JSON string.
    String(String),
    /// Already encoded binary JSON.
    Binary(BinaryJSON),
    /// Ordered JSON array.
    Array(Vec<BinaryJSONValue>),
    /// JSON object; keys encode in bytewise order.
    Object(BTreeMap<String, BinaryJSONValue>),
    /// MySQL opaque value.
    Opaque(Opaque),
    /// MySQL date, datetime, or timestamp.
    Time(Time),
    /// MySQL TIME duration.
    Duration(MySqlDuration),
}

/// Invalid JSON text or binary representation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BinaryJSONError {
    /// The JSON document contains no value.
    EmptyDocument,
    /// A complete root value is followed by another token.
    TrailingValues,
    /// Text is not valid JSON.
    InvalidText,
    /// Binary bytes violate the source layout.
    InvalidBinary,
    /// Document nesting exceeds TiDB's limit.
    TooDeep,
    /// An object key cannot fit the source uint16 key length.
    KeyTooLong,
    /// One UTF-16 high or low surrogate appeared without its pair.
    LoneSurrogate,
    /// A modifying operation received a wildcard, range, recursive, or root path.
    InvalidPath,
    /// Go `unquoteJSONString`'s plain error (a trailing backslash or a
    /// malformed `\u` escape), carrying Go's message.
    Unquote(String),
}

impl fmt::Display for BinaryJSONError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyDocument => formatter.write_str("The document is empty"),
            Self::TrailingValues => {
                formatter.write_str("The document root must not be followed by other values.")
            }
            Self::InvalidText => formatter.write_str("invalid JSON text"),
            Self::InvalidBinary => formatter.write_str("invalid binary JSON"),
            Self::TooDeep => formatter.write_str("JSON document is too deep"),
            Self::KeyTooLong => formatter.write_str(
                "[types:8129]TiDB does not yet support JSON objects with the key length >= 65536",
            ),
            Self::LoneSurrogate => formatter.write_str("invalid lone UTF-16 surrogate"),
            Self::InvalidPath => formatter.write_str("invalid JSON path for modification"),
            Self::Unquote(message) => formatter.write_str(message),
        }
    }
}

impl std::error::Error for BinaryJSONError {}

impl BinaryJSON {
    /// Reconstructs one value from TiDB's persisted `type code + value`
    /// representation.
    ///
    /// Rowcodec already owns the exact value boundary through its offset
    /// table, so this constructor deliberately preserves the bytes without
    /// reparsing or normalizing the document.
    pub fn from_encoded_parts(type_code: u8, value: impl Into<Vec<u8>>) -> Self {
        Self::from_shared_parts(type_code, Bytes::from(value.into()))
    }

    /// Reconstructs an encoded value using immutable shared payload bytes.
    pub fn from_shared_parts(type_code: u8, value: Bytes) -> Self {
        Self {
            type_code,
            value,
            layout: OnceLock::new(),
        }
    }

    /// Retains the encoded payload as a slice of the immutable backing vector.
    pub fn from_shared_subslice(type_code: u8, owner: &Arc<Vec<u8>>, value: &[u8]) -> Self {
        let value = tidb_goruntime::GoString::from_shared_subslice(owner, value);
        Self::from_shared_parts(type_code, Bytes::from_owner(value))
    }

    /// Reconstructs the exact internal type-code/payload pair stored by
    /// `Datum.UnmarshalJSON`.
    pub(crate) fn from_binary_parts(type_code: u8, value: Vec<u8>) -> Self {
        Self::from_encoded_parts(type_code, value)
    }

    /// Go's zero `types.BinaryJSON{}`: no type code and no payload. It is not
    /// a JSON document; Go renders it as the empty string, as
    /// [`fmt::Display`] does here.
    #[must_use]
    pub const fn zero() -> Self {
        Self {
            type_code: 0,
            value: Bytes::new(),
            layout: OnceLock::new(),
        }
    }

    /// Constructs from an exact source type code and value payload.
    pub fn from_raw(type_code: u8, value: Vec<u8>) -> Result<Self, BinaryJSONError> {
        Self::from_encoded_parts(type_code, value).validate_layout()
    }

    /// Builds a source-layout opaque JSON value.
    pub fn from_opaque(value: Opaque) -> Self {
        let mut bytes = Vec::with_capacity(2 + value.bytes.len());
        bytes.push(value.type_code);
        encode_uvarint(value.bytes.len(), &mut bytes);
        bytes.extend_from_slice(&value.bytes);
        Self {
            type_code: JSON_TYPE_CODE_OPAQUE,
            value: Bytes::from(bytes),
            layout: OnceLock::new(),
        }
    }

    /// Embeds a MySQL date, datetime, or timestamp.
    pub fn from_time(value: Time) -> Self {
        let type_code = match value.kind() {
            TimeType::Date => JSON_TYPE_CODE_DATE,
            TimeType::DateTime => JSON_TYPE_CODE_DATETIME,
            TimeType::Timestamp => JSON_TYPE_CODE_TIMESTAMP,
        };
        Self {
            type_code,
            value: Bytes::from(value.core_time().raw().to_le_bytes().to_vec()),
            layout: OnceLock::new(),
        }
    }

    /// Decodes an embedded MySQL date, datetime, or timestamp.
    pub fn as_time(&self, fsp: i64) -> Result<Time, BinaryJSONError> {
        let kind = match self.type_code {
            JSON_TYPE_CODE_DATE => TimeType::Date,
            JSON_TYPE_CODE_DATETIME => TimeType::DateTime,
            JSON_TYPE_CODE_TIMESTAMP => TimeType::Timestamp,
            _ => return Err(BinaryJSONError::InvalidBinary),
        };
        let raw = u64::from_le_bytes(
            self.value
                .as_ref()
                .try_into()
                .map_err(|_| BinaryJSONError::InvalidBinary)?,
        );
        Time::new(CoreTime::from_raw(raw), kind, fsp).map_err(|_| BinaryJSONError::InvalidBinary)
    }

    /// Embeds a MySQL TIME duration.
    pub fn from_duration(value: MySqlDuration) -> Self {
        let mut bytes = value.nanoseconds().to_le_bytes().to_vec();
        bytes.extend_from_slice(&(value.fsp() as u32).to_le_bytes());
        Self {
            type_code: JSON_TYPE_CODE_DURATION,
            value: Bytes::from(bytes),
            layout: OnceLock::new(),
        }
    }

    /// Decodes an embedded MySQL TIME duration.
    pub fn as_duration(&self) -> Result<MySqlDuration, BinaryJSONError> {
        if self.type_code != JSON_TYPE_CODE_DURATION || self.value.len() != 12 {
            return Err(BinaryJSONError::InvalidBinary);
        }
        let nanoseconds = i64::from_le_bytes(self.value[..8].try_into().unwrap());
        let fsp = u32::from_le_bytes(self.value[8..].try_into().unwrap());
        Ok(MySqlDuration::from_raw_parts(nanoseconds, i64::from(fsp)))
    }

    /// Parses JSON text and builds TiDB's binary representation.
    pub fn parse(text: &str) -> Result<Self, BinaryJSONError> {
        if text.trim().is_empty() {
            return Err(BinaryJSONError::EmptyDocument);
        }
        let value: Value = match serde_json::from_str(text) {
            Ok(value) => value,
            Err(error) => {
                if error.to_string().contains("trailing characters") {
                    return Err(BinaryJSONError::TrailingValues);
                }
                // Go decodes any document `json.Valid` accepts (nesting up to
                // 10000) and only then refuses one deeper than 100;
                // `serde_json` stops at 128 levels instead.
                if error.to_string().contains("recursion limit exceeded") {
                    return Err(if crate::json_scanner::valid(text.as_bytes()) {
                        BinaryJSONError::TooDeep
                    } else {
                        BinaryJSONError::InvalidText
                    });
                }
                // Go `json.Valid` accepts lone `\uXXXX` surrogate escapes and
                // `encoding/json` decodes them to U+FFFD; serde_json refuses
                // them. Rewrite those escapes the way Go's decoder would and
                // retry once — the sanitizer copies everything else
                // verbatim, so a text without surrogate escapes fails again
                // identically.
                let sanitized = replace_lone_surrogate_escapes(text);
                serde_json::from_str(&sanitized).map_err(|_| BinaryJSONError::InvalidText)?
            }
        };
        Self::from_value(&value)
    }

    /// Builds a BinaryJSON value from a validated JSON tree.
    pub fn from_value(value: &Value) -> Result<Self, BinaryJSONError> {
        let mut depth = 1;
        let mut encoded = encode_value(value, 0, &mut depth)?;
        encoded.layout = OnceLock::from(BinaryLayout::Validated(ValidatedLayout {
            canonical: true,
            depth,
        }));
        Ok(encoded)
    }

    /// Builds a BinaryJSON value from every source-supported input type.
    pub fn from_typed_value(value: &BinaryJSONValue) -> Result<Self, BinaryJSONError> {
        encode_typed_value(value)
    }

    /// Calculates the source binary payload size for a typed input.
    pub fn calculate_typed_size(value: &BinaryJSONValue) -> Result<usize, BinaryJSONError> {
        typed_payload_size(value, 0, &mut Vec::new())
    }

    /// Returns the source type code.
    pub const fn type_code(&self) -> u8 {
        self.type_code
    }

    /// Returns the exact bytes after the type code.
    pub fn value(&self) -> &[u8] {
        &self.value
    }

    /// Returns `type code + value` for the TiDB codec boundary.
    pub fn encoded(&self) -> Vec<u8> {
        let mut encoded = Vec::with_capacity(1 + self.value.len());
        self.append_encoded_to(&mut encoded);
        encoded
    }

    /// Appends the type code and original payload to an existing buffer.
    pub fn append_encoded_to(&self, output: &mut Vec<u8>) {
        output.push(self.type_code);
        output.extend_from_slice(&self.value);
    }

    /// Appends JSON text from an encoded cell without copying its binary payload.
    pub fn append_encoded_text(encoded: &[u8], output: &mut Vec<u8>) -> fmt::Result {
        struct BytesWriter<'a>(&'a mut Vec<u8>);

        impl fmt::Write for BytesWriter<'_> {
            fn write_str(&mut self, text: &str) -> fmt::Result {
                self.0.extend_from_slice(text.as_bytes());
                Ok(())
            }
        }

        let (&type_code, value) = encoded.split_first().ok_or(fmt::Error)?;
        let view = BinaryJSONRef { type_code, value };
        let mut output = BytesWriter(output);
        if view.validate_layout_at::<true>(0, true).is_ok_and(|layout| layout.canonical) {
            format_json_root(view, &mut output)
        } else {
            fmt::Write::write_fmt(&mut output, format_args!("{}", view.copy()))
        }
    }

    /// Decodes into a JSON tree without losing signed/unsigned integer text.
    pub fn to_value(&self) -> Result<Value, BinaryJSONError> {
        decode_value(self.type_code, &self.value, 0)
    }

    pub(crate) fn to_node(&self) -> Result<JSONNode, BinaryJSONError> {
        decode_node(self.view(), 0)
    }

    pub(crate) fn from_node(node: &JSONNode) -> Result<Self, BinaryJSONError> {
        encode_node(node, 0)
    }

    pub(crate) fn view(&self) -> BinaryJSONRef<'_> {
        BinaryJSONRef {
            type_code: self.type_code,
            value: &self.value,
        }
    }

    pub(crate) fn canonical_view(&self) -> Option<BinaryJSONRef<'_>> {
        EncodedJSONLayout(self.layout())
            .is_canonical()
            .then(|| self.view())
    }

    fn layout(&self) -> BinaryLayout {
        *self.layout.get_or_init(|| self.view().checked_layout())
    }

    /// Returns a layout result already known for these bytes without validating them.
    #[doc(hidden)]
    pub fn known_encoded_layout(&self) -> Option<bool> {
        self.known_layout().map(EncodedJSONLayout::is_canonical)
    }

    /// Returns the existing layout check, including any validation failure.
    #[doc(hidden)]
    pub fn known_layout(&self) -> Option<EncodedJSONLayout> {
        self.layout.get().copied().map(EncodedJSONLayout)
    }

    /// Checks a complete packed cell with a leading JSON type code.
    #[doc(hidden)]
    #[inline]
    pub fn check_encoded_layout(cell: &[u8]) -> EncodedJSONLayout {
        if let [JSON_TYPE_CODE_STRING, length @ 0..=127, text @ ..] = cell {
            if text.len() != usize::from(*length) {
                return EncodedJSONLayout(BinaryLayout::Invalid);
            }
            if text.is_ascii() {
                return EncodedJSONLayout(BinaryLayout::Validated(ValidatedLayout {
                    canonical: true,
                    depth: 1,
                }));
            }
        }
        EncodedJSONLayout(match cell.split_first() {
            Some((&type_code, value)) => BinaryJSONRef { type_code, value }.checked_layout(),
            None => BinaryLayout::Invalid,
        })
    }

    /// Validates a flat scalar object with short ASCII keys. Integer-only
    /// objects also return the prefix needed to reuse the layout check.
    #[doc(hidden)]
    pub fn check_flat_object_layout(cell: &[u8]) -> Option<(EncodedJSONLayout, Option<usize>)> {
        let [JSON_TYPE_CODE_OBJECT, value @ ..] = cell else {
            return None;
        };
        let container = BinaryJSONRef { type_code: JSON_TYPE_CODE_OBJECT, value }.container().ok()?;
        if container.count == 0 {
            return None;
        }
        let value_end = container.value_entries + container.count * VALUE_ENTRY_SIZE;
        let entries = value[container.value_entries..value_end]
            .as_chunks::<VALUE_ENTRY_SIZE>().0;
        let keys = value[HEADER_SIZE..container.value_entries].as_chunks::<KEY_ENTRY_SIZE>().0;
        let first = keys.first()?;
        let first_len = u16::from_le_bytes([first[4], first[5]]);
        let layout = EncodedJSONLayout(BinaryLayout::Validated(ValidatedLayout {
            canonical: true,
            depth: 2,
        }));
        if container.count >= 16 {
            if let Some(end) = check_packed_integer_object(value, keys, entries, value_end, first_len) {
                return Some((layout, Some(1 + end)));
            }
        }
        if !(1..=16).contains(&first_len)
            || matches!(entries.last()?[0], JSON_TYPE_CODE_ARRAY | JSON_TYPE_CODE_OBJECT)
        {
            return None;
        }
        let prefix = if check_flat_values(value, entries)? && container.count >= 16 {
            let prefix = value_entry_offset(entries.first()?);
            if !(value_end..=value.len()).contains(&prefix) {
                return None;
            }
            Some(prefix)
        } else {
            None
        };
        let key_end = prefix.unwrap_or(value.len());
        check_short_ascii_keys(value, keys, key_end)?;
        Some((layout, prefix.map(|end| 1 + end)))
    }

    /// Reuses a layout result from a storage range holding these exact bytes.
    /// The result must belong to this value's type code and payload.
    #[doc(hidden)]
    pub fn with_known_layout(mut self, layout: Option<EncodedJSONLayout>) -> Self {
        if let Some(layout) = layout {
            self.layout = OnceLock::from(layout.0);
        }
        self
    }

    /// Returns the maximum document nesting depth, with every scalar at depth one.
    pub fn element_depth(&self) -> Result<usize, BinaryJSONError> {
        self.view().depth_for_layout(self.layout())
    }

    /// Go `BinaryJSON.GetElemDepth` for SQL `JSON_DEPTH`. Scalar types
    /// have depth one. Container traversal checks the framing it reads.
    #[inline]
    pub fn get_elem_depth(&self) -> Result<usize, BinaryJSONError> {
        match self.type_code {
            JSON_TYPE_CODE_ARRAY | JSON_TYPE_CODE_OBJECT if self.value() == EMPTY_JSON_CONTAINER => Ok(1),
            JSON_TYPE_CODE_ARRAY | JSON_TYPE_CODE_OBJECT => {
                self.view().depth_from_framing(self.layout.get().copied())
            }
            _ => Ok(1),
        }
    }

    /// Go `BinaryJSON.GetElemDepth` on a borrowed binary cell. An existing
    /// layout may be reused if it was checked for these exact bytes.
    #[doc(hidden)]
    #[inline]
    pub fn get_encoded_elem_depth(
        cell: &[u8],
        known_layout: impl FnOnce() -> Option<EncodedJSONLayout>,
    ) -> Result<usize, BinaryJSONError> {
        let (&type_code, value) = cell.split_first().ok_or(BinaryJSONError::InvalidBinary)?;
        match type_code {
            JSON_TYPE_CODE_ARRAY | JSON_TYPE_CODE_OBJECT if value == EMPTY_JSON_CONTAINER => Ok(1),
            JSON_TYPE_CODE_ARRAY | JSON_TYPE_CODE_OBJECT => {
                let layout = known_layout().map(|layout| layout.0);
                BinaryJSONRef { type_code, value }.depth_from_framing(layout)
            }
            _ => Ok(1),
        }
    }

    /// Whether the container has enough traversal work to amortize a cache lookup.
    #[doc(hidden)]
    pub fn should_cache_encoded_depth(cell: &[u8]) -> bool {
        let Some(&[type_code @ (JSON_TYPE_CODE_OBJECT | JSON_TYPE_CODE_ARRAY), a, b, c, d]) = cell.first_chunk::<5>() else {
            return false;
        };
        match u32::from_le_bytes([a, b, c, d]) {
            0 => false,
            1 => {
                let entry = 1 + HEADER_SIZE + usize::from(type_code == JSON_TYPE_CODE_OBJECT) * KEY_ENTRY_SIZE;
                matches!(cell.get(entry), Some(&JSON_TYPE_CODE_ARRAY | &JSON_TYPE_CODE_OBJECT))
            }
            _ => true,
        }
    }

    /// Reads the depth of one packed cell. A supplied layout must describe
    /// these exact bytes, including the type code.
    #[doc(hidden)]
    #[inline]
    pub fn encoded_element_depth(
        cell: &[u8],
        layout: impl FnOnce() -> Option<EncodedJSONLayout>,
    ) -> Result<usize, BinaryJSONError> {
        let (&type_code, value) = cell.split_first().ok_or(BinaryJSONError::InvalidBinary)?;
        match (type_code, value) {
            (JSON_TYPE_CODE_INT64 | JSON_TYPE_CODE_UINT64, [_, _, _, _, _, _, _, _])
            | (
                JSON_TYPE_CODE_LITERAL,
                [JSON_LITERAL_NULL | JSON_LITERAL_TRUE | JSON_LITERAL_FALSE],
            ) => return Ok(1),
            (JSON_TYPE_CODE_ARRAY | JSON_TYPE_CODE_OBJECT, value)
                if value == EMPTY_JSON_CONTAINER =>
            {
                return Ok(1)
            }
            _ => {}
        }
        let view = BinaryJSONRef { type_code, value };
        view.depth_for_layout(layout().map_or_else(|| view.checked_layout(), |layout| layout.0))
    }

    pub(crate) fn empty_array() -> Self {
        Self {
            type_code: JSON_TYPE_CODE_ARRAY,
            value: Bytes::from_static(EMPTY_JSON_CONTAINER),
            layout: OnceLock::from(BinaryLayout::Validated(ValidatedLayout {
                canonical: true,
                depth: 1,
            })),
        }
    }

    pub(crate) fn share_view(&self, view: BinaryJSONRef<'_>) -> Self {
        Self {
            type_code: view.type_code,
            value: self.value.slice_ref(view.value),
            layout: OnceLock::from(BinaryLayout::Canonical),
        }
    }

    pub(crate) fn share_extracted_view(
        &self,
        view: BinaryJSONRef<'_>,
        layout: Option<EncodedJSONLayout>,
    ) -> Self {
        Self::from_shared_parts(view.type_code, self.value.slice_ref(view.value))
            .with_known_layout(layout)
    }

    /// Wraps already validated canonical values, preserving the depth check.
    pub(crate) fn array_from_views(
        values: &[BinaryJSONRef<'_>],
    ) -> Result<Self, BinaryJSONError> {
        encode_view_array(values.iter().copied())?.finish_edit(false)?.validate_layout()
    }

    /// Finalizes a wire edit assembled from previously validated values.
    pub(crate) fn finish_edit(mut self, may_increase_depth: bool) -> Result<Self, BinaryJSONError> {
        if may_increase_depth && self.view().container_depth()? - 1 > MAX_JSON_DEPTH {
            return Err(BinaryJSONError::TooDeep);
        }
        self.layout = OnceLock::from(BinaryLayout::Canonical);
        Ok(self)
    }

    /// Validates the complete binary layout and records whether object keys are sorted.
    pub fn validate_layout(mut self) -> Result<Self, BinaryJSONError> {
        match self.layout() {
            BinaryLayout::Validated(_) => Ok(self),
            BinaryLayout::Invalid => Err(BinaryJSONError::InvalidBinary),
            BinaryLayout::TooDeep => Err(BinaryJSONError::TooDeep),
            BinaryLayout::Canonical => {
                // The writer established the layout. Supply the depth so
                // future checked reads can reuse the full result.
                let depth = self.view().container_depth()?;
                if depth > MAX_JSON_DEPTH + 1 {
                    return Err(BinaryJSONError::TooDeep);
                }
                self.layout = OnceLock::from(BinaryLayout::Validated(ValidatedLayout {
                    canonical: true,
                    depth: depth as u8,
                }));
                Ok(self)
            }
        }
    }

    /// Returns a signed integer payload.
    pub fn as_i64(&self) -> Option<i64> {
        if self.type_code != JSON_TYPE_CODE_INT64 {
            return None;
        }
        Some(i64::from_le_bytes(self.value.as_ref().try_into().ok()?))
    }

    /// Returns an unsigned integer payload.
    pub fn as_u64(&self) -> Option<u64> {
        if self.type_code != JSON_TYPE_CODE_UINT64 {
            return None;
        }
        Some(u64::from_le_bytes(self.value.as_ref().try_into().ok()?))
    }

    /// Returns a double payload.
    pub fn as_f64(&self) -> Option<f64> {
        if self.type_code != JSON_TYPE_CODE_FLOAT64 {
            return None;
        }
        Some(f64::from_bits(u64::from_le_bytes(
            self.value.as_ref().try_into().ok()?,
        )))
    }

    /// Returns whether this value compares equal to JSON numeric zero.
    pub fn is_zero(&self) -> bool {
        match self.type_code {
            JSON_TYPE_CODE_INT64 | JSON_TYPE_CODE_UINT64 => self.value.as_ref() == [0; 8],
            JSON_TYPE_CODE_FLOAT64 => self
                .as_f64()
                .is_some_and(|value| value.abs() < FLOAT_EPSILON),
            _ => false,
        }
    }

    /// Returns string bytes.
    #[inline]
    pub fn as_string(&self) -> Option<&[u8]> {
        self.view().string_bytes()
    }

    /// Compares two BinaryJSON cells whose first bytes are their type codes.
    /// An empty cell carries no JSON value and returns `None`.
    pub fn compare_encoded(left: &[u8], right: &[u8]) -> Option<Ordering> {
        Self::compare_encoded_with_validation(left, right, || None, || None)
    }

    /// Compares encoded cells using layout checks supplied by their storage.
    /// A supplied result must equal `encoded_is_canonical` for its exact cell.
    #[doc(hidden)]
    pub fn compare_encoded_with_validation(
        left: &[u8],
        right: &[u8],
        left_is_canonical: impl FnOnce() -> Option<bool>,
        right_is_canonical: impl FnOnce() -> Option<bool>,
    ) -> Option<Ordering> {
        let (&left_type, left_value) = left.split_first()?;
        let (&right_type, right_value) = right.split_first()?;
        let left = BinaryJSONRef {
            type_code: left_type,
            value: left_value,
        };
        let right = BinaryJSONRef {
            type_code: right_type,
            value: right_value,
        };
        Some(compare_json_views(left, right, left_is_canonical, right_is_canonical).unwrap_or_else(
            || {
                let left = Self::from_encoded_parts(left.type_code, left.value);
                let right = Self::from_encoded_parts(right.type_code, right.value);
                compare_binary_json_fallback(&left, &right)
            },
        ))
    }

    /// Compares a packed left cell with this value using its exact layout result.
    /// Returns `None` if the left cell has no type code.
    #[doc(hidden)]
    pub fn compare_encoded_left(
        &self,
        left: &[u8],
        left_is_canonical: impl FnOnce() -> Option<bool>,
    ) -> Option<Ordering> {
        let (&type_code, value) = left.split_first()?;
        let left = BinaryJSONRef { type_code, value };
        let ordering = compare_json_views(left, self.view(), left_is_canonical, || {
            Some(self.canonical_view().is_some())
        });
        Some(ordering.unwrap_or_else(|| {
            let left = Self::from_encoded_parts(left.type_code, left.value);
            compare_binary_json_fallback(&left, self)
        }))
    }

    /// Checks the complete layout, including strings and object key order.
    #[doc(hidden)]
    pub fn encoded_is_canonical(cell: &[u8]) -> bool {
        cell.split_first().is_some_and(|(&type_code, value)| {
            BinaryJSONRef { type_code, value }.is_canonical_layout()
        })
    }

    /// Decodes an opaque value without changing its bytes.
    pub fn opaque(&self) -> Result<Opaque, BinaryJSONError> {
        if self.type_code != JSON_TYPE_CODE_OPAQUE {
            return Err(BinaryJSONError::InvalidBinary);
        }
        let (type_code, bytes) = read_opaque(&self.value)?;
        Ok(Opaque {
            type_code,
            bytes: bytes.to_vec(),
        })
    }

    /// Returns MySQL's JSON_TYPE name.
    pub fn type_name(&self) -> Result<&'static str, BinaryJSONError> {
        self.view().type_name().map(|name| name.text)
    }

    /// Reads the JSON_TYPE name directly from a chunk cell.
    #[doc(hidden)]
    #[inline]
    pub fn encoded_type_name(cell: &[u8]) -> Result<(&'static [u8; 16], usize), BinaryJSONError> {
        let (&type_code, value) = cell.split_first().ok_or(BinaryJSONError::InvalidBinary)?;
        let name = BinaryJSONRef { type_code, value }.type_name()?;
        Ok((&name.bytes, name.text.len()))
    }

    /// Implements JSON_UNQUOTE for one binary JSON value.
    #[inline]
    pub fn unquote(&self) -> Result<String, BinaryJSONError> {
        match self.as_string() {
            Some(bytes) => {
                if matches!(bytes, [b'"', .., b'"']) {
                    let text = simdutf8::basic::from_utf8(bytes)
                        .map_err(|_| BinaryJSONError::InvalidBinary)?;
                    return unquote_string(text);
                }
                if bytes.len() <= JSON_TEXT_COPY_BLOCK {
                    return simdutf8::basic::from_utf8(bytes)
                        .map(str::to_owned)
                        .map_err(|_| BinaryJSONError::InvalidBinary);
                }
                copy_json_text(bytes)
            }
            None => Ok(self.to_string()),
        }
    }
}

fn check_packed_integer_object(
    value: &[u8],
    keys: &[[u8; KEY_ENTRY_SIZE]],
    entries: &[[u8; VALUE_ENTRY_SIZE]],
    key_start: usize,
    width: u16,
) -> Option<usize> {
    let kind = entries.first()?[0];
    if !(1..=8).contains(&width)
        || !matches!(kind, JSON_TYPE_CODE_INT64 | JSON_TYPE_CODE_UINT64)
    {
        return None;
    }
    let key_end = key_start.checked_add(keys.len().checked_mul(usize::from(width))?)?;
    if key_end.checked_add(entries.len().checked_mul(8)?)? != value.len() {
        return None;
    }
    // Match the complete directories, including each type or key width.
    // A matching offset always has its whole payload inside the object.
    let first_key = key_start as u64 | (u64::from(width) << 32);
    let first_value = u64::from(kind) | ((key_end as u64) << 8);
    if !check_packed_entries::<KEY_ENTRY_SIZE, 12>(keys.as_flattened(), first_key, u64::from(width))
        || !check_packed_entries::<VALUE_ENTRY_SIZE, 15>(entries.as_flattened(), first_value, 8 << 8)
    {
        return None;
    }
    let data = &value[key_start..key_end];
    let ordered = match width {
        1 => check_packed_key_order::<1>(data),
        2 => check_packed_key_order::<2>(data),
        3 => check_packed_key_order::<3>(data),
        4 => check_packed_key_order::<4>(data),
        5 => check_packed_key_order::<5>(data),
        6 => check_packed_key_order::<6>(data),
        7 => check_packed_key_order::<7>(data),
        8 => check_packed_key_order::<8>(data),
        _ => false,
    };
    (ordered && data.is_ascii()).then_some(key_end)
}

fn check_packed_entries<const ENTRY: usize, const BLOCK: usize>(
    bytes: &[u8],
    first: u64,
    step: u64,
) -> bool {
    // The offset fields advance by the same amount in every block. Pack
    // two key entries or three value entries into each comparison.
    let entries = BLOCK / ENTRY;
    let mut expected = 0;
    let mut increment = 0;
    for index in 0..entries {
        let shift = index * ENTRY * 8;
        expected |= u128::from(first + step * index as u64) << shift;
        increment |= u128::from(step * entries as u64) << shift;
    }
    let (blocks, tail) = bytes.as_chunks::<BLOCK>();
    for block in blocks {
        if *block != *expected.to_le_bytes().first_chunk::<BLOCK>().expect("block fits in a word") {
            return false;
        }
        expected += increment;
    }
    tail == &expected.to_le_bytes()[..tail.len()]
}

fn check_packed_key_order<const WIDTH: usize>(data: &[u8]) -> bool {
    let (keys, _) = data.as_chunks::<WIDTH>();
    let word = |key: &[u8; WIDTH]| {
        let mut bytes = [0; 8];
        bytes[..WIDTH].copy_from_slice(key);
        u64::from_be_bytes(bytes)
    };
    let Some((first, keys)) = keys.split_first() else {
        return true;
    };
    let mut previous = word(first);
    keys.iter().all(|key| {
        let current = word(key);
        let ordered = previous < current;
        previous = current;
        ordered
    })
}

fn check_short_ascii_keys(
    value: &[u8],
    entries: &[[u8; KEY_ENTRY_SIZE]],
    key_end: usize,
) -> Option<()> {
    let first = entries.first()?;
    match u16::from_le_bytes([first[4], first[5]]) {
        1..=8 => check_flat_keys::<8>(value, entries, key_end)
            .or_else(|| check_flat_keys::<16>(value, entries, key_end)),
        9..=16 => check_flat_keys::<16>(value, entries, key_end),
        _ => None,
    }
}

#[inline]
fn is_valid_json_text(text: &[u8]) -> bool {
    (text.len() <= 16 && text.is_ascii()) || simdutf8::basic::from_utf8(text).is_ok()
}

#[inline]
fn is_valid_child_json_text(text: &[u8]) -> bool {
    if text.len() <= 16 {
        text.is_ascii() || is_valid_short_json_unicode(text)
    } else {
        simdutf8::basic::from_utf8(text).is_ok()
    }
}

#[inline(never)]
fn is_valid_short_json_unicode(text: &[u8]) -> bool {
    if let Some((&[lead, first, second], tail)) = text.split_first_chunk::<3>() {
        if three_byte_mismatch(lead, first, second) == 0 {
            return tail.is_ascii() || is_valid_other_short_json_unicode(tail);
        }
    }
    is_valid_other_short_json_unicode(text)
}

#[inline(never)]
fn is_valid_other_short_json_unicode(text: &[u8]) -> bool {
    let continuation = |byte: u8| byte & 0xc0 == 0x80;
    // Validate a complete leading character before checking the remaining
    // text. E0, ED, F0 and F4 have additional bounds on the next byte.
    let tail = match text {
        [lead @ 0xe0..=0xef, a, b, tail @ ..]
            if continuation(*a) && continuation(*b)
                && (*lead != 0xe0 || *a >= 0xa0)
                && (*lead != 0xed || *a < 0xa0) => tail,
        [0xc2..=0xdf, a, tail @ ..] if continuation(*a) => tail,
        [lead @ 0xf0..=0xf4, a, b, c, tail @ ..]
            if continuation(*a) && continuation(*b) && continuation(*c)
                && (*lead != 0xf0 || *a >= 0x90)
                && (*lead != 0xf4 || *a < 0x90) => tail,
        _ => return std::str::from_utf8(text).is_ok(),
    };
    tail.is_ascii() || std::str::from_utf8(tail).is_ok()
}

fn check_flat_keys<const MAX_WIDTH: usize>(
    value: &[u8],
    entries: &[[u8; KEY_ENTRY_SIZE]],
    key_end: usize,
) -> Option<()> {
    let mut previous = (0, 0, 0);
    for &[a, b, c, d, e, f] in entries {
        let offset = u32::from_le_bytes([a, b, c, d]) as usize;
        let len = usize::from(u16::from_le_bytes([e, f]));
        if !(1..=MAX_WIDTH).contains(&len) || offset > key_end || len > key_end - offset {
            return None;
        }
        let (word, remaining) = value.get(offset..)?.split_first_chunk::<8>()?;
        // Big-endian words preserve byte order. Length breaks ties for keys
        // that differ only by trailing zero bytes.
        let (head, tail) = if MAX_WIDTH == 8 || len <= 8 {
            (u64::from_be_bytes(*word) & (u64::MAX << (8 * (8 - len))), 0)
        } else {
            let tail = u64::from_be_bytes(*remaining.first_chunk::<8>()?);
            (u64::from_be_bytes(*word), tail & (u64::MAX << (8 * (16 - len))))
        };
        let key = (head, tail, len);
        if (head | tail) & 0x8080_8080_8080_8080 != 0 || key <= previous {
            return None;
        }
        previous = key;
    }
    Some(())
}

fn check_flat_values(value: &[u8], entries: &[[u8; VALUE_ENTRY_SIZE]]) -> Option<bool> {
    let max_offset = value.len() - 8;
    let mut integer_only = true;
    for entry in entries {
        integer_only &= matches!(entry[0], JSON_TYPE_CODE_INT64 | JSON_TYPE_CODE_UINT64);
        match entry[0] {
            JSON_TYPE_CODE_INT64 | JSON_TYPE_CODE_UINT64
            | JSON_TYPE_CODE_DATE | JSON_TYPE_CODE_DATETIME | JSON_TYPE_CODE_TIMESTAMP => {
                if value_entry_offset(entry) > max_offset {
                    return None;
                }
            }
            JSON_TYPE_CODE_LITERAL => {
                if !matches!(entry[1], JSON_LITERAL_NULL | JSON_LITERAL_FALSE | JSON_LITERAL_TRUE) {
                    return None;
                }
            }
            JSON_TYPE_CODE_STRING => {
                validate_string_payload::<true>(value.get(value_entry_offset(entry)..)?).ok()?;
            }
            JSON_TYPE_CODE_FLOAT64 => {
                let word = value.get(value_entry_offset(entry)..)?.first_chunk::<8>()?;
                if !f64::from_le_bytes(*word).is_finite() {
                    return None;
                }
            }
            _ => return None,
        }
    }
    Some(integer_only)
}

#[inline]
fn validate_string_payload<const CHECK_WIDE_ASCII: bool>(payload: &[u8]) -> Result<(), BinaryJSONError> {
    if let [length @ 0..=127, text @ ..] = payload {
        let text = text.get(..usize::from(*length)).ok_or(BinaryJSONError::InvalidBinary)?;
        let valid = if CHECK_WIDE_ASCII {
            is_valid_json_text(text)
        } else {
            is_valid_child_json_text(text)
        };
        if !valid {
            return Err(BinaryJSONError::InvalidBinary);
        }
        return Ok(());
    }
    if CHECK_WIDE_ASCII {
        if let Some(length) = WideStringLength::read(payload) {
            let text = payload.get(2..length.width()).ok_or(BinaryJSONError::InvalidBinary)?;
            if !is_valid_wide_json_text(text) {
                return Err(BinaryJSONError::InvalidBinary);
            }
            return Ok(());
        }
    }
    BinaryJSONRef { type_code: JSON_TYPE_CODE_STRING, value: payload }.validate_scalar_payload::<true>(false)
}

fn read_opaque(value: &[u8]) -> Result<(u8, &[u8]), BinaryJSONError> {
    let (&type_code, payload) = value.split_first().ok_or(BinaryJSONError::InvalidBinary)?;
    let (length, prefix) = decode_uvarint(payload)?;
    if prefix.checked_add(length) != Some(payload.len()) {
        return Err(BinaryJSONError::InvalidBinary);
    }
    Ok((type_code, &payload[prefix..]))
}

struct JSONTypeName {
    text: &'static str,
    bytes: [u8; 16],
}

impl JSONTypeName {
    const fn new(text: &'static str) -> Self {
        let mut bytes = [0; 16];
        let mut i = 0;
        while i < text.len() {
            bytes[i] = text.as_bytes()[i];
            i += 1;
        }
        Self { text, bytes }
    }
}

impl BinaryJSONRef<'_> {
    #[inline]
    fn type_name(self) -> Result<&'static JSONTypeName, BinaryJSONError> {
        Ok(match self.type_code {
            JSON_TYPE_CODE_OBJECT => &const { JSONTypeName::new("OBJECT") },
            JSON_TYPE_CODE_ARRAY => &const { JSONTypeName::new("ARRAY") },
            JSON_TYPE_CODE_LITERAL if self.value == [JSON_LITERAL_NULL] => {
                &const { JSONTypeName::new("NULL") }
            }
            JSON_TYPE_CODE_LITERAL => &const { JSONTypeName::new("BOOLEAN") },
            JSON_TYPE_CODE_INT64 => &const { JSONTypeName::new("INTEGER") },
            JSON_TYPE_CODE_UINT64 => &const { JSONTypeName::new("UNSIGNED INTEGER") },
            JSON_TYPE_CODE_FLOAT64 => &const { JSONTypeName::new("DOUBLE") },
            JSON_TYPE_CODE_STRING => &const { JSONTypeName::new("STRING") },
            JSON_TYPE_CODE_OPAQUE => match read_opaque(self.value)?.0 {
                0x0f | 0xf9..=0xfe => &const { JSONTypeName::new("BLOB") },
                0x10 => &const { JSONTypeName::new("BIT") },
                _ => &const { JSONTypeName::new("OPAQUE") },
            },
            JSON_TYPE_CODE_DATE => &const { JSONTypeName::new("DATE") },
            JSON_TYPE_CODE_DATETIME | JSON_TYPE_CODE_TIMESTAMP => &const { JSONTypeName::new("DATETIME") },
            JSON_TYPE_CODE_DURATION => &const { JSONTypeName::new("TIME") },
            _ => return Err(BinaryJSONError::InvalidBinary),
        })
    }
}

const JSON_TEXT_COPY_BLOCK: usize = 16 * 1024;

fn copy_json_text(mut bytes: &[u8]) -> Result<String, BinaryJSONError> {
    let mut output = String::with_capacity(bytes.len());
    while bytes.len() > JSON_TEXT_COPY_BLOCK {
        // Move at most three bytes past the proposed split to avoid
        // separating a UTF-8 code point. Validation checks each part.
        let mut end = JSON_TEXT_COPY_BLOCK;
        while end < bytes.len().min(JSON_TEXT_COPY_BLOCK + 3) && bytes[end] & 0xc0 == 0x80 {
            end += 1;
        }
        let (head, tail) = bytes.split_at(end);
        output.push_str(simdutf8::basic::from_utf8(head).map_err(|_| BinaryJSONError::InvalidBinary)?);
        bytes = tail;
    }
    output.push_str(simdutf8::basic::from_utf8(bytes).map_err(|_| BinaryJSONError::InvalidBinary)?);
    Ok(output)
}

/// A borrowed value within a TiDB binary JSON document.
#[derive(Clone, Copy)]
pub(crate) struct BinaryJSONRef<'a> {
    pub(crate) type_code: u8,
    pub(crate) value: &'a [u8],
}

/// A validated binary JSON cell borrowed from packed storage.
#[derive(Clone, Copy)]
pub struct ValidatedBinaryJSONRef<'a> {
    cell: &'a [u8],
    layout: ValidatedLayout,
}

impl<'a> ValidatedBinaryJSONRef<'a> {
    /// Validates a complete cell with a leading type code. Accepts unsorted object keys.
    pub fn from_encoded(cell: &'a [u8]) -> Result<Self, BinaryJSONError> {
        let (&type_code, value) = cell.split_first().ok_or(BinaryJSONError::InvalidBinary)?;
        let value = BinaryJSONRef { type_code, value };
        let layout = value.validate_layout_at::<true>(0, true)?;
        Ok(Self { cell, layout })
    }

    fn view(self) -> BinaryJSONRef<'a> {
        BinaryJSONRef {
            type_code: self.cell[0],
            value: &self.cell[1..],
        }
    }

    /// Borrows the equality hash when the wire bytes already have that representation.
    pub fn hash_value_as_encoded(self) -> Option<&'a [u8]> {
        match self.cell[0] {
            JSON_TYPE_CODE_OBJECT
            | JSON_TYPE_CODE_ARRAY
            | JSON_TYPE_CODE_INT64
            | JSON_TYPE_CODE_UINT64 => None,
            _ => Some(self.cell),
        }
    }

    pub(crate) fn canonical_view(self) -> Option<BinaryJSONRef<'a>> {
        self.layout.canonical.then(|| self.view())
    }

    pub(crate) fn to_owned(self) -> BinaryJSON {
        let value = self.view();
        BinaryJSON {
            type_code: value.type_code,
            value: Bytes::copy_from_slice(value.value),
            layout: OnceLock::from(BinaryLayout::Validated(self.layout)),
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) struct JSONContainer<'a> {
    value: &'a [u8],
    pub(crate) count: usize,
    value_entries: usize,
}

/// The checked key entries and output size for a packed JSON array of keys.
#[doc(hidden)]
pub struct JSONKeyArray<'a> {
    object: JSONContainer<'a>,
    size: usize,
}

impl JSONKeyArray<'_> {
    /// Length of the packed array, including its type code.
    #[inline]
    pub fn encoded_len(&self) -> usize {
        1 + self.size
    }

    /// Checked layout of the packed output array.
    #[inline]
    pub fn layout(&self) -> EncodedJSONLayout {
        EncodedJSONLayout::canonical()
    }

    /// Writes the packed array into a cell of `encoded_len()` bytes.
    #[inline]
    pub fn write_cell(self, cell: &mut [u8]) {
        cell[0] = JSON_TYPE_CODE_ARRAY;
        if self.object.count == 0 {
            cell[1..].copy_from_slice(EMPTY_JSON_CONTAINER);
        } else {
            self.write_value(&mut cell[1..]);
        }
    }

    pub(crate) fn into_json(self) -> BinaryJSON {
        if self.object.count == 0 {
            return BinaryJSON::empty_array();
        }
        let mut output = vec![0; self.size];
        self.write_value(&mut output);
        BinaryJSON {
            type_code: JSON_TYPE_CODE_ARRAY,
            value: Bytes::from(output),
            layout: OnceLock::from(BinaryLayout::Canonical),
        }
    }

    fn write_value(self, output: &mut [u8]) {
        let count = self.object.count;
        write_header(output, count).expect("checked JSON key array size");
        let data_start = HEADER_SIZE + count * VALUE_ENTRY_SIZE;
        let (entries, data) = output.split_at_mut(data_start);
        let keys = self.object.value[HEADER_SIZE..self.object.value_entries]
            .as_chunks::<KEY_ENTRY_SIZE>().0;
        let mut offset = 0;
        for (entry, key_entry) in entries[HEADER_SIZE..].as_chunks_mut::<VALUE_ENTRY_SIZE>().0.iter_mut().zip(keys) {
            let key = self.object.key_from_entry(key_entry).expect("checked JSON key entry");
            entry[0] = JSON_TYPE_CODE_STRING;
            entry[1..].copy_from_slice(&((data_start + offset) as u32).to_le_bytes());
            let mut len = key.len();
            while len >= 0x80 {
                data[offset] = (len as u8) | 0x80;
                offset += 1;
                len >>= 7;
            }
            data[offset] = len as u8;
            offset += 1;
            data[offset..offset + key.len()].copy_from_slice(key);
            offset += key.len();
        }
    }
}

impl<'a> BinaryJSONRef<'a> {
    #[inline]
    fn depth_from_framing(self, known_layout: Option<BinaryLayout>) -> Result<usize, BinaryJSONError> {
        match known_layout {
            Some(BinaryLayout::Canonical) => self.container_depth(),
            Some(BinaryLayout::Validated(layout)) => Ok(usize::from(layout.depth)),
            // A rejected layout may contain invalid text that depth ignores.
            _ => self.validate_layout_at::<false>(0, true).map(|layout| usize::from(layout.depth)),
        }
    }

    fn depth_for_layout(self, layout: BinaryLayout) -> Result<usize, BinaryJSONError> {
        match layout {
            BinaryLayout::Canonical => self.container_depth(),
            BinaryLayout::Validated(layout) => Ok(usize::from(layout.depth)),
            BinaryLayout::Invalid => Err(BinaryJSONError::InvalidBinary),
            BinaryLayout::TooDeep => Err(BinaryJSONError::TooDeep),
        }
    }

    // For a validated document, scalar payloads and object keys cannot affect
    // depth. Only follow value entries that point to nested containers.
    pub(crate) fn container_depth(self) -> Result<usize, BinaryJSONError> {
        if !matches!(self.type_code, JSON_TYPE_CODE_ARRAY | JSON_TYPE_CODE_OBJECT) {
            return Ok(1);
        }
        let container = self.container()?;
        let mut deepest_child = usize::from(container.count != 0);
        for (index, code) in container.child_type_codes().enumerate() {
            if matches!(code, JSON_TYPE_CODE_ARRAY | JSON_TYPE_CODE_OBJECT) {
                deepest_child = deepest_child.max(container.child(index)?.container_depth()?);
            }
        }
        Ok(1 + deepest_child)
    }

    #[inline]
    pub(crate) fn string_bytes(self) -> Option<&'a [u8]> {
        if self.type_code != JSON_TYPE_CODE_STRING {
            return None;
        }
        let (length, prefix) = decode_uvarint(self.value).ok()?;
        self.value.get(prefix..prefix.checked_add(length)?)
    }

    #[inline]
    pub(crate) fn container(self) -> Result<JSONContainer<'a>, BinaryJSONError> {
        self.container_payload(true)
    }

    #[inline]
    fn container_payload(self, exact: bool) -> Result<JSONContainer<'a>, BinaryJSONError> {
        let key_width = match self.type_code {
            JSON_TYPE_CODE_ARRAY => 0,
            JSON_TYPE_CODE_OBJECT => KEY_ENTRY_SIZE,
            _ => return Err(BinaryJSONError::InvalidBinary),
        };
        let (count, size) = read_header(self.value)?;
        let tables_size = count
            .checked_mul(key_width + VALUE_ENTRY_SIZE)
            .and_then(|size| size.checked_add(HEADER_SIZE))
            .ok_or(BinaryJSONError::InvalidBinary)?;
        if (exact && size != self.value.len()) || tables_size > size {
            return Err(BinaryJSONError::InvalidBinary);
        }
        let value = self.value.get(..size).ok_or(BinaryJSONError::InvalidBinary)?;
        Ok(JSONContainer {
            value,
            count,
            value_entries: HEADER_SIZE + count * key_width,
        })
    }

    fn validate_layout_at<const CHECK_TEXT: bool>(
        self,
        depth: usize,
        exact: bool,
    ) -> Result<ValidatedLayout, BinaryJSONError> {
        if !matches!(self.type_code, JSON_TYPE_CODE_ARRAY | JSON_TYPE_CODE_OBJECT) {
            self.validate_scalar_payload::<CHECK_TEXT>(exact)?;
            return Ok(ValidatedLayout {
                canonical: true,
                depth: 1,
            });
        }
        self.validate_container_layout::<CHECK_TEXT>(self.container_payload(exact)?, depth, false)
    }

    fn validate_container_layout<const CHECK_TEXT: bool>(
        self,
        container: JSONContainer<'a>,
        depth: usize,
        keys_checked: bool,
    ) -> Result<ValidatedLayout, BinaryJSONError> {
        let value_end = container.value_entries + container.count * VALUE_ENTRY_SIZE;
        let keys = container.value[HEADER_SIZE..container.value_entries]
            .as_chunks::<KEY_ENTRY_SIZE>().0;
        // A successful scan proves that no key check can fail. On a miss,
        // keep checking each key before its value to preserve error order.
        let keys = if !CHECK_TEXT || keys_checked || (keys.len() >= 8
            && container.check_short_keys(keys, container.value.len()).is_some())
        {
            &[]
        } else {
            keys
        };
        let mut keys = keys.iter();
        let values = container.value[container.value_entries..value_end]
            .as_chunks::<VALUE_ENTRY_SIZE>()
            .0;
        if depth < MAX_JSON_DEPTH
            && self.type_code == JSON_TYPE_CODE_ARRAY
            && values.len() >= 8
            && matches!(values[0][0], JSON_TYPE_CODE_INT64 | JSON_TYPE_CODE_UINT64)
            && values.iter().fold(true, |valid, entry| {
                valid
                    & matches!(entry[0], JSON_TYPE_CODE_INT64 | JSON_TYPE_CODE_UINT64)
                    & (value_entry_offset(entry) <= container.value.len() - 8)
            })
        {
            return Ok(ValidatedLayout {
                canonical: true,
                depth: 2,
            });
        }
        let mut previous = None;
        let mut previous_container: Option<(BinaryJSONRef<'_>, ValidatedLayout)> = None;
        let mut compare_siblings = values.len() >= 8;
        let mut canonical = true;
        let mut deepest_child = u8::from(container.count != 0);
        for (index, entry) in values.iter().enumerate() {
            if let Some(key) = keys.next() {
                let key = container.key_from_entry(key)?;
                if !is_valid_json_text(key) {
                    return Err(BinaryJSONError::InvalidBinary);
                }
                canonical &= previous.is_none_or(|prior| prior < key);
                previous = Some(key);
            }
            if depth == MAX_JSON_DEPTH {
                // Check the entry's bounds before rejecting its nesting level.
                container.child(index)?;
                return Err(BinaryJSONError::TooDeep);
            }
            match entry[0] {
                JSON_TYPE_CODE_LITERAL => {
                    if !matches!(
                        entry[1],
                        JSON_LITERAL_NULL | JSON_LITERAL_FALSE | JSON_LITERAL_TRUE
                    ) {
                        return Err(BinaryJSONError::InvalidBinary);
                    }
                }
                JSON_TYPE_CODE_INT64
                | JSON_TYPE_CODE_UINT64
                | JSON_TYPE_CODE_DATE
                | JSON_TYPE_CODE_DATETIME
                | JSON_TYPE_CODE_TIMESTAMP => {
                    // The validated container has at least its eight-byte header.
                    if value_entry_offset(entry) > container.value.len() - 8 {
                        return Err(BinaryJSONError::InvalidBinary);
                    }
                }
                JSON_TYPE_CODE_ARRAY | JSON_TYPE_CODE_OBJECT => {
                    let child = container.child_from_entry(entry)?;
                    let layout = if compare_siblings {
                        match previous_container {
                            // The encoded header includes the child's size.
                            Some((prior, layout))
                                if prior.type_code == child.type_code
                                    && child.value.starts_with(prior.value) =>
                            {
                                layout
                            }
                            _ => {
                                let container = child.container_payload(false)?;
                                let layout = child.validate_container_layout::<CHECK_TEXT>(container, depth + 1, false)?;
                                // A miss after the first child ends the probe.
                                compare_siblings = previous_container.is_none()
                                    && (32..=4096).contains(&container.value.len());
                                previous_container = compare_siblings.then_some((
                                    BinaryJSONRef {
                                        value: container.value,
                                        ..child
                                    },
                                    layout,
                                ));
                                layout
                            }
                        }
                    } else {
                        child.validate_container_layout::<CHECK_TEXT>(
                            child.container_payload(false)?,
                            depth + 1,
                            false,
                        )?
                    };
                    canonical &= layout.canonical;
                    deepest_child = deepest_child.max(layout.depth);
                }
                _ => container.child_from_entry(entry)?.validate_scalar_payload::<CHECK_TEXT>(false)?,
            }
        }
        Ok(ValidatedLayout {
            canonical,
            depth: deepest_child + 1,
        })
    }

    fn checked_layout(self) -> BinaryLayout {
        EncodedJSONLayout::from_validation(self.validate_layout_at::<true>(0, true)).0
    }

    fn is_canonical_layout(self) -> bool {
        self.validate_layout_at::<true>(0, true)
            .is_ok_and(|layout| layout.canonical)
    }

    fn validate_scalar(self) -> Result<(), BinaryJSONError> {
        self.validate_scalar_payload::<true>(true)
    }

    fn validate_scalar_payload<const CHECK_TEXT: bool>(self, exact: bool) -> Result<(), BinaryJSONError> {
        let payload = |size: usize| {
            if exact && size != self.value.len() {
                return Err(BinaryJSONError::InvalidBinary);
            }
            self.value.get(..size).ok_or(BinaryJSONError::InvalidBinary)
        };
        match self.type_code {
            JSON_TYPE_CODE_STRING => {
                let (length, prefix) = decode_uvarint(self.value)?;
                let size = prefix
                    .checked_add(length)
                    .ok_or(BinaryJSONError::InvalidBinary)?;
                let bytes = &payload(size)?[prefix..];
                if !CHECK_TEXT {
                    return Ok(());
                }
                let (blocks, tail) = bytes.as_chunks::<128>();
                let text = match blocks.iter().position(|block| !block.is_ascii()) {
                    // Every preceding byte is ASCII, so this starts at a character boundary.
                    Some(index) => &bytes[index * 128..],
                    None => tail,
                };
                is_valid_json_text(text).then_some(()).ok_or(BinaryJSONError::InvalidBinary)
            }
            JSON_TYPE_CODE_LITERAL => match payload(1)?[0] {
                JSON_LITERAL_NULL | JSON_LITERAL_FALSE | JSON_LITERAL_TRUE => Ok(()),
                _ => Err(BinaryJSONError::InvalidBinary),
            },
            JSON_TYPE_CODE_FLOAT64 => {
                let bytes = payload(8)?.try_into().unwrap();
                f64::from_le_bytes(bytes)
                    .is_finite()
                    .then_some(())
                    .ok_or(BinaryJSONError::InvalidBinary)
            }
            _ => payload(value_length(self.type_code, self.value)?).map(|_| ()),
        }
    }

    pub(crate) fn copy(self) -> BinaryJSON {
        BinaryJSON {
            type_code: self.type_code,
            value: Bytes::copy_from_slice(self.value),
            layout: OnceLock::from(BinaryLayout::Canonical),
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) enum JSONContainerEdit<'a> {
    Replace {
        index: usize,
        value: BinaryJSONRef<'a>,
    },
    Insert {
        index: usize,
        key: &'a [u8],
        value: BinaryJSONRef<'a>,
    },
    Remove {
        index: usize,
    },
}

#[derive(Clone, Copy)]
enum WireEdit<'a> {
    Child(JSONContainerEdit<'a>),
    Descendant { index: usize, type_code: u8 },
}

impl<'a> JSONContainer<'a> {
    pub(crate) fn child_type_codes(self) -> impl Iterator<Item = u8> + 'a {
        let end = self.value_entries + self.count * VALUE_ENTRY_SIZE;
        self.value[self.value_entries..end]
            .chunks_exact(VALUE_ENTRY_SIZE)
            .map(|entry| entry[0])
    }

    pub(crate) fn encode_keys(self) -> Result<BinaryJSON, BinaryJSONError> {
        self.checked_key_array::<false>().map(JSONKeyArray::into_json)
    }

    pub(crate) fn checked_key_array<const CHECK_KEYS: bool>(self) -> Result<JSONKeyArray<'a>, BinaryJSONError> {
        let keys = self.value[HEADER_SIZE..self.value_entries].as_chunks::<KEY_ENTRY_SIZE>().0;
        let values_end = self.value_entries + self.count * VALUE_ENTRY_SIZE;
        let data_start = HEADER_SIZE + self.count * VALUE_ENTRY_SIZE;
        let mut size = data_start;
        for entry in keys {
            let key = self.key_from_entry(entry)?;
            if CHECK_KEYS {
                let offset = u32::from_le_bytes(entry[..4].try_into().unwrap()) as usize;
                if offset < values_end || !is_valid_json_text(key) {
                    return Err(BinaryJSONError::InvalidBinary);
                }
            }
            let len = key.len();
            let prefix_len = uvarint_size(len);
            size = size
                .checked_add(prefix_len + len)
                .ok_or(BinaryJSONError::InvalidBinary)?;
        }
        u32::try_from(size).map_err(|_| BinaryJSONError::InvalidBinary)?;
        Ok(JSONKeyArray { object: self, size })
    }

    pub(crate) fn key(self, index: usize) -> Result<&'a [u8], BinaryJSONError> {
        let entry = HEADER_SIZE + index * KEY_ENTRY_SIZE;
        self.key_from_entry(self.value[entry..entry + KEY_ENTRY_SIZE].try_into().unwrap())
    }

    fn check_short_keys(self, keys: &[[u8; KEY_ENTRY_SIZE]], key_end: usize) -> Option<()> {
        // Non-ASCII keys often sort after the ASCII keys. Inspect the last
        // key before attempting the full short-key scan.
        let last = self.key_from_entry(keys.last()?).ok()?;
        if last.len() > 16 || !last.is_ascii() {
            return None;
        }
        check_short_ascii_keys(self.value, keys, key_end)
    }

    fn checked_key_prefix(self, limit: usize) -> Option<usize> {
        let keys = self.value[HEADER_SIZE..self.value_entries].as_chunks::<KEY_ENTRY_SIZE>().0;
        let mut end = self.value_entries + self.count * VALUE_ENTRY_SIZE;
        if let Some(&[a, b, c, d, e, f]) = keys.last() {
            let offset = u32::from_le_bytes([a, b, c, d]) as usize;
            end = end.max(offset.checked_add(usize::from(u16::from_le_bytes([e, f])))?);
        }
        if end > limit || end > self.value.len() {
            return None;
        }
        if !keys.is_empty() && self.check_short_keys(keys, end).is_none() {
            let prefix = Self { value: &self.value[..end], ..self };
            let mut previous = None;
            for entry in keys {
                let key = prefix.key_from_entry(entry).ok()?;
                if !is_valid_json_text(key) || previous.is_some_and(|prior| prior >= key) {
                    return None;
                }
                previous = Some(key);
            }
        }
        Some(end)
    }

    fn key_from_entry(
        self,
        &[a, b, c, d, e, f]: &[u8; KEY_ENTRY_SIZE],
    ) -> Result<&'a [u8], BinaryJSONError> {
        let offset = u32::from_le_bytes([a, b, c, d]) as usize;
        let len = u16::from_le_bytes([e, f]) as usize;
        self.value
            .get(offset..)
            .and_then(|key| key.get(..len))
            .ok_or(BinaryJSONError::InvalidBinary)
    }

    pub(crate) fn child(self, index: usize) -> Result<BinaryJSONRef<'a>, BinaryJSONError> {
        let child = self.child_payload(index)?;
        if child.type_code == JSON_TYPE_CODE_LITERAL {
            return Ok(child);
        }
        let len = value_length(child.type_code, child.value)?;
        let value = child.value.get(..len).ok_or(BinaryJSONError::InvalidBinary)?;
        Ok(BinaryJSONRef { value, ..child })
    }

    fn child_payload(self, index: usize) -> Result<BinaryJSONRef<'a>, BinaryJSONError> {
        let entry = self.value_entries + index * VALUE_ENTRY_SIZE;
        self.child_from_entry(self.value[entry..entry + VALUE_ENTRY_SIZE].try_into().unwrap())
    }

    fn payload_checks(self) -> Result<Option<Vec<PayloadCheck>>, BinaryJSONError> {
        const MAX_PAYLOAD_CHECKS: usize = 32;
        let end = self.value_entries + self.count * VALUE_ENTRY_SIZE;
        let entries = self.value[self.value_entries..end].as_chunks::<VALUE_ENTRY_SIZE>().0;
        let mut checks = Vec::new();
        let mut child_budget = 8192;
        let mut dynamic_checks = 0;
        let mut index = 0;
        while let Some(entry) = entries.get(index) {
            let check = match entry[0] {
                JSON_TYPE_CODE_INT64 | JSON_TYPE_CODE_UINT64
                | JSON_TYPE_CODE_DATE | JSON_TYPE_CODE_DATETIME | JSON_TYPE_CODE_TIMESTAMP => {
                    if value_entry_offset(entry) > self.value.len() - 8 {
                        return Err(BinaryJSONError::InvalidBinary);
                    }
                    None
                }
                JSON_TYPE_CODE_LITERAL => {
                    if !matches!(entry[1], JSON_LITERAL_NULL | JSON_LITERAL_FALSE | JSON_LITERAL_TRUE) {
                        return Err(BinaryJSONError::InvalidBinary);
                    }
                    None
                }
                JSON_TYPE_CODE_ARRAY | JSON_TYPE_CODE_OBJECT => {
                    if let Some((child, prefix_len)) =
                        PayloadCheck::for_child(self, index, entry, child_budget)
                    {
                        child_budget -= prefix_len;
                        checks.push(child);
                        index += 1;
                        continue;
                    }
                    Some(PayloadCheck::Value(index))
                }
                JSON_TYPE_CODE_STRING => {
                    if let Some(run) = PackedStringRun::find(self.value, &entries[index..]) {
                        index += run.count() - 1;
                        Some(PayloadCheck::Strings(run))
                    } else {
                        Some(PayloadCheck::Value(index))
                    }
                }
                _ => Some(PayloadCheck::Value(index)),
            };
            if let Some(check) = check {
                if dynamic_checks == MAX_PAYLOAD_CHECKS {
                    return Ok(None);
                }
                dynamic_checks += 1;
                checks.push(check);
            }
            index += 1;
        }
        Ok(Some(checks))
    }

    fn validate_payloads(self, checks: &mut [PayloadCheck]) -> Result<ValidatedLayout, BinaryJSONError> {
        let mut canonical = true;
        let mut deepest = 1;
        for check in checks {
            let layout = match check {
                PayloadCheck::Value(index) => self.child_payload(*index)?.validate_layout_at::<true>(1, false)?,
                PayloadCheck::Child(child) => {
                    // The root proof fixes the offset and the available length.
                    // The child prefix fixes its bounds, entries, and keys.
                    if child.matches(&self.value[child.offset..]) {
                        ValidatedLayout { canonical: true, depth: child.depth }
                    } else {
                        let index = child.index;
                        *check = PayloadCheck::Value(index);
                        self.child_payload(index)?.validate_layout_at::<true>(1, false)?
                    }
                }
                PayloadCheck::ChildWithValues(child) => {
                    let value = &self.value[child.layout.offset..];
                    if child.layout.matches(value) {
                        child.validate_values(value)?;
                        ValidatedLayout { canonical: true, depth: child.layout.depth }
                    } else {
                        let index = child.layout.index;
                        *check = PayloadCheck::Value(index);
                        self.child_payload(index)?.validate_layout_at::<true>(1, false)?
                    }
                }
                PayloadCheck::Strings(run) => {
                    if !run.validate(self.value)? {
                        let view = BinaryJSONRef {
                            type_code: JSON_TYPE_CODE_OBJECT,
                            value: self.value,
                        };
                        return view.validate_container_layout::<true>(self, 0, true);
                    }
                    continue;
                }
            };
            canonical &= layout.canonical;
            deepest = deepest.max(layout.depth);
        }
        Ok(ValidatedLayout { canonical, depth: deepest + 1 })
    }

    fn child_from_entry(
        self,
        entry: &'a [u8; VALUE_ENTRY_SIZE],
    ) -> Result<BinaryJSONRef<'a>, BinaryJSONError> {
        let type_code = entry[0];
        let value = if type_code == JSON_TYPE_CODE_LITERAL {
            &entry[1..2]
        } else {
            let offset = value_entry_offset(entry);
            self.value.get(offset..).ok_or(BinaryJSONError::InvalidBinary)?
        };
        Ok(BinaryJSONRef { type_code, value })
    }

    pub(crate) fn find(self, key: &[u8]) -> Result<Option<BinaryJSONRef<'a>>, BinaryJSONError> {
        let (position, found) = self.key_position(key)?;
        found.then(|| self.child(position)).transpose()
    }

    /// Returns the member index or the sorted insertion point.
    pub(crate) fn key_position(self, key: &[u8]) -> Result<(usize, bool), BinaryJSONError> {
        let (mut start, mut end) = (0, self.count);
        while start < end {
            let middle = start + (end - start) / 2;
            match self.key(middle)?.cmp(key) {
                Ordering::Less => start = middle + 1,
                Ordering::Greater => end = middle,
                Ordering::Equal => return Ok((middle, true)),
            }
        }
        Ok((start, false))
    }

    /// Packs the changed container and its ancestors into one output buffer.
    pub(crate) fn rewrite(
        self,
        type_code: u8,
        edit: JSONContainerEdit<'_>,
        ancestors: &[(BinaryJSONRef<'_>, usize)],
    ) -> Result<BinaryJSON, BinaryJSONError> {
        let (root_type, root_len) = ancestors
            .first()
            .map_or((type_code, self.value.len()), |(root, _)| {
                (root.type_code, root.value.len())
            });
        let size = match edit {
            JSONContainerEdit::Replace { index, value } => {
                root_len.saturating_sub(stored_value_len(self.child(index)?))
                    + stored_value_len(value)
            }
            JSONContainerEdit::Insert { key, value, .. } => {
                root_len
                    + VALUE_ENTRY_SIZE
                    + stored_value_len(value)
                    + if type_code == JSON_TYPE_CODE_OBJECT {
                        KEY_ENTRY_SIZE + key.len()
                    } else {
                        0
                    }
            }
            JSONContainerEdit::Remove { index } => {
                let key = if type_code == JSON_TYPE_CODE_OBJECT {
                    KEY_ENTRY_SIZE + self.key(index)?.len()
                } else {
                    0
                };
                root_len
                    .saturating_sub(VALUE_ENTRY_SIZE + key + stored_value_len(self.child(index)?))
            }
        };
        let mut output = Vec::with_capacity(size);
        append_edited_path(&mut output, ancestors, type_code, &mut |output| {
            self.append_edit(output, type_code, WireEdit::Child(edit), |_| unreachable!())
        })?;
        Ok(BinaryJSON {
            type_code: root_type,
            value: Bytes::from(output),
            layout: OnceLock::new(),
        })
    }

    fn append_edit(
        self,
        output: &mut Vec<u8>,
        type_code: u8,
        edit: WireEdit<'_>,
        append_descendant: impl FnOnce(&mut Vec<u8>) -> Result<(), BinaryJSONError>,
    ) -> Result<(), BinaryJSONError> {
        let (count, index, replacement, inserted_key, removed) = match edit {
            WireEdit::Child(JSONContainerEdit::Replace { index, value }) => {
                (self.count, index, Some(value), None, false)
            }
            WireEdit::Child(JSONContainerEdit::Insert { index, key, value }) => {
                (self.count + 1, index, Some(value), Some(key), false)
            }
            WireEdit::Child(JSONContainerEdit::Remove { index }) => {
                (self.count - 1, index, None, None, true)
            }
            WireEdit::Descendant { index, .. } => (self.count, index, None, None, false),
        };
        let start = output.len();
        let object = type_code == JSON_TYPE_CODE_OBJECT;
        let value_entries = start + HEADER_SIZE + count * if object { KEY_ENTRY_SIZE } else { 0 };
        output.resize(value_entries + count * VALUE_ENTRY_SIZE, 0);
        let source_index = |i| {
            if i >= index && removed {
                i + 1
            } else if i > index && inserted_key.is_some() {
                i - 1
            } else {
                i
            }
        };
        if object {
            for i in 0..count {
                let key = match inserted_key {
                    Some(key) if i == index => key,
                    _ => self.key(source_index(i))?,
                };
                write_key_entry(output, start, i, key)?;
            }
        }
        let mut append_descendant = Some(append_descendant);
        for i in 0..count {
            let entry = value_entries + i * VALUE_ENTRY_SIZE;
            if i == index {
                if let WireEdit::Descendant { type_code, .. } = edit {
                    write_value_offset(output, start, entry, type_code)?;
                    append_descendant.take().expect("one edited descendant")(output)?;
                    continue;
                }
                if let Some(value) = replacement {
                    write_value_entry(output, start, entry, value)?;
                    continue;
                }
            }
            write_value_entry(output, start, entry, self.child(source_index(i))?)?;
        }
        write_header(&mut output[start..], count)
    }
}

fn append_edited_path(
    output: &mut Vec<u8>,
    ancestors: &[(BinaryJSONRef<'_>, usize)],
    leaf_type: u8,
    append_leaf: &mut impl FnMut(&mut Vec<u8>) -> Result<(), BinaryJSONError>,
) -> Result<(), BinaryJSONError> {
    let Some(((ancestor, index), descendants)) = ancestors.split_first() else {
        return append_leaf(output);
    };
    let type_code = descendants
        .first()
        .map_or(leaf_type, |(child, _)| child.type_code);
    ancestor.container()?.append_edit(
        output,
        ancestor.type_code,
        WireEdit::Descendant {
            index: *index,
            type_code,
        },
        |output| append_edited_path(output, descendants, leaf_type, append_leaf),
    )
}

/// The size pass checks the same nesting and wire bounds as construction.
fn typed_payload_size(
    value: &BinaryJSONValue,
    depth: usize,
    normalized: &mut Vec<BinaryJSON>,
) -> Result<usize, BinaryJSONError> {
    if depth > MAX_JSON_DEPTH {
        return Err(BinaryJSONError::TooDeep);
    }
    use BinaryJSONValue::*;
    let bytes = match value {
        Null | Bool(_) => 1,
        Int64(_) | Uint64(_) | Time(_) => 8,
        Float64(value) => {
            typed_float(*value)?;
            8
        }
        Number(value) => {
            typed_number(value)?;
            8
        }
        String(value) => value.len() + uvarint_size(value.len()),
        Opaque(value) => 1 + uvarint_size(value.bytes.len()) + value.bytes.len(),
        Duration(_) => 12,
        Binary(value) => {
            if reusable_typed_binary(value, depth)? {
                value.value.len()
            } else {
                let value = encode_node(&value.to_node()?, depth)?;
                let bytes = value.value.len();
                normalized.push(value);
                bytes
            }
        }
        Array(values) => {
            let entries = typed_container_entries(values.len(), VALUE_ENTRY_SIZE)?;
            let size = typed_container_size(values.iter(), entries, depth, normalized)?;
            u32::try_from(size).map_err(|_| BinaryJSONError::InvalidBinary)?;
            size
        }
        Object(values) => {
            let entries = typed_container_entries(values.len(), KEY_ENTRY_SIZE + VALUE_ENTRY_SIZE)?;
            // The encoder evaluates all children before checking key lengths.
            let size = typed_container_size(values.values(), entries, depth, normalized)?;
            let size = values.keys().try_fold(size, |size, key| {
                u16::try_from(key.len()).map_err(|_| BinaryJSONError::KeyTooLong)?;
                size.checked_add(key.len()).ok_or(BinaryJSONError::InvalidBinary)
            })?;
            u32::try_from(size).map_err(|_| BinaryJSONError::InvalidBinary)?;
            size
        }
    };
    Ok(bytes)
}

fn typed_container_entries(count: usize, width: usize) -> Result<usize, BinaryJSONError> {
    count.checked_mul(width)
        .and_then(|bytes| bytes.checked_add(HEADER_SIZE))
        .ok_or(BinaryJSONError::InvalidBinary)
}

fn typed_container_size<'a>(
    mut values: impl Iterator<Item = &'a BinaryJSONValue>,
    entries: usize,
    depth: usize,
    normalized: &mut Vec<BinaryJSON>,
) -> Result<usize, BinaryJSONError> {
    values.try_fold(entries, |size, value| {
        let bytes = typed_payload_size(value, depth + 1, normalized)?;
        if typed_literal(value).is_some() {
            Ok(size)
        } else {
            size.checked_add(bytes).ok_or(BinaryJSONError::InvalidBinary)
        }
    })
}

fn reusable_typed_binary(value: &BinaryJSON, depth: usize) -> Result<bool, BinaryJSONError> {
    let child_depth = match value.layout() {
        BinaryLayout::Canonical => value.view().container_depth()?,
        BinaryLayout::Validated(ValidatedLayout {
            canonical: true,
            depth,
        }) => usize::from(depth),
        _ => return Ok(false),
    };
    if depth + child_depth - 1 > MAX_JSON_DEPTH {
        return Err(BinaryJSONError::TooDeep);
    }
    Ok(true)
}

fn typed_literal(value: &BinaryJSONValue) -> Option<u8> {
    match value {
        BinaryJSONValue::Null => Some(JSON_LITERAL_NULL),
        BinaryJSONValue::Bool(true) => Some(JSON_LITERAL_TRUE),
        BinaryJSONValue::Bool(false) => Some(JSON_LITERAL_FALSE),
        BinaryJSONValue::Binary(value) if value.type_code == JSON_TYPE_CODE_LITERAL => {
            value.value.first().copied()
        }
        _ => None,
    }
}

fn typed_float(value: f64) -> Result<u64, BinaryJSONError> {
    value
        .is_finite()
        .then_some(value.to_bits())
        .ok_or(BinaryJSONError::InvalidText)
}

fn typed_number(value: &str) -> Result<(u8, u64), BinaryJSONError> {
    if let Ok(value) = value.parse::<i64>() {
        Ok((JSON_TYPE_CODE_INT64, value as u64))
    } else if let Ok(value) = value.parse::<u64>() {
        Ok((JSON_TYPE_CODE_UINT64, value))
    } else {
        let value = value.parse::<f64>().map_err(|_| BinaryJSONError::InvalidText)?;
        Ok((JSON_TYPE_CODE_FLOAT64, typed_float(value)?))
    }
}

fn encode_typed_value(value: &BinaryJSONValue) -> Result<BinaryJSON, BinaryJSONError> {
    use BinaryJSONValue::*;
    let (type_code, bits) = match value {
        Int64(value) => (JSON_TYPE_CODE_INT64, *value as u64),
        Uint64(value) => (JSON_TYPE_CODE_UINT64, *value),
        Float64(value) => (JSON_TYPE_CODE_FLOAT64, typed_float(*value)?),
        Number(value) => typed_number(value)?,
        Null | Bool(_) => return Ok(literal(typed_literal(value).unwrap())),
        Array(values) if values.is_empty() => {
            return Ok(BinaryJSON::from_shared_parts(
                JSON_TYPE_CODE_ARRAY,
                Bytes::from_static(EMPTY_JSON_CONTAINER),
            ));
        }
        Object(values) if values.is_empty() => {
            return Ok(BinaryJSON::from_shared_parts(
                JSON_TYPE_CODE_OBJECT,
                Bytes::from_static(EMPTY_JSON_CONTAINER),
            ));
        }
        Binary(value) => {
            return if reusable_typed_binary(value, 0)? {
                Ok(value.clone())
            } else {
                encode_node(&value.to_node()?, 0)
            };
        }
        Time(value) => return Ok(BinaryJSON::from_time(*value)),
        Duration(value) => return Ok(BinaryJSON::from_duration(*value)),
        _ => return encode_sized_typed_value(value),
    };
    Ok(BinaryJSON::from_shared_parts(
        type_code,
        Bytes::from(bits.to_le_bytes().to_vec()),
    ))
}

fn encode_sized_typed_value(value: &BinaryJSONValue) -> Result<BinaryJSON, BinaryJSONError> {
    let mut normalized = Vec::new();
    let size = typed_payload_size(value, 0, &mut normalized)?;
    let mut output = Vec::with_capacity(size);
    let type_code = append_sized_typed_value(value, &mut output, &mut normalized.into_iter())?;
    Ok(BinaryJSON {
        type_code,
        value: Bytes::from(output),
        layout: OnceLock::new(),
    })
}

/// Writes the input checked by typed_payload_size, reusing any binary
/// children that had to be normalized during sizing.
fn append_sized_typed_value(
    value: &BinaryJSONValue,
    output: &mut Vec<u8>,
    normalized: &mut std::vec::IntoIter<BinaryJSON>,
) -> Result<u8, BinaryJSONError> {
    use BinaryJSONValue::*;
    match value {
        Null | Bool(_) => {
            output.push(typed_literal(value).unwrap());
            Ok(JSON_TYPE_CODE_LITERAL)
        }
        Int64(value) => {
            output.extend_from_slice(&value.to_le_bytes());
            Ok(JSON_TYPE_CODE_INT64)
        }
        Uint64(value) => {
            output.extend_from_slice(&value.to_le_bytes());
            Ok(JSON_TYPE_CODE_UINT64)
        }
        Float64(value) => {
            output.extend_from_slice(&value.to_le_bytes());
            Ok(JSON_TYPE_CODE_FLOAT64)
        }
        Number(value) => {
            let (type_code, bits) = typed_number(value)?;
            output.extend_from_slice(&bits.to_le_bytes());
            Ok(type_code)
        }
        String(value) => {
            encode_uvarint(value.len(), output);
            output.extend_from_slice(value.as_bytes());
            Ok(JSON_TYPE_CODE_STRING)
        }
        Binary(value) => {
            if EncodedJSONLayout(value.layout()).is_canonical() {
                output.extend_from_slice(&value.value);
                Ok(value.type_code)
            } else {
                let value = normalized
                    .next()
                    .expect("binary child normalized during sizing");
                output.extend_from_slice(&value.value);
                Ok(value.type_code)
            }
        }
        Array(values) => {
            let start = output.len();
            let entries = start + HEADER_SIZE;
            output.resize(entries + values.len() * VALUE_ENTRY_SIZE, 0);
            append_typed_entries(values.iter(), output, start, entries, normalized)?;
            write_header(&mut output[start..], values.len())?;
            Ok(JSON_TYPE_CODE_ARRAY)
        }
        Object(values) => {
            let start = output.len();
            let entries = start + HEADER_SIZE + values.len() * KEY_ENTRY_SIZE;
            output.resize(entries + values.len() * VALUE_ENTRY_SIZE, 0);
            for (index, key) in values.keys().enumerate() {
                write_key_entry(output, start, index, key.as_bytes())?;
            }
            append_typed_entries(values.values(), output, start, entries, normalized)?;
            write_header(&mut output[start..], values.len())?;
            Ok(JSON_TYPE_CODE_OBJECT)
        }
        Opaque(value) => {
            output.push(value.type_code);
            encode_uvarint(value.bytes.len(), output);
            output.extend_from_slice(&value.bytes);
            Ok(JSON_TYPE_CODE_OPAQUE)
        }
        Time(value) => {
            output.extend_from_slice(&value.core_time().raw().to_le_bytes());
            Ok(match value.kind() {
                TimeType::Date => JSON_TYPE_CODE_DATE,
                TimeType::DateTime => JSON_TYPE_CODE_DATETIME,
                TimeType::Timestamp => JSON_TYPE_CODE_TIMESTAMP,
            })
        }
        Duration(value) => {
            output.extend_from_slice(&value.nanoseconds().to_le_bytes());
            output.extend_from_slice(&(value.fsp() as u32).to_le_bytes());
            Ok(JSON_TYPE_CODE_DURATION)
        }
    }
}

fn append_typed_entries<'a>(
    values: impl Iterator<Item = &'a BinaryJSONValue>,
    output: &mut Vec<u8>,
    start: usize,
    entries: usize,
    normalized: &mut std::vec::IntoIter<BinaryJSON>,
) -> Result<(), BinaryJSONError> {
    for (index, value) in values.enumerate() {
        let entry = entries + index * VALUE_ENTRY_SIZE;
        if let Some(literal) = typed_literal(value) {
            output[entry] = JSON_TYPE_CODE_LITERAL;
            output[entry + 1] = literal;
        } else {
            let offset = output.len();
            let type_code = append_sized_typed_value(value, output, normalized)?;
            write_value_offset(&mut output[..offset], start, entry, type_code)?;
        }
    }
    Ok(())
}

impl fmt::Display for BinaryJSON {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(view) = self.canonical_view() {
            if view.type_code == JSON_TYPE_CODE_OBJECT && view.value.len() <= 16 * 1024 {
                let mut text = String::with_capacity(view.value.len());
                format_json_ref(view, &mut text, &mut Vec::new())?;
                return formatter.write_str(&text);
            }
            return format_json_root(view, formatter);
        }
        self.format_unverified(formatter)
    }
}

impl BinaryJSON {
    fn to_special_string(&self) -> Result<String, fmt::Error> {
        if let Ok(opaque) = self.opaque() {
            return Ok(format!(
                "\"base64:type{}:{}\"",
                opaque.type_code,
                encode_base64(&opaque.bytes)
            ));
        }
        if let Ok(mut time) = self.as_time(6) {
            let _ = time.set_fsp(6);
            return Ok(quote_json_string(&time.to_string()));
        }
        if let Ok(duration) = self.as_duration() {
            let duration = MySqlDuration::from_nanoseconds(duration.nanoseconds(), 6)
                .map_err(|_| fmt::Error)?;
            return Ok(quote_json_string(&duration.to_string()));
        }
        Ok(String::new())
    }

    fn format_unverified(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let special = self.to_special_string()?;
        if !special.is_empty() {
            return formatter.write_str(&special);
        }
        if let Some(value) = self.as_f64() {
            return formatter.write_str(format_float64(value, &mut Vec::new()).ok_or(fmt::Error)?);
        }
        if matches!(self.type_code, JSON_TYPE_CODE_ARRAY | JSON_TYPE_CODE_OBJECT) {
            return match self.to_node() {
                Ok(node) => formatter.write_str(&format_node(&node)),
                Err(_) => Ok(()),
            };
        }
        match self.to_value() {
            Ok(value) => formatter.write_str(&format_value(&value)),
            Err(_) => Ok(()),
        }
    }
}

fn format_json_root(value: BinaryJSONRef<'_>, output: &mut impl fmt::Write) -> fmt::Result {
    if let Some(bytes) = value.string_bytes().filter(|bytes| bytes.len() <= 96) {
        let text = simdutf8::basic::from_utf8(bytes).map_err(|_| fmt::Error)?;
        return write_short_json_string(text, output);
    }
    format_json_ref(value, output, &mut Vec::new())
}

fn format_json_ref(
    value: BinaryJSONRef<'_>,
    output: &mut impl fmt::Write,
    float_buffer: &mut Vec<u8>,
) -> fmt::Result {
    match value.type_code {
        JSON_TYPE_CODE_ARRAY | JSON_TYPE_CODE_OBJECT => {
            let is_object = value.type_code == JSON_TYPE_CODE_OBJECT;
            output.write_char(if is_object { '{' } else { '[' })?;
            let container = value.container_payload(false).map_err(|_| fmt::Error)?;
            let end = container.value_entries + container.count * VALUE_ENTRY_SIZE;
            let entries = container.value[container.value_entries..end]
                .as_chunks::<VALUE_ENTRY_SIZE>()
                .0;
            let mut keys = container.value[HEADER_SIZE..container.value_entries]
                .as_chunks::<KEY_ENTRY_SIZE>()
                .0
                .iter();
            for (index, entry) in entries.iter().enumerate() {
                if index != 0 {
                    output.write_str(", ")?;
                }
                if is_object {
                    let key = container
                        .key_from_entry(keys.next().ok_or(fmt::Error)?)
                        .map_err(|_| fmt::Error)?;
                    write_json_key(key, output)?;
                }
                let child = container.child_from_entry(entry).map_err(|_| fmt::Error)?;
                format_json_ref(child, output, float_buffer)?;
            }
            output.write_char(if is_object { '}' } else { ']' })
        }
        JSON_TYPE_CODE_LITERAL => output.write_str(match value.value[0] {
            JSON_LITERAL_NULL => "null",
            JSON_LITERAL_TRUE => "true",
            _ => "false",
        }),
        JSON_TYPE_CODE_INT64 => {
            let number = i64::from_le_bytes(*value.value.first_chunk::<8>().ok_or(fmt::Error)?);
            output.write_str(itoa::Buffer::new().format(number))
        }
        JSON_TYPE_CODE_UINT64 => {
            let number = u64::from_le_bytes(*value.value.first_chunk::<8>().ok_or(fmt::Error)?);
            output.write_str(itoa::Buffer::new().format(number))
        }
        JSON_TYPE_CODE_FLOAT64 => {
            let number = f64::from_le_bytes(*value.value.first_chunk::<8>().ok_or(fmt::Error)?);
            output.write_str(format_float64(number, float_buffer).ok_or(fmt::Error)?)
        }
        JSON_TYPE_CODE_STRING => {
            let bytes = value.string_bytes().ok_or(fmt::Error)?;
            let text = simdutf8::basic::from_utf8(bytes).map_err(|_| fmt::Error)?;
            write_json_string(text, output)
        }
        JSON_TYPE_CODE_OPAQUE => {
            let (&type_code, payload) = value.value.split_first().ok_or(fmt::Error)?;
            let (length, prefix) = decode_uvarint(payload).map_err(|_| fmt::Error)?;
            let bytes = payload.get(prefix..prefix + length).ok_or(fmt::Error)?;
            let encoded = encode_base64(bytes);
            write!(output, "\"base64:type{type_code}:{encoded}\"")
        }
        _ => {
            let len = value_length(value.type_code, value.value).map_err(|_| fmt::Error)?;
            let value = BinaryJSONRef {
                value: &value.value[..len],
                ..value
            };
            output.write_str(&value.copy().to_special_string()?)
        }
    }
}

fn write_json_key(key: &[u8], output: &mut impl fmt::Write) -> fmt::Result {
    if key.len() <= 16
        && key
            .iter()
            .all(|&b| matches!(b, 0x20..=0x7f) && !matches!(b, b'"' | b'\\'))
    {
        let mut buffer = [b'"'; 20];
        let len = key.len();
        buffer[1..len + 1].copy_from_slice(key);
        buffer[len + 2..len + 4].copy_from_slice(b": ");
        return output.write_str(std::str::from_utf8(&buffer[..len + 4]).map_err(|_| fmt::Error)?);
    }
    let text = simdutf8::basic::from_utf8(key).map_err(|_| fmt::Error)?;
    write_json_string(text, output)?;
    output.write_str(": ")
}

/// Formats a JSON double using TiDB's MySQL-compatible exponent boundary.
///
/// This is the source `marshalFloat64To` rule: fixed notation is used in
/// `[1e-15, 1e15)`, scientific notation outside it, and fixed integral doubles
/// retain `.0`.
fn format_float64(value: f64, buffer: &mut Vec<u8>) -> Option<&str> {
    if !value.is_finite() {
        return None;
    }
    buffer.clear();
    let absolute = value.abs();
    let scaled = absolute * 256.0;
    if absolute < 16_777_216.0 && scaled.fract() == 0.0 {
        // Multiplication by 2^8 is exact. Such values need at most eight
        // decimal places, and the spacing below 2^24 is less than 10^-8.
        // The shortest conversion therefore emits these exact decimal digits.
        let units = scaled as u32;
        if value.is_sign_negative() {
            buffer.push(b'-');
        }
        buffer.extend_from_slice(itoa::Buffer::new().format(units >> 8).as_bytes());
        buffer.push(b'.');
        let (digits, len) = &SMALL_FLOAT_FRACTIONS[(units & 255) as usize];
        buffer.extend_from_slice(&digits[..*len]);
    } else if absolute != 0.0 && !(1e-15..1e15).contains(&absolute) {
        crate::go_strconv::append_float(buffer, value, b'e', -1, 64);
        if let Some(plus) = buffer.iter().position(|&byte| byte == b'+') {
            buffer.remove(plus);
        }
    } else {
        crate::go_strconv::append_float(buffer, value, b'f', -1, 64);
        if !buffer.contains(&b'.') {
            buffer.extend_from_slice(b".0");
        }
    }
    std::str::from_utf8(buffer).ok()
}

const SMALL_FLOAT_FRACTIONS: [([u8; 8], usize); 256] = small_float_fractions();

const fn small_float_fractions() -> [([u8; 8], usize); 256] {
    let mut table = [([b'0'; 8], 1); 256];
    let mut index = 1;
    while index < table.len() {
        // index / 256 expressed in units of 10^-8.
        let mut decimal = index * 390_625;
        let mut len = 8;
        while decimal % 10 == 0 {
            decimal /= 10;
            len -= 1;
        }
        let mut digit = len;
        while digit != 0 {
            digit -= 1;
            table[index].0[digit] = b'0' + (decimal % 10) as u8;
            decimal /= 10;
        }
        table[index].1 = len;
        index += 1;
    }
    table
}

/// Removes surrounding quotes and MySQL JSON escape sequences.
pub fn unquote_string(text: &str) -> Result<String, BinaryJSONError> {
    if text.len() >= 2 && text.starts_with('"') && text.ends_with('"') {
        return unquote_json_string(&text[1..text.len() - 1]);
    }
    Ok(text.to_owned())
}

/// Quotes a JSON path key, leaving an unescaped ECMAScript identifier bare.
pub fn quote_json_string(text: &str) -> String {
    let quoted = json_marshal_string_to(text);
    if is_ecmascript_identifier(text)
        && quoted.as_bytes()[1..quoted.len() - 1] == text.as_bytes()[..]
    {
        text.to_owned()
    } else {
        quoted
    }
}

/// Marshals a JSON string using Go's escaping rules.
pub fn json_marshal_string_to(text: &str) -> String {
    let mut output = String::with_capacity(text.len() + 2);
    write_json_string(text, &mut output).expect("writing to a String succeeds");
    output
}

/// Marshals arbitrary Go string bytes with HTML escaping disabled.
pub fn json_marshal_string_bytes_to(bytes: &[u8]) -> String {
    if let Ok(text) = simdutf8::basic::from_utf8(bytes) {
        return json_marshal_string_to(text);
    }
    let mut output = String::with_capacity(bytes.len() + 2);
    output.push('"');
    for chunk in bytes.utf8_chunks() {
        write_json_string_content(chunk.valid(), &mut output).expect("writing to a String succeeds");
        for _ in chunk.invalid() {
            output.push('\u{fffd}');
        }
    }
    output.push('"');
    output
}

fn write_json_string(text: &str, output: &mut impl fmt::Write) -> fmt::Result {
    output.write_char('"')?;
    write_json_string_content(text, output)?;
    output.write_char('"')
}

fn write_short_json_string(text: &str, output: &mut impl fmt::Write) -> fmt::Result {
    let bytes = text.as_bytes();
    let (blocks, tail) = bytes.as_chunks::<16>();
    if blocks.iter().all(|&block| !may_contain_escape(block))
        && tail.iter().all(|&byte| byte >= 0x20 && !matches!(byte, b'"' | b'\\' | 0xe2))
    {
        let mut quoted = [b'"'; 98];
        quoted[1..1 + bytes.len()].copy_from_slice(bytes);
        let quoted = simdutf8::basic::from_utf8(&quoted[..2 + bytes.len()]).map_err(|_| fmt::Error)?;
        return output.write_str(quoted);
    }

    let mut quoted = [0; 96 * 6 + 6];
    quoted[0] = b'"';
    let mut len = 1;
    let mut start = 0;
    buffer_json_escapes(text, 0, bytes, output, &mut start, &mut quoted, &mut len)?;
    let tail = &bytes[start..];
    quoted[len..len + tail.len()].copy_from_slice(tail);
    len += tail.len();
    quoted[len] = b'"';
    output.write_str(simdutf8::basic::from_utf8(&quoted[..len + 1]).map_err(|_| fmt::Error)?)
}

fn write_json_string_content(text: &str, output: &mut impl fmt::Write) -> fmt::Result {
    let bytes = text.as_bytes();
    let (blocks, tail) = bytes.as_chunks::<16>();
    let mut start = 0;
    for (i, block) in blocks.iter().enumerate() {
        if may_contain_escape(*block) {
            let offset = i * block.len();
            write_json_escape_block(text, offset, block, output, &mut start)?;
        }
    }
    write_json_escape_block(text, bytes.len() - tail.len(), tail, output, &mut start)?;
    if start < text.len() {
        output.write_str(&text[start..])?;
    }
    Ok(())
}

#[inline]
fn may_contain_escape(block: [u8; 16]) -> bool {
    // Check every byte lane at once. Borrow across lanes may flag an extra
    // byte, which the exact byte scan below handles.
    const ONES: u128 = u128::from_ne_bytes([1; 16]);
    const HIGH: u128 = u128::from_ne_bytes([0x80; 16]);
    let bytes = u128::from_ne_bytes(block);
    let below_space = bytes.wrapping_sub(ONES * 0x20) & !bytes & HIGH;
    let has = |byte: u128| {
        let lanes = bytes ^ (ONES * byte);
        lanes.wrapping_sub(ONES) & !lanes & HIGH != 0
    };
    below_space != 0 || has(u128::from(b'"')) || has(u128::from(b'\\')) || has(0xe2)
}

fn write_json_escape_block(
    text: &str,
    offset: usize,
    block: &[u8],
    output: &mut impl fmt::Write,
    start: &mut usize,
) -> fmt::Result {
    // At most six output bytes per byte in a 16-byte block. Copy the
    // prefix from earlier blocks directly before collecting local escapes.
    // Leave enough room for a six-byte store at the last short escape.
    let mut escaped = [0; 100];
    let mut len = 0;
    buffer_json_escapes(text, offset, block, output, start, &mut escaped, &mut len)?;
    if len != 0 {
        output.write_str(simdutf8::basic::from_utf8(&escaped[..len]).map_err(|_| fmt::Error)?)?;
    }
    Ok(())
}

fn buffer_json_escapes(
    text: &str,
    offset: usize,
    block: &[u8],
    output: &mut impl fmt::Write,
    start: &mut usize,
    escaped: &mut [u8],
    len: &mut usize,
) -> fmt::Result {
    for (index, byte) in block.iter().copied().enumerate() {
        let index = offset + index;
        let (escape, size, width) = match byte {
            0..=0x1f | b'"' | b'\\' => {
                let (escape, len) = ASCII_JSON_ESCAPES[usize::from(byte)];
                (escape, len, 1)
            }
            0xe2 => match text.as_bytes().get(index + 1..index + 3) {
                Some([0x80, 0xa8]) => (*br"\u2028", 6, 3),
                Some([0x80, 0xa9]) => (*br"\u2029", 6, 3),
                _ => continue,
            },
            _ => continue,
        };
        if *start < offset {
            output.write_str(&text[*start..index])?;
        } else if *start < index {
            let bytes = &text.as_bytes()[*start..index];
            escaped[*len..*len + bytes.len()].copy_from_slice(bytes);
            *len += bytes.len();
        }
        *escaped[*len..].first_chunk_mut::<6>().unwrap() = escape;
        *len += size;
        *start = index + width;
    }
    Ok(())
}

const ASCII_JSON_ESCAPES: [([u8; 6], usize); 128] = ascii_json_escapes();

const fn ascii_json_escapes() -> [([u8; 6], usize); 128] {
    let hex = b"0123456789abcdef";
    let mut escapes = [([0; 6], 0); 128];
    let mut byte = 0;
    while byte < 32 {
        escapes[byte] = ([b'\\', b'u', b'0', b'0', hex[byte / 16], hex[byte % 16]], 6);
        byte += 1;
    }
    let short = [
        (8, b'b'),
        (9, b't'),
        (10, b'n'),
        (12, b'f'),
        (13, b'r'),
        (b'"', b'"'),
        (b'\\', b'\\'),
    ];
    let mut i = 0;
    while i < short.len() {
        let (byte, suffix) = short[i];
        escapes[byte as usize] = ([b'\\', suffix, 0, 0, 0, 0], 2);
        i += 1;
    }
    escapes
}

/// Go `unquoteJSONString`: MySQL's JSON_UNQUOTE escapes over the string's
/// bytes; any other escaped byte stands for itself. A trailing backslash and
/// a malformed `\u` escape fail with Go's own messages; a `\u` surrogate
/// combines with an adjacent `\u` escape (`utf16.DecodeRune`, U+FFFD for a
/// bad pair).
pub fn unquote_json_string(text: &str) -> Result<String, BinaryJSONError> {
    let mut ret = Vec::with_capacity(text.len());
    unquote_json_string_bytes_to(text.as_bytes(), &mut ret)?;
    String::from_utf8(ret).map_err(|_| BinaryJSONError::InvalidText)
}

/// Appends the decoded contents of a MySQL JSON string without its quotes.
/// Ordinary bytes, including malformed UTF-8, pass through unchanged.
pub fn unquote_json_string_bytes_to(s: &[u8], ret: &mut Vec<u8>) -> Result<(), BinaryJSONError> {
    let mut i = 0;
    while i < s.len() {
        if s[i] != b'\\' {
            let end = s[i..]
                .iter()
                .position(|&byte| byte == b'\\')
                .map_or(s.len(), |offset| i + offset);
            ret.extend_from_slice(&s[i..end]);
            i = end;
            continue;
        }
        i += 1;
        if i == s.len() {
            return Err(BinaryJSONError::Unquote(
                "Missing a closing quotation mark in string".to_owned(),
            ));
        }
        match s[i] {
            b'"' => ret.push(b'"'),
            b'b' => ret.push(0x08),
            b'f' => ret.push(0x0c),
            b'n' => ret.push(b'\n'),
            b'r' => ret.push(b'\r'),
            b't' => ret.push(b'\t'),
            b'\\' => ret.push(b'\\'),
            b'u' => {
                if i + 5 > s.len() {
                    return Err(BinaryJSONError::Unquote(format!(
                        "Invalid unicode: {}",
                        String::from_utf8_lossy(&s[i + 1..])
                    )));
                }
                let first = &s[i + 1..i + 5];
                match decode_one_escaped_unicode(first) {
                    Ok(ch) => {
                        let mut buffer = [0; 4];
                        ret.extend_from_slice(ch.encode_utf8(&mut buffer).as_bytes());
                        i += 4;
                    }
                    Err((error, in_surrogate_range)) => {
                        if !(in_surrogate_range
                            && s.len() >= i + 11
                            && s[i + 5] == b'\\'
                            && s[i + 6] == b'u')
                        {
                            return Err(error);
                        }
                        let mut pair = [0; 8];
                        pair[..4].copy_from_slice(first);
                        pair[4..].copy_from_slice(&s[i + 7..i + 11]);
                        let ch = decode_one_escaped_unicode(&pair).map_err(|(error, _)| error)?;
                        let mut buffer = [0; 4];
                        ret.extend_from_slice(ch.encode_utf8(&mut buffer).as_bytes());
                        i += 10;
                    }
                }
            }
            other => ret.push(other),
        }
        i += 1;
    }
    Ok(())
}

/// Go `decodeOneEscapedUnicode` over four (or, for a surrogate pair, eight)
/// hex digits; the flag reports a lone surrogate, which the caller may pair.
fn decode_one_escaped_unicode(hex: &[u8]) -> Result<char, (BinaryJSONError, bool)> {
    let mut units = [0_u16; 2];
    for (index, digits) in hex.chunks(4).enumerate() {
        let mut unit = 0_u16;
        for &byte in digits {
            let Some(nibble) = (byte as char).to_digit(16) else {
                // Go `hex.InvalidByteError`: `encoding/hex: invalid byte: %#U`.
                let rune = char::from(byte);
                let shown = if rune.is_control() {
                    format!("U+{:04X}", u32::from(rune))
                } else {
                    format!("U+{:04X} '{rune}'", u32::from(rune))
                };
                return Err((
                    BinaryJSONError::Unquote(format!("encoding/hex: invalid byte: {shown}")),
                    false,
                ));
            };
            unit = (unit << 4) | nibble as u16;
        }
        units[index] = unit;
    }
    let scalar = if hex.len() == 8 {
        // Go `utf16.DecodeRune`.
        match (units[0], units[1]) {
            (high @ 0xd800..=0xdbff, low @ 0xdc00..=0xdfff) => {
                0x10000 + ((u32::from(high) - 0xd800) << 10) + (u32::from(low) - 0xdc00)
            }
            _ => 0xfffd,
        }
    } else {
        u32::from(units[0])
    };
    char::from_u32(scalar).ok_or_else(|| {
        (
            BinaryJSONError::Unquote(format!("Invalid unicode: {}", String::from_utf8_lossy(hex))),
            (0xd800..=0xdfff).contains(&scalar),
        )
    })
}

/// Rewrites the `\uXXXX` surrogate escapes that Go `encoding/json` decodes
/// to U+FFFD (lone surrogates and invalid pairs) into the literal U+FFFD
/// character, so the strict serde parser accepts what Go's lenient scanner
/// accepts. Everything else is copied verbatim.
fn replace_lone_surrogate_escapes(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut output = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        let is_surrogate_escape = bytes[index] == b'\\'
            && text.get(index..index + 2) == Some("\\u")
            && text.get(index + 2..index + 6).is_some_and(|hex| {
                u16::from_str_radix(hex, 16).is_ok_and(|value| (0xd800..=0xdfff).contains(&value))
            });
        if !is_surrogate_escape {
            output.push(bytes[index]);
            index += 1;
            continue;
        }
        let first = u16::from_str_radix(text.get(index + 2..index + 6).expect("checked above"), 16)
            .expect("checked above");
        // A high surrogate MAY pair with an adjacent `\uDC00..=\uDFFF`.
        let paired = if (0xd800..=0xdbff).contains(&first) {
            text.get(index + 6..index + 8)
                .filter(|next| *next == "\\u")
                .and_then(|_| text.get(index + 8..index + 12))
                .and_then(|hex| u16::from_str_radix(hex, 16).ok())
                .filter(|second| (0xdc00..=0xdfff).contains(second))
                .map(|second| {
                    let scalar = 0x10000
                        + ((u32::from(first) - 0xd800) << 10)
                        + (u32::from(second) - 0xdc00);
                    (index + 12, scalar)
                })
        } else {
            None
        };
        match paired {
            Some((next, scalar)) => {
                let ch = char::from_u32(scalar).unwrap_or('\u{fffd}');
                let mut encoded = [0; 4];
                output.extend_from_slice(ch.encode_utf8(&mut encoded).as_bytes());
                index = next;
            }
            None => {
                output.extend_from_slice("\\ufffd".as_bytes());
                index += 6;
            }
        }
    }
    String::from_utf8(output).unwrap_or_else(|_| text.to_owned())
}

/// Decodes one four- or eight-hex-digit escaped Unicode value.
pub fn decode_escaped_unicode(hex: &[u8]) -> Result<([u8; 4], usize, bool), BinaryJSONError> {
    if hex.len() != 4 && hex.len() != 8 {
        return Err(BinaryJSONError::InvalidText);
    }
    let first = decode_hex_u16(hex.get(..4).ok_or(BinaryJSONError::InvalidText)?)?;
    // Go `utf16.DecodeRune` (`json_binary_functions.go:166`) substitutes
    // U+FFFD for a lone surrogate or an invalid pair instead of failing.
    let scalar = if hex.len() == 8 {
        let second = decode_hex_u16(hex.get(4..).ok_or(BinaryJSONError::InvalidText)?)?;
        if (0xd800..=0xdbff).contains(&first) && (0xdc00..=0xdfff).contains(&second) {
            0x10000 + ((u32::from(first) - 0xd800) << 10) + (u32::from(second) - 0xdc00)
        } else {
            0xFFFD
        }
    } else if (0xd800..=0xdfff).contains(&first) {
        0xFFFD
    } else {
        u32::from(first)
    };
    let ch = char::from_u32(scalar).ok_or(BinaryJSONError::InvalidText)?;
    let mut output = [0_u8; 4];
    let size = ch.encode_utf8(&mut output).len();
    Ok((output, size, false))
}

fn decode_hex_u16(hex: &[u8]) -> Result<u16, BinaryJSONError> {
    if hex.len() != 4 {
        return Err(BinaryJSONError::InvalidText);
    }
    hex.iter().try_fold(0_u16, |value, byte| {
        let digit = (*byte as char)
            .to_digit(16)
            .ok_or(BinaryJSONError::InvalidText)?;
        Ok((value << 4) | digit as u16)
    })
}

fn is_ecmascript_identifier(value: &str) -> bool {
    let bytes = value.as_bytes();
    let Some(&first) = bytes.first() else {
        return false;
    };
    let is_letter = |byte: u8| {
        byte.is_ascii_alphabetic()
            || matches!(
                byte,
                0xAA | 0xB5 | 0xBA | 0xC0..=0xD6 | 0xD8..=0xF6 | 0xF8..=0xFF
            )
    };
    (is_letter(first) || first == b'$' || first == b'_')
        && bytes[1..]
            .iter()
            .all(|byte| is_letter(*byte) || byte.is_ascii_digit() || matches!(byte, b'$' | b'_'))
}

fn encode_base64(bytes: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut output = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let value = (u32::from(chunk[0]) << 16)
            | (u32::from(*chunk.get(1).unwrap_or(&0)) << 8)
            | u32::from(*chunk.get(2).unwrap_or(&0));
        output.push(TABLE[((value >> 18) & 0x3f) as usize] as char);
        output.push(TABLE[((value >> 12) & 0x3f) as usize] as char);
        output.push(if chunk.len() > 1 {
            TABLE[((value >> 6) & 0x3f) as usize] as char
        } else {
            '='
        });
        output.push(if chunk.len() > 2 {
            TABLE[(value & 0x3f) as usize] as char
        } else {
            '='
        });
    }
    output
}

/// Compares two binary JSON values using TiDB's JSON precedence and scalar rules.
pub fn compare_binary_json(left: &BinaryJSON, right: &BinaryJSON) -> Ordering {
    if left.type_code == JSON_TYPE_CODE_STRING && right.type_code == JSON_TYPE_CODE_STRING {
        return compare_string_payloads(&left.value, &right.value, |_, _| {
            left.canonical_view().is_some() && right.canonical_view().is_some()
        });
    }
    let rank = json_precedence(left.view()).cmp(&json_precedence(right.view()));
    if !rank.is_eq() {
        return rank;
    }
    match (left.canonical_view(), right.canonical_view()) {
        (Some(left), Some(right)) => compare_json_refs(left, right),
        _ => compare_binary_json_fallback(left, right),
    }
}

fn compare_json_views(
    left: BinaryJSONRef<'_>,
    right: BinaryJSONRef<'_>,
    left_is_canonical: impl FnOnce() -> Option<bool>,
    right_is_canonical: impl FnOnce() -> Option<bool>,
) -> Option<Ordering> {
    if left.type_code == JSON_TYPE_CODE_STRING && right.type_code == JSON_TYPE_CODE_STRING {
        return Some(compare_string_payloads(left.value, right.value, |left, right| {
            left_is_canonical().unwrap_or_else(|| simdutf8::basic::from_utf8(left).is_ok())
                && right_is_canonical().unwrap_or_else(|| simdutf8::basic::from_utf8(right).is_ok())
        }));
    }
    let rank = json_precedence(left).cmp(&json_precedence(right));
    if !rank.is_eq() {
        return Some(rank);
    }
    if left_is_canonical().unwrap_or_else(|| left.is_canonical_layout())
        && right_is_canonical().unwrap_or_else(|| right.is_canonical_layout())
    {
        Some(compare_json_refs(left, right))
    } else {
        None
    }
}

fn compare_string_payloads(
    left: &[u8],
    right: &[u8],
    valid_utf8: impl FnOnce(&[u8], &[u8]) -> bool,
) -> Ordering {
    fn split(payload: &[u8]) -> Option<(&[u8], &[u8])> {
        let (length, prefix) = decode_uvarint(payload).ok()?;
        let (header, text) = payload.split_at(prefix);
        (text.len() == length).then_some((header, text))
    }

    let (Some((left_header, left_text)), Some((right_header, right_text))) =
        (split(left), split(right))
    else {
        return left.cmp(right);
    };
    let text_order = left_text.cmp(right_text);
    let header_order = left_header.cmp(right_header);
    // Malformed strings compare by their full encoded bytes. With equal
    // headers, or when the headers and bodies order alike, validation
    // cannot change the comparison result.
    if header_order.is_eq() || header_order == text_order || valid_utf8(left_text, right_text) {
        text_order
    } else {
        header_order
    }
}

pub(crate) fn compare_json_refs(left: BinaryJSONRef<'_>, right: BinaryJSONRef<'_>) -> Ordering {
    let rank = json_precedence(left).cmp(&json_precedence(right));
    if !rank.is_eq() {
        return rank;
    }

    match (left.type_code, right.type_code) {
        (JSON_TYPE_CODE_ARRAY, JSON_TYPE_CODE_ARRAY)
        | (JSON_TYPE_CODE_OBJECT, JSON_TYPE_CODE_OBJECT) => {
            let is_object = left.type_code == JSON_TYPE_CODE_OBJECT;
            let left = left.container().expect("validated JSON layout");
            let right = right.container().expect("validated JSON layout");
            let count = left.count.cmp(&right.count);
            if is_object && !count.is_eq() {
                return count;
            }
            for index in 0..left.count.min(right.count) {
                if is_object {
                    let key = left.key(index).expect("validated JSON key")
                        .cmp(right.key(index).expect("validated JSON key"));
                    if !key.is_eq() {
                        return key;
                    }
                }
                let value = compare_json_refs(
                    left.child(index).expect("validated JSON entry"),
                    right.child(index).expect("validated JSON entry"),
                );
                if !value.is_eq() {
                    return value;
                }
            }
            count
        }
        (JSON_TYPE_CODE_LITERAL, JSON_TYPE_CODE_LITERAL) => right.value.cmp(left.value),
        (JSON_TYPE_CODE_INT64, JSON_TYPE_CODE_INT64) => {
            let left = i64::from_le_bytes(left.value.try_into().unwrap());
            let right = i64::from_le_bytes(right.value.try_into().unwrap());
            left.cmp(&right)
        }
        (JSON_TYPE_CODE_UINT64, JSON_TYPE_CODE_UINT64) => {
            let left = u64::from_le_bytes(left.value.try_into().unwrap());
            let right = u64::from_le_bytes(right.value.try_into().unwrap());
            left.cmp(&right)
        }
        (JSON_TYPE_CODE_STRING, JSON_TYPE_CODE_STRING) => {
            let (_, left_prefix) = decode_uvarint(left.value).expect("validated JSON string");
            let (_, right_prefix) = decode_uvarint(right.value).expect("validated JSON string");
            left.value[left_prefix..].cmp(&right.value[right_prefix..])
        }
        (JSON_TYPE_CODE_OPAQUE, JSON_TYPE_CODE_OPAQUE) => {
            let (_, left_prefix) = decode_uvarint(&left.value[1..]).expect("validated JSON opaque");
            let (_, right_prefix) = decode_uvarint(&right.value[1..]).expect("validated JSON opaque");
            left.value[1 + left_prefix..].cmp(&right.value[1 + right_prefix..])
        }
        (JSON_TYPE_CODE_DATE | JSON_TYPE_CODE_DATETIME | JSON_TYPE_CODE_TIMESTAMP, _) => {
            let left = CoreTime::from_raw(u64::from_le_bytes(left.value.try_into().unwrap()));
            let right = CoreTime::from_raw(u64::from_le_bytes(right.value.try_into().unwrap()));
            left.compare(right)
        }
        (JSON_TYPE_CODE_DURATION, JSON_TYPE_CODE_DURATION) => {
            let left = i64::from_le_bytes(left.value[..8].try_into().unwrap());
            let right = i64::from_le_bytes(right.value[..8].try_into().unwrap());
            left.cmp(&right)
        }
        _ => match (
            decode_value(left.type_code, left.value, 0),
            decode_value(right.type_code, right.value, 0),
        ) {
            (Ok(left), Ok(right)) => compare_json_value(&left, &right),
            _ => left.value.cmp(right.value),
        },
    }
}

fn compare_binary_json_fallback(left: &BinaryJSON, right: &BinaryJSON) -> Ordering {
    if left.type_code == JSON_TYPE_CODE_OPAQUE && right.type_code == JSON_TYPE_CODE_OPAQUE {
        return match (left.opaque(), right.opaque()) {
            (Ok(left), Ok(right)) => left.bytes.cmp(&right.bytes),
            _ => Ordering::Equal,
        };
    }
    if matches!(
        left.type_code,
        JSON_TYPE_CODE_DATE | JSON_TYPE_CODE_DATETIME | JSON_TYPE_CODE_TIMESTAMP
    ) && matches!(
        right.type_code,
        JSON_TYPE_CODE_DATE | JSON_TYPE_CODE_DATETIME | JSON_TYPE_CODE_TIMESTAMP
    ) {
        return match (left.as_time(0), right.as_time(0)) {
            (Ok(left), Ok(right)) => left.compare(right),
            _ => Ordering::Equal,
        };
    }
    if left.type_code == JSON_TYPE_CODE_DURATION && right.type_code == JSON_TYPE_CODE_DURATION {
        return match (left.as_duration(), right.as_duration()) {
            (Ok(left), Ok(right)) => left.nanoseconds().cmp(&right.nanoseconds()),
            _ => Ordering::Equal,
        };
    }
    if matches!(left.type_code, JSON_TYPE_CODE_ARRAY | JSON_TYPE_CODE_OBJECT)
        && left.type_code == right.type_code
    {
        return match (left.to_node(), right.to_node()) {
            (Ok(left), Ok(right)) => compare_container_nodes(&left, &right),
            _ => Ordering::Equal,
        };
    }
    match (left.to_value(), right.to_value()) {
        (Ok(left), Ok(right)) => compare_json_value(&left, &right),
        _ => left.value.cmp(&right.value),
    }
}

fn compare_container_nodes(left: &JSONNode, right: &JSONNode) -> Ordering {
    match (left, right) {
        (JSONNode::Array(left), JSONNode::Array(right)) => left
            .iter()
            .zip(right)
            .map(|(left, right)| compare_nodes(left, right))
            .find(|ordering| !ordering.is_eq())
            .unwrap_or_else(|| left.len().cmp(&right.len())),
        (JSONNode::Object(left), JSONNode::Object(right)) => {
            let count = left.len().cmp(&right.len());
            if !count.is_eq() {
                return count;
            }
            let mut left = left.iter().collect::<Vec<_>>();
            let mut right = right.iter().collect::<Vec<_>>();
            left.sort_unstable_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
            right.sort_unstable_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
            left.into_iter()
                .zip(right)
                .find_map(|((left_key, left_value), (right_key, right_value))| {
                    let key = left_key.as_bytes().cmp(right_key.as_bytes());
                    if !key.is_eq() {
                        return Some(key);
                    }
                    let value = compare_nodes(left_value, right_value);
                    (!value.is_eq()).then_some(value)
                })
                .unwrap_or(Ordering::Equal)
        }
        _ => Ordering::Equal,
    }
}

fn compare_nodes(left: &JSONNode, right: &JSONNode) -> Ordering {
    match (left, right) {
        (JSONNode::Scalar(left), JSONNode::Scalar(right)) => compare_binary_json(left, right),
        (JSONNode::Array(_), JSONNode::Array(_)) | (JSONNode::Object(_), JSONNode::Object(_)) => {
            compare_container_nodes(left, right)
        }
        _ => {
            let left = BinaryJSON::from_node(left).expect("decoded JSON node must re-encode");
            let right = BinaryJSON::from_node(right).expect("decoded JSON node must re-encode");
            json_precedence(left.view()).cmp(&json_precedence(right.view()))
        }
    }
}

fn json_precedence(value: BinaryJSONRef<'_>) -> i8 {
    match value.type_code {
        JSON_TYPE_CODE_OPAQUE => {
            if value_length(value.type_code, value.value) != Ok(value.value.len()) {
                return 12;
            }
            match value.value[0] {
                0x0f | 0xf9..=0xfe => 14,
                0x10 => 13,
                _ => 12,
            }
        }
        JSON_TYPE_CODE_DATETIME | JSON_TYPE_CODE_TIMESTAMP => 11,
        JSON_TYPE_CODE_DURATION => 10,
        JSON_TYPE_CODE_DATE => 9,
        JSON_TYPE_CODE_LITERAL if value.value != [JSON_LITERAL_NULL] => 8,
        JSON_TYPE_CODE_ARRAY => 7,
        JSON_TYPE_CODE_OBJECT => 6,
        JSON_TYPE_CODE_STRING => 5,
        JSON_TYPE_CODE_INT64 | JSON_TYPE_CODE_UINT64 | JSON_TYPE_CODE_FLOAT64 => 4,
        JSON_TYPE_CODE_LITERAL => 3,
        _ => 0,
    }
}

fn compare_json_value(left: &Value, right: &Value) -> Ordering {
    match (left, right) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Bool(left), Value::Bool(right)) => left.cmp(right),
        (Value::Number(left), Value::Number(right)) => compare_json_number(left, right),
        (Value::String(left), Value::String(right)) => left.as_bytes().cmp(right.as_bytes()),
        (Value::Array(left), Value::Array(right)) => left
            .iter()
            .zip(right)
            .map(|(left, right)| compare_json_value(left, right))
            .find(|ordering| !ordering.is_eq())
            .unwrap_or_else(|| left.len().cmp(&right.len())),
        (Value::Object(left), Value::Object(right)) => {
            let count = left.len().cmp(&right.len());
            if !count.is_eq() {
                return count;
            }
            let mut left = left.iter().collect::<Vec<_>>();
            let mut right = right.iter().collect::<Vec<_>>();
            left.sort_unstable_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
            right.sort_unstable_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
            left.into_iter()
                .zip(right)
                .find_map(|((left_key, left_value), (right_key, right_value))| {
                    let key = left_key.as_bytes().cmp(right_key.as_bytes());
                    if !key.is_eq() {
                        return Some(key);
                    }
                    let value = compare_json_value(left_value, right_value);
                    (!value.is_eq()).then_some(value)
                })
                .unwrap_or(Ordering::Equal)
        }
        _ => value_precedence(left).cmp(&value_precedence(right)),
    }
}

fn value_precedence(value: &Value) -> i8 {
    match value {
        Value::Null => 3,
        Value::Number(_) => 4,
        Value::String(_) => 5,
        Value::Object(_) => 6,
        Value::Array(_) => 7,
        Value::Bool(_) => 8,
    }
}

fn compare_json_number(left: &Number, right: &Number) -> Ordering {
    match (
        left.as_i64(),
        left.as_u64(),
        left.as_f64(),
        right.as_i64(),
        right.as_u64(),
        right.as_f64(),
    ) {
        (Some(left), _, _, Some(right), _, _) => left.cmp(&right),
        (Some(left), _, _, None, Some(right), _) => {
            if left < 0 {
                Ordering::Less
            } else {
                (left as u64).cmp(&right)
            }
        }
        (None, Some(left), _, Some(right), _, _) => {
            if right < 0 {
                Ordering::Greater
            } else {
                left.cmp(&(right as u64))
            }
        }
        (None, Some(left), _, None, Some(right), _) => left.cmp(&right),
        // Two doubles compare exactly (source `compareFloat64`). The epsilon
        // below is only for a double against an integer, where the source
        // widens the integer and accepts the precision loss
        // (`compareFloat64PrecisionLoss`).
        (None, None, Some(left), None, None, Some(right)) => {
            left.partial_cmp(&right).unwrap_or(Ordering::Greater)
        }
        (_, _, Some(left), _, _, Some(right)) => {
            if (left - right).abs() < FLOAT_EPSILON {
                Ordering::Equal
            } else {
                left.partial_cmp(&right).unwrap_or(Ordering::Greater)
            }
        }
        _ => Ordering::Equal,
    }
}

fn encode_node(node: &JSONNode, depth: usize) -> Result<BinaryJSON, BinaryJSONError> {
    if depth > MAX_JSON_DEPTH {
        return Err(BinaryJSONError::TooDeep);
    }
    match node {
        JSONNode::Scalar(value) => Ok(value.clone()),
        JSONNode::Array(values) => {
            let values = values
                .iter()
                .map(|value| encode_node(value, depth + 1))
                .collect::<Result<Vec<_>, _>>()?;
            encode_binary_array(&values)
        }
        JSONNode::Object(values) => {
            let values = values
                .iter()
                .map(|(key, value)| Ok((key.as_str(), encode_node(value, depth + 1)?)))
                .collect::<Result<Vec<_>, BinaryJSONError>>()?;
            encode_binary_object(&values)
        }
    }
}

pub(crate) fn decode_node(value: BinaryJSONRef<'_>, depth: usize) -> Result<JSONNode, BinaryJSONError> {
    if depth > MAX_JSON_DEPTH {
        return Err(BinaryJSONError::TooDeep);
    }
    match value.type_code {
        JSON_TYPE_CODE_ARRAY => {
            let container = value.container()?;
            let mut values = Vec::with_capacity(container.count);
            for index in 0..container.count {
                values.push(decode_node(container.child(index)?, depth + 1)?);
            }
            Ok(JSONNode::Array(values))
        }
        JSON_TYPE_CODE_OBJECT => {
            let container = value.container()?;
            let mut values = Vec::with_capacity(container.count);
            for index in 0..container.count {
                let key = std::str::from_utf8(container.key(index)?)
                    .map_err(|_| BinaryJSONError::InvalidBinary)?;
                values.push((
                    key.to_owned(),
                    decode_node(container.child(index)?, depth + 1)?,
                ));
            }
            Ok(JSONNode::Object(values))
        }
        _ => {
            value.validate_scalar()?;
            Ok(JSONNode::Scalar(value.copy()))
        }
    }
}

fn encode_binary_array(values: &[BinaryJSON]) -> Result<BinaryJSON, BinaryJSONError> {
    encode_view_array(values.iter().map(BinaryJSON::view))
}

pub(crate) fn encode_view_array<'a>(
    values: impl ExactSizeIterator<Item = BinaryJSONRef<'a>> + Clone,
) -> Result<BinaryJSON, BinaryJSONError> {
    let count = values.len();
    let data_start = HEADER_SIZE + count * VALUE_ENTRY_SIZE;
    let total = values.clone().try_fold(data_start, |size, value| {
        size.checked_add(stored_value_len(value))
            .ok_or(BinaryJSONError::InvalidBinary)
    })?;
    let mut output = Vec::with_capacity(total);
    output.resize(data_start, 0);
    for (index, value) in values.enumerate() {
        write_value_entry(
            &mut output,
            0,
            HEADER_SIZE + index * VALUE_ENTRY_SIZE,
            value,
        )?;
    }
    write_header(&mut output, count)?;
    Ok(BinaryJSON {
        type_code: JSON_TYPE_CODE_ARRAY,
        value: Bytes::from(output),
        layout: OnceLock::new(),
    })
}

fn encode_binary_object(values: &[(&str, BinaryJSON)]) -> Result<BinaryJSON, BinaryJSONError> {
    let mut values = values.to_vec();
    values.sort_unstable_by(|left, right| left.0.as_bytes().cmp(right.0.as_bytes()));
    encode_sorted_object_views(values.iter().map(|(key, value)| (key.as_bytes(), value.view())))
}

pub(crate) fn encode_sorted_object_views<'a>(
    values: impl ExactSizeIterator<Item = (&'a [u8], BinaryJSONRef<'a>)> + Clone,
) -> Result<BinaryJSON, BinaryJSONError> {
    let count = values.len();
    let value_entry_start = HEADER_SIZE + count * KEY_ENTRY_SIZE;
    let data_start = value_entry_start + count * VALUE_ENTRY_SIZE;
    let total = values.clone().try_fold(data_start, |total, (key, value)| {
        if key.len() > u16::MAX as usize {
            Err(BinaryJSONError::KeyTooLong)
        } else {
            total
                .checked_add(key.len())
                .and_then(|total| total.checked_add(stored_value_len(value)))
                .ok_or(BinaryJSONError::InvalidBinary)
        }
    })?;
    let mut output = Vec::with_capacity(total);
    output.resize(data_start, 0);
    for (index, (key, _)) in values.clone().enumerate() {
        write_key_entry(&mut output, 0, index, key)?;
    }
    for (index, (_, value)) in values.enumerate() {
        write_value_entry(
            &mut output,
            0,
            value_entry_start + index * VALUE_ENTRY_SIZE,
            value,
        )?;
    }
    write_header(&mut output, count)?;
    Ok(BinaryJSON {
        type_code: JSON_TYPE_CODE_OBJECT,
        value: Bytes::from(output),
        layout: OnceLock::new(),
    })
}

fn stored_value_len(value: BinaryJSONRef<'_>) -> usize {
    if value.type_code == JSON_TYPE_CODE_LITERAL {
        0
    } else {
        value.value.len()
    }
}

fn write_key_entry(
    output: &mut Vec<u8>,
    start: usize,
    index: usize,
    key: &[u8],
) -> Result<(), BinaryJSONError> {
    let entry = start + HEADER_SIZE + index * KEY_ENTRY_SIZE;
    let offset = u32::try_from(output.len() - start).map_err(|_| BinaryJSONError::InvalidBinary)?;
    let length = u16::try_from(key.len()).map_err(|_| BinaryJSONError::KeyTooLong)?;
    output[entry..entry + 4].copy_from_slice(&offset.to_le_bytes());
    output[entry + 4..entry + 6].copy_from_slice(&length.to_le_bytes());
    output.extend_from_slice(key);
    Ok(())
}

fn write_value_entry(
    output: &mut Vec<u8>,
    start: usize,
    entry: usize,
    value: BinaryJSONRef<'_>,
) -> Result<(), BinaryJSONError> {
    if value.type_code == JSON_TYPE_CODE_LITERAL {
        output[entry] = value.type_code;
        output[entry + 1] = *value.value.first().ok_or(BinaryJSONError::InvalidBinary)?;
    } else {
        write_value_offset(output, start, entry, value.type_code)?;
        output.extend_from_slice(value.value);
    }
    Ok(())
}

fn write_value_offset(
    output: &mut [u8],
    start: usize,
    entry: usize,
    type_code: u8,
) -> Result<(), BinaryJSONError> {
    let offset = u32::try_from(output.len() - start).map_err(|_| BinaryJSONError::InvalidBinary)?;
    output[entry] = type_code;
    output[entry + 1..entry + VALUE_ENTRY_SIZE].copy_from_slice(&offset.to_le_bytes());
    Ok(())
}

fn encode_value(
    value: &Value,
    depth: usize,
    max_depth: &mut u8,
) -> Result<BinaryJSON, BinaryJSONError> {
    if depth > MAX_JSON_DEPTH {
        return Err(BinaryJSONError::TooDeep);
    }
    match value {
        Value::Null => Ok(literal(JSON_LITERAL_NULL)),
        Value::Bool(true) => Ok(literal(JSON_LITERAL_TRUE)),
        Value::Bool(false) => Ok(literal(JSON_LITERAL_FALSE)),
        Value::Number(number) => encode_number(number),
        Value::String(text) => Ok(encode_string(text)),
        Value::Array(values) => {
            *max_depth = (*max_depth).max((depth + 1 + usize::from(!values.is_empty())) as u8);
            encode_array(values, depth + 1, max_depth)
        }
        Value::Object(values) => {
            *max_depth = (*max_depth).max((depth + 1 + usize::from(!values.is_empty())) as u8);
            encode_object(values, depth + 1, max_depth)
        }
    }
}

fn encode_string(text: &str) -> BinaryJSON {
    let mut bytes = Vec::new();
    encode_uvarint(text.len(), &mut bytes);
    bytes.extend_from_slice(text.as_bytes());
    BinaryJSON {
        type_code: JSON_TYPE_CODE_STRING,
        value: Bytes::from(bytes),
        layout: OnceLock::new(),
    }
}

fn literal(value: u8) -> BinaryJSON {
    let bytes = match value {
        JSON_LITERAL_NULL => Bytes::from_static(&[JSON_LITERAL_NULL]),
        JSON_LITERAL_TRUE => Bytes::from_static(&[JSON_LITERAL_TRUE]),
        JSON_LITERAL_FALSE => Bytes::from_static(&[JSON_LITERAL_FALSE]),
        _ => Bytes::from(vec![value]),
    };
    BinaryJSON {
        type_code: JSON_TYPE_CODE_LITERAL,
        value: bytes,
        layout: OnceLock::new(),
    }
}

fn encode_number(number: &Number) -> Result<BinaryJSON, BinaryJSONError> {
    if let Some(value) = number.as_i64() {
        Ok(BinaryJSON {
            type_code: JSON_TYPE_CODE_INT64,
            value: Bytes::from(value.to_le_bytes().to_vec()),
            layout: OnceLock::new(),
        })
    } else if let Some(value) = number.as_u64() {
        Ok(BinaryJSON {
            type_code: JSON_TYPE_CODE_UINT64,
            value: Bytes::from(value.to_le_bytes().to_vec()),
            layout: OnceLock::new(),
        })
    } else {
        let value = number.as_f64().ok_or(BinaryJSONError::InvalidText)?;
        Ok(BinaryJSON {
            type_code: JSON_TYPE_CODE_FLOAT64,
            value: Bytes::from(value.to_bits().to_le_bytes().to_vec()),
            layout: OnceLock::new(),
        })
    }
}

fn encode_array(
    values: &[Value],
    depth: usize,
    max_depth: &mut u8,
) -> Result<BinaryJSON, BinaryJSONError> {
    let entry_start = HEADER_SIZE;
    let data_start = entry_start + values.len() * VALUE_ENTRY_SIZE;
    let mut output = vec![0; data_start];
    let mut payload = Vec::new();
    for (index, value) in values.iter().enumerate() {
        let encoded = encode_value(value, depth, max_depth)?;
        let entry = entry_start + index * VALUE_ENTRY_SIZE;
        output[entry] = encoded.type_code;
        if encoded.type_code == JSON_TYPE_CODE_LITERAL {
            output[entry + 1] = encoded.value[0];
        } else {
            let offset = data_start
                .checked_add(payload.len())
                .and_then(|offset| u32::try_from(offset).ok())
                .ok_or(BinaryJSONError::InvalidBinary)?;
            output[entry + 1..entry + 5].copy_from_slice(&offset.to_le_bytes());
            payload.extend_from_slice(&encoded.value);
        }
    }
    output.extend_from_slice(&payload);
    write_header(&mut output, values.len())?;
    Ok(BinaryJSON {
        type_code: JSON_TYPE_CODE_ARRAY,
        value: Bytes::from(output),
        layout: OnceLock::new(),
    })
}

fn encode_object(
    values: &Map<String, Value>,
    depth: usize,
    max_depth: &mut u8,
) -> Result<BinaryJSON, BinaryJSONError> {
    let mut entries: Vec<_> = values.iter().collect();
    entries.sort_unstable_by(|left, right| left.0.as_bytes().cmp(right.0.as_bytes()));
    let key_entry_start = HEADER_SIZE;
    let value_entry_start = key_entry_start + entries.len() * KEY_ENTRY_SIZE;
    let key_data_start = value_entry_start + entries.len() * VALUE_ENTRY_SIZE;
    let key_bytes = entries.iter().try_fold(0_usize, |total, (key, _)| {
        if key.len() > u16::MAX as usize {
            Err(BinaryJSONError::KeyTooLong)
        } else {
            total
                .checked_add(key.len())
                .ok_or(BinaryJSONError::InvalidBinary)
        }
    })?;
    let value_data_start = key_data_start + key_bytes;
    let mut output = vec![0; key_data_start];
    let mut keys = Vec::with_capacity(key_bytes);
    let mut payload = Vec::new();

    for (index, (key, value)) in entries.into_iter().enumerate() {
        let key_entry = key_entry_start + index * KEY_ENTRY_SIZE;
        let key_offset = u32::try_from(key_data_start + keys.len())
            .map_err(|_| BinaryJSONError::InvalidBinary)?;
        output[key_entry..key_entry + 4].copy_from_slice(&key_offset.to_le_bytes());
        output[key_entry + 4..key_entry + 6].copy_from_slice(&(key.len() as u16).to_le_bytes());
        keys.extend_from_slice(key.as_bytes());

        let encoded = encode_value(value, depth, max_depth)?;
        let value_entry = value_entry_start + index * VALUE_ENTRY_SIZE;
        output[value_entry] = encoded.type_code;
        if encoded.type_code == JSON_TYPE_CODE_LITERAL {
            output[value_entry + 1] = encoded.value[0];
        } else {
            let offset = u32::try_from(value_data_start + payload.len())
                .map_err(|_| BinaryJSONError::InvalidBinary)?;
            output[value_entry + 1..value_entry + 5].copy_from_slice(&offset.to_le_bytes());
            payload.extend_from_slice(&encoded.value);
        }
    }
    output.extend_from_slice(&keys);
    output.extend_from_slice(&payload);
    write_header(&mut output, values.len())?;
    Ok(BinaryJSON {
        type_code: JSON_TYPE_CODE_OBJECT,
        value: Bytes::from(output),
        layout: OnceLock::new(),
    })
}

fn write_header(output: &mut [u8], count: usize) -> Result<(), BinaryJSONError> {
    let count = u32::try_from(count).map_err(|_| BinaryJSONError::InvalidBinary)?;
    let size = u32::try_from(output.len()).map_err(|_| BinaryJSONError::InvalidBinary)?;
    output[..4].copy_from_slice(&count.to_le_bytes());
    output[4..8].copy_from_slice(&size.to_le_bytes());
    Ok(())
}

fn decode_value(type_code: u8, bytes: &[u8], depth: usize) -> Result<Value, BinaryJSONError> {
    if depth > MAX_JSON_DEPTH {
        return Err(BinaryJSONError::TooDeep);
    }
    match type_code {
        JSON_TYPE_CODE_LITERAL if bytes == [JSON_LITERAL_NULL] => Ok(Value::Null),
        JSON_TYPE_CODE_LITERAL if bytes == [JSON_LITERAL_TRUE] => Ok(Value::Bool(true)),
        JSON_TYPE_CODE_LITERAL if bytes == [JSON_LITERAL_FALSE] => Ok(Value::Bool(false)),
        JSON_TYPE_CODE_INT64 if bytes.len() == 8 => Ok(Value::Number(
            i64::from_le_bytes(bytes.try_into().expect("length checked")).into(),
        )),
        JSON_TYPE_CODE_UINT64 if bytes.len() == 8 => Ok(Value::Number(
            u64::from_le_bytes(bytes.try_into().expect("length checked")).into(),
        )),
        JSON_TYPE_CODE_FLOAT64 if bytes.len() == 8 => {
            let value = f64::from_bits(u64::from_le_bytes(
                bytes.try_into().expect("length checked"),
            ));
            Number::from_f64(value)
                .map(Value::Number)
                .ok_or(BinaryJSONError::InvalidBinary)
        }
        JSON_TYPE_CODE_STRING => {
            let (length, prefix) = decode_uvarint(bytes)?;
            let text = std::str::from_utf8(
                bytes
                    .get(prefix..prefix + length)
                    .ok_or(BinaryJSONError::InvalidBinary)?,
            )
            .map_err(|_| BinaryJSONError::InvalidBinary)?;
            if prefix + length != bytes.len() {
                return Err(BinaryJSONError::InvalidBinary);
            }
            Ok(Value::String(text.to_owned()))
        }
        JSON_TYPE_CODE_ARRAY => decode_array(bytes, depth + 1),
        JSON_TYPE_CODE_OBJECT => decode_object(bytes, depth + 1),
        _ => Err(BinaryJSONError::InvalidBinary),
    }
}

fn decode_array(bytes: &[u8], depth: usize) -> Result<Value, BinaryJSONError> {
    let (count, size) = read_header(bytes)?;
    if size != bytes.len() || HEADER_SIZE + count * VALUE_ENTRY_SIZE > bytes.len() {
        return Err(BinaryJSONError::InvalidBinary);
    }
    let mut values = Vec::with_capacity(count);
    for index in 0..count {
        let entry = HEADER_SIZE + index * VALUE_ENTRY_SIZE;
        values.push(decode_entry(
            bytes[entry],
            &bytes[entry + 1..entry + 5],
            bytes,
            depth,
        )?);
    }
    Ok(Value::Array(values))
}

fn decode_object(bytes: &[u8], depth: usize) -> Result<Value, BinaryJSONError> {
    let (count, size) = read_header(bytes)?;
    let value_entries = HEADER_SIZE + count * KEY_ENTRY_SIZE;
    if size != bytes.len() || value_entries + count * VALUE_ENTRY_SIZE > bytes.len() {
        return Err(BinaryJSONError::InvalidBinary);
    }
    let mut values = Map::new();
    for index in 0..count {
        let key_entry = HEADER_SIZE + index * KEY_ENTRY_SIZE;
        let key_offset =
            u32::from_le_bytes(bytes[key_entry..key_entry + 4].try_into().unwrap()) as usize;
        let key_length =
            u16::from_le_bytes(bytes[key_entry + 4..key_entry + 6].try_into().unwrap()) as usize;
        let key = std::str::from_utf8(
            bytes
                .get(key_offset..key_offset + key_length)
                .ok_or(BinaryJSONError::InvalidBinary)?,
        )
        .map_err(|_| BinaryJSONError::InvalidBinary)?;
        let entry = value_entries + index * VALUE_ENTRY_SIZE;
        values.insert(
            key.to_owned(),
            decode_entry(bytes[entry], &bytes[entry + 1..entry + 5], bytes, depth)?,
        );
    }
    Ok(Value::Object(values))
}

fn decode_entry(
    type_code: u8,
    entry: &[u8],
    container: &[u8],
    depth: usize,
) -> Result<Value, BinaryJSONError> {
    if type_code == JSON_TYPE_CODE_LITERAL {
        return decode_value(type_code, &entry[..1], depth);
    }
    let offset = u32::from_le_bytes(entry.try_into().unwrap()) as usize;
    let value = container
        .get(offset..)
        .ok_or(BinaryJSONError::InvalidBinary)?;
    let length = value_length(type_code, value)?;
    decode_value(type_code, &value[..length], depth)
}

fn value_length(type_code: u8, bytes: &[u8]) -> Result<usize, BinaryJSONError> {
    match type_code {
        JSON_TYPE_CODE_OBJECT | JSON_TYPE_CODE_ARRAY => {
            let (_, size) = read_header(bytes)?;
            Ok(size)
        }
        JSON_TYPE_CODE_OPAQUE => {
            let payload = bytes.get(1..).ok_or(BinaryJSONError::InvalidBinary)?;
            let (length, prefix) = decode_uvarint(payload)?;
            length.checked_add(prefix + 1).ok_or(BinaryJSONError::InvalidBinary)
        }
        JSON_TYPE_CODE_STRING => {
            let (length, prefix) = decode_uvarint(bytes)?;
            length.checked_add(prefix).ok_or(BinaryJSONError::InvalidBinary)
        }
        _ => fixed_payload_len(type_code).ok_or(BinaryJSONError::InvalidBinary),
    }
}

fn value_entry_offset(&[_, a, b, c, d]: &[u8; VALUE_ENTRY_SIZE]) -> usize {
    u32::from_le_bytes([a, b, c, d]) as usize
}

pub(crate) fn fixed_payload_len(type_code: u8) -> Option<usize> {
    match type_code {
        JSON_TYPE_CODE_LITERAL => Some(1),
        JSON_TYPE_CODE_INT64 | JSON_TYPE_CODE_UINT64 | JSON_TYPE_CODE_FLOAT64
        | JSON_TYPE_CODE_DATE | JSON_TYPE_CODE_DATETIME | JSON_TYPE_CODE_TIMESTAMP => Some(8),
        JSON_TYPE_CODE_DURATION => Some(12),
        _ => None,
    }
}

#[inline]
fn read_header(bytes: &[u8]) -> Result<(usize, usize), BinaryJSONError> {
    let header = bytes
        .get(..HEADER_SIZE)
        .ok_or(BinaryJSONError::InvalidBinary)?;
    let count = u32::from_le_bytes(header[..4].try_into().unwrap()) as usize;
    let size = u32::from_le_bytes(header[4..8].try_into().unwrap()) as usize;
    Ok((count, size))
}

pub(crate) fn uvarint_size(value: usize) -> usize {
    (value.max(1).ilog2() / 7 + 1) as usize
}

pub(crate) fn encode_uvarint(mut value: usize, output: &mut Vec<u8>) {
    while value >= 0x80 {
        output.push((value as u8) | 0x80);
        value >>= 7;
    }
    output.push(value as u8);
}

#[inline]
fn decode_uvarint(bytes: &[u8]) -> Result<(usize, usize), BinaryJSONError> {
    let (&first, rest) = bytes.split_first().ok_or(BinaryJSONError::InvalidBinary)?;
    if first < 0x80 {
        return Ok((usize::from(first), 1));
    }
    let mut value = usize::from(first & 0x7f);
    for (index, byte) in rest.iter().copied().enumerate().take(9) {
        value |= usize::from(byte & 0x7f) << ((index + 1) * 7);
        if byte < 0x80 {
            return Ok((value, index + 2));
        }
    }
    Err(BinaryJSONError::InvalidBinary)
}

fn format_value(value: &Value) -> String {
    match value {
        Value::Null => "null".to_owned(),
        Value::Bool(value) => value.to_string(),
        Value::Number(value) => value.to_string(),
        Value::String(value) => json_marshal_string_to(value),
        Value::Array(values) => format!(
            "[{}]",
            values
                .iter()
                .map(format_value)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        Value::Object(values) => {
            let mut entries: Vec<_> = values.iter().collect();
            entries.sort_unstable_by(|left, right| left.0.as_bytes().cmp(right.0.as_bytes()));
            format!(
                "{{{}}}",
                entries
                    .into_iter()
                    .map(|(key, value)| format!(
                        "{}: {}",
                        json_marshal_string_to(key),
                        format_value(value)
                    ))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        }
    }
}

fn format_node(value: &JSONNode) -> String {
    match value {
        JSONNode::Scalar(value) => value.to_string(),
        JSONNode::Array(values) => format!(
            "[{}]",
            values
                .iter()
                .map(format_node)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        JSONNode::Object(values) => format!(
            "{{{}}}",
            values
                .iter()
                .map(|(key, value)| format!("{}: {}", json_marshal_string_to(key), format_node(value)))
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn short_json_unicode_validation_preserves_utf8_boundaries() {
        let prefixes: &[&[u8]] = &[
            b"\xc1\xbf",
            b"\xc2\x80",
            b"\xdf\xbf",
            b"\xe0\x9f\xbf",
            b"\xe0\xa0\x80",
            b"\xe4\x80\x80",
            b"\xeb\xbf\xbf",
            b"\xed\x9f\xbf",
            b"\xed\xa0\x80",
            b"\xef\xbf\xbf",
            b"\xf0\x8f\xbf\xbf",
            b"\xf0\x90\x80\x80",
            b"\xf4\x8f\xbf\xbf",
            b"\xf4\x90\x80\x80",
            b"\xf5\x80\x80\x80",
            b"\xe8\xb5",
        ];
        let tails: &[&[u8]] = &[b"", b"123", "值é🙂".as_bytes(), b"ascii\xff"];
        for &prefix in prefixes {
            for &tail in tails {
                let text = [prefix, tail].concat();
                assert_eq!(is_valid_child_json_text(&text), std::str::from_utf8(&text).is_ok());
            }
        }
    }

    #[test]
    fn checked_object_keys_do_not_reuse_child_validation() {
        let members = (0..16)
            .map(|index| format!(r#""k{index:02}":{index}"#))
            .collect::<Vec<_>>()
            .join(",");
        let cell = BinaryJSON::parse(&format!(r#"{{{members},"z":{{"v":"ok"}}}}"#))
            .unwrap()
            .encoded();
        let mut keys = CheckedJSONKeys::new(&cell).unwrap();
        assert!(keys.check_layout(&cell).unwrap().is_canonical());

        let mut changed = cell.clone();
        *changed.last_mut().unwrap() = 0xff;
        assert!(!keys.check_layout(&changed).unwrap().is_valid());
        assert!(!keys.check_layout(&cell[..cell.len() - 1]).unwrap().is_valid());

        let first_key = 1 + u32::from_le_bytes(cell[1 + HEADER_SIZE..1 + HEADER_SIZE + 4].try_into().unwrap()) as usize;
        changed = cell;
        changed[first_key] = b'a';
        assert!(keys.check_layout(&changed).unwrap().is_canonical());
        changed[first_key] = b'z';
        assert!(keys.check_layout(&changed).is_none());
    }

    #[test]
    fn checked_packed_keys_recheck_only_when_order_is_proven() {
        let object = |changed: &[usize]| {
            let members = (0..32)
                .map(|index| {
                    let suffix = if changed.contains(&index) { 'b' } else { 'a' };
                    format!(r#""long-key-{index:02}-{suffix}":{index}"#)
                })
                .collect::<Vec<_>>()
                .join(",");
            BinaryJSON::parse(&format!("{{{members}}}")).unwrap().encoded()
        };
        let original = object(&[]);
        let mut keys = CheckedJSONKeys::new(&original).unwrap();
        let PackedObjectKeys { last_start, data_start, .. } = *keys.packed_keys.as_ref().unwrap();
        for changed in [&[0][..], &[15], &[31], &[0, 15, 31]] {
            assert!(keys.check_layout(&object(changed)).unwrap().is_canonical());
        }

        let mut invalid = object(&[31]);
        invalid[last_start] = 0xff;
        assert!(keys.check_layout(&invalid).is_none());
        assert!(!BinaryJSON::check_encoded_layout(&invalid).is_valid());

        let mut unordered = object(&[0, 15]);
        unordered[data_start] = b'z';
        assert!(keys.check_layout(&unordered).is_none());
        let layout = BinaryJSON::check_encoded_layout(&unordered);
        assert!(layout.is_valid() && !layout.is_canonical());
    }

    #[test]
    fn checked_child_prefix_covers_only_unchanged_layout() {
        let record = |value| {
            let members = (0..40)
                .map(|index| format!(r#""k{index:02}":{{"a":{value},"b":true}}"#))
                .collect::<Vec<_>>()
                .join(",");
            BinaryJSON::parse(&format!("{{{members}}}")).unwrap().encoded()
        };
        let mut keys = CheckedJSONKeys::new(&record(1)).unwrap();
        let checks = keys.value_checks.as_deref().unwrap();
        assert_eq!(checks.len(), 40);
        let PayloadCheck::Child(child) = &checks[39] else {
            panic!("expected a checked child object");
        };

        let offset = child.offset;
        let prefix_len = child.prefix.len();
        let updated = record(2);
        assert!(keys.check_layout(&updated).unwrap().is_canonical());
        let mut malformed = updated.clone();
        malformed[1 + offset + HEADER_SIZE + 2 * KEY_ENTRY_SIZE + VALUE_ENTRY_SIZE + 1] = 0xff;
        assert!(!keys.check_layout(&malformed).unwrap().is_valid());

        let mut unordered = updated;
        assert!(matches!(keys.value_checks.as_deref().unwrap()[39], PayloadCheck::Value(39)));

        let key_offset = 1 + offset + prefix_len - 2;
        unordered[key_offset] = b'z';
        let layout = keys.check_layout(&unordered).unwrap();
        assert!(layout.is_valid() && !layout.is_canonical());
        assert_eq!(BinaryJSON::encoded_element_depth(&unordered, || Some(layout)), Ok(3));
    }

    #[test]
    fn checked_child_keeps_its_proof_when_only_the_last_key_changes() {
        let record = |last| {
            let members = (0..16)
                .map(|index| format!(r#""k{index:02}":{index}"#))
                .collect::<Vec<_>>()
                .join(",");
            BinaryJSON::parse(&format!(r#"{{"child":{{"a":1,"{last}":2}},{members}}}"#))
                .unwrap()
                .encoded()
        };
        let original = record("foo");
        let mut keys = CheckedJSONKeys::new(&original).unwrap();
        let [PayloadCheck::Child(child)] = keys.value_checks.as_deref().unwrap() else {
            panic!("expected a checked child object");
        };
        let key_start = 1 + child.offset + child.prefix.len() - 3;
        for key in ["fog", "zap"] {
            assert!(keys.check_layout(&record(key)).unwrap().is_canonical());
            assert!(matches!(keys.value_checks.as_deref(), Some([PayloadCheck::Child(_)])));
        }

        let mut invalid = record("fog");
        invalid[key_start] = 0xff;
        assert!(!keys.check_layout(&invalid).unwrap().is_valid());

        let mut unordered = record("fog");
        unordered[key_start] = b'!';
        let mut keys = CheckedJSONKeys::new(&original).unwrap();
        let layout = keys.check_layout(&unordered).unwrap();
        assert!(layout.is_valid() && !layout.is_canonical());
        assert!(matches!(keys.value_checks.as_deref(), Some([PayloadCheck::Value(_)])));
    }

    #[test]
    fn checked_child_revalidates_floats_and_bounds_strings_to_the_child() {
        let record = |number, text, key| {
            let members = (0..16)
                .map(|index| format!(r#""k{index:02}":{index}"#))
                .collect::<Vec<_>>()
                .join(",");
            BinaryJSON::parse(&format!(r#"{{"child":{{"number":{number},"{key}":"{text}"}},{members}}}"#))
                .unwrap()
                .encoded()
        };
        let mut keys = CheckedJSONKeys::new(&record(1.5, "abc", "text")).unwrap();
        let [PayloadCheck::ChildWithValues(child)] = keys.value_checks.as_deref().unwrap() else {
            panic!("expected a cached child object");
        };
        let ChildValueChecks::FloatAndString { number, text } = child.checks else {
            panic!("expected float and string checks");
        };
        let number = 1 + child.layout.offset + number;
        let text = 1 + child.layout.offset + text;
        let current = record(2.5, "雪", "term");
        assert!(keys.check_layout(&current).unwrap().is_canonical());
        assert!(matches!(keys.value_checks.as_deref(), Some([PayloadCheck::ChildWithValues(_)])));

        let mut nonfinite = current.clone();
        nonfinite[number..number + 8].copy_from_slice(&f64::NAN.to_le_bytes());
        let mut invalid_utf8 = current.clone();
        invalid_utf8[text + 1] = 0xff;
        let mut beyond_child = current;
        beyond_child[text] += 1;
        for broken in [&nonfinite, &invalid_utf8, &beyond_child] {
            assert!(!keys.check_layout(broken).unwrap().is_valid());
            assert!(!BinaryJSON::check_encoded_layout(broken).is_valid());
        }
    }

    #[test]
    fn checked_child_revalidates_second_unicode_character() {
        let record = |key, text| {
            let members = (0..16)
                .map(|index| format!(r#""k{index:02}":{index}"#))
                .collect::<Vec<_>>()
                .join(",");
            BinaryJSON::parse(&format!(r#"{{"child":{{"number":1.5,"{key}":"{text}"}},{members}}}"#))
                .unwrap()
                .encoded()
        };
        let original = record("字é0", "読🙂");
        let changed = record("字ñ1", "読𐀀");
        let mut keys = CheckedJSONKeys::new(&original).unwrap();
        let [PayloadCheck::ChildWithValues(child)] = keys.value_checks.as_deref().unwrap() else {
            panic!("expected a cached child object");
        };
        let ChildValueChecks::FloatAndString { text, .. } = child.checks else {
            panic!("expected float and string checks");
        };
        let child_start = 1 + child.layout.offset;
        let key = child.layout.last_key.as_ref().unwrap();
        let second_key_character = child_start + usize::from(key.start) + 3;
        let second_text_character = child_start + text + 1 + 3;
        assert!(keys.check_layout(&changed).unwrap().is_canonical());

        let mut invalid_key = changed.clone();
        invalid_key[second_key_character] = 0xc0;
        let mut invalid_text = changed;
        invalid_text[second_text_character + 1] = 0x8f;
        for invalid in [&invalid_text, &invalid_key] {
            assert!(!keys.check_layout(invalid).unwrap().is_valid());
            assert!(!BinaryJSON::check_encoded_layout(invalid).is_valid());
        }
    }

    #[test]
    fn checked_child_run_revalidates_changed_layouts_and_payloads() {
        let record = |number, change_key, distinct_text| {
            let members = (0..20)
                .map(|index| {
                    let key = if change_key && index == 4 { "zvalue" } else { "status" };
                    let status = if distinct_text { index } else { 0 };
                    format!(
                        r#""key{index:03}":{{"index":{index},"reading":{number},"{key}":"値{status:03}"}}"#
                    )
                })
                .collect::<Vec<_>>()
                .join(",");
            BinaryJSON::parse(&format!("{{{members}}}")).unwrap().encoded()
        };
        let initial = record(1.5, false, false);
        let changed = record(2.5, true, true);
        let mut keys = CheckedJSONKeys::new(&initial).unwrap();
        let PayloadChecks::Run(run) = &keys.value_checks else {
            panic!("expected a shared child proof");
        };
        assert_eq!(run.count, 20);
        let (offset, size, number, text) = (1 + run.offset, run.size, run.number, run.text);
        let same_layout = |keys: &mut CheckedJSONKeys, cell: &[u8]| {
            let cached = keys.check_layout(cell).unwrap();
            let general = BinaryJSON::check_encoded_layout(cell);
            assert_eq!(cached.is_valid(), general.is_valid());
            assert_eq!(cached.is_canonical(), general.is_canonical());
            assert_eq!(
                BinaryJSON::encoded_element_depth(cell, || Some(cached)),
                BinaryJSON::encoded_element_depth(cell, || Some(general)),
            );
        };
        same_layout(&mut keys, &initial);
        same_layout(&mut keys, &record(2.5, false, true));
        same_layout(&mut keys, &changed);

        let mut empty = initial.clone();
        for child in empty[offset..].chunks_exact_mut(size) {
            child[..4].fill(0);
        }
        same_layout(&mut keys, &empty);

        let last = offset + 19 * size;
        let mut nonfinite = initial.clone();
        nonfinite[last + number..last + number + 8].copy_from_slice(&f64::NAN.to_le_bytes());
        let mut invalid_utf8 = initial.clone();
        invalid_utf8[offset + 8 * size + text + 1] = 0xff;
        let mut beyond_child = initial.clone();
        beyond_child[last + text] += 1;
        for broken in [&nonfinite, &invalid_utf8, &beyond_child] {
            assert!(!keys.check_layout(broken).unwrap().is_valid());
            same_layout(&mut keys, broken);
        }
        same_layout(&mut keys, &initial);
    }

    #[test]
    fn checked_unicode_keys_preserve_bounds_and_value_validation() {
        let record = |number| {
            let members = (0..128)
                .map(|index| format!(r#""long-attribute-name-雪{index:03}":{number}"#))
                .collect::<Vec<_>>()
                .join(",");
            BinaryJSON::parse(&format!(r#"{{{members},"雪":{{"v":"ok"}}}}"#))
                .unwrap()
                .encoded()
        };
        let old = record(0);
        let mut keys = CheckedJSONKeys::new(&old).unwrap();
        assert!((4097..=8192).contains(&keys.prefix_len()));

        let mut changed = record(129);
        assert!(keys.check_layout(&changed).unwrap().is_canonical());
        *changed.last_mut().unwrap() = 0xff;
        assert!(!keys.check_layout(&changed).unwrap().is_valid());

        let mut changed = record(32);
        let first_entry = 1 + HEADER_SIZE + 129 * KEY_ENTRY_SIZE;
        changed[first_entry + 1..first_entry + VALUE_ENTRY_SIZE].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(keys.check_layout(&changed).is_none());
        assert!(CheckedJSONKeys::new(&changed).is_none());
    }

    #[test]
    fn checked_packed_string_run_revalidates_lengths_and_utf8() {
        const FIRST: &str = "雪abc";
        for varying in [false, true] {
            let members = (0..40)
                .map(|index| {
                    let text = if varying && index % 2 != 0 { "x" } else { FIRST };
                    format!(r#""k{index:02}":"{text}""#)
                })
                .collect::<Vec<_>>()
                .join(",");
            let cell = BinaryJSON::parse(&format!("{{{members}}}"))
                .unwrap()
                .encoded();
            let mut keys = CheckedJSONKeys::new(&cell).unwrap();
            let Some([PayloadCheck::Strings(run)]) = keys.value_checks.as_deref() else {
                panic!("expected one packed string run");
            };
            assert_eq!(matches!(run.lengths, PackedStringLengths::Varying(_)), varying);
            assert_eq!(run.count(), 40);
            let start = 1 + run.start;
            assert!(keys.check_layout(&cell).unwrap().is_canonical());

            let mut shorter = cell.clone();
            shorter[start] -= 1;
            assert!(BinaryJSON::check_encoded_layout(&shorter).is_canonical());
            assert!(keys.check_layout(&shorter).unwrap().is_canonical());

            let mut split_character = cell;
            split_character[start + FIRST.len()] = 0xc2;
            assert!(!BinaryJSON::check_encoded_layout(&split_character).is_valid());
            assert!(!keys.check_layout(&split_character).unwrap().is_valid());
        }
    }

    #[test]
    fn checked_packed_long_strings_keep_utf8_within_each_value() {
        let text = format!("{}0123", "雪語".repeat(25));
        let members = (0..40)
            .map(|index| format!(r#""k{index:02}":"{text}""#))
            .collect::<Vec<_>>()
            .join(",");
        let cell = BinaryJSON::parse(&format!("{{{members}}}")).unwrap().encoded();
        let mut keys = CheckedJSONKeys::new(&cell).unwrap();
        let Some([PayloadCheck::Strings(run)]) = keys.value_checks.as_deref() else {
            panic!("expected a packed long-string run");
        };
        assert_eq!(run.count(), 40);
        let start = 1 + run.start;
        assert!(keys.check_layout(&cell).unwrap().is_canonical());

        let mut changed = cell.clone();
        changed[start + 2..start + 5].copy_from_slice("風".as_bytes());
        let end = start + 2 + text.len();
        changed[end - 3..end].copy_from_slice("雨".as_bytes());
        assert!(keys.check_layout(&changed).unwrap().is_canonical());

        let mut shortened = cell.clone();
        shortened[start] -= 1;
        assert!(BinaryJSON::check_encoded_layout(&shortened).is_canonical());
        assert!(keys.check_layout(&shortened).unwrap().is_canonical());

        let mut broken = cell;
        // The next length begins with a continuation byte. It cannot supply
        // the missing byte of this string's final code point.
        broken[start + 2 + text.len() - 1] = 0xc2;
        assert!(!BinaryJSON::check_encoded_layout(&broken).is_valid());
        assert!(!keys.check_layout(&broken).unwrap().is_valid());
    }

    #[test]
    fn checked_packed_long_strings_keep_a_run_when_widths_differ() {
        let text = |index: usize| format!("{}0123", "雪語".repeat(25 + index % 3));
        let members = (0..48)
            .map(|index| format!(r#""k{index:02}":"{}""#, text(index)))
            .collect::<Vec<_>>()
            .join(",");
        let cell = BinaryJSON::parse(&format!("{{{members}}}")).unwrap().encoded();
        let mut keys = CheckedJSONKeys::new(&cell).unwrap();
        let Some([PayloadCheck::Strings(run)]) = keys.value_checks.as_deref() else {
            panic!("expected one run of varying-width long strings");
        };
        assert_eq!(run.count(), 48);
        let start = 1 + run.start;
        assert!(keys.check_layout(&cell).unwrap().is_canonical());

        let second_length = start + 2 + text(0).len();
        let mut shortened = cell.clone();
        shortened[second_length] -= 1;
        assert!(BinaryJSON::check_encoded_layout(&shortened).is_canonical());
        assert!(keys.check_layout(&shortened).unwrap().is_canonical());

        let mut broken = cell;
        broken[second_length + 2 + text(1).len() - 1] = 0xc2;
        assert!(!BinaryJSON::check_encoded_layout(&broken).is_valid());
        assert!(!keys.check_layout(&broken).unwrap().is_valid());
    }

    #[test]
    fn scalar_string_checks_utf8_after_ascii_blocks() {
        let mut cell = vec![JSON_TYPE_CODE_STRING];
        encode_uvarint(513, &mut cell);
        let text_start = cell.len();
        cell.resize(text_start + 513, b'a');
        assert!(BinaryJSON::check_encoded_layout(&cell).is_canonical());

        for offset in [0, 127, 128, 129, 509] {
            let mut cell = cell.clone();
            let start = text_start + offset;
            cell[start..start + 4].copy_from_slice("🙂".as_bytes());
            assert!(BinaryJSON::check_encoded_layout(&cell).is_canonical());
            cell[start + 3] = 0xff;
            assert!(!BinaryJSON::check_encoded_layout(&cell).is_valid());
        }
        *cell.last_mut().unwrap() = 0xf0;
        assert!(!BinaryJSON::check_encoded_layout(&cell).is_valid());
    }

    #[test]
    #[cfg(target_arch = "aarch64")]
    fn mixed_pair_retry_preserves_utf8_at_a_window_boundary() {
        let peel = after_mixed_pairs::<false, true>;
        let pair = [0xc2, 0x80, 0xf4, 0x8f, 0xbf, 0xbf];
        let mut text = pair.repeat(170);
        text.extend_from_slice(b"tail");

        text[600..606].copy_from_slice("é値a".as_bytes());
        let (missed, remaining) = peel(&text).split_first_chunk::<6>().unwrap();
        assert!(std::str::from_utf8(missed).is_ok());
        assert!(remaining.first_chunk().is_some_and(valid_mixed_pair::<false>));
        assert!(validate_mixed_text::<true>(&text, peel));
        for index in [601, 1020] {
            let mut invalid = text.clone();
            invalid[index] = 0xff;
            assert!(!validate_mixed_text::<true>(&invalid, peel));
        }

        text[600..606].copy_from_slice(&pair);
        text[618..636].copy_from_slice("値🙂🙂🙂読".as_bytes());
        let (missed, _) = peel(&text).split_first_chunk::<6>().unwrap();
        assert!(std::str::from_utf8(missed).is_err());
        assert!(std::str::from_utf8(&text).is_ok());
        assert!(validate_mixed_text::<true>(&text, peel));
    }

    #[test]
    #[cfg(target_arch = "aarch64")]
    fn mixed_pair_batches_check_both_alignments_and_stop_before_invalid_input() {
        use zerocopy::IntoBytes;

        let mut words = [0_u16; 195];
        let bytes = words.as_mut_bytes();
        let pair = [0xc2, 0x80, 0xf4, 0x8f, 0xbf, 0xbf];
        for cell in bytes[..384].chunks_exact_mut(6) {
            cell.copy_from_slice(&pair);
        }
        bytes[384..388].copy_from_slice(b"tail");
        let text = &mut bytes[..388];
        assert_eq!(after_mixed_pair_batches(text), b"tail");
        assert_eq!(after_mixed_pairs::<false, true>(text), b"tail");

        text[193] = 0xff;
        assert_eq!(after_mixed_pair_batches(text), &text[192..]);
        assert!(std::str::from_utf8(after_mixed_pairs::<false, true>(text)).is_err());
        text[193] = 0x80;

        bytes.copy_within(0..388, 1);
        let unaligned = &bytes[1..389];
        assert_eq!(after_mixed_pair_batches(unaligned), b"tail");
        assert_eq!(after_mixed_pair_batches(&unaligned[..384]), &unaligned[192..384]);
        assert_eq!(after_mixed_pairs::<false, true>(unaligned), b"tail");

        for index in [192, 383] {
            let original = bytes[1 + index];
            bytes[1 + index] = 0x7f;
            let unaligned = &bytes[1..389];
            assert_eq!(after_mixed_pair_batches(unaligned), &unaligned[192..]);
            assert!(std::str::from_utf8(after_mixed_pairs::<false, true>(unaligned)).is_err());
            bytes[1 + index] = original;
        }
    }

    #[test]
    #[cfg(target_arch = "aarch64")]
    fn packed_mixed_width_prefixes_prove_complete_characters() {
        fn check<const FOUR_FIRST: bool>() {
            for (repeated, range_edges) in [(false, false), (true, false), (false, true)] {
                let peel: fn(&[u8]) -> &[u8] = if repeated {
                    after_repeated_pair::<FOUR_FIRST>
                } else {
                    after_mixed_pairs::<FOUR_FIRST, false>
                };
                let mut text = String::new();
                for index in 0..25 {
                    let delta = if repeated { 0 } else { index };
                    let two = char::from_u32(0x100 + delta).unwrap();
                    let four = if range_edges {
                        if index % 2 == 0 { '\u{10000}' } else { '\u{10ffff}' }
                    } else {
                        char::from_u32(0x1f600 + delta).unwrap()
                    };
                    if FOUR_FIRST {
                        text.push(four);
                        text.push(two);
                    } else {
                        text.push(two);
                        text.push(four);
                    }
                }
                text.push_str("tail");
                let text = text.into_bytes();
                assert_eq!(peel(&text), b"tail");
                for index in [0, 1, 2, 3, 4, 5, 47, 48, 95, 96, 144, 149, 153] {
                    for byte in [0, 0x7f, 0x80, 0x8f, 0xc0, 0xf4, 0xf5, 0xff] {
                        let mut changed = text.clone();
                        changed[index] = byte;
                        assert_eq!(
                            validate_mixed_text::<false>(&changed, peel),
                            std::str::from_utf8(&changed).is_ok(),
                        );
                    }
                }
            }
        }
        check::<false>();
        check::<true>();
    }

    #[test]
    #[cfg(target_arch = "aarch64")]
    fn checked_wide_strings_validate_mixed_utf8_after_the_three_byte_prefix() {
        let members = (0..40)
            .map(|index| {
                let ch = char::from_u32(0x4e00 + index).unwrap();
                let text = ch.to_string().repeat(50);
                format!(r#""k{index:02}":"{text}0123""#)
            })
            .collect::<Vec<_>>()
            .join(",");
        let cell = BinaryJSON::parse(&format!("{{{members}}}")).unwrap().encoded();
        let mut keys = CheckedJSONKeys::new(&cell).unwrap();
        let Some([PayloadCheck::Strings(run)]) = keys.value_checks.as_deref() else {
            panic!("expected a packed long-string run");
        };
        let PackedStringLengths::Wide(wide) = &run.lengths else {
            panic!("expected wide string lengths");
        };
        assert!(matches!(wide.text, WideStringText::ThreeBytePrefix));
        let start = 1 + run.start + 2;
        for offset in [0, 15, 30, 144] {
            for replacement in [
                [0xe4, 0x80, 0x80, 0xeb, 0xbf, 0xbf],
                [0xf0, 0x9f, 0x99, 0x82, b'x', b'y'],
                [0xe0, 0x80, 0x80, b'x', b'y', b'z'],
                [0xed, 0xa0, 0x80, b'x', b'y', b'z'],
                [0xe4, 0x80, 0x7f, b'x', b'y', b'z'],
            ] {
                let mut changed = cell.clone();
                changed[start + offset..start + offset + replacement.len()]
                    .copy_from_slice(&replacement);
                let general = BinaryJSON::check_encoded_layout(&changed);
                let cached = keys.check_layout(&changed).unwrap();
                assert_eq!(cached.is_valid(), general.is_valid());
                assert_eq!(cached.is_canonical(), general.is_canonical());
            }
        }
        let mut broken = cell;
        broken[start + 153] = 0xc2;
        assert!(!keys.check_layout(&broken).unwrap().is_valid());
    }

    #[test]
    fn packed_long_strings_learn_a_later_prefix_and_keep_field_bounds() {
        let common = "é🙂".repeat(25);
        let record = |shared| {
            let members = (0..40)
                .map(|index| {
                    let text = if shared {
                        common.clone()
                    } else {
                        char::from_u32(0x4e00 + index).unwrap().to_string().repeat(50)
                    };
                    format!(r#""k{index:02}":"{text}0123""#)
                })
                .collect::<Vec<_>>()
                .join(",");
            BinaryJSON::parse(&format!("{{{members}}}")).unwrap().encoded()
        };
        let original = record(false);
        let later = record(true);
        let mut keys = CheckedJSONKeys::new(&original).unwrap();
        for _ in 0..24 {
            assert!(keys.check_layout(&original).unwrap().is_canonical());
        }
        for row in 0..32 {
            let input = if row % 2 == 0 { &later } else { &original };
            assert!(keys.check_layout(input).unwrap().is_canonical());
        }
        let Some([PayloadCheck::Strings(run)]) = keys.value_checks.as_deref() else {
            panic!("expected a packed long-string run");
        };
        let PackedStringLengths::Wide(wide) = &run.lengths else {
            panic!("expected wide string lengths");
        };
        assert_eq!(wide.adaptive_prefix.as_ref().unwrap().prefix.as_deref(), Some(common.as_bytes()));
        let start = 1 + run.start;
        assert!(keys.check_layout(&original).unwrap().is_canonical());

        let width = 2 + common.len() + 4;
        let second = start + width;
        let mut mixed = later.clone();
        mixed[second..second + width].copy_from_slice(&original[second..second + width]);
        assert!(keys.check_layout(&mixed).unwrap().is_canonical());
        mixed[second + width - 1] = 0xc2;
        assert!(!keys.check_layout(&mixed).unwrap().is_valid());

        let mut shorter = later.clone();
        shorter[start] -= 1;
        assert!(BinaryJSON::check_encoded_layout(&shorter).is_canonical());
        assert!(keys.check_layout(&shorter).unwrap().is_canonical());

        let mut broken = later;
        broken[start + 2 + common.len() + 3] = 0xc2;
        assert!(!keys.check_layout(&broken).unwrap().is_valid());
    }

    #[test]
    fn packed_integer_objects_check_directory_tails_and_sorted_ascii_keys() {
        for count in [16, 17, 18] {
            for width in 1..=8 {
                let padding = "x".repeat(width - 1);
                let keys = (0..count)
                    .map(|index| format!(r#""{padding}\u{:04x}":{index}"#, b'A' as usize + index))
                    .collect::<Vec<_>>()
                    .join(",");
                let cell = BinaryJSON::parse(&format!("{{{keys}}}")).unwrap().encoded();
                let value_entries = 1 + HEADER_SIZE + count * KEY_ENTRY_SIZE;
                let key_start = value_entries + count * VALUE_ENTRY_SIZE;
                let (layout, prefix) = BinaryJSON::check_flat_object_layout(&cell).unwrap();
                assert!(layout.is_canonical());
                assert_eq!(prefix, Some(key_start + count * width));

                let mut unordered = cell.clone();
                unordered.swap(key_start + width - 1, key_start + 2 * width - 1);
                assert!(BinaryJSON::check_flat_object_layout(&unordered).is_none());
                let layout = BinaryJSON::check_encoded_layout(&unordered);
                assert!(layout.is_valid() && !layout.is_canonical());

                let mut invalid_key = cell.clone();
                invalid_key[key_start + count * width - 1] = 0xff;
                assert!(BinaryJSON::check_flat_object_layout(&invalid_key).is_none());
                assert!(!BinaryJSON::check_encoded_layout(&invalid_key).is_valid());

                let mut invalid_offset = cell;
                let last_offset = value_entries + (count - 1) * VALUE_ENTRY_SIZE + 1;
                invalid_offset[last_offset..last_offset + 4].copy_from_slice(&u32::MAX.to_le_bytes());
                assert!(BinaryJSON::check_flat_object_layout(&invalid_offset).is_none());
                assert!(!BinaryJSON::check_encoded_layout(&invalid_offset).is_valid());
            }
        }
    }

    #[test]
    fn flat_object_validation_checks_mixed_scalars_and_key_order() {
        let cell = BinaryJSON::parse(r#"{"a":0,"a\u0000":"ok","abcdef001":1.25,"z":false}"#)
            .unwrap()
            .encoded();
        let (layout, prefix) = BinaryJSON::check_flat_object_layout(&cell).unwrap();
        assert!(layout.is_valid() && layout.is_canonical());
        assert_eq!(prefix, None);

        let value_entries = 1 + HEADER_SIZE + 4 * KEY_ENTRY_SIZE;
        let entry = |index: usize| value_entries + index * VALUE_ENTRY_SIZE;
        let value_offset = |index| {
            1 + value_entry_offset(cell[entry(index)..entry(index) + VALUE_ENTRY_SIZE].try_into().unwrap())
        };
        let mut invalid_text = cell.clone();
        invalid_text[value_offset(1) + 1] = 0xff;
        let mut invalid_float = cell.clone();
        invalid_float[value_offset(2)..value_offset(2) + 8].copy_from_slice(&f64::INFINITY.to_le_bytes());
        let mut invalid_literal = cell;
        invalid_literal[entry(3) + 1] = 0xff;
        for invalid in [invalid_text, invalid_float, invalid_literal] {
            assert!(BinaryJSON::check_flat_object_layout(&invalid).is_none());
            assert!(!BinaryJSON::check_encoded_layout(&invalid).is_valid());
        }
    }

    #[test]
    fn decimal_text_rounds_to_go_binary64_before_serializing() {
        let binary = BinaryJSON::parse("-1.0080000000000001e-12").unwrap();
        assert_eq!(binary.to_string(), "-0.0000000000010080000000000001");
    }

    #[test]
    fn unicode_line_separators_can_cross_escape_scan_blocks() {
        let text = "123456789012345\u{2028}€\u{2029}\"\n";
        assert_eq!(
            json_marshal_string_to(text),
            r#""123456789012345\u2028€\u2029\"\n""#,
        );
        for width in [0, 15, 80, 96] {
            let prefix = "x".repeat(width);
            let value = BinaryJSON::from_typed_value(&BinaryJSONValue::String(format!(
                "{prefix}\u{2028}€\u{2029}\"\n"
            )))
            .unwrap();
            assert_eq!(
                value.to_string(),
                format!(r#""{prefix}\u2028€\u2029\"\n""#)
            );
        }
    }

    #[test]
    fn owned_unquote_validates_and_copies_across_a_utf8_block_boundary() {
        let text = format!("{}🦀tail", "a".repeat(JSON_TEXT_COPY_BLOCK - 1));
        let value = BinaryJSON::from_typed_value(&BinaryJSONValue::String(text.clone())).unwrap();
        assert_eq!(value.unquote().unwrap(), text);

        let mut invalid = value.value().to_vec();
        *invalid.last_mut().unwrap() = 0xff;
        let value = BinaryJSON::from_encoded_parts(JSON_TYPE_CODE_STRING, invalid);
        assert_eq!(value.unquote(), Err(BinaryJSONError::InvalidBinary));
    }

    /// Go's zero `types.BinaryJSON{}` renders as the empty string
    /// (`String()` swallows its marshal error).
    #[test]
    fn zero_value_renders_empty() {
        assert_eq!(BinaryJSON::zero().to_string(), "");
        assert_eq!(BinaryJSON::zero().type_code(), 0);
    }

    /// `expression/json.test`'s 100-deep documents. Each decoded element was
    /// validated by fully decoding it and then decoded again, doubling the
    /// work per level: a 20-deep document took 0.7s and a 100-deep one never
    /// finished, where Go's `valEntryGet` slices each element once.
    #[test]
    fn a_document_at_the_maximum_depth_decodes_in_linear_time() {
        let text = format!("{}1{}", "[".repeat(100), "]".repeat(100));
        let binary = BinaryJSON::parse(&text).unwrap();
        assert_eq!(binary.element_depth().unwrap(), 101);
        assert_eq!(binary.to_string(), text);
        assert_eq!(
            BinaryJSON::from_node(&binary.to_node().unwrap()).unwrap(),
            binary
        );
        let started = std::time::Instant::now();
        for _ in 0..100 {
            binary.to_node().unwrap();
        }
        assert!(started.elapsed() < std::time::Duration::from_secs(5));
    }

    #[test]
    fn test_binary_json_marshal_unmarshal() {
        for text in [
            "null",
            "true",
            "false",
            "-1",
            "18446744073709551615",
            "1.5",
            "\"hello\"",
            "[1, true, \"x\", {\"b\": 2, \"a\": null}]",
            "{\"b\": [2, 3], \"a\": 1}",
        ] {
            let binary = BinaryJSON::parse(text).unwrap();
            let decoded = binary.to_value().unwrap();
            let rebuilt = BinaryJSON::from_value(&decoded).unwrap();
            assert_eq!(binary, rebuilt, "{text}");
        }
    }

    #[test]
    fn test_parse_binary_from_string() {
        assert_eq!(
            BinaryJSON::parse("").unwrap_err().to_string(),
            "The document is empty"
        );
        assert_eq!(
            BinaryJSON::parse(r#""a"""#).unwrap_err().to_string(),
            "The document root must not be followed by other values."
        );
    }

    #[test]
    fn test_binary_json_string_and_types() {
        assert_eq!(
            BinaryJSON::parse("{\"b\":2,\"a\":[1,true]}")
                .unwrap()
                .to_string(),
            "{\"a\": [1, true], \"b\": 2}"
        );
        assert_eq!(BinaryJSON::parse("-2").unwrap().as_i64(), Some(-2));
        assert_eq!(
            BinaryJSON::from_value(&Value::Number(Number::from(u64::MAX)))
                .unwrap()
                .as_u64(),
            Some(u64::MAX)
        );
        // Go parses a number past i64::MAX via strconv.ParseUint and stores it
        // as JSONTypeCodeUint64 (pkg/types/json_binary.go:894-911), so
        // JSON_TYPE reports UNSIGNED INTEGER -- the live builtin's ported Go
        // suite asserts the same (tidb-session tests_json). This test said
        // DOUBLE because the pre-fix parser rewrote such numbers to f64 before
        // encoding; that was the divergence, not the contract.
        assert_eq!(
            BinaryJSON::parse("18446744073709551615")
                .unwrap()
                .type_name()
                .unwrap(),
            "UNSIGNED INTEGER"
        );
        assert_eq!(
            BinaryJSON::parse("\"TiDB\"").unwrap().as_string(),
            Some(&b"TiDB"[..])
        );

        for (value, expected) in [
            (123_456_789.123_456_7, "123456789.1234567"),
            (0.000_000_01, "0.00000001"),
            (1e-20, "1e-20"),
            (1e15, "1e15"),
            (1e14, "100000000000000.0"),
            (16_777_215.996_093_75, "16777215.99609375"),
            (-1.003_906_25, "-1.00390625"),
            (9.0, "9.0"),
            (-0.0, "-0.0"),
        ] {
            let value =
                BinaryJSON::from_value(&Value::Number(Number::from_f64(value).unwrap())).unwrap();
            assert_eq!(value.to_string(), expected);
            let array =
                BinaryJSON::from_value(&Value::Array(vec![value.to_value().unwrap()])).unwrap();
            assert_eq!(array.to_string(), format!("[{expected}]"));
        }

        let opaque = Opaque {
            type_code: 233,
            bytes: vec![1, 2, 3],
        };
        let input = BinaryJSONValue::Object(BTreeMap::from([(
            "values".to_owned(),
            BinaryJSONValue::Array(vec![
                BinaryJSONValue::Bool(true),
                BinaryJSONValue::Opaque(opaque.clone()),
            ]),
        )]));
        let value = BinaryJSON::from_typed_value(&input).unwrap();
        assert_eq!(
            value.to_node().unwrap(),
            JSONNode::Object(vec![(
                "values".to_owned(),
                JSONNode::Array(vec![
                    JSONNode::Scalar(literal(JSON_LITERAL_TRUE)),
                    JSONNode::Scalar(BinaryJSON::from_opaque(opaque)),
                ]),
            )])
        );
    }

    #[test]
    fn test_create_binary() {
        for (input, expected_type) in [
            (BinaryJSONValue::Int64(1_i64 << 62), JSON_TYPE_CODE_INT64),
            (
                BinaryJSONValue::Float64(123_456_789.123_456_7),
                JSON_TYPE_CODE_FLOAT64,
            ),
            (
                BinaryJSONValue::Float64(0.000_000_01),
                JSON_TYPE_CODE_FLOAT64,
            ),
            (
                BinaryJSONValue::Number(u64::MAX.to_string()),
                JSON_TYPE_CODE_UINT64,
            ),
        ] {
            let value = BinaryJSON::from_typed_value(&input).unwrap();
            assert_eq!(value.type_code(), expected_type);
            assert_eq!(
                BinaryJSON::calculate_typed_size(&input).unwrap(),
                value.value().len()
            );
        }

        let original = BinaryJSON::from_typed_value(&BinaryJSONValue::Float64(1e-20)).unwrap();
        let copied =
            BinaryJSON::from_typed_value(&BinaryJSONValue::Binary(original.clone())).unwrap();
        assert_eq!(copied.type_code(), original.type_code());
        assert_eq!(copied.value(), original.value());

        // Go accepts `any`, so its source test can pass int8 and assert the
        // dynamic unknown-type panic. Rust accepts the closed BinaryJSONValue
        // enum instead; an unlisted dynamic input cannot reach this function.
    }

    #[test]
    fn typed_binary_preserves_valid_wire_padding_like_go() {
        // [4] with one padding byte between the value entry and the integer.
        let payload = vec![
            1, 0, 0, 0, 22, 0, 0, 0, 9, 14, 0, 0, 0, 0xfe, 4, 0, 0, 0, 0, 0, 0, 0,
        ];
        let input = BinaryJSON::from_encoded_parts(JSON_TYPE_CODE_ARRAY, payload.clone());
        let input = BinaryJSONValue::Binary(input);
        let direct = BinaryJSON::from_typed_value(&input).unwrap();
        assert_eq!(direct.value(), payload);
        let outer = BinaryJSON::from_typed_value(&BinaryJSONValue::Array(vec![input])).unwrap();
        assert_eq!(outer.array_get(0).unwrap().unwrap().value(), payload);
        assert_eq!(outer.to_string(), "[[4]]");
    }

    #[test]
    fn checked_integer_array_rejects_invalid_last_entry() {
        let value = BinaryJSON::parse("[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15]").unwrap();
        let raw = BinaryJSON::from_encoded_parts(value.type_code(), value.value());
        assert_eq!(raw.element_depth(), Ok(2));

        let mut bytes = raw.value().to_vec();
        let last = HEADER_SIZE + 15 * VALUE_ENTRY_SIZE;
        bytes[last + 1..last + VALUE_ENTRY_SIZE].copy_from_slice(&u32::MAX.to_le_bytes());
        let malformed = BinaryJSON::from_encoded_parts(JSON_TYPE_CODE_ARRAY, bytes);
        assert_eq!(
            malformed.element_depth(),
            Err(BinaryJSONError::InvalidBinary)
        );
    }

    #[test]
    fn layout_checks_a_different_sibling_after_identical_containers() {
        let text = "界".repeat(24);
        let child = format!(r#"{{"text":"{text}"}}"#);
        let document = BinaryJSON::parse(&format!("[{}]", vec![child; 9].join(","))).unwrap();
        let valid = BinaryJSON::from_encoded_parts(document.type_code(), document.value());
        assert_eq!(valid.element_depth(), Ok(3));

        let mut bytes = document.value().to_vec();
        let index = bytes
            .windows(text.len())
            .rposition(|bytes| bytes == text.as_bytes())
            .unwrap();
        bytes[index + text.len() / 2] = 0xff;
        let invalid = BinaryJSON::from_encoded_parts(document.type_code(), bytes);
        assert_eq!(invalid.element_depth(), Err(BinaryJSONError::InvalidBinary));
    }

    #[test]
    fn typed_binary_checks_depth_and_invalid_payload_before_reuse() {
        let invalid = BinaryJSONValue::Binary(BinaryJSON::from_encoded_parts(
            JSON_TYPE_CODE_STRING,
            vec![1, 0xff],
        ));
        let invalid = BinaryJSONValue::Array(vec![invalid]);
        assert_eq!(
            BinaryJSON::from_typed_value(&invalid),
            Err(BinaryJSONError::InvalidBinary),
        );
        assert_eq!(
            BinaryJSON::calculate_typed_size(&invalid),
            Err(BinaryJSONError::InvalidBinary),
        );

        let text = format!(
            "{}0{}",
            "[".repeat(MAX_JSON_DEPTH),
            "]".repeat(MAX_JSON_DEPTH)
        );
        let parsed = BinaryJSON::parse(&text).unwrap();
        let raw = BinaryJSON::from_encoded_parts(parsed.type_code(), parsed.value());
        for document in [parsed, raw] {
            let input = BinaryJSONValue::Binary(document);
            assert!(BinaryJSON::from_typed_value(&input).is_ok());
            let input = BinaryJSONValue::Array(vec![input]);
            assert_eq!(
                BinaryJSON::from_typed_value(&input),
                Err(BinaryJSONError::TooDeep),
            );
            assert_eq!(
                BinaryJSON::calculate_typed_size(&input),
                Err(BinaryJSONError::TooDeep),
            );
        }
    }

    #[test]
    fn typed_wire_size_with_normalized_children() {
        let unsorted = |text| {
            let parsed = BinaryJSON::parse(text).unwrap();
            let mut bytes = parsed.value().to_vec();
            for (start, width) in [
                (HEADER_SIZE, KEY_ENTRY_SIZE),
                (HEADER_SIZE + 2 * KEY_ENTRY_SIZE, VALUE_ENTRY_SIZE),
            ] {
                bytes[start..start + 2 * width].rotate_left(width);
            }
            BinaryJSONValue::Binary(BinaryJSON::from_encoded_parts(parsed.type_code(), bytes))
        };
        let string = "é".repeat(64);
        let input = BinaryJSONValue::Array(vec![
            unsorted(r#"{"a":[1],"b":true}"#),
            BinaryJSONValue::Binary(BinaryJSON::parse("false").unwrap()),
            BinaryJSONValue::Object(BTreeMap::from([
                ("str".to_owned(), BinaryJSONValue::String(string.clone())),
                ("obj".to_owned(), unsorted(r#"{"c":[],"value":3}"#)),
            ])),
        ]);
        let expected = BinaryJSON::parse(&format!(
            r#"[{{"a":[1],"b":true}},false,{{"obj":{{"c":[],"value":3}},"str":"{string}"}}]"#,
        ))
        .unwrap();
        assert_eq!(BinaryJSON::from_typed_value(&input).unwrap(), expected);
        assert_eq!(
            BinaryJSON::calculate_typed_size(&input).unwrap(),
            expected.value().len(),
        );
    }

    #[test]
    fn json_number_uint64_uses_unsigned_storage_like_go() {
        let value =
            BinaryJSON::from_typed_value(&BinaryJSONValue::Number(u64::MAX.to_string())).unwrap();

        assert_eq!(value.type_code(), JSON_TYPE_CODE_UINT64);
        assert_eq!(value.as_u64(), Some(u64::MAX));
        assert_eq!(value.to_string(), u64::MAX.to_string());
    }

    #[test]
    fn test_decode_escaped_unicode() {
        for (input, expected, size, surrogate, valid) in [
            ("597d", "好\0", 3, false, true),
            ("fffd", "�\0", 3, false, true),
            ("D83DDE0A", "😊", 4, false, true),
            // Go utf16.DecodeRune substitutes U+FFFD for a lone surrogate or an
            // invalid pair instead of failing.
            ("D83D", "\u{fffd}\0", 3, false, true),
            ("D83DD83D", "\u{fffd}\0", 3, false, true),
            ("D83D11", "", 0, false, false),
            ("ZZZZ", "", 0, false, false),
            ("D83DDE0A597d", "", 0, false, false),
        ] {
            let result = decode_escaped_unicode(input.as_bytes());
            assert_eq!(result.is_ok(), valid, "{input}");
            assert_eq!(
                matches!(&result, Err(BinaryJSONError::LoneSurrogate)),
                surrogate,
                "{input}"
            );
            if let Ok((bytes, actual_size, _)) = result {
                assert_eq!(std::str::from_utf8(&bytes).unwrap(), expected, "{input}");
                assert_eq!(actual_size, size, "{input}");
            }
        }
    }

    #[test]
    fn test_unquote_json_string() {
        for (input, expected, valid) in [
            ("\\b", "\u{8}", true),
            ("\\f", "\u{c}", true),
            ("\\n", "\n", true),
            ("\\r", "\r", true),
            ("\\t", "\t", true),
            ("\\\\", "\\", true),
            ("\\u597d", "好", true),
            ("0\\u597d0", "0好0", true),
            ("\\a", "a", true),
            ("[", "[", true),
            ("\\ud83e\\udd21", "🤡", true),
            ("\\ufffd", "�", true),
            ("\\", "", false),
            ("\\u59", "", false),
        ] {
            let result = unquote_json_string(input);
            assert_eq!(result.is_ok(), valid, "{input}");
            if let Ok(result) = result {
                assert_eq!(result, expected, "{input}");
            }
        }
    }

    /// Complete translation of `pkg/types/json_binary_test.go::TestQuoteString`.
    #[test]
    fn test_quote_string() {
        for (raw, quoted) in [
            ("3", "\"3\""),
            (
                "hello, \"escaped quotes\" world",
                r#""hello, \"escaped quotes\" world""#,
            ),
            ("你", "\"你\""),
            ("true", "true"),
            ("null", "null"),
            ("\"", r#""\"""#),
            ("'", "\"'\""),
            ("''", "\"''\""),
            ("", "\"\""),
            ("\\ \" \u{8} \u{c} \n \r \t", r#""\\ \" \b \f \n \r \t""#),
            ("\u{2028}\u{2029}", r#""\u2028\u2029""#),
        ] {
            assert_eq!(quote_json_string(raw), quoted, "{raw:?}");
        }
    }

    /// Go `jsonMarshalStringTo` escapes LINE SEPARATOR and PARAGRAPH SEPARATOR
    /// in scalar values and object keys so rendered JSON is safe for JSONP.
    #[test]
    fn json_text_escapes_line_and_paragraph_separators_like_go() {
        let scalar = BinaryJSON::parse(r#""\u2028\u2029""#).unwrap();
        assert_eq!(scalar.to_string(), r#""\u2028\u2029""#);

        let object = BinaryJSON::parse(r#"{"\u2028":"\u2029"}"#).unwrap();
        assert_eq!(object.to_string(), r#"{"\u2028": "\u2029"}"#);
    }

    /// Complete translation of
    /// `pkg/types/json_binary_functions_test.go::TestBinaryCompare`.
    #[test]
    fn test_binary_compare() {
        assert_eq!(
            compare_binary_json(
                &BinaryJSON::from_value(&Value::String("a".to_owned())).unwrap(),
                &BinaryJSON::from_value(&Value::String("b".to_owned())).unwrap()
            ),
            Ordering::Less
        );

        let long = BinaryJSON::from_opaque(Opaque {
            type_code: 0,
            bytes: vec![0, 1, 2, 3],
        });
        let short = BinaryJSON::from_opaque(Opaque {
            type_code: 0,
            bytes: vec![0, 1, 2],
        });
        assert_eq!(compare_binary_json(&long, &short), Ordering::Greater);
        assert_eq!(long.type_name().unwrap(), "OPAQUE");
        assert_eq!(long.opaque().unwrap().bytes, vec![0, 1, 2, 3]);

        assert_eq!(
            compare_binary_json(
                &BinaryJSON::from_opaque(Opaque {
                    type_code: 0,
                    bytes: vec![0, 1, 2, 3],
                }),
                &BinaryJSON::from_opaque(Opaque {
                    type_code: 0,
                    bytes: vec![0, 2, 1],
                }),
            ),
            Ordering::Less
        );
        assert_eq!(
            compare_binary_json(
                &BinaryJSON::from_typed_value(&BinaryJSONValue::String("test".to_owned())).unwrap(),
                &BinaryJSON::from_opaque(Opaque {
                    type_code: 0,
                    bytes: vec![0, 2, 1],
                }),
            ),
            Ordering::Less
        );
    }

    /// Complete translation of `pkg/types/json_binary_test.go::TestCompareBinary`.
    #[test]
    fn test_compare_binary() {
        let int = |value| BinaryJSON::from_value(&Value::Number(Number::from(value))).unwrap();
        let uint = |value| BinaryJSON::from_value(&Value::Number(Number::from(value))).unwrap();
        let real = |value| {
            BinaryJSON::from_value(&Value::Number(Number::from_f64(value).unwrap())).unwrap()
        };
        for (left, right, expected) in [
            (int(-1), uint(u64::MAX), Ordering::Less),
            (
                int(922_337_203_685_477_580),
                int(922_337_203_685_477_580),
                Ordering::Equal,
            ),
            (
                int(922_337_203_685_477_580),
                int(922_337_203_685_477_581),
                Ordering::Less,
            ),
            (
                int(922_337_203_685_477_581),
                int(922_337_203_685_477_580),
                Ordering::Greater,
            ),
            (
                int(922_337_203_685_477_580),
                uint(922_337_203_685_477_581),
                Ordering::Less,
            ),
            (int(2), uint(1), Ordering::Greater),
            (int(i64::MAX), uint(i64::MAX as u64), Ordering::Equal),
            (uint(u64::MAX), int(-1), Ordering::Greater),
            (
                uint(922_337_203_685_477_581),
                int(922_337_203_685_477_580),
                Ordering::Greater,
            ),
            (uint(1), int(2), Ordering::Less),
            (uint(i64::MAX as u64), int(i64::MAX), Ordering::Equal),
            (real(9.0), int(9), Ordering::Equal),
            (real(8.9), int(9), Ordering::Less),
            (real(9.1), int(9), Ordering::Greater),
            (real(9.0), uint(9), Ordering::Equal),
            (real(8.9), uint(9), Ordering::Less),
            (real(9.1), uint(9), Ordering::Greater),
            (int(9), real(9.0), Ordering::Equal),
            (int(9), real(8.9), Ordering::Greater),
            (int(9), real(9.1), Ordering::Less),
            (uint(9), real(9.0), Ordering::Equal),
            (uint(9), real(8.9), Ordering::Greater),
            (uint(9), real(9.1), Ordering::Less),
        ] {
            assert_eq!(compare_binary_json(&left, &right), expected);
        }

        assert_eq!(
            compare_binary_json(&int(3), &uint(1_u64 << 63)),
            Ordering::Less,
            "3 < uint64(1<<63)"
        );

        for (left, right) in [
            ("null", "3"),
            ("3", r#""hello""#),
            (r#""hello""#, r#""hello, world""#),
            (r#""hello, world""#, r#"{"a":"b"}"#),
            (r#"{"a":"b"}"#, r#"["a","b"]"#),
            (r#"["a","b"]"#, r#"["a","c"]"#),
            (r#"["a","c"]"#, "false"),
            ("false", "true"),
        ] {
            assert_eq!(
                compare_binary_json(
                    &BinaryJSON::parse(left).unwrap(),
                    &BinaryJSON::parse(right).unwrap()
                ),
                Ordering::Less,
                "{left} < {right}"
            );
        }
    }

    #[test]
    fn stored_depth_checks_entry_bounds_before_the_depth_limit() {
        let wrap = |type_code, value: &[u8]| {
            let offset = HEADER_SIZE + VALUE_ENTRY_SIZE;
            let mut payload = vec![0; offset];
            payload[HEADER_SIZE] = type_code;
            payload[HEADER_SIZE + 1..offset].copy_from_slice(&(offset as u32).to_le_bytes());
            payload.extend_from_slice(value);
            write_header(&mut payload, 1).unwrap();
            payload
        };
        let mut valid = vec![0; HEADER_SIZE];
        write_header(&mut valid, 0).unwrap();
        let mut invalid = wrap(0xff, &[]);
        for _ in 0..MAX_JSON_DEPTH {
            valid = wrap(JSON_TYPE_CODE_ARRAY, &valid);
            invalid = wrap(JSON_TYPE_CODE_ARRAY, &invalid);
        }

        let too_deep = wrap(JSON_TYPE_CODE_ARRAY, &valid);
        assert_eq!(
            BinaryJSON::from_encoded_parts(JSON_TYPE_CODE_ARRAY, valid).element_depth(),
            Ok(MAX_JSON_DEPTH + 1)
        );
        assert_eq!(
            BinaryJSON::from_encoded_parts(JSON_TYPE_CODE_ARRAY, too_deep).element_depth(),
            Err(BinaryJSONError::TooDeep)
        );
        assert_eq!(
            BinaryJSON::from_encoded_parts(JSON_TYPE_CODE_ARRAY, invalid).element_depth(),
            Err(BinaryJSONError::InvalidBinary)
        );
    }

    #[test]
    fn binary_json_numeric_zero_matches_go() {
        for (value, expected) in [
            (BinaryJSONValue::Int64(0), true),
            (BinaryJSONValue::Uint64(0), true),
            (BinaryJSONValue::Int64(-1), false),
            (BinaryJSONValue::Float64(-0.0), true),
            (BinaryJSONValue::Float64(-FLOAT_EPSILON / 2.0), true),
            (BinaryJSONValue::Float64(FLOAT_EPSILON), false),
            (BinaryJSONValue::Float64(-FLOAT_EPSILON), false),
            (BinaryJSONValue::String("0".to_owned()), false),
            (BinaryJSONValue::Null, false),
        ] {
            assert_eq!(
                BinaryJSON::from_typed_value(&value).unwrap().is_zero(),
                expected
            );
        }
        assert!(!BinaryJSON::from_encoded_parts(JSON_TYPE_CODE_INT64, vec![0; 7]).is_zero());
        assert!(!BinaryJSON::from_encoded_parts(JSON_TYPE_CODE_UINT64, vec![0; 9]).is_zero());
        assert!(
            !BinaryJSON::from_encoded_parts(JSON_TYPE_CODE_FLOAT64, f64::NAN.to_le_bytes().to_vec())
                .is_zero()
        );
    }

    #[test]
    fn binary_json_temporal_array_comparison_uses_value_order() {
        let core = Time::from_date_checked(2024, 2, 29, 0, 0, 0, 1, TimeType::DateTime, 6)
            .unwrap()
            .core_time();
        let datetime = BinaryJSONValue::Time(Time::new(core, TimeType::DateTime, 6).unwrap());
        let timestamp = BinaryJSONValue::Time(Time::new(core, TimeType::Timestamp, 6).unwrap());
        let array = |time, nanoseconds, fsp| {
            BinaryJSON::from_typed_value(&BinaryJSONValue::Array(vec![
                time,
                BinaryJSONValue::Duration(MySqlDuration::from_raw_parts(nanoseconds, fsp)),
            ]))
            .unwrap()
        };

        let left = array(datetime, -1000, 6);
        let equal = array(timestamp.clone(), -1000, 3);
        let greater = array(timestamp, 1000, 6);
        assert_eq!(compare_binary_json(&left, &equal), Ordering::Equal);
        assert_eq!(compare_binary_json(&left, &greater), Ordering::Less);
    }

    #[test]
    fn binary_json_special_scalar_container_supplemental() {
        let time =
            Time::from_date_checked(2020, 2, 3, 4, 5, 6, 123_456, TimeType::DateTime, 3).unwrap();
        let encoded_time = BinaryJSON::from_time(time);
        assert_eq!(encoded_time.as_time(3).unwrap(), time);
        assert_eq!(encoded_time.to_string(), r#""2020-02-03 04:05:06.123456""#);

        let duration = MySqlDuration::new(12, 34, 56, 123_456, 3).unwrap();
        let encoded_duration = BinaryJSON::from_duration(duration);
        assert_eq!(encoded_duration.as_duration().unwrap(), duration);
        assert_eq!(encoded_duration.to_string(), r#""12:34:56.123456""#);

        let display = BinaryJSON::from_opaque(Opaque {
            type_code: 233,
            bytes: vec![b'9'],
        });
        let container = BinaryJSON::from_node(&JSONNode::Array(vec![
            JSONNode::Scalar(encoded_time),
            JSONNode::Scalar(encoded_duration),
            JSONNode::Scalar(display),
        ]))
        .unwrap();
        assert_eq!(
            container.to_string(),
            r#"["2020-02-03 04:05:06.123456", "12:34:56.123456", "base64:type233:OQ=="]"#
        );
        assert_eq!(
            BinaryJSON::from_raw(container.type_code(), container.value().to_vec()).unwrap(),
            container
        );
    }

    #[test]
    fn test_binary_json_opaque() {
        for (bytes, expected) in [
            (vec![b'9'], "\"base64:type233:OQ==\""),
            (
                vec![0; 128],
                "\"base64:type233:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=\"",
            ),
        ] {
            let value = BinaryJSON::from_opaque(Opaque {
                type_code: 233,
                bytes: bytes.clone(),
            });
            assert_eq!(value.opaque().unwrap().type_code, 233);
            assert_eq!(value.opaque().unwrap().bytes, bytes);
            assert_eq!(value.to_string(), expected);
        }
    }
}
