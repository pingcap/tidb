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

//! Compare and key authority for the collations needed by the scalar domain.
//!
//! The generated images are exact little-endian conversions of TiDB's Go
//! tables. Runtime keys remain the source-defined big-endian weight stream.

use std::cmp::Ordering;

use crate::Collation;

const GENERAL_CI: &[u8; 65_536 * 2] = include_bytes!("collation_data/general_ci_u16_le.bin");
const UNICODE_0400: &[u8; 65_536 * 8] = include_bytes!("collation_data/unicode_0400_u64_le.bin");
const UNICODE_0400_LONG: &[u8; 22 * 20] =
    include_bytes!("collation_data/unicode_0400_long_u64_le.bin");

impl Collation {
    /// Compares arbitrary Go-string bytes using TiDB's source semantics.
    pub fn compare(self, left: &[u8], right: &[u8]) -> Ordering {
        match self {
            Self::Binary | Self::AsciiBin | Self::Latin1Bin => left.cmp(right),
            Self::Utf8Bin | Self::Utf8Mb4Bin => {
                trim_trailing_spaces(left).cmp(trim_trailing_spaces(right))
            }
            Self::Utf8GeneralCi | Self::Utf8Mb4GeneralCi => general_ci_compare(left, right),
            Self::Utf8UnicodeCi | Self::Utf8Mb4UnicodeCi => unicode_0400_compare(left, right),
        }
    }

    /// Returns TiDB's sort key for arbitrary Go-string bytes.
    pub fn key(self, value: &[u8]) -> Vec<u8> {
        match self {
            Self::Binary | Self::AsciiBin | Self::Latin1Bin => value.to_vec(),
            Self::Utf8Bin | Self::Utf8Mb4Bin => trim_trailing_spaces(value).to_vec(),
            Self::Utf8GeneralCi | Self::Utf8Mb4GeneralCi => general_ci_key(value, true),
            Self::Utf8UnicodeCi | Self::Utf8Mb4UnicodeCi => unicode_0400_key(value, true),
        }
    }

    /// Returns the source key without the collation's PAD SPACE preprocessing.
    pub fn key_without_trim_right_space(self, value: &[u8]) -> Vec<u8> {
        match self {
            Self::Binary | Self::AsciiBin | Self::Latin1Bin | Self::Utf8Bin | Self::Utf8Mb4Bin => {
                value.to_vec()
            }
            Self::Utf8GeneralCi | Self::Utf8Mb4GeneralCi => general_ci_key(value, false),
            Self::Utf8UnicodeCi | Self::Utf8Mb4UnicodeCi => unicode_0400_key(value, false),
        }
    }

    /// Returns the exact upper bound exposed by the corresponding Go collator.
    pub fn max_key_len(self, value: &[u8]) -> usize {
        match self {
            Self::Binary | Self::AsciiBin | Self::Latin1Bin | Self::Utf8Bin | Self::Utf8Mb4Bin => {
                value.len()
            }
            Self::Utf8GeneralCi | Self::Utf8Mb4GeneralCi => go_rune_count(value) * 2,
            Self::Utf8UnicodeCi | Self::Utf8Mb4UnicodeCi => go_rune_count(value) * 16,
        }
    }
}

fn trim_trailing_spaces(mut value: &[u8]) -> &[u8] {
    while value.last() == Some(&b' ') {
        value = &value[..value.len() - 1];
    }
    value
}

pub(crate) fn decode_rune(value: &[u8]) -> Result<(u32, usize), ()> {
    let first = *value.first().ok_or(())?;
    if first < 0x80 {
        return Ok((u32::from(first), 1));
    }
    let width = match first {
        0xC2..=0xDF => 2,
        0xE0..=0xEF => 3,
        0xF0..=0xF4 => 4,
        _ => return Err(()),
    };
    let bytes = value.get(..width).ok_or(())?;
    let character = std::str::from_utf8(bytes)
        .map_err(|_| ())?
        .chars()
        .next()
        .ok_or(())?;
    Ok((character as u32, width))
}

pub(crate) fn go_rune_count(value: &[u8]) -> usize {
    let mut index = 0;
    let mut count = 0;
    while index < value.len() {
        index += decode_rune(&value[index..])
            .map(|(_, width)| width)
            .unwrap_or(1);
        count += 1;
    }
    count
}

fn general_weight(codepoint: u32) -> u16 {
    let codepoint = codepoint as usize;
    if codepoint > 0xFFFF {
        return 0xFFFD;
    }
    let offset = codepoint * 2;
    u16::from_le_bytes([GENERAL_CI[offset], GENERAL_CI[offset + 1]])
}

fn general_ci_compare(left: &[u8], right: &[u8]) -> Ordering {
    let (left, right) = (trim_trailing_spaces(left), trim_trailing_spaces(right));
    let (mut left_index, mut right_index) = (0, 0);
    while left_index < left.len() && right_index < right.len() {
        let (left_rune, left_width) = match decode_rune(&left[left_index..]) {
            Ok(decoded) => decoded,
            Err(()) => return Ordering::Equal,
        };
        let (right_rune, right_width) = match decode_rune(&right[right_index..]) {
            Ok(decoded) => decoded,
            Err(()) => return Ordering::Equal,
        };
        left_index += left_width;
        right_index += right_width;
        let ordering = general_weight(left_rune).cmp(&general_weight(right_rune));
        if !ordering.is_eq() {
            return ordering;
        }
    }
    (left.len() - left_index).cmp(&(right.len() - right_index))
}

fn general_ci_key(value: &[u8], trim: bool) -> Vec<u8> {
    let value = if trim {
        trim_trailing_spaces(value)
    } else {
        value
    };
    let mut key = Vec::with_capacity(value.len());
    let mut index = 0;
    while index < value.len() {
        let (codepoint, width) = match decode_rune(&value[index..]) {
            Ok(decoded) => decoded,
            Err(()) => break,
        };
        index += width;
        key.extend_from_slice(&general_weight(codepoint).to_be_bytes());
    }
    key
}

fn uca_weight(codepoint: u32) -> (u64, u64) {
    let codepoint = codepoint as usize;
    if codepoint > 0xFFFF {
        return (0xFFFD, 0);
    }
    let offset = codepoint * 8;
    let first = u64::from_le_bytes(
        UNICODE_0400[offset..offset + 8]
            .try_into()
            .expect("fixed UCA table width"),
    );
    if first != 0xFFFD {
        return (first, 0);
    }
    long_uca_weight(codepoint as u32)
        .expect("generated UCA 4.0 long-rune marker must have an expansion record")
}

fn long_uca_weight(codepoint: u32) -> Option<(u64, u64)> {
    let rune_at = |index: usize| {
        let offset = index * 20;
        u32::from_le_bytes(
            UNICODE_0400_LONG[offset..offset + 4]
                .try_into()
                .expect("fixed long-rune record width"),
        )
    };
    let (mut low, mut high) = (0_usize, 22_usize);
    while low < high {
        let middle = low + (high - low) / 2;
        match rune_at(middle).cmp(&codepoint) {
            Ordering::Less => low = middle + 1,
            Ordering::Greater => high = middle,
            Ordering::Equal => {
                low = middle;
                break;
            }
        }
    }
    (low < 22 && rune_at(low) == codepoint).then(|| {
        let index = low;
        let offset = index * 20 + 4;
        let first = u64::from_le_bytes(
            UNICODE_0400_LONG[offset..offset + 8]
                .try_into()
                .expect("fixed long-rune first weight"),
        );
        let second = u64::from_le_bytes(
            UNICODE_0400_LONG[offset + 8..offset + 16]
                .try_into()
                .expect("fixed long-rune second weight"),
        );
        (first, second)
    })
}

struct UcaCursor<'a> {
    bytes: &'a [u8],
    byte_index: usize,
    pending: [u16; 8],
    pending_index: usize,
    pending_len: usize,
}

impl<'a> UcaCursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self {
            bytes,
            byte_index: 0,
            pending: [0; 8],
            pending_index: 0,
            pending_len: 0,
        }
    }

    fn next_weight(&mut self) -> Result<Option<u16>, ()> {
        loop {
            if self.pending_index < self.pending_len {
                let weight = self.pending[self.pending_index];
                self.pending_index += 1;
                return Ok(Some(weight));
            }
            if self.byte_index == self.bytes.len() {
                return Ok(None);
            }
            let (codepoint, width) = decode_rune(&self.bytes[self.byte_index..])?;
            self.byte_index += width;
            self.pending_index = 0;
            self.pending_len = 0;
            let (first, second) = uca_weight(codepoint);
            self.append_packed(first);
            self.append_packed(second);
        }
    }

    fn append_packed(&mut self, mut packed: u64) {
        while packed != 0 {
            self.pending[self.pending_len] = packed as u16;
            self.pending_len += 1;
            packed >>= 16;
        }
    }
}

fn unicode_0400_compare(left: &[u8], right: &[u8]) -> Ordering {
    let mut left = UcaCursor::new(trim_trailing_spaces(left));
    let mut right = UcaCursor::new(trim_trailing_spaces(right));
    loop {
        let left_weight = match left.next_weight() {
            Ok(weight) => weight,
            Err(()) => return Ordering::Equal,
        };
        let right_weight = match right.next_weight() {
            Ok(weight) => weight,
            Err(()) => return Ordering::Equal,
        };
        match (left_weight, right_weight) {
            (Some(left), Some(right)) => {
                let ordering = left.cmp(&right);
                if !ordering.is_eq() {
                    return ordering;
                }
            }
            (None, None) => return Ordering::Equal,
            (None, Some(_)) => return Ordering::Less,
            (Some(_), None) => return Ordering::Greater,
        }
    }
}

fn unicode_0400_key(value: &[u8], trim: bool) -> Vec<u8> {
    let value = if trim {
        trim_trailing_spaces(value)
    } else {
        value
    };
    let mut key = Vec::with_capacity(value.len() * 2);
    let mut cursor = UcaCursor::new(value);
    while let Ok(Some(weight)) = cursor.next_weight() {
        key.extend_from_slice(&weight.to_be_bytes());
    }
    key
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use sha2::{Digest, Sha256};

    use super::{Collation, GENERAL_CI, UNICODE_0400, UNICODE_0400_LONG};

    fn digest(bytes: &[u8]) -> String {
        format!("{:x}", Sha256::digest(bytes))
    }

    #[test]
    fn generated_images_have_source_pinned_lengths_and_hashes() {
        assert_eq!(GENERAL_CI.len(), 131_072);
        assert_eq!(UNICODE_0400.len(), 524_288);
        assert_eq!(UNICODE_0400_LONG.len(), 440);
        assert_eq!(
            digest(GENERAL_CI),
            "787ea411c0600e485ae7dd52ce4b609848b5b832c179f2aed6deaf1e3a173d61"
        );
        assert_eq!(
            digest(UNICODE_0400),
            "87fbb2751d6afe9ff48b4f19136204846e778dd88a1ba8ef8b2d5398354852b6"
        );
        assert_eq!(
            digest(UNICODE_0400_LONG),
            "fc2ea60aa8caa70d615fcdffaf1d8e1d3d2438eae11847d719266be88bb5d776"
        );
    }

    /// The UCA 4.0 half of `TestAllItemInLongRUneMapIsUnique`.
    #[test]
    fn all_uca_0400_long_rune_weights_are_unique() {
        let rows: Vec<_> = UNICODE_0400_LONG
            .chunks_exact(20)
            .map(|row| (&row[4..12], &row[12..20]))
            .collect();
        assert_eq!(rows.len(), 22);
        assert_eq!(rows.iter().copied().collect::<HashSet<_>>().len(), 22);
    }

    #[test]
    fn every_uca_0400_long_marker_has_exactly_one_expansion() {
        let markers: HashSet<_> = UNICODE_0400
            .chunks_exact(8)
            .enumerate()
            .filter_map(|(codepoint, bytes)| {
                (u64::from_le_bytes(bytes.try_into().expect("fixed UCA weight")) == 0xFFFD)
                    .then_some(codepoint as u32)
            })
            .collect();
        let expansions: HashSet<_> = UNICODE_0400_LONG
            .chunks_exact(20)
            .map(|row| u32::from_le_bytes(row[..4].try_into().expect("fixed long-rune key")))
            .collect();
        assert_eq!(markers.len(), 22);
        assert_eq!(markers, expansions);
    }

    #[test]
    fn max_key_lengths_and_without_trim_follow_go_collators() {
        assert_eq!(Collation::Binary.max_key_len(b"a "), 2);
        assert_eq!(Collation::Utf8GeneralCi.max_key_len("😜".as_bytes()), 2);
        assert_eq!(Collation::Utf8UnicodeCi.max_key_len("😜".as_bytes()), 16);
        assert_eq!(Collation::Utf8GeneralCi.max_key_len(&[0xFF]), 2);
        assert_eq!(Collation::Utf8UnicodeCi.max_key_len(&[0xFF]), 16);
        assert_eq!(Collation::Utf8Mb4Bin.key(b"a "), b"a");
        assert_eq!(
            Collation::Utf8Mb4Bin.key_without_trim_right_space(b"a "),
            b"a "
        );
        assert_eq!(
            Collation::Utf8GeneralCi.key_without_trim_right_space(b"a "),
            [0, 0x41, 0, 0x20]
        );
    }
}
