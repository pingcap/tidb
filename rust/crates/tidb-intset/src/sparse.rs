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

// The Go spill set uses 256-bit blocks. Keep the blocks ordered by signed
// offset so membership can use binary search and set operations can merge.
const BLOCK_SIZE: i64 = 256;
const WORD_SIZE: usize = u64::BITS as usize;
const WORDS_PER_BLOCK: usize = BLOCK_SIZE as usize / WORD_SIZE;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Block {
    start: i64,
    words: [u64; WORDS_PER_BLOCK],
}

impl Block {
    fn first_at(&self, bit: usize) -> Option<i64> {
        let first_word = bit / WORD_SIZE;
        for (index, &word) in self.words.iter().enumerate().skip(first_word) {
            let word = if index == first_word {
                word & (u64::MAX << (bit % WORD_SIZE))
            } else {
                word
            };
            if word != 0 {
                return Some(
                    self.start + (index * WORD_SIZE) as i64 + i64::from(word.trailing_zeros()),
                );
            }
        }
        None
    }

    fn len(&self) -> usize {
        self.words
            .iter()
            .map(|word| word.count_ones() as usize)
            .sum()
    }
}

fn block_start(value: i64) -> i64 {
    value & !(BLOCK_SIZE - 1)
}

/// Bits in the inclusive range within one word.
pub(super) fn word_mask(first: usize, last: usize) -> u64 {
    (u64::MAX << first) & (u64::MAX >> (WORD_SIZE - 1 - last))
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(super) struct SparseSet {
    blocks: Vec<Block>,
    len: usize,
}

impl SparseSet {
    pub(super) fn from_small(small: u64) -> Self {
        let mut set = Self::default();
        set.insert_small(small);
        set
    }

    pub(super) fn len(&self) -> usize {
        self.len
    }

    pub(super) fn is_empty(&self) -> bool {
        self.blocks.is_empty()
    }

    pub(super) fn clear(&mut self) {
        self.blocks.clear();
        self.len = 0;
    }

    pub(super) fn copy_from(&mut self, source: &Self) {
        self.blocks.clone_from(&source.blocks);
        self.len = source.len;
    }

    pub(super) fn has_outside_small(&self) -> bool {
        match self.blocks.as_slice() {
            [] => false,
            [block] if block.start == 0 => block.words[1..].iter().any(|&word| word != 0),
            _ => true,
        }
    }

    fn block_index(&self, start: i64) -> Result<usize, usize> {
        // Sequential insertions stay in the last block or append a new one.
        if let Some(last) = self.blocks.last() {
            match start.cmp(&last.start) {
                std::cmp::Ordering::Equal => return Ok(self.blocks.len() - 1),
                std::cmp::Ordering::Greater => return Err(self.blocks.len()),
                std::cmp::Ordering::Less => {}
            }
        }
        self.blocks
            .binary_search_by_key(&start, |block| block.start)
    }

    pub(super) fn insert(&mut self, value: i64) {
        let start = block_start(value);
        let bit = (value - start) as usize;
        let word = bit / WORD_SIZE;
        let mask = 1 << (bit % WORD_SIZE);
        match self.block_index(start) {
            Ok(index) => {
                let stored = &mut self.blocks[index].words[word];
                self.len += usize::from(*stored & mask == 0);
                *stored |= mask;
            }
            Err(index) => {
                let mut words = [0; WORDS_PER_BLOCK];
                words[word] = mask;
                self.blocks.insert(index, Block { start, words });
                self.len += 1;
            }
        }
    }

    pub(super) fn remove(&mut self, value: i64) {
        let start = block_start(value);
        if let Ok(index) = self.block_index(start) {
            let bit = (value - start) as usize;
            let block = &mut self.blocks[index];
            let mask = 1 << (bit % WORD_SIZE);
            let word = &mut block.words[bit / WORD_SIZE];
            self.len -= usize::from(*word & mask != 0);
            *word &= !mask;
            if block.words == [0; WORDS_PER_BLOCK] {
                self.blocks.remove(index);
            }
        }
    }

    pub(super) fn contains(&self, value: i64) -> bool {
        let start = block_start(value);
        self.block_index(start).is_ok_and(|index| {
            let bit = (value - start) as usize;
            self.blocks[index].words[bit / WORD_SIZE] & (1 << (bit % WORD_SIZE)) != 0
        })
    }

    pub(super) fn next(&self, value: i64) -> Option<i64> {
        let start = block_start(value);
        match self.block_index(start) {
            Ok(index) => self.blocks[index]
                .first_at((value - start) as usize)
                .or_else(|| self.blocks.get(index + 1)?.first_at(0)),
            Err(index) => self.blocks.get(index)?.first_at(0),
        }
    }

    pub(super) fn for_each(&self, mut visit: impl FnMut(i64)) {
        for block in &self.blocks {
            for (index, mut word) in block.words.iter().copied().enumerate() {
                let start = block.start + (index * WORD_SIZE) as i64;
                while word != 0 {
                    visit(start + i64::from(word.trailing_zeros()));
                    word &= word - 1;
                }
            }
        }
    }

    pub(super) fn insert_small(&mut self, small: u64) {
        if small == 0 {
            return;
        }
        match self.block_index(0) {
            Ok(index) => {
                let word = &mut self.blocks[index].words[0];
                self.len += (small & !*word).count_ones() as usize;
                *word |= small;
            }
            Err(index) => {
                self.blocks.insert(
                    index,
                    Block {
                        start: 0,
                        words: [small, 0, 0, 0],
                    },
                );
                self.len += small.count_ones() as usize;
            }
        }
    }

    pub(super) fn remove_small(&mut self, small: u64) {
        if small == 0 {
            return;
        }
        if let Ok(index) = self.block_index(0) {
            let block = &mut self.blocks[index];
            let word = &mut block.words[0];
            self.len -= (*word & small).count_ones() as usize;
            *word &= !small;
            if block.words == [0; WORDS_PER_BLOCK] {
                self.blocks.remove(index);
            }
        }
    }

    pub(super) fn add_range(&mut self, from: i64, to: i64) {
        let first = block_start(from);
        let last = block_start(to);
        let begin = self.blocks.partition_point(|block| block.start < first);
        let end = begin + self.blocks[begin..].partition_point(|block| block.start <= last);
        let replaced = &self.blocks[begin..end];
        self.len -= replaced.iter().map(Block::len).sum::<usize>();
        let boundary_words = |block: Option<&Block>, start| {
            block
                .filter(|block| block.start == start)
                .map_or([0; WORDS_PER_BLOCK], |block| block.words)
        };
        // Interior blocks become full. Preserve unmodified bits at either end.
        let first_words = boundary_words(replaced.first(), first);
        let last_words = boundary_words(replaced.last(), last);
        let mut added = 0;
        let blocks = (first..=last).step_by(BLOCK_SIZE as usize).map(|start| {
            let first_bit = (from.max(start) - start) as usize;
            let last_bit = (to.min(start + (BLOCK_SIZE - 1)) - start) as usize;
            let previous = if start == first {
                first_words
            } else if start == last {
                last_words
            } else {
                [0; WORDS_PER_BLOCK]
            };
            let words = std::array::from_fn(|index| {
                let word_start = index * WORD_SIZE;
                let word_end = word_start + WORD_SIZE - 1;
                let mask = if first_bit > word_end || last_bit < word_start {
                    0
                } else {
                    word_mask(
                        first_bit.saturating_sub(word_start),
                        last_bit.min(word_end) - word_start,
                    )
                };
                mask | previous[index]
            });
            let block = Block { start, words };
            added += block.len();
            block
        });
        drop(self.blocks.splice(begin..end, blocks));
        self.len += added;
    }

    pub(super) fn union(&self, other: &Self) -> Self {
        if self.is_empty() {
            return other.clone();
        }
        if other.is_empty() {
            return self.clone();
        }
        let mut blocks = Vec::with_capacity(self.blocks.len() + other.blocks.len());
        let mut right = other.blocks.as_slice();
        let mut len = self.len + other.len;
        for &left in &self.blocks {
            while let Some((first, rest)) = right.split_first() {
                if first.start >= left.start {
                    break;
                }
                blocks.push(*first);
                right = rest;
            }
            match right.split_first() {
                Some((block, rest)) if block.start == left.start => {
                    let mut merged = left;
                    for (word, &mask) in merged.words.iter_mut().zip(&block.words) {
                        len -= (*word & mask).count_ones() as usize;
                        *word |= mask;
                    }
                    blocks.push(merged);
                    right = rest;
                }
                _ => blocks.push(left),
            }
        }
        blocks.extend_from_slice(right);
        Self { blocks, len }
    }

    pub(super) fn difference_with(&mut self, other: &Self) {
        if !other.is_empty() {
            self.combine_with(other, |left, right| left & !right);
        }
    }

    pub(super) fn intersection_with(&mut self, other: &Self) {
        if other.is_empty() {
            self.clear();
        } else {
            self.combine_with(other, |left, right| left & right);
        }
    }

    fn combine_with(&mut self, other: &Self, combine: impl Fn(u64, u64) -> u64) {
        let mut right = other.blocks.as_slice();
        self.len = 0;
        self.blocks.retain_mut(|block| {
            let mask =
                align(&mut right, block.start).map_or([0; WORDS_PER_BLOCK], |right| right.words);
            for (word, mask) in block.words.iter_mut().zip(mask) {
                *word = combine(*word, mask);
            }
            let len = block.len();
            self.len += len;
            len != 0
        });
    }

    pub(super) fn intersects(&self, other: &Self) -> bool {
        let mut right = other.blocks.as_slice();
        if self
            .blocks
            .first()
            .zip(right.last())
            .is_some_and(|(first, last)| first.start > last.start)
            || right
                .first()
                .zip(self.blocks.last())
                .is_some_and(|(first, last)| first.start > last.start)
        {
            return false;
        }
        self.blocks.iter().any(|left| {
            align(&mut right, left.start).is_some_and(|right| {
                left.words
                    .iter()
                    .zip(&right.words)
                    .any(|(left, right)| left & right != 0)
            })
        })
    }

    pub(super) fn subset_of(&self, other: &Self) -> bool {
        let mut right = other.blocks.as_slice();
        self.blocks.iter().all(|left| {
            align(&mut right, left.start).is_some_and(|right| {
                left.words
                    .iter()
                    .zip(&right.words)
                    .all(|(left, right)| left & !right == 0)
            })
        })
    }
}

fn align<'a>(blocks: &mut &'a [Block], start: i64) -> Option<&'a Block> {
    while let Some((first, rest)) = blocks.split_first() {
        if first.start >= start {
            return (first.start == start).then_some(first);
        }
        *blocks = rest;
    }
    None
}
