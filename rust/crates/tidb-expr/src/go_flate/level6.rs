// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.
// Ported from Go 1.27.4 compress/flate/{level6,deflatefast}.go.

use super::{
    MAX_MATCH_LENGTH, MAX_STORE_BLOCK_SIZE, Token, WINDOW_SIZE, literal_token, match_token,
};

const TABLE_BITS: u32 = 15;
const TABLE_SIZE: usize = 1 << TABLE_BITS;
const HISTORY_CAPACITY: usize = MAX_STORE_BLOCK_SIZE * 5;
const BUFFER_RESET: i32 = i32::MAX - HISTORY_CAPACITY as i32 - MAX_STORE_BLOCK_SIZE as i32;
const INPUT_MARGIN: usize = 11;

#[derive(Clone, Copy, Default)]
struct LongEntry {
    current: i32,
    previous: i32,
}

#[derive(Clone, Copy)]
struct Match {
    source: usize,
    target: usize,
    length: usize,
    next: usize,
}

pub(super) struct Level6 {
    short: Box<[i32]>,
    long: Box<[LongEntry]>,
    cursor: i32,
    history_start: usize,
    history_len: usize,
}

impl Level6 {
    pub(super) fn new() -> Self {
        Self {
            short: vec![0; TABLE_SIZE].into_boxed_slice(),
            long: vec![LongEntry::default(); TABLE_SIZE].into_boxed_slice(),
            cursor: MAX_STORE_BLOCK_SIZE as i32,
            history_start: 0,
            history_len: 0,
        }
    }

    pub(super) fn reset(&mut self) {
        // Discard the old history by moving past every recorded match.
        if self.cursor <= BUFFER_RESET {
            self.cursor += (WINDOW_SIZE + self.history_len) as i32;
        }
        self.history_start = 0;
        self.history_len = 0;
    }

    fn add_block<'a>(&mut self, data: &'a [u8], start: usize, end: usize) -> (&'a [u8], usize) {
        if self.cursor >= BUFFER_RESET {
            if self.history_len == 0 {
                self.short.fill(0);
                self.long.fill(LongEntry::default());
            } else {
                let min_offset = self.cursor + self.history_len as i32 - WINDOW_SIZE as i32;
                let delta = self.cursor - WINDOW_SIZE as i32;
                let rebase = |offset| {
                    if offset <= min_offset {
                        0
                    } else {
                        offset - delta
                    }
                };
                for off in &mut self.short {
                    *off = rebase(*off);
                }
                for entry in &mut self.long {
                    entry.current = rebase(entry.current);
                    entry.previous = if entry.current == 0 {
                        0
                    } else {
                        rebase(entry.previous)
                    };
                }
            }
            self.cursor = WINDOW_SIZE as i32;
        }
        if end - self.history_start > HISTORY_CAPACITY {
            let keep_from = start - WINDOW_SIZE;
            self.cursor += (keep_from - self.history_start) as i32;
            self.history_start = keep_from;
        }
        self.history_len = end - self.history_start;
        // This call receives the whole SQL value, so it can borrow exactly
        // the history Go would have copied into its staging buffer.
        (&data[self.history_start..end], start - self.history_start)
    }

    #[inline]
    fn insert_long(&mut self, hash: usize, offset: i32) {
        let entry = &mut self.long[hash];
        *entry = LongEntry {
            current: offset,
            previous: entry.current,
        };
    }

    #[inline]
    fn insert_both(&mut self, short: usize, long: usize, pos: usize) {
        let offset = self.cursor + pos as i32;
        self.short[short] = offset;
        self.insert_long(long, offset);
    }

    fn target(&self, offset: i32, source: usize) -> Option<usize> {
        let target = offset - self.cursor;
        (source as i32 - target < WINDOW_SIZE as i32).then_some(target as usize)
    }

    fn search(
        &mut self,
        bytes: &[u8],
        mut source: usize,
        emitted: usize,
        repeat: usize,
        limit: usize,
    ) -> Option<Match> {
        let mut word = load64(bytes, source);
        loop {
            let next = source + 1 + ((source - emitted) >> 7);
            if next > limit {
                return None;
            }
            let short_hash = hash_short(word);
            let long_hash = hash_long(word);
            let short = self.short[short_hash];
            let long = self.long[long_hash];
            let next_word = load64(bytes, next);
            self.insert_both(short_hash, long_hash, source);
            let next_short_hash = hash_short(next_word);
            let next_long_hash = hash_long(next_word);

            let get = |state: &Self, off| {
                state
                    .target(off, source)
                    .filter(|&target| load32(bytes, target) == word as u32)
            };
            if let Some(target) = get(self, long.current) {
                self.insert_both(next_short_hash, next_long_hash, next);
                if let Some(previous) = get(self, long.previous) {
                    let length = 4 + limited_match(bytes, source + 4, target + 4);
                    let previous_len = 4 + limited_match(bytes, source + 4, previous + 4);
                    if previous_len > length {
                        return Some(Match {
                            source,
                            target: previous,
                            length: previous_len,
                            next,
                        });
                    }
                    return Some(Match {
                        source,
                        target,
                        length,
                        next,
                    });
                }
                return Some(Match {
                    source,
                    target,
                    length: 0,
                    next,
                });
            }
            if let Some(target) = get(self, long.previous) {
                self.insert_both(next_short_hash, next_long_hash, next);
                return Some(Match {
                    source,
                    target,
                    length: 0,
                    next,
                });
            }
            if let Some(target) = get(self, short) {
                let length = 4 + limited_match(bytes, source + 4, target + 4);
                let mut found = Match {
                    source,
                    target,
                    length,
                    next,
                };
                let at_next = self.long[next_long_hash];
                self.insert_both(next_short_hash, next_long_hash, next);

                let repeated = source + 1 - repeat;
                if load32(bytes, repeated) == (word >> 8) as u32 {
                    let length = 4 + limited_match(bytes, source + 5, repeated + 4);
                    if length > found.length {
                        return Some(Match {
                            source: source + 1,
                            target: repeated,
                            length,
                            next,
                        });
                    }
                }
                for offset in [at_next.current, at_next.previous] {
                    if let Some(target) = self.target(offset, next) {
                        if load32(bytes, target) == next_word as u32 {
                            let length = 4 + limited_match(bytes, next + 4, target + 4);
                            if length > found.length {
                                found = Match {
                                    source: next,
                                    target,
                                    length,
                                    next,
                                };
                            }
                        }
                    }
                }
                return Some(found);
            }
            source = next;
            word = next_word;
        }
    }

    pub(super) fn encode(
        &mut self,
        tokens: &mut Vec<Token>,
        data: &[u8],
        start: usize,
        end: usize,
    ) {
        let (bytes, mut source) = self.add_block(data, start, end);
        let mut emitted = source;
        let mut repeat = 1;
        let limit = bytes.len() - INPUT_MARGIN;
        while let Some(mut found) = self.search(bytes, source, emitted, repeat, limit) {
            if found.length == 0 {
                found.length = 4 + long_match(bytes, found.source + 4, found.target + 4);
            } else if found.length == MAX_MATCH_LENGTH {
                found.length += long_match(
                    bytes,
                    found.source + found.length,
                    found.target + found.length,
                );
            }
            let at = found.source + found.length;
            if at < limit {
                let long = self.long[hash_long(load64(bytes, at))];
                let candidate_source = found.source + 2;
                let original_distance = candidate_source as i64
                    - (i64::from(long.current) - i64::from(self.cursor) - found.length as i64 + 2);
                if original_distance < WINDOW_SIZE as i64 {
                    for offset in [long.current, long.previous] {
                        let target =
                            i64::from(offset) - i64::from(self.cursor) - found.length as i64 + 2;
                        let distance = candidate_source as i64 - target;
                        if target >= 0 && distance > 0 && distance < WINDOW_SIZE as i64 {
                            let length = long_match(bytes, candidate_source, target as usize);
                            if length > found.length {
                                found.source = candidate_source;
                                found.target = target as usize;
                                found.length = length;
                            }
                        }
                    }
                }
            }
            while found.target > 0
                && found.source > emitted
                && bytes[found.target - 1] == bytes[found.source - 1]
            {
                found.target -= 1;
                found.source -= 1;
                found.length += 1;
            }
            emit_literals(tokens, &bytes[emitted..found.source]);
            repeat = found.source - found.target;
            emit_match(tokens, found.length, repeat);
            emitted = found.source + found.length;
            source = emitted.max(found.next + 1);

            if source >= limit {
                for index in (found.next + 1..bytes.len() - 8).step_by(2) {
                    let word = load64(bytes, index);
                    self.insert_both(hash_short(word), hash_long(word), index);
                }
                break;
            }
            for index in (found.next + 1..source - 1).step_by(2) {
                let word = load64(bytes, index);
                self.insert_both(hash_short(word), hash_long(word), index);
                self.insert_long(hash_long(word >> 8), self.cursor + index as i32 + 1);
            }
        }
        if !tokens.is_empty() {
            emit_literals(tokens, &bytes[emitted..]);
        }
    }
}

#[inline]
fn load64(bytes: &[u8], index: usize) -> u64 {
    u64::from_le_bytes(bytes[index..][..8].try_into().unwrap())
}

#[inline]
fn load32(bytes: &[u8], index: usize) -> u32 {
    u32::from_le_bytes(bytes[index..][..4].try_into().unwrap())
}

#[inline]
fn hash_short(word: u64) -> usize {
    ((word as u32).wrapping_mul(2_654_435_761) >> (32 - TABLE_BITS)) as usize
}

#[inline]
fn hash_long(word: u64) -> usize {
    ((word << 8).wrapping_mul(58_295_818_150_454_627) >> (64 - TABLE_BITS)) as usize
}

fn limited_match(bytes: &[u8], source: usize, target: usize) -> usize {
    super::match_len(
        &bytes[source..],
        &bytes[target..],
        (bytes.len() - source).min(MAX_MATCH_LENGTH - 4),
    )
}

fn long_match(bytes: &[u8], source: usize, target: usize) -> usize {
    super::match_len(&bytes[source..], &bytes[target..], bytes.len() - source)
}

fn emit_literals(tokens: &mut Vec<Token>, bytes: &[u8]) {
    tokens.extend(bytes.iter().map(|&byte| literal_token(u32::from(byte))));
}

fn emit_match(tokens: &mut Vec<Token>, mut length: usize, offset: usize) {
    while length > 0 {
        let n = if length > MAX_MATCH_LENGTH + 3 {
            MAX_MATCH_LENGTH
        } else if length > MAX_MATCH_LENGTH {
            MAX_MATCH_LENGTH - 3
        } else {
            length
        };
        tokens.push(match_token((n - 3) as u32, (offset - 1) as u32));
        length -= n;
    }
}
