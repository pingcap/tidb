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

//! Bit-exact transcription of go's `compress/flate` DEFLATE encoder at
//! compression level 6 (`DefaultCompression`), plus the `compress/zlib`
//! framing, for `COMPRESS()`'s wire bytes.
//!
//! MySQL compatibility is mostly arbitrary, and the compressed BYTES are the
//! arbitrary part: TiDB evaluates `COMPRESS` through go's standard library,
//! whose flate encoder makes different match/Huffman choices from every Rust
//! backend (`flate2`'s miniz included), so the wire values differ byte for
//! byte even though both decode to the same text. Instead of calling a
//! backend we transcribe go's own encoder (go src/compress/flate/deflate.go,
//! huffman_bit_writer.go, huffman_code.go, token.go, BSD licensed) so every
//! input produces go's exact bytes. On amd64 go's `haveArchExp`-style
//! assembly does not exist for flate — the pure-Go bodies below are exactly
//! what runs there.
//!
//! # What COMPRESS drives
//!
//! TiDB's `deflate()` helper (`pkg/expression/builtin_encryption.go`) is
//! `zlib.NewWriter` + `Write(data)` + `Close()` — Write feeds the window,
//! Close sets `sync` and drains: the pending tokens become a NON-final block
//! and `close()` appends the final empty STORED block
//! (`writeStoredHeader(0, true)`), then zlib's Close writes the adler.
//! Captured end to end: `COMPRESS('aaaaaaaaaa')` is
//! `0a000000 789c 4a840340 000000ffff 14e103cb`.

const LOG_WINDOW_SIZE: usize = 15;
const WINDOW_SIZE: usize = 1 << LOG_WINDOW_SIZE;
const WINDOW_MASK: usize = WINDOW_SIZE - 1;

const BASE_MATCH_LENGTH: usize = 3;
const MIN_MATCH_LENGTH: usize = 4;
const MAX_MATCH_LENGTH: usize = 258;
const BASE_MATCH_OFFSET: usize = 1;

const MAX_FLATE_BLOCK_TOKENS: usize = 1 << 14;
const MAX_STORE_BLOCK_SIZE: usize = 65535;
const HASH_BITS: u32 = 17;
const HASH_SIZE: usize = 1 << HASH_BITS;
const HASH_MASK: usize = HASH_SIZE - 1;
const MAX_HASH_OFFSET: usize = 1 << 24;

const SKIP_NEVER: i32 = i32::MAX;

const HASHMUL: u32 = 0x1e35_a7bd;

// huffman_bit_writer.go's length tables.
const LENGTH_EXTRA_BITS: [i8; 29] = [
    /* 257 */ 0, 0, 0, /* 260 */ 0, 0, 0, 0, 0, 1, 1, 1, 1, 2, /* 270 */ 2, 2, 2, 3,
    3, 3, 3, 4, 4, 4, /* 280 */ 4, 5, 5, 5, 5, 0,
];
const LENGTH_BASE: [u32; 29] = [
    0, 1, 2, 3, 4, 5, 6, 7, 8, 10, 12, 14, 16, 20, 24, 28, 32, 40, 48, 56, 64, 80, 96, 112, 128,
    160, 192, 224, 255,
];
const OFFSET_EXTRA_BITS: [i8; 30] = [
    0, 0, 0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10, 10, 11, 11, 12, 12, 13,
    13,
];
const OFFSET_BASE: [u32; 30] = [
    0x000000, 0x000001, 0x000002, 0x000003, 0x000004, 0x000006, 0x000008, 0x00000c, 0x000010,
    0x000018, 0x000020, 0x000030, 0x000040, 0x000060, 0x000080, 0x0000c0, 0x000100, 0x000180,
    0x000200, 0x000300, 0x000400, 0x000600, 0x000800, 0x000c00, 0x001000, 0x001800, 0x002000,
    0x003000, 0x004000, 0x006000,
];
const CODEGEN_ORDER: [usize; 19] = [
    16, 17, 18, 0, 8, 7, 9, 6, 10, 5, 11, 4, 12, 3, 13, 2, 14, 1, 15,
];

// token.go: the LZ77 token encoding.
const LENGTH_SHIFT: u32 = 22;
const OFFSET_MASK: u32 = (1 << LENGTH_SHIFT) - 1;
const LITERAL_TYPE: u32 = 0 << 30;
const MATCH_TYPE: u32 = 1 << 30;

const LENGTH_CODES: [u32; 256] = [
    0, 1, 2, 3, 4, 5, 6, 7, 8, 8, 9, 9, 10, 10, 11, 11, 12, 12, 12, 12, 13, 13, 13, 13, 14, 14, 14,
    14, 15, 15, 15, 15, 16, 16, 16, 16, 16, 16, 16, 16, 17, 17, 17, 17, 17, 17, 17, 17, 18, 18, 18,
    18, 18, 18, 18, 18, 19, 19, 19, 19, 19, 19, 19, 19, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20,
    20, 20, 20, 20, 20, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 22, 22, 22,
    22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23,
    23, 23, 23, 23, 23, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24,
    24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25,
    25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 26, 26, 26,
    26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26,
    26, 26, 26, 26, 26, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27,
    27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 28,
];
const LENGTH_CODES_START: usize = 257;
const OFFSET_CODES: [u32; 256] = [
    0, 1, 2, 3, 4, 4, 5, 5, 6, 6, 6, 6, 7, 7, 7, 7, 8, 8, 8, 8, 8, 8, 8, 8, 9, 9, 9, 9, 9, 9, 9, 9,
    10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 11, 11, 11, 11, 11, 11, 11, 11,
    11, 11, 11, 11, 11, 11, 11, 11, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12,
    12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 13, 13, 13, 13, 13, 13, 13, 13,
    13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13,
    14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14,
    14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14,
    14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 15, 15, 15, 15, 15, 15, 15, 15,
    15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15,
    15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15,
    15, 15, 15, 15, 15, 15, 15, 15,
];

/// go `token`: the low 22 bits hold the literal or the offset.
#[derive(Clone, Copy, PartialEq, Eq)]
struct Token(u32);

fn literal_token(literal: u32) -> Token {
    Token(LITERAL_TYPE + literal)
}

fn match_token(xlength: u32, xoffset: u32) -> Token {
    Token(MATCH_TYPE + (xlength << LENGTH_SHIFT) + xoffset)
}

impl Token {
    fn literal(self) -> u32 {
        self.0 - LITERAL_TYPE
    }

    fn offset(self) -> u32 {
        self.0 & OFFSET_MASK
    }

    fn length(self) -> u32 {
        (self.0 - MATCH_TYPE) >> LENGTH_SHIFT
    }

    fn is_match(self) -> bool {
        self.0 >= MATCH_TYPE
    }
}

fn length_code(len: u32) -> u32 {
    LENGTH_CODES[len as usize]
}

fn offset_code(off: u32) -> u32 {
    if (off as usize) < OFFSET_CODES.len() {
        OFFSET_CODES[off as usize]
    } else if ((off >> 7) as usize) < OFFSET_CODES.len() {
        OFFSET_CODES[(off >> 7) as usize] + 14
    } else {
        OFFSET_CODES[(off >> 14) as usize] + 28
    }
}

/// go `reverseBits` (`bits.Reverse16(number << (16 - bitLength))`).
fn reverse_bits(number: u16, bit_length: u8) -> u16 {
    (number << (16 - bit_length as u16)).reverse_bits()
}

/// go `hcode`.
#[derive(Clone, Copy, Default)]
struct Hcode {
    code: u16,
    len: u16,
}

/// go `literalNode`.
#[derive(Clone, Copy)]
struct LiteralNode {
    literal: u16,
    freq: i32,
}

/// go `huffmanEncoder` (huffman_code.go). The reusable `freqcache` becomes a
/// scratch vec handed in by the bit writer, which owns the allocations.
struct HuffmanEncoder {
    codes: Vec<Hcode>,
    bit_count: [i32; 17],
}

const MAX_NUM_LIT: usize = 286;
const MAX_BITS_LIMIT: usize = 16;
const CODEGEN_CODE_COUNT: usize = 19;
const BAD_CODE: u8 = 255;

impl HuffmanEncoder {
    fn new(size: usize) -> Self {
        Self {
            codes: vec![Hcode::default(); size],
            bit_count: [0; 17],
        }
    }

    fn bit_length(&self, freq: &[i32]) -> usize {
        let mut total = 0usize;
        for (i, f) in freq.iter().enumerate() {
            if *f != 0 {
                total += (*f as usize) * (self.codes[i].len as usize);
            }
        }
        total
    }

    /// go `huffmanEncoder.bitCounts` (huffman_code.go) — the level-table
    /// algorithm that fits `n` leaves into `maxBits` levels, transcribed
    /// line for line (including the `maxNode()` sentinel append).
    fn bit_counts(&mut self, list: &mut Vec<LiteralNode>, max_bits_in: i32) -> Vec<i32> {
        let mut max_bits = max_bits_in;
        let n = list.len() as i32;
        list.push(LiteralNode {
            literal: u16::MAX,
            freq: i32::MAX,
        });
        if max_bits > n - 1 {
            max_bits = n - 1;
        }

        let mut levels = [LevelInfo::default(); MAX_BITS_LIMIT];
        let mut leaf_counts = [[0i32; MAX_BITS_LIMIT]; MAX_BITS_LIMIT];

        for level in 1..=max_bits {
            levels[level as usize] = LevelInfo {
                level,
                last_freq: list[1].freq,
                next_char_freq: list[2].freq,
                next_pair_freq: list[0].freq + list[1].freq,
                needed: 0,
            };
            leaf_counts[level as usize][level as usize] = 2;
            if level == 1 {
                levels[level as usize].next_pair_freq = i32::MAX;
            }
        }

        levels[max_bits as usize].needed = 2 * n - 4;
        let mut level = max_bits;
        loop {
            // go `l := &levels[level]` — direct indexing keeps the borrow
            // checker happy with the cross-level updates below (l.level is
            // always `level` here).
            if levels[level as usize].next_pair_freq == i32::MAX
                && levels[level as usize].next_char_freq == i32::MAX
            {
                levels[level as usize].needed = 0;
                levels[(level + 1) as usize].next_pair_freq = i32::MAX;
                level += 1;
                continue;
            }
            let prev_freq = levels[level as usize].last_freq;
            if levels[level as usize].next_char_freq < levels[level as usize].next_pair_freq {
                let n2 = leaf_counts[level as usize][level as usize] + 1;
                levels[level as usize].last_freq = levels[level as usize].next_char_freq;
                leaf_counts[level as usize][level as usize] = n2;
                levels[level as usize].next_char_freq = list[n2 as usize].freq;
            } else {
                levels[level as usize].last_freq = levels[level as usize].next_pair_freq;
                let lower = leaf_counts[(level - 1) as usize][..level as usize].to_vec();
                leaf_counts[level as usize][..level as usize].copy_from_slice(&lower);
                levels[(level - 1) as usize].needed = 2;
            }
            levels[level as usize].needed -= 1;
            if levels[level as usize].needed == 0 {
                if level == max_bits {
                    break;
                }
                levels[(level + 1) as usize].next_pair_freq =
                    prev_freq + levels[level as usize].last_freq;
                level += 1;
            } else {
                while levels[(level - 1) as usize].needed > 0 {
                    level -= 1;
                }
            }
        }

        let counts = &leaf_counts[max_bits as usize];
        let mut bit_count = vec![0i32; max_bits as usize + 1];
        let mut bits = 1usize;
        for lvl in (1..=max_bits as usize).rev() {
            bit_count[bits] = counts[lvl] - counts[lvl - 1];
            bits += 1;
        }
        bit_count
    }

    /// go `huffmanEncoder.assignEncodingAndSize`.
    fn assign_encoding_and_size(&mut self, bit_count: &[i32], list: &mut Vec<LiteralNode>) {
        let mut code = 0u16;
        let mut list_len = list.len();
        for (n, bits) in bit_count.iter().enumerate() {
            let (n, bits) = (n as i32, *bits);
            code <<= 1;
            if n == 0 || bits == 0 {
                continue;
            }
            let chunk = &mut list[list_len - bits as usize..list_len];
            // go `byLiteral.sort`.
            chunk.sort_by_key(|node| node.literal);
            for node in chunk.iter() {
                if node.literal == u16::MAX {}
                self.codes[node.literal as usize] = Hcode {
                    code: reverse_bits(code, n as u8),
                    len: n as u16,
                };
                code += 1;
            }
            list_len -= bits as usize;
        }
    }

    /// go `huffmanEncoder.generate`. (go's `freqcache` is a reusable
    /// scratch buffer; allocating per call here changes no output byte.)
    fn generate(&mut self, freq: &[i32], max_bits: i32) {
        let mut list: Vec<LiteralNode> = Vec::with_capacity(freq.len() + 1);
        let mut count = 0usize;
        for (i, f) in freq.iter().enumerate() {
            if *f != 0 {
                list.push(LiteralNode {
                    literal: i as u16,
                    freq: *f,
                });
                count += 1;
            } else {
                self.codes[i].len = 0;
            }
        }
        list.truncate(count);
        if count <= 2 {
            for (i, node) in list.iter().enumerate() {
                self.codes[node.literal as usize] = Hcode {
                    code: i as u16,
                    len: 1,
                };
            }
            return;
        }
        // go `byFreq.sort`: frequency first, literal second.
        list.sort_by(|a, b| (a.freq, a.literal).cmp(&(b.freq, b.literal)));
        let real_len = list.len();
        let bit_count = self.bit_counts(&mut list, max_bits);
        // go's `list = list[0 : n+1]; list[n] = maxNode()` re-slices locally:
        // the sentinel (and the freqcache slot behind it) never reaches
        // assignEncodingAndSize — its chunks cover exactly the `count` real
        // leaves. Truncate the appended sentinel away before assigning.
        list.truncate(real_len);
        self.assign_encoding_and_size(&bit_count, &mut list);
    }
}

/// go `levelInfo` (huffman_code.go).
#[derive(Clone, Copy, Default)]
struct LevelInfo {
    level: i32,
    last_freq: i32,
    next_char_freq: i32,
    next_pair_freq: i32,
    needed: i32,
}

fn fixed_literal_encoding() -> HuffmanEncoder {
    let mut h = HuffmanEncoder::new(MAX_NUM_LIT);
    for ch in 0..MAX_NUM_LIT as u16 {
        let (bits, size): (u16, u16) = if ch < 144 {
            (ch + 48, 8)
        } else if ch < 256 {
            (ch + 400 - 144, 9)
        } else if ch < 280 {
            (ch - 256, 7)
        } else {
            (ch + 192 - 280, 8)
        };
        h.codes[ch as usize] = Hcode {
            code: reverse_bits(bits, size as u8),
            len: size,
        };
    }
    h
}

fn fixed_offset_encoding() -> HuffmanEncoder {
    let mut h = HuffmanEncoder::new(30);
    for (ch, code) in h.codes.iter_mut().enumerate() {
        *code = Hcode {
            code: reverse_bits(ch as u16, 5),
            len: 5,
        };
    }
    h
}

/// go `huffmanBitWriter`, writing into a growable byte buffer (the deflate
/// output is consumed in memory by COMPRESS's framing).
struct HuffmanBitWriter {
    bits: u64,
    nbits: usize,
    out: Vec<u8>,
    codegen_freq: [i32; CODEGEN_CODE_COUNT],
    literal_freq: Vec<i32>,
    offset_freq: Vec<i32>,
    codegen: Vec<u8>,
    literal_encoding: HuffmanEncoder,
    offset_encoding: HuffmanEncoder,
    codegen_encoding: HuffmanEncoder,
}

const BUFFER_FLUSH_SIZE: usize = 240;

impl HuffmanBitWriter {
    fn new() -> Self {
        Self {
            bits: 0,
            nbits: 0,
            out: Vec::new(),
            codegen_freq: [0; CODEGEN_CODE_COUNT],
            literal_freq: vec![0; MAX_NUM_LIT],
            offset_freq: vec![0; 30],
            codegen: vec![0; MAX_NUM_LIT + 30 + 1],
            literal_encoding: HuffmanEncoder::new(MAX_NUM_LIT),
            codegen_encoding: HuffmanEncoder::new(CODEGEN_CODE_COUNT),
            offset_encoding: HuffmanEncoder::new(30),
        }
    }

    /// go `flush`: pad the trailing bits into full bytes.
    fn flush(&mut self) {
        let mut n = self.out.len();
        while self.nbits != 0 {
            let byte = self.bits as u8;
            self.out.push(byte);
            self.bits >>= 8;
            if self.nbits > 8 {
                self.nbits -= 8;
            } else {
                self.nbits = 0;
            }
            n += 1;
        }
        self.bits = 0;
    }

    /// go `writeBits`.
    fn write_bits(&mut self, b: i32, nb: u32) {
        self.bits |= (b as u64) << self.nbits;
        self.nbits += nb as usize;
        if self.nbits >= 48 {
            let bits = self.bits;
            self.bits >>= 48;
            self.nbits -= 48;
            for shift in [0u32, 8, 16, 24, 32, 40] {
                self.out.push((bits >> shift) as u8);
            }
        }
    }

    /// go `writeBytes`: byte-align then append.
    fn write_bytes(&mut self, bytes: &[u8]) {
        if self.nbits & 7 != 0 {
            // go InternalError("writeBytes with unfinished bits") — unreachable
            // on the paths that reach here (stored blocks follow an aligned
            // header); the bit writer state would be corrupt otherwise.
            self.nbits = 0;
        }
        while self.nbits != 0 {
            let byte = self.bits as u8;
            self.out.push(byte);
            self.bits >>= 8;
            self.nbits -= 8;
        }
        self.out.extend_from_slice(bytes);
    }

    /// go `writeCode`.
    fn write_code(&mut self, c: Hcode) {
        self.bits |= (c.code as u64) << self.nbits;
        self.nbits += c.len as usize;
        if self.nbits >= 48 {
            let bits = self.bits;
            self.bits >>= 48;
            self.nbits -= 48;
            for shift in [0u32, 8, 16, 24, 32, 40] {
                self.out.push((bits >> shift) as u8);
            }
        }
    }

    /// go `generateCodegen`.
    fn generate_codegen(&mut self, num_literals: usize, num_offsets: usize) {
        self.codegen_freq = [0; CODEGEN_CODE_COUNT];
        for i in 0..num_literals {
            self.codegen[i] = self.literal_encoding.codes[i].len as u8;
        }
        for i in 0..num_offsets {
            self.codegen[num_literals + i] = self.offset_encoding.codes[i].len as u8;
        }
        self.codegen[num_literals + num_offsets] = BAD_CODE;
        let mut size = self.codegen[0];
        let mut count = 1i32;
        let mut out_index = 0usize;
        let mut in_index = 1usize;
        while size != BAD_CODE {
            let next_size = self.codegen[in_index];
            in_index += 1;
            if next_size == size {
                count += 1;
                continue;
            }
            if size != 0 {
                self.codegen[out_index] = size;
                out_index += 1;
                self.codegen_freq[size as usize] += 1;
                count -= 1;
                while count >= 3 {
                    let n = count.min(6);
                    self.codegen[out_index] = 16;
                    out_index += 1;
                    self.codegen[out_index] = (n - 3) as u8;
                    out_index += 1;
                    self.codegen_freq[16] += 1;
                    count -= n;
                }
            } else {
                while count >= 11 {
                    let n = count.min(138);
                    self.codegen[out_index] = 18;
                    out_index += 1;
                    self.codegen[out_index] = (n - 11) as u8;
                    out_index += 1;
                    self.codegen_freq[18] += 1;
                    count -= n;
                }
                if count >= 3 {
                    self.codegen[out_index] = 17;
                    out_index += 1;
                    self.codegen[out_index] = (count - 3) as u8;
                    out_index += 1;
                    self.codegen_freq[17] += 1;
                    count = 0;
                }
            }
            count -= 1;
            while count >= 0 {
                self.codegen[out_index] = size;
                out_index += 1;
                self.codegen_freq[size as usize] += 1;
                count -= 1;
            }
            size = next_size;
            count = 1;
        }
        self.codegen[out_index] = BAD_CODE;
    }

    /// go `dynamicSize`.
    fn dynamic_size(&self, extra_bits: usize) -> (usize, usize) {
        let mut num_codegens = self.codegen_freq.len();
        while num_codegens > 4 && self.codegen_freq[CODEGEN_ORDER[num_codegens - 1]] == 0 {
            num_codegens -= 1;
        }
        let header = 3
            + 5
            + 5
            + 4
            + (3 * num_codegens)
            + self.codegen_encoding.bit_length(&self.codegen_freq)
            + self.codegen_freq[16] as usize * 2
            + self.codegen_freq[17] as usize * 3
            + self.codegen_freq[18] as usize * 7;
        let size = header
            + self.literal_encoding.bit_length(&self.literal_freq)
            + self.offset_encoding.bit_length(&self.offset_freq)
            + extra_bits;
        (size, num_codegens)
    }

    /// go `fixedSize`.
    fn fixed_size(&self, extra_bits: usize) -> usize {
        3 + fixed_literal_encoding().bit_length(&self.literal_freq)
            + fixed_offset_encoding().bit_length(&self.offset_freq)
            + extra_bits
    }

    /// go `storedSize`.
    fn stored_size(&self, input: Option<&[u8]>) -> (usize, bool) {
        match input {
            None => (0, false),
            Some(input) => {
                if input.len() <= MAX_STORE_BLOCK_SIZE {
                    ((input.len() + 5) * 8, true)
                } else {
                    (0, false)
                }
            }
        }
    }

    /// go `writeDynamicHeader`.
    fn write_dynamic_header(
        &mut self,
        num_literals: usize,
        num_offsets: usize,
        num_codegens: usize,
        is_eof: bool,
    ) {
        let first_bits: i32 = if is_eof { 5 } else { 4 };
        self.write_bits(first_bits, 3);
        self.write_bits((num_literals - 257) as i32, 5);
        self.write_bits((num_offsets - 1) as i32, 5);
        self.write_bits((num_codegens - 4) as i32, 4);
        for i in 0..num_codegens {
            let value = self.codegen_encoding.codes[CODEGEN_ORDER[i]].len as i32;
            self.write_bits(value, 3);
        }
        let mut i = 0usize;
        loop {
            let code_word = self.codegen[i] as i32;
            i += 1;
            if code_word == i32::from(BAD_CODE) {
                break;
            }
            self.write_code(self.codegen_encoding.codes[code_word as usize]);
            match code_word {
                16 => {
                    self.write_bits(self.codegen[i] as i32, 2);
                    i += 1;
                }
                17 => {
                    self.write_bits(self.codegen[i] as i32, 3);
                    i += 1;
                }
                18 => {
                    self.write_bits(self.codegen[i] as i32, 7);
                    i += 1;
                }
                _ => {}
            }
        }
    }

    /// go `writeStoredHeader`.
    fn write_stored_header(&mut self, length: usize, is_eof: bool) {
        let flag: i32 = if is_eof { 1 } else { 0 };
        self.write_bits(flag, 3);
        self.flush();
        self.write_bits(length as i32, 16);
        self.write_bits(!(length as u16) as i32, 16);
    }

    /// go `writeFixedHeader`.
    fn write_fixed_header(&mut self, is_eof: bool) {
        let value: i32 = if is_eof { 3 } else { 2 };
        self.write_bits(value, 3);
    }

    /// go `indexTokens`.
    fn index_tokens(&mut self, tokens: &[Token]) -> (usize, usize) {
        self.literal_freq.iter_mut().for_each(|f| *f = 0);
        self.offset_freq.iter_mut().for_each(|f| *f = 0);
        for t in tokens {
            if !t.is_match() {
                self.literal_freq[t.literal() as usize] += 1;
                continue;
            }
            let length = t.length();
            let offset = t.offset();
            self.literal_freq[LENGTH_CODES_START + length_code(length) as usize] += 1;
            self.offset_freq[offset_code(offset) as usize] += 1;
        }
        let mut num_literals = self.literal_freq.len();
        while self.literal_freq[num_literals - 1] == 0 {
            num_literals -= 1;
        }
        let mut num_offsets = self.offset_freq.len();
        while num_offsets > 0 && self.offset_freq[num_offsets - 1] == 0 {
            num_offsets -= 1;
        }
        if num_offsets == 0 {
            self.offset_freq[0] = 1;
            num_offsets = 1;
        }
        self.literal_encoding.generate(&self.literal_freq, 15);
        self.offset_encoding.generate(&self.offset_freq, 15);
        (num_literals, num_offsets)
    }

    /// go `writeTokens`.
    fn write_tokens_with(&mut self, tokens: &[Token], le_codes: &[Hcode], oe_codes: &[Hcode]) {
        for t in tokens {
            if !t.is_match() {
                self.write_code(le_codes[t.literal() as usize]);
                continue;
            }
            let length = t.length();
            let length_code = length_code(length) as usize;
            self.write_code(le_codes[length_code + 257]);
            let extra_length_bits = LENGTH_EXTRA_BITS[length_code] as u32;
            if extra_length_bits > 0 {
                let extra_length = (length - LENGTH_BASE[length_code]) as i32;
                self.write_bits(extra_length, extra_length_bits);
            }
            let offset = t.offset();
            let offset_code = offset_code(offset) as usize;
            self.write_code(oe_codes[offset_code]);
            let extra_offset_bits = OFFSET_EXTRA_BITS[offset_code] as u32;
            if extra_offset_bits > 0 {
                let extra_offset = (offset - OFFSET_BASE[offset_code]) as i32;
                self.write_bits(extra_offset, extra_offset_bits);
            }
        }
    }

    /// go `writeBlock` (huffman_bit_writer.go): the dynamic/fixed/stored
    /// decision and the block emission.
    fn write_block(&mut self, mut tokens: Vec<Token>, eof: bool, input: Option<&[u8]>) {
        tokens.push(literal_token(256)); // endBlockMarker
        let (num_literals, num_offsets) = self.index_tokens(&tokens);
        let mut extra_bits = 0usize;
        let (stored_size, storable) = self.stored_size(input);
        if storable {
            for length_code in (257 + 8)..num_literals {
                extra_bits += self.literal_freq[length_code] as usize
                    * LENGTH_EXTRA_BITS[length_code - 257] as usize;
            }
            for offset_code in 4..num_offsets {
                extra_bits += self.offset_freq[offset_code] as usize
                    * OFFSET_EXTRA_BITS[offset_code] as usize;
            }
        }
        let mut using_fixed = true;
        let mut size = self.fixed_size(extra_bits);
        self.generate_codegen(num_literals, num_offsets);
        self.codegen_encoding.generate(&self.codegen_freq, 7);
        let (dynamic_size, num_codegens) = self.dynamic_size(extra_bits);
        if dynamic_size < size {
            size = dynamic_size;
            using_fixed = false;
        }
        if storable && stored_size < size {
            self.write_stored_header(input.map_or(0, <[u8]>::len), eof);
            if let Some(input) = input {
                self.write_bytes(input);
            }
            return;
        }
        if using_fixed {
            self.write_fixed_header(eof);
            // go writes the tokens through the FIXED tables here.
            let fixed_lit = fixed_literal_encoding();
            let fixed_off = fixed_offset_encoding();
            let (le_codes, oe_codes): (Vec<Hcode>, Vec<Hcode>) =
                (fixed_lit.codes.clone(), fixed_off.codes.clone());
            self.write_tokens_with(&tokens, &le_codes, &oe_codes);
            return;
        }
        self.write_dynamic_header(num_literals, num_offsets, num_codegens, eof);
        let literal_codes = self.literal_encoding.codes.clone();
        let offset_codes = self.offset_encoding.codes.clone();
        self.write_tokens_with(&tokens, &literal_codes, &offset_codes);
    }
}

/// go `compressor` at level 6 (`good 8, lazy 16, nice 128, chain 128,
/// skipNever`), writing through [`HuffmanBitWriter`]. The window/hash-chain
/// arrays are boxed: 1<<17 + 1<<15 u32s exceed a comfortable stack frame.
struct Compressor {
    w: HuffmanBitWriter,
    // go `compressionLevel{6, 8, 16, 128, 128, skipNever}`.
    good: i32,
    lazy: i32,
    nice: i32,
    chain: i32,
    fast_skip_hashing: i32,
    chain_head: i32,
    hash_head: Box<[u32; HASH_SIZE]>,
    hash_prev: Box<[u32; WINDOW_SIZE]>,
    hash_offset: usize,
    index: usize,
    window: Box<[u8; 2 * WINDOW_SIZE]>,
    window_end: usize,
    block_start: usize,
    byte_available: bool,
    tokens: Vec<Token>,
    length: usize,
    offset: usize,
    max_insert_index: usize,
    sync: bool,
    scratch: Vec<LiteralNode>,
}

const HASH_MATCH_LEN: usize = MAX_MATCH_LENGTH - 1;

impl Compressor {
    /// go `initDeflate`.
    fn new_level6() -> Self {
        Self {
            w: HuffmanBitWriter::new(),
            good: 8,
            lazy: 16,
            nice: 128,
            chain: 128,
            fast_skip_hashing: SKIP_NEVER,
            chain_head: -1,
            hash_head: Box::new([0; HASH_SIZE]),
            hash_prev: Box::new([0; WINDOW_SIZE]),
            hash_offset: 1,
            index: 0,
            window: Box::new([0; 2 * WINDOW_SIZE]),
            window_end: 0,
            block_start: 0,
            byte_available: false,
            tokens: Vec::with_capacity(MAX_FLATE_BLOCK_TOKENS + 1),
            length: MIN_MATCH_LENGTH - 1,
            offset: 0,
            max_insert_index: 0,
            sync: false,
            scratch: Vec::new(),
        }
    }

    /// go `writeBlock` (deflate.go): bounds the block's input window slice.
    fn write_block_at(&mut self, tokens: Vec<Token>, index: usize) {
        if index > 0 {
            let window: Option<&[u8]> = if self.block_start <= index {
                Some(&self.window[self.block_start..index])
            } else {
                None
            };
            self.block_start = index;
            let input = window.map(|window| window.to_vec());
            self.w.write_block(tokens, false, input.as_deref());
        }
    }

    /// go `fillDeflate`.
    fn fill_deflate(&mut self, b: &[u8]) -> usize {
        if self.index >= 2 * WINDOW_SIZE - (MIN_MATCH_LENGTH + MAX_MATCH_LENGTH) {
            // shift the window by windowSize
            self.window.copy_within(WINDOW_SIZE..2 * WINDOW_SIZE, 0);
            self.index -= WINDOW_SIZE;
            self.window_end -= WINDOW_SIZE;
            if self.block_start >= WINDOW_SIZE {
                self.block_start -= WINDOW_SIZE;
            } else {
                self.block_start = usize::MAX;
            }
            self.hash_offset += WINDOW_SIZE;
            if self.hash_offset > MAX_HASH_OFFSET {
                let delta = self.hash_offset - 1;
                self.hash_offset -= delta;
                self.chain_head -= delta as i32;
                for v in self.hash_prev.iter_mut() {
                    if (*v as usize) > delta {
                        *v -= delta as u32;
                    } else {
                        *v = 0;
                    }
                }
                for v in self.hash_head.iter_mut() {
                    if (*v as usize) > delta {
                        *v -= delta as u32;
                    } else {
                        *v = 0;
                    }
                }
            }
        }
        let n = (b.len()).min(self.window.len() - self.window_end);
        self.window[self.window_end..self.window_end + n].copy_from_slice(&b[..n]);
        self.window_end += n;
        n
    }

    /// go `fillWindow`: prime the hash chains for a dictionary (unused by
    /// COMPRESS, kept for the transcription's completeness boundary).
    fn fill_window(&mut self, b: &[u8]) {
        if self.index != 0 || self.window_end != 0 {
            return;
        }
        let b = if b.len() > WINDOW_SIZE {
            &b[b.len() - WINDOW_SIZE..]
        } else {
            b
        };
        let n = b.len();
        self.window[..n].copy_from_slice(b);
        let loops = (n + 256 - MIN_MATCH_LENGTH) / 256;
        for j in 0..loops {
            let index = j * 256;
            let end = (index + 256 + MIN_MATCH_LENGTH - 1).min(n);
            if end <= index + MIN_MATCH_LENGTH - 1 {
                continue;
            }
            let to_check = &self.window[index..end];
            let dst_size = to_check.len() - MIN_MATCH_LENGTH + 1;
            if dst_size == 0 {
                continue;
            }
            let mut dst = [0u32; HASH_MATCH_LEN];
            bulk_hash4(to_check, &mut dst[..dst_size]);
            for (i, val) in dst.iter().take(dst_size).enumerate() {
                let di = i + index;
                let hh = &mut self.hash_head[(val & (HASH_MASK as u32)) as usize];
                self.hash_prev[di & WINDOW_MASK] = *hh;
                *hh = (di + self.hash_offset) as u32;
            }
        }
        self.window_end = n;
        self.index = n;
    }

    /// go `findMatch`.
    fn find_match(
        &self,
        pos: usize,
        prev_head: i32,
        prev_length: usize,
        lookahead: usize,
    ) -> (usize, usize, bool) {
        let mut min_match_look = MAX_MATCH_LENGTH;
        if lookahead < min_match_look {
            min_match_look = lookahead;
        }
        let win = &self.window[0..pos + min_match_look];
        let mut nice = win.len() - pos;
        if (self.nice as usize) < nice {
            nice = self.nice as usize;
        }
        let mut tries = self.chain as usize;
        let mut length = prev_length;
        if length >= self.good as usize {
            tries >>= 2;
        }
        let w_end = win[pos + length];
        let min_index = pos.saturating_sub(WINDOW_SIZE) as i32;
        let mut offset = 0usize;
        let mut ok = false;
        let mut i = prev_head;
        while tries > 0 {
            let iu = i as usize;
            if w_end == win[iu + length] {
                let n = match_len(&win[iu..], &win[pos..], min_match_look);
                if n > length && (n > MIN_MATCH_LENGTH || pos - iu <= 4096) {
                    length = n;
                    offset = pos - iu;
                    ok = true;
                    if n >= nice {
                        break;
                    }
                }
            }
            if i == min_index {
                // hashPrev[i & windowMask] has already been overwritten.
                break;
            }
            i = self.hash_prev[iu & WINDOW_MASK] as i32 - self.hash_offset as i32;
            if i < min_index || i < 0 {
                break;
            }
        }
        (length, offset, ok)
    }

    /// go `deflate` (the level-6 step), transcribed loop for loop.
    fn deflate_step(&mut self) {
        if self.window_end - self.index < MIN_MATCH_LENGTH + MAX_MATCH_LENGTH && !self.sync {
            return;
        }
        self.max_insert_index = self.window_end - (MIN_MATCH_LENGTH - 1);
        loop {
            let lookahead = self.window_end - self.index;
            if lookahead < MIN_MATCH_LENGTH + MAX_MATCH_LENGTH {
                if !self.sync {
                    break;
                }
                if lookahead == 0 {
                    if self.byte_available {
                        self.tokens
                            .push(literal_token(self.window[self.index - 1] as u32));
                        self.byte_available = false;
                    }
                    if !self.tokens.is_empty() {
                        let tokens = std::mem::take(&mut self.tokens);
                        self.write_block_at(tokens, self.index);
                        self.tokens = Vec::with_capacity(MAX_FLATE_BLOCK_TOKENS + 1);
                    }
                    break;
                }
            }
            if self.index < self.max_insert_index {
                let hash = hash4(&self.window[self.index..self.index + MIN_MATCH_LENGTH]);
                let hh = &mut self.hash_head[(hash as usize & HASH_MASK)];
                self.chain_head = *hh as i32;
                self.hash_prev[self.index & WINDOW_MASK] = self.chain_head as u32;
                *hh = (self.index + self.hash_offset) as u32;
            }
            let prev_length = self.length;
            let prev_offset = self.offset;
            self.length = MIN_MATCH_LENGTH - 1;
            self.offset = 0;
            let min_index = self.index.saturating_sub(WINDOW_SIZE);
            if self.chain_head - self.hash_offset as i32 >= min_index as i32
                && lookahead > prev_length
                && prev_length < self.lazy as usize
            {
                let (new_length, new_offset, ok) = self.find_match(
                    self.index,
                    self.chain_head - self.hash_offset as i32,
                    MIN_MATCH_LENGTH - 1,
                    lookahead,
                );
                if ok {
                    self.length = new_length;
                    self.offset = new_offset;
                }
            }
            if prev_length >= MIN_MATCH_LENGTH && self.length <= prev_length {
                // The previous match is better: emit it.
                self.tokens.push(match_token(
                    (prev_length - BASE_MATCH_LENGTH) as u32,
                    (prev_offset - BASE_MATCH_OFFSET) as u32,
                ));
                // Insert into the hash table everything up to the end of the
                // match; without enough lookahead the last two strings stay
                // out of the table.
                let new_index = self.index + prev_length - 1;
                let mut index = self.index;
                index += 1;
                while index < new_index {
                    if index < self.max_insert_index {
                        let hash = hash4(&self.window[index..index + MIN_MATCH_LENGTH]);
                        let hh = &mut self.hash_head[(hash as usize & HASH_MASK)];
                        self.hash_prev[index & WINDOW_MASK] = *hh;
                        *hh = (index + self.hash_offset) as u32;
                    }
                    index += 1;
                }
                self.index = index;
                self.byte_available = false;
                self.length = MIN_MATCH_LENGTH - 1;
            } else {
                // go: `if d.fastSkipHashing != skipNever || d.byteAvailable`
                // — at level 6 only a PENDING byte emits here; the very first
                // loop iteration just advances the index.
                if self.byte_available {
                    let i = self.index - 1;
                    self.tokens.push(literal_token(self.window[i] as u32));
                    if self.tokens.len() == MAX_FLATE_BLOCK_TOKENS {
                        let tokens = std::mem::take(&mut self.tokens);
                        self.write_block_at(tokens, i + 1);
                        self.tokens = Vec::with_capacity(MAX_FLATE_BLOCK_TOKENS + 1);
                    }
                }
                self.index += 1;
                self.byte_available = true;
            }
            if self.tokens.len() == MAX_FLATE_BLOCK_TOKENS {
                let tokens = std::mem::take(&mut self.tokens);
                let index = self.index;
                self.write_block_at(tokens, index);
                self.tokens = Vec::with_capacity(MAX_FLATE_BLOCK_TOKENS + 1);
            }
        }
    }

    /// go `compressor.close`: drain with sync, then the final empty STORED
    /// block (`writeStoredHeader(0, true)`).
    fn close(&mut self) {
        self.sync = true;
        self.deflate_step();
        self.w.write_stored_header(0, true);
        self.w.flush();
    }
}

/// go `hash4`.
fn hash4(b: &[u8]) -> u32 {
    ((u32::from(b[3]) | u32::from(b[2]) << 8 | u32::from(b[1]) << 16 | u32::from(b[0]) << 24)
        .wrapping_mul(HASHMUL))
        >> (32 - HASH_BITS)
}

/// go `bulkHash4`.
fn bulk_hash4(b: &[u8], dst: &mut [u32]) {
    if b.len() < MIN_MATCH_LENGTH {
        return;
    }
    let mut hb =
        u32::from(b[3]) | u32::from(b[2]) << 8 | u32::from(b[1]) << 16 | u32::from(b[0]) << 24;
    dst[0] = (hb.wrapping_mul(HASHMUL)) >> (32 - HASH_BITS);
    let end = b.len() - MIN_MATCH_LENGTH + 1;
    for i in 1..end {
        hb = (hb << 8) | u32::from(b[i + 3]);
        dst[i] = (hb.wrapping_mul(HASHMUL)) >> (32 - HASH_BITS);
    }
}

/// go `matchLen`.
fn match_len(a: &[u8], b: &[u8], max: usize) -> usize {
    let a = &a[..max];
    let b = &b[..a.len()];
    for (i, av) in a.iter().enumerate() {
        if b[i] != *av {
            return i;
        }
    }
    max
}

/// go `zlib.NewWriter` + `Write` + `Close` at level 6: the full zlib stream
/// (header + deflate + adler32) COMPRESS's `deflate()` helper produces.
pub fn go_zlib_deflate(data: &[u8]) -> Vec<u8> {
    let mut compressor = Compressor::new_level6();
    // zlib.NewWriter's header.
    let mut out = vec![0x78, 0x9c];
    compressor.w.out = std::mem::take(&mut out);
    // Writer.Write: step then fill until the input drains.
    let mut b = data;
    while !b.is_empty() {
        compressor.deflate_step();
        let n = compressor.fill_deflate(b);
        b = &b[n..];
    }
    // Writer.Close: sync drain + final empty stored block; then the adler.
    compressor.close();
    let mut out = std::mem::take(&mut compressor.w.out);
    let adler = adler32(data);
    out.extend_from_slice(&adler.to_be_bytes());
    out
}

/// go `adler32` (hash/adler32.go): `s = (1 + Σ b[i])`, `s1/s2` rolling.
fn adler32(data: &[u8]) -> u32 {
    const MOD: u32 = 65521;
    let mut s1: u32 = 1;
    let mut s2: u32 = 0;
    for byte in data {
        s1 += u32::from(*byte);
        if s1 >= MOD {
            s1 -= MOD;
        }
        s2 += s1;
        if s2 >= MOD {
            s2 -= MOD;
        }
    }
    (s2 << 16) | s1
}

#[cfg(test)]
mod tests {
    use super::go_zlib_deflate;

    /// Captured from `go run` against compress/zlib (NewWriter + Write +
    /// Close), which is TiDB's `deflate()` helper byte for byte.
    #[test]
    fn compresses_like_go_zlib() {
        assert_eq!(
            go_zlib_deflate(b"aaaaaaaaaa"),
            [
                0x78, 0x9c, 0x4a, 0x84, 0x03, 0x40, 0x00, 0x00, 0x00, 0xff, 0xff, 0x14, 0xe1, 0x03,
                0xcb,
            ]
        );
        assert_eq!(
            go_zlib_deflate(b"hello world"),
            [
                0x78, 0x9c, 0xca, 0x48, 0xcd, 0xc9, 0xc9, 0x57, 0x28, 0xcf, 0x2f, 0xca, 0x49, 0x01,
                0x04, 0x00, 0x00, 0xff, 0xff, 0x1a, 0x0b, 0x04, 0x5d,
            ]
        );
    }
}
