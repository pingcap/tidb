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

//! Go 1.27.4's default zlib encoder for SQL COMPRESS.
//!
//! TiDB writes each payload once and closes the writer. The level-6 parser
//! finds matches in input blocks and the writer chooses the Go block format
//! and tables. The SQL wrapper adds the length prefix and optional dot.

mod blocks;
mod level6;

use std::sync::LazyLock;

const WINDOW_SIZE: usize = 1 << 15;
const MAX_MATCH_LENGTH: usize = 258;
const MAX_STORE_BLOCK_SIZE: usize = 65_535;

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
#[derive(Clone, Copy, Default)]
struct LiteralNode {
    literal: u16,
    freq: i32,
}

/// Go's canonical Huffman code generator.
struct HuffmanEncoder {
    codes: Vec<Hcode>,
}

const MAX_NUM_LIT: usize = 286;
const MAX_BITS_LIMIT: usize = 16;
const CODEGEN_CODE_COUNT: usize = 19;
const BAD_CODE: u8 = 255;

impl HuffmanEncoder {
    fn covers(&self, freq: &[i32]) -> bool {
        freq.iter()
            .zip(&self.codes)
            .all(|(&n, code)| n == 0 || code.len != 0)
    }

    fn new(size: usize) -> Self {
        Self {
            codes: vec![Hcode::default(); size],
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
    fn bit_counts(list: &[LiteralNode], max_bits_in: i32) -> [i32; MAX_BITS_LIMIT] {
        let mut max_bits = max_bits_in;
        let n = list.len() as i32 - 1;
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
                let count = leaf_counts[level as usize][level as usize];
                leaf_counts[level as usize] = leaf_counts[(level - 1) as usize];
                leaf_counts[level as usize][level as usize] = count;
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
        let mut bit_count = [0; MAX_BITS_LIMIT];
        let mut bits = 1usize;
        for lvl in (1..=max_bits as usize).rev() {
            bit_count[bits] = counts[lvl] - counts[lvl - 1];
            bits += 1;
        }
        bit_count
    }

    /// go `huffmanEncoder.assignEncodingAndSize`.
    fn assign_encoding_and_size(&mut self, bit_count: &[i32], list: &mut [LiteralNode]) {
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
            chunk.sort_unstable_by_key(|node| node.literal);
            for node in chunk.iter() {
                self.codes[node.literal as usize] = Hcode {
                    code: reverse_bits(code, n as u8),
                    len: n as u16,
                };
                code += 1;
            }
            list_len -= bits as usize;
        }
    }

    /// Go `huffmanEncoder.generate`.
    fn generate(&mut self, freq: &[i32], max_bits: i32) {
        let mut list = [LiteralNode::default(); MAX_NUM_LIT + 1];
        let mut count = 0;
        for (i, f) in freq.iter().enumerate() {
            if *f != 0 {
                list[count] = LiteralNode {
                    literal: i as u16,
                    freq: *f,
                };
                count += 1;
            } else {
                self.codes[i].len = 0;
            }
        }
        if count <= 2 {
            for (i, node) in list[..count].iter().enumerate() {
                self.codes[node.literal as usize] = Hcode {
                    code: i as u16,
                    len: 1,
                };
            }
            return;
        }
        // go `byFreq.sort`: frequency first, literal second.
        list[..count].sort_unstable_by_key(|node| (node.freq, node.literal));
        list[count] = LiteralNode {
            literal: u16::MAX,
            freq: i32::MAX,
        };
        let bit_count = Self::bit_counts(&list[..=count], max_bits);
        self.assign_encoding_and_size(&bit_count, &mut list[..count]);
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

fn fixed_literal_encoding() -> &'static HuffmanEncoder {
    static ENCODING: LazyLock<HuffmanEncoder> = LazyLock::new(|| {
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
    });
    &ENCODING
}

fn fixed_offset_encoding() -> &'static HuffmanEncoder {
    static ENCODING: LazyLock<HuffmanEncoder> = LazyLock::new(|| {
        let mut h = HuffmanEncoder::new(30);
        for (ch, code) in h.codes.iter_mut().enumerate() {
            *code = Hcode {
                code: reverse_bits(ch as u16, 5),
                len: 5,
            };
        }
        h
    });
    &ENCODING
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
    temp_literal_encoding: HuffmanEncoder,
    previous_header: usize,
    wrote_huffman: bool,
}

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
            temp_literal_encoding: HuffmanEncoder::new(MAX_NUM_LIT),
            previous_header: 0,
            wrote_huffman: false,
        }
    }

    /// go `flush`: pad the trailing bits into full bytes.
    fn flush(&mut self) {
        while self.nbits != 0 {
            let byte = self.bits as u8;
            self.out.push(byte);
            self.bits >>= 8;
            if self.nbits > 8 {
                self.nbits -= 8;
            } else {
                self.nbits = 0;
            }
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
    fn generate_codegen(&mut self, num_literals: usize, num_offsets: usize, huffman_only: bool) {
        self.codegen_freq = [0; CODEGEN_CODE_COUNT];
        for i in 0..num_literals {
            self.codegen[i] = self.literal_encoding.codes[i].len as u8;
        }
        if huffman_only {
            self.codegen[num_literals] = 1;
        } else {
            for i in 0..num_offsets {
                self.codegen[num_literals + i] = self.offset_encoding.codes[i].len as u8;
            }
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
    fn header_size(&self) -> (usize, usize) {
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
        (header, num_codegens)
    }

    /// go `fixedSize`.
    fn fixed_size(&self, extra_bits: usize) -> usize {
        3 + fixed_literal_encoding().bit_length(&self.literal_freq)
            + fixed_offset_encoding().bit_length(&self.offset_freq)
            + extra_bits
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
        self.close_block();
        if length == 0 && is_eof {
            self.write_fixed_header(true);
            self.write_bits(0, 7);
            self.flush();
            return;
        }
        self.write_bits(i32::from(is_eof), 3);
        self.flush();
        self.write_bits(length as i32, 16);
        self.write_bits(!(length as u16) as i32, 16);
    }

    /// go `writeFixedHeader`.
    fn write_fixed_header(&mut self, is_eof: bool) {
        self.close_block();
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
        self.literal_freq[256] = 1;
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
}

/// go `matchLen`.
fn match_len(a: &[u8], b: &[u8], max: usize) -> usize {
    let a = &a[..max];
    let b = &b[..a.len()];
    let (a_words, a_tail) = a.as_chunks::<8>();
    let (b_words, b_tail) = b.as_chunks::<8>();
    for (i, (&a, &b)) in a_words.iter().zip(b_words).enumerate() {
        let different = u64::from_le_bytes(a) ^ u64::from_le_bytes(b);
        if different != 0 {
            return i * 8 + different.trailing_zeros() as usize / 8;
        }
    }
    a_words.len() * 8
        + a_tail
            .iter()
            .zip(b_tail)
            .position(|(a, b)| a != b)
            .unwrap_or(a_tail.len())
}

/// Reusable state for Go's single-write zlib stream.
pub(crate) struct ZlibEncoder {
    writer: HuffmanBitWriter,
    level6: Option<level6::Level6>,
    tokens: Vec<Token>,
}

impl ZlibEncoder {
    pub(crate) fn new() -> Self {
        Self {
            writer: HuffmanBitWriter::new(),
            level6: None,
            tokens: Vec::new(),
        }
    }

    pub(crate) fn encode(&mut self, data: &[u8], prefix: &[u8]) -> &mut Vec<u8> {
        let writer = &mut self.writer;
        writer.out.clear();
        writer.out.extend_from_slice(prefix);
        writer.out.extend_from_slice(&[0x78, 0x9c]);
        writer.bits = 0;
        writer.nbits = 0;
        writer.previous_header = 0;
        writer.wrote_huffman = false;
        if let Some(level6) = &mut self.level6 {
            level6.reset();
        }

        for (index, input) in data.chunks(MAX_STORE_BLOCK_SIZE).enumerate() {
            let start = index * MAX_STORE_BLOCK_SIZE;
            let end = start + input.len();
            let sync = end == data.len();
            if input.len() <= 32 {
                writer.write_stored_block(input);
                continue;
            }
            if input.len() < 128 {
                writer.write_huffman_block(input, sync);
                continue;
            }
            self.tokens.clear();
            self.level6.get_or_insert_with(level6::Level6::new).encode(
                &mut self.tokens,
                data,
                start,
                end,
            );
            if self.tokens.is_empty() {
                writer.write_stored_block(input);
            } else if self.tokens.len() > input.len() - (input.len() >> 4) {
                writer.write_huffman_block(input, sync);
            } else {
                writer.write_dynamic_block(&mut self.tokens, input, sync);
            }
        }
        writer.write_stored_header(0, true);
        writer.out.extend_from_slice(&adler32(data).to_be_bytes());
        &mut writer.out
    }

    pub(crate) fn into_output(self) -> Vec<u8> {
        self.writer.out
    }
}

/// go `adler32` (hash/adler32.go): `s = (1 + Σ b[i])`, `s1/s2` rolling.
fn adler32(data: &[u8]) -> u32 {
    const MOD: u32 = 65521;
    let mut s1: u32 = 1;
    let mut s2: u32 = 0;
    // RFC 1950 bounds the unreduced sums to u32 over 5,552 bytes.
    for block in data.chunks(5552) {
        for &byte in block {
            s1 += u32::from(byte);
            s2 += s1;
        }
        s1 %= MOD;
        s2 %= MOD;
    }
    (s2 << 16) | s1
}

#[cfg(test)]
mod tests {
    use std::io::Read;

    use sha2::{Digest, Sha256};

    use super::ZlibEncoder;

    #[test]
    fn compresses_like_go_1_27_4_zlib() {
        // Captured from Go 1.27.4 zlib.NewWriter, Write, and Close.
        let cases: &[(&[u8], &[u8])] = &[
            (b"", &[0x78, 0x9c, 0x03, 0x00, 0x00, 0x00, 0x00, 0x01]),
            (
                b"a",
                &[
                    0x78, 0x9c, 0x00, 0x01, 0x00, 0xfe, 0xff, 0x61, 0x03, 0x00, 0x00, 0x62, 0x00,
                    0x62,
                ],
            ),
            (
                b"aaaaaaaaaa",
                &[
                    0x78, 0x9c, 0x00, 0x0a, 0x00, 0xf5, 0xff, b'a', b'a', b'a', b'a', b'a', b'a',
                    b'a', b'a', b'a', b'a', 0x03, 0x00, 0x14, 0xe1, 0x03, 0xcb,
                ],
            ),
            (
                b"hello world",
                &[
                    0x78, 0x9c, 0x00, 0x0b, 0x00, 0xf4, 0xff, b'h', b'e', b'l', b'l', b'o', b' ',
                    b'w', b'o', b'r', b'l', b'd', 0x03, 0x00, 0x1a, 0x0b, 0x04, 0x5d,
                ],
            ),
        ];
        let mut encoder = ZlibEncoder::new();
        for &(input, expected) in cases {
            let compressed = encoder.encode(input, &[]);
            assert_eq!(compressed, expected, "{input:?}");
            let mut inflated = Vec::new();
            flate2::read::ZlibDecoder::new(compressed.as_slice())
                .read_to_end(&mut inflated)
                .unwrap();
            assert_eq!(inflated, input);
        }
    }

    #[test]
    fn go_1_27_4_multiblock_streams_match_after_reset() {
        let tile = (0..274).map(|i| (i * 31 + i / 7) as u8).collect::<Vec<_>>();
        let mut encoder = ZlibEncoder::new();
        // Lengths and hashes captured from Go 1.27.4. Both values cross the
        // match history boundary and use more than one DEFLATE block.
        for (input, size, sha256) in [
            (
                b"tidb-zlib: stored,huffman,match; ".repeat(13_000),
                1395,
                "941d4e51dfd98ebbfe041acc0ad80bc430c91e881282fa0bbb8f214ab895d095",
            ),
            (
                tile.repeat(1680),
                2431,
                "76bfe7c6fb114778e53742fd88d0c5e75aebd27ebc6865ac1e023dada82d401c",
            ),
        ] {
            let compressed = encoder.encode(&input, &[]);
            assert_eq!(compressed.len(), size);
            assert_eq!(
                tidb_goruntime::go_hex::encode_to_string(&Sha256::digest(compressed)),
                sha256,
            );
        }
    }
}
