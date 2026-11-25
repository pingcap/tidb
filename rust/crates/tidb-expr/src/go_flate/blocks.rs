// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.
// Ported from Go 1.27.4 compress/flate/huffman_bit_writer.go.

use super::{
    HuffmanBitWriter, LENGTH_CODES_START, LENGTH_EXTRA_BITS, MAX_NUM_LIT, OFFSET_EXTRA_BITS, Token,
    fixed_literal_encoding, fixed_offset_encoding, literal_token,
};

const END_BLOCK: usize = 256;
const MAX_PREDEFINED_TOKENS: usize = 250;
const LOG_NEW_TABLE_PENALTY: u32 = 7;

impl HuffmanBitWriter {
    pub(super) fn close_block(&mut self) {
        if self.previous_header != 0 {
            self.write_code(self.literal_encoding.codes[END_BLOCK]);
            self.previous_header = 0;
        }
    }

    pub(super) fn write_stored_block(&mut self, input: &[u8]) {
        self.write_stored_header(input.len(), false);
        self.write_bytes(input);
    }

    pub(super) fn write_dynamic_block(
        &mut self,
        tokens: &mut Vec<Token>,
        input: &[u8],
        sync: bool,
    ) {
        if sync {
            tokens.push(literal_token(END_BLOCK as u32));
        }
        let (num_literals, num_offsets) = self.index_tokens(tokens);
        if self.previous_header != 0
            && (self.wrote_huffman
                || !self.literal_encoding.covers(&self.literal_freq)
                || !self.offset_encoding.covers(&self.offset_freq))
        {
            self.close_block();
            self.wrote_huffman = false;
        }

        let extra_bits = self.extra_bits();
        let stored_size = (input.len() + 5) * 8;
        if self.previous_header != 0 {
            let mut estimated = self.previous_header + self.estimated_bits(tokens.len());
            estimated += usize::from(self.literal_encoding.codes[END_BLOCK].len)
                + (estimated >> LOG_NEW_TABLE_PENALTY);
            let reused = self.literal_encoding.bit_length(&self.literal_freq)
                + self.offset_encoding.bit_length(&self.offset_freq)
                + extra_bits;
            let mut size = reused;
            if estimated < reused {
                self.close_block();
                size = estimated;
            }

            if tokens.len() < MAX_PREDEFINED_TOKENS && self.fixed_size(extra_bits) + 7 < size {
                if stored_size <= size {
                    self.write_stored_block(input);
                } else {
                    self.write_fixed_block(tokens, sync);
                }
                return;
            }
            if stored_size <= size {
                self.write_stored_block(input);
                return;
            }
        }

        if self.previous_header == 0 {
            self.literal_encoding.generate(&self.literal_freq, 15);
            self.offset_encoding.generate(&self.offset_freq, 15);
            self.generate_codegen(num_literals, num_offsets, false);
            self.codegen_encoding.generate(&self.codegen_freq, 7);
            let (header_size, codegens) = self.header_size();
            let size = header_size
                + self.literal_encoding.bit_length(&self.literal_freq)
                + self.offset_encoding.bit_length(&self.offset_freq)
                + extra_bits;

            if tokens.len() < MAX_PREDEFINED_TOKENS {
                let fixed_size = self.fixed_size(extra_bits);
                if fixed_size <= size {
                    if stored_size <= fixed_size {
                        self.write_stored_block(input);
                    } else {
                        self.write_fixed_block(tokens, sync);
                    }
                    return;
                }
            }
            if stored_size <= size {
                self.write_stored_block(input);
                return;
            }
            self.write_dynamic_header(num_literals, num_offsets, codegens, false);
            if !sync {
                self.previous_header = header_size;
            }
            self.wrote_huffman = false;
        }
        if sync {
            self.previous_header = 0;
        }
        let literals: [_; MAX_NUM_LIT] = self.literal_encoding.codes.as_slice().try_into().unwrap();
        let offsets: [_; 30] = self.offset_encoding.codes.as_slice().try_into().unwrap();
        self.write_tokens_with(tokens, &literals, &offsets);
    }

    fn write_fixed_block(&mut self, tokens: &mut Vec<Token>, sync: bool) {
        self.write_fixed_header(false);
        if !sync {
            tokens.push(literal_token(END_BLOCK as u32));
        }
        self.write_tokens_with(
            tokens,
            &fixed_literal_encoding().codes,
            &fixed_offset_encoding().codes,
        );
    }

    pub(super) fn write_huffman_block(&mut self, input: &[u8], sync: bool) {
        self.literal_freq.fill(0);
        // Multiple independent streams of reads help the histogram on wide blocks.
        if input.len() >= 8192 {
            let (tail, tiles) = input.split_at(input.len() % 4);
            for &byte in tail {
                self.literal_freq[usize::from(byte)] += 1;
            }
            let n = tiles.len() / 4;
            let (a, rest) = tiles.split_at(n);
            let (b, rest) = rest.split_at(n);
            let (c, d) = rest.split_at(n);
            for (((&a, &b), &c), &d) in a.iter().zip(b).zip(c).zip(d) {
                self.literal_freq[usize::from(a)] += 1;
                self.literal_freq[usize::from(b)] += 1;
                self.literal_freq[usize::from(c)] += 1;
                self.literal_freq[usize::from(d)] += 1;
            }
        } else {
            for &byte in input {
                self.literal_freq[usize::from(byte)] += 1;
            }
        }

        if input.len() > 1024 {
            let average = input.len() as f64 / 256.0;
            let threshold = (input.len() * 2) as f64;
            let mut variance = 0.0;
            for &freq in &self.literal_freq[..256] {
                let diff = f64::from(freq) - average;
                variance += diff * diff;
                if variance >= threshold {
                    break;
                }
            }
            if variance < threshold {
                self.write_stored_block(input);
                return;
            }
        }
        self.literal_freq[END_BLOCK] = 1;
        self.temp_literal_encoding
            .generate(&self.literal_freq[..=END_BLOCK], 15);
        let mut estimated = self
            .temp_literal_encoding
            .bit_length(&self.literal_freq[..=END_BLOCK]);
        estimated += if self.previous_header == 0 {
            70 * 8
        } else {
            self.previous_header
        };
        estimated += estimated >> LOG_NEW_TABLE_PENALTY;
        if (input.len() + 5) * 8 <= estimated {
            self.write_stored_block(input);
            return;
        }

        if self.previous_header != 0
            && (!self.literal_encoding.covers(&self.literal_freq[..256])
                || estimated < self.literal_encoding.bit_length(&self.literal_freq[..256]))
        {
            self.close_block();
        }
        if self.previous_header == 0 {
            std::mem::swap(&mut self.literal_encoding, &mut self.temp_literal_encoding);
            self.generate_codegen(END_BLOCK + 1, 1, true);
            self.codegen_encoding.generate(&self.codegen_freq, 7);
            let (header_size, codegens) = self.header_size();
            self.write_dynamic_header(END_BLOCK + 1, 1, codegens, false);
            self.wrote_huffman = true;
            self.previous_header = header_size;
        }
        for &byte in input {
            self.write_code(self.literal_encoding.codes[usize::from(byte)]);
        }
        if sync {
            self.close_block();
            self.wrote_huffman = false;
        }
    }

    fn extra_bits(&self) -> usize {
        extra_bits(&self.literal_freq[LENGTH_CODES_START..], &LENGTH_EXTRA_BITS)
            + extra_bits(&self.offset_freq, &OFFSET_EXTRA_BITS)
    }

    fn estimated_bits(&self, count: usize) -> usize {
        let inv_count = 1.0 / count as f32;
        let mut shannon = 0.0_f32;
        let mut bits = 0;
        let mut matches = 0;
        for &freq in &self.literal_freq[..256] {
            if freq > 0 {
                shannon += estimate_symbol(freq, inv_count);
            }
        }
        shannon += 15.0;
        for (&freq, &extra) in self.literal_freq[LENGTH_CODES_START..]
            .iter()
            .zip(&LENGTH_EXTRA_BITS)
        {
            if freq > 0 {
                shannon += estimate_symbol(freq, inv_count);
                bits += usize::from(extra as u8) * freq as usize;
                matches += freq;
            }
        }
        if matches > 0 {
            let inv_matches = 1.0 / matches as f32;
            for (&freq, &extra) in self.offset_freq.iter().zip(&OFFSET_EXTRA_BITS) {
                if freq > 0 {
                    shannon += estimate_symbol(freq, inv_matches);
                    bits += usize::from(extra as u8) * freq as usize;
                }
            }
        }
        shannon as usize + bits
    }
}

fn extra_bits(freq: &[i32], extra: &[i8]) -> usize {
    freq.iter()
        .zip(extra)
        .map(|(&freq, &bits)| freq as usize * bits as usize)
        .sum()
}

fn estimate_symbol(freq: i32, inverse_total: f32) -> f32 {
    let n = freq as f32;
    let bits = (n * inverse_total).to_bits();
    let exponent = ((bits >> 23 & 255) as i32 - 128) as f32;
    let mantissa = f32::from_bits((bits & 0x807f_ffff) + (127 << 23));
    let log2 = exponent + ((-0.344_848_43 * mantissa + 2.024_665_8) * mantissa - 0.674_877_6);
    (-log2).clamp(1.0, 15.0) * n
}
