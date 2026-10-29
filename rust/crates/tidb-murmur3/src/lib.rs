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

//! Native owner of `github.com/twmb/murmur3`'s `Sum32`, the MurmurHash3 TiDB
//! partitions rows with.

/// `github.com/twmb/murmur3.Sum32`: MurmurHash3 x86 32-bit with seed 0.
///
/// The executor's shuffle and the MPP exchange partition rows with it, and the
/// assignment must agree with Go bit for bit -- a different hash silently
/// reshuffles which worker a row lands on.
#[must_use]
pub fn sum32(data: &[u8]) -> u32 {
    const C1: u32 = 0xcc9e_2d51;
    const C2: u32 = 0x1b87_3593;

    let mut h1: u32 = 0;
    let mut blocks = data.chunks_exact(4);
    for block in &mut blocks {
        let mut k1 = u32::from_le_bytes([block[0], block[1], block[2], block[3]]);
        k1 = k1.wrapping_mul(C1).rotate_left(15).wrapping_mul(C2);
        h1 ^= k1;
        h1 = h1.rotate_left(13).wrapping_mul(5).wrapping_add(0xe654_6b64);
    }

    let tail = blocks.remainder();
    let mut k1: u32 = 0;
    if tail.len() >= 3 {
        k1 ^= u32::from(tail[2]) << 16;
    }
    if tail.len() >= 2 {
        k1 ^= u32::from(tail[1]) << 8;
    }
    if !tail.is_empty() {
        k1 ^= u32::from(tail[0]);
        k1 = k1.wrapping_mul(C1).rotate_left(15).wrapping_mul(C2);
        h1 ^= k1;
    }

    h1 ^= data.len() as u32;
    // fmix32
    h1 ^= h1 >> 16;
    h1 = h1.wrapping_mul(0x85eb_ca6b);
    h1 ^= h1 >> 13;
    h1 = h1.wrapping_mul(0xc2b2_ae35);
    h1 ^= h1 >> 16;
    h1
}

#[cfg(test)]
mod tests {
    use super::sum32;

    /// Published MurmurHash3 x86_32 (seed 0) reference vectors, which is what
    /// `github.com/twmb/murmur3.Sum32` computes.
    #[test]
    fn murmur3_matches_reference_vectors() {
        assert_eq!(sum32(b""), 0x0000_0000);
        assert_eq!(sum32(b"a"), 0x3c25_69b2);
        assert_eq!(sum32(b"abc"), 0xb3dd_93fa);
        assert_eq!(sum32(b"abcd"), 0x43ed_676a);
        assert_eq!(sum32(b"hello"), 0x248b_fa47);
        assert_eq!(sum32(b"hello, world"), 0x149b_bb7f);
        assert_eq!(
            sum32(b"The quick brown fox jumps over the lazy dog"),
            0x2e4f_f723
        );
    }

    /// The tail switch has a case per remaining byte; a length sweep proves
    /// none of them was dropped or duplicated.
    #[test]
    fn murmur3_tail_lengths_are_all_distinct() {
        let data = b"0123456789abcdef";
        let hashes: Vec<u32> = (0..=data.len())
            .map(|n| sum32(&data[..n]))
            .collect();
        let mut sorted = hashes.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(sorted.len(), hashes.len());
    }
}
