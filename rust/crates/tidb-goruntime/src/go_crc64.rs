// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

//! The built-in ISO and ECMA polynomials of Go `hash/crc64`.

type SliceTables = [[u64; 256]; 8];

static ECMA: [SliceTables; 2] = slicing_tables(0xC96C_5795_D787_0F42);
static ISO: [SliceTables; 2] = slicing_tables(0xD800_0000_0000_0000);

const fn slicing_tables(polynomial: u64) -> [SliceTables; 2] {
    let mut tables = [[[0; 256]; 8]; 2];
    let mut byte = 0;
    while byte < 256 {
        let mut crc = byte as u64;
        let mut bit = 0;
        while bit < 8 {
            crc = (crc >> 1) ^ (polynomial & (crc & 1).wrapping_neg());
            bit += 1;
        }
        tables[0][0][byte] = crc;
        byte += 1;
    }

    let mut slice = 1;
    while slice < 16 {
        let mut byte = 0;
        while byte < 256 {
            let previous = slice - 1;
            let crc = tables[previous / 8][previous % 8][byte];
            tables[slice / 8][slice % 8][byte] = tables[0][0][(crc as u8) as usize] ^ (crc >> 8);
            byte += 1;
        }
        slice += 1;
    }
    tables
}

/// Go `crc64.Update(crc, crc64.MakeTable(crc64.ECMA), data)`.
#[inline]
pub fn update_ecma(crc: u64, data: &[u8]) -> u64 {
    update(crc, data, &ECMA)
}

/// Go `crc64.Update(crc, crc64.MakeTable(crc64.ISO), data)`.
#[inline]
pub fn update_iso(crc: u64, data: &[u8]) -> u64 {
    update(crc, data, &ISO)
}

#[inline]
fn update(crc: u64, data: &[u8], [low, high]: &[SliceTables; 2]) -> u64 {
    let mut crc = !crc;
    // Extend Go's slicing-by-8 loop with two independent halves per iteration.
    let (blocks, tail) = data.as_chunks::<16>();
    for block in blocks {
        let first = u64::from_le_bytes(block[..8].try_into().unwrap());
        let second = u64::from_le_bytes(block[8..].try_into().unwrap());
        crc = slice8(crc ^ first, high) ^ slice8(second, low);
    }
    let (words, tail) = tail.as_chunks::<8>();
    for word in words {
        crc = slice8(crc ^ u64::from_le_bytes(*word), low);
    }
    for &byte in tail {
        crc = low[0][((crc as u8) ^ byte) as usize] ^ (crc >> 8);
    }
    !crc
}

#[inline]
fn slice8(word: u64, tables: &SliceTables) -> u64 {
    let [a, b, c, d, e, f, g, h] = word.to_le_bytes();
    tables[7][a as usize]
        ^ tables[6][b as usize]
        ^ tables[5][c as usize]
        ^ tables[4][d as usize]
        ^ tables[3][e as usize]
        ^ tables[2][f as usize]
        ^ tables[1][g as usize]
        ^ tables[0][h as usize]
}

#[cfg(test)]
mod tests {
    use super::{update_ecma, update_iso};

    #[test]
    fn seeded_updates_match_go_across_split_points() {
        let data = b"abcdef0123456789\x00\xff\x80\xefxyz";
        let seed = 0x0123_4567_89AB_CDEF;
        // Go 1.27.4 hash/crc64.Update with the corresponding polynomial.
        let vectors: [(fn(u64, &[u8]) -> u64, u64); 2] = [
            (update_ecma, 0x64C4_6D89_4EB5_3957),
            (update_iso, 0x6C53_032C_3DE6_0485),
        ];
        for (update, expected) in vectors {
            for split in 0..=data.len() {
                let (prefix, suffix) = data.split_at(split);
                assert_eq!(update(update(seed, prefix), suffix), expected);
            }
        }
    }
}
