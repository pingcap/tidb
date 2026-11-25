// Copyright 2011 The Go Authors. All rights reserved.
// Use of this source code is governed by the BSD-style license in LICENSE-GO.

//! The all-zero-key variant of Go crypto/des, used only for Vitess sharding.

const P: [u8; 32] = [
    16, 25, 12, 11, 3, 20, 4, 15, 31, 17, 9, 6, 27, 14, 1, 22, 30, 24, 8, 18, 0, 5, 29, 23, 13, 19,
    2, 26, 10, 21, 28, 7,
];

const S_BOXES: [[[u8; 16]; 4]; 8] = [
    [
        [14, 4, 13, 1, 2, 15, 11, 8, 3, 10, 6, 12, 5, 9, 0, 7],
        [0, 15, 7, 4, 14, 2, 13, 1, 10, 6, 12, 11, 9, 5, 3, 8],
        [4, 1, 14, 8, 13, 6, 2, 11, 15, 12, 9, 7, 3, 10, 5, 0],
        [15, 12, 8, 2, 4, 9, 1, 7, 5, 11, 3, 14, 10, 0, 6, 13],
    ],
    [
        [15, 1, 8, 14, 6, 11, 3, 4, 9, 7, 2, 13, 12, 0, 5, 10],
        [3, 13, 4, 7, 15, 2, 8, 14, 12, 0, 1, 10, 6, 9, 11, 5],
        [0, 14, 7, 11, 10, 4, 13, 1, 5, 8, 12, 6, 9, 3, 2, 15],
        [13, 8, 10, 1, 3, 15, 4, 2, 11, 6, 7, 12, 0, 5, 14, 9],
    ],
    [
        [10, 0, 9, 14, 6, 3, 15, 5, 1, 13, 12, 7, 11, 4, 2, 8],
        [13, 7, 0, 9, 3, 4, 6, 10, 2, 8, 5, 14, 12, 11, 15, 1],
        [13, 6, 4, 9, 8, 15, 3, 0, 11, 1, 2, 12, 5, 10, 14, 7],
        [1, 10, 13, 0, 6, 9, 8, 7, 4, 15, 14, 3, 11, 5, 2, 12],
    ],
    [
        [7, 13, 14, 3, 0, 6, 9, 10, 1, 2, 8, 5, 11, 12, 4, 15],
        [13, 8, 11, 5, 6, 15, 0, 3, 4, 7, 2, 12, 1, 10, 14, 9],
        [10, 6, 9, 0, 12, 11, 7, 13, 15, 1, 3, 14, 5, 2, 8, 4],
        [3, 15, 0, 6, 10, 1, 13, 8, 9, 4, 5, 11, 12, 7, 2, 14],
    ],
    [
        [2, 12, 4, 1, 7, 10, 11, 6, 8, 5, 3, 15, 13, 0, 14, 9],
        [14, 11, 2, 12, 4, 7, 13, 1, 5, 0, 15, 10, 3, 9, 8, 6],
        [4, 2, 1, 11, 10, 13, 7, 8, 15, 9, 12, 5, 6, 3, 0, 14],
        [11, 8, 12, 7, 1, 14, 2, 13, 6, 15, 0, 9, 10, 4, 5, 3],
    ],
    [
        [12, 1, 10, 15, 9, 2, 6, 8, 0, 13, 3, 4, 14, 7, 5, 11],
        [10, 15, 4, 2, 7, 12, 9, 5, 6, 1, 13, 14, 0, 11, 3, 8],
        [9, 14, 15, 5, 2, 8, 12, 3, 7, 0, 4, 10, 1, 13, 11, 6],
        [4, 3, 2, 12, 9, 5, 15, 10, 11, 14, 1, 7, 6, 0, 8, 13],
    ],
    [
        [4, 11, 2, 14, 15, 0, 8, 13, 3, 12, 9, 7, 5, 10, 6, 1],
        [13, 0, 11, 7, 4, 9, 1, 10, 14, 3, 5, 12, 2, 15, 8, 6],
        [1, 4, 11, 13, 12, 3, 7, 14, 10, 15, 6, 8, 0, 5, 9, 2],
        [6, 11, 13, 8, 1, 4, 10, 7, 9, 5, 0, 15, 14, 2, 3, 12],
    ],
    [
        [13, 2, 8, 4, 6, 15, 11, 1, 10, 9, 3, 14, 5, 0, 12, 7],
        [1, 15, 13, 8, 10, 3, 7, 4, 12, 5, 6, 11, 0, 14, 9, 2],
        [7, 11, 4, 1, 9, 12, 14, 2, 0, 6, 10, 13, 15, 3, 5, 8],
        [2, 1, 14, 7, 4, 10, 8, 13, 15, 12, 9, 0, 3, 5, 6, 11],
    ],
];

const fn make_feistel_boxes() -> [[u32; 64]; 8] {
    let mut boxes = [[0; 64]; 8];
    let mut s = 0;
    while s < 8 {
        let mut bits = 0;
        while bits < 64 {
            // Outer bits give the S-box row. Middle bits give the column.
            let row = ((bits >> 4) & 2) | (bits & 1);
            let col = (bits >> 1) & 15;
            let substituted = (S_BOXES[s][row][col] as u32) << (4 * (7 - s));
            let mut permuted = 0_u32;
            let mut p = 0;
            while p < 32 {
                permuted = (permuted << 1) | ((substituted >> P[p]) & 1);
                p += 1;
            }
            // The round halves are represented rotated left by one bit.
            boxes[s][bits] = permuted.rotate_left(1);
            bits += 1;
        }
        s += 1;
    }
    boxes
}

static FEISTEL: [[u32; 64]; 8] = make_feistel_boxes();

#[inline]
fn feistel(half: u32) -> u32 {
    let shifted = half.rotate_right(4);
    FEISTEL[7][(half & 63) as usize]
        ^ FEISTEL[5][((half >> 8) & 63) as usize]
        ^ FEISTEL[3][((half >> 16) & 63) as usize]
        ^ FEISTEL[1][((half >> 24) & 63) as usize]
        ^ FEISTEL[6][(shifted & 63) as usize]
        ^ FEISTEL[4][((shifted >> 8) & 63) as usize]
        ^ FEISTEL[2][((shifted >> 16) & 63) as usize]
        ^ FEISTEL[0][((shifted >> 24) & 63) as usize]
}

// Exchange groups of bits separated by a fixed number of positions.
#[inline]
fn exchange(block: u64, distance: u32, mask: u64) -> u64 {
    let differing = (block ^ (block >> distance)) & mask;
    block ^ differing ^ (differing << distance)
}

#[inline]
fn permute_initial(mut block: u64) -> u64 {
    block = exchange(block, 48, 0x0000_0000_0000_ffff);
    block = exchange(block, 24, 0x0000_0000_ff00_ff00);
    block = exchange(block, 12, 0x0000_f0f0_0000_f0f0);
    block = exchange(block, 6, 0x00cc_00cc_00cc_00cc);
    exchange(block, 33, 0x0000_0000_5555_5555)
}

#[inline]
fn permute_final(mut block: u64) -> u64 {
    block = exchange(block, 33, 0x0000_0000_5555_5555);
    block = exchange(block, 6, 0x00cc_00cc_00cc_00cc);
    block = exchange(block, 12, 0x0000_f0f0_0000_f0f0);
    block = exchange(block, 24, 0x0000_0000_ff00_ff00);
    exchange(block, 48, 0x0000_0000_0000_ffff)
}

pub(super) fn encrypt_null_key(input: u64) -> u64 {
    let block = permute_initial(input);
    let mut left = ((block >> 32) as u32).rotate_left(1);
    let mut right = (block as u32).rotate_left(1);
    // The all-zero key produces zero in every subkey. Each iteration
    // computes two rounds without a swap or a key XOR.
    for _ in 0..8 {
        left ^= feistel(right);
        right ^= feistel(left);
    }
    let swapped = (u64::from(right.rotate_right(1)) << 32) | u64::from(left.rotate_right(1));
    permute_final(swapped)
}
