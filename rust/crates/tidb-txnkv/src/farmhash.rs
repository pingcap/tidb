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
// See the License for the specific language governing permissions and
// limitations under the License.

//! FarmHash fingerprint used by TiKV's deadlock detector.

const K0: u64 = 0xc3a5_c85c_97cb_3127;
const K1: u64 = 0xb492_b66f_be98_f273;
const K2: u64 = 0x9ae1_6a3b_2f90_404f;

fn load(input: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(input[offset..offset + 8].try_into().expect("eight bytes"))
}

const fn shift_mix(value: u64) -> u64 {
    value ^ (value >> 47)
}

fn hash_len_16_mul(u: u64, v: u64, mul: u64) -> u64 {
    let mut a = (u ^ v).wrapping_mul(mul);
    a ^= a >> 47;
    let mut b = (v ^ a).wrapping_mul(mul);
    b ^= b >> 47;
    b.wrapping_mul(mul)
}

fn hash_len_0_to_16(input: &[u8]) -> u64 {
    let len = input.len();
    if len >= 8 {
        let mul = K2.wrapping_add((len as u64).wrapping_mul(2));
        let a = load(input, 0).wrapping_add(K2);
        let b = load(input, len - 8);
        let c = b.rotate_right(37).wrapping_mul(mul).wrapping_add(a);
        let d = a.rotate_right(25).wrapping_add(b).wrapping_mul(mul);
        return hash_len_16_mul(c, d, mul);
    }
    if len >= 4 {
        let mul = K2.wrapping_add((len as u64).wrapping_mul(2));
        let a = u32::from_le_bytes(input[..4].try_into().expect("four bytes"));
        let b = u32::from_le_bytes(input[len - 4..].try_into().expect("four bytes"));
        return hash_len_16_mul(len as u64 + (u64::from(a) << 3), u64::from(b), mul);
    }
    if len > 0 {
        let a = u32::from(input[0]);
        let b = u32::from(input[len >> 1]);
        let c = u32::from(input[len - 1]);
        let y = a + (b << 8);
        let z = len as u32 + (c << 2);
        return shift_mix(u64::from(y).wrapping_mul(K2) ^ u64::from(z).wrapping_mul(K0))
            .wrapping_mul(K2);
    }
    K2
}

fn hash_len_17_to_32(input: &[u8]) -> u64 {
    let len = input.len();
    let mul = K2.wrapping_add((len as u64).wrapping_mul(2));
    let a = load(input, 0).wrapping_mul(K1);
    let b = load(input, 8);
    let c = load(input, len - 8).wrapping_mul(mul);
    let d = load(input, len - 16).wrapping_mul(K2);
    hash_len_16_mul(
        a.wrapping_add(b)
            .rotate_right(43)
            .wrapping_add(c.rotate_right(30))
            .wrapping_add(d),
        a.wrapping_add(b.wrapping_add(K2).rotate_right(18))
            .wrapping_add(c),
        mul,
    )
}

fn weak_hash_len_32_with_seeds(input: &[u8], mut a: u64, mut b: u64) -> (u64, u64) {
    let w = load(input, 0);
    let x = load(input, 8);
    let y = load(input, 16);
    let z = load(input, 24);
    a = a.wrapping_add(w);
    b = b.wrapping_add(a).wrapping_add(z).rotate_right(21);
    let c = a;
    a = a.wrapping_add(x).wrapping_add(y);
    b = b.wrapping_add(a.rotate_right(44));
    (a.wrapping_add(z), b.wrapping_add(c))
}

fn hash_len_33_to_64(input: &[u8]) -> u64 {
    let len = input.len();
    let mul = K2.wrapping_add((len as u64).wrapping_mul(2));
    let a = load(input, 0).wrapping_mul(K2);
    let b = load(input, 8);
    let c = load(input, len - 8).wrapping_mul(mul);
    let d = load(input, len - 16).wrapping_mul(K2);
    let y = a
        .wrapping_add(b)
        .rotate_right(43)
        .wrapping_add(c.rotate_right(30))
        .wrapping_add(d);
    let z = hash_len_16_mul(
        y,
        a.wrapping_add(b.wrapping_add(K2).rotate_right(18))
            .wrapping_add(c),
        mul,
    );
    let e = load(input, 16).wrapping_mul(mul);
    let f = load(input, 24);
    let g = y.wrapping_add(load(input, len - 32)).wrapping_mul(mul);
    let h = z.wrapping_add(load(input, len - 24)).wrapping_mul(mul);
    hash_len_16_mul(
        e.wrapping_add(f)
            .rotate_right(43)
            .wrapping_add(g.rotate_right(30))
            .wrapping_add(h),
        e.wrapping_add(f.wrapping_add(a).rotate_right(18))
            .wrapping_add(g),
        mul,
    )
}

/// Returns the stable 64-bit fingerprint TiKV stores in `deadlock_key_hash`.
pub(crate) fn fingerprint64(input: &[u8]) -> u64 {
    let len = input.len();
    if len <= 16 {
        return hash_len_0_to_16(input);
    }
    if len <= 32 {
        return hash_len_17_to_32(input);
    }
    if len <= 64 {
        return hash_len_33_to_64(input);
    }

    let seed = 81_u64;
    let mut v = (0_u64, 0_u64);
    let mut w = (0_u64, 0_u64);
    let mut x = seed.wrapping_mul(K2).wrapping_add(load(input, 0));
    let mut y = seed.wrapping_mul(K1).wrapping_add(113);
    let mut z = shift_mix(y.wrapping_mul(K2).wrapping_add(113)).wrapping_mul(K2);
    let end = ((len - 1) / 64) * 64;
    let last64 = end + ((len - 1) & 63) - 63;
    let mut offset = 0;
    while offset < end {
        x = x
            .wrapping_add(y)
            .wrapping_add(v.0)
            .wrapping_add(load(input, offset + 8))
            .rotate_right(37)
            .wrapping_mul(K1);
        y = y
            .wrapping_add(v.1)
            .wrapping_add(load(input, offset + 48))
            .rotate_right(42)
            .wrapping_mul(K1);
        x ^= w.1;
        y = y.wrapping_add(v.0).wrapping_add(load(input, offset + 40));
        z = z.wrapping_add(w.0).rotate_right(33).wrapping_mul(K1);
        v = weak_hash_len_32_with_seeds(
            &input[offset..offset + 32],
            v.1.wrapping_mul(K1),
            x.wrapping_add(w.0),
        );
        w = weak_hash_len_32_with_seeds(
            &input[offset + 32..offset + 64],
            z.wrapping_add(w.1),
            y.wrapping_add(load(input, offset + 16)),
        );
        std::mem::swap(&mut x, &mut z);
        offset += 64;
    }

    let tail = &input[last64..];
    let mul = K1.wrapping_add((z & 0xff) << 1);
    w.0 = w.0.wrapping_add(((len - 1) & 63) as u64);
    v.0 = v.0.wrapping_add(w.0);
    w.0 = w.0.wrapping_add(v.0);
    x = x
        .wrapping_add(y)
        .wrapping_add(v.0)
        .wrapping_add(load(tail, 8))
        .rotate_right(37)
        .wrapping_mul(mul);
    y = y
        .wrapping_add(v.1)
        .wrapping_add(load(tail, 48))
        .rotate_right(42)
        .wrapping_mul(mul);
    x ^= w.1.wrapping_mul(9);
    y = y
        .wrapping_add(v.0.wrapping_mul(9))
        .wrapping_add(load(tail, 40));
    z = z.wrapping_add(w.0).rotate_right(33).wrapping_mul(mul);
    v = weak_hash_len_32_with_seeds(tail, v.1.wrapping_mul(mul), x.wrapping_add(w.0));
    w = weak_hash_len_32_with_seeds(
        &tail[32..],
        z.wrapping_add(w.1),
        y.wrapping_add(load(tail, 16)),
    );
    std::mem::swap(&mut x, &mut z);
    hash_len_16_mul(
        hash_len_16_mul(v.0, w.0, mul)
            .wrapping_add(shift_mix(y).wrapping_mul(K0))
            .wrapping_add(z),
        hash_len_16_mul(v.1, w.1, mul).wrapping_add(x),
        mul,
    )
}

#[cfg(test)]
mod tests {
    use super::fingerprint64;

    #[test]
    fn fingerprints_match_client_go_farmhash() {
        for (len, expected) in [
            (0, 11_160_318_154_034_397_263),
            (1, 4_787_810_249_829_893_994),
            (3, 3_979_540_680_440_595_841),
            (4, 14_708_324_829_907_283_305),
            (7, 10_810_953_559_526_984_053),
            (8, 9_029_840_561_075_318_013),
            (16, 15_579_719_139_305_551_062),
            (17, 6_692_022_265_393_568_454),
            (32, 11_543_571_239_247_179_175),
            (33, 9_480_451_918_888_261_270),
            (64, 4_226_431_515_964_725),
            (65, 18_101_669_290_602_532_278),
            (127, 15_496_381_160_035_339_524),
            (128, 3_742_283_891_066_476_345),
            (129, 14_054_188_206_783_653_085),
        ] {
            let input = (0..len)
                .map(|index| (index * 37 + 11) as u8)
                .collect::<Vec<_>>();
            assert_eq!(fingerprint64(&input), expected, "length {len}");
        }
        assert_eq!(fingerprint64(b"a"), 12_917_804_110_809_363_939);
    }
}
