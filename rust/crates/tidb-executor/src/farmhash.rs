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

//! A faithful port of `github.com/dgryski/go-farm`'s `Hash64`, the pure-Go
//! transliteration of Google's FarmHash (the `farmhashna`/`farmhashxo`/
//! `farmhashuo` families) that `APPROX_COUNT_DISTINCT` hashes its encoded
//! argument tuple through (Go `func_count_distinct.go`'s
//! `farm.Hash64(encodedBytes)`).
//!
//! Only `Hash64` (and the internal helpers it calls) is ported: the crate
//! never calls `Hash64WithSeed(s)`, `Hash128`, or the 32-bit family, so those
//! are left out rather than carried as untested dead code.
//!
//! Every routine below is a line-by-line transliteration of the
//! corresponding Go function so the arithmetic -- including the exact rotate
//! amounts, magic constants, and byte offsets -- matches bit for bit.

const K0: u64 = 0xc3a5_c85c_97cb_3127;
const K1: u64 = 0xb492_b66f_be98_f273;
const K2: u64 = 0x9ae1_6a3b_2f90_404f;

#[inline]
fn shift_mix(val: u64) -> u64 {
    val ^ (val >> 47)
}

#[inline]
fn hash128to64(lo: u64, hi: u64) -> u64 {
    const MUL: u64 = 0x9ddf_ea08_eb38_2d69;
    let mut a = (lo ^ hi).wrapping_mul(MUL);
    a ^= a >> 47;
    let mut b = (hi ^ a).wrapping_mul(MUL);
    b ^= b >> 47;
    b = b.wrapping_mul(MUL);
    b
}

#[inline]
fn hash_len16(u: u64, v: u64) -> u64 {
    hash128to64(u, v)
}

#[inline]
fn hash_len16_mul(u: u64, v: u64, mul: u64) -> u64 {
    // Murmur-inspired hashing.
    let mut a = (u ^ v).wrapping_mul(mul);
    a ^= a >> 47;
    let mut b = (v ^ a).wrapping_mul(mul);
    b ^= b >> 47;
    b = b.wrapping_mul(mul);
    b
}

#[inline]
fn le_u32(s: &[u8]) -> u32 {
    u32::from_le_bytes(s[0..4].try_into().unwrap())
}

#[inline]
fn le_u64(s: &[u8]) -> u64 {
    u64::from_le_bytes(s[0..8].try_into().unwrap())
}

fn hash_len0to16(s: &[u8]) -> u64 {
    let slen = s.len() as u64;
    if slen >= 8 {
        let mul = K2.wrapping_add(slen.wrapping_mul(2));
        let a = le_u64(&s[0..8]).wrapping_add(K2);
        let b = le_u64(&s[(slen as usize - 8)..]);
        let c = b.rotate_right(37).wrapping_mul(mul).wrapping_add(a);
        let d = (a.rotate_right(25).wrapping_add(b)).wrapping_mul(mul);
        return hash_len16_mul(c, d, mul);
    }
    if slen >= 4 {
        let mul = K2.wrapping_add(slen.wrapping_mul(2));
        let a = le_u32(&s[0..4]);
        return hash_len16_mul(
            slen.wrapping_add((a as u64) << 3),
            le_u32(&s[(slen as usize - 4)..]) as u64,
            mul,
        );
    }
    if slen > 0 {
        let a = s[0];
        let b = s[(slen >> 1) as usize];
        let c = s[(slen - 1) as usize];
        let y = a as u32 + ((b as u32) << 8);
        let z = slen as u32 + ((c as u32) << 2);
        return shift_mix((y as u64).wrapping_mul(K2) ^ (z as u64).wrapping_mul(K0))
            .wrapping_mul(K2);
    }
    K2
}

fn hash_len17to32(s: &[u8]) -> u64 {
    let slen = s.len();
    let mul = K2.wrapping_add((slen as u64).wrapping_mul(2));
    let a = le_u64(&s[0..8]).wrapping_mul(K1);
    let b = le_u64(&s[8..16]);
    let c = le_u64(&s[slen - 8..]).wrapping_mul(mul);
    let d = le_u64(&s[slen - 16..]).wrapping_mul(K2);
    hash_len16_mul(
        (a.wrapping_add(b))
            .rotate_right(43)
            .wrapping_add(c.rotate_right(30))
            .wrapping_add(d),
        a.wrapping_add((b.wrapping_add(K2)).rotate_right(18))
            .wrapping_add(c),
        mul,
    )
}

#[inline]
fn weak_hash_len32_with_seeds_words(w: u64, x: u64, y: u64, z: u64, a: u64, b: u64) -> (u64, u64) {
    let mut a = a.wrapping_add(w);
    let mut b = (b.wrapping_add(a).wrapping_add(z)).rotate_right(21);
    let c = a;
    a = a.wrapping_add(x);
    a = a.wrapping_add(y);
    b = b.wrapping_add(a.rotate_right(44));
    (a.wrapping_add(z), b.wrapping_add(c))
}

#[inline]
fn weak_hash_len32_with_seeds(s: &[u8], a: u64, b: u64) -> (u64, u64) {
    weak_hash_len32_with_seeds_words(
        le_u64(&s[0..8]),
        le_u64(&s[8..16]),
        le_u64(&s[16..24]),
        le_u64(&s[24..32]),
        a,
        b,
    )
}

fn h32(s: &[u8], mul: u64) -> u64 {
    let slen = s.len();
    let a = le_u64(&s[0..8]).wrapping_mul(K1);
    let b = le_u64(&s[8..16]);
    let c = le_u64(&s[slen - 8..]).wrapping_mul(mul);
    let d = le_u64(&s[slen - 16..]).wrapping_mul(K2);
    let u = (a.wrapping_add(b))
        .rotate_right(43)
        .wrapping_add(c.rotate_right(30))
        .wrapping_add(d);
    let v = a
        .wrapping_add((b.wrapping_add(K2)).rotate_right(18))
        .wrapping_add(c);
    let a = shift_mix((u ^ v).wrapping_mul(mul));
    shift_mix((v ^ a).wrapping_mul(mul))
}

fn h32_seeds(s: &[u8], mul: u64, seed0: u64, seed1: u64) -> u64 {
    let slen = s.len();
    let a = le_u64(&s[0..8]).wrapping_mul(K1);
    let b = le_u64(&s[8..16]);
    let c = le_u64(&s[slen - 8..]).wrapping_mul(mul);
    let d = le_u64(&s[slen - 16..]).wrapping_mul(K2);
    let u = (a.wrapping_add(b))
        .rotate_right(43)
        .wrapping_add(c.rotate_right(30))
        .wrapping_add(d)
        .wrapping_add(seed0);
    let v = a
        .wrapping_add((b.wrapping_add(K2)).rotate_right(18))
        .wrapping_add(c)
        .wrapping_add(seed1);
    let a = shift_mix((u ^ v).wrapping_mul(mul));
    shift_mix((v ^ a).wrapping_mul(mul))
}

fn xo_hash_len33to64(s: &[u8]) -> u64 {
    let slen = s.len();
    let mul0 = K2.wrapping_sub(30);
    let mul1 = K2.wrapping_sub(30).wrapping_add(2 * slen as u64);
    let h0 = h32(&s[0..32], mul0);
    let h1 = h32(&s[slen - 32..], mul1);
    (h1.wrapping_mul(mul1).wrapping_add(h0)).wrapping_mul(mul1)
}

fn xo_hash_len65to96(s: &[u8]) -> u64 {
    let slen = s.len();
    let mul0 = K2.wrapping_sub(114);
    let mul1 = K2.wrapping_sub(114).wrapping_add(2 * slen as u64);
    let h0 = h32(&s[0..32], mul0);
    let h1 = h32(&s[32..64], mul1);
    let h2 = h32_seeds(&s[slen - 32..], mul1, h0, h1);
    (h2.wrapping_mul(9)
        .wrapping_add(h0 >> 17)
        .wrapping_add(h1 >> 21))
    .wrapping_mul(mul1)
}

fn na_hash64(s: &[u8]) -> u64 {
    let slen = s.len();
    let seed: u64 = 81;
    if slen <= 32 {
        if slen <= 16 {
            return hash_len0to16(s);
        }
        return hash_len17to32(s);
    }
    if slen <= 64 {
        return hash_len33to64(s);
    }
    // For strings over 64 bytes we loop.
    // Internal state consists of 56 bytes: v, w, x, y, and z.
    let (mut v_lo, mut v_hi) = (0u64, 0u64);
    let (mut w_lo, mut w_hi) = (0u64, 0u64);
    let mut x = seed.wrapping_mul(K2).wrapping_add(le_u64(&s[0..8]));
    let mut y = seed.wrapping_mul(K1).wrapping_add(113);
    let mut z = shift_mix(y.wrapping_mul(K2).wrapping_add(113)).wrapping_mul(K2);
    // Set end so that after the loop we have 1 to 64 bytes left to process.
    let end_idx = ((slen - 1) / 64) * 64;
    let last64_idx = end_idx + ((slen - 1) & 63) - 63;
    let last64 = &s[last64_idx..];

    let mut cur = s;
    while cur.len() > 64 {
        x = (x
            .wrapping_add(y)
            .wrapping_add(v_lo)
            .wrapping_add(le_u64(&cur[8..16])))
        .rotate_right(37)
        .wrapping_mul(K1);
        y = (y.wrapping_add(v_hi).wrapping_add(le_u64(&cur[48..56])))
            .rotate_right(42)
            .wrapping_mul(K1);
        x ^= w_hi;
        y = y.wrapping_add(v_lo).wrapping_add(le_u64(&cur[40..48]));
        z = (z.wrapping_add(w_lo)).rotate_right(33).wrapping_mul(K1);
        let (nv_lo, nv_hi) =
            weak_hash_len32_with_seeds(cur, v_hi.wrapping_mul(K1), x.wrapping_add(w_lo));
        v_lo = nv_lo;
        v_hi = nv_hi;
        let (nw_lo, nw_hi) = weak_hash_len32_with_seeds(
            &cur[32..],
            z.wrapping_add(w_hi),
            y.wrapping_add(le_u64(&cur[16..24])),
        );
        w_lo = nw_lo;
        w_hi = nw_hi;
        std::mem::swap(&mut x, &mut z);
        cur = &cur[64..];
    }
    let mul = K1.wrapping_add((z & 0xff) << 1);
    // Make s point to the last 64 bytes of input.
    let s = last64;
    w_lo = w_lo.wrapping_add((slen as u64 - 1) & 63);
    v_lo = v_lo.wrapping_add(w_lo);
    w_lo = w_lo.wrapping_add(v_lo);
    x = (x
        .wrapping_add(y)
        .wrapping_add(v_lo)
        .wrapping_add(le_u64(&s[8..16])))
    .rotate_right(37)
    .wrapping_mul(mul);
    y = (y.wrapping_add(v_hi).wrapping_add(le_u64(&s[48..56])))
        .rotate_right(42)
        .wrapping_mul(mul);
    x ^= w_hi.wrapping_mul(9);
    y = y
        .wrapping_add(v_lo.wrapping_mul(9))
        .wrapping_add(le_u64(&s[40..48]));
    z = (z.wrapping_add(w_lo)).rotate_right(33).wrapping_mul(mul);
    let (nv_lo, nv_hi) =
        weak_hash_len32_with_seeds(s, v_hi.wrapping_mul(mul), x.wrapping_add(w_lo));
    v_lo = nv_lo;
    v_hi = nv_hi;
    let (nw_lo, nw_hi) = weak_hash_len32_with_seeds(
        &s[32..],
        z.wrapping_add(w_hi),
        y.wrapping_add(le_u64(&s[16..24])),
    );
    w_lo = nw_lo;
    w_hi = nw_hi;
    std::mem::swap(&mut x, &mut z);
    hash_len16_mul(
        hash_len16_mul(v_lo, w_lo, mul)
            .wrapping_add(shift_mix(y).wrapping_mul(K0))
            .wrapping_add(z),
        hash_len16_mul(v_hi, w_hi, mul).wrapping_add(x),
        mul,
    )
}

fn hash_len33to64(s: &[u8]) -> u64 {
    let slen = s.len();
    let mul = K2.wrapping_add((slen as u64).wrapping_mul(2));
    let a = le_u64(&s[0..8]).wrapping_mul(K2);
    let b = le_u64(&s[8..16]);
    let c = le_u64(&s[slen - 8..]).wrapping_mul(mul);
    let d = le_u64(&s[slen - 16..]).wrapping_mul(K2);
    let y = (a.wrapping_add(b))
        .rotate_right(43)
        .wrapping_add(c.rotate_right(30))
        .wrapping_add(d);
    let z = hash_len16_mul(
        y,
        a.wrapping_add((b.wrapping_add(K2)).rotate_right(18))
            .wrapping_add(c),
        mul,
    );
    let e = le_u64(&s[16..24]).wrapping_mul(mul);
    let f = le_u64(&s[24..32]);
    let g = (y.wrapping_add(le_u64(&s[slen - 32..]))).wrapping_mul(mul);
    let h = (z.wrapping_add(le_u64(&s[slen - 24..]))).wrapping_mul(mul);
    hash_len16_mul(
        (e.wrapping_add(f))
            .rotate_right(43)
            .wrapping_add(g.rotate_right(30))
            .wrapping_add(h),
        e.wrapping_add((f.wrapping_add(a)).rotate_right(18))
            .wrapping_add(g),
        mul,
    )
}

#[inline]
fn uo_h(x: u64, y: u64, mul: u64, r: u32) -> u64 {
    let mut a = (x ^ y).wrapping_mul(mul);
    a ^= a >> 47;
    let b = (y ^ a).wrapping_mul(mul);
    b.rotate_right(r).wrapping_mul(mul)
}

fn hash64_with_seeds(s: &[u8], seed0: u64, seed1: u64) -> u64 {
    let slen = s.len();
    if slen <= 64 {
        return hash_len16(na_hash64(s).wrapping_sub(seed0), seed1);
    }

    // For strings over 64 bytes we loop.
    // Internal state consists of 64 bytes: u, v, w, x, y, and z.
    let mut x = seed0;
    let mut y = seed1.wrapping_mul(K2).wrapping_add(113);
    let mut z = shift_mix(y.wrapping_mul(K2)).wrapping_mul(K2);
    let (mut v_lo, mut v_hi) = (seed0, seed1);
    let (mut w_lo, mut w_hi) = (0u64, 0u64);
    let mut u = x.wrapping_sub(z);
    x = x.wrapping_mul(K2);
    let mul = K2.wrapping_add(u & 0x82);

    // Set end so that after the loop we have 1 to 64 bytes left to process.
    let end_idx = ((slen - 1) / 64) * 64;
    let last64_idx = end_idx + ((slen - 1) & 63) - 63;
    let last64 = &s[last64_idx..];

    let mut cur = s;
    while cur.len() > 64 {
        let a0 = le_u64(&cur[0..8]);
        let a1 = le_u64(&cur[8..16]);
        let a2 = le_u64(&cur[16..24]);
        let a3 = le_u64(&cur[24..32]);
        let a4 = le_u64(&cur[32..40]);
        let a5 = le_u64(&cur[40..48]);
        let a6 = le_u64(&cur[48..56]);
        let a7 = le_u64(&cur[56..64]);
        x = x.wrapping_add(a0).wrapping_add(a1);
        y = y.wrapping_add(a2);
        z = z.wrapping_add(a3);
        v_lo = v_lo.wrapping_add(a4);
        v_hi = v_hi.wrapping_add(a5).wrapping_add(a1);
        w_lo = w_lo.wrapping_add(a6);
        w_hi = w_hi.wrapping_add(a7);

        x = x.rotate_right(26);
        x = x.wrapping_mul(9);
        y = y.rotate_right(29);
        z = z.wrapping_mul(mul);
        v_lo = v_lo.rotate_right(33);
        v_hi = v_hi.rotate_right(30);
        w_lo ^= x;
        w_lo = w_lo.wrapping_mul(9);
        z = z.rotate_right(32);
        z = z.wrapping_add(w_hi);
        w_hi = w_hi.wrapping_add(z);
        z = z.wrapping_mul(9);
        std::mem::swap(&mut u, &mut y);

        z = z.wrapping_add(a0).wrapping_add(a6);
        v_lo = v_lo.wrapping_add(a2);
        v_hi = v_hi.wrapping_add(a3);
        w_lo = w_lo.wrapping_add(a4);
        w_hi = w_hi.wrapping_add(a5).wrapping_add(a6);
        x = x.wrapping_add(a1);
        y = y.wrapping_add(a7);

        y = y.wrapping_add(v_lo);
        v_lo = v_lo.wrapping_add(x.wrapping_sub(y));
        v_hi = v_hi.wrapping_add(w_lo);
        w_lo = w_lo.wrapping_add(v_hi);
        w_hi = w_hi.wrapping_add(x.wrapping_sub(y));
        x = x.wrapping_add(w_hi);
        w_hi = w_hi.rotate_right(34);
        std::mem::swap(&mut u, &mut z);
        cur = &cur[64..];
    }
    // Make s point to the last 64 bytes of input.
    let s = last64;
    u = u.wrapping_mul(9);
    v_hi = v_hi.rotate_right(28);
    v_lo = v_lo.rotate_right(20);
    w_lo = w_lo.wrapping_add((slen as u64 - 1) & 63);
    u = u.wrapping_add(y);
    y = y.wrapping_add(u);
    x = (y
        .wrapping_sub(x)
        .wrapping_add(v_lo)
        .wrapping_add(le_u64(&s[8..16])))
    .rotate_right(37)
    .wrapping_mul(mul);
    y = (y ^ v_hi ^ le_u64(&s[48..56]))
        .rotate_right(42)
        .wrapping_mul(mul);
    x ^= w_hi.wrapping_mul(9);
    y = y.wrapping_add(v_lo).wrapping_add(le_u64(&s[40..48]));
    z = (z.wrapping_add(w_lo)).rotate_right(33).wrapping_mul(mul);
    let (nv_lo, nv_hi) =
        weak_hash_len32_with_seeds(s, v_hi.wrapping_mul(mul), x.wrapping_add(w_lo));
    v_lo = nv_lo;
    v_hi = nv_hi;
    let (nw_lo, nw_hi) = weak_hash_len32_with_seeds(
        &s[32..],
        z.wrapping_add(w_hi),
        y.wrapping_add(le_u64(&s[16..24])),
    );
    w_lo = nw_lo;
    w_hi = nw_hi;
    uo_h(
        hash_len16_mul(v_lo.wrapping_add(x), w_lo ^ y, mul)
            .wrapping_add(z)
            .wrapping_sub(u),
        uo_h(v_hi.wrapping_add(y), w_hi.wrapping_add(z), K2, 30) ^ x,
        K2,
        31,
    )
}

fn uo_hash64(s: &[u8]) -> u64 {
    if s.len() <= 64 {
        return na_hash64(s);
    }
    hash64_with_seeds(s, 81, 0)
}

/// Go `farm.Hash64`: FarmHash's default, unseeded 64-bit hash.
///
/// `APPROX_COUNT_DISTINCT` hashes the row's encoded argument tuple through
/// this exact function, so an unfaithful port would still produce a
/// plausible-looking sketch whose numbers silently diverge from Go's above
/// the 65536-distinct-value threshold where the sketch starts discarding
/// samples.
pub fn hash64(s: &[u8]) -> u64 {
    let slen = s.len();
    if slen <= 32 {
        if slen <= 16 {
            hash_len0to16(s)
        } else {
            hash_len17to32(s)
        }
    } else if slen <= 64 {
        xo_hash_len33to64(s)
    } else if slen <= 96 {
        xo_hash_len65to96(s)
    } else if slen <= 256 {
        na_hash64(s)
    } else {
        uo_hash64(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Expected values captured from Go `farm.Hash64` (module
    // `github.com/dgryski/go-farm v0.0.0-20240924180020-3414d57e47da`) via a
    // throwaway `go run` snippet, one per length bucket the dispatcher above
    // branches on.
    #[test]
    fn matches_go_farm_hash64() {
        let empty: &[u8] = b"";
        assert_eq!(hash64(empty), 0x9ae1_6a3b_2f90_404f);

        assert_eq!(hash64(b"a"), 0xb345_4265_b6df_75e3);

        // 8 bytes: the little-endian i64 encoding APPROX_COUNT_DISTINCT uses
        // for INT arguments (`appendInt64`).
        assert_eq!(hash64(&42i64.to_le_bytes()), 0xd860_6f79_9f58_34b6);
        assert_eq!(hash64(&0i64.to_le_bytes()), 0x1439_7a23_417a_f284);
        assert_eq!(hash64(&(-1i64).to_le_bytes()), 0x2fff_e7a4_1fb5_9155);

        // 17-32 bytes.
        assert_eq!(hash64(b"the quick brown fo"), 0x2a48_f8f0_997b_07a5);

        // 33-64 bytes.
        assert_eq!(
            hash64(b"the quick brown fox jumps over the lazy"),
            0x3868_66cb_3220_5342
        );

        // 65-96 bytes.
        assert_eq!(
            hash64(b"the quick brown fox jumps over the lazy dog again and again and ag"),
            0xc064_ec58_cb4e_c4ac
        );

        // 97-256 bytes (naHash64 loop path).
        let long = b"the quick brown fox jumps over the lazy dog. \
the quick brown fox jumps over the lazy dog again. \
the quick brown fox jumps over the lazy dog once more for good measure.";
        assert_eq!(hash64(long), 0x8405_c02d_f02a_b212);

        // >256 bytes (uoHash64 loop path).
        let mut huge = Vec::new();
        for i in 0..40u8 {
            huge.extend_from_slice(format!("segment-{i:03}-").as_bytes());
        }
        assert_eq!(hash64(&huge), 0xc94d_ca0c_4996_8af0);
    }
}
