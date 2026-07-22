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

//! Complete transcreation of `pkg/parser/auth`.
//!
//! This module deliberately keeps the source algorithms and byte layout.  In
//! particular, caching SHA-2 and TiDB SM3 share MySQL's SHA-crypt derivative;
//! changing it to a generic password-hashing API would change stored hashes.

use std::fmt;

use sha1::{Digest, Sha1};
use sha2::Sha256;
use subtle::ConstantTimeEq;
use tidb_ast::{RestoreCtx, RestoreWriter};
use tidb_mysql::{AuthCachingSha2Password, AuthTiDBSM3Password};

/// Maximum username length accepted by the parser contract.
pub const USER_NAME_MAX_LENGTH: usize = 32;
/// Maximum hostname length accepted by the parser contract.
pub const HOST_NAME_MAX_LENGTH: usize = 255;
/// Number of digest bytes mixed at a time by MySQL's SHA-crypt derivative.
pub const MIX_CHARS: usize = 32;
/// Salt length used by caching SHA-2 and TiDB SM3 password hashes.
pub const SALT_LENGTH: usize = 20;
/// Stored iteration field multiplier.
pub const ITERATION_MULTIPLIER: usize = 1_000;

/// Parser-visible login identity and the privilege entry it matched.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct UserIdentity {
    /// Login username.
    pub username: String,
    /// Login hostname.
    pub hostname: String,
    /// Whether this represents `CURRENT_USER`.
    pub current_user: bool,
    /// Username matched by the privilege system.
    pub auth_username: String,
    /// Host pattern matched by the privilege system.
    pub auth_hostname: String,
    /// Client plugin named during authentication.
    pub auth_plugin: String,
}

impl UserIdentity {
    /// Restores the identity exactly as Go `UserIdentity.Restore` does.
    pub fn restore<W: RestoreWriter>(&self, context: &mut RestoreCtx<W>) {
        if self.current_user {
            context.write_keyword("CURRENT_USER");
        } else {
            context.write_name(&self.username);
            context.write_plain("@");
            context.write_name(&self.hostname);
        }
    }

    /// Returns the privilege-table identity when present, otherwise the login.
    pub fn identity_string(&self) -> String {
        if self.auth_username.is_empty() {
            self.login_string()
        } else {
            format!("{}@{}", self.auth_username, self.auth_hostname)
        }
    }

    /// Returns the actual login identity.
    pub fn login_string(&self) -> String {
        format!("{}@{}", self.username, self.hostname)
    }
}

impl fmt::Display for UserIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.identity_string())
    }
}

/// Parser-visible role identity.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RoleIdentity {
    /// Role username.
    pub username: String,
    /// Role host.
    pub hostname: String,
}

impl RoleIdentity {
    /// Restores the role, omitting `@host` only when the host is empty.
    pub fn restore<W: RestoreWriter>(&self, context: &mut RestoreCtx<W>) {
        context.write_name(&self.username);
        if !self.hostname.is_empty() {
            context.write_plain("@");
            context.write_name(&self.hostname);
        }
    }
}

impl fmt::Display for RoleIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "`{}`@`{}`", self.username, self.hostname)
    }
}

/// Errors returned by stored-password decoding and verification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthError {
    /// The stored `$A$...` hash did not have four parts.
    HashParts,
    /// The digest type was not `A`.
    DigestType,
    /// The hexadecimal iteration field was invalid.
    Iterations,
    /// Native-password hexadecimal text was malformed.
    Hex(String),
    /// The operating system did not provide random bytes.
    Random,
}

impl fmt::Display for AuthError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::HashParts => formatter.write_str("failed to decode hash parts"),
            Self::DigestType => formatter.write_str("digest type is incompatible"),
            Self::Iterations => formatter.write_str("failed to decode iterations"),
            Self::Hex(message) => formatter.write_str(message),
            Self::Random => formatter.write_str("operating system random source failed"),
        }
    }
}

impl std::error::Error for AuthError {}

/// Calculates SHA-1 using the same byte contract as Go's helper.
pub fn sha1_hash(input: &[u8]) -> [u8; 20] {
    Sha1::digest(input).into()
}

/// Calculates SHA-256 using the same byte contract as Go's helper.
pub fn sha256_hash(input: &[u8]) -> [u8; 32] {
    Sha256::digest(input).into()
}

/// Checks a `mysql_native_password` client scramble against a stage-two hash.
pub fn check_scrambled_password(salt: &[u8], password_hash: &[u8], auth: &[u8]) -> bool {
    let mut crypt = Sha1::new();
    crypt.update(salt);
    crypt.update(password_hash);
    let mut scramble_hash: [u8; 20] = crypt.finalize().into();
    if auth.len() != scramble_hash.len() {
        return false;
    }
    for (hash, token) in scramble_hash.iter_mut().zip(auth) {
        *hash ^= token;
    }
    password_hash.ct_eq(&sha1_hash(&scramble_hash)).into()
}

/// Encodes plaintext bytes as MySQL's uppercase `*SHA1(SHA1(password))` form.
pub fn encode_password_bytes(password: &[u8]) -> String {
    if password.is_empty() {
        return String::new();
    }
    let stage_one = sha1_hash(password);
    let stage_two = sha1_hash(&stage_one);
    let mut encoded = String::with_capacity(41);
    encoded.push('*');
    for byte in stage_two {
        use fmt::Write as _;
        let _ = write!(encoded, "{byte:02X}");
    }
    encoded
}

/// Encodes a UTF-8 plaintext password using the source byte algorithm.
pub fn encode_password(password: &str) -> String {
    encode_password_bytes(password.as_bytes())
}

/// Decodes the hex bytes after the first stored-password byte.
pub fn decode_password(password: &str) -> Result<Vec<u8>, AuthError> {
    let bytes = password.as_bytes();
    // Preserve Go's direct `pwd[1:]` indexing, including its panic on an empty
    // input. Callers pass the stored `*...` form.
    let encoded = &bytes[1..];
    if !encoded.len().is_multiple_of(2) {
        return Err(AuthError::Hex(
            "encoding/hex: odd length hex string".to_owned(),
        ));
    }
    let mut decoded = Vec::with_capacity(encoded.len() / 2);
    for pair in encoded.chunks_exact(2) {
        let high = hex_nibble(pair[0]);
        let low = hex_nibble(pair[1]);
        match (high, low) {
            (Some(high), Some(low)) => decoded.push((high << 4) | low),
            _ => {
                let bad = if high.is_none() { pair[0] } else { pair[1] };
                return Err(AuthError::Hex(format!(
                    "encoding/hex: invalid byte: U+{bad:04X}"
                )));
            }
        }
    }
    Ok(decoded)
}

fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn b64_from_24_bit(bytes: [u8; 3], count: usize, output: &mut Vec<u8>) {
    const TABLE: &[u8; 64] = b"./0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    let mut word = (u64::from(bytes[0]) << 16) | (u64::from(bytes[1]) << 8) | u64::from(bytes[2]);
    for _ in 0..count {
        output.push(TABLE[(word & 0x3f) as usize]);
        word >>= 6;
    }
}

fn plugin_hash(input: &[u8], plugin: &str) -> Option<[u8; 32]> {
    if plugin == AuthCachingSha2Password {
        Some(sha256_hash(input))
    } else if plugin == AuthTiDBSM3Password {
        Some(sm3_hash(input))
    } else {
        None
    }
}

fn hash_crypt(plaintext: &[u8], salt: &[u8], iterations: i64, plugin: &str) -> Vec<u8> {
    let hash = |input: &[u8]| plugin_hash(input, plugin).expect("known auth plugin");

    let mut buffer_a = Vec::with_capacity(4096);
    buffer_a.extend_from_slice(plaintext);
    buffer_a.extend_from_slice(salt);

    let mut buffer_b = Vec::with_capacity(plaintext.len() * 2 + salt.len());
    buffer_b.extend_from_slice(plaintext);
    buffer_b.extend_from_slice(salt);
    buffer_b.extend_from_slice(plaintext);
    let sum_b = hash(&buffer_b);

    let mut remaining = plaintext.len();
    while remaining > MIX_CHARS {
        buffer_a.extend_from_slice(&sum_b);
        remaining -= MIX_CHARS;
    }
    buffer_a.extend_from_slice(&sum_b[..remaining]);

    let mut length = plaintext.len();
    while length > 0 {
        if length.is_multiple_of(2) {
            buffer_a.extend_from_slice(plaintext);
        } else {
            buffer_a.extend_from_slice(&sum_b);
        }
        length >>= 1;
    }
    let mut sum_a = hash(&buffer_a);

    let mut buffer_dp = Vec::with_capacity(plaintext.len() * plaintext.len());
    for _ in plaintext {
        buffer_dp.extend_from_slice(plaintext);
    }
    let sum_dp = hash(&buffer_dp);
    let mut p = Vec::with_capacity(plaintext.len());
    let mut remaining = plaintext.len();
    while remaining > 0 {
        let count = remaining.min(MIX_CHARS);
        p.extend_from_slice(&sum_dp[..count]);
        remaining -= count;
    }

    let mut buffer_ds = Vec::with_capacity((16 + usize::from(sum_a[0])) * salt.len());
    for _ in 0..(16 + usize::from(sum_a[0])) {
        buffer_ds.extend_from_slice(salt);
    }
    let sum_ds = hash(&buffer_ds);
    let mut s = Vec::with_capacity(salt.len());
    let mut remaining = salt.len();
    while remaining > 0 {
        let count = remaining.min(MIX_CHARS);
        s.extend_from_slice(&sum_ds[..count]);
        remaining -= count;
    }

    let mut sum_c = None;
    for iteration in 0..iterations {
        let mut buffer_c = Vec::with_capacity(p.len() * 2 + s.len() + sum_a.len());
        if iteration & 1 != 0 {
            buffer_c.extend_from_slice(&p);
        } else {
            buffer_c.extend_from_slice(&sum_a);
        }
        if iteration % 3 != 0 {
            buffer_c.extend_from_slice(&s);
        }
        if iteration % 7 != 0 {
            buffer_c.extend_from_slice(&p);
        }
        if iteration & 1 != 0 {
            buffer_c.extend_from_slice(&sum_a);
        } else {
            buffer_c.extend_from_slice(&p);
        }
        let current = hash(&buffer_c);
        sum_a = current;
        sum_c = Some(current);
    }
    // Go indexes `sumC` after the loop. Zero or negative iterations therefore
    // panic; keeping that behavior avoids inventing a new accepted hash form.
    let sum_c = sum_c.expect("hashCrypt requires at least one iteration");

    let mut output = format!("$A${:03X}$", iterations / ITERATION_MULTIPLIER as i64).into_bytes();
    output.extend_from_slice(salt);
    for bytes in [
        [sum_c[0], sum_c[10], sum_c[20]],
        [sum_c[21], sum_c[1], sum_c[11]],
        [sum_c[12], sum_c[22], sum_c[2]],
        [sum_c[3], sum_c[13], sum_c[23]],
        [sum_c[24], sum_c[4], sum_c[14]],
        [sum_c[15], sum_c[25], sum_c[5]],
        [sum_c[6], sum_c[16], sum_c[26]],
        [sum_c[27], sum_c[7], sum_c[17]],
        [sum_c[18], sum_c[28], sum_c[8]],
        [sum_c[9], sum_c[19], sum_c[29]],
    ] {
        b64_from_24_bit(bytes, 4, &mut output);
    }
    b64_from_24_bit([0, sum_c[31], sum_c[30]], 3, &mut output);
    output
}

/// Checks a caching SHA-2 or TiDB SM3 stored password.
pub fn check_hashing_password(
    password_hash: &[u8],
    password: &str,
    plugin: &str,
) -> Result<bool, AuthError> {
    check_hashing_password_bytes(password_hash, password.as_bytes(), plugin)
}

/// Byte-preserving variant of [`check_hashing_password`].
///
/// Go strings may contain arbitrary bytes. Authentication callers that have
/// not decoded a password as UTF-8 must use this entry point so transcreation
/// does not narrow the source domain.
pub fn check_hashing_password_bytes(
    password_hash: &[u8],
    password: &[u8],
    plugin: &str,
) -> Result<bool, AuthError> {
    let parts: Vec<&[u8]> = password_hash.split(|byte| *byte == b'$').collect();
    if parts.len() != 4 {
        return Err(AuthError::HashParts);
    }
    if parts[1] != b"A" {
        return Err(AuthError::DigestType);
    }
    let iterations_text = std::str::from_utf8(parts[2]).map_err(|_| AuthError::Iterations)?;
    let iterations = i64::from_str_radix(iterations_text, 16)
        .map_err(|_| AuthError::Iterations)?
        .wrapping_mul(ITERATION_MULTIPLIER as i64);
    let salt = &parts[3][..SALT_LENGTH];
    let Some(_) = plugin_hash(&[], plugin) else {
        return Ok(false);
    };
    let candidate = hash_crypt(password, salt, iterations, plugin);
    Ok(password_hash.ct_eq(&candidate).into())
}

/// Creates a caching SHA-2 or TiDB SM3 password using source-compatible salt.
pub fn new_hash_password(password: &str, plugin: &str) -> Result<String, AuthError> {
    let encoded = new_hash_password_bytes(password.as_bytes(), plugin)?;
    Ok(String::from_utf8(encoded).expect("generated password hashes are ASCII"))
}

/// Byte-preserving variant of [`new_hash_password`].
pub fn new_hash_password_bytes(password: &[u8], plugin: &str) -> Result<Vec<u8>, AuthError> {
    let mut salt = [0_u8; SALT_LENGTH];
    getrandom::fill(&mut salt).map_err(|_| AuthError::Random)?;
    for byte in &mut salt {
        *byte &= 0x7f;
        while *byte == b'$' || *byte == 0 {
            getrandom::fill(std::slice::from_mut(byte)).map_err(|_| AuthError::Random)?;
            *byte &= 0x7f;
        }
    }
    if plugin != AuthCachingSha2Password && plugin != AuthTiDBSM3Password {
        return Ok(Vec::new());
    }
    Ok(hash_crypt(
        password,
        &salt,
        5 * ITERATION_MULTIPLIER as i64,
        plugin,
    ))
}

/// Incremental SM3 state transcreated from TiDB's Go implementation.
#[derive(Debug, Clone)]
pub struct Sm3 {
    digest: [u32; 8],
    length_bits: u64,
    pending: Vec<u8>,
}

impl Default for Sm3 {
    fn default() -> Self {
        Self {
            digest: [
                0x7380_166f,
                0x4914_b2b9,
                0x1724_42d7,
                0xda8a_0600,
                0xa96f_30bc,
                0x1631_38aa,
                0xe38d_ee4d,
                0xb0fb_0e4e,
            ],
            length_bits: 0,
            pending: Vec::new(),
        }
    }
}

impl Sm3 {
    /// Underlying block size.
    pub const fn block_size(&self) -> usize {
        64
    }

    /// Digest byte size.
    pub const fn size(&self) -> usize {
        32
    }

    /// Resets the state to the SM3 initialization vector.
    pub fn reset(&mut self) {
        *self = Self::default();
    }

    /// Adds bytes to the running hash and returns their source byte count.
    pub fn write(&mut self, input: &[u8]) -> usize {
        self.length_bits = self
            .length_bits
            .wrapping_add((input.len() as u64).wrapping_mul(8));
        self.pending.extend_from_slice(input);
        let complete = self.pending.len() / 64 * 64;
        if complete != 0 {
            let blocks = self.pending[..complete].to_vec();
            compress_sm3_blocks(&mut self.digest, &blocks);
            self.pending.drain(..complete);
        }
        input.len()
    }

    /// Mirrors Go's concrete `Sum`: input is written, but only the digest returns.
    pub fn sum(&mut self, input: &[u8]) -> Vec<u8> {
        self.write(input);
        let mut final_digest = self.digest;
        let mut padded = self.pending.clone();
        padded.push(0x80);
        while padded.len() % 64 != 56 {
            padded.push(0);
        }
        padded.extend_from_slice(&self.length_bits.to_be_bytes());
        compress_sm3_blocks(&mut final_digest, &padded);
        let mut output = Vec::with_capacity(32);
        for word in final_digest {
            output.extend_from_slice(&word.to_be_bytes());
        }
        output
    }
}

/// Constructs a reset SM3 state.
pub fn new_sm3() -> Sm3 {
    Sm3::default()
}

/// Calculates one SM3 digest.
pub fn sm3_hash(input: &[u8]) -> [u8; 32] {
    let mut hasher = new_sm3();
    hasher.write(input);
    hasher.sum(&[]).try_into().expect("SM3 output is 32 bytes")
}

fn p0(value: u32) -> u32 {
    value ^ value.rotate_left(9) ^ value.rotate_left(17)
}

fn p1(value: u32) -> u32 {
    value ^ value.rotate_left(15) ^ value.rotate_left(23)
}

fn compress_sm3_blocks(digest: &mut [u32; 8], mut input: &[u8]) {
    while input.len() >= 64 {
        let mut w = [0_u32; 68];
        let mut w1 = [0_u32; 64];
        for (index, chunk) in input[..64].chunks_exact(4).enumerate() {
            w[index] = u32::from_be_bytes(chunk.try_into().expect("four-byte word"));
        }
        for index in 16..68 {
            w[index] = p1(w[index - 16] ^ w[index - 9] ^ w[index - 3].rotate_left(15))
                ^ w[index - 13].rotate_left(7)
                ^ w[index - 6];
        }
        for index in 0..64 {
            w1[index] = w[index] ^ w[index + 4];
        }
        let [mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut h] = *digest;
        for index in 0..64 {
            let constant: u32 = if index < 16 { 0x79cc_4519 } else { 0x7a87_9d8a };
            let ss1 = a
                .rotate_left(12)
                .wrapping_add(e)
                .wrapping_add(constant.rotate_left(index as u32))
                .rotate_left(7);
            let ss2 = ss1 ^ a.rotate_left(12);
            let ff = if index < 16 {
                a ^ b ^ c
            } else {
                (a & b) | (a & c) | (b & c)
            };
            let gg = if index < 16 {
                e ^ f ^ g
            } else {
                (e & f) | ((!e) & g)
            };
            let tt1 = ff.wrapping_add(d).wrapping_add(ss2).wrapping_add(w1[index]);
            let tt2 = gg.wrapping_add(h).wrapping_add(ss1).wrapping_add(w[index]);
            d = c;
            c = b.rotate_left(9);
            b = a;
            a = tt1;
            h = g;
            g = f.rotate_left(19);
            f = e;
            e = p0(tt2);
        }
        digest[0] ^= a;
        digest[1] ^= b;
        digest[2] ^= c;
        digest[3] ^= d;
        digest[4] ^= e;
        digest[5] ^= f;
        digest[6] ^= g;
        digest[7] ^= h;
        input = &input[64..];
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hashing_password_accepts_non_utf8_go_string_bytes_and_salt() {
        let password = b"not-utf8-\xff";
        let salt = [0xff; SALT_LENGTH];
        let stored = hash_crypt(
            password,
            &salt,
            5 * ITERATION_MULTIPLIER as i64,
            AuthCachingSha2Password,
        );
        assert!(check_hashing_password_bytes(&stored, password, AuthCachingSha2Password,).unwrap());
    }

    #[test]
    #[should_panic(expected = "range end index 20")]
    fn hashing_password_keeps_go_short_salt_panic() {
        let _ = check_hashing_password_bytes(b"$A$005$short", b"password", AuthCachingSha2Password);
    }

    #[test]
    #[should_panic(expected = "hashCrypt requires at least one iteration")]
    fn hashing_password_keeps_go_zero_iteration_panic() {
        let mut stored = b"$A$000$".to_vec();
        stored.extend_from_slice(&[b'x'; SALT_LENGTH + 43]);
        let _ = check_hashing_password_bytes(&stored, b"password", AuthCachingSha2Password);
    }

    #[test]
    fn sm3_sum_keeps_the_source_nonempty_input_behavior() {
        let mut hasher = new_sm3();
        assert_eq!(hasher.sum(b"abc"), sm3_hash(b"abc"));
        assert_eq!(hasher.sum(&[]), sm3_hash(b"abc"));
    }
}
