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

//! `mysql_native_password` stored-hash parsing and challenge verification.
//!
//! The configured value is MySQL's password-equivalent stage-two SHA-1 hash,
//! not a plaintext password. It must therefore never be rendered by a public
//! API, diagnostic, or error. The verifier follows TiDB's
//! `auth.CheckScrambledPassword` exactly and keeps the final hash comparison
//! constant time.

use sha1::{Digest, Sha1};
use subtle::ConstantTimeEq;

/// SHA-1 output and native-password response width.
pub const NATIVE_PASSWORD_HASH_LEN: usize = 20;

/// MySQL protocol handshake salt width used by TiDB.
pub const HANDSHAKE_SALT_LEN: usize = 20;

/// Stored `mysql_native_password` stage-two hash.
///
/// The inner password-equivalent material is deliberately private. `Debug`
/// is redacted, and this type has no display or byte-export API.
#[derive(Clone)]
pub struct NativePasswordHash([u8; NATIVE_PASSWORD_HASH_LEN]);

impl NativePasswordHash {
    /// Parses exactly `*` followed by 40 ASCII hexadecimal digits.
    pub fn parse(encoded: &str) -> Result<Self, NativePasswordHashError> {
        let bytes = encoded.as_bytes();
        if bytes.len() != 1 + NATIVE_PASSWORD_HASH_LEN * 2 {
            return Err(NativePasswordHashError::InvalidLength);
        }
        if bytes[0] != b'*' {
            return Err(NativePasswordHashError::MissingPrefix);
        }

        let mut decoded = [0; NATIVE_PASSWORD_HASH_LEN];
        for (destination, pair) in decoded.iter_mut().zip(bytes[1..].chunks_exact(2)) {
            let high = decode_hex(pair[0]).ok_or(NativePasswordHashError::InvalidHex)?;
            let low = decode_hex(pair[1]).ok_or(NativePasswordHashError::InvalidHex)?;
            *destination = high << 4 | low;
        }
        Ok(Self(decoded))
    }

    /// Verifies one native-password response against this stored hash.
    ///
    /// This is the server half of the native protocol:
    /// `stage1 = response XOR SHA1(salt || stage2)`, followed by a
    /// constant-time comparison of `SHA1(stage1)` with stored `stage2`.
    #[must_use]
    pub fn verify(&self, salt: &[u8], response: &[u8]) -> bool {
        if salt.len() != HANDSHAKE_SALT_LEN {
            return false;
        }
        let Ok(response) = <&[u8; NATIVE_PASSWORD_HASH_LEN]>::try_from(response) else {
            return false;
        };

        let mut challenge_hasher = Sha1::new();
        challenge_hasher.update(salt);
        challenge_hasher.update(self.0);
        let challenge = challenge_hasher.finalize();

        let mut stage_one = [0; NATIVE_PASSWORD_HASH_LEN];
        for ((destination, response), challenge) in stage_one
            .iter_mut()
            .zip(response.iter())
            .zip(challenge.iter())
        {
            *destination = response ^ challenge;
        }

        let candidate_stage_two = Sha1::digest(stage_one);
        bool::from(candidate_stage_two.as_slice().ct_eq(&self.0))
    }
}

impl PartialEq for NativePasswordHash {
    fn eq(&self, other: &Self) -> bool {
        bool::from(self.0.ct_eq(&other.0))
    }
}

impl Eq for NativePasswordHash {}

impl std::fmt::Debug for NativePasswordHash {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("NativePasswordHash([REDACTED])")
    }
}

/// Safe parse failure categories for a stored native-password hash.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NativePasswordHashError {
    /// The encoded value is not exactly 41 bytes.
    InvalidLength,
    /// The encoded value does not begin with `*`.
    MissingPrefix,
    /// A stage-two hash digit is not hexadecimal.
    InvalidHex,
}

impl std::fmt::Display for NativePasswordHashError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidLength => formatter.write_str("native password hash has invalid length"),
            Self::MissingPrefix => formatter.write_str("native password hash has invalid prefix"),
            Self::InvalidHex => formatter.write_str("native password hash has invalid encoding"),
        }
    }
}

impl std::error::Error for NativePasswordHashError {}

/// Generates the source server's 20-byte, NUL- and `$`-free handshake salt.
#[must_use]
pub fn generate_handshake_salt() -> [u8; HANDSHAKE_SALT_LEN] {
    tidb_util::fastrand::buf(HANDSHAKE_SALT_LEN as isize)
        .try_into()
        .expect("fastrand returned the requested handshake salt width")
}

/// Verifies a candidate account without revealing whether it existed.
///
/// Unknown users execute the same SHA-1 verifier with a fixed dummy hash. The
/// bitwise conjunction deliberately avoids a short-circuit after verification.
#[must_use]
pub fn verify_candidate(
    candidate: Option<&NativePasswordHash>,
    salt: &[u8],
    response: &[u8],
) -> bool {
    const DUMMY_STAGE_TWO: [u8; NATIVE_PASSWORD_HASH_LEN] = [
        0x5c, 0x19, 0xf3, 0x71, 0x1e, 0xd0, 0xb5, 0xc7, 0x95, 0xf0, 0x10, 0xc3, 0xf7, 0x58, 0xa4,
        0x96, 0x1d, 0x49, 0x3f, 0x86,
    ];
    let dummy = NativePasswordHash(DUMMY_STAGE_TWO);
    let matched = candidate.is_some();
    let verified = candidate.unwrap_or(&dummy).verify(salt, response);
    matched & verified
}

const fn decode_hex(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}
