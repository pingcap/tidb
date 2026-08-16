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

//! boundary: Go `crypto/cipher.NewGCM` over `crypto/aes.NewCipher`.
//!
//! Go gets AES-256-GCM from its standard library; the Rust workspace has no
//! AEAD crate vendored, and this crate may not take a new dependency. What it
//! *does* already depend on is the `aes` crate, which supplies the AES-256
//! **block cipher** itself — the primitive that must never be hand-written.
//! GCM is a mode of operation layered on that primitive (counter-mode
//! encryption plus a GHASH universal hash), exactly like the CBC/CTR/OFB/CFB
//! modes `crate::encrypt` already builds on the same `aes` block cipher.
//!
//! This module therefore implements only the mode, per NIST SP 800-38D, and
//! pins it against that document's AES-256 test vectors (cases 13, 14 and 15)
//! plus the fixed BR vector in `file_backend`'s tests. If an AEAD crate is ever
//! added to the workspace, this module should be deleted in favor of it.
//!
//! Only the 96-bit nonce case is implemented, because that is the only case Go
//! `cipher.NewGCM` accepts: Go *panics* on any other nonce length, whereas the
//! functions here return [`GcmError::InvalidNonceLength`].
//!
//! Side channels: the tag comparison is constant-time, and [`gf_mul`] uses no
//! lookup tables, but its per-bit accumulate is a data-dependent branch, so
//! this is not a hardened implementation the way Go's assembly-backed GCM (or
//! a real AEAD crate) is. That is another reason to replace it rather than
//! extend it.

use aes::cipher::{BlockCipherEncrypt, KeyInit};
use aes::Aes256;

/// The AES block / GCM tag size in bytes.
pub const TAG_LEN: usize = 16;
/// The only nonce length Go `cipher.NewGCM` accepts.
pub const STANDARD_NONCE_LEN: usize = 12;

/// Why an AES-GCM operation failed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GcmError {
    /// The nonce was not 12 bytes (Go panics here instead).
    InvalidNonceLength(usize),
    /// The authentication tag did not verify: Go's `cipher: message
    /// authentication failed`.
    AuthenticationFailed,
}

impl std::fmt::Display for GcmError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidNonceLength(length) => {
                write!(
                    formatter,
                    "cipher: incorrect nonce length given to GCM: {length}"
                )
            }
            Self::AuthenticationFailed => {
                write!(formatter, "cipher: message authentication failed")
            }
        }
    }
}

impl std::error::Error for GcmError {}

/// AES-256-GCM with the standard 96-bit nonce and no additional data, which is
/// the whole of what `br/pkg/encryption/master_key` uses.
pub struct Aes256Gcm {
    cipher: Aes256,
    /// The hash subkey `H = CIPH_K(0^128)`, held as a big-endian integer.
    hash_subkey: u128,
}

impl Aes256Gcm {
    /// Builds the AEAD from a 256-bit key.
    pub fn new(key: &[u8; 32]) -> Aes256Gcm {
        let cipher = Aes256::new_from_slice(key).expect("32-byte AES-256 key");
        let hash_subkey = u128::from_be_bytes(encrypt_block(&cipher, [0u8; TAG_LEN]));
        Aes256Gcm {
            cipher,
            hash_subkey,
        }
    }

    /// Go `AEAD.Seal` with a nil destination and nil additional data, split into
    /// the ciphertext and the tag that Go would have appended to it.
    pub fn seal(
        &self,
        nonce: &[u8],
        plaintext: &[u8],
    ) -> Result<(Vec<u8>, [u8; TAG_LEN]), GcmError> {
        let counter_block = self.initial_counter(nonce)?;
        let mut counter = counter_block;
        increment32(&mut counter);
        let ciphertext = self.gctr(counter, plaintext);
        let tag = self.tag(&counter_block, &ciphertext);
        Ok((ciphertext, tag))
    }

    /// Go `AEAD.Open` with nil additional data, taking the ciphertext and tag
    /// separately because `master_key` stores them in separate fields.
    pub fn open(&self, nonce: &[u8], ciphertext: &[u8], tag: &[u8]) -> Result<Vec<u8>, GcmError> {
        let counter_block = self.initial_counter(nonce)?;
        let expected = self.tag(&counter_block, ciphertext);
        if tag.len() != TAG_LEN || !constant_time_eq(&expected, tag) {
            return Err(GcmError::AuthenticationFailed);
        }
        let mut counter = counter_block;
        increment32(&mut counter);
        Ok(self.gctr(counter, ciphertext))
    }

    /// `J0` for a 96-bit nonce: `IV || 0x00000001`.
    fn initial_counter(&self, nonce: &[u8]) -> Result<[u8; TAG_LEN], GcmError> {
        if nonce.len() != STANDARD_NONCE_LEN {
            return Err(GcmError::InvalidNonceLength(nonce.len()));
        }
        let mut block = [0u8; TAG_LEN];
        block[..STANDARD_NONCE_LEN].copy_from_slice(nonce);
        block[TAG_LEN - 1] = 1;
        Ok(block)
    }

    /// `GCTR`: XOR the data with the keystream produced by encrypting
    /// successive counter blocks.
    fn gctr(&self, mut counter: [u8; TAG_LEN], data: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(data.len());
        for chunk in data.chunks(TAG_LEN) {
            let keystream = encrypt_block(&self.cipher, counter);
            for (index, byte) in chunk.iter().enumerate() {
                out.push(byte ^ keystream[index]);
            }
            increment32(&mut counter);
        }
        out
    }

    /// `T = CIPH_K(J0) XOR GHASH_H(C || pad || [len(A)]64 || [len(C)]64)`, with
    /// no additional data.
    fn tag(&self, counter_block: &[u8; TAG_LEN], ciphertext: &[u8]) -> [u8; TAG_LEN] {
        let mut hash = 0u128;
        for chunk in ciphertext.chunks(TAG_LEN) {
            let mut block = [0u8; TAG_LEN];
            block[..chunk.len()].copy_from_slice(chunk);
            hash = gf_mul(hash ^ u128::from_be_bytes(block), self.hash_subkey);
        }
        // Bit lengths of the additional data (always empty here) and the
        // ciphertext, as two big-endian u64s in one block.
        let mut lengths = [0u8; TAG_LEN];
        lengths[8..].copy_from_slice(&((ciphertext.len() as u64) * 8).to_be_bytes());
        hash = gf_mul(hash ^ u128::from_be_bytes(lengths), self.hash_subkey);

        let mask = encrypt_block(&self.cipher, *counter_block);
        let mut tag = hash.to_be_bytes();
        for (index, byte) in tag.iter_mut().enumerate() {
            *byte ^= mask[index];
        }
        tag
    }
}

fn encrypt_block(cipher: &Aes256, block: [u8; TAG_LEN]) -> [u8; TAG_LEN] {
    let mut value = aes::Block::from(block);
    cipher.encrypt_block(&mut value);
    value.into()
}

/// `inc32`: increment the trailing 32 bits of the counter block, modulo 2^32.
fn increment32(block: &mut [u8; TAG_LEN]) {
    let mut counter = u32::from_be_bytes([block[12], block[13], block[14], block[15]]);
    counter = counter.wrapping_add(1);
    block[12..].copy_from_slice(&counter.to_be_bytes());
}

/// Multiplication in GF(2^128) with GCM's bit ordering: bit 127 of the `u128`
/// is the first (most significant) bit of the block, and the reduction
/// polynomial is `x^128 + x^7 + x^2 + x + 1`, spelled `0xe1 << 120` after the
/// right shift.
fn gf_mul(x: u128, y: u128) -> u128 {
    const R: u128 = 0xe1 << 120;
    let mut product = 0u128;
    let mut value = y;
    for bit in 0..128 {
        if (x >> (127 - bit)) & 1 == 1 {
            product ^= value;
        }
        let carry = value & 1 == 1;
        value >>= 1;
        if carry {
            value ^= R;
        }
    }
    product
}

fn constant_time_eq(a: &[u8; TAG_LEN], b: &[u8]) -> bool {
    if b.len() != TAG_LEN {
        return false;
    }
    let mut difference = 0u8;
    for (left, right) in a.iter().zip(b.iter()) {
        difference |= left ^ right;
    }
    difference == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unhex(text: &str) -> Vec<u8> {
        assert!(text.len().is_multiple_of(2));
        (0..text.len() / 2)
            .map(|index| u8::from_str_radix(&text[index * 2..index * 2 + 2], 16).expect("hex"))
            .collect()
    }

    fn key32(text: &str) -> [u8; 32] {
        let bytes = unhex(text);
        let mut key = [0u8; 32];
        key.copy_from_slice(&bytes);
        key
    }

    /// NIST SP 800-38D, GCM test case 13: AES-256, zero key, zero IV, empty
    /// plaintext.
    #[test]
    fn test_nist_case_13() {
        let gcm = Aes256Gcm::new(&[0u8; 32]);
        let (ciphertext, tag) = gcm.seal(&[0u8; 12], b"").unwrap();
        assert!(ciphertext.is_empty());
        assert_eq!(tag.to_vec(), unhex("530f8afbc74536b9a963b4f1c4cb738b"));
        assert_eq!(gcm.open(&[0u8; 12], &ciphertext, &tag).unwrap(), b"");
    }

    /// NIST SP 800-38D, GCM test case 14: one all-zero plaintext block.
    #[test]
    fn test_nist_case_14() {
        let gcm = Aes256Gcm::new(&[0u8; 32]);
        let (ciphertext, tag) = gcm.seal(&[0u8; 12], &[0u8; 16]).unwrap();
        assert_eq!(ciphertext, unhex("cea7403d4d606b6e074ec5d3baf39d18"));
        assert_eq!(tag.to_vec(), unhex("d0d1c8a799996bf0265b98b5d48ab919"));
        assert_eq!(gcm.open(&[0u8; 12], &ciphertext, &tag).unwrap(), [0u8; 16]);
    }

    /// NIST SP 800-38D, GCM test case 15: four full plaintext blocks.
    #[test]
    fn test_nist_case_15() {
        let gcm = Aes256Gcm::new(&key32(
            "feffe9928665731c6d6a8f9467308308feffe9928665731c6d6a8f9467308308",
        ));
        let nonce = unhex("cafebabefacedbaddecaf888");
        let plaintext = unhex(
            "d9313225f88406e5a55909c5aff5269a86a7a9531534f7da2e4c303d8a318a72\
             1c3c0c95956809532fcf0e2449a6b525b16aedf5aa0de657ba637b391aafd255",
        );
        let (ciphertext, tag) = gcm.seal(&nonce, &plaintext).unwrap();
        assert_eq!(
            ciphertext,
            unhex(
                "522dc1f099567d07f47f37a32a84427d643a8cdcbfe5c0c97598a2bd2555d1aa\
                 8cb08e48590dbb3da7b08b1056828838c5f61e6393ba7a0abcc9f662898015ad"
            )
        );
        assert_eq!(tag.to_vec(), unhex("b094dac5d93471bdec1a502270e3cc6c"));
        assert_eq!(gcm.open(&nonce, &ciphertext, &tag).unwrap(), plaintext);
    }

    /// Partial trailing blocks are what exercise GHASH's zero padding and
    /// GCTR's short final XOR; NIST's published cases all end on a block
    /// boundary (case 16 pads only through its AAD), so these two are taken
    /// from a reference AES-GCM implementation instead.
    #[test]
    fn test_partial_trailing_block() {
        let gcm = Aes256Gcm::new(&key32(
            "feffe9928665731c6d6a8f9467308308feffe9928665731c6d6a8f9467308308",
        ));
        let nonce = unhex("cafebabefacedbaddecaf888");
        // Case 15's plaintext truncated to 60 bytes: 3 full blocks + 12 bytes.
        let plaintext = unhex(
            "d9313225f88406e5a55909c5aff5269a86a7a9531534f7da2e4c303d8a318a72\
             1c3c0c95956809532fcf0e2449a6b525b16aedf5aa0de657ba637b39",
        );
        let (ciphertext, tag) = gcm.seal(&nonce, &plaintext).unwrap();
        assert_eq!(
            ciphertext,
            unhex(
                "522dc1f099567d07f47f37a32a84427d643a8cdcbfe5c0c97598a2bd2555d1aa\
                 8cb08e48590dbb3da7b08b1056828838c5f61e6393ba7a0abcc9f662"
            )
        );
        assert_eq!(tag.to_vec(), unhex("eb9f796c8d356fc31a8433884b696f4f"));
        assert_eq!(gcm.open(&nonce, &ciphertext, &tag).unwrap(), plaintext);

        // A plaintext shorter than one block.
        let gcm = Aes256Gcm::new(&[0u8; 32]);
        let nonce = unhex("0102030405060708090a0b0c");
        let (ciphertext, tag) = gcm.seal(&nonce, &[1, 2, 3]).unwrap();
        assert_eq!(ciphertext, unhex("3c46d4"));
        assert_eq!(tag.to_vec(), unhex("ebe1c4a8bd43d0ac9ac39a70e42ed9c9"));
        assert_eq!(gcm.open(&nonce, &ciphertext, &tag).unwrap(), [1, 2, 3]);
    }

    /// Tampering with either the ciphertext or the tag must fail the tag check
    /// before any plaintext is produced.
    #[test]
    fn test_authentication_rejects_tampering() {
        let gcm = Aes256Gcm::new(&[7u8; 32]);
        let nonce = [3u8; 12];
        let (mut ciphertext, mut tag) = gcm.seal(&nonce, b"hello world, hello world").unwrap();

        ciphertext[0] ^= 1;
        assert_eq!(
            gcm.open(&nonce, &ciphertext, &tag),
            Err(GcmError::AuthenticationFailed)
        );
        ciphertext[0] ^= 1;

        tag[0] ^= 1;
        assert_eq!(
            gcm.open(&nonce, &ciphertext, &tag),
            Err(GcmError::AuthenticationFailed)
        );
        tag[0] ^= 1;

        // A truncated tag is rejected on length alone.
        assert_eq!(
            gcm.open(&nonce, &ciphertext, &tag[..8]),
            Err(GcmError::AuthenticationFailed)
        );
        assert!(gcm.open(&nonce, &ciphertext, &tag).is_ok());
    }

    /// Go panics on a non-standard nonce; this port reports it.
    #[test]
    fn test_nonce_length_is_checked() {
        let gcm = Aes256Gcm::new(&[0u8; 32]);
        assert_eq!(
            gcm.seal(&[0u8; 16], b""),
            Err(GcmError::InvalidNonceLength(16))
        );
        assert_eq!(
            gcm.open(&[0u8; 16], b"", &[0u8; 16]),
            Err(GcmError::InvalidNonceLength(16))
        );
    }

    /// The counter rolls over the low 32 bits only, which matters for payloads
    /// long enough to wrap; check the increment in isolation.
    #[test]
    fn test_increment32_wraps_low_word_only() {
        let mut block = [0u8; 16];
        block[..12].copy_from_slice(&[9u8; 12]);
        block[12..].copy_from_slice(&u32::MAX.to_be_bytes());
        increment32(&mut block);
        assert_eq!(&block[..12], &[9u8; 12]);
        assert_eq!(&block[12..], &[0, 0, 0, 0]);
    }
}
