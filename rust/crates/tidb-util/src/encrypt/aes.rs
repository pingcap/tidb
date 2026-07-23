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

//! AES helpers transcreated from `pkg/util/encrypt/aes.go`.

use aes::cipher::{BlockCipherDecrypt, BlockCipherEncrypt, KeyInit};
use aes::{Aes128, Aes192, Aes256};
use std::fmt;

pub(super) const AES_BLOCK_SIZE: usize = 16;

/// Source-compatible failures from TiDB's AES helpers.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EncryptError {
    /// Go `aes.NewCipher` rejected a key outside 128/192/256 bits.
    InvalidKeyLength(usize),
    /// A block-mode ciphertext was not a whole number of AES blocks.
    CorruptedData,
    /// PKCS#7 size or terminal length was invalid.
    InvalidPaddingSize,
    /// PKCS#7 bytes disagreed with the terminal pad byte.
    InvalidPadding,
}

impl fmt::Display for EncryptError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidKeyLength(length) => {
                write!(formatter, "crypto/aes: invalid key size {length}")
            }
            Self::CorruptedData => formatter.write_str("Corrupted data"),
            Self::InvalidPaddingSize => formatter.write_str("Invalid padding size"),
            Self::InvalidPadding => formatter.write_str("Invalid padding"),
        }
    }
}

impl std::error::Error for EncryptError {}

pub(super) enum AesCipher {
    Aes128(Aes128),
    Aes192(Aes192),
    Aes256(Aes256),
}

impl AesCipher {
    pub(super) fn new(key: &[u8]) -> Result<Self, EncryptError> {
        match key.len() {
            16 => Ok(Self::Aes128(
                Aes128::new_from_slice(key).expect("validated AES-128 key"),
            )),
            24 => Ok(Self::Aes192(
                Aes192::new_from_slice(key).expect("validated AES-192 key"),
            )),
            32 => Ok(Self::Aes256(
                Aes256::new_from_slice(key).expect("validated AES-256 key"),
            )),
            length => Err(EncryptError::InvalidKeyLength(length)),
        }
    }

    pub(super) fn encrypt_block(&self, block: &mut [u8; AES_BLOCK_SIZE]) {
        let mut value = aes::Block::from(*block);
        match self {
            Self::Aes128(cipher) => cipher.encrypt_block(&mut value),
            Self::Aes192(cipher) => cipher.encrypt_block(&mut value),
            Self::Aes256(cipher) => cipher.encrypt_block(&mut value),
        }
        block.copy_from_slice(&value);
    }

    pub(super) fn decrypt_block(&self, block: &mut [u8; AES_BLOCK_SIZE]) {
        let mut value = aes::Block::from(*block);
        match self {
            Self::Aes128(cipher) => cipher.decrypt_block(&mut value),
            Self::Aes192(cipher) => cipher.decrypt_block(&mut value),
            Self::Aes256(cipher) => cipher.decrypt_block(&mut value),
        }
        block.copy_from_slice(&value);
    }
}

/// Pads `data` using the PKCS#7 algorithm.
#[must_use]
pub fn pkcs7_pad(data: &[u8], block_size: usize) -> Vec<u8> {
    let pad_len = block_size - data.len() % block_size;
    let mut padded = Vec::with_capacity(data.len() + pad_len);
    padded.extend_from_slice(data);
    padded.resize(data.len() + pad_len, pad_len as u8);
    padded
}

/// Removes PKCS#7 padding.
#[allow(clippy::manual_is_multiple_of)] // `% 0` preserves Go's zero-block panic.
pub fn pkcs7_unpad(data: &[u8], block_size: usize) -> Result<Vec<u8>, EncryptError> {
    if data.is_empty() || data.len() % block_size != 0 {
        return Err(EncryptError::InvalidPaddingSize);
    }
    let pad = data[data.len() - 1];
    let pad_len = usize::from(pad);
    if pad_len > block_size || pad_len == 0 {
        return Err(EncryptError::InvalidPaddingSize);
    }
    if data[data.len() - pad_len..data.len() - 1]
        .iter()
        .any(|value| *value != pad)
    {
        return Err(EncryptError::InvalidPadding);
    }
    Ok(data[..data.len() - pad_len].to_vec())
}

fn encrypt_ecb_blocks(cipher: &AesCipher, source: &[u8]) -> Vec<u8> {
    assert_eq!(
        source.len() % AES_BLOCK_SIZE,
        0,
        "ECBEncrypter: input not full blocks"
    );
    let mut destination = source.to_vec();
    for block in destination.chunks_exact_mut(AES_BLOCK_SIZE) {
        let block: &mut [u8; AES_BLOCK_SIZE] = block.try_into().expect("AES chunk has block size");
        cipher.encrypt_block(block);
    }
    destination
}

fn decrypt_ecb_blocks(cipher: &AesCipher, source: &[u8]) -> Vec<u8> {
    assert_eq!(
        source.len() % AES_BLOCK_SIZE,
        0,
        "ECBDecrypter: input not full blocks"
    );
    let mut destination = source.to_vec();
    for block in destination.chunks_exact_mut(AES_BLOCK_SIZE) {
        let block: &mut [u8; AES_BLOCK_SIZE] = block.try_into().expect("AES chunk has block size");
        cipher.decrypt_block(block);
    }
    destination
}

#[cfg(test)]
pub(super) fn ecb_encrypt_raw(source: &[u8], key: &[u8]) -> Result<Vec<u8>, EncryptError> {
    Ok(encrypt_ecb_blocks(&AesCipher::new(key)?, source))
}

#[cfg(test)]
pub(super) fn ecb_decrypt_raw(source: &[u8], key: &[u8]) -> Result<Vec<u8>, EncryptError> {
    Ok(decrypt_ecb_blocks(&AesCipher::new(key)?, source))
}

/// Encrypts arbitrary-length data using AES-ECB and PKCS#7 padding.
pub fn aes_encrypt_with_ecb(data: &[u8], key: &[u8]) -> Result<Vec<u8>, EncryptError> {
    let cipher = AesCipher::new(key)?;
    Ok(encrypt_ecb_blocks(
        &cipher,
        &pkcs7_pad(data, AES_BLOCK_SIZE),
    ))
}

/// Decrypts AES-ECB data and removes PKCS#7 padding.
pub fn aes_decrypt_with_ecb(data: &[u8], key: &[u8]) -> Result<Vec<u8>, EncryptError> {
    let cipher = AesCipher::new(key)?;
    if !data.len().is_multiple_of(AES_BLOCK_SIZE) {
        return Err(EncryptError::CorruptedData);
    }
    pkcs7_unpad(&decrypt_ecb_blocks(&cipher, data), AES_BLOCK_SIZE)
}

/// Derives an AES key using MySQL's historical XOR-folding algorithm.
#[must_use]
pub fn derive_key_mysql(key: &[u8], block_size: usize) -> Vec<u8> {
    let mut derived = vec![0; block_size];
    let mut index = 0;
    for value in key {
        if index == block_size {
            index = 0;
        }
        derived[index] ^= value;
        index += 1;
    }
    derived
}

fn validate_iv(iv: &[u8]) -> [u8; AES_BLOCK_SIZE] {
    assert_eq!(
        iv.len(),
        AES_BLOCK_SIZE,
        "cipher.NewCBCEncrypter: IV length must equal block size"
    );
    iv.try_into().expect("validated AES IV")
}

/// Encrypts arbitrary-length data using AES-CBC and PKCS#7 padding.
pub fn aes_encrypt_with_cbc(data: &[u8], key: &[u8], iv: &[u8]) -> Result<Vec<u8>, EncryptError> {
    let cipher = AesCipher::new(key)?;
    let mut previous = validate_iv(iv);
    let mut destination = pkcs7_pad(data, AES_BLOCK_SIZE);
    for block in destination.chunks_exact_mut(AES_BLOCK_SIZE) {
        for (value, prior) in block.iter_mut().zip(previous) {
            *value ^= prior;
        }
        let block: &mut [u8; AES_BLOCK_SIZE] = block.try_into().expect("AES chunk has block size");
        cipher.encrypt_block(block);
        previous = *block;
    }
    Ok(destination)
}

/// Decrypts AES-CBC data and removes PKCS#7 padding.
pub fn aes_decrypt_with_cbc(data: &[u8], key: &[u8], iv: &[u8]) -> Result<Vec<u8>, EncryptError> {
    let cipher = AesCipher::new(key)?;
    let mut previous = validate_iv(iv);
    if !data.len().is_multiple_of(AES_BLOCK_SIZE) {
        return Err(EncryptError::CorruptedData);
    }
    let mut destination = data.to_vec();
    for block in destination.chunks_exact_mut(AES_BLOCK_SIZE) {
        let mut ciphertext = [0_u8; AES_BLOCK_SIZE];
        ciphertext.copy_from_slice(block);
        let block: &mut [u8; AES_BLOCK_SIZE] = block.try_into().expect("AES chunk has block size");
        cipher.decrypt_block(block);
        for (value, prior) in block.iter_mut().zip(previous) {
            *value ^= prior;
        }
        previous = ciphertext;
    }
    pkcs7_unpad(&destination, AES_BLOCK_SIZE)
}

fn increment_counter(counter: &mut [u8; AES_BLOCK_SIZE]) {
    for value in counter.iter_mut().rev() {
        *value = value.wrapping_add(1);
        if *value != 0 {
            break;
        }
    }
}

fn aes_crypt_with_ofb(data: &[u8], key: &[u8], iv: &[u8]) -> Result<Vec<u8>, EncryptError> {
    let cipher = AesCipher::new(key)?;
    let mut feedback = validate_iv(iv);
    let mut destination = Vec::with_capacity(data.len());
    for chunk in data.chunks(AES_BLOCK_SIZE) {
        cipher.encrypt_block(&mut feedback);
        destination.extend(chunk.iter().zip(feedback).map(|(value, mask)| value ^ mask));
    }
    Ok(destination)
}

/// Encrypts data using AES-OFB.
pub fn aes_encrypt_with_ofb(data: &[u8], key: &[u8], iv: &[u8]) -> Result<Vec<u8>, EncryptError> {
    aes_crypt_with_ofb(data, key, iv)
}

/// Decrypts data using AES-OFB.
pub fn aes_decrypt_with_ofb(data: &[u8], key: &[u8], iv: &[u8]) -> Result<Vec<u8>, EncryptError> {
    aes_crypt_with_ofb(data, key, iv)
}

fn aes_crypt_with_ctr(data: &[u8], key: &[u8], iv: &[u8]) -> Result<Vec<u8>, EncryptError> {
    let cipher = AesCipher::new(key)?;
    let mut counter = validate_iv(iv);
    let mut destination = Vec::with_capacity(data.len());
    for chunk in data.chunks(AES_BLOCK_SIZE) {
        let mut mask = counter;
        cipher.encrypt_block(&mut mask);
        increment_counter(&mut counter);
        destination.extend(chunk.iter().zip(mask).map(|(value, mask)| value ^ mask));
    }
    Ok(destination)
}

/// Encrypts data using AES-CTR.
pub fn aes_encrypt_with_ctr(data: &[u8], key: &[u8], iv: &[u8]) -> Result<Vec<u8>, EncryptError> {
    aes_crypt_with_ctr(data, key, iv)
}

/// Decrypts data using AES-CTR.
pub fn aes_decrypt_with_ctr(data: &[u8], key: &[u8], iv: &[u8]) -> Result<Vec<u8>, EncryptError> {
    aes_crypt_with_ctr(data, key, iv)
}

/// Encrypts data using full-block AES-CFB.
pub fn aes_encrypt_with_cfb(data: &[u8], key: &[u8], iv: &[u8]) -> Result<Vec<u8>, EncryptError> {
    let cipher = AesCipher::new(key)?;
    let mut feedback = validate_iv(iv);
    let mut destination = Vec::with_capacity(data.len());
    for chunk in data.chunks(AES_BLOCK_SIZE) {
        let mut mask = feedback;
        cipher.encrypt_block(&mut mask);
        let start = destination.len();
        destination.extend(chunk.iter().zip(mask).map(|(value, mask)| value ^ mask));
        if chunk.len() == AES_BLOCK_SIZE {
            feedback.copy_from_slice(&destination[start..start + AES_BLOCK_SIZE]);
        }
    }
    Ok(destination)
}

/// Decrypts data using full-block AES-CFB.
pub fn aes_decrypt_with_cfb(data: &[u8], key: &[u8], iv: &[u8]) -> Result<Vec<u8>, EncryptError> {
    let cipher = AesCipher::new(key)?;
    let mut feedback = validate_iv(iv);
    let mut destination = Vec::with_capacity(data.len());
    for chunk in data.chunks(AES_BLOCK_SIZE) {
        let mut mask = feedback;
        cipher.encrypt_block(&mut mask);
        destination.extend(chunk.iter().zip(mask).map(|(value, mask)| value ^ mask));
        if chunk.len() == AES_BLOCK_SIZE {
            feedback.copy_from_slice(chunk);
        }
    }
    Ok(destination)
}

#[cfg(test)]
mod tests {
    #![allow(non_snake_case)]

    use super::*;

    fn from_hex(value: &str) -> Vec<u8> {
        assert_eq!(value.len() % 2, 0);
        value
            .as_bytes()
            .chunks_exact(2)
            .map(|pair| {
                let pair = std::str::from_utf8(pair).expect("ASCII hex");
                u8::from_str_radix(pair, 16).expect("valid hex")
            })
            .collect()
    }

    fn to_hex(value: &[u8]) -> String {
        value.iter().map(|byte| format!("{byte:02X}")).collect()
    }

    #[test]
    fn TestPad() {
        assert_eq!(
            to_hex(&pkcs7_pad(&[0x0a, 0x0b, 0x0c, 0x0d], 8)),
            "0A0B0C0D04040404"
        );
        assert_eq!(
            to_hex(&pkcs7_pad(
                &[0x0a, 0x0b, 0x0c, 0x0d, 0x0a, 0x0b, 0x0c, 0x0d],
                8
            )),
            "0A0B0C0D0A0B0C0D0808080808080808"
        );
        assert_eq!(
            to_hex(&pkcs7_pad(&[0x0a, 0x0b, 0x0c, 0x0d], 16)),
            "0A0B0C0D0C0C0C0C0C0C0C0C0C0C0C0C"
        );
    }

    #[test]
    fn TestUnpad() {
        let valid = [
            ("0A0B0C0D04040404", 8, "0A0B0C0D"),
            ("0A0B0C0D0A0B0C0D0808080808080808", 8, "0A0B0C0D0A0B0C0D"),
            ("0A0B0C0D0C0C0C0C0C0C0C0C0C0C0C0C", 16, "0A0B0C0D"),
            ("0808080808080808", 8, ""),
        ];
        for (data, block_size, expected) in valid {
            assert_eq!(
                to_hex(&pkcs7_unpad(&from_hex(data), block_size).expect("valid padding")),
                expected
            );
        }

        let invalid = [
            ("0A0B0C04040404", 8),
            ("0A0B0C020304040404", 8),
            ("", 8),
            ("0C0C0C0C0C0C0C090909090909090909", 8),
            ("0C0C0C0C0C0C0C00", 8),
            ("0A0B0C0D0A0B0C0D0408080808080808", 8),
            ("0308080808080808", 8),
            ("0A0B0C0D04040304", 8),
        ];
        for (data, block_size) in invalid {
            assert!(pkcs7_unpad(&from_hex(data), block_size).is_err());
        }
    }

    #[test]
    fn TestAESECB() {
        let input = from_hex(concat!(
            "6BC1BEE22E409F96E93D7E117393172A",
            "AE2D8A571E03AC9C9EB76FAC45AF8E51",
            "30C81C46A35CE411E5FBC1191A0A52EF",
            "F69F2445DF4F9B17AD2B417BE66C3710"
        ));
        let cases = [
            (
                "2B7E151628AED2A6ABF7158809CF4F3C",
                concat!(
                    "3AD77BB40D7A3660A89ECAF32466EF97",
                    "F5D3D58503B9699DE785895A96FDBAAF",
                    "43B1CD7F598ECE23881B00E3ED030688",
                    "7B0C785E27E8AD3F8223207104725DD4"
                ),
            ),
            (
                "8E73B0F7DA0E6452C810F32B809079E562F8EAD2522C6B7B",
                concat!(
                    "BD334F1D6E45F25FF712A214571FA5CC",
                    "974104846D0AD3AD7734ECB3ECEE4EEF",
                    "EF7AFD2270E2E60ADCE0BA2FACE6444E",
                    "9A4B41BA738D6C72FB16691603C18E0E"
                ),
            ),
            (
                concat!(
                    "603DEB1015CA71BE2B73AEF0857D7781",
                    "1F352C073B6108D72D9810A30914DFF4"
                ),
                concat!(
                    "F3EED1BDB5D2A03C064B5A7E3DB181F8",
                    "591CCB10D410ED26DC5BA74A31362870",
                    "B6ED21B99CA6F4F9F153E7B1BEAFED1D",
                    "23304B7A39F9F3FF067D8D8F9E24ECC7"
                ),
            ),
        ];
        for (key, expected) in cases {
            let encrypted = ecb_encrypt_raw(&input, &from_hex(key)).expect("valid AES key");
            assert_eq!(to_hex(&encrypted), expected);
            assert_eq!(
                ecb_decrypt_raw(&encrypted, &from_hex(key)).expect("valid AES key"),
                input
            );
        }
    }

    #[test]
    fn TestAESEncryptWithECB() {
        let cases = [
            (
                "pingcap",
                "1234567890123456",
                "697BFE9B3F8C2F289DD82C88C7BC95C4",
            ),
            (
                "pingcap123",
                "1234567890123456",
                "CEC348F4EF5F84D3AA6C4FA184C65766",
            ),
            (
                "pingcap",
                "123456789012345678901234",
                "E435438AC6798B4718533096436EC342",
            ),
        ];
        for (data, key, expected) in cases {
            assert_eq!(
                to_hex(&aes_encrypt_with_ecb(data.as_bytes(), key.as_bytes()).unwrap()),
                expected
            );
        }
        for key in ["12345678901234567", "123456789012345"] {
            assert!(aes_encrypt_with_ecb(b"pingcap", key.as_bytes()).is_err());
        }
    }

    #[test]
    fn TestAESDecryptWithECB() {
        for (expected, key, ciphertext) in [
            (
                "pingcap",
                "1234567890123456",
                "697BFE9B3F8C2F289DD82C88C7BC95C4",
            ),
            (
                "pingcap123",
                "1234567890123456",
                "CEC348F4EF5F84D3AA6C4FA184C65766",
            ),
            (
                "pingcap",
                "123456789012345678901234",
                "E435438AC6798B4718533096436EC342",
            ),
        ] {
            assert_eq!(
                aes_decrypt_with_ecb(&from_hex(ciphertext), key.as_bytes()).unwrap(),
                expected.as_bytes()
            );
        }
        for key in ["12345678901234567", "123456789012345"] {
            assert!(aes_decrypt_with_ecb(&[], key.as_bytes()).is_err());
        }
        for ciphertext in [
            "11223344556677112233",
            "11223344556677112233112233445566",
            "1122334455667711223311223344556611",
        ] {
            assert!(aes_decrypt_with_ecb(&from_hex(ciphertext), b"1234567890123456").is_err());
        }
    }

    #[test]
    fn TestAESEncryptWithCBC() {
        for (data, key, expected) in [
            (
                "pingcap",
                "1234567890123456",
                "2ECA0077C5EA5768A0485AA522774792",
            ),
            (
                "pingcap123",
                "1234567890123456",
                "042962D340F2F95BCC07B56EAC378D3A",
            ),
            (
                "pingcap",
                "123456789012345678901234",
                "EDECE05D9FE662E381130F7F19BA67F7",
            ),
        ] {
            assert_eq!(
                to_hex(
                    &aes_encrypt_with_cbc(data.as_bytes(), key.as_bytes(), b"1234567890123456")
                        .unwrap()
                ),
                expected
            );
        }
        for key in ["12345678901234567", "123456789012345"] {
            assert!(aes_encrypt_with_cbc(b"pingcap", key.as_bytes(), b"1234567890123456").is_err());
        }
    }

    #[test]
    fn TestAESDecryptWithCBC() {
        for (expected, key, ciphertext) in [
            (
                "pingcap",
                "1234567890123456",
                "2ECA0077C5EA5768A0485AA522774792",
            ),
            (
                "pingcap123",
                "1234567890123456",
                "042962D340F2F95BCC07B56EAC378D3A",
            ),
            (
                "pingcap",
                "123456789012345678901234",
                "EDECE05D9FE662E381130F7F19BA67F7",
            ),
        ] {
            assert_eq!(
                aes_decrypt_with_cbc(&from_hex(ciphertext), key.as_bytes(), b"1234567890123456")
                    .unwrap(),
                expected.as_bytes()
            );
        }
        for key in ["12345678901234567", "123456789012345"] {
            assert!(aes_decrypt_with_cbc(&[], key.as_bytes(), b"1234567890123456").is_err());
        }
        for ciphertext in [
            "11223344556677112233",
            "11223344556677112233112233445566",
            "1122334455667711223311223344556611",
        ] {
            assert!(aes_decrypt_with_cbc(
                &from_hex(ciphertext),
                b"1234567890123456",
                b"1234567890123456"
            )
            .is_err());
        }
    }

    const STREAM_CASES: [(&str, &str, &str); 3] = [
        ("pingcap", "1234567890123456", "0515A36BBF3DE0"),
        ("pingcap123", "1234567890123456", "0515A36BBF3DE0DBE9DD"),
        ("pingcap", "123456789012345678901234", "45A57592449893"),
    ];

    type StreamOperation = fn(&[u8], &[u8], &[u8]) -> Result<Vec<u8>, EncryptError>;

    fn check_stream_encrypt(operation: StreamOperation) {
        for (data, key, expected) in STREAM_CASES {
            assert_eq!(
                to_hex(&operation(data.as_bytes(), key.as_bytes(), b"1234567890123456").unwrap()),
                expected
            );
        }
        for key in ["12345678901234567", "123456789012345"] {
            assert!(operation(b"pingcap", key.as_bytes(), b"1234567890123456").is_err());
        }
    }

    fn check_stream_decrypt(operation: StreamOperation) {
        for (expected, key, ciphertext) in STREAM_CASES {
            assert_eq!(
                operation(&from_hex(ciphertext), key.as_bytes(), b"1234567890123456").unwrap(),
                expected.as_bytes()
            );
        }
        for key in ["12345678901234567", "123456789012345"] {
            assert!(operation(b"pingcap", key.as_bytes(), b"1234567890123456").is_err());
        }
    }

    #[test]
    fn TestAESEncryptWithOFB() {
        check_stream_encrypt(aes_encrypt_with_ofb);
    }

    #[test]
    fn TestAESDecryptWithOFB() {
        check_stream_decrypt(aes_decrypt_with_ofb);
    }

    #[test]
    fn TestAESEncryptWithCTR() {
        check_stream_encrypt(aes_encrypt_with_ctr);
    }

    #[test]
    fn TestAESDecryptWithCTR() {
        check_stream_decrypt(aes_decrypt_with_ctr);
    }

    #[test]
    fn TestAESEncryptWithCFB() {
        check_stream_encrypt(aes_encrypt_with_cfb);
    }

    #[test]
    fn TestAESDecryptWithCFB() {
        check_stream_decrypt(aes_decrypt_with_cfb);
    }

    #[test]
    fn TestDeriveKeyMySQL() {
        assert_eq!(
            to_hex(&derive_key_mysql(b"MySQL=insecure! MySQL=insecure! ", 16)),
            "00000000000000000000000000000000"
        );
        assert_eq!(
            to_hex(&derive_key_mysql(&[0xc0, 0x10, 0x44, 0xcc, 0x10, 0xd9], 16)),
            "C01044CC10D900000000000000000000"
        );
        assert_eq!(
            to_hex(&derive_key_mysql(b"MySecretVeryLooooongPassword", 16)),
            "22163D0233131607210A001D4C6F6F6F"
        );
    }
}
