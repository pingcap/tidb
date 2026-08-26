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

//! Ports of `pkg/util/encrypt` unit tests from Go (`aes_test.go`,
//! `aes_layer_test.go`, `crypt_test.go`).

use crate::encrypt::{
    aes_decrypt_with_cbc, aes_decrypt_with_cfb, aes_decrypt_with_ctr, aes_decrypt_with_ecb,
    aes_decrypt_with_ofb, aes_encrypt_with_cbc, aes_encrypt_with_cfb, aes_encrypt_with_ctr,
    aes_encrypt_with_ecb, aes_encrypt_with_ofb, derive_key_mysql, pkcs7_pad, pkcs7_unpad,
    sql_decode, sql_encode, CtrCipher, EncryptError, Reader, Writer,
};
use crate::layered_io::{ReadAt, ReadAtError, ReadAtResult};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};

fn to_hex(buf: &[u8]) -> String {
    buf.iter().map(|b| format!("{b:02X}")).collect()
}

// Mirrors Go's lenient use of hex.DecodeString in the tests: bytes decoded
// until the first invalid pair are kept, the error is discarded.
fn hex_decode(s: &str) -> Vec<u8> {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len() / 2);
    for pair in bytes.chunks_exact(2) {
        let hi = (pair[0] as char).to_digit(16);
        let lo = (pair[1] as char).to_digit(16);
        match (hi, lo) {
            (Some(hi), Some(lo)) => out.push((hi * 16 + lo) as u8),
            _ => break,
        }
    }
    out
}

/// Go: pkg/util/encrypt/aes_test.go TestPad
#[test]
fn pad_cases() {
    let p = pkcs7_pad(&[0x0A, 0x0B, 0x0C, 0x0D], 8);
    assert_eq!(to_hex(&p), "0A0B0C0D04040404");

    let p = pkcs7_pad(&[0x0A, 0x0B, 0x0C, 0x0D, 0x0A, 0x0B, 0x0C, 0x0D], 8);
    assert_eq!(to_hex(&p), "0A0B0C0D0A0B0C0D0808080808080808");

    let p = pkcs7_pad(&[0x0A, 0x0B, 0x0C, 0x0D], 16);
    assert_eq!(to_hex(&p), "0A0B0C0D0C0C0C0C0C0C0C0C0C0C0C0C");
}

/// Go: pkg/util/encrypt/aes_test.go TestUnpad
#[test]
fn unpad_cases() {
    // Valid paddings.
    let p = pkcs7_unpad(&[0x0A, 0x0B, 0x0C, 0x0D, 0x04, 0x04, 0x04, 0x04], 8).unwrap();
    assert_eq!(to_hex(&p), "0A0B0C0D");

    let p = pkcs7_unpad(
        &[
            0x0A, 0x0B, 0x0C, 0x0D, 0x0A, 0x0B, 0x0C, 0x0D, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08,
            0x08, 0x08,
        ],
        8,
    )
    .unwrap();
    assert_eq!(to_hex(&p), "0A0B0C0D0A0B0C0D");

    let p = pkcs7_unpad(
        &[
            0x0A, 0x0B, 0x0C, 0x0D, 0x0C, 0x0C, 0x0C, 0x0C, 0x0C, 0x0C, 0x0C, 0x0C, 0x0C, 0x0C,
            0x0C, 0x0C,
        ],
        16,
    )
    .unwrap();
    assert_eq!(to_hex(&p), "0A0B0C0D");

    let p = pkcs7_unpad(&[0x08; 8], 8).unwrap();
    assert_eq!(to_hex(&p), "");

    // Invalid padding: incorrect block size
    assert!(pkcs7_unpad(&[0x0A, 0x0B, 0x0C, 0x04, 0x04, 0x04, 0x04], 8).is_err());

    assert!(
        pkcs7_unpad(
            &[0x0A, 0x0B, 0x0C, 0x02, 0x03, 0x04, 0x04, 0x04, 0x04],
            8
        )
        .is_err()
    );

    assert!(pkcs7_unpad(&[], 8).is_err());

    // Invalid padding: padding length > block length
    assert!(pkcs7_unpad(
        &[
            0x0C, 0x0C, 0x0C, 0x0C, 0x0C, 0x0C, 0x0C, 0x09, 0x09, 0x09, 0x09, 0x09, 0x09, 0x09,
            0x09, 0x09
        ],
        8
    )
    .is_err());

    // Invalid padding: padding length == 0
    assert!(
        pkcs7_unpad(&[0x0C, 0x0C, 0x0C, 0x0C, 0x0C, 0x0C, 0x0C, 0x00], 8).is_err()
    );

    // Invalid padding: padding content invalid
    assert!(pkcs7_unpad(
        &[
            0x0A, 0x0B, 0x0C, 0x0D, 0x0A, 0x0B, 0x0C, 0x0D, 0x04, 0x08, 0x08, 0x08, 0x08, 0x08,
            0x08, 0x08
        ],
        8
    )
    .is_err());

    // Invalid padding: padding content invalid
    assert!(
        pkcs7_unpad(&[0x03, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08], 8).is_err()
    );

    // Invalid padding: padding content invalid
    assert!(pkcs7_unpad(&[0x0A, 0x0B, 0x0C, 0x0D, 0x04, 0x04, 0x03, 0x04], 8).is_err());
}

/// Go: pkg/util/encrypt/aes_test.go TestAESECB (NIST SP 800-38A vectors)
// go-parity-gap: Go's test drives the package-private raw ECB mode
// (newECBEncrypter/newECBDecrypter, no PKCS#7 framing); tidb-util::encrypt
// only exports padding-wrapped aes_encrypt_with_ecb/aes_decrypt_with_ecb.
#[test]
#[ignore]
fn aes_ecb_vectors() {
    let common_input: [u8; 64] = [
        0x6b, 0xc1, 0xbe, 0xe2, 0x2e, 0x40, 0x9f, 0x96, 0xe9, 0x3d, 0x7e, 0x11, 0x73, 0x93, 0x17,
        0x2a, 0xae, 0x2d, 0x8a, 0x57, 0x1e, 0x03, 0xac, 0x9c, 0x9e, 0xb7, 0x6f, 0xac, 0x45, 0xaf,
        0x8e, 0x51, 0x30, 0xc8, 0x1c, 0x46, 0xa3, 0x5c, 0xe4, 0x11, 0xe5, 0xfb, 0xc1, 0x19, 0x1a,
        0x0a, 0x52, 0xef, 0xf6, 0x9f, 0x24, 0x45, 0xdf, 0x4f, 0x9b, 0x17, 0xad, 0x2b, 0x41, 0x7b,
        0xe6, 0x6c, 0x37, 0x10,
    ];
    let common_key_128: [u8; 16] = [
        0x2b, 0x7e, 0x15, 0x16, 0x28, 0xae, 0xd2, 0xa6, 0xab, 0xf7, 0x15, 0x88, 0x09, 0xcf, 0x4f,
        0x3c,
    ];
    let common_key_192: [u8; 24] = [
        0x8e, 0x73, 0xb0, 0xf7, 0xda, 0x0e, 0x64, 0x52, 0xc8, 0x10, 0xf3, 0x2b, 0x80, 0x90, 0x79,
        0xe5, 0x62, 0xf8, 0xea, 0xd2, 0x52, 0x2c, 0x6b, 0x7b,
    ];
    let common_key_256: [u8; 32] = [
        0x60, 0x3d, 0xeb, 0x10, 0x15, 0xca, 0x71, 0xbe, 0x2b, 0x73, 0xae, 0xf0, 0x85, 0x7d, 0x77,
        0x81, 0x1f, 0x35, 0x2c, 0x07, 0x3b, 0x61, 0x08, 0xd7, 0x2d, 0x98, 0x10, 0xa3, 0x09, 0x14,
        0xdf, 0xf4,
    ];
    let ecb_aes_tests: [(&str, &[u8], &[u8]); 3] = [
        (
            "ECB-AES128",
            &common_key_128,
            &[
                0x3a, 0xd7, 0x7b, 0xb4, 0x0d, 0x7a, 0x36, 0x60, 0xa8, 0x9e, 0xca, 0xf3, 0x24,
                0x66, 0xef, 0x97, 0xf5, 0xd3, 0xd5, 0x85, 0x03, 0xb9, 0x69, 0x9d, 0xe7, 0x85,
                0x89, 0x5a, 0x96, 0xfd, 0xba, 0xaf, 0x43, 0xb1, 0xcd, 0x7f, 0x59, 0x8e, 0xce,
                0x23, 0x88, 0x1b, 0x00, 0xe3, 0xed, 0x03, 0x06, 0x88, 0x7b, 0x0c, 0x78, 0x5e,
                0x27, 0xe8, 0xad, 0x3f, 0x82, 0x23, 0x20, 0x71, 0x04, 0x72, 0x5d, 0xd4,
            ],
        ),
        (
            "ECB-AES192",
            &common_key_192,
            &[
                0xbd, 0x33, 0x4f, 0x1d, 0x6e, 0x45, 0xf2, 0x5f, 0xf7, 0x12, 0xa2, 0x14, 0x57,
                0x1f, 0xa5, 0xcc, 0x97, 0x41, 0x04, 0x84, 0x6d, 0x0a, 0xd3, 0xad, 0x77, 0x34,
                0xec, 0xb3, 0xec, 0xee, 0x4e, 0xef, 0xef, 0x7a, 0xfd, 0x22, 0x70, 0xe2, 0xe6,
                0x0a, 0xdc, 0xe0, 0xba, 0x2f, 0xac, 0xe6, 0x44, 0x4e, 0x9a, 0x4b, 0x41, 0xba,
                0x73, 0x8d, 0x6c, 0x72, 0xfb, 0x16, 0x69, 0x16, 0x03, 0xc1, 0x8e, 0x0e,
            ],
        ),
        (
            "ECB-AES256",
            &common_key_256,
            &[
                0xf3, 0xee, 0xd1, 0xbd, 0xb5, 0xd2, 0xa0, 0x3c, 0x06, 0x4b, 0x5a, 0x7e, 0x3d,
                0xb1, 0x81, 0xf8, 0x59, 0x1c, 0xcb, 0x10, 0xd4, 0x10, 0xed, 0x26, 0xdc, 0x5b,
                0xa7, 0x4a, 0x31, 0x36, 0x28, 0x70, 0xb6, 0xed, 0x21, 0xb9, 0x9c, 0xa6, 0xf4,
                0xf9, 0xf1, 0x53, 0xe7, 0xb1, 0xbe, 0xaf, 0xed, 0x1d, 0x23, 0x30, 0x4b, 0x7a,
                0x39, 0xf9, 0xf3, 0xff, 0x06, 0x7d, 0x8d, 0x8f, 0x9e, 0x24, 0xec, 0xc7,
            ],
        ),
    ];

    for (name, key, expected) in ecb_aes_tests {
        let encrypted = aes_encrypt_with_ecb(&common_input, key)
            .unwrap_or_else(|err| panic!("{name}: NewCipher({} bytes) = {err}", key.len()));
        assert_eq!(to_hex(expected), to_hex(&encrypted), "{name}: ECBEncrypter");

        let decrypted = aes_decrypt_with_ecb(&encrypted, key)
            .unwrap_or_else(|err| panic!("{name}: decrypt failed: {err}"));
        assert_eq!(
            to_hex(&common_input),
            to_hex(&decrypted),
            "{name}: ECBDecrypter"
        );
    }
}

/// Go: pkg/util/encrypt/aes_test.go TestAESEncryptWithECB
#[test]
fn aes_encrypt_with_ecb_cases() {
    let tests: [(&str, &str, &str, bool); 5] = [
        // 128 bits key
        ("pingcap", "1234567890123456", "697BFE9B3F8C2F289DD82C88C7BC95C4", false),
        ("pingcap123", "1234567890123456", "CEC348F4EF5F84D3AA6C4FA184C65766", false),
        // 192 bits key
        ("pingcap", "123456789012345678901234", "E435438AC6798B4718533096436EC342", false),
        // negative cases: invalid key length
        ("pingcap", "12345678901234567", "", true),
        ("pingcap", "123456789012345", "", true),
    ];
    for (str, key, expect, is_error) in tests {
        let result = aes_encrypt_with_ecb(str.as_bytes(), key.as_bytes());
        if is_error {
            assert_eq!(result.unwrap_err(), EncryptError::InvalidKeyLength(key.len()));
        } else {
            assert_eq!(expect, to_hex(&result.unwrap()), "{str}/{key}");
        }
    }
}

/// Go: pkg/util/encrypt/aes_test.go TestAESDecryptWithECB
#[test]
fn aes_decrypt_with_ecb_cases() {
    let tests: [(&str, &str, &str, bool); 8] = [
        // 128 bits key
        ("pingcap", "1234567890123456", "697BFE9B3F8C2F289DD82C88C7BC95C4", false),
        ("pingcap123", "1234567890123456", "CEC348F4EF5F84D3AA6C4FA184C65766", false),
        // 192 bits key
        ("pingcap", "123456789012345678901234", "E435438AC6798B4718533096436EC342", false),
        // negative cases: invalid key length
        ("pingcap", "12345678901234567", "", true),
        ("pingcap", "123456789012345", "", true),
        // negative cases: invalid padding / padding size
        ("", "1234567890123456", "11223344556677112233", true),
        ("", "1234567890123456", "11223344556677112233112233445566", true),
        ("", "1234567890123456", "1122334455667711223311223344556611", true),
    ];
    for (expect, key, hex_crypt_str, is_error) in tests {
        let crypt_str = hex_decode(hex_crypt_str);
        let result = aes_decrypt_with_ecb(&crypt_str, key.as_bytes());
        if is_error {
            assert!(result.is_err(), "{key}/{hex_crypt_str}");
        } else {
            assert_eq!(expect.as_bytes(), result.unwrap().as_slice());
        }
    }
}

/// Go: pkg/util/encrypt/aes_test.go TestAESEncryptWithCBC
#[test]
fn aes_encrypt_with_cbc_cases() {
    let tests: [(&str, &str, &str, &str, bool); 5] = [
        // 128 bits key
        ("pingcap", "1234567890123456", "1234567890123456", "2ECA0077C5EA5768A0485AA522774792", false),
        ("pingcap123", "1234567890123456", "1234567890123456", "042962D340F2F95BCC07B56EAC378D3A", false),
        // 192 bits key
        ("pingcap", "123456789012345678901234", "1234567890123456", "EDECE05D9FE662E381130F7F19BA67F7", false),
        // negative cases: invalid key length
        ("pingcap", "12345678901234567", "1234567890123456", "", true),
        ("pingcap", "123456789012345", "1234567890123456", "", true),
    ];
    for (str, key, iv, expect, is_error) in tests {
        let result = aes_encrypt_with_cbc(str.as_bytes(), key.as_bytes(), iv.as_bytes());
        if is_error {
            assert_eq!(result.unwrap_err(), EncryptError::InvalidKeyLength(key.len()));
        } else {
            assert_eq!(expect, to_hex(&result.unwrap()), "{str}/{key}");
        }
    }
}

/// Go: pkg/util/encrypt/aes_test.go TestAESEncryptWithOFB
#[test]
fn aes_encrypt_with_ofb_cases() {
    let tests: [(&str, &str, &str, &str, bool); 5] = [
        // 128 bits key
        ("pingcap", "1234567890123456", "1234567890123456", "0515A36BBF3DE0", false),
        ("pingcap123", "1234567890123456", "1234567890123456", "0515A36BBF3DE0DBE9DD", false),
        // 192 bits key
        ("pingcap", "123456789012345678901234", "1234567890123456", "45A57592449893", false),
        // negative cases: invalid key length
        ("pingcap", "12345678901234567", "1234567890123456", "", true),
        ("pingcap", "123456789012345", "1234567890123456", "", true),
    ];
    for (str, key, iv, expect, is_error) in tests {
        let result = aes_encrypt_with_ofb(str.as_bytes(), key.as_bytes(), iv.as_bytes());
        if is_error {
            assert_eq!(result.unwrap_err(), EncryptError::InvalidKeyLength(key.len()));
        } else {
            assert_eq!(expect, to_hex(&result.unwrap()), "{str}/{key}");
        }
    }
}

/// Go: pkg/util/encrypt/aes_test.go TestAESDecryptWithOFB
#[test]
fn aes_decrypt_with_ofb_cases() {
    let tests: [(&str, &str, &str, &str, bool); 5] = [
        // 128 bits key
        ("0515A36BBF3DE0", "1234567890123456", "1234567890123456", "pingcap", false),
        ("0515A36BBF3DE0DBE9DD", "1234567890123456", "1234567890123456", "pingcap123", false),
        // 192 bits key
        ("45A57592449893", "123456789012345678901234", "1234567890123456", "pingcap", false),
        // negative cases: invalid key length
        ("pingcap", "12345678901234567", "1234567890123456", "", true),
        ("pingcap", "123456789012345", "1234567890123456", "", true),
    ];
    for (str, key, iv, expect, is_error) in tests {
        let cipher_text = hex_decode(str);
        let result = aes_decrypt_with_ofb(&cipher_text, key.as_bytes(), iv.as_bytes());
        if is_error {
            assert_eq!(result.unwrap_err(), EncryptError::InvalidKeyLength(key.len()));
        } else {
            assert_eq!(expect.as_bytes(), result.unwrap().as_slice(), "{str}/{key}");
        }
    }
}

/// Go: pkg/util/encrypt/aes_test.go TestAESEncryptWithCTR
#[test]
fn aes_encrypt_with_ctr_cases() {
    let tests: [(&str, &str, &str, &str, bool); 5] = [
        // 128 bits key
        ("pingcap", "1234567890123456", "1234567890123456", "0515A36BBF3DE0", false),
        ("pingcap123", "1234567890123456", "1234567890123456", "0515A36BBF3DE0DBE9DD", false),
        // 192 bits key
        ("pingcap", "123456789012345678901234", "1234567890123456", "45A57592449893", false),
        // negative cases: invalid key length
        ("pingcap", "12345678901234567", "1234567890123456", "", true),
        ("pingcap", "123456789012345", "1234567890123456", "", true),
    ];
    for (str, key, iv, expect, is_error) in tests {
        let result = aes_encrypt_with_ctr(str.as_bytes(), key.as_bytes(), iv.as_bytes());
        if is_error {
            assert_eq!(result.unwrap_err(), EncryptError::InvalidKeyLength(key.len()));
        } else {
            assert_eq!(expect, to_hex(&result.unwrap()), "{str}/{key}");
        }
    }
}

/// Go: pkg/util/encrypt/aes_test.go TestAESDecryptWithCTR
#[test]
fn aes_decrypt_with_ctr_cases() {
    let tests: [(&str, &str, &str, &str, bool); 5] = [
        // 128 bits key
        ("0515A36BBF3DE0", "1234567890123456", "1234567890123456", "pingcap", false),
        ("0515A36BBF3DE0DBE9DD", "1234567890123456", "1234567890123456", "pingcap123", false),
        // 192 bits key
        ("45A57592449893", "123456789012345678901234", "1234567890123456", "pingcap", false),
        // negative cases: invalid key length
        ("pingcap", "12345678901234567", "1234567890123456", "", true),
        ("pingcap", "123456789012345", "1234567890123456", "", true),
    ];
    for (str, key, iv, expect, is_error) in tests {
        let cipher_text = hex_decode(str);
        let result = aes_decrypt_with_ctr(&cipher_text, key.as_bytes(), iv.as_bytes());
        if is_error {
            assert_eq!(result.unwrap_err(), EncryptError::InvalidKeyLength(key.len()));
        } else {
            assert_eq!(expect.as_bytes(), result.unwrap().as_slice(), "{str}/{key}");
        }
    }
}

/// Go: pkg/util/encrypt/aes_test.go TestAESDecryptWithCBC
#[test]
fn aes_decrypt_with_cbc_cases() {
    let tests: [(&str, &str, &str, &str, bool); 8] = [
        // 128 bits key
        ("pingcap", "1234567890123456", "1234567890123456", "2ECA0077C5EA5768A0485AA522774792", false),
        ("pingcap123", "1234567890123456", "1234567890123456", "042962D340F2F95BCC07B56EAC378D3A", false),
        // 192 bits key
        ("pingcap", "123456789012345678901234", "1234567890123456", "EDECE05D9FE662E381130F7F19BA67F7", false),
        // negative cases: invalid key length
        ("pingcap", "12345678901234567", "1234567890123456", "", true),
        ("pingcap", "123456789012345", "1234567890123456", "", true),
        // negative cases: invalid padding / padding size
        ("", "1234567890123456", "1234567890123456", "11223344556677112233", true),
        ("", "1234567890123456", "1234567890123456", "11223344556677112233112233445566", true),
        ("", "1234567890123456", "1234567890123456", "1122334455667711223311223344556611", true),
    ];
    for (expect, key, _iv, hex_crypt_str, is_error) in tests {
        let crypt_str = hex_decode(hex_crypt_str);
        let result = aes_decrypt_with_cbc(&crypt_str, key.as_bytes(), _iv.as_bytes());
        if is_error {
            assert!(result.is_err(), "{key}/{hex_crypt_str}");
        } else {
            assert_eq!(expect.as_bytes(), result.unwrap().as_slice());
        }
    }
}

/// Go: pkg/util/encrypt/aes_test.go TestAESEncryptWithCFB
#[test]
fn aes_encrypt_with_cfb_cases() {
    let tests: [(&str, &str, &str, &str, bool); 5] = [
        // 128 bits key
        ("pingcap", "1234567890123456", "1234567890123456", "0515A36BBF3DE0", false),
        ("pingcap123", "1234567890123456", "1234567890123456", "0515A36BBF3DE0DBE9DD", false),
        // 192 bits key
        ("pingcap", "123456789012345678901234", "1234567890123456", "45A57592449893", false),
        // negative cases: invalid key length
        ("pingcap", "12345678901234567", "1234567890123456", "", true),
        ("pingcap", "123456789012345", "1234567890123456", "", true),
    ];
    for (str, key, iv, expect, is_error) in tests {
        let result = aes_encrypt_with_cfb(str.as_bytes(), key.as_bytes(), iv.as_bytes());
        if is_error {
            assert_eq!(result.unwrap_err(), EncryptError::InvalidKeyLength(key.len()));
        } else {
            assert_eq!(expect, to_hex(&result.unwrap()), "{str}/{key}");
        }
    }
}

/// Go: pkg/util/encrypt/aes_test.go TestAESDecryptWithCFB
#[test]
fn aes_decrypt_with_cfb_cases() {
    let tests: [(&str, &str, &str, &str, bool); 5] = [
        // 128 bits key
        ("0515A36BBF3DE0", "1234567890123456", "1234567890123456", "pingcap", false),
        ("0515A36BBF3DE0DBE9DD", "1234567890123456", "1234567890123456", "pingcap123", false),
        // 192 bits key
        ("45A57592449893", "123456789012345678901234", "1234567890123456", "pingcap", false),
        // negative cases: invalid key length
        ("pingcap", "12345678901234567", "1234567890123456", "", true),
        ("pingcap", "123456789012345", "1234567890123456", "", true),
    ];
    for (str, key, iv, expect, is_error) in tests {
        let cipher_text = hex_decode(str);
        let result = aes_decrypt_with_cfb(&cipher_text, key.as_bytes(), iv.as_bytes());
        if is_error {
            assert_eq!(result.unwrap_err(), EncryptError::InvalidKeyLength(key.len()));
        } else {
            assert_eq!(expect.as_bytes(), result.unwrap().as_slice(), "{str}/{key}");
        }
    }
}

/// Go: pkg/util/encrypt/aes_test.go TestDeriveKeyMySQL
#[test]
fn derive_key_mysql_cases() {
    let p = derive_key_mysql(b"MySQL=insecure! MySQL=insecure! ", 16);
    assert_eq!("00000000000000000000000000000000", to_hex(&p));

    // Short password.
    let p = derive_key_mysql(&[0xC0, 0x10, 0x44, 0xCC, 0x10, 0xD9], 16);
    assert_eq!("C01044CC10D900000000000000000000", to_hex(&p));

    // Long password.
    let p = derive_key_mysql(b"MySecretVeryLooooongPassword", 16);
    assert_eq!("22163D0233131607210A001D4C6F6F6F", to_hex(&p));
}

/// Go: pkg/util/encrypt/crypt_test.go TestSQLDecode
#[test]
fn sql_decode_cases() {
    let tests: [(&str, &str, &str); 10] = [
        ("", "", ""),
        ("pingcap", "1234567890123456", "2C35B5A4ADF391"),
        ("pingcap", "asdfjasfwefjfjkj", "351CC412605905"),
        ("pingcap123", "123456789012345678901234", "7698723DC6DFE7724221"),
        ("pingcap#%$%^", "*^%YTu1234567", "8634B9C55FF55E5B6328F449"),
        ("pingcap", "", "4A77B524BD2C5C"),
        ("分布式データベース", "pass1234@#$%%^^&", "80CADC8D328B3026D04FB285F36FED04BBCA0CC685BF78B1E687CE"),
        ("分布式データベース", "分布式7782734adgwy1242", "0E24CFEF272EE32B6E0BFBDB89F29FB43B4B30DAA95C3F914444BC"),
        ("pingcap", "密匙", "CE5C02A5010010"),
        ("pingcap数据库", "数据库passwd12345667", "36D5F90D3834E30E396BE3226E3B4ED3"),
    ];
    for (str, passwd, expect) in tests {
        let crypted = sql_decode(str.as_bytes(), passwd.as_bytes());
        assert_eq!(expect, to_hex(&crypted), "{str}/{passwd}");
    }
}

/// Go: pkg/util/encrypt/crypt_test.go TestSQLEncode
#[test]
fn sql_encode_round_trip_cases() {
    let tests: [(&str, &str); 10] = [
        ("", ""),
        ("pingcap", "1234567890123456"),
        ("pingcap", "asdfjasfwefjfjkj"),
        ("pingcap123", "123456789012345678901234"),
        ("pingcap#%$%^", "*^%YTu1234567"),
        ("pingcap", ""),
        ("分布式データベース", "pass1234@#$%%^^&"),
        ("分布式データベース", "分布式7782734adgwy1242"),
        ("pingcap", "密匙"),
        ("pingcap数据库", "数据库passwd12345667"),
    ];
    for (str, passwd) in tests {
        let crypted = sql_decode(str.as_bytes(), passwd.as_bytes());
        let uncrypted = sql_encode(&crypted, passwd.as_bytes());
        assert_eq!(str.as_bytes(), uncrypted.as_slice(), "{str}/{passwd}");
    }
}

/// Go: pkg/util/encrypt/aes_layer_test.go TestReadAt — random-access reads
/// through the CTR layer alone and stacked with itself and the checksum
/// layer, over a real file like Go's os.File-based test.
#[test]
fn read_at_layer_combinations() {
    let ctr_cipher1 = CtrCipher::new().unwrap();
    let ctr_cipher2 = CtrCipher::new().unwrap();

    let write_string = b"0123456789";
    let mut payload = Vec::new();
    for _ in 0..510 {
        payload.extend_from_slice(write_string);
    }
    let logical_length = payload.len() * 2;

    fn assert_reads(reader: impl ReadAt, logical_length: usize, name: &str) {
        for (offset, expected_n, expected, eof) in [
            (0_i64, 10, b"0123456789".as_slice(), false),
            (5, 10, b"5678901234".as_slice(), false),
            (
                logical_length as i64 - 5,
                5,
                b"56789\0\0\0\0\0".as_slice(),
                true,
            ),
        ] {
            let mut destination = [0_u8; 10];
            let result = reader.read_at(&mut destination, offset);
            assert_eq!(result.n, expected_n, "{name} offset {offset}");
            assert_eq!(destination, expected, "{name} offset {offset}");
            assert_eq!(
                result.error.as_ref().is_some_and(ReadAtError::is_eof),
                eof,
                "{name} offset {offset} eof flag"
            );
        }
    }

    // Stack 1: data -> encrypt -> file.
    {
        let dir = tempfile::Builder::new()
            .prefix("testport-b021")
            .tempdir()
            .unwrap();
        let file = std::fs::File::options()
            .read(true)
            .write(true)
            .create_new(true)
            .open(dir.path().join("ase"))
            .unwrap();
        {
            let mut writer = Writer::new(file.try_clone().unwrap(), &ctr_cipher1);
            writer.write_all(&payload).unwrap();
            writer.write_all(&payload).unwrap();
            writer.close().unwrap();
        }
        assert_reads(Reader::new(file, &ctr_cipher1), logical_length, "ctr");
    }

    // Stack 2: data -> checksum -> encrypt -> file.
    {
        let dir = tempfile::Builder::new()
            .prefix("testport-b021")
            .tempdir()
            .unwrap();
        let file = std::fs::File::options()
            .read(true)
            .write(true)
            .create_new(true)
            .open(dir.path().join("ase"))
            .unwrap();
        {
            let encrypted = Writer::new(file.try_clone().unwrap(), &ctr_cipher1);
            let mut writer = crate::checksum::Writer::new(encrypted);
            writer.write_all(&payload).unwrap();
            writer.write_all(&payload).unwrap();
            writer.close().unwrap();
        }
        assert_reads(
            crate::checksum::Reader::new(Reader::new(file, &ctr_cipher1)),
            logical_length,
            "checksum(ctr)",
        );
    }

    // Stack 3: data -> encrypt -> checksum -> file.
    {
        let dir = tempfile::Builder::new()
            .prefix("testport-b021")
            .tempdir()
            .unwrap();
        let file = std::fs::File::options()
            .read(true)
            .write(true)
            .create_new(true)
            .open(dir.path().join("ase"))
            .unwrap();
        {
            let checksummed = crate::checksum::Writer::new(file.try_clone().unwrap());
            let mut writer = Writer::new(checksummed, &ctr_cipher1);
            writer.write_all(&payload).unwrap();
            writer.write_all(&payload).unwrap();
            writer.close().unwrap();
        }
        assert_reads(
            Reader::new(crate::checksum::Reader::new(file), &ctr_cipher1),
            logical_length,
            "ctr(checksum)",
        );
    }

    // Stack 4: data -> encrypt -> encrypt -> file (two independent ciphers).
    {
        let dir = tempfile::Builder::new()
            .prefix("testport-b021")
            .tempdir()
            .unwrap();
        let file = std::fs::File::options()
            .read(true)
            .write(true)
            .create_new(true)
            .open(dir.path().join("ase"))
            .unwrap();
        {
            let first = Writer::new(file.try_clone().unwrap(), &ctr_cipher1);
            let mut writer = Writer::new(first, &ctr_cipher2);
            writer.write_all(&payload).unwrap();
            writer.write_all(&payload).unwrap();
            writer.close().unwrap();
        }
        assert_reads(
            Reader::new(Reader::new(file, &ctr_cipher1), &ctr_cipher2),
            logical_length,
            "ctr(ctr)",
        );
    }
}
