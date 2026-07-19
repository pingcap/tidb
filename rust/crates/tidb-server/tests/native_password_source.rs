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

#![allow(dead_code, missing_docs)]

#[path = "../src/native_password.rs"]
mod native_password;

use native_password::{
    generate_handshake_salt, verify_candidate, NativePasswordHash, NativePasswordHashError,
    HANDSHAKE_SALT_LEN,
};
use sha1::{Digest, Sha1};

const ABC_STAGE_TWO: &str = "*0D3CED9BEC10A777AEC23CCC353A8C08A633045E";
const SOURCE_SALT: [u8; 20] = [
    85, 92, 45, 22, 58, 79, 107, 6, 122, 125, 58, 80, 12, 90, 103, 32, 90, 10, 74, 82,
];
const SOURCE_RESPONSE: [u8; 20] = [
    24, 180, 183, 225, 166, 6, 81, 102, 70, 248, 199, 143, 91, 204, 169, 9, 161, 171, 203, 33,
];

#[test]
fn parses_exact_upper_lower_and_mixed_case_stage_two_hashes() {
    let upper = NativePasswordHash::parse(ABC_STAGE_TWO).expect("upper-case hash");
    let lower =
        NativePasswordHash::parse(&ABC_STAGE_TWO.to_ascii_lowercase()).expect("lower-case hash");
    let mixed = NativePasswordHash::parse("*0d3CeD9bEc10A777aEc23cCc353A8C08a633045e")
        .expect("mixed-case hash");

    assert_eq!(upper, lower);
    assert_eq!(upper, mixed);
    assert_eq!(format!("{upper:?}"), "NativePasswordHash([REDACTED])");
    assert!(!format!("{upper:?}").contains(&ABC_STAGE_TWO[1..]));
}

#[test]
fn rejects_every_noncanonical_encoded_shape_without_echoing_secret() {
    let cases = [
        ("", NativePasswordHashError::InvalidLength),
        (
            "0D3CED9BEC10A777AEC23CCC353A8C08A633045E",
            NativePasswordHashError::InvalidLength,
        ),
        (
            "!0D3CED9BEC10A777AEC23CCC353A8C08A633045E",
            NativePasswordHashError::MissingPrefix,
        ),
        (
            "*0D3CED9BEC10A777AEC23CCC353A8C08A633045",
            NativePasswordHashError::InvalidLength,
        ),
        (
            "*0D3CED9BEC10A777AEC23CCC353A8C08A633045EE",
            NativePasswordHashError::InvalidLength,
        ),
        (
            "*0D3CED9BEC10A777AEC23CCC353A8C08A633045Z",
            NativePasswordHashError::InvalidHex,
        ),
        (
            "*0D3CED9BEC10A777AEC23CCC353A8C08A63304é",
            NativePasswordHashError::InvalidLength,
        ),
    ];

    for (encoded, expected) in cases {
        let error = NativePasswordHash::parse(encoded).expect_err("invalid hash");
        assert_eq!(error, expected);
        assert!(!error.to_string().contains(encoded));
    }
}

#[test]
fn verifies_tidb_source_vector_and_rejects_wrong_or_malformed_responses() {
    let hash = NativePasswordHash::parse(ABC_STAGE_TWO).expect("source hash");
    assert!(hash.verify(&SOURCE_SALT, &SOURCE_RESPONSE));

    let mut wrong = SOURCE_RESPONSE;
    wrong[7] ^= 0x80;
    assert!(!hash.verify(&SOURCE_SALT, &wrong));
    assert!(!hash.verify(&SOURCE_SALT, &[]));
    assert!(!hash.verify(&SOURCE_SALT, &[0; 19]));
    assert!(!hash.verify(&SOURCE_SALT, &[0; 21]));
    assert!(!hash.verify(&SOURCE_SALT[..19], &SOURCE_RESPONSE));
}

#[test]
fn challenge_verifier_matches_independent_client_scramble() {
    let password = b"correct horse battery staple";
    let stage_one = Sha1::digest(password);
    let stage_two = Sha1::digest(stage_one);
    let encoded = format!("*{}", encode_hex_upper(&stage_two));
    let hash = NativePasswordHash::parse(&encoded).expect("generated stage-two hash");
    let response = scramble(password, &SOURCE_SALT);

    assert!(hash.verify(&SOURCE_SALT, &response));
    assert!(verify_candidate(Some(&hash), &SOURCE_SALT, &response));
    assert!(!verify_candidate(None, &SOURCE_SALT, &response));
}

#[test]
fn operating_system_salt_has_protocol_width_and_no_nul_bytes() {
    for _ in 0..32 {
        let salt = generate_handshake_salt().expect("OS entropy");
        assert_eq!(salt.len(), HANDSHAKE_SALT_LEN);
        assert!(salt.iter().all(|byte| *byte != 0));
    }
}

fn scramble(password: &[u8], salt: &[u8]) -> [u8; 20] {
    let stage_one = Sha1::digest(password);
    let stage_two = Sha1::digest(stage_one);
    let mut hasher = Sha1::new();
    hasher.update(salt);
    hasher.update(stage_two);
    let challenge = hasher.finalize();
    let mut response = [0; 20];
    for ((destination, stage_one), challenge) in response
        .iter_mut()
        .zip(stage_one.iter())
        .zip(challenge.iter())
    {
        *destination = stage_one ^ challenge;
    }
    response
}

fn encode_hex_upper(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(HEX[usize::from(byte >> 4)] as char);
        encoded.push(HEX[usize::from(byte & 0x0f)] as char);
    }
    encoded
}
