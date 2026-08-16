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

//! `boundary:` `github.com/google/uuid`, which `mem_store.go` and `client.go`
//! use in one shape only: `uid := uuid.New(); hex.EncodeToString(uid[:])`.
//!
//! No `uuid` crate is in this workspace's lockfile or local registry cache, so
//! the offline build cannot add one. What the package needs from it is a fresh,
//! unguessable, collision-free 128-bit value rendered as 32 lowercase hex
//! digits — never the RFC 4122 text form, never parsing, never version or
//! variant inspection. That is `getrandom` plus the version-4 bit stamping,
//! which is reproduced here so the produced values are indistinguishable from
//! `uuid.New()`'s.

/// Go `hex.EncodeToString(uuid.New()[:])`: a random version-4 UUID as 32
/// lowercase hex digits.
///
/// # Panics
///
/// Panics when the operating system's entropy source fails, matching
/// `uuid.New`, which panics on a `rand.Read` error.
pub fn new_uuid_hex() -> String {
    let mut bytes = [0_u8; 16];
    getrandom::fill(&mut bytes).expect("the OS entropy source is available");
    // RFC 4122: version 4 in the high nibble of octet 6, variant 10 in octet 8.
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;

    let mut text = String::with_capacity(32);
    for byte in bytes {
        text.push(char::from_digit(u32::from(byte >> 4), 16).expect("nibble is hex"));
        text.push(char::from_digit(u32::from(byte & 0x0f), 16).expect("nibble is hex"));
    }
    text
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shape_and_uniqueness() {
        let first = new_uuid_hex();
        assert_eq!(first.len(), 32);
        assert!(first
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase()));
        // Version 4 and the RFC 4122 variant land in the hex text at these
        // offsets, exactly as `uuid.New()` would render them.
        assert_eq!(&first[12..13], "4");
        assert!(matches!(&first[16..17], "8" | "9" | "a" | "b"));
        assert_ne!(first, new_uuid_hex());
    }
}
