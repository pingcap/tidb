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

//! Lockdown owner for `pkg/util/vitess/vitess_hash.go`.
//!
//! `vitess.inventory.tsv` classifies every declaration, branch, and rule in
//! that Go file. The source fingerprint, inventory fingerprint, and Rust symbol
//! gate below make unreviewed source or inventory drift fail.
//!
//! The value path is Vitess' shard-key hash: a single-block DES encryption of
//! the big-endian shard key under an all-zero ("null") 64-bit key. The inventory
//! explicitly declines Go's eager package initialization, its FIPS-only DES
//! rejection, and the post-initialization error result omitted by this module's
//! existing API.

use des::cipher::{Block, BlockCipherEncrypt, KeyInit};
use des::Des;
use std::sync::LazyLock;

const NULL_KEY: [u8; 8] = [0; 8];

static NULL_KEY_BLOCK: LazyLock<Des> = LazyLock::new(|| {
    Des::new_from_slice(&NULL_KEY).expect("DES accepts the fixed-width all-zero Vitess key")
});

/// Implements Vitess' method of calculating a hash used for determining a shard
/// key range: a DES encryption with a 64-bit null key over a 64-bit block.
#[must_use]
pub fn hash_uint64(shard_key: u64) -> u64 {
    let mut block = Block::<Des>::default();
    block.copy_from_slice(&shard_key.to_be_bytes());
    NULL_KEY_BLOCK.encrypt_block(&mut block);
    u64::from_be_bytes(block.into())
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, BTreeSet},
        fmt::Write as _,
    };

    use sha2::{Digest, Sha256};

    use super::*;

    const GO_SOURCE: &[u8] = include_bytes!("../../../../pkg/util/vitess/vitess_hash.go");
    const LOCKDOWN_INVENTORY: &str = include_str!("vitess.inventory.tsv");
    const EXPECTED_INVENTORY_SHA256: &str =
        "4c3ad9185cb587695f1c88d2c798058ef3d4a25c206334deda3601d961064fb0";
    const EXPECTED_ITEMS: [(&str, (&str, &str)); 9] = [
        ("D01", ("PORTED", "NULL_KEY_BLOCK")),
        ("F01", ("DECLINED", "-")),
        ("B01", ("DECLINED", "-")),
        ("R01", ("PORTED", "NULL_KEY")),
        ("F02", ("PORTED", "hash_uint64")),
        ("R02", ("PORTED", "hash_uint64")),
        ("R03", ("PORTED", "hash_uint64")),
        ("R04", ("PORTED", "hash_uint64")),
        ("R05", ("DECLINED", "-")),
    ];

    #[test]
    fn lockdown_inventory_matches_go_source_and_rust_symbols() {
        let recorded_hash = LOCKDOWN_INVENTORY
            .lines()
            .find_map(|line| line.strip_prefix("# source-sha256\t"))
            .expect("inventory records the owning Go source SHA-256");
        assert_eq!(recorded_hash, sha256_hex(GO_SOURCE), "Go source drifted");
        assert_eq!(
            sha256_hex(LOCKDOWN_INVENTORY.as_bytes()),
            EXPECTED_INVENTORY_SHA256,
            "lockdown inventory drifted"
        );

        let mut lines = LOCKDOWN_INVENTORY
            .lines()
            .filter(|line| !line.is_empty() && !line.starts_with('#'));
        assert_eq!(
            lines.next(),
            Some("id\tcategory\tgo_item\tstatus\trust_symbol\tevidence")
        );

        let allowed_statuses = BTreeSet::from(["PORTED", "DECLINED", "UNREACHABLE"]);
        let mut actual = BTreeMap::new();
        for line in lines {
            let columns: Vec<_> = line.split('\t').collect();
            assert_eq!(columns.len(), 6, "invalid inventory row: {line}");
            assert!(
                allowed_statuses.contains(columns[3]),
                "unclassified inventory row: {line}"
            );
            assert!(
                !columns[5].is_empty(),
                "inventory evidence is required: {line}"
            );
            assert!(
                actual
                    .insert(columns[0], (columns[3], columns[4]))
                    .is_none(),
                "duplicate inventory id: {}",
                columns[0]
            );
        }
        assert_eq!(actual, BTreeMap::from(EXPECTED_ITEMS));

        let _: &LazyLock<Des> = &NULL_KEY_BLOCK;
        let _: [u8; 8] = NULL_KEY;
        let _: fn(u64) -> u64 = hash_uint64;
    }

    fn to_hex(value: u64) -> String {
        format!("{value:016X}")
    }

    // Go `TestVitessHash`.
    #[test]
    fn vitess_hash() {
        assert_eq!(to_hex(hash_uint64(30375298039)), "031265661E5F1133");
        assert_eq!(to_hex(hash_uint64(1123)), "031B565D41BDF8CA");
        assert_eq!(to_hex(hash_uint64(30573721600)), "1EFD6439F2050FFD");
        assert_eq!(to_hex(hash_uint64(116)), "1E1788FF0FDE093C");
        assert_eq!(to_hex(hash_uint64(u64::MAX)), "355550B2150E2451");
    }

    #[test]
    fn source_boundary_vectors_are_exact() {
        assert_eq!(NULL_KEY, [0; 8]);
        assert_eq!(to_hex(hash_uint64(0)), "8CA64DE9C1B123A7");
        assert_eq!(to_hex(hash_uint64(1)), "166B40B44ABA4BD6");
        assert_eq!(to_hex(hash_uint64(0x100)), "DD7C0BBD61FAFD54");
        assert_eq!(
            to_hex(hash_uint64(0x0102_0304_0506_0708)),
            "CEAD373DB80EABF8"
        );
    }

    fn sha256_hex(input: &[u8]) -> String {
        Sha256::digest(input)
            .iter()
            .fold(String::with_capacity(64), |mut output, byte| {
                write!(output, "{byte:02x}").expect("write to String");
                output
            })
    }
}
