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

//! Lockdown owner for `pkg/util/fastrand/runtime.go`.
//!
//! `runtime.inventory.tsv` classifies the source declaration and the behavior
//! reached through its `runtime.cheaprand` link. Rust preserves the lock-free
//! Wyrand path used by Go on the supported 64-bit targets, but has no Go
//! `go:linkname` or per-M state and therefore uses per-thread state instead.

use std::cell::Cell;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use super::random::Wyrand;

const FALLBACK_INCREMENT: u64 = 0xa076_1d64_78bd_642f;
static FALLBACK_SEED: AtomicU64 = AtomicU64::new(0xe703_7ed1_a0b4_28db);

thread_local! {
    static RANDOM: Cell<Wyrand> = Cell::new(Wyrand::new(initial_seed()));
}

fn initial_seed() -> u64 {
    let mut bytes = [0; 8];
    if getrandom::fill(&mut bytes).is_ok() {
        return u64::from_ne_bytes(bytes);
    }

    // Go's runtime.cheaprand cannot report initialization failure. Preserve
    // that infallible contract with a unique monotonic fallback mixed with the
    // current clock; this path is not a cryptographic promise.
    let sequence = FALLBACK_SEED.fetch_add(FALLBACK_INCREMENT, Ordering::Relaxed);
    let clock = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos() as u64);
    sequence ^ clock
}

/// Returns a lock-free pseudo-random `u32`.
#[must_use]
pub fn uint32() -> u32 {
    RANDOM.with(|state| {
        let mut random = state.get();
        let value = random.next() as u32;
        state.set(random);
        value
    })
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::collections::{BTreeMap, BTreeSet};
    use std::fmt::Write as _;

    use sha2::{Digest, Sha256};

    use super::{uint32, Wyrand, RANDOM};

    const GO_SOURCE: &[u8] = include_bytes!("../../../../../pkg/util/fastrand/runtime.go");
    const RUST_SOURCE: &str = include_str!("runtime.rs");
    const LOCKDOWN_INVENTORY: &str = include_str!("runtime.inventory.tsv");
    const EXPECTED_INVENTORY_SHA256: &str =
        "214192d13a3da7def207f4b493521dff3f265f498d5a898a1090e9dfef61eef8";
    const EXPECTED_RUST_PRODUCTION_SHA256: &str =
        "56729015f4def133f09c1f286bfc85e0a771782459480be5fac10aab117caa66";
    const EXPECTED_ITEMS: [(&str, (&str, &str)); 9] = [
        ("D01", ("DECLINED", "-")),
        ("F01", ("PORTED", "uint32")),
        ("R01", ("PORTED", "uint32")),
        ("R02", ("PORTED", "RANDOM")),
        ("R03", ("DECLINED", "-")),
        ("B01", ("PORTED", "uint32")),
        ("B02", ("DECLINED", "-")),
        ("R04", ("DECLINED", "-")),
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

        let _: fn() -> u32 = uint32;
        RANDOM.with(|_: &Cell<Wyrand>| {});
    }

    #[test]
    fn source_lock_free_production_path_is_stable() {
        let production = RUST_SOURCE
            .split_once("#[cfg(test)]")
            .expect("runtime module keeps tests after production code")
            .0;
        assert_eq!(
            sha256_hex(production.as_bytes()),
            EXPECTED_RUST_PRODUCTION_SHA256,
            "Rust production path drifted"
        );
        assert!(production.contains("thread_local!"));
        assert!(production.contains("Cell<Wyrand>"));
        for locking_primitive in ["Mutex", "RwLock", "OnceLock", ".lock("] {
            assert!(
                !production.contains(locking_primitive),
                "lock-free path contains {locking_primitive}"
            );
        }
    }

    #[test]
    fn source_arm64_wyrand_and_thread_local_state_are_exact() {
        const SOURCE_VALUES_FROM_SEED_ZERO: [u32; 2] = [0x8f59_a58e, 0xff4e_856d];

        RANDOM.with(|state| state.set(Wyrand::new(0)));

        let worker_values = std::thread::spawn(|| {
            RANDOM.with(|state| state.set(Wyrand::new(0)));
            [uint32(), uint32()]
        })
        .join()
        .expect("random worker");

        assert_eq!(worker_values, SOURCE_VALUES_FROM_SEED_ZERO);
        assert_eq!([uint32(), uint32()], SOURCE_VALUES_FROM_SEED_ZERO);
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
