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

//! Lockdown owner for `pkg/util/slice/slice.go`.
//!
//! `slice.inventory.tsv` classifies every function, branch, and state rule in
//! that Go file. The source fingerprint and Rust symbol gate below make an
//! unreviewed source or inventory drift fail. The original `TestSlice` is
//! retained by name. Go's `TestMain` installs common Go test state and
//! third-party goleak exclusions; this Rust module owns no global state or
//! background workers, so it needs neither hook nor exclusion.

/// Returns true when every item matches `predicate`.
///
/// Like the source's negated `slices.ContainsFunc`, this is vacuously true for
/// an empty slice and stops at the first mismatch.
pub fn all_of<T>(slice: &[T], predicate: impl FnMut(&T) -> bool) -> bool {
    slice.iter().all(predicate)
}

/// Converts signed 64-bit integers to their base-ten Go string form.
#[must_use]
pub fn int64s_to_strings(ints: &[i64]) -> Vec<String> {
    ints.iter().map(i64::to_string).collect()
}

/// Uses [`Clone::clone`] to clone every element while preserving source nil.
///
/// `None` represents a nil Go slice. `Some(&[])` represents a present empty
/// slice and therefore returns `Some(Vec::new())`, keeping those source states
/// distinct.
#[must_use]
pub fn deep_clone<T: Clone>(slice: Option<&[T]>) -> Option<Vec<T>> {
    slice.map(|items| {
        let mut cloned = Vec::with_capacity(items.len());
        cloned.extend(items.iter().cloned());
        cloned
    })
}

#[cfg(test)]
#[allow(non_snake_case)]
mod tests {
    use std::{
        cell::{Cell, RefCell},
        collections::{BTreeMap, BTreeSet},
        fmt::Write as _,
        rc::Rc,
    };

    use sha2::{Digest, Sha256};

    use super::{all_of, deep_clone, int64s_to_strings};

    const GO_SOURCE: &[u8] = include_bytes!("../../../../pkg/util/slice/slice.go");
    const LOCKDOWN_INVENTORY: &str = include_str!("slice.inventory.tsv");
    const EXPECTED_ITEMS: [(&str, (&str, &str)); 19] = [
        ("F01", ("PORTED", "all_of")),
        ("R01", ("PORTED", "all_of")),
        ("R02", ("PORTED", "all_of")),
        ("R03", ("PORTED", "all_of")),
        ("B01", ("PORTED", "all_of")),
        ("B02", ("PORTED", "all_of")),
        ("B03", ("PORTED", "all_of")),
        ("F02", ("PORTED", "int64s_to_strings")),
        ("R04", ("PORTED", "int64s_to_strings")),
        ("B04", ("PORTED", "int64s_to_strings")),
        ("B05", ("PORTED", "int64s_to_strings")),
        ("R05", ("PORTED", "int64s_to_strings")),
        ("F03", ("PORTED", "deep_clone")),
        ("B06", ("PORTED", "deep_clone")),
        ("B07", ("PORTED", "deep_clone")),
        ("R06", ("PORTED", "deep_clone")),
        ("B08", ("PORTED", "deep_clone")),
        ("B09", ("PORTED", "deep_clone")),
        ("R07", ("PORTED", "deep_clone")),
    ];

    #[test]
    fn TestSlice() {
        let tests = [
            (&[][..], true),
            (&[1, 2, 3][..], false),
            (&[1, 3][..], false),
            (&[2, 2, 4][..], true),
        ];

        for (values, expected) in tests {
            assert_eq!(all_of(values, |value| value % 2 == 0), expected);
        }
    }

    #[test]
    fn all_of_preserves_source_order_short_circuit_and_empty_truth() {
        let calls = Cell::new(0);
        assert!(all_of::<i32>(&[], |_| {
            calls.set(calls.get() + 1);
            false
        }));
        assert_eq!(calls.get(), 0);

        let visited = Cell::new(Vec::new());
        assert!(!all_of(&[2, 4, 5, 6], |value| {
            let mut values = visited.take();
            values.push(*value);
            visited.set(values);
            value % 2 == 0
        }));
        assert_eq!(visited.take(), vec![2, 4, 5]);
    }

    #[test]
    fn int64s_to_strings_preserves_source_decimal_domain() {
        assert!(int64s_to_strings(&[]).is_empty());
        let input = [i64::MIN, -1, 0, 1, i64::MAX];
        let output = int64s_to_strings(&input);
        assert_eq!(output.capacity(), input.len());
        assert_eq!(
            output,
            [
                "-9223372036854775808",
                "-1",
                "0",
                "1",
                "9223372036854775807",
            ]
        );
    }

    #[test]
    fn deep_clone_preserves_nil_empty_and_element_clone_ownership() {
        assert_eq!(deep_clone::<String>(None), None);
        assert_eq!(deep_clone(Some(&[] as &[String])), Some(Vec::new()));

        let source = vec![String::from("left"), String::from("right")];
        let mut cloned = deep_clone(Some(&source)).expect("present source");
        cloned[0].push_str("-changed");
        assert_eq!(source, ["left", "right"]);
        assert_eq!(cloned, ["left-changed", "right"]);
        assert_eq!(cloned.capacity(), source.len());
    }

    #[test]
    fn deep_clone_invokes_clone_once_per_item_in_source_order() {
        struct CloneProbe {
            id: i32,
            calls: Rc<RefCell<Vec<i32>>>,
        }

        impl Clone for CloneProbe {
            fn clone(&self) -> Self {
                self.calls.borrow_mut().push(self.id);
                Self {
                    id: self.id,
                    calls: Rc::clone(&self.calls),
                }
            }
        }

        let calls = Rc::new(RefCell::new(Vec::new()));
        let source = [1, 2, 3].map(|id| CloneProbe {
            id,
            calls: Rc::clone(&calls),
        });
        let cloned = deep_clone(Some(&source)).expect("present source");

        assert_eq!(&*calls.borrow(), &[1, 2, 3]);
        assert_eq!(
            cloned.iter().map(|probe| probe.id).collect::<Vec<_>>(),
            [1, 2, 3]
        );
    }

    #[test]
    fn lockdown_inventory_matches_go_source_and_rust_symbols() {
        let recorded_hash = LOCKDOWN_INVENTORY
            .lines()
            .find_map(|line| line.strip_prefix("# source-sha256\t"))
            .expect("inventory records the owning Go source SHA-256");
        assert_eq!(recorded_hash, sha256_hex(GO_SOURCE), "Go source drifted");

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

        fn even(value: &i32) -> bool {
            value % 2 == 0
        }
        assert!(all_of(&[2, 4], even));
        let _: fn(&[i64]) -> Vec<String> = int64s_to_strings;
        let _: for<'a> fn(Option<&'a [String]>) -> Option<Vec<String>> = deep_clone::<String>;
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
