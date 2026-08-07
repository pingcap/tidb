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

//! Lockdown owner for the complete Go `pkg/util/slice` package.
//!
//! `slice.artifacts.tsv` fingerprints every direct package artifact and
//! `slice.inventory.tsv` classifies every generated Go AST obligation. The
//! original `TestSlice` is retained by name. Go's `TestMain` installs common
//! Go test state and third-party goleak exclusions; this Rust module owns no
//! matching global state or background workers.

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

    const GO_BUILD: &[u8] = include_bytes!("../../../../pkg/util/slice/BUILD.bazel");
    const GO_MAIN_TEST: &[u8] = include_bytes!("../../../../pkg/util/slice/main_test.go");
    const GO_SOURCE: &[u8] = include_bytes!("../../../../pkg/util/slice/slice.go");
    const GO_TEST: &[u8] = include_bytes!("../../../../pkg/util/slice/slice_test.go");
    const ARTIFACT_MANIFEST: &str = include_str!("slice.artifacts.tsv");
    const LOCKDOWN_INVENTORY: &str = include_str!("slice.inventory.tsv");
    const DECLINED_EVIDENCE: &str = "source-quote:go_testsetup_and_goleak_only";
    const SYMBOL_EVIDENCE: &str =
        "rust-test:slice_lockdown_inventory_is_complete_and_symbols_compile";
    const ARTIFACTS: [(&str, &str, &[u8]); 4] = [
        ("pkg/util/slice/BUILD.bazel", "build", GO_BUILD),
        ("pkg/util/slice/main_test.go", "test-support", GO_MAIN_TEST),
        ("pkg/util/slice/slice.go", "production", GO_SOURCE),
        ("pkg/util/slice/slice_test.go", "test", GO_TEST),
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
    fn slice_lockdown_inventory_is_complete_and_symbols_compile() {
        let expected_manifest_prefix = [
            "# pkg-slice-artifacts-v1",
            "# zero\tbuild_tags\t0",
            "# zero\tplatform_variants\t0",
            "# zero\tcode_generated\t0",
            "# zero\tgo_generate\t0",
            "# zero\tgo_embed\t0",
            "# zero\ttracked_testdata\t0",
            "path\trole\tsha256",
        ];
        let mut manifest_lines = ARTIFACT_MANIFEST.lines();
        for expected in expected_manifest_prefix {
            assert_eq!(manifest_lines.next(), Some(expected));
        }
        let mut manifest = BTreeMap::new();
        for line in manifest_lines {
            let columns: Vec<_> = line.split('\t').collect();
            assert_eq!(columns.len(), 3, "invalid artifact row: {line}");
            assert!(manifest
                .insert(columns[0], (columns[1], columns[2]))
                .is_none());
        }
        assert_eq!(manifest.len(), ARTIFACTS.len());
        for (path, role, bytes) in ARTIFACTS {
            assert!(manifest
                .get(path)
                .is_some_and(|(actual_role, actual_hash)| {
                    *actual_role == role && *actual_hash == sha256_hex(bytes)
                }));
        }

        let mut lines = LOCKDOWN_INVENTORY
            .lines()
            .filter(|line| !line.is_empty() && !line.starts_with('#'));
        assert_eq!(
            lines.next(),
            Some(
                "obligation_id\tcategory\tsource_path\tast_anchor\tnode_sha256\towner\tstatus\trust_symbol\tevidence\tmutation_policy"
            )
        );

        let allowed_statuses = BTreeSet::from(["PORTED", "DECLINED", "UNREACHABLE"]);
        let mut ids = BTreeSet::new();
        let mut anchors = BTreeSet::new();
        let mut categories = BTreeMap::new();
        let mut statuses = BTreeMap::new();
        let mut declined = BTreeSet::new();
        for line in lines {
            let columns: Vec<_> = line.split('\t').collect();
            assert_eq!(columns.len(), 10, "invalid inventory row: {line}");
            assert!(
                allowed_statuses.contains(columns[6]),
                "unclassified inventory row: {line}"
            );
            assert!(
                !columns[8].is_empty(),
                "inventory evidence is required: {line}"
            );
            assert!(ids.insert(columns[0]), "duplicate id: {line}");
            assert!(
                anchors.insert((columns[2], columns[3])),
                "duplicate anchor: {line}"
            );
            *categories.entry(columns[1]).or_insert(0usize) += 1;
            *statuses.entry(columns[6]).or_insert(0usize) += 1;

            match (columns[2], columns[1], columns[5], columns[3]) {
                ("pkg/util/slice/slice.go", "function", owner, anchor)
                    if owner == anchor
                        && matches!(owner, "AllOf" | "DeepClone" | "Int64sToStrings") =>
                {
                    let symbol = match owner {
                        "AllOf" => "all_of",
                        "DeepClone" => "deep_clone",
                        _ => "int64s_to_strings",
                    };
                    assert_eq!(
                        columns[6..10],
                        ["PORTED", symbol, SYMBOL_EVIDENCE, "compile-owner-gate"]
                    );
                }
                ("pkg/util/slice/slice.go", "branch" | "closure" | "loop", owner, anchor)
                    if anchor.starts_with(owner)
                        && matches!(owner, "AllOf" | "DeepClone" | "Int64sToStrings") =>
                {
                    let (symbol, evidence) = match owner {
                        "AllOf" => (
                            "all_of",
                            "rust-test:all_of_preserves_source_order_short_circuit_and_empty_truth",
                        ),
                        "Int64sToStrings" => (
                            "int64s_to_strings",
                            "rust-test:int64s_to_strings_preserves_source_decimal_domain",
                        ),
                        "DeepClone" if columns[1] == "loop" && anchor.ends_with("/enters") => (
                            "deep_clone",
                            "rust-test:deep_clone_invokes_clone_once_per_item_in_source_order",
                        ),
                        _ => (
                            "deep_clone",
                            "rust-test:deep_clone_preserves_nil_empty_and_element_clone_ownership",
                        ),
                    };
                    assert_eq!(
                        columns[6..10],
                        ["PORTED", symbol, evidence, "behavior-mutation"]
                    );
                }
                (
                    "pkg/util/slice/slice_test.go",
                    "test" | "test_assertion" | "test_helper_closure" | "test_loop" | "test_row",
                    "TestSlice",
                    anchor,
                ) if anchor.starts_with("TestSlice") => {
                    assert_eq!(
                        columns[6..10],
                        [
                            "PORTED",
                            "TestSlice",
                            "rust-test:TestSlice",
                            "test-evidence-gate"
                        ]
                    );
                }
                ("pkg/util/slice/main_test.go", "test_main" | "test_row", "TestMain", anchor) => {
                    assert!(matches!(
                        anchor,
                        "TestMain"
                            | "TestMain/composite:1/element:0"
                            | "TestMain/composite:1/element:1"
                            | "TestMain/composite:1/element:2"
                            | "TestMain/composite:1/element:3"
                    ));
                    assert_eq!(
                        columns[6..10],
                        [
                            "DECLINED",
                            "-",
                            DECLINED_EVIDENCE,
                            "classification-evidence-gate"
                        ]
                    );
                    declined.insert(anchor);
                }
                _ => panic!("unexpected slice inventory row: {line}"),
            }
        }
        assert_eq!(ids.len(), 41);
        assert_eq!(
            categories,
            BTreeMap::from([
                ("branch", 2),
                ("closure", 1),
                ("function", 3),
                ("loop", 4),
                ("test", 1),
                ("test_assertion", 1),
                ("test_helper_closure", 2),
                ("test_loop", 2),
                ("test_main", 1),
                ("test_row", 24),
            ])
        );
        assert_eq!(statuses, BTreeMap::from([("DECLINED", 5), ("PORTED", 36)]));
        assert_eq!(
            declined,
            BTreeSet::from([
                "TestMain",
                "TestMain/composite:1/element:0",
                "TestMain/composite:1/element:1",
                "TestMain/composite:1/element:2",
                "TestMain/composite:1/element:3",
            ])
        );

        let go_main = std::str::from_utf8(GO_MAIN_TEST).expect("Go support is UTF-8");
        assert!(go_main.contains("testsetup.SetupForCommonTest()"));
        assert!(go_main.contains("goleak.VerifyTestMain"));
        assert_eq!(go_main.matches("goleak.IgnoreTopFunction").count(), 4);
        assert!(std::str::from_utf8(GO_TEST)
            .expect("Go test is UTF-8")
            .contains("func TestSlice"));

        fn even(value: &i32) -> bool {
            value % 2 == 0
        }
        assert!(all_of(&[2, 4], even));
        let _: fn(&[i64]) -> Vec<String> = int64s_to_strings;
        let _: for<'a> fn(Option<&'a [String]>) -> Option<Vec<String>> = deep_clone::<String>;
        let _: fn() = TestSlice;
        let _: fn() = all_of_preserves_source_order_short_circuit_and_empty_truth;
        let _: fn() = int64s_to_strings_preserves_source_decimal_domain;
        let _: fn() = deep_clone_preserves_nil_empty_and_element_clone_ownership;
        let _: fn() = deep_clone_invokes_clone_once_per_item_in_source_order;
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
