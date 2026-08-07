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

//! Lockdown owner for `pkg/util/paging/paging.go`.
//!
//! `paging.inventory.tsv` classifies every declaration, function, branch, and
//! arithmetic rule in that Go file. The source fingerprint and Rust symbol
//! gate below make an unreviewed source or inventory drift fail.

/// The minimum paging size.
pub const MIN_PAGING_SIZE: u64 = 128;
const MAX_PAGING_SIZE_SHIFT: u32 = 7;
const PAGING_SIZE_GROW: u64 = 2;
/// The minimum allowed value for the maximum paging size.
pub const MIN_ALLOWED_MAX_PAGING_SIZE: u64 = 50000;
const PAGING_GROWING_SUM: u64 = ((2u64 << MAX_PAGING_SIZE_SHIFT) - 1) * MIN_PAGING_SIZE;
/// The paging-size threshold.
pub const THRESHOLD: u64 = 960;

/// Grows the paging size and ensures it does not exceed
/// `max(maxv, MIN_ALLOWED_MAX_PAGING_SIZE)`.
#[must_use]
pub fn grow_paging_size(size: u64, mut maxv: u64) -> u64 {
    if maxv < MIN_ALLOWED_MAX_PAGING_SIZE {
        // Defensive programming, for example, called with maxv = 0. `maxv`
        // should never be less than `MIN_ALLOWED_MAX_PAGING_SIZE`; otherwise the
        // session variable may be wrong, or the distsql request does not obey
        // the session variable setting.
        maxv = MIN_ALLOWED_MAX_PAGING_SIZE;
    }

    let size = size.wrapping_shl(1);
    if size > maxv {
        return maxv;
    }
    size
}

/// Calculates the seek count from the expected count.
#[must_use]
pub fn calculate_seek_cnt(expect_cnt: u64) -> f64 {
    if expect_cnt == 0 {
        return 0.0;
    }
    if expect_cnt > PAGING_GROWING_SUM {
        // If `expect_cnt` is larger than `PAGING_GROWING_SUM`, calculate the
        // seek count for the excess.
        let rounded_excess = expect_cnt
            .wrapping_sub(PAGING_GROWING_SUM)
            .wrapping_add(MIN_ALLOWED_MAX_PAGING_SIZE - 1)
            / MIN_ALLOWED_MAX_PAGING_SIZE;
        return 8u64.wrapping_add(rounded_excess) as f64;
    }
    if expect_cnt > MIN_PAGING_SIZE {
        // If `expect_cnt` is less than `PAGING_GROWING_SUM`, calculate the seek
        // count (number of terms) from the sum of a geometric progression.
        //   expect_cnt = MIN_PAGING_SIZE * (PAGING_SIZE_GROW^seek_cnt - 1)
        //                                  / (PAGING_SIZE_GROW - 1)
        // Simplifying `PAGING_SIZE_GROW^seek_cnt - 1` to `PAGING_SIZE_GROW^seek_cnt`:
        //   seek_cnt = log((PAGING_SIZE_GROW - 1) * expect_cnt / MIN_PAGING_SIZE)
        //              / log(PAGING_SIZE_GROW)
        let ratio = (((PAGING_SIZE_GROW - 1) * expect_cnt) as f64 / MIN_PAGING_SIZE as f64).ln()
            / (PAGING_SIZE_GROW as f64).ln();
        return 1.0 + (ratio as i64) as f64;
    }
    1.0
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, BTreeSet},
        fmt::Write as _,
    };

    use sha2::{Digest, Sha256};

    use super::{
        calculate_seek_cnt, grow_paging_size, MAX_PAGING_SIZE_SHIFT, MIN_ALLOWED_MAX_PAGING_SIZE,
        MIN_PAGING_SIZE, PAGING_GROWING_SUM, PAGING_SIZE_GROW, THRESHOLD,
    };

    const GO_BUILD: &[u8] = include_bytes!("../../../../pkg/util/paging/BUILD.bazel");
    const GO_MAIN_TEST: &[u8] = include_bytes!("../../../../pkg/util/paging/main_test.go");
    const GO_SOURCE: &[u8] = include_bytes!("../../../../pkg/util/paging/paging.go");
    const GO_TEST: &[u8] = include_bytes!("../../../../pkg/util/paging/paging_test.go");
    const ARTIFACT_MANIFEST: &str = include_str!("paging.artifacts.tsv");
    const LOCKDOWN_INVENTORY: &str = include_str!("paging.inventory.tsv");
    const DECLINED_EVIDENCE: &str = "source-quote:go_testsetup_and_goleak_only";
    const SYMBOL_EVIDENCE: &str =
        "rust-test:paging_lockdown_inventory_is_complete_and_symbols_compile";

    const ARTIFACTS: [(&str, &str, &[u8]); 4] = [
        ("pkg/util/paging/BUILD.bazel", "build", GO_BUILD),
        ("pkg/util/paging/main_test.go", "test-support", GO_MAIN_TEST),
        ("pkg/util/paging/paging.go", "production", GO_SOURCE),
        ("pkg/util/paging/paging_test.go", "test", GO_TEST),
    ];

    fn assert_in_delta(actual: f64, expected: f64, delta: f64) {
        assert!(
            (actual - expected).abs() < delta,
            "expected {expected} +/- {delta}, got {actual}"
        );
    }

    #[test]
    fn paging_lockdown_inventory_is_complete_and_symbols_compile() {
        let expected_manifest_prefix = [
            "# pkg-paging-artifacts-v1",
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
            assert!(
                manifest
                    .insert(columns[0], (columns[1], columns[2]))
                    .is_none(),
                "duplicate artifact row: {line}"
            );
        }
        assert_eq!(manifest.len(), ARTIFACTS.len());
        for (path, role, bytes) in ARTIFACTS {
            let expected_hash = sha256_hex(bytes);
            assert!(
                manifest
                    .get(path)
                    .is_some_and(|(actual_role, actual_hash)| {
                        *actual_role == role && *actual_hash == expected_hash
                    }),
                "artifact manifest drifted: {path}"
            );
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
        let mut source_anchors = BTreeSet::new();
        let mut categories = BTreeMap::new();
        let mut statuses = BTreeMap::new();
        let mut declined_support = BTreeSet::new();
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
            assert!(
                ids.insert(columns[0]),
                "duplicate inventory id: {}",
                columns[0]
            );
            assert!(
                source_anchors.insert((columns[2], columns[3])),
                "duplicate source anchor: {line}"
            );
            *categories.entry(columns[1]).or_insert(0usize) += 1;
            *statuses.entry(columns[6]).or_insert(0usize) += 1;

            match (columns[2], columns[1], columns[5], columns[3]) {
                ("pkg/util/paging/paging.go", "const", owner, anchor) => {
                    let symbol = match (owner, anchor) {
                        ("const:MinPagingSize:0", "const:MinPagingSize:0") => "MIN_PAGING_SIZE",
                        ("const:maxPagingSizeShift:0", "const:maxPagingSizeShift:0") => {
                            "MAX_PAGING_SIZE_SHIFT"
                        }
                        ("const:pagingSizeGrow:0", "const:pagingSizeGrow:0") => "PAGING_SIZE_GROW",
                        ("const:MinAllowedMaxPagingSize:0", "const:MinAllowedMaxPagingSize:0") => {
                            "MIN_ALLOWED_MAX_PAGING_SIZE"
                        }
                        ("const:pagingGrowingSum:0", "const:pagingGrowingSum:0") => {
                            "PAGING_GROWING_SUM"
                        }
                        ("const:Threshold:0", "const:Threshold:0") => "THRESHOLD",
                        _ => panic!("unexpected source constant row: {line}"),
                    };
                    assert_eq!(
                        columns[6..10],
                        ["PORTED", symbol, SYMBOL_EVIDENCE, "compile-owner-gate"]
                    );
                }
                ("pkg/util/paging/paging.go", "function", "GrowPagingSize", "GrowPagingSize") => {
                    assert_eq!(
                        columns[6..10],
                        [
                            "PORTED",
                            "grow_paging_size",
                            SYMBOL_EVIDENCE,
                            "compile-owner-gate"
                        ]
                    );
                }
                (
                    "pkg/util/paging/paging.go",
                    "function",
                    "CalculateSeekCnt",
                    "CalculateSeekCnt",
                ) => {
                    assert_eq!(
                        columns[6..10],
                        [
                            "PORTED",
                            "calculate_seek_cnt",
                            SYMBOL_EVIDENCE,
                            "compile-owner-gate"
                        ]
                    );
                }
                ("pkg/util/paging/paging.go", "branch", "GrowPagingSize", anchor)
                    if anchor.starts_with("GrowPagingSize/if:") =>
                {
                    assert_eq!(
                        columns[6..10],
                        [
                            "PORTED",
                            "grow_paging_size",
                            "rust-test:grow_paging_size_preserves_source_wrapping_and_cap_order",
                            "behavior-mutation"
                        ]
                    );
                }
                ("pkg/util/paging/paging.go", "branch", "CalculateSeekCnt", anchor)
                    if anchor.starts_with("CalculateSeekCnt/if:") =>
                {
                    assert_eq!(
                        columns[6..10],
                        [
                            "PORTED",
                            "calculate_seek_cnt",
                            "rust-test:calculate_seek_cnt_preserves_source_piecewise_boundaries",
                            "behavior-mutation"
                        ]
                    );
                }
                (
                    "pkg/util/paging/paging_test.go",
                    "test",
                    "TestGrowPagingSize",
                    "TestGrowPagingSize",
                ) => {
                    assert_eq!(
                        columns[6..10],
                        [
                            "PORTED",
                            "grow_paging_size_test",
                            "rust-test:grow_paging_size_test",
                            "test-evidence-gate"
                        ]
                    );
                }
                (
                    "pkg/util/paging/paging_test.go",
                    "test",
                    "TestCalculateSeekCnt",
                    "TestCalculateSeekCnt",
                ) => {
                    assert_eq!(
                        columns[6..10],
                        [
                            "PORTED",
                            "calculate_seek_cnt_test",
                            "rust-test:calculate_seek_cnt_test",
                            "test-evidence-gate"
                        ]
                    );
                }
                (
                    "pkg/util/paging/paging_test.go",
                    "test_assertion",
                    "TestGrowPagingSize",
                    anchor,
                ) if anchor.starts_with("TestGrowPagingSize/assertion:") => {
                    assert_eq!(
                        columns[6..10],
                        [
                            "PORTED",
                            "grow_paging_size_test",
                            "rust-test:grow_paging_size_test",
                            "test-evidence-gate"
                        ]
                    );
                }
                ("pkg/util/paging/main_test.go", "test_main" | "test_row", "TestMain", anchor) => {
                    assert!(
                        matches!(
                            anchor,
                            "TestMain"
                                | "TestMain/composite:1/element:0"
                                | "TestMain/composite:1/element:1"
                                | "TestMain/composite:1/element:2"
                                | "TestMain/composite:1/element:3"
                        ),
                        "unexpected declined support row: {line}"
                    );
                    assert_eq!(
                        columns[6..10],
                        [
                            "DECLINED",
                            "-",
                            DECLINED_EVIDENCE,
                            "classification-evidence-gate"
                        ]
                    );
                    declined_support.insert(anchor);
                }
                _ => panic!("unexpected paging inventory row: {line}"),
            }
        }
        assert_eq!(ids.len(), 28);
        assert_eq!(
            categories,
            BTreeMap::from([
                ("branch", 10),
                ("const", 6),
                ("function", 2),
                ("test", 2),
                ("test_assertion", 3),
                ("test_main", 1),
                ("test_row", 4),
            ])
        );
        assert_eq!(statuses, BTreeMap::from([("DECLINED", 5), ("PORTED", 23)]));
        assert_eq!(
            declined_support,
            BTreeSet::from([
                "TestMain",
                "TestMain/composite:1/element:0",
                "TestMain/composite:1/element:1",
                "TestMain/composite:1/element:2",
                "TestMain/composite:1/element:3",
            ])
        );

        let go_main_test = std::str::from_utf8(GO_MAIN_TEST).expect("Go test support is UTF-8");
        assert!(go_main_test.contains("testsetup.SetupForCommonTest()"));
        assert!(go_main_test.contains("goleak.VerifyTestMain"));
        assert_eq!(go_main_test.matches("goleak.IgnoreTopFunction").count(), 4);
        let go_test = std::str::from_utf8(GO_TEST).expect("Go test source is UTF-8");
        assert!(go_test.contains("func TestGrowPagingSize"));
        assert!(go_test.contains("func TestCalculateSeekCnt"));

        assert_eq!(MIN_PAGING_SIZE, 128);
        assert_eq!(MAX_PAGING_SIZE_SHIFT, 7);
        assert_eq!(PAGING_SIZE_GROW, 2);
        assert_eq!(MIN_ALLOWED_MAX_PAGING_SIZE, 50_000);
        assert_eq!(PAGING_GROWING_SUM, 32_640);
        assert_eq!(THRESHOLD, 960);
        let _: fn(u64, u64) -> u64 = grow_paging_size;
        let _: fn(u64) -> f64 = calculate_seek_cnt;
        let _: fn() = grow_paging_size_test;
        let _: fn() = calculate_seek_cnt_test;
        let _: fn() = grow_paging_size_preserves_source_wrapping_and_cap_order;
        let _: fn() = calculate_seek_cnt_preserves_source_piecewise_boundaries;
    }

    // Go `TestGrowPagingSize`.
    #[test]
    fn grow_paging_size_test() {
        assert_eq!(
            grow_paging_size(MIN_PAGING_SIZE, MIN_ALLOWED_MAX_PAGING_SIZE),
            MIN_PAGING_SIZE * PAGING_SIZE_GROW
        );
        assert_eq!(
            grow_paging_size(MIN_ALLOWED_MAX_PAGING_SIZE, MIN_ALLOWED_MAX_PAGING_SIZE),
            MIN_ALLOWED_MAX_PAGING_SIZE
        );
        assert_eq!(
            grow_paging_size(
                MIN_ALLOWED_MAX_PAGING_SIZE / PAGING_SIZE_GROW + 1,
                MIN_ALLOWED_MAX_PAGING_SIZE
            ),
            MIN_ALLOWED_MAX_PAGING_SIZE
        );
    }

    // Go `TestCalculateSeekCnt`.
    #[test]
    fn calculate_seek_cnt_test() {
        let shift = f64::from(MAX_PAGING_SIZE_SHIFT);
        assert_in_delta(calculate_seek_cnt(0), 0.0, 0.1);
        assert_in_delta(calculate_seek_cnt(1), 1.0, 0.1);
        assert_in_delta(calculate_seek_cnt(MIN_PAGING_SIZE), 1.0, 0.1);
        assert_in_delta(calculate_seek_cnt(PAGING_GROWING_SUM), shift + 1.0, 0.1);
        assert_in_delta(calculate_seek_cnt(PAGING_GROWING_SUM + 1), shift + 2.0, 0.1);
        assert_in_delta(
            calculate_seek_cnt(PAGING_GROWING_SUM + MIN_ALLOWED_MAX_PAGING_SIZE),
            shift + 2.0,
            0.1,
        );
    }

    #[test]
    fn grow_paging_size_preserves_source_wrapping_and_cap_order() {
        assert_eq!(grow_paging_size(0, 0), 0);
        assert_eq!(grow_paging_size(25_000, 49_999), 50_000);
        assert_eq!(grow_paging_size(25_001, u64::MAX), 50_002);
        assert_eq!(grow_paging_size(u64::MAX / 2, u64::MAX), u64::MAX - 1);
        assert_eq!(grow_paging_size(u64::MAX / 2 + 1, u64::MAX), 0);
        assert_eq!(grow_paging_size(u64::MAX, u64::MAX), u64::MAX - 1);
        assert_eq!(grow_paging_size(u64::MAX, 0), MIN_ALLOWED_MAX_PAGING_SIZE);
    }

    #[test]
    fn calculate_seek_cnt_preserves_source_piecewise_boundaries() {
        assert_eq!(
            calculate_seek_cnt(0).to_bits(),
            0.0_f64.to_bits(),
            "the source returns positive zero"
        );

        let cases = [
            (1, 1.0),
            (MIN_PAGING_SIZE - 1, 1.0),
            (MIN_PAGING_SIZE, 1.0),
            (MIN_PAGING_SIZE + 1, 1.0),
            (2 * MIN_PAGING_SIZE - 1, 1.0),
            (2 * MIN_PAGING_SIZE, 2.0),
            (2 * MIN_PAGING_SIZE + 1, 2.0),
            (PAGING_GROWING_SUM - 1, 8.0),
            (PAGING_GROWING_SUM, 8.0),
            (PAGING_GROWING_SUM + 1, 9.0),
            (PAGING_GROWING_SUM + MIN_ALLOWED_MAX_PAGING_SIZE - 1, 9.0),
            (PAGING_GROWING_SUM + MIN_ALLOWED_MAX_PAGING_SIZE, 9.0),
            (PAGING_GROWING_SUM + MIN_ALLOWED_MAX_PAGING_SIZE + 1, 10.0),
        ];

        for (expect_cnt, expected) in cases {
            assert_eq!(calculate_seek_cnt(expect_cnt), expected, "{expect_cnt}");
        }
    }

    #[test]
    fn calculate_seek_cnt_preserves_source_ceil_addition_wrap() {
        const CEIL_OVERFLOW_BIAS: u64 = MIN_ALLOWED_MAX_PAGING_SIZE - 1 - PAGING_GROWING_SUM;
        let last_before_wrap = u64::MAX - CEIL_OVERFLOW_BIAS;
        let first_after_wrap = last_before_wrap + 1;
        let last_before_wrap_result = (8 + u64::MAX / MIN_ALLOWED_MAX_PAGING_SIZE) as f64;

        assert_eq!(
            calculate_seek_cnt(last_before_wrap - 1),
            last_before_wrap_result
        );
        assert_eq!(
            calculate_seek_cnt(last_before_wrap),
            last_before_wrap_result
        );
        assert_eq!(calculate_seek_cnt(first_after_wrap), 8.0);
        assert_eq!(calculate_seek_cnt(u64::MAX), 8.0);
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
