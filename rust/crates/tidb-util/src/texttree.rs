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

//! Lockdown owner for the complete Go `pkg/util/texttree` package.
//!
//! `texttree.artifacts.tsv` fingerprints every direct package artifact and
//! `texttree.inventory.tsv` classifies every generated Go AST obligation. The
//! gate below also compile-anchors all Rust owners and preserves the explicit
//! Go-only harness and arbitrary-byte string declines.
//!
//! For valid UTF-8, Go's `[]rune` and Rust's [`char`] preserve the same Unicode
//! scalar values, so the source algorithms carry over directly on `Vec<char>`.
//! The inventory explicitly declines Go's arbitrary-byte string domain because
//! this module's existing public API accepts `&str`.

/// Indicates the current operator sub-tree is not finished, still has child
/// operators to be attached on.
pub const TREE_BODY: char = '│';
/// Indicates this operator is not the last child of the current sub-tree rooted
/// by its parent.
pub const TREE_MIDDLE_NODE: char = '├';
/// Indicates this operator is the last child of the current sub-tree rooted by
/// its parent.
pub const TREE_LAST_NODE: char = '└';
/// Represents the gap between the branches of the tree.
pub const TREE_GAP: char = ' ';
/// Replaces the [`TREE_GAP`] once we need to attach a node to a sub-tree.
pub const TREE_NODE_IDENTIFIER: char = '─';

/// Appends more blank to the `indent` string.
#[must_use]
pub fn indent_4_child(indent: &str, is_last_child: bool) -> String {
    let mut indent_chars: Vec<char> = indent.chars().collect();

    if is_last_child {
        // If the current node is the last node of the current operator tree, we
        // need to end this sub-tree by changing the closest tree body to a tree
        // gap.
        for c in indent_chars.iter_mut().rev() {
            if *c == TREE_BODY {
                *c = TREE_GAP;
                break;
            }
        }
    }

    indent_chars.push(TREE_BODY);
    indent_chars.push(TREE_GAP);
    indent_chars.into_iter().collect()
}

/// Returns a pretty identifier which contains indent and tree node hierarchy
/// indicator.
#[must_use]
pub fn pretty_identifier(id: &str, indent: &str, is_last_child: bool) -> String {
    if indent.is_empty() {
        return id.to_string();
    }

    let mut indent_chars: Vec<char> = indent.chars().collect();
    for c in indent_chars.iter_mut().rev() {
        if *c != TREE_BODY {
            continue;
        }

        // Here we attach a new node to the current sub-tree by changing the
        // closest tree body to a:
        // 1. tree last node, if this operator is the last child.
        // 2. tree middle node, if this operator is not the last child.
        *c = if is_last_child {
            TREE_LAST_NODE
        } else {
            TREE_MIDDLE_NODE
        };
        break;
    }

    // Replace the tree gap between the tree body and the node with a tree node
    // identifier.
    let last = indent_chars.len() - 1;
    indent_chars[last] = TREE_NODE_IDENTIFIER;
    indent_chars.into_iter().collect::<String>() + id
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, BTreeSet},
        fmt::Write as _,
    };

    use sha2::{Digest, Sha256};

    use super::*;

    const GO_BUILD: &[u8] = include_bytes!("../../../../pkg/util/texttree/BUILD.bazel");
    const GO_MAIN_TEST: &[u8] = include_bytes!("../../../../pkg/util/texttree/main_test.go");
    const GO_SOURCE: &[u8] = include_bytes!("../../../../pkg/util/texttree/texttree.go");
    const GO_TEST: &[u8] = include_bytes!("../../../../pkg/util/texttree/texttree_test.go");
    const ARTIFACT_MANIFEST: &str = include_str!("texttree.artifacts.tsv");
    const LOCKDOWN_INVENTORY: &str = include_str!("texttree.inventory.tsv");
    const SEMANTIC_DIVERGENCES: &str = include_str!("texttree.semantic-divergences.tsv");
    const DECLINED_EVIDENCE: &str = "source-quote:go_testsetup_and_goleak_only";
    const SYMBOL_EVIDENCE: &str =
        "rust-test:texttree_lockdown_inventory_is_complete_and_symbols_compile";
    const ARTIFACTS: [(&str, &str, &[u8]); 4] = [
        ("pkg/util/texttree/BUILD.bazel", "build", GO_BUILD),
        (
            "pkg/util/texttree/main_test.go",
            "test-support",
            GO_MAIN_TEST,
        ),
        ("pkg/util/texttree/texttree.go", "production", GO_SOURCE),
        ("pkg/util/texttree/texttree_test.go", "test", GO_TEST),
    ];

    #[test]
    fn texttree_lockdown_inventory_is_complete_and_symbols_compile() {
        let expected_manifest_prefix = [
            "# pkg-texttree-artifacts-v1",
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
                ("pkg/util/texttree/texttree.go", "const", owner, anchor) => {
                    let symbol = match (owner, anchor) {
                        ("const:TreeBody:0", "const:TreeBody:0") => "TREE_BODY",
                        ("const:TreeGap:0", "const:TreeGap:0") => "TREE_GAP",
                        ("const:TreeLastNode:0", "const:TreeLastNode:0") => "TREE_LAST_NODE",
                        ("const:TreeMiddleNode:0", "const:TreeMiddleNode:0") => "TREE_MIDDLE_NODE",
                        ("const:TreeNodeIdentifier:0", "const:TreeNodeIdentifier:0") => {
                            "TREE_NODE_IDENTIFIER"
                        }
                        _ => panic!("unexpected source constant row: {line}"),
                    };
                    assert_eq!(
                        columns[6..10],
                        ["PORTED", symbol, SYMBOL_EVIDENCE, "compile-owner-gate"]
                    );
                }
                ("pkg/util/texttree/texttree.go", "function", owner, anchor)
                    if owner == anchor && matches!(owner, "Indent4Child" | "PrettyIdentifier") =>
                {
                    let symbol = if owner == "Indent4Child" {
                        "indent_4_child"
                    } else {
                        "pretty_identifier"
                    };
                    assert_eq!(
                        columns[6..10],
                        ["PORTED", symbol, SYMBOL_EVIDENCE, "compile-owner-gate"]
                    );
                }
                ("pkg/util/texttree/texttree.go", "branch" | "loop", owner, anchor)
                    if anchor.starts_with(owner)
                        && matches!(owner, "Indent4Child" | "PrettyIdentifier") =>
                {
                    let (symbol, evidence) = if owner == "Indent4Child" {
                        (
                            "indent_4_child",
                            "rust-test:indent_4_child_preserves_source_rune_rules",
                        )
                    } else {
                        (
                            "pretty_identifier",
                            "rust-test:pretty_identifier_preserves_source_rune_rules",
                        )
                    };
                    assert_eq!(
                        columns[6..10],
                        ["PORTED", symbol, evidence, "behavior-mutation"]
                    );
                }
                (
                    "pkg/util/texttree/texttree_test.go",
                    "test" | "test_assertion",
                    owner,
                    anchor,
                ) if matches!(owner, "TestIndent4Child" | "TestPrettyIdentifier")
                    && anchor.starts_with(owner) =>
                {
                    let (symbol, evidence) = if owner == "TestIndent4Child" {
                        ("indent_4_child_go_test", "rust-test:indent_4_child_go_test")
                    } else {
                        (
                            "pretty_identifier_go_test",
                            "rust-test:pretty_identifier_go_test",
                        )
                    };
                    assert_eq!(
                        columns[6..10],
                        ["PORTED", symbol, evidence, "test-evidence-gate"]
                    );
                }
                (
                    "pkg/util/texttree/main_test.go",
                    "test_main" | "test_row",
                    "TestMain",
                    anchor,
                ) => {
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
                _ => panic!("unexpected texttree inventory row: {line}"),
            }
        }

        assert_eq!(ids.len(), 36);
        assert_eq!(
            categories,
            BTreeMap::from([
                ("branch", 10),
                ("const", 5),
                ("function", 2),
                ("loop", 4),
                ("test", 2),
                ("test_assertion", 8),
                ("test_main", 1),
                ("test_row", 4),
            ])
        );
        assert_eq!(statuses, BTreeMap::from([("DECLINED", 5), ("PORTED", 31)]));
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

        let semantic_rows: Vec<Vec<_>> = SEMANTIC_DIVERGENCES
            .lines()
            .filter(|line| !line.is_empty() && !line.starts_with('#'))
            .skip(1)
            .map(|line| line.split('\t').collect())
            .collect();
        assert_eq!(semantic_rows.len(), 3);
        assert!(semantic_rows.iter().all(|row| row.len() == 6));
        assert_eq!(
            semantic_rows
                .iter()
                .map(|row| row[0])
                .collect::<BTreeSet<_>>(),
            BTreeSet::from(["S01", "S02", "S03"])
        );
        assert!(semantic_rows.iter().all(|row| {
            row[2] == "DECLINED"
                && row[3] == "&str excludes invalid UTF-8"
                && row[4].starts_with("go-oracle:")
                && row[5] == "classification-evidence-gate"
        }));

        let go_main_test = std::str::from_utf8(GO_MAIN_TEST).expect("Go test support is UTF-8");
        assert!(go_main_test.contains("testsetup.SetupForCommonTest()"));
        assert!(go_main_test.contains("goleak.VerifyTestMain"));
        assert_eq!(go_main_test.matches("goleak.IgnoreTopFunction").count(), 4);
        let go_test = std::str::from_utf8(GO_TEST).expect("Go test source is UTF-8");
        assert!(go_test.contains("func TestIndent4Child"));
        assert!(go_test.contains("func TestPrettyIdentifier"));

        let _: [char; 5] = [
            TREE_BODY,
            TREE_MIDDLE_NODE,
            TREE_LAST_NODE,
            TREE_GAP,
            TREE_NODE_IDENTIFIER,
        ];
        let _: fn(&str, bool) -> String = indent_4_child;
        let _: fn(&str, &str, bool) -> String = pretty_identifier;
        let _: fn() = indent_4_child_go_test;
        let _: fn() = pretty_identifier_go_test;
        let _: fn() = indent_4_child_preserves_source_rune_rules;
        let _: fn() = pretty_identifier_preserves_source_rune_rules;
    }

    #[test]
    fn source_constants_are_exact() {
        assert_eq!(
            (
                TREE_BODY,
                TREE_MIDDLE_NODE,
                TREE_LAST_NODE,
                TREE_GAP,
                TREE_NODE_IDENTIFIER
            ),
            ('│', '├', '└', ' ', '─')
        );
    }

    // Go `TestIndent4Child`.
    #[test]
    fn indent_4_child_go_test() {
        assert_eq!(indent_4_child("    ", false), "    │ ");
        assert_eq!(indent_4_child("    ", true), "    │ ");
        assert_eq!(indent_4_child("   │ ", true), "     │ ");
    }

    // Go `TestPrettyIdentifier`.
    #[test]
    fn pretty_identifier_go_test() {
        assert_eq!(pretty_identifier("test", "", false), "test");
        assert_eq!(pretty_identifier("test", "  │  ", false), "  ├ ─test");
        assert_eq!(
            pretty_identifier("test", "\t\t│\t\t", false),
            "\t\t├\t─test"
        );
        assert_eq!(pretty_identifier("test", "  │  ", true), "  └ ─test");
        assert_eq!(pretty_identifier("test", "\t\t│\t\t", true), "\t\t└\t─test");
    }

    #[test]
    fn indent_4_child_preserves_source_rune_rules() {
        assert_eq!(indent_4_child("    ", false), "    │ ");
        assert_eq!(indent_4_child("    ", true), "    │ ");
        assert_eq!(indent_4_child("   │ ", true), "     │ ");
        assert_eq!(indent_4_child("", false), "│ ");
        assert_eq!(indent_4_child("", true), "│ ");
        assert_eq!(indent_4_child("α│x│y", false), "α│x│y│ ");
        assert_eq!(indent_4_child("α│x│y", true), "α│x y│ ");
        assert_eq!(indent_4_child("αxy", true), "αxy│ ");
    }

    #[test]
    fn pretty_identifier_preserves_source_rune_rules() {
        assert_eq!(pretty_identifier("test", "", false), "test");
        assert_eq!(pretty_identifier("test", "  │  ", false), "  ├ ─test");
        assert_eq!(
            pretty_identifier("test", "\t\t│\t\t", false),
            "\t\t├\t─test"
        );
        assert_eq!(pretty_identifier("test", "  │  ", true), "  └ ─test");
        assert_eq!(pretty_identifier("test", "\t\t│\t\t", true), "\t\t└\t─test");
        assert_eq!(pretty_identifier("标", "", false), "标");
        assert_eq!(pretty_identifier("标", "α│x│y", false), "α│x├─标");
        assert_eq!(pretty_identifier("标", "α│x│y", true), "α│x└─标");
        assert_eq!(pretty_identifier("标", "αxy", false), "αx─标");
        assert_eq!(pretty_identifier("标", "界", true), "─标");
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
