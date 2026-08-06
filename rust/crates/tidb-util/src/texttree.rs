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

//! Lockdown owner for `pkg/util/texttree/texttree.go`.
//!
//! `texttree.inventory.tsv` classifies every declaration, branch, and rule in
//! that Go file. The source fingerprint, inventory fingerprint, and Rust symbol
//! gate below make unreviewed source or inventory drift fail.
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

    const GO_SOURCE: &[u8] = include_bytes!("../../../../pkg/util/texttree/texttree.go");
    const LOCKDOWN_INVENTORY: &str = include_str!("texttree.inventory.tsv");
    const EXPECTED_INVENTORY_SHA256: &str =
        "f9da248738225c41915fc4e069046bd025a14468404f18dcf99085da6b51b1f9";
    const EXPECTED_ITEMS: [(&str, (&str, &str)); 22] = [
        ("D01", ("PORTED", "TREE_BODY")),
        ("D02", ("PORTED", "TREE_MIDDLE_NODE")),
        ("D03", ("PORTED", "TREE_LAST_NODE")),
        ("D04", ("PORTED", "TREE_GAP")),
        ("D05", ("PORTED", "TREE_NODE_IDENTIFIER")),
        ("F01", ("PORTED", "indent_4_child")),
        ("B01", ("PORTED", "indent_4_child")),
        ("B02", ("PORTED", "indent_4_child")),
        ("B03", ("PORTED", "indent_4_child")),
        ("B04", ("PORTED", "indent_4_child")),
        ("B05", ("PORTED", "indent_4_child")),
        ("R01", ("DECLINED", "-")),
        ("F02", ("PORTED", "pretty_identifier")),
        ("B06", ("PORTED", "pretty_identifier")),
        ("B07", ("PORTED", "pretty_identifier")),
        ("B08", ("PORTED", "pretty_identifier")),
        ("B09", ("PORTED", "pretty_identifier")),
        ("B10", ("PORTED", "pretty_identifier")),
        ("B11", ("PORTED", "pretty_identifier")),
        ("B12", ("PORTED", "pretty_identifier")),
        ("R02", ("DECLINED", "-")),
        ("R03", ("DECLINED", "-")),
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

        let _: [char; 5] = [
            TREE_BODY,
            TREE_MIDDLE_NODE,
            TREE_LAST_NODE,
            TREE_GAP,
            TREE_NODE_IDENTIFIER,
        ];
        let _: fn(&str, bool) -> String = indent_4_child;
        let _: fn(&str, &str, bool) -> String = pretty_identifier;
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
