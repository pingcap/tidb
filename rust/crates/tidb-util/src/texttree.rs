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

//! Complete transcreation of Go `pkg/util/texttree` (`texttree.go`).
//!
//! Helpers for rendering the box-drawing tree indentation used in `EXPLAIN`
//! output. Go works on `[]rune`; Rust's [`char`] is the same Unicode scalar
//! value, so the algorithms carry over directly on `Vec<char>`.
//!
//! `main_test.go` is a goroutine-leak `TestMain` with no observable behavior of
//! its own; it has no Rust equivalent.

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
    use super::{indent_4_child, pretty_identifier};

    // Go `TestPrettyIdentifier`.
    #[test]
    fn pretty_identifier_test() {
        assert_eq!(pretty_identifier("test", "", false), "test");
        assert_eq!(pretty_identifier("test", "  │  ", false), "  ├ ─test");
        assert_eq!(
            pretty_identifier("test", "\t\t│\t\t", false),
            "\t\t├\t─test"
        );
        assert_eq!(pretty_identifier("test", "  │  ", true), "  └ ─test");
        assert_eq!(pretty_identifier("test", "\t\t│\t\t", true), "\t\t└\t─test");
    }

    // Go `TestIndent4Child`.
    #[test]
    fn indent_4_child_test() {
        assert_eq!(indent_4_child("    ", false), "    │ ");
        assert_eq!(indent_4_child("    ", true), "    │ ");
        assert_eq!(indent_4_child("   │ ", true), "     │ ");
    }
}
