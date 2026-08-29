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

//! Native Rust mapping of the Go `pkg/util/texttree` package.

use tidb_datatype::{go_chars, GoString, GoStringSource};

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
pub fn indent_4_child<T>(indent: &T, is_last_child: bool) -> GoString
where
    T: GoStringSource + ?Sized,
{
    let mut indent_chars: Vec<char> = go_chars(indent).collect();

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
    GoString::from(indent_chars.into_iter().collect::<String>())
}

/// Returns a pretty identifier which contains indent and tree node hierarchy
/// indicator.
#[must_use]
pub fn pretty_identifier<I, D>(id: &I, indent: &D, is_last_child: bool) -> GoString
where
    I: GoStringSource + ?Sized,
    D: GoStringSource + ?Sized,
{
    if indent.as_go_bytes().is_empty() {
        return id.to_go_string();
    }

    let mut indent_chars: Vec<char> = go_chars(indent).collect();
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
    let mut value = indent_chars.into_iter().collect::<String>().into_bytes();
    value.extend_from_slice(id.as_go_bytes());
    GoString::from(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Go `TestIndent4Child`.
    #[test]
    fn indent_4_child_go_test() {
        assert_eq!(
            indent_4_child("    ", false).as_bytes(),
            "    │ ".as_bytes()
        );
        assert_eq!(indent_4_child("    ", true).as_bytes(), "    │ ".as_bytes());
        assert_eq!(
            indent_4_child("   │ ", true).as_bytes(),
            "     │ ".as_bytes()
        );
    }

    // Go `TestPrettyIdentifier`.
    #[test]
    fn pretty_identifier_go_test() {
        assert_eq!(pretty_identifier("test", "", false).as_bytes(), b"test");
        assert_eq!(
            pretty_identifier("test", "  │  ", false).as_bytes(),
            "  ├ ─test".as_bytes()
        );
        assert_eq!(
            pretty_identifier("test", "\t\t│\t\t", false).as_bytes(),
            "\t\t├\t─test".as_bytes()
        );
        assert_eq!(
            pretty_identifier("test", "  │  ", true).as_bytes(),
            "  └ ─test".as_bytes()
        );
        assert_eq!(
            pretty_identifier("test", "\t\t│\t\t", true).as_bytes(),
            "\t\t└\t─test".as_bytes()
        );
    }

    #[test]
    fn arbitrary_go_string_bytes_follow_rune_conversion_and_raw_id_append() {
        assert_eq!(
            indent_4_child(&[0xff, b'|'][..], false).as_bytes(),
            "�|│ ".as_bytes()
        );
        assert_eq!(
            pretty_identifier(&[0xff][..], &[0xff][..], false).as_bytes(),
            &[0xe2, 0x94, 0x80, 0xff]
        );
    }
}
