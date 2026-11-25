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

use std::borrow::Cow;
use tidb_goruntime::decode_go_utf8_lossy;
use tidb_goruntime::GoString;
use tidb_goruntime::GoStringSource;

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

// Go converts the indentation through []rune, replacing each invalid byte.
fn indent_text(indent: &[u8]) -> Cow<'_, str> {
    match std::str::from_utf8(indent) {
        Ok(text) => Cow::Borrowed(text),
        Err(_) => Cow::Owned(decode_go_utf8_lossy(indent)),
    }
}

fn branch_parts<'a>(indent: &'a str, replacement: &'a [u8]) -> [&'a [u8]; 3] {
    if let Some((prefix, suffix)) = indent.rsplit_once(TREE_BODY) {
        [prefix.as_bytes(), replacement, suffix.as_bytes()]
    } else {
        [indent.as_bytes(), b"", b""]
    }
}

fn join_parts(parts: &[&[u8]]) -> GoString {
    const INLINE_BYTES: usize = 64;
    let len = parts.iter().map(|part| part.len()).sum();
    if len <= INLINE_BYTES {
        let mut inline = [0; INLINE_BYTES];
        let mut destination = &mut inline[..len];
        for part in parts {
            let (prefix, rest) = destination.split_at_mut(part.len());
            prefix.copy_from_slice(part);
            destination = rest;
        }
        let bytes = &inline[..len];
        return GoString::try_inline_snapshot(bytes).unwrap_or_else(|| GoString::from(bytes));
    }
    let mut bytes = Vec::with_capacity(len);
    for part in parts {
        bytes.extend_from_slice(part);
    }
    GoString::from(bytes)
}

/// Appends more blank to the `indent` string.
pub fn indent_4_child<T>(indent: &T, is_last_child: bool) -> GoString
where
    T: GoStringSource + ?Sized,
{
    let indent = indent.as_go_bytes();
    if indent.is_empty() {
        return GoString::try_inline_snapshot("│ ".as_bytes()).expect("a single branch fits inline");
    }
    let indent = indent_text(indent);
    let mut body = [0; 4];
    let mut gap = [0; 4];
    let gap = TREE_GAP.encode_utf8(&mut gap).as_bytes();
    let [prefix, branch, suffix] = if is_last_child {
        branch_parts(&indent, gap)
    } else {
        [indent.as_bytes(), b"", b""]
    };
    join_parts(&[
        prefix,
        branch,
        suffix,
        TREE_BODY.encode_utf8(&mut body).as_bytes(),
        gap,
    ])
}

/// Returns a pretty identifier which contains indent and tree node hierarchy
/// indicator.
pub fn pretty_identifier<I, D>(id: &I, indent: &D, is_last_child: bool) -> GoString
where
    I: GoStringSource + ?Sized,
    D: GoStringSource + ?Sized,
{
    let indent = indent.as_go_bytes();
    if indent.is_empty() {
        return id.to_go_string();
    }

    let indent = indent_text(indent);
    let mut prefix = indent.chars();
    let last = prefix.next_back();
    let mut branch = [0; 4];
    let [prefix, branch, suffix] = if last == Some(TREE_BODY) {
        // Go overwrites this branch again when it replaces the final rune.
        [prefix.as_str().as_bytes(), b"", b""]
    } else {
        let replacement = if is_last_child {
            TREE_LAST_NODE
        } else {
            TREE_MIDDLE_NODE
        };
        branch_parts(
            prefix.as_str(),
            replacement.encode_utf8(&mut branch).as_bytes(),
        )
    };
    let mut node = [0; 4];
    join_parts(&[
        prefix,
        branch,
        suffix,
        TREE_NODE_IDENTIFIER.encode_utf8(&mut node).as_bytes(),
        id.as_go_bytes(),
    ])
}

#[cfg(test)]
mod tests {
    #![allow(non_snake_case)]

    use super::*;

    #[test]
    fn TestIndent4Child() {
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

    #[test]
    fn TestPrettyIdentifier() {
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
    fn malformed_indent_is_decoded_one_invalid_byte_at_a_time() {
        let indent = b" \xf0\x9f\xe2\x94\x82\x80 ";
        assert_eq!(indent_4_child(indent, true).as_bytes(), " �� � │ ".as_bytes());
        let mut expected = " ��├�─".as_bytes().to_vec();
        expected.extend_from_slice(b"\xffID");
        assert_eq!(
            pretty_identifier(b"\xffID", indent, false).as_bytes(),
            expected
        );
    }
}
