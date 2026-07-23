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

use std::collections::HashSet;

use serde_json::Value;

use crate::{
    binary_json::JSONNode, compare_binary_json, BinaryJSON, BinaryJSONError,
    JSONPathArraySelection, JSONPathExpression, JSONPathLeg,
};

/// JSON modification mode.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JSONModifyType {
    /// Insert only when the path does not exist.
    Insert,
    /// Replace only when the path exists.
    Replace,
    /// Insert or replace.
    Set,
}

/// JSON_SEARCH match cardinality.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JSONSearchMode {
    /// Return the first matching path.
    One,
    /// Return every matching path.
    All,
}

impl BinaryJSON {
    /// Extracts one or more parsed paths using MySQL autowrap semantics.
    pub fn extract(
        &self,
        paths: &[JSONPathExpression],
    ) -> Result<Option<BinaryJSON>, BinaryJSONError> {
        let root = self.to_node()?;
        let mut matches = Vec::new();
        let mut seen = HashSet::new();
        for path in paths {
            extract_value(&root, path.legs(), &mut matches, &mut seen);
        }
        if matches.is_empty() {
            return Ok(None);
        }
        if paths.len() == 1 && !paths[0].could_match_multiple_values() && matches.len() == 1 {
            return BinaryJSON::from_node(matches.remove(0)).map(Some);
        }
        BinaryJSON::from_node(&JSONNode::Array(matches.into_iter().cloned().collect())).map(Some)
    }

    /// Returns sorted object keys as a JSON array, or an empty array otherwise.
    pub fn keys(&self) -> Result<BinaryJSON, BinaryJSONError> {
        let keys = match self.to_node()? {
            JSONNode::Object(values) => {
                let mut keys = values.into_iter().map(|(key, _)| key).collect::<Vec<_>>();
                keys.sort_unstable_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
                keys.into_iter()
                    .map(|key| BinaryJSON::from_value(&Value::String(key)).map(JSONNode::Scalar))
                    .collect::<Result<Vec<_>, _>>()?
            }
            _ => Vec::new(),
        };
        BinaryJSON::from_node(&JSONNode::Array(keys))
    }

    /// Returns an array/object element count, or one for a scalar.
    pub fn element_count(&self) -> Result<usize, BinaryJSONError> {
        Ok(match self.to_node()? {
            JSONNode::Array(values) => values.len(),
            JSONNode::Object(values) => values.len(),
            _ => 1,
        })
    }

    /// Returns one array element by zero-based index.
    pub fn array_get(&self, index: usize) -> Result<Option<BinaryJSON>, BinaryJSONError> {
        let JSONNode::Array(values) = self.to_node()? else {
            return Ok(None);
        };
        values.get(index).map(BinaryJSON::from_node).transpose()
    }

    /// Returns one object member by exact UTF-8 key.
    pub fn object_get(&self, key: &str) -> Result<Option<BinaryJSON>, BinaryJSONError> {
        let JSONNode::Object(values) = self.to_node()? else {
            return Ok(None);
        };
        values
            .iter()
            .find(|(name, _)| name == key)
            .map(|(_, value)| BinaryJSON::from_node(value))
            .transpose()
    }

    /// Returns object entries in source byte-sorted key order.
    pub fn object_entries(&self) -> Result<Vec<(String, BinaryJSON)>, BinaryJSONError> {
        let JSONNode::Object(values) = self.to_node()? else {
            return Ok(Vec::new());
        };
        values
            .into_iter()
            .map(|(key, value)| Ok((key, BinaryJSON::from_node(&value)?)))
            .collect()
    }

    /// Returns the maximum document nesting depth, with every scalar at depth one.
    pub fn element_depth(&self) -> Result<usize, BinaryJSONError> {
        fn depth(value: &JSONNode) -> usize {
            match value {
                JSONNode::Array(values) => 1 + values.iter().map(depth).max().unwrap_or(0),
                JSONNode::Object(values) => {
                    1 + values
                        .iter()
                        .map(|(_, value)| depth(value))
                        .max()
                        .unwrap_or(0)
                }
                _ => 1,
            }
        }
        Ok(depth(&self.to_node()?))
    }

    /// Applies JSON_INSERT, JSON_REPLACE, or JSON_SET paths from left to right.
    pub fn modify(
        &self,
        paths: &[JSONPathExpression],
        values: &[BinaryJSON],
        mode: JSONModifyType,
    ) -> Result<BinaryJSON, BinaryJSONError> {
        if paths.len() != values.len() {
            return Err(BinaryJSONError::InvalidPath);
        }
        let mut document = self.to_node()?;
        for (path, value) in paths.iter().zip(values) {
            if path.contains_any_asterisk() || path.contains_any_range() {
                return Err(BinaryJSONError::InvalidPath);
            }
            document = modify_node(document, path.legs(), value.to_node()?, mode);
        }
        BinaryJSON::from_node(&document)
    }

    /// Inserts before one exact array cell, or appends when the index is past the end.
    pub fn array_insert(
        &self,
        path: &JSONPathExpression,
        value: &BinaryJSON,
    ) -> Result<BinaryJSON, BinaryJSONError> {
        if path.contains_any_asterisk() || path.contains_any_range() {
            return Err(BinaryJSONError::InvalidPath);
        }
        let Some((parent, JSONPathLeg::Array(JSONPathArraySelection::Index(index)))) =
            path.pop_last()
        else {
            return Err(BinaryJSONError::InvalidPath);
        };
        let Some(parent_value) = self.extract(std::slice::from_ref(&parent))? else {
            return Ok(self.clone());
        };
        let JSONNode::Array(mut array) = parent_value.to_node()? else {
            return Ok(self.clone());
        };
        let index = if index < 0 {
            let Some(index) = i64::try_from(array.len())
                .ok()
                .and_then(|length| length.checked_add(index))
                .and_then(|index| usize::try_from(index).ok())
            else {
                return Ok(self.clone());
            };
            index
        } else {
            usize::try_from(index).unwrap_or(usize::MAX)
        }
        .min(array.len());
        array.insert(index, value.to_node()?);
        self.modify(
            &[parent],
            &[BinaryJSON::from_node(&JSONNode::Array(array))?],
            JSONModifyType::Set,
        )
    }

    /// Removes exact paths from left to right.
    pub fn remove(&self, paths: &[JSONPathExpression]) -> Result<BinaryJSON, BinaryJSONError> {
        let mut document = self.to_node()?;
        for path in paths {
            if path.legs().is_empty() || path.contains_any_asterisk() || path.contains_any_range() {
                return Err(BinaryJSONError::InvalidPath);
            }
            remove_node(&mut document, path.legs());
        }
        BinaryJSON::from_node(&document)
    }

    /// Builds TiDB's equality hash representation.
    pub fn hash_value(&self) -> Result<Vec<u8>, BinaryJSONError> {
        let mut output = Vec::new();
        append_hash_value(self, &mut output)?;
        Ok(output)
    }

    /// Returns the exact number of bytes produced by [`BinaryJSON::hash_value`].
    pub fn hash_value_size(&self) -> Result<usize, BinaryJSONError> {
        self.hash_value().map(|value| value.len())
    }

    /// Returns whether this value compares equal to JSON numeric zero.
    pub fn is_zero(&self) -> bool {
        let zero =
            BinaryJSON::from_value(&Value::Number(0.into())).expect("static JSON zero must encode");
        compare_binary_json(self, &zero).is_eq()
    }

    /// Walks selected subtrees in source preorder and reports each full path once.
    pub fn walk(
        &self,
        paths: &[JSONPathExpression],
    ) -> Result<Vec<(JSONPathExpression, BinaryJSON)>, BinaryJSONError> {
        let root = self.to_node()?;
        let mut roots = Vec::new();
        if paths.is_empty() {
            roots.push((JSONPathExpression::default(), &root));
        } else {
            for path in paths {
                select_walk_roots(
                    &root,
                    path.legs(),
                    JSONPathExpression::default(),
                    &mut roots,
                );
            }
        }

        let mut seen = HashSet::new();
        let mut output = Vec::new();
        for (path, value) in roots {
            walk_value(value, path, &mut seen, &mut output)?;
        }
        Ok(output)
    }

    /// Extracts path matches without scalar-to-array autowrapping.
    ///
    /// This is the source package's `extractToCallback` behavior, represented
    /// as values so Rust callers do not need a callback-only internal API.
    pub fn extract_matches(
        &self,
        path: &JSONPathExpression,
    ) -> Result<Vec<(JSONPathExpression, BinaryJSON)>, BinaryJSONError> {
        let root = self.to_node()?;
        let mut matches = Vec::new();
        select_walk_roots(
            &root,
            path.legs(),
            JSONPathExpression::default(),
            &mut matches,
        );
        matches
            .into_iter()
            .map(|(path, value)| Ok((path, BinaryJSON::from_node(value)?)))
            .collect()
    }

    /// Implements JSON_SEARCH over string values.
    pub fn search(
        &self,
        mode: JSONSearchMode,
        pattern: &str,
        escape: char,
        paths: &[JSONPathExpression],
    ) -> Result<Option<BinaryJSON>, BinaryJSONError> {
        let mut matches = Vec::new();
        for (path, value) in self.walk(paths)? {
            if value
                .as_string()
                .and_then(|bytes| std::str::from_utf8(bytes).ok())
                .is_some_and(|text| like_matches(text, pattern, escape))
            {
                matches.push(Value::String(path.to_string()));
                if mode == JSONSearchMode::One {
                    break;
                }
            }
        }
        match matches.len() {
            0 => Ok(None),
            1 => BinaryJSON::from_value(&matches.remove(0)).map(Some),
            _ => BinaryJSON::from_value(&Value::Array(matches)).map(Some),
        }
    }
}

/// Returns the byte length of one `type code + payload` binary JSON value.
pub fn peek_binary_json_len(bytes: &[u8]) -> Result<usize, BinaryJSONError> {
    let (&type_code, payload) = bytes.split_first().ok_or(BinaryJSONError::InvalidBinary)?;
    let payload_len = match type_code {
        crate::JSON_TYPE_CODE_OBJECT | crate::JSON_TYPE_CODE_ARRAY => {
            let header = payload.get(..8).ok_or(BinaryJSONError::InvalidBinary)?;
            u32::from_le_bytes(header[4..8].try_into().unwrap()) as usize
        }
        crate::JSON_TYPE_CODE_STRING => {
            let (length, prefix) = decode_uvarint_for_peek(payload)?;
            prefix + length
        }
        crate::JSON_TYPE_CODE_INT64
        | crate::JSON_TYPE_CODE_UINT64
        | crate::JSON_TYPE_CODE_FLOAT64
        | crate::JSON_TYPE_CODE_DATE
        | crate::JSON_TYPE_CODE_DATETIME
        | crate::JSON_TYPE_CODE_TIMESTAMP => 8,
        crate::JSON_TYPE_CODE_LITERAL => 1,
        crate::JSON_TYPE_CODE_OPAQUE => {
            let payload = payload.get(1..).ok_or(BinaryJSONError::InvalidBinary)?;
            let (length, prefix) = decode_uvarint_for_peek(payload)?;
            1 + prefix + length
        }
        crate::JSON_TYPE_CODE_DURATION => 12,
        _ => return Err(BinaryJSONError::InvalidBinary),
    };
    let total = 1_usize
        .checked_add(payload_len)
        .ok_or(BinaryJSONError::InvalidBinary)?;
    if total > bytes.len() {
        return Err(BinaryJSONError::InvalidBinary);
    }
    Ok(total)
}

/// Implements MySQL JSON_CONTAINS structural containment.
pub fn contains_binary_json(
    object: &BinaryJSON,
    target: &BinaryJSON,
) -> Result<bool, BinaryJSONError> {
    contains_node(&object.to_node()?, &target.to_node()?)
}

/// Implements MySQL JSON_OVERLAPS structural overlap.
pub fn overlaps_binary_json(
    object: &BinaryJSON,
    target: &BinaryJSON,
) -> Result<bool, BinaryJSONError> {
    overlaps_node(&object.to_node()?, &target.to_node()?)
}

/// Implements MySQL JSON_MERGE_PRESERVE.
pub fn merge_binary_json(values: &[BinaryJSON]) -> Result<BinaryJSON, BinaryJSONError> {
    let mut values = values
        .iter()
        .map(BinaryJSON::to_node)
        .collect::<Result<Vec<_>, _>>()?;
    let merged = if values.is_empty() {
        JSONNode::Array(Vec::new())
    } else {
        let mut result = values.remove(0);
        for value in values {
            result = merge_preserve_node(result, value);
        }
        result
    };
    BinaryJSON::from_node(&merged)
}

/// Implements RFC 7396 JSON merge-patch.
pub fn merge_patch_binary_json(
    values: &[BinaryJSON],
) -> Result<Option<BinaryJSON>, BinaryJSONError> {
    let Some(first) = values.first() else {
        return Ok(None);
    };
    let mut result = first.to_node()?;
    for patch in &values[1..] {
        result = merge_patch_node(result, patch.to_node()?);
    }
    BinaryJSON::from_node(&result).map(Some)
}

fn extract_value<'a>(
    value: &'a JSONNode,
    legs: &[JSONPathLeg],
    output: &mut Vec<&'a JSONNode>,
    seen: &mut HashSet<*const JSONNode>,
) {
    let Some((leg, remain)) = legs.split_first() else {
        if seen.insert(value) {
            output.push(value);
        }
        return;
    };
    match leg {
        JSONPathLeg::Key(key) if key == "*" => {
            if let JSONNode::Object(values) = value {
                let mut entries = values.iter().collect::<Vec<_>>();
                entries.sort_unstable_by(|left, right| left.0.as_bytes().cmp(right.0.as_bytes()));
                for (_, value) in entries {
                    extract_value(value, remain, output, seen);
                }
            }
        }
        JSONPathLeg::Key(key) => {
            if let JSONNode::Object(values) = value {
                if let Some((_, value)) = values.iter().find(|(name, _)| name == key) {
                    extract_value(value, remain, output, seen);
                }
            }
        }
        JSONPathLeg::Array(selection) => {
            if let JSONNode::Array(values) = value {
                for index in selected_indices(selection, values.len()) {
                    extract_value(&values[index], remain, output, seen);
                }
            } else if selection_includes_zero(selection, 1) {
                extract_value(value, remain, output, seen);
            }
        }
        JSONPathLeg::DoubleAsterisk => {
            extract_value(value, remain, output, seen);
            match value {
                JSONNode::Array(values) => {
                    for value in values {
                        extract_descendants(value, remain, output, seen);
                    }
                }
                JSONNode::Object(values) => {
                    let mut entries = values.iter().collect::<Vec<_>>();
                    entries
                        .sort_unstable_by(|left, right| left.0.as_bytes().cmp(right.0.as_bytes()));
                    for (_, value) in entries {
                        extract_descendants(value, remain, output, seen);
                    }
                }
                _ => {}
            }
        }
    }
}

fn extract_descendants<'a>(
    value: &'a JSONNode,
    remain: &[JSONPathLeg],
    output: &mut Vec<&'a JSONNode>,
    seen: &mut HashSet<*const JSONNode>,
) {
    extract_value(value, remain, output, seen);
    match value {
        JSONNode::Array(values) => {
            for value in values {
                extract_descendants(value, remain, output, seen);
            }
        }
        JSONNode::Object(values) => {
            let mut entries = values.iter().collect::<Vec<_>>();
            entries.sort_unstable_by(|left, right| left.0.as_bytes().cmp(right.0.as_bytes()));
            for (_, value) in entries {
                extract_descendants(value, remain, output, seen);
            }
        }
        _ => {}
    }
}

fn select_walk_roots<'a>(
    value: &'a JSONNode,
    legs: &[JSONPathLeg],
    path: JSONPathExpression,
    output: &mut Vec<(JSONPathExpression, &'a JSONNode)>,
) {
    let Some((leg, remain)) = legs.split_first() else {
        output.push((path, value));
        return;
    };
    match leg {
        JSONPathLeg::Key(key) if key == "*" => {
            if let JSONNode::Object(values) = value {
                let mut values = values.iter().collect::<Vec<_>>();
                values.sort_unstable_by(|left, right| left.0.as_bytes().cmp(right.0.as_bytes()));
                for (key, value) in values {
                    select_walk_roots(value, remain, path.push_back_key(key), output);
                }
            }
        }
        JSONPathLeg::Key(key) => {
            if let JSONNode::Object(values) = value {
                if let Some((_, value)) = values.iter().find(|(name, _)| name == key) {
                    select_walk_roots(value, remain, path.push_back_key(key), output);
                }
            }
        }
        JSONPathLeg::Array(selection) => {
            if let JSONNode::Array(values) = value {
                for index in selected_indices(selection, values.len()) {
                    select_walk_roots(
                        &values[index],
                        remain,
                        path.push_back_index(index as i64),
                        output,
                    );
                }
            }
        }
        JSONPathLeg::DoubleAsterisk => {
            select_walk_roots(value, remain, path.clone(), output);
            match value {
                JSONNode::Array(values) => {
                    for (index, value) in values.iter().enumerate() {
                        select_walk_roots(value, legs, path.push_back_index(index as i64), output);
                    }
                }
                JSONNode::Object(values) => {
                    let mut values = values.iter().collect::<Vec<_>>();
                    values
                        .sort_unstable_by(|left, right| left.0.as_bytes().cmp(right.0.as_bytes()));
                    for (key, value) in values {
                        select_walk_roots(value, legs, path.push_back_key(key), output);
                    }
                }
                _ => {}
            }
        }
    }
}

fn walk_value(
    value: &JSONNode,
    path: JSONPathExpression,
    seen: &mut HashSet<String>,
    output: &mut Vec<(JSONPathExpression, BinaryJSON)>,
) -> Result<(), BinaryJSONError> {
    if !seen.insert(path.to_string()) {
        return Ok(());
    }
    output.push((path.clone(), BinaryJSON::from_node(value)?));
    match value {
        JSONNode::Array(values) => {
            for (index, value) in values.iter().enumerate() {
                walk_value(value, path.push_back_index(index as i64), seen, output)?;
            }
        }
        JSONNode::Object(values) => {
            let mut values = values.iter().collect::<Vec<_>>();
            values.sort_unstable_by(|left, right| left.0.as_bytes().cmp(right.0.as_bytes()));
            for (key, value) in values {
                walk_value(value, path.push_back_key(key), seen, output)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn like_matches(text: &str, pattern: &str, escape: char) -> bool {
    let text = text.chars().collect::<Vec<_>>();
    let pattern = pattern.chars().collect::<Vec<_>>();
    let mut memo = std::collections::HashMap::new();
    like_matches_from(&text, &pattern, escape, 0, 0, &mut memo)
}

fn like_matches_from(
    text: &[char],
    pattern: &[char],
    escape: char,
    text_index: usize,
    pattern_index: usize,
    memo: &mut std::collections::HashMap<(usize, usize), bool>,
) -> bool {
    if let Some(result) = memo.get(&(text_index, pattern_index)) {
        return *result;
    }
    let result = if pattern_index == pattern.len() {
        text_index == text.len()
    } else if pattern[pattern_index] == escape {
        pattern.get(pattern_index + 1).is_some_and(|literal| {
            text.get(text_index) == Some(literal)
                && like_matches_from(
                    text,
                    pattern,
                    escape,
                    text_index + 1,
                    pattern_index + 2,
                    memo,
                )
        })
    } else if pattern[pattern_index] == '%' {
        like_matches_from(text, pattern, escape, text_index, pattern_index + 1, memo)
            || (text_index < text.len()
                && like_matches_from(text, pattern, escape, text_index + 1, pattern_index, memo))
    } else if pattern[pattern_index] == '_' {
        text_index < text.len()
            && like_matches_from(
                text,
                pattern,
                escape,
                text_index + 1,
                pattern_index + 1,
                memo,
            )
    } else {
        text.get(text_index) == pattern.get(pattern_index)
            && like_matches_from(
                text,
                pattern,
                escape,
                text_index + 1,
                pattern_index + 1,
                memo,
            )
    };
    memo.insert((text_index, pattern_index), result);
    result
}

fn selected_indices(selection: &JSONPathArraySelection, length: usize) -> Vec<usize> {
    match selection {
        JSONPathArraySelection::Asterisk => (0..length).collect(),
        JSONPathArraySelection::Index(index) => {
            normalize_index(*index, length).into_iter().collect()
        }
        JSONPathArraySelection::Range { start, end } => {
            if length == 0 {
                return Vec::new();
            }
            let Some(start) = normalize_range_start(*start, length) else {
                return Vec::new();
            };
            let end = normalize_range_end(*end, length);
            if start > end {
                Vec::new()
            } else {
                (start..=end).collect()
            }
        }
    }
}

fn normalize_range_start(index: i64, length: usize) -> Option<usize> {
    if index >= 0 {
        return usize::try_from(index).ok().filter(|index| *index < length);
    }
    Some(i64::try_from(length).ok()?.saturating_add(index).max(0) as usize)
}

fn normalize_range_end(index: i64, length: usize) -> usize {
    if index >= 0 {
        return usize::try_from(index).unwrap_or(usize::MAX).min(length - 1);
    }
    i64::try_from(length)
        .unwrap_or(i64::MAX)
        .saturating_add(index)
        .max(0) as usize
}

fn selection_includes_zero(selection: &JSONPathArraySelection, length: usize) -> bool {
    selected_indices(selection, length).contains(&0)
}

fn normalize_index(index: i64, length: usize) -> Option<usize> {
    let index = if index < 0 {
        i64::try_from(length).ok()?.checked_add(index)?
    } else {
        index
    };
    usize::try_from(index).ok().filter(|index| *index < length)
}

fn contains_node(object: &JSONNode, target: &JSONNode) -> Result<bool, BinaryJSONError> {
    Ok(match (object, target) {
        (JSONNode::Object(object), JSONNode::Object(target)) => {
            target.iter().all(|(key, target)| {
                object
                    .iter()
                    .find(|(name, _)| name == key)
                    .is_some_and(|(_, object)| contains_node(object, target).unwrap_or(false))
            })
        }
        (JSONNode::Array(object), JSONNode::Array(target)) => target.iter().all(|target| {
            object
                .iter()
                .any(|object| contains_node(object, target).unwrap_or(false))
        }),
        (JSONNode::Array(object), target) => object
            .iter()
            .any(|object| contains_node(object, target).unwrap_or(false)),
        _ => {
            let object = BinaryJSON::from_node(object)?;
            let target = BinaryJSON::from_node(target)?;
            compare_binary_json(&object, &target).is_eq()
        }
    })
}

fn overlaps_node(left: &JSONNode, right: &JSONNode) -> Result<bool, BinaryJSONError> {
    Ok(match (left, right) {
        (JSONNode::Object(left), JSONNode::Object(right)) => left.iter().any(|(key, left)| {
            right
                .iter()
                .find(|(name, _)| name == key)
                .is_some_and(|(_, right)| overlaps_node(left, right).unwrap_or(false))
        }),
        (JSONNode::Array(left), JSONNode::Array(right)) => left.iter().any(|left| {
            right
                .iter()
                .any(|right| overlaps_node(left, right).unwrap_or(false))
        }),
        (JSONNode::Array(values), scalar) | (scalar, JSONNode::Array(values)) => values
            .iter()
            .any(|value| overlaps_node(value, scalar).unwrap_or(false)),
        _ => {
            let left = BinaryJSON::from_node(left)?;
            let right = BinaryJSON::from_node(right)?;
            compare_binary_json(&left, &right).is_eq()
        }
    })
}

fn merge_preserve_node(left: JSONNode, right: JSONNode) -> JSONNode {
    match (left, right) {
        (JSONNode::Object(mut left), JSONNode::Object(right)) => {
            for (key, right) in right {
                if let Some(index) = left.iter().position(|(name, _)| name == &key) {
                    let (_, left_value) = left.remove(index);
                    left.push((key, merge_preserve_node(left_value, right)));
                } else {
                    left.push((key, right));
                }
            }
            JSONNode::Object(left)
        }
        (JSONNode::Array(mut left), JSONNode::Array(right)) => {
            left.extend(right);
            JSONNode::Array(left)
        }
        (JSONNode::Array(mut left), right) => {
            left.push(right);
            JSONNode::Array(left)
        }
        (left, JSONNode::Array(mut right)) => {
            right.insert(0, left);
            JSONNode::Array(right)
        }
        (left, right) => JSONNode::Array(vec![left, right]),
    }
}

fn merge_patch_node(target: JSONNode, patch: JSONNode) -> JSONNode {
    let JSONNode::Object(patch) = patch else {
        return patch;
    };
    let mut target = match target {
        JSONNode::Object(target) => target,
        _ => Vec::new(),
    };
    for (key, patch) in patch {
        if node_is_null(&patch) {
            if let Some(index) = target.iter().position(|(name, _)| name == &key) {
                target.remove(index);
            }
        } else {
            let current = target
                .iter()
                .position(|(name, _)| name == &key)
                .map(|index| target.remove(index).1)
                .unwrap_or_else(|| {
                    JSONNode::Scalar(BinaryJSON::parse("null").expect("static JSON null"))
                });
            target.push((key, merge_patch_node(current, patch)));
        }
    }
    JSONNode::Object(target)
}

fn append_hash_value(value: &BinaryJSON, output: &mut Vec<u8>) -> Result<(), BinaryJSONError> {
    if let Some(integer) = value.as_i64() {
        if significant_fraction_bits(integer.unsigned_abs()) <= 52 {
            let real = integer as f64;
            output.push(crate::JSON_TYPE_CODE_FLOAT64);
            output.extend_from_slice(&real.to_bits().to_le_bytes());
            return Ok(());
        }
    }
    if let Some(integer) = value.as_u64() {
        if significant_fraction_bits(integer) <= 52 {
            let real = integer as f64;
            output.push(crate::JSON_TYPE_CODE_FLOAT64);
            output.extend_from_slice(&real.to_bits().to_le_bytes());
            return Ok(());
        }
    }
    match value.to_node()? {
        JSONNode::Array(values) => {
            output.push(crate::JSON_TYPE_CODE_ARRAY);
            output.extend_from_slice(&(values.len() as u32).to_le_bytes());
            for value in values {
                append_hash_value(&BinaryJSON::from_node(&value)?, output)?;
            }
        }
        JSONNode::Object(mut values) => {
            output.push(crate::JSON_TYPE_CODE_OBJECT);
            output.extend_from_slice(&(values.len() as u32).to_le_bytes());
            values.sort_unstable_by(|left, right| left.0.as_bytes().cmp(right.0.as_bytes()));
            for (key, value) in values {
                let key = BinaryJSON::from_value(&Value::String(key))?;
                output.extend_from_slice(key.value());
                append_hash_value(&BinaryJSON::from_node(&value)?, output)?;
            }
        }
        _ => output.extend_from_slice(&value.encoded()),
    }
    Ok(())
}

fn significant_fraction_bits(value: u64) -> u32 {
    if value == 0 {
        0
    } else {
        64 - value.leading_zeros() - value.trailing_zeros() - 1
    }
}

fn node_is_null(value: &JSONNode) -> bool {
    matches!(
        value,
        JSONNode::Scalar(value)
            if value.type_code() == crate::JSON_TYPE_CODE_LITERAL
                && value.value() == [crate::JSON_LITERAL_NULL]
    )
}

fn decode_uvarint_for_peek(bytes: &[u8]) -> Result<(usize, usize), BinaryJSONError> {
    let mut value = 0_usize;
    for (index, byte) in bytes.iter().copied().enumerate().take(10) {
        value |= usize::from(byte & 0x7f) << (index * 7);
        if byte < 0x80 {
            return Ok((value, index + 1));
        }
    }
    Err(BinaryJSONError::InvalidBinary)
}

fn modify_node(
    mut document: JSONNode,
    legs: &[JSONPathLeg],
    replacement: JSONNode,
    mode: JSONModifyType,
) -> JSONNode {
    let Some((leg, remain)) = legs.split_first() else {
        return match mode {
            JSONModifyType::Insert => document,
            JSONModifyType::Replace | JSONModifyType::Set => replacement,
        };
    };
    match leg {
        JSONPathLeg::Key(key) => {
            let JSONNode::Object(values) = &mut document else {
                return document;
            };
            let position = values.iter().position(|(name, _)| name == key);
            if remain.is_empty() {
                match (position.is_some(), mode) {
                    (true, JSONModifyType::Insert) | (false, JSONModifyType::Replace) => {}
                    _ => {
                        if let Some(position) = position {
                            values[position].1 = replacement;
                        } else {
                            values.push((key.clone(), replacement));
                        }
                    }
                }
            } else if let Some(position) = position {
                let (_, value) = values.remove(position);
                values.push((key.clone(), modify_node(value, remain, replacement, mode)));
            }
            document
        }
        JSONPathLeg::Array(JSONPathArraySelection::Index(index)) => {
            if let JSONNode::Array(values) = &mut document {
                if let Some(index) = normalize_index(*index, values.len()) {
                    if remain.is_empty() && mode == JSONModifyType::Insert {
                        return document;
                    }
                    let value = values.remove(index);
                    values.insert(index, modify_node(value, remain, replacement, mode));
                } else if remain.is_empty() && *index >= 0 && mode != JSONModifyType::Replace {
                    values.push(replacement);
                }
                return document;
            }

            if normalize_index(*index, 1) == Some(0) {
                return modify_node(document, remain, replacement, mode);
            }
            if remain.is_empty() && *index > 0 && mode != JSONModifyType::Replace {
                return JSONNode::Array(vec![document, replacement]);
            }
            document
        }
        JSONPathLeg::Array(_) | JSONPathLeg::DoubleAsterisk => document,
    }
}

fn remove_node(document: &mut JSONNode, legs: &[JSONPathLeg]) {
    let Some((leg, remain)) = legs.split_first() else {
        return;
    };
    match leg {
        JSONPathLeg::Key(key) => {
            let JSONNode::Object(values) = document else {
                return;
            };
            if remain.is_empty() {
                if let Some(position) = values.iter().position(|(name, _)| name == key) {
                    values.remove(position);
                }
            } else if let Some((_, value)) = values.iter_mut().find(|(name, _)| name == key) {
                remove_node(value, remain);
            }
        }
        JSONPathLeg::Array(JSONPathArraySelection::Index(index)) => {
            let JSONNode::Array(values) = document else {
                return;
            };
            let Some(index) = normalize_index(*index, values.len()) else {
                return;
            };
            if remain.is_empty() {
                values.remove(index);
            } else {
                remove_node(&mut values[index], remain);
            }
        }
        JSONPathLeg::Array(_) | JSONPathLeg::DoubleAsterisk => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{parse_json_path_expr, CoreTime, MySqlDuration, Time, TimeType};

    fn json(text: &str) -> BinaryJSON {
        BinaryJSON::parse(text).unwrap()
    }

    #[test]
    fn test_binary_json_extract_source_rows() {
        let cases = [
            (
                r#"{"\"hello\"":"world","a":[1,"2",{"aa":"bb"},4.0,{"aa":"cc"}],"b":true,"c":["d"]}"#,
                vec!["$.a"],
                Some(r#"[1,"2",{"aa":"bb"},4.0,{"aa":"cc"}]"#),
            ),
            (
                r#"[{"a":1,"b":true},3,3.5,"hello, world",null,true]"#,
                vec!["$.a"],
                None,
            ),
            (
                r#"{"\"hello\"":"world","a":[1,"2",{"aa":"bb"},4.0,{"aa":"cc"}],"b":true,"c":["d"]}"#,
                vec!["$[0]"],
                Some(
                    r#"{"\"hello\"":"world","a":[1,"2",{"aa":"bb"},4.0,{"aa":"cc"}],"b":true,"c":["d"]}"#,
                ),
            ),
            (
                r#"[{"a":1,"b":true},3,3.5,"hello, world",null,true]"#,
                vec!["$[0]"],
                Some(r#"{"a":1,"b":true}"#),
            ),
            (
                r#"{"\"hello\"":"world","a":[1,"2",{"aa":"bb"},4.0,{"aa":"cc"}],"b":true,"c":["d"]}"#,
                vec!["$.a[2].aa"],
                Some(r#""bb""#),
            ),
            (
                r#"{"\"hello\"":"world","a":[1,"2",{"aa":"bb"},4.0,{"aa":"cc"}],"b":true,"c":["d"]}"#,
                vec!["$.a[*].aa"],
                Some(r#"["bb","cc"]"#),
            ),
            (
                r#"{"\"hello\"":"world","a":[1,"2",{"aa":"bb"},4.0,{"aa":"cc"}],"b":true,"c":["d"]}"#,
                vec![r#"$.a[*]."aa""#],
                Some(r#"["bb","cc"]"#),
            ),
            (
                r#"{"\"hello\"":"world","a":[1,"2",{"aa":"bb"},4.0,{"aa":"cc"}],"b":true,"c":["d"]}"#,
                vec!["$.*[0]"],
                Some(r#"["world",1,true,"d"]"#),
            ),
            (
                r#"{"\"hello\"":"world","a":[1,"2",{"aa":"bb"},4.0,{"aa":"cc"}],"b":true,"c":["d"]}"#,
                vec![r#"$."\"hello\"""#],
                Some(r#""world""#),
            ),
            (
                r#"{"\"hello\"":"world","a":[1,"2",{"aa":"bb"},4.0,{"aa":"cc"}],"b":true,"c":["d"]}"#,
                vec!["$**[1]"],
                Some(r#"["2"]"#),
            ),
            (
                r#"{"properties":{"$type":"TiDB"}}"#,
                vec!["$.properties.$type"],
                Some(r#""TiDB""#),
            ),
            (
                r#"{"properties":{"$type$type":{"$a$a":"TiDB"}}}"#,
                vec!["$.properties.$type$type"],
                Some(r#"{"$a$a":"TiDB"}"#),
            ),
            (
                r#"{"properties":{"$type$type":{"$a$a":"TiDB"}}}"#,
                vec!["$.properties.$type$type.$a$a"],
                Some(r#""TiDB""#),
            ),
            (
                r#"{"properties":{"$type":{"$a":{"$b":"TiDB"}}}}"#,
                vec!["$.properties.$type.$a.$b"],
                Some(r#""TiDB""#),
            ),
            (
                r#"{"properties":{"$type":{"$a":{"$b":"TiDB"}}}}"#,
                vec!["$.properties.$type.$a.*[0]"],
                Some(r#"["TiDB"]"#),
            ),
            (
                r#"{"metadata":{"comment":"1234"}}"#,
                vec!["$.metadata.comment"],
                Some(r#""1234""#),
            ),
            (r#"[[0,1],[2,3],[4,[5,6]]]"#, vec!["$[0]"], Some("[0,1]")),
            (
                r#"[[0,1],[2,3],[4,[5,6]]]"#,
                vec!["$[last][last]"],
                Some("[5,6]"),
            ),
            (
                r#"[[0,1],[2,3],[4,[5,6]]]"#,
                vec!["$[last-1][last]"],
                Some("3"),
            ),
            (
                r#"[[0,1],[2,3],[4,[5,6]]]"#,
                vec!["$[last-1][last-1]"],
                Some("2"),
            ),
            (
                r#"[[0,1],[2,3],[4,[5,6]]]"#,
                vec!["$[1 to 2]"],
                Some("[[2,3],[4,[5,6]]]"),
            ),
            (
                r#"[[0,1],[2,3],[4,[5,6]]]"#,
                vec!["$[1 to 2][1 to 2]"],
                Some("[3,[5,6]]"),
            ),
            (
                r#"[[0,1],[2,3],[4,[5,6]]]"#,
                vec!["$[1 to last][1 to last]"],
                Some("[3,[5,6]]"),
            ),
            (
                r#"[[0,1],[2,3],[4,[5,6]]]"#,
                vec!["$[1 to last][1 to last-1]"],
                None,
            ),
            (
                r#"[[0,1],[2,3],[4,[5,6]]]"#,
                vec!["$[1 to last][0 to last-1]"],
                Some("[2,4]"),
            ),
            (
                r#"{"\"hello\"":"world","a":[1,"2",{"aa":"bb"},4.0,{"aa":"cc"}],"b":true,"c":["d"]}"#,
                vec!["$.a", "$[5]"],
                Some(r#"[[1,"2",{"aa":"bb"},4.0,{"aa":"cc"}]]"#),
            ),
            (
                r#"[{"a":1,"b":true},3,3.5,"hello, world",null,true]"#,
                vec!["$.a", "$[0]"],
                Some(r#"[{"a":1,"b":true}]"#),
            ),
            (
                r#"{"properties":{"$type":{"$a$a":"TiDB"}},"hello":{"$b$b":"world","$c":"amazing"}}"#,
                vec!["$.properties", "$[1]"],
                Some(r#"[{"$type":{"$a$a":"TiDB"}}]"#),
            ),
            (
                r#"{"properties":{"$type":{"$a$a":"TiDB"}},"hello":{"$b$b":"world","$c":"amazing"}}"#,
                vec!["$.hello", "$[2]"],
                Some(r#"[{"$b$b":"world","$c":"amazing"}]"#),
            ),
            (
                r#"{"metadata":{"age":19,"name":"Tom"}}"#,
                vec!["$.metadata.age", "$.metadata.name"],
                Some(r#"[19,"Tom"]"#),
            ),
            (
                r#"{"a":{"x":{"b":{"y":{"b":{"z":{"c":100}}}}}}}"#,
                vec!["$.a**.b**.c"],
                Some("[100]"),
            ),
            (
                r#"{"a":{"b":[1,2,3]}}"#,
                vec!["$**[0]"],
                Some(r#"[{"a":{"b":[1,2,3]}},{"b":[1,2,3]},1,2,3]"#),
            ),
            (
                r#"[[0,1],[2,3],[4,[5,6]]]"#,
                vec!["$**[0]"],
                Some("[[0,1],0,1,2,3,4,5,6]"),
            ),
            ("[1]", vec!["$**[0]"], Some("[1]")),
        ];

        for (document, paths, expected) in cases {
            let paths = paths
                .into_iter()
                .map(|path| parse_json_path_expr(path).unwrap())
                .collect::<Vec<_>>();
            let actual = json(document).extract(&paths).unwrap();
            assert_eq!(
                actual.as_ref().map(ToString::to_string),
                expected.map(|expected| json(expected).to_string()),
                "{document} {paths:?}"
            );
        }
    }

    #[test]
    fn test_binary_json_type_unquote_keys_and_depth() {
        for (input, expected) in [
            (r#"{"a":"b"}"#, "OBJECT"),
            (r#"["a","b"]"#, "ARRAY"),
            ("3", "INTEGER"),
            ("3.0", "DOUBLE"),
            ("null", "NULL"),
            ("true", "BOOLEAN"),
        ] {
            assert_eq!(json(input).type_name().unwrap(), expected);
        }
        for (input, expected) in [
            ("3", "3"),
            (r#""3""#, "3"),
            (
                r#""[{\"x\":\"{\\\"y\\\":12}\"}]""#,
                r#"[{"x":"{\"y\":12}"}]"#,
            ),
            (
                r#""hello, \"escaped quotes\" world""#,
                r#"hello, "escaped quotes" world"#,
            ),
            (r#""\u4f60""#, "你"),
            ("true", "true"),
            ("null", "null"),
            (r#"{"a":[1,2]}"#, r#"{"a": [1, 2]}"#),
            (r#""'""#, "'"),
            (r#""''""#, "''"),
            (r#""""#, ""),
        ] {
            assert_eq!(json(input).unquote().unwrap(), expected, "{input}");
        }
        assert_eq!(
            json(r#"{"name":"Tom","age":19}"#)
                .keys()
                .unwrap()
                .to_string(),
            r#"["age", "name"]"#
        );
        let long_key = format!("{{\"{}\":1}}", "a".repeat(65_536));
        assert_eq!(
            BinaryJSON::parse(&long_key).unwrap_err().to_string(),
            "[types:8129]TiDB does not yet support JSON objects with the key length >= 65536"
        );
        for (input, expected) in [
            ("{}", 1),
            ("[]", 1),
            ("true", 1),
            ("[10,20]", 2),
            ("[[],{}]", 2),
            (r#"[10,{"a":20}]"#, 3),
            (
                r#"{"Person":{"Name":"Homer","Age":39,"Hobbies":["Eating","Sleeping"]}}"#,
                4,
            ),
        ] {
            assert_eq!(json(input).element_depth().unwrap(), expected, "{input}");
        }
        assert_eq!(json(r#"{"a":1,"b":2}"#).element_count().unwrap(), 2);
        assert_eq!(
            BinaryJSON::from_typed_value(&crate::BinaryJSONValue::Uint64(1_u64 << 63))
                .unwrap()
                .type_name()
                .unwrap(),
            "UNSIGNED INTEGER"
        );

        for source in [
            r#"{"a":[1,"2",{"aa":"bb"},4,null],"b":true,"c":null}"#,
            r#"{"aaaaaaaaaaa":[1,"2",{"aa":"bb"},4.1],"bbbbbbbbbb":true,"ccccccccc":"d"}"#,
            r#"[{"a":1,"b":true},3,3.5,"hello, world",null,true]"#,
        ] {
            let value = json(source);
            assert_eq!(value.clone().to_string(), value.to_string());
        }
    }

    #[test]
    fn test_binary_json_merge_and_contains_source_rows() {
        for (inputs, expected) in [
            (vec![r#"{"a":1}"#, r#"{"b":2}"#], r#"{"a":1,"b":2}"#),
            (vec![r#"{"a":1}"#, r#"{"a":2}"#], r#"{"a":[1,2]}"#),
            (vec!["[1]", "[2]"], "[1,2]"),
            (vec![r#"{"a":1}"#, "[1]"], r#"[{"a":1},1]"#),
            (vec!["[1]", r#"{"a":1}"#], r#"[1,{"a":1}]"#),
            (vec![r#"{"a":1}"#, "4"], r#"[{"a":1},4]"#),
            (vec!["[1]", "4"], "[1,4]"),
            (vec!["4", r#"{"a":1}"#], r#"[4,{"a":1}]"#),
            (vec!["4", "1"], "[4,1]"),
            (vec!["{}", "[]"], "[{}]"),
            (
                vec![r#"{"comment":"1234"}"#, r#"{"age":19,"name":"Tom"}"#],
                r#"{"age":19,"comment":"1234","name":"Tom"}"#,
            ),
            (
                vec![
                    r#"{"metadata":{"comment":"1234"}}"#,
                    r#"{"metadata":{"age":19,"name":"Tom"}}"#,
                ],
                r#"{"metadata":{"age":19,"comment":"1234","name":"Tom"}}"#,
            ),
            (
                vec![r#"{"comment":"1234"}"#, r#"{"comment":"abc"}"#],
                r#"{"comment":["1234","abc"]}"#,
            ),
        ] {
            let inputs = inputs.into_iter().map(json).collect::<Vec<_>>();
            assert_eq!(
                merge_binary_json(&inputs).unwrap().to_string(),
                json(expected).to_string()
            );
        }

        for (object, target, expected) in [
            ("{}", "{}", true),
            (r#"{"a":1}"#, "{}", true),
            (r#"{"a":1}"#, "1", false),
            (r#"{"a":[1]}"#, "[1]", false),
            (r#"{"b":2,"c":3}"#, r#"{"c":3}"#, true),
            ("1", "1", true),
            ("[1]", "1", true),
            ("[1,2]", "[1]", true),
            ("[1,2]", "[1,3]", false),
            ("[1,2]", r#"["1"]"#, false),
            (r#"[1,2,[1,3]]"#, "[1,3]", true),
            (r#"[1,2,[1,[5,[3]]]]"#, "[1,3]", true),
            (r#"[1,2,[1,[5,{"a":[2,3]}]]]"#, r#"[1,{"a":[3]}]"#, true),
            (r#"[{"a":1}]"#, r#"{"a":1}"#, true),
            (r#"[{"a":1,"b":2}]"#, r#"{"a":1}"#, true),
            (r#"[{"a":{"a":1},"b":2}]"#, r#"{"a":1}"#, false),
        ] {
            assert_eq!(
                contains_binary_json(&json(object), &json(target)).unwrap(),
                expected,
                "{object} contains {target}"
            );
        }

        let target = json(
            r#"{"title":"Goodbye!","author":{"givenName":"John","familyName":"Doe"},"tags":["example","sample"],"content":"This will be unchanged"}"#,
        );
        let patch = json(
            r#"{"title":"Hello!","phoneNumber":"+01-123-456-7890","author":{"familyName":null},"tags":["example"]}"#,
        );
        assert_eq!(
            merge_patch_binary_json(&[target, patch])
                .unwrap()
                .unwrap()
                .to_string(),
            json(
                r#"{"title":"Hello!","phoneNumber":"+01-123-456-7890","author":{"givenName":"John"},"tags":["example"],"content":"This will be unchanged"}"#
            )
            .to_string()
        );

        assert!(overlaps_binary_json(&json("[1,2]"), &json("[2,3]")).unwrap());
        assert!(!overlaps_binary_json(&json("[1,2]"), &json("[3,4]")).unwrap());
    }

    #[test]
    fn test_binary_json_modify_and_remove_source_rows() {
        for (base, path, value, expected, mode) in [
            ("null", "$", "{}", "{}", JSONModifyType::Set),
            ("{}", "$.a", "3", r#"{"a":3}"#, JSONModifyType::Set),
            (
                r#"{"a":3}"#,
                "$.a",
                "[]",
                r#"{"a":[]}"#,
                JSONModifyType::Replace,
            ),
            (
                r#"{"a":3}"#,
                "$.b",
                r#""3""#,
                r#"{"a":3,"b":"3"}"#,
                JSONModifyType::Set,
            ),
            (
                r#"{"a":[]}"#,
                "$.a[0]",
                "3",
                r#"{"a":[3]}"#,
                JSONModifyType::Set,
            ),
            (
                r#"{"a":[3]}"#,
                "$.a[1]",
                "4",
                r#"{"a":[3,4]}"#,
                JSONModifyType::Insert,
            ),
            (r#"{"a":[3]}"#, "$[0]", "4", "4", JSONModifyType::Set),
            (
                r#"{"a":[3]}"#,
                "$[1]",
                "4",
                r#"[{"a":[3]},4]"#,
                JSONModifyType::Set,
            ),
            (
                r#"{"b":true}"#,
                "$.b",
                "false",
                r#"{"b":false}"#,
                JSONModifyType::Set,
            ),
            (
                r#"{"foo":"bar"}"#,
                "$.foo",
                r#""moo""#,
                r#"{"foo":"bar"}"#,
                JSONModifyType::Insert,
            ),
            (
                r#"{"foo":"bar"}"#,
                "$.foo",
                r#""moo""#,
                r#"{"foo":"moo"}"#,
                JSONModifyType::Replace,
            ),
            (
                r#"{"foo":"bar"}"#,
                "$.foo",
                r#""moo""#,
                r#"{"foo":"moo"}"#,
                JSONModifyType::Set,
            ),
            (
                r#"{"foo":"bar"}"#,
                "$.foo",
                "null",
                r#"{"foo":null}"#,
                JSONModifyType::Set,
            ),
            (
                r#"{"foo":"bar"}"#,
                "$.baz",
                r#""moo""#,
                r#"{"foo":"bar","baz":"moo"}"#,
                JSONModifyType::Insert,
            ),
            (
                r#"{"foo":"bar"}"#,
                "$.baz",
                r#""moo""#,
                r#"{"foo":"bar"}"#,
                JSONModifyType::Replace,
            ),
            (
                r#"{"foo":"bar"}"#,
                "$.baz",
                r#""moo""#,
                r#"{"foo":"bar","baz":"moo"}"#,
                JSONModifyType::Set,
            ),
            (
                r#"{"foo":"bar"}"#,
                "$.baz",
                "null",
                r#"{"foo":"bar","baz":null}"#,
                JSONModifyType::Set,
            ),
            ("{}", "$", "1", "{}", JSONModifyType::Insert),
            (
                r#"{"a":[3,4]}"#,
                "$.b[1]",
                "3",
                r#"{"a":[3,4]}"#,
                JSONModifyType::Set,
            ),
            (
                r#"{"a":[3,4]}"#,
                "$.a[0]",
                "30",
                r#"{"a":[3,4]}"#,
                JSONModifyType::Insert,
            ),
            (
                r#"{"a":[3,4]}"#,
                "$.a[2]",
                "30",
                r#"{"a":[3,4]}"#,
                JSONModifyType::Replace,
            ),
            (
                r#"{"a":[3,4]}"#,
                "$.a[2].b",
                "3",
                r#"{"a":[3,4]}"#,
                JSONModifyType::Set,
            ),
        ] {
            let actual = json(base)
                .modify(&[parse_json_path_expr(path).unwrap()], &[json(value)], mode)
                .unwrap();
            assert_eq!(
                actual.to_string(),
                json(expected).to_string(),
                "{base} {path}"
            );
        }

        for (base, path, expected) in [
            ("{}", "$.a", "{}"),
            (r#"{"a":3}"#, "$.a", "{}"),
            (r#"{"a":1,"b":2,"c":3}"#, "$.b", r#"{"a":1,"c":3}"#),
            (r#"{"a":1,"b":2,"c":3}"#, "$.d", r#"{"a":1,"b":2,"c":3}"#),
            (r#"{"a":3}"#, "$[0]", r#"{"a":3}"#),
            (r#"{"a":[3,4,5]}"#, "$.a[0]", r#"{"a":[4,5]}"#),
            (r#"{"a":[3,4,5]}"#, "$.a[1]", r#"{"a":[3,5]}"#),
            (r#"{"a":[3,4,5]}"#, "$.a[4]", r#"{"a":[3,4,5]}"#),
            (
                r#"{"a":[1,2,{"aa":"xx"}]}"#,
                "$.a[2].aa",
                r#"{"a":[1,2,{}]}"#,
            ),
        ] {
            let actual = json(base)
                .remove(&[parse_json_path_expr(path).unwrap()])
                .unwrap();
            assert_eq!(
                actual.to_string(),
                json(expected).to_string(),
                "{base} {path}"
            );
        }
        assert!(json("null")
            .remove(&[parse_json_path_expr("$").unwrap()])
            .is_err());
        for path in ["$.*", "$[*]", "$**.a", "$**[3]"] {
            assert!(json("null")
                .modify(
                    &[parse_json_path_expr(path).unwrap()],
                    &[json("{}")],
                    JSONModifyType::Set
                )
                .is_err());
        }
        assert!(json(r#"{"a":[3]}"#)
            .remove(&[parse_json_path_expr("$.a[*]").unwrap()])
            .is_err());

        assert_eq!(
            json(r#"{"a":[1,3]}"#)
                .array_insert(&parse_json_path_expr("$.a[1]").unwrap(), &json("2"))
                .unwrap()
                .to_string(),
            r#"{"a": [1, 2, 3]}"#
        );
        assert_eq!(
            json("[1]")
                .array_insert(&parse_json_path_expr("$[9]").unwrap(), &json("2"))
                .unwrap()
                .to_string(),
            "[1, 2]"
        );
    }

    #[test]
    fn test_binary_json_peek_and_hash_source_contract() {
        for document in [
            "null",
            "true",
            "1",
            "1.5",
            r#""hello""#,
            "[1,2]",
            r#"{"a":1}"#,
        ] {
            let value = json(document);
            let encoded = value.encoded();
            assert_eq!(peek_binary_json_len(&encoded).unwrap(), encoded.len());
            let mut with_suffix = encoded.clone();
            with_suffix.extend_from_slice(b"suffix");
            assert_eq!(peek_binary_json_len(&with_suffix).unwrap(), encoded.len());
        }
        assert!(peek_binary_json_len(&[]).is_err());
        assert!(peek_binary_json_len(b"\\bfnrtuz0").is_err());

        let values = [
            json("[]"),
            json("[[]]"),
            json("[[[]]]"),
            json("{}"),
            json("[false]"),
            json("[true]"),
            json("[null]"),
        ];
        let hashes = values
            .iter()
            .map(BinaryJSON::hash_value)
            .collect::<Result<HashSet<_>, _>>()
            .unwrap();
        assert_eq!(hashes.len(), values.len());
        assert_eq!(
            json("3").hash_value().unwrap(),
            json("3.0").hash_value().unwrap()
        );
        assert_ne!(
            BinaryJSON::from_value(&Value::Number(u64::MAX.into()))
                .unwrap()
                .hash_value()
                .unwrap(),
            BinaryJSON::from_value(&Value::Number(
                serde_json::Number::from_f64(u64::MAX as f64).unwrap(),
            ))
            .unwrap()
            .hash_value()
            .unwrap()
        );
        assert_eq!(
            BinaryJSON::from_value(&Value::Number((1_u64 << 62).into()))
                .unwrap()
                .hash_value()
                .unwrap(),
            BinaryJSON::from_value(&Value::Number(
                serde_json::Number::from_f64((1_u64 << 62) as f64).unwrap()
            ))
            .unwrap()
            .hash_value()
            .unwrap()
        );
    }

    #[test]
    fn test_binary_json_walk_and_search_source_rows() {
        let callback_document = json(
            r#"{"\"hello\"":"world","a":[1,"2",{"aa":"bb"},4.0,{"aa":"cc"}],"b":true,"c":["d"]}"#,
        );
        let callback_array = json(r#"[{"a":1,"b":true},3,3.5,"hello, world",null,true]"#);
        for (document, path, expected) in [
            (
                &callback_document,
                "$.a",
                vec![("$.a", r#"[1,"2",{"aa":"bb"},4.0,{"aa":"cc"}]"#)],
            ),
            (&callback_array, "$.a", vec![]),
            (&callback_document, "$[0]", vec![]),
            (
                &callback_array,
                "$[0]",
                vec![("$[0]", r#"{"a":1,"b":true}"#)],
            ),
            (
                &callback_document,
                "$.a[2].aa",
                vec![("$.a[2].aa", r#""bb""#)],
            ),
            (
                &callback_document,
                "$.a[*].aa",
                vec![("$.a[2].aa", r#""bb""#), ("$.a[4].aa", r#""cc""#)],
            ),
            (
                &callback_document,
                "$.*[0]",
                vec![("$.a[0]", "1"), ("$.c[0]", r#""d""#)],
            ),
            (
                &callback_document,
                r#"$.a[*]."aa""#,
                vec![("$.a[2].aa", r#""bb""#), ("$.a[4].aa", r#""cc""#)],
            ),
            (
                &callback_document,
                r#"$."\"hello\"""#,
                vec![(r#"$."\"hello\"""#, r#""world""#)],
            ),
            (&callback_document, "$**[1]", vec![("$.a[1]", r#""2""#)]),
        ] {
            let actual = document
                .extract_matches(&parse_json_path_expr(path).unwrap())
                .unwrap();
            assert_eq!(actual.len(), expected.len(), "{path}");
            for ((actual_path, actual_value), (expected_path, expected_value)) in
                actual.iter().zip(expected)
            {
                assert_eq!(actual_path.to_string(), expected_path, "{path}");
                assert_eq!(
                    actual_value.to_string(),
                    json(expected_value).to_string(),
                    "{path}"
                );
            }
        }

        let document = json(r#"["abc",[{"k":"10"},"def"],{"x":"abc"},{"y":"bcd"}]"#);
        let expected = [
            ("$", r#"["abc",[{"k":"10"},"def"],{"x":"abc"},{"y":"bcd"}]"#),
            ("$[0]", r#""abc""#),
            ("$[1]", r#"[{"k":"10"},"def"]"#),
            ("$[1][0]", r#"{"k":"10"}"#),
            ("$[1][0].k", r#""10""#),
            ("$[1][1]", r#""def""#),
            ("$[2]", r#"{"x":"abc"}"#),
            ("$[2].x", r#""abc""#),
            ("$[3]", r#"{"y":"bcd"}"#),
            ("$[3].y", r#""bcd""#),
        ];
        let walked = document.walk(&[]).unwrap();
        assert_eq!(walked.len(), expected.len());
        for ((path, value), (expected_path, expected_value)) in walked.iter().zip(expected) {
            assert_eq!(path.to_string(), expected_path);
            assert_eq!(value.to_string(), json(expected_value).to_string());
        }

        let subtree = document
            .walk(&[
                parse_json_path_expr("$[1]").unwrap(),
                parse_json_path_expr("$[1]").unwrap(),
            ])
            .unwrap();
        assert_eq!(
            subtree
                .iter()
                .map(|(path, _)| path.to_string())
                .collect::<Vec<_>>(),
            ["$[1]", "$[1][0]", "$[1][0].k", "$[1][1]"]
        );
        assert!(document
            .walk(&[parse_json_path_expr("$.m").unwrap()])
            .unwrap()
            .is_empty());

        assert_eq!(
            document
                .search(JSONSearchMode::One, "abc", '\\', &[])
                .unwrap()
                .unwrap()
                .to_string(),
            r#""$[0]""#
        );
        assert_eq!(
            document
                .search(JSONSearchMode::All, "%bc", '\\', &[])
                .unwrap()
                .unwrap()
                .to_string(),
            r#"["$[0]", "$[2].x"]"#
        );
    }

    #[test]
    fn special_scalars_survive_every_container_operation() {
        let time = BinaryJSON::from_time(
            Time::new(
                CoreTime::from_date(2024, 1, 2, 3, 4, 5, 600_000),
                TimeType::DateTime,
                6,
            )
            .unwrap(),
        );
        let duration =
            BinaryJSON::from_duration(MySqlDuration::new(12, 34, 56, 700_000, 6).unwrap());
        let opaque = BinaryJSON::from_opaque(crate::Opaque {
            type_code: 233,
            bytes: vec![1, 2, 3],
        });
        let document = BinaryJSON::from_node(&JSONNode::Array(vec![
            JSONNode::Scalar(time.clone()),
            JSONNode::Scalar(duration.clone()),
            JSONNode::Scalar(opaque.clone()),
        ]))
        .unwrap();

        assert_eq!(
            document
                .extract(&[parse_json_path_expr("$[0]").unwrap()])
                .unwrap(),
            Some(time.clone())
        );
        let modified = document
            .modify(
                &[parse_json_path_expr("$[1]").unwrap()],
                std::slice::from_ref(&opaque),
                JSONModifyType::Set,
            )
            .unwrap();
        assert_eq!(
            modified
                .extract(&[parse_json_path_expr("$[1]").unwrap()])
                .unwrap(),
            Some(opaque.clone())
        );
        assert_eq!(
            modified
                .extract(&[parse_json_path_expr("$[0]").unwrap()])
                .unwrap(),
            Some(time)
        );
        assert_eq!(
            compare_binary_json(&document, &document.clone()),
            std::cmp::Ordering::Equal
        );
        assert_ne!(
            compare_binary_json(&document, &modified),
            std::cmp::Ordering::Equal
        );
        assert!(contains_binary_json(&document, &duration).unwrap());
        assert!(modified.hash_value().is_ok());
        assert_eq!(modified.walk(&[]).unwrap().len(), 4);

        let merged = merge_binary_json(&[opaque.clone(), duration]).unwrap();
        assert_eq!(
            merged
                .extract(&[parse_json_path_expr("$[0]").unwrap()])
                .unwrap(),
            Some(opaque)
        );
    }

    #[test]
    fn fuzz_json_extract_source_seeds_do_not_produce_invalid_values() {
        for (document, path) in [
            (r#"["abc", 5, 1.234]"#, "$[0]"),
            (r#"{"key": "value"}"#, "$.key"),
            (r#"{"key": "value"}"#, "$.*"),
            (r#"{"key": "value"}"#, "$.**"),
            (r#""abc""#, "$"),
            ("5", "$"),
            ("1.2345", "$"),
        ] {
            let document = BinaryJSON::parse(document).unwrap();
            let Ok(path) = parse_json_path_expr(path) else {
                continue;
            };
            if let Some(extracted) = document.extract(&[path]).unwrap() {
                assert_ne!(extracted.type_code(), 0);
                BinaryJSON::from_raw(extracted.type_code(), extracted.value().to_vec()).unwrap();
            }
        }
    }
}
