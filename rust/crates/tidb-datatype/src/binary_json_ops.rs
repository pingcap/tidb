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

use std::collections::{btree_map::Entry, BTreeMap, HashSet};
use std::ops::Range;

use serde_json::Value;
use smallvec::SmallVec;

mod walk;

use crate::json_path::format_concrete_path;
use walk::{visit_binary, walk_index, walk_indices};

use crate::{
    binary_json::{
        compare_json_refs, decode_node, encode_sorted_object_views, encode_uvarint,
        encode_view_array, fixed_payload_len, uvarint_size, BinaryJSONRef, JSONContainerEdit,
        JSONKeyArray, JSONNode, ValidatedBinaryJSONRef, EMPTY_JSON_CONTAINER, MAX_JSON_DEPTH,
    },
    compare_binary_json, BinaryJSON, BinaryJSONError, EncodedJSONLayout, JSONPathArraySelection,
    JSONPathExpression, JSONPathLeg, JSON_LITERAL_NULL, JSON_TYPE_CODE_ARRAY,
    JSON_TYPE_CODE_FLOAT64, JSON_TYPE_CODE_INT64, JSON_TYPE_CODE_LITERAL, JSON_TYPE_CODE_OBJECT,
    JSON_TYPE_CODE_UINT64,
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

/// A borrowed exact match or an array assembled from the selected values.
#[doc(hidden)]
pub enum EncodedJSONSelection<'a> {
    /// A value already present in the source cell.
    Borrowed {
        /// The selected value's wire type.
        type_code: u8,
        /// The selected value's wire payload.
        value: &'a [u8],
        /// The selection is the original document.
        root: bool,
    },
    /// A packed result assembled by extraction.
    Owned(BinaryJSON),
}

impl EncodedJSONSelection<'_> {
    /// Derives a match layout, if the original cell was already checked.
    #[inline]
    pub fn layout(&self, source: Option<EncodedJSONLayout>) -> Option<EncodedJSONLayout> {
        match self {
            Self::Borrowed { root: true, .. } => source,
            Self::Borrowed { .. } => source
                .filter(|layout| layout.is_canonical())
                .map(|_| EncodedJSONLayout::canonical()),
            Self::Owned(value) => value.known_layout(),
        }
    }
}

impl<'a> EncodedJSONSelection<'a> {
    fn borrowed(root: BinaryJSONRef<'a>, value: BinaryJSONRef<'a>) -> Self {
        Self::Borrowed {
            type_code: value.type_code,
            value: value.value,
            root: value.type_code == root.type_code && std::ptr::eq(value.value, root.value),
        }
    }
}

impl BinaryJSON {
    /// Extracts one or more parsed paths using MySQL autowrap semantics.
    pub fn extract(
        &self,
        paths: &[JSONPathExpression],
    ) -> Result<Option<BinaryJSON>, BinaryJSONError> {
        if let Some(root) = self.canonical_view() {
            if let [path] = paths {
                if !path.requires_walk() {
                    return extract_exact(root, path.legs())
                        .map(|value| value.map(|value| self.share_view(value)));
                }
            }
            return pack_path_matches(root, paths);
        }
        let root = self.to_node()?;
        let mut matches = Vec::new();
        for path in paths {
            extract_value(&root, path.legs(), &mut matches, &mut HashSet::new());
        }
        if matches.is_empty() {
            return Ok(None);
        }
        if paths.len() == 1 && !paths[0].could_match_multiple_values() && matches.len() == 1 {
            return BinaryJSON::from_node(matches.remove(0)).map(Some);
        }
        BinaryJSON::from_node(&JSONNode::Array(matches.into_iter().cloned().collect())).map(Some)
    }

    /// Go SQL extraction reads only the selected path entries.
    pub fn extract_for_sql(&self, paths: &[JSONPathExpression]) -> Result<Option<Self>, BinaryJSONError> {
        Ok(extract_sql_selection(self.view(), paths)?.map(|selection| {
            let layout = selection.layout(self.known_layout());
            match selection {
                EncodedJSONSelection::Borrowed { type_code, value, .. } => {
                    self.share_extracted_view(BinaryJSONRef { type_code, value }, layout)
                }
                EncodedJSONSelection::Owned(value) => value,
            }
        }))
    }

    /// Extracts from a packed SQL cell without scanning unrelated values.
    #[doc(hidden)]
    #[inline]
    pub fn extract_encoded_for_sql<'a>(
        cell: &'a [u8],
        paths: &[JSONPathExpression],
    ) -> Result<Option<EncodedJSONSelection<'a>>, BinaryJSONError> {
        extract_sql_selection(encoded_view(cell)?, paths)
    }

    /// Returns sorted object keys as a JSON array, or an empty array otherwise.
    pub fn keys(&self) -> Result<BinaryJSON, BinaryJSONError> {
        keys_from_view(self.view(), self.canonical_view().is_some())
    }

    /// Go `GetKeys` on the object selected by SQL `JSON_KEYS`.
    /// Only the path and the selected object's key entries are read.
    #[inline]
    pub fn get_keys(&self, path: Option<&JSONPathExpression>) -> Result<Option<Self>, BinaryJSONError> {
        sql_keys(self.view(), path)
    }

    /// Go `GetKeys` on a packed SQL cell, including its leading type code.
    #[doc(hidden)]
    #[inline]
    pub fn get_encoded_keys(
        cell: &[u8],
        path: Option<&JSONPathExpression>,
    ) -> Result<Option<Self>, BinaryJSONError> {
        sql_keys(encoded_view(cell)?, path)
    }

    /// Checks the selected object's key entries before writing a packed result.
    #[doc(hidden)]
    #[inline]
    pub fn get_encoded_key_array<'a>(
        cell: &'a [u8],
        path: Option<&JSONPathExpression>,
    ) -> Result<Option<JSONKeyArray<'a>>, BinaryJSONError> {
        sql_key_array(encoded_view(cell)?, path)
    }

    /// Reads object keys from one packed cell. A supplied layout must
    /// describe these exact bytes, including the type code.
    #[doc(hidden)]
    pub fn encoded_keys(
        cell: &[u8],
        layout: Option<EncodedJSONLayout>,
    ) -> Result<BinaryJSON, BinaryJSONError> {
        let (&type_code, value) = cell.split_first().ok_or(BinaryJSONError::InvalidBinary)?;
        let layout = layout.unwrap_or_else(|| Self::check_encoded_layout(cell));
        keys_from_view(BinaryJSONRef { type_code, value }, layout.is_canonical())
    }

    /// Returns an array/object element count, or one for a scalar.
    pub fn element_count(&self) -> Result<usize, BinaryJSONError> {
        if let Some(value) = self.canonical_view() {
            return count_elements(value);
        }
        Ok(match self.to_node()? {
            JSONNode::Array(values) => values.len(),
            JSONNode::Object(values) => values.len(),
            _ => 1,
        })
    }

    /// Reads the SQL JSON_LENGTH result from the root's type and header.
    #[doc(hidden)]
    #[inline]
    pub fn encoded_root_count(cell: &[u8]) -> Result<usize, BinaryJSONError> {
        let (&type_code, value) = cell.split_first().ok_or(BinaryJSONError::InvalidBinary)?;
        count_elements(BinaryJSONRef { type_code, value })
    }

    /// SQL `JSON_LENGTH` reads the type or container count at an exact path.
    /// `None` means the selected path was absent.
    #[inline]
    pub fn get_length(&self, path: Option<&JSONPathExpression>) -> Result<Option<usize>, BinaryJSONError> {
        sql_length(self.view(), path)
    }

    /// SQL `JSON_LENGTH` on a packed cell with its leading type code.
    #[doc(hidden)]
    #[inline]
    pub fn get_encoded_length(
        cell: &[u8],
        path: Option<&JSONPathExpression>,
    ) -> Result<Option<usize>, BinaryJSONError> {
        sql_length(encoded_view(cell)?, path)
    }

    /// SQL path existence only reads the entries needed to find a match.
    #[inline]
    pub fn path_exists_for_sql(&self, path: &JSONPathExpression) -> Result<bool, BinaryJSONError> {
        sql_path_exists(self.view(), path)
    }

    /// SQL path existence on a packed cell with its leading type code.
    #[doc(hidden)]
    #[inline]
    pub fn encoded_path_exists_for_sql(
        cell: &[u8],
        path: &JSONPathExpression,
    ) -> Result<bool, BinaryJSONError> {
        sql_path_exists(encoded_view(cell)?, path)
    }

    /// Returns one array element by zero-based index.
    pub fn array_get(&self, index: usize) -> Result<Option<BinaryJSON>, BinaryJSONError> {
        if let Some(root) = self.canonical_view() {
            if root.type_code != JSON_TYPE_CODE_ARRAY {
                return Ok(None);
            }
            let array = root.container()?;
            return if index < array.count {
                Ok(Some(self.share_view(array.child(index)?)))
            } else {
                Ok(None)
            };
        }
        let JSONNode::Array(values) = self.to_node()? else {
            return Ok(None);
        };
        values.get(index).map(BinaryJSON::from_node).transpose()
    }

    /// Returns one object member by exact UTF-8 key.
    pub fn object_get(&self, key: &str) -> Result<Option<BinaryJSON>, BinaryJSONError> {
        if let Some(root) = self.canonical_view() {
            if root.type_code != JSON_TYPE_CODE_OBJECT {
                return Ok(None);
            }
            return root.container()?.find(key.as_bytes())
                .map(|value| value.map(|value| self.share_view(value)));
        }
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
        if self.canonical_view().is_some()
            && values.iter().all(|value| value.canonical_view().is_some())
        {
            if paths
                .iter()
                .any(|path| path.contains_any_asterisk() || path.contains_any_range())
            {
                return Err(BinaryJSONError::InvalidPath);
            }
            let mut edited = None;
            for (path, value) in paths.iter().zip(values) {
                let document = edited.as_ref().unwrap_or(self);
                if let Some(rewritten) =
                    edit_binary(document.view(), path.legs(), PathEdit::Modify(mode, value))?
                {
                    edited = Some(rewritten);
                }
            }
            return match edited {
                Some(value) => value.finish_edit(true),
                None => Ok(self.clone()),
            };
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
        let Some((JSONPathLeg::Array(JSONPathArraySelection::Index(index)), parent_legs)) =
            path.legs().split_last()
        else {
            return Err(BinaryJSONError::InvalidPath);
        };
        if let (Some(root), Some(replacement)) = (self.canonical_view(), value.canonical_view()) {
            let mut ancestors = SmallVec::<[_; 8]>::new();
            let Some(parent) = extract_exact_with(root, parent_legs, |value, index| {
                ancestors.push((value, index));
            })?
            else {
                return Ok(self.clone());
            };
            if parent.type_code != JSON_TYPE_CODE_ARRAY {
                return Ok(self.clone());
            }
            let array = parent.container()?;
            let Some(index) = array_insert_index(*index, array.count) else {
                return Ok(self.clone());
            };
            return array
                .rewrite(
                    JSON_TYPE_CODE_ARRAY,
                    JSONContainerEdit::Insert {
                        index,
                        key: &[],
                        value: replacement,
                    },
                    &ancestors,
                )?
                .finish_edit(true);
        }

        let (parent, _) = path.pop_last().expect("array insertion path is nonempty");
        let Some(parent_value) = self.extract(std::slice::from_ref(&parent))? else {
            return Ok(self.clone());
        };
        let JSONNode::Array(mut array) = parent_value.to_node()? else {
            return Ok(self.clone());
        };
        let Some(index) = array_insert_index(*index, array.len()) else {
            return Ok(self.clone());
        };
        array.insert(index, value.to_node()?);
        let new_array = BinaryJSON::from_node(&JSONNode::Array(array))?;
        self.modify(&[parent], &[new_array], JSONModifyType::Set)
    }

    /// Removes exact paths from left to right.
    pub fn remove(&self, paths: &[JSONPathExpression]) -> Result<BinaryJSON, BinaryJSONError> {
        if self.canonical_view().is_some() {
            let mut edited = None;
            for path in paths {
                validate_remove_path(path)?;
                let document = edited.as_ref().unwrap_or(self);
                if let Some(rewritten) =
                    edit_binary(document.view(), path.legs(), PathEdit::Remove)?
                {
                    edited = Some(rewritten);
                }
            }
            return match edited {
                Some(value) => value.finish_edit(false),
                None => Ok(self.clone()),
            };
        }
        let mut document = self.to_node()?;
        for path in paths {
            validate_remove_path(path)?;
            remove_node(&mut document, path.legs());
        }
        BinaryJSON::from_node(&document)
    }

    /// Builds TiDB's equality hash representation.
    pub fn hash_value(&self) -> Result<Vec<u8>, BinaryJSONError> {
        let mut output = Vec::new();
        self.append_hash_value(&mut output)?;
        Ok(output)
    }

    /// Appends TiDB's equality hash representation into a reusable buffer.
    /// Leaves the buffer's contents intact when the value cannot be hashed.
    pub fn append_hash_value(&self, output: &mut Vec<u8>) -> Result<(), BinaryJSONError> {
        let start = output.len();
        let result = if let Some(value) = self.canonical_view() {
            append_hash_ref(value, output)
        } else {
            append_hash_value_fallback(self, output)
        };
        if result.is_err() {
            output.truncate(start);
        }
        result
    }

    /// Returns the exact number of bytes produced by [`BinaryJSON::hash_value`].
    #[inline]
    pub fn hash_value_size(&self) -> Result<usize, BinaryJSONError> {
        if let Some(value) = self.canonical_view() {
            hash_ref_size(value)
        } else {
            self.hash_value().map(|value| value.len())
        }
    }

    /// Walks selected subtrees in source preorder and reports each full path once.
    pub fn walk(
        &self,
        paths: &[JSONPathExpression],
    ) -> Result<Vec<(JSONPathExpression, BinaryJSON)>, BinaryJSONError> {
        if let Some(root) = self.canonical_view() {
            let mut output = Vec::new();
            visit_binary(root, paths, true, |path, value| {
                output.push((
                    JSONPathExpression::from_locations(path),
                    self.share_view(value),
                ));
                false
            })?;
            return Ok(output);
        }
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
        if let Some(root) = self.canonical_view() {
            let mut output = Vec::new();
            visit_binary(root, std::slice::from_ref(path), false, |path, value| {
                output.push((
                    JSONPathExpression::from_locations(path),
                    self.share_view(value),
                ));
                false
            })?;
            return Ok(output);
        }
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
        let pattern = SearchPattern::compile(pattern, escape);
        let mut matches = Vec::new();
        if let Some(root) = self.canonical_view() {
            visit_binary(root, paths, true, |path, value| {
                if value
                    .string_bytes()
                    .is_some_and(|text| pattern.is_match(text))
                {
                    matches.push(Value::String(format_concrete_path(path)));
                    return mode == JSONSearchMode::One;
                }
                false
            })?;
        } else {
            for (path, value) in self.walk(paths)? {
                if value.as_string().is_some_and(|text| pattern.is_match(text)) {
                    matches.push(Value::String(path.to_string()));
                    if mode == JSONSearchMode::One {
                        break;
                    }
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

enum SearchPattern<'a> {
    Literal(&'a [u8]),
    Wildcard(Vec<char>, Vec<tidb_stringutil::PatternType>),
    UnicodeEscape(&'a str, char),
}

impl<'a> SearchPattern<'a> {
    fn compile(pattern: &'a str, escape: char) -> Self {
        if !pattern.contains(['%', '_', escape]) {
            return Self::Literal(pattern.as_bytes());
        }
        let Ok(byte) = u8::try_from(u32::from(escape)) else {
            // Rust callers can pass escapes beyond the Go byte domain.
            return Self::UnicodeEscape(pattern, escape);
        };
        let (weights, kinds) = tidb_stringutil::compile_pattern(pattern, byte);
        Self::Wildcard(weights, kinds)
    }

    fn is_match(&self, text: &[u8]) -> bool {
        match self {
            Self::Literal(pattern) => text == *pattern,
            Self::Wildcard(weights, kinds) => tidb_stringutil::do_match(text, weights, kinds),
            Self::UnicodeEscape(pattern, escape) => {
                std::str::from_utf8(text).is_ok_and(|text| like_matches(text, pattern, *escape))
            }
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
    if let (Some(object), Some(target)) = (object.canonical_view(), target.canonical_view()) {
        return contains_ref(object, target);
    }
    contains_node(&object.to_node()?, &target.to_node()?)
}

/// Implements MySQL JSON_OVERLAPS structural overlap.
pub fn overlaps_binary_json(
    object: &BinaryJSON,
    target: &BinaryJSON,
) -> Result<bool, BinaryJSONError> {
    if let (Some(object), Some(target)) = (object.canonical_view(), target.canonical_view()) {
        return overlaps_ref(object, target);
    }
    overlaps_node(&object.to_node()?, &target.to_node()?)
}

/// Implements MySQL JSON_MERGE_PRESERVE.
pub fn merge_binary_json(values: &[BinaryJSON]) -> Result<BinaryJSON, BinaryJSONError> {
    let views: Option<SmallVec<[BinaryJSONRef<'_>; 4]>> =
        values.iter().map(BinaryJSON::canonical_view).collect();
    if let Some(views) = views {
        if let [value] = values {
            if value.type_code() != JSON_TYPE_CODE_OBJECT {
                return Ok(value.clone());
            }
        }
        let merged = merge_preserve_refs(&views)?;
        // Flattening arrays cannot add a nesting level. Merging other
        // values can, so retain the encoder's output depth check there.
        if views.iter().all(|view| view.type_code == JSON_TYPE_CODE_ARRAY) {
            return Ok(merged);
        }
        return merged.validate_layout();
    }
    let values = values
        .iter()
        .map(BinaryJSON::to_node)
        .collect::<Result<Vec<_>, _>>()?;
    let merged = merge_binary_nodes(&values);
    BinaryJSON::from_node(&merged)
}

/// Go `MergePatchBinaryJSON` (RFC 7396), whose `nil` arguments are SQL
/// NULL: the merge starts at the last argument that is NULL or not an
/// object; a NULL patch makes the result NULL, and so does an object patch
/// over a NULL target.
pub fn merge_patch_binary_json(
    values: &[Option<BinaryJSON>],
) -> Result<Option<BinaryJSON>, BinaryJSONError> {
    let start = values
        .iter()
        .rposition(|value| {
            value
                .as_ref()
                .is_none_or(|value| value.type_code() != crate::JSON_TYPE_CODE_OBJECT)
        })
        .unwrap_or(0);
    let Some((first, patches)) = values[start..].split_first() else {
        return Ok(None);
    };
    // A SQL NULL starting the active suffix overrides the following objects.
    let Some(first) = first else {
        return Ok(None);
    };
    if first.canonical_view().is_some()
        && patches.iter().all(|value| {
            value
                .as_ref()
                .is_some_and(|value| value.canonical_view().is_some())
        })
    {
        let mut target = first.clone();
        for patch in patches.iter().flatten() {
            target = merge_object_pair(Some(target.view()), patch.view(), ObjectMerge::Patch)?;
        }
        return Ok(Some(target));
    }

    let mut target = Some(first.to_node()?);
    for patch in patches {
        target = match (target, patch) {
            (_, None) => None,
            (None, Some(patch)) if patch.type_code() == crate::JSON_TYPE_CODE_OBJECT => None,
            (target, Some(patch)) => Some(merge_patch_node(
                target.unwrap_or_else(|| {
                    JSONNode::Scalar(BinaryJSON::parse("null").expect("static JSON null"))
                }),
                patch.to_node()?,
            )),
        };
    }
    target.map(|node| BinaryJSON::from_node(&node)).transpose()
}

enum MergeValue<'a> {
    Borrowed(BinaryJSONRef<'a>),
    Owned(BinaryJSON),
}

impl MergeValue<'_> {
    fn view(&self) -> BinaryJSONRef<'_> {
        match self {
            Self::Borrowed(value) => *value,
            Self::Owned(value) => value.view(),
        }
    }
}

#[derive(Clone, Copy)]
enum ObjectMerge {
    Patch,
    Preserve,
}

#[inline]
fn sql_keys(
    root: BinaryJSONRef<'_>,
    path: Option<&JSONPathExpression>,
) -> Result<Option<BinaryJSON>, BinaryJSONError> {
    if root.type_code == JSON_TYPE_CODE_OBJECT && root.value == EMPTY_JSON_CONTAINER
        && path.is_none_or(|path| path.legs().is_empty())
    {
        return Ok(Some(BinaryJSON::empty_array()));
    }
    sql_key_array(root, path).map(|keys| keys.map(JSONKeyArray::into_json))
}

fn sql_key_array<'a>(
    root: BinaryJSONRef<'a>,
    path: Option<&JSONPathExpression>,
) -> Result<Option<JSONKeyArray<'a>>, BinaryJSONError> {
    let object = match path {
        Some(path) => select_key_object(root, path)?,
        None => Some(root),
    };
    object.filter(|value| value.type_code == JSON_TYPE_CODE_OBJECT)
        .map(|value| value.container()?.checked_key_array::<true>())
        .transpose()
}

fn select_key_object<'a>(
    root: BinaryJSONRef<'a>,
    path: &JSONPathExpression,
) -> Result<Option<BinaryJSONRef<'a>>, BinaryJSONError> {
    if !path.requires_walk() {
        return extract_exact(root, path.legs());
    }
    match extract_sql_selection(root, std::slice::from_ref(path))? {
        Some(EncodedJSONSelection::Borrowed { type_code, value, .. }) => {
            Ok(Some(BinaryJSONRef { type_code, value }))
        }
        _ => Ok(None), // Wrapped matches form an array, which has no object keys.
    }
}

fn keys_from_view(root: BinaryJSONRef<'_>, canonical: bool) -> Result<BinaryJSON, BinaryJSONError> {
    if canonical {
        return if root.type_code == JSON_TYPE_CODE_OBJECT {
            root.container()?.encode_keys()
        } else {
            Ok(BinaryJSON::empty_array())
        };
    }
    let keys = match decode_node(root, 0)? {
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

fn object_entries<'a>(
    value: Option<BinaryJSONRef<'a>>,
) -> Result<impl ExactSizeIterator<Item = (&'a [u8], BinaryJSONRef<'a>)> + Clone, BinaryJSONError> {
    let object = value
        .filter(|value| value.type_code == JSON_TYPE_CODE_OBJECT)
        .map(BinaryJSONRef::container)
        .transpose()?;
    let count = object.map_or(0, |object| object.count);
    Ok((0..count).map(move |i| {
        let object = object.expect("entry belongs to a validated object");
        (
            object.key(i).expect("validated object key"),
            object.child(i).expect("validated object value"),
        )
    }))
}

fn merge_object_pair(
    target: Option<BinaryJSONRef<'_>>,
    patch: BinaryJSONRef<'_>,
    mode: ObjectMerge,
) -> Result<BinaryJSON, BinaryJSONError> {
    let target = object_entries(target)?;
    let patch = object_entries(Some(patch))?;
    let mut entries =
        SmallVec::<[(&[u8], MergeValue<'_>); 8]>::with_capacity(target.len() + patch.len());
    let mut target = target.peekable();
    for (key, value) in patch {
        while target.peek().is_some_and(|(other, _)| *other < key) {
            let (key, value) = target.next().expect("peeked target entry");
            entries.push((key, MergeValue::Borrowed(value)));
        }
        let old = target
            .next_if(|(other, _)| *other == key)
            .map(|(_, value)| value);
        let value = match mode {
            ObjectMerge::Patch => {
                if value.type_code == JSON_TYPE_CODE_LITERAL && value.value == [JSON_LITERAL_NULL] {
                    continue;
                }
                if value.type_code == JSON_TYPE_CODE_OBJECT {
                    MergeValue::Owned(merge_object_pair(old, value, ObjectMerge::Patch)?)
                } else {
                    MergeValue::Borrowed(value)
                }
            }
            ObjectMerge::Preserve => match old {
                Some(old) => MergeValue::Owned(merge_preserve_refs(&[old, value])?),
                None => MergeValue::Borrowed(value),
            },
        };
        entries.push((key, value));
    }
    entries.extend(target.map(|(key, value)| (key, MergeValue::Borrowed(value))));
    encode_sorted_object_views(entries.iter().map(|(key, value)| (*key, value.view())))
}

fn merge_preserve_objects(objects: &[BinaryJSONRef<'_>]) -> Result<BinaryJSON, BinaryJSONError> {
    match objects {
        [object] => return encode_sorted_object_views(object_entries(Some(*object))?),
        [left, right] => return merge_object_pair(Some(*left), *right, ObjectMerge::Preserve),
        [left, middle, right] => {
            let merged = merge_object_pair(Some(*left), *middle, ObjectMerge::Preserve)?;
            return merge_object_pair(Some(merged.view()), *right, ObjectMerge::Preserve);
        }
        _ => {}
    }
    let mut entries: BTreeMap<&[u8], MergeValue<'_>> = BTreeMap::new();
    for &object in objects {
        for (key, value) in object_entries(Some(object))? {
            match entries.entry(key) {
                Entry::Occupied(mut entry) => {
                    let merged = merge_preserve_refs(&[entry.get().view(), value])?;
                    entry.insert(MergeValue::Owned(merged));
                }
                Entry::Vacant(entry) => {
                    entry.insert(MergeValue::Borrowed(value));
                }
            }
        }
    }
    encode_sorted_object_views(entries.iter().map(|(key, value)| (*key, value.view())))
}

fn merge_preserve_refs(values: &[BinaryJSONRef<'_>]) -> Result<BinaryJSON, BinaryJSONError> {
    if values.len() == 2
        && values.iter().all(|value| {
            !matches!(value.type_code, JSON_TYPE_CODE_ARRAY | JSON_TYPE_CODE_OBJECT)
        })
    {
        return encode_view_array(values.iter().copied());
    }
    let mut groups = SmallVec::<[MergeValue<'_>; 4]>::new();
    let mut remain = values;
    while let Some((&first, tail)) = remain.split_first() {
        if first.type_code == JSON_TYPE_CODE_OBJECT {
            let count = 1 + tail
                .iter()
                .take_while(|value| value.type_code == JSON_TYPE_CODE_OBJECT)
                .count();
            groups.push(MergeValue::Owned(merge_preserve_objects(&remain[..count])?));
            remain = &remain[count..];
        } else {
            groups.push(MergeValue::Borrowed(first));
            remain = tail;
        }
    }
    if groups.len() == 1 {
        return Ok(match groups.pop().expect("one merge group") {
            MergeValue::Borrowed(value) => value.copy(),
            MergeValue::Owned(value) => value,
        });
    }
    let mut elements = SmallVec::<[BinaryJSONRef<'_>; 8]>::new();
    for group in &groups {
        let value = group.view();
        if value.type_code == JSON_TYPE_CODE_ARRAY {
            let array = value.container()?;
            elements.reserve(array.count);
            for i in 0..array.count {
                elements.push(array.child(i)?);
            }
        } else {
            elements.push(value);
        }
    }
    encode_view_array(elements.iter().copied())
}

#[inline]
fn count_elements(value: BinaryJSONRef<'_>) -> Result<usize, BinaryJSONError> {
    match value.type_code {
        JSON_TYPE_CODE_ARRAY | JSON_TYPE_CODE_OBJECT => Ok(value.container()?.count),
        _ => Ok(1),
    }
}

#[inline]
fn sql_length(
    root: BinaryJSONRef<'_>,
    path: Option<&JSONPathExpression>,
) -> Result<Option<usize>, BinaryJSONError> {
    if let Some(path) = path.filter(|path| !path.legs().is_empty()) {
        if path.requires_walk() {
            return match extract_sql_selection(root, std::slice::from_ref(path))? {
                Some(EncodedJSONSelection::Borrowed { type_code, value, .. }) => {
                    count_elements(BinaryJSONRef { type_code, value }).map(Some)
                }
                Some(EncodedJSONSelection::Owned(value)) => value.element_count().map(Some),
                None => Ok(None),
            };
        }
        return extract_exact(root, path.legs())?.map(count_elements).transpose();
    }
    count_elements(root).map(Some)
}

fn extract_exact<'a>(
    value: BinaryJSONRef<'a>,
    legs: &[JSONPathLeg],
) -> Result<Option<BinaryJSONRef<'a>>, BinaryJSONError> {
    extract_exact_with(value, legs, |_, _| {})
}

#[inline]
fn sql_path_exists(root: BinaryJSONRef<'_>, path: &JSONPathExpression) -> Result<bool, BinaryJSONError> {
    if path.requires_walk() {
        visit_path_matches(root, path.legs(), &mut |_| true)
    } else {
        extract_exact(root, path.legs()).map(|target| target.is_some())
    }
}

fn extract_exact_with<'a>(
    mut value: BinaryJSONRef<'a>,
    legs: &[JSONPathLeg],
    mut visit: impl FnMut(BinaryJSONRef<'a>, usize),
) -> Result<Option<BinaryJSONRef<'a>>, BinaryJSONError> {
    for leg in legs {
        let (container, index) = match leg {
            JSONPathLeg::Key(key) if value.type_code == JSON_TYPE_CODE_OBJECT => {
                let object = value.container()?;
                let (index, found) = object.key_position(key.as_bytes())?;
                if !found {
                    return Ok(None);
                }
                (object, index)
            }
            JSONPathLeg::Array(JSONPathArraySelection::Index(index))
                if value.type_code == JSON_TYPE_CODE_ARRAY =>
            {
                let array = value.container()?;
                let Some(index) = normalize_index(*index, array.count) else {
                    return Ok(None);
                };
                (array, index)
            }
            JSONPathLeg::Array(selection) if autowraps_non_array(selection) => continue,
            _ => return Ok(None),
        };
        visit(value, index);
        value = container.child(index)?;
    }
    Ok(Some(value))
}

fn encoded_view(cell: &[u8]) -> Result<BinaryJSONRef<'_>, BinaryJSONError> {
    let (&type_code, value) = cell.split_first().ok_or(BinaryJSONError::InvalidBinary)?;
    Ok(BinaryJSONRef { type_code, value })
}

fn pack_path_matches(
    root: BinaryJSONRef<'_>,
    paths: &[JSONPathExpression],
) -> Result<Option<BinaryJSON>, BinaryJSONError> {
    let matches = collect_path_matches(root, paths)?;
    match (paths, matches.as_slice()) {
        (_, []) => Ok(None),
        ([path], [value]) if !path.could_match_multiple_values() => Ok(Some(
            value.copy().with_known_layout(Some(EncodedJSONLayout::canonical())),
        )),
        _ => BinaryJSON::array_from_views(&matches).map(Some),
    }
}

fn extract_sql_selection<'a>(
    root: BinaryJSONRef<'a>,
    paths: &[JSONPathExpression],
) -> Result<Option<EncodedJSONSelection<'a>>, BinaryJSONError> {
    if let [path] = paths {
        if !path.requires_walk() {
            return Ok(extract_exact(root, path.legs())?
                .map(|value| EncodedJSONSelection::borrowed(root, value)));
        }
    }
    let matches = collect_path_matches(root, paths)?;
    match (paths, matches.as_slice()) {
        (_, []) => Ok(None),
        ([path], [value]) if !path.could_match_multiple_values() => {
            Ok(Some(EncodedJSONSelection::borrowed(root, *value)))
        }
        _ => encode_view_array(matches.into_iter()).map(|value| Some(EncodedJSONSelection::Owned(value))),
    }
}

fn collect_path_matches<'a>(
    root: BinaryJSONRef<'a>,
    paths: &[JSONPathExpression],
) -> Result<SmallVec<[BinaryJSONRef<'a>; 4]>, BinaryJSONError> {
    let mut matches = SmallVec::<[BinaryJSONRef<'_>; 4]>::new();
    for path in paths {
        if path.requires_walk() {
            let first = matches.len();
            let mut seen = None;
            visit_path_matches(root, path.legs(), &mut |value| {
                let pointer = value.value.as_ptr();
                let new = if let Some(seen) = &mut seen {
                    HashSet::insert(seen, pointer)
                } else if matches.len() - first == 8 {
                    let mut set: HashSet<_> = matches[first..].iter()
                        .map(|value| value.value.as_ptr())
                        .collect();
                    let new = set.insert(pointer);
                    seen = Some(set);
                    new
                } else {
                    !matches[first..].iter().any(|value| value.value.as_ptr() == pointer)
                };
                if new {
                    matches.push(value);
                }
                false
            })?;
        } else if let Some(value) = extract_exact(root, path.legs())? {
            matches.push(value);
        }
    }
    Ok(matches)
}

fn visit_path_matches<'a>(
    value: BinaryJSONRef<'a>,
    legs: &[JSONPathLeg],
    visit: &mut impl FnMut(BinaryJSONRef<'a>) -> bool,
) -> Result<bool, BinaryJSONError> {
    visit_path_matches_at(value, legs, 0, visit)
}

fn visit_path_matches_at<'a>(
    value: BinaryJSONRef<'a>,
    mut legs: &[JSONPathLeg],
    depth: usize,
    visit: &mut impl FnMut(BinaryJSONRef<'a>) -> bool,
) -> Result<bool, BinaryJSONError> {
    if depth > MAX_JSON_DEPTH {
        return Err(BinaryJSONError::TooDeep);
    }
    // Autowrapping consumes a path leg without descending in the document.
    while let [JSONPathLeg::Array(selection), rest @ ..] = legs {
        if value.type_code == JSON_TYPE_CODE_ARRAY || !autowraps_non_array(selection) {
            break;
        }
        legs = rest;
    }
    let Some((leg, remain)) = legs.split_first() else {
        return Ok(visit(value));
    };
    match leg {
        JSONPathLeg::Key(key) if value.type_code == JSON_TYPE_CODE_OBJECT => {
            let object = value.container()?;
            if key == "*" {
                for index in 0..object.count {
                    if visit_path_matches_at(object.child(index)?, remain, depth + 1, visit)? {
                        return Ok(true);
                    }
                }
            } else if let Some(child) = object.find(key.as_bytes())? {
                return visit_path_matches_at(child, remain, depth + 1, visit);
            }
        }
        JSONPathLeg::Array(selection) => {
            if value.type_code == JSON_TYPE_CODE_ARRAY {
                let array = value.container()?;
                for index in selected_indices(selection, array.count) {
                    if visit_path_matches_at(array.child(index)?, remain, depth + 1, visit)? {
                        return Ok(true);
                    }
                }
            }
        }
        JSONPathLeg::DoubleAsterisk => {
            if visit_path_matches_at(value, remain, depth, visit)? {
                return Ok(true);
            }
            if matches!(value.type_code, JSON_TYPE_CODE_ARRAY | JSON_TYPE_CODE_OBJECT) {
                let container = value.container()?;
                for index in 0..container.count {
                    if visit_path_matches_at(container.child(index)?, legs, depth + 1, visit)? {
                        return Ok(true);
                    }
                }
            }
        }
        JSONPathLeg::Key(_) => {}
    }
    Ok(false)
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
            } else if autowraps_non_array(selection) {
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
                for index in walk_indices(selection, values.len()) {
                    select_walk_roots(
                        &values[index],
                        remain,
                        path.push_back_index(walk_index(selection, index)),
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

/// MySQL `LIKE` over chars: `%` any run, `_` any one, `escape` quotes the
/// next pattern char. Case handling is the caller's (fold for `_ci`
/// collations); this matcher is the wildcard state machine itself.
pub fn like_matches(text: &str, pattern: &str, escape: char) -> bool {
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

fn selected_indices(selection: &JSONPathArraySelection, length: usize) -> Range<usize> {
    match selection {
        JSONPathArraySelection::Asterisk => 0..length,
        JSONPathArraySelection::Index(index) => normalize_index(*index, length)
            .map_or(0..0, |index| index..index + 1),
        JSONPathArraySelection::Range { start, end } if length != 0 => {
            let Some(start) = normalize_range_start(*start, length) else {
                return 0..0;
            };
            let end = normalize_range_end(*end, length);
            start..end + 1
        }
        JSONPathArraySelection::Range { .. } => 0..0,
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

/// Whether an array selection applied to a non-array autowraps it, i.e. selects
/// the value itself as if it were a one-element array.
///
/// The source rule is stated on the selection, not on a synthesised one-element
/// array: `[0]` and `[last]` autowrap, `[0 to <non-negative>]` and `[0 to last]`
/// autowrap, and `[*]` never does. A range starting past `0`, or ending before
/// `last`, therefore selects nothing even though it would select index 0 of a
/// real one-element array.
fn autowraps_non_array(selection: &JSONPathArraySelection) -> bool {
    match selection {
        JSONPathArraySelection::Asterisk => false,
        JSONPathArraySelection::Index(index) => *index == 0 || *index == -1,
        JSONPathArraySelection::Range { start, end } => *start == 0 && *end >= -1,
    }
}

fn normalize_index(index: i64, length: usize) -> Option<usize> {
    let index = if index < 0 {
        i64::try_from(length).ok()?.checked_add(index)?
    } else {
        index
    };
    usize::try_from(index).ok().filter(|index| *index < length)
}

fn contains_ref(
    object: BinaryJSONRef<'_>,
    target: BinaryJSONRef<'_>,
) -> Result<bool, BinaryJSONError> {
    match (object.type_code, target.type_code) {
        (JSON_TYPE_CODE_OBJECT, JSON_TYPE_CODE_OBJECT) => {
            let object = object.container()?;
            let target = target.container()?;
            for index in 0..target.count {
                let Some(child) = object.find(target.key(index)?)? else {
                    return Ok(false);
                };
                if !contains_ref(child, target.child(index)?)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        (JSON_TYPE_CODE_ARRAY, JSON_TYPE_CODE_ARRAY) => {
            let target = target.container()?;
            for index in 0..target.count {
                if !contains_ref(object, target.child(index)?)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        (JSON_TYPE_CODE_ARRAY, _) => {
            let object = object.container()?;
            for index in 0..object.count {
                if contains_ref(object.child(index)?, target)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        _ => Ok(compare_json_refs(object, target).is_eq()),
    }
}

fn overlaps_ref<'a>(
    mut left: BinaryJSONRef<'a>,
    mut right: BinaryJSONRef<'a>,
) -> Result<bool, BinaryJSONError> {
    if left.type_code != JSON_TYPE_CODE_ARRAY && right.type_code == JSON_TYPE_CODE_ARRAY {
        std::mem::swap(&mut left, &mut right);
    }
    match (left.type_code, right.type_code) {
        (JSON_TYPE_CODE_OBJECT, JSON_TYPE_CODE_OBJECT) => {
            let left = left.container()?;
            let right = right.container()?;
            for index in 0..right.count {
                if let Some(child) = left.find(right.key(index)?)? {
                    if compare_json_refs(child, right.child(index)?).is_eq() {
                        return Ok(true);
                    }
                }
            }
            Ok(false)
        }
        (JSON_TYPE_CODE_ARRAY, JSON_TYPE_CODE_ARRAY) => {
            let left = left.container()?;
            let right = right.container()?;
            for i in 0..left.count {
                let value = left.child(i)?;
                for j in 0..right.count {
                    if compare_json_refs(value, right.child(j)?).is_eq() {
                        return Ok(true);
                    }
                }
            }
            Ok(false)
        }
        (JSON_TYPE_CODE_ARRAY, _) => {
            let left = left.container()?;
            for index in 0..left.count {
                if compare_json_refs(left.child(index)?, right).is_eq() {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        _ => Ok(compare_json_refs(left, right).is_eq()),
    }
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
        (JSONNode::Array(_), JSONNode::Array(target)) => target
            .iter()
            .all(|target| contains_node(object, target).unwrap_or(false)),
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

/// Go `OverlapsBinaryJSON` (`pkg/types/json_binary_functions.go`).
///
/// Overlap is decided ONE level down and by whole-value equality: two objects
/// overlap when they share a key whose values compare equal, two arrays when
/// they share an equal element. It does not recurse. `{"a":[1,2]}` and
/// `{"a":[2]}` do NOT overlap, and neither do `[[1,2]]` and `[1,2]` — the
/// shared `2` is one level too deep in both.
fn overlaps_node(left: &JSONNode, right: &JSONNode) -> Result<bool, BinaryJSONError> {
    // Go's single normalisation: a non-array against an array is answered as
    // that array against the non-array, so only one asymmetric arm exists.
    let (object, target) = match (left, right) {
        (left @ (JSONNode::Object(_) | JSONNode::Scalar(_)), right @ JSONNode::Array(_)) => {
            (right, left)
        }
        _ => (left, right),
    };
    Ok(match (object, target) {
        (JSONNode::Object(object), JSONNode::Object(target)) => {
            target.iter().try_fold(false, |found, (key, value)| {
                if found {
                    return Ok(true);
                }
                match object.iter().find(|(name, _)| name == key) {
                    Some((_, existing)) => nodes_equal(existing, value),
                    None => Ok(false),
                }
            })?
        }
        // Go returns false for an object against anything that is not an
        // object, which the equality below also answers.
        (JSONNode::Object(_), _) => false,
        (JSONNode::Array(object), JSONNode::Array(target)) => {
            object.iter().try_fold(false, |found, element| {
                if found {
                    return Ok(true);
                }
                target.iter().try_fold(false, |found, other| {
                    if found {
                        return Ok(true);
                    }
                    nodes_equal(element, other)
                })
            })?
        }
        (JSONNode::Array(object), target) => object.iter().try_fold(false, |found, element| {
            if found {
                return Ok(true);
            }
            nodes_equal(element, target)
        })?,
        (object, target) => nodes_equal(object, target)?,
    })
}

/// Go `CompareBinaryJSON(a, b) == 0`, the only comparison overlap uses.
fn nodes_equal(left: &JSONNode, right: &JSONNode) -> Result<bool, BinaryJSONError> {
    let left = BinaryJSON::from_node(left)?;
    let right = BinaryJSON::from_node(right)?;
    Ok(compare_binary_json(&left, &right).is_eq())
}

/// Go `MergeBinaryJSON` groups each adjacent run of objects before flattening
/// the remaining arrays. A left fold is observably different when an array
/// interrupts two objects: the objects after that array still merge with each
/// other, but never with the earlier array element.
fn merge_binary_nodes(values: &[JSONNode]) -> JSONNode {
    if values.is_empty() {
        return JSONNode::Array(Vec::new());
    }

    let mut results = Vec::with_capacity(values.len());
    let mut index = 0;
    while index < values.len() {
        if matches!(values[index], JSONNode::Object(_)) {
            let start = index;
            while index < values.len() && matches!(values[index], JSONNode::Object(_)) {
                index += 1;
            }
            results.push(merge_binary_objects(&values[start..index]));
        } else {
            results.push(values[index].clone());
            index += 1;
        }
    }

    if results.len() == 1 {
        return results.pop().expect("one merge result");
    }
    let mut flattened = Vec::new();
    for result in results {
        match result {
            JSONNode::Array(values) => flattened.extend(values),
            value => flattened.push(value),
        }
    }
    JSONNode::Array(flattened)
}

/// Go `mergeBinaryObject`: duplicate keys recursively use the same adjacent
/// object/array grouping rules, and the final object is bytewise key-sorted.
fn merge_binary_objects(objects: &[JSONNode]) -> JSONNode {
    let mut entries: Vec<(String, JSONNode)> = Vec::new();
    for object in objects {
        let JSONNode::Object(values) = object else {
            unreachable!("merge_binary_objects receives only objects");
        };
        for (key, value) in values {
            if let Some(index) = entries.iter().position(|(name, _)| name == key) {
                let previous = entries[index].1.clone();
                entries[index].1 = merge_binary_nodes(&[previous, value.clone()]);
            } else {
                entries.push((key.clone(), value.clone()));
            }
        }
    }
    entries.sort_unstable_by(|left, right| left.0.as_bytes().cmp(right.0.as_bytes()));
    JSONNode::Object(entries)
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

impl ValidatedBinaryJSONRef<'_> {
    /// Returns the exact size of this cell's TiDB equality hash payload.
    pub fn hash_value_size(self) -> Result<usize, BinaryJSONError> {
        match self.canonical_view() {
            Some(value) => hash_ref_size(value),
            None => self.to_owned().hash_value_size(),
        }
    }

    /// Appends the TiDB equality hash representation of this cell.
    pub fn append_hash_value(self, output: &mut Vec<u8>) -> Result<(), BinaryJSONError> {
        match self.canonical_view() {
            Some(value) => append_hash_ref(value, output),
            None => self.to_owned().append_hash_value(output),
        }
    }
}

fn hash_ref_size(value: BinaryJSONRef<'_>) -> Result<usize, BinaryJSONError> {
    let is_object = value.type_code == JSON_TYPE_CODE_OBJECT;
    if !is_object && value.type_code != JSON_TYPE_CODE_ARRAY {
        // Integer normalization only changes the type tag and payload bytes.
        return Ok(1 + value.value.len());
    }

    let container = value.container()?;
    let mut size: usize = 1 + size_of::<u32>();
    for (index, code) in container.child_type_codes().enumerate() {
        if is_object {
            let key_len = container.key(index)?.len();
            size = size
                .checked_add(uvarint_size(key_len) + key_len)
                .ok_or(BinaryJSONError::InvalidBinary)?;
        }
        let child_size = match fixed_payload_len(code) {
            Some(len) => 1 + len,
            None => hash_ref_size(container.child(index)?)?,
        };
        size = size
            .checked_add(child_size)
            .ok_or(BinaryJSONError::InvalidBinary)?;
    }
    Ok(size)
}

fn append_hash_ref(value: BinaryJSONRef<'_>, output: &mut Vec<u8>) -> Result<(), BinaryJSONError> {
    if matches!(value.type_code, JSON_TYPE_CODE_INT64 | JSON_TYPE_CODE_UINT64) {
        let integer = u64::from_le_bytes(
            value
                .value
                .try_into()
                .map_err(|_| BinaryJSONError::InvalidBinary)?,
        );
        let (magnitude, real) = if value.type_code == JSON_TYPE_CODE_INT64 {
            ((integer as i64).unsigned_abs(), integer as i64 as f64)
        } else {
            (integer, integer as f64)
        };
        if significant_fraction_bits(magnitude) <= 52 {
            let mut encoded = [JSON_TYPE_CODE_FLOAT64; 9];
            encoded[1..].copy_from_slice(&real.to_le_bytes());
            output.extend_from_slice(&encoded);
            return Ok(());
        }
    }
    output.push(value.type_code);
    if matches!(value.type_code, JSON_TYPE_CODE_ARRAY | JSON_TYPE_CODE_OBJECT) {
        let container = value.container()?;
        output.extend_from_slice(&(container.count as u32).to_le_bytes());
        for index in 0..container.count {
            if value.type_code == JSON_TYPE_CODE_OBJECT {
                let key = container.key(index)?;
                encode_uvarint(key.len(), output);
                output.extend_from_slice(key);
            }
            append_hash_ref(container.child(index)?, output)?;
        }
    } else {
        output.extend_from_slice(value.value);
    }
    Ok(())
}

fn append_hash_value_fallback(
    value: &BinaryJSON,
    output: &mut Vec<u8>,
) -> Result<(), BinaryJSONError> {
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
                append_hash_value_fallback(&BinaryJSON::from_node(&value)?, output)?;
            }
        }
        JSONNode::Object(mut values) => {
            output.push(crate::JSON_TYPE_CODE_OBJECT);
            output.extend_from_slice(&(values.len() as u32).to_le_bytes());
            values.sort_unstable_by(|left, right| left.0.as_bytes().cmp(right.0.as_bytes()));
            for (key, value) in values {
                let key = BinaryJSON::from_value(&Value::String(key))?;
                output.extend_from_slice(key.value());
                append_hash_value_fallback(&BinaryJSON::from_node(&value)?, output)?;
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

#[derive(Clone, Copy)]
enum PathEdit<'a> {
    Modify(JSONModifyType, &'a BinaryJSON),
    Remove,
}

fn edit_binary(
    document: BinaryJSONRef<'_>,
    legs: &[JSONPathLeg],
    edit: PathEdit<'_>,
) -> Result<Option<BinaryJSON>, BinaryJSONError> {
    let Some((leg, parent_legs)) = legs.split_last() else {
        return replace_binary_value(&[], edit);
    };
    let mut ancestors = SmallVec::<[_; 8]>::new();
    let Some(parent) = extract_exact_with(document, parent_legs, |value, index| {
        ancestors.push((value, index));
    })?
    else {
        return Ok(None);
    };
    let (container, index, found, key) = match leg {
        JSONPathLeg::Key(key) if parent.type_code == JSON_TYPE_CODE_OBJECT => {
            let object = parent.container()?;
            let key = key.as_bytes();
            let (index, found) = object.key_position(key)?;
            (object, index, found, key)
        }
        JSONPathLeg::Array(JSONPathArraySelection::Index(index))
            if parent.type_code == JSON_TYPE_CODE_ARRAY =>
        {
            let array = parent.container()?;
            let normalized = normalize_index(*index, array.count);
            (
                array,
                normalized.unwrap_or(array.count),
                normalized.is_some(),
                &[][..],
            )
        }
        JSONPathLeg::Array(selection) if autowraps_non_array(selection) => {
            return replace_binary_value(&ancestors, edit);
        }
        JSONPathLeg::Array(_) => {
            if let PathEdit::Modify(JSONModifyType::Set | JSONModifyType::Insert, value) = edit {
                let array = encode_view_array([parent, value.view()].into_iter())?;
                return replace_binary_value(
                    &ancestors,
                    PathEdit::Modify(JSONModifyType::Set, &array),
                );
            }
            return Ok(None);
        }
        _ => return Ok(None),
    };

    let container_edit = match (found, edit) {
        (true, PathEdit::Remove) => JSONContainerEdit::Remove { index },
        (true, PathEdit::Modify(JSONModifyType::Set | JSONModifyType::Replace, value)) => {
            JSONContainerEdit::Replace {
                index,
                value: value.view(),
            }
        }
        (false, PathEdit::Modify(JSONModifyType::Set | JSONModifyType::Insert, value)) => {
            JSONContainerEdit::Insert {
                index,
                key,
                value: value.view(),
            }
        }
        _ => return Ok(None),
    };
    container
        .rewrite(parent.type_code, container_edit, &ancestors)
        .map(Some)
}

fn replace_binary_value(
    ancestors: &[(BinaryJSONRef<'_>, usize)],
    edit: PathEdit<'_>,
) -> Result<Option<BinaryJSON>, BinaryJSONError> {
    let PathEdit::Modify(JSONModifyType::Replace | JSONModifyType::Set, value) = edit else {
        return Ok(None);
    };
    match ancestors.split_last() {
        Some(((parent, index), remaining)) => parent
            .container()?
            .rewrite(
                parent.type_code,
                JSONContainerEdit::Replace {
                    index: *index,
                    value: value.view(),
                },
                remaining,
            )
            .map(Some),
        None => Ok(Some(value.clone())),
    }
}

fn array_insert_index(index: i64, length: usize) -> Option<usize> {
    if index < 0 {
        normalize_index(index, length)
    } else {
        Some(usize::try_from(index).unwrap_or(usize::MAX).min(length))
    }
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
                } else if remain.is_empty() && mode != JSONModifyType::Replace {
                    // Go `binaryModifier.doInsert`: when the path selected no
                    // cell, the new value is APPENDED to the parent array.
                    // Which cell it named does not matter, so `$[last]` on an
                    // empty array appends exactly like `$[7]` does.
                    values.push(replacement);
                }
                return document;
            }

            if normalize_index(*index, 1) == Some(0) {
                return modify_node(document, remain, replacement, mode);
            }
            if remain.is_empty() && mode != JSONModifyType::Replace {
                // Go `doInsert` again, its non-array arm: a document that is
                // not an array becomes `[document, value]`. `$[last-1]` names
                // no cell of a one-element autowrap, so it lands here.
                return JSONNode::Array(vec![document, replacement]);
            }
            document
        }
        JSONPathLeg::Array(_) | JSONPathLeg::DoubleAsterisk => document,
    }
}

fn validate_remove_path(path: &JSONPathExpression) -> Result<(), BinaryJSONError> {
    if path.legs().is_empty() || path.contains_any_asterisk() || path.contains_any_range() {
        Err(BinaryJSONError::InvalidPath)
    } else {
        Ok(())
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
                // A non-array can stand for a one-element array while walking
                // to the parent, but its virtual element cannot be removed.
                if !remain.is_empty() && normalize_index(*index, 1) == Some(0) {
                    remove_node(document, remain);
                }
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
    fn extraction_preserves_matches_from_separate_paths() {
        let value = json(r#"{"x":1}"#);
        let path = parse_json_path_expr("$.x").unwrap();
        let result = value.extract(&[path.clone(), path]).unwrap().unwrap();
        assert_eq!(result.to_string(), "[1, 1]");
    }

    #[test]
    fn quoted_asterisks_walk_object_members_and_only_wrap_multiple_matches() {
        let multiple = json(r#"{"a": 1,"b": 2}"#);
        let single = json(r#"{"a": {"key": 1}}"#);
        for spelling in [r#"$."*""#, r#"$."\u002a""#] {
            let path = parse_json_path_expr(spelling).unwrap();
            let paths = std::slice::from_ref(&path);
            assert!(!path.could_match_multiple_values());
            assert_eq!(multiple.extract_for_sql(paths).unwrap().unwrap(), json("[1, 2]"));
            assert_eq!(multiple.extract(paths).unwrap().unwrap(), json("[1, 2]"));
            assert_eq!(multiple.get_length(Some(&path)).unwrap(), Some(2));
            assert!(multiple.get_keys(Some(&path)).unwrap().is_none());
            assert!(multiple.path_exists_for_sql(&path).unwrap());
            assert_eq!(single.extract_for_sql(paths).unwrap().unwrap(), json(r#"{"key": 1}"#));
            assert_eq!(single.get_keys(Some(&path)).unwrap().unwrap(), json(r#"["key"]"#));
        }
        let unquoted = parse_json_path_expr("$.*").unwrap();
        assert_eq!(single.extract_for_sql(&[unquoted]).unwrap().unwrap(), json(r#"[{"key": 1}]"#));
    }

    #[test]
    fn containment_checks_target_arrays_against_the_whole_source_array() {
        assert!(contains_binary_json(&json("[]"), &json("[[]]")).unwrap());
        assert!(contains_binary_json(&json("[1,2]"), &json("[[1,2]]")).unwrap());
    }

    #[test]
    fn extraction_checks_unselected_entries_in_existing_binary_payloads() {
        let array = json("[1,2]");
        let mut payload = array.value().to_vec();
        // There are eight header bytes and five bytes per entry.
        payload[13] = 0xff;
        let malformed = BinaryJSON::from_encoded_parts(array.type_code(), payload);
        let path = parse_json_path_expr("$[0]").unwrap();
        assert_eq!(malformed.extract(&[path]), Err(BinaryJSONError::InvalidBinary));
        assert_eq!(malformed.element_count(), Err(BinaryJSONError::InvalidBinary));
        assert_eq!(
            malformed.element_depth(),
            Err(BinaryJSONError::InvalidBinary)
        );
        assert_eq!(malformed.keys(), Err(BinaryJSONError::InvalidBinary));
        assert_eq!(
            malformed.hash_value_size(),
            Err(BinaryJSONError::InvalidBinary)
        );
    }

    #[test]
    fn sql_extraction_reads_only_matches_and_leaves_unknown_layout_unchecked() {
        let object = json(r#"{"n":7,"payload":"text","z":{"n":8}}"#);
        let mut payload = object.value().to_vec();
        let text = payload.windows(4).position(|bytes| bytes == b"text").unwrap();
        payload[text] = 0xff;
        let raw = BinaryJSON::from_encoded_parts(object.type_code(), payload);
        let exact = parse_json_path_expr("$.n").unwrap();
        let missing = parse_json_path_expr("$.missing").unwrap();
        let wildcard = parse_json_path_expr("$.*.n").unwrap();
        let selected = raw.extract_for_sql(std::slice::from_ref(&exact)).unwrap().unwrap();
        assert_eq!(selected, json("7"));
        assert!(selected.known_layout().is_none());
        assert_eq!(raw.path_exists_for_sql(&exact), Ok(true));
        assert_eq!(raw.path_exists_for_sql(&missing), Ok(false));
        assert_eq!(BinaryJSON::encoded_path_exists_for_sql(&raw.encoded(), &wildcard), Ok(true));
        assert_eq!(raw.extract_for_sql(&[exact.clone(), missing, exact]), Ok(Some(json("[7,7]"))));
        assert_eq!(raw.extract_for_sql(&[wildcard]), Ok(Some(json("[8]"))));
        assert_eq!(raw.extract(&[parse_json_path_expr("$.n").unwrap()]), Err(BinaryJSONError::InvalidBinary));
    }

    #[test]
    fn sql_extraction_bounds_recursive_binary_traversal() {
        let mut value = json("0");
        for _ in 0..=MAX_JSON_DEPTH {
            value = encode_view_array(std::iter::once(value.view())).unwrap();
        }
        let path = parse_json_path_expr("$**.missing").unwrap();
        assert_eq!(value.extract_for_sql(&[path]), Err(BinaryJSONError::TooDeep));
    }

    #[test]
    fn unsorted_binary_objects_preserve_existing_lookup_and_comparison() {
        let encoded = json(r#"{"a":1,"b":2}"#);
        let mut payload = encoded.value().to_vec();
        // Swap the key records and their corresponding value records.
        for index in 8..14 {
            payload.swap(index, index + 6);
        }
        for index in 20..25 {
            payload.swap(index, index + 5);
        }
        let unsorted = BinaryJSON::from_raw(encoded.type_code(), payload).unwrap();
        assert_eq!(unsorted.object_get("a").unwrap(), Some(json("1")));
        assert_eq!(compare_binary_json(&encoded, &unsorted), std::cmp::Ordering::Equal);
        assert_eq!(unsorted.keys().unwrap(), json(r#"["a","b"]"#));
        assert_eq!(unsorted.get_keys(None).unwrap(), Some(json(r#"["b","a"]"#)));
        assert_eq!(unsorted.element_depth().unwrap(), 2);
        assert_eq!(
            unsorted.hash_value_size().unwrap(),
            encoded.hash_value().unwrap().len()
        );
    }

    /// Complete translation of `pkg/types/json_binary_test.go::TestBinaryJSONExtract`.
    #[test]
    fn test_binary_json_extract() {
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
    fn test_binary_json_type() {
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

        assert_eq!(
            BinaryJSON::from_typed_value(&crate::BinaryJSONValue::Uint64(1_u64 << 63))
                .unwrap()
                .type_name()
                .unwrap(),
            "UNSIGNED INTEGER"
        );
    }

    #[test]
    fn test_binary_json_unquote() {
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
    }

    #[test]
    fn test_get_keys() {
        for (input, expected) in [
            ("[]", "[]"),
            ("{}", "[]"),
            (r#"{"comment":"1234"}"#, r#"["comment"]"#),
            (r#"{"name":"Tom","age":19}"#, r#"["age","name"]"#),
            (
                r#"{"🦀": {"nested": [0]}, "é": false, "": null, "a\u0000z": []}"#,
                r#"["", "a\u0000z", "é", "🦀"]"#,
            ),
        ] {
            assert_eq!(json(input).keys().unwrap(), json(expected), "{input}");
        }
        for length in [127, 128, 16_383, 16_384, 65_535] {
            let key = "k".repeat(length);
            let object = json(&format!(r#"{{"{key}":null}}"#));
            assert_eq!(object.keys().unwrap(), json(&format!(r#"["{key}"]"#)));
        }
        assert_eq!(
            json(r#"{"name":"Tom","age":19}"#)
                .keys()
                .unwrap()
                .element_count()
                .unwrap(),
            2
        );
        let long_key = format!("{{\"{}\":1}}", "a".repeat(65_536));
        assert_eq!(
            BinaryJSON::parse(&long_key).unwrap_err().to_string(),
            "[types:8129]TiDB does not yet support JSON objects with the key length >= 65536"
        );
    }

    #[test]
    fn binary_json_keys_and_depth_validate_stored_values() {
        let object = json(r#"{"a":false}"#);
        let mut payload = object.value().to_vec();
        // The header and key entry precede the type code and inline literal.
        payload[15] = 0xff;
        let malformed = BinaryJSON::from_encoded_parts(object.type_code(), payload);
        assert_eq!(malformed.keys(), Err(BinaryJSONError::InvalidBinary));
        assert_eq!(
            malformed.element_depth(),
            Err(BinaryJSONError::InvalidBinary)
        );
        let raw = BinaryJSON::from_encoded_parts(object.type_code(), object.value().to_vec());
        assert_eq!(raw.keys().unwrap(), json(r#"["a"]"#));
        assert_eq!(raw.element_depth().unwrap(), 2);
    }

    #[test]
    fn sql_json_keys_reads_only_the_selected_key_table() {
        let object = json(r#"{"a":{"":"value","é":1},"z":"unrelated"}"#);
        let mut encoded = object.encoded();
        *encoded.last_mut().unwrap() = 0xff;
        let raw = BinaryJSON::from_encoded_parts(encoded[0], &encoded[1..]);
        let path = parse_json_path_expr("$.a").unwrap();
        assert_eq!(raw.get_keys(None), Ok(Some(json(r#"["a","z"]"#))));
        assert_eq!(raw.get_keys(Some(&path)), Ok(Some(json(r#"["","é"]"#))));
        assert_eq!(BinaryJSON::get_encoded_keys(&encoded, Some(&path)), raw.get_keys(Some(&path)));
        assert!(raw.known_layout().is_none());
        assert_eq!(raw.keys(), Err(BinaryJSONError::InvalidBinary));

        let mut object = json(r#"{"a":true}"#).encoded();
        let key_offset = u32::from_le_bytes(object[9..13].try_into().unwrap()) as usize;
        object[1 + key_offset] = 0xff;
        assert_eq!(BinaryJSON::get_encoded_keys(&object, None), Err(BinaryJSONError::InvalidBinary));
        object[9..13].copy_from_slice(&u32::MAX.to_le_bytes());
        assert_eq!(BinaryJSON::get_encoded_keys(&object, None), Err(BinaryJSONError::InvalidBinary));
    }

    #[test]
    fn sql_json_length_counts_only_the_selected_header() {
        let object = json(r#"{"a":[1,2],"z":"unrelated"}"#);
        let mut encoded = object.encoded();
        *encoded.last_mut().unwrap() = 0xff;
        let raw = BinaryJSON::from_encoded_parts(encoded[0], &encoded[1..]);
        assert_eq!(raw.get_length(None), Ok(Some(2)));
        assert_eq!(raw.element_count(), Err(BinaryJSONError::InvalidBinary));

        for (path, count) in [("$.a", Some(2)), ("$.a[1]", Some(1)), ("$.missing", None)] {
            let path = parse_json_path_expr(path).unwrap();
            assert_eq!(raw.get_length(Some(&path)), Ok(count));
            assert_eq!(BinaryJSON::get_encoded_length(&encoded, Some(&path)), Ok(count));
        }
        encoded[5] ^= 1;
        assert_eq!(BinaryJSON::get_encoded_length(&encoded, None), Err(BinaryJSONError::InvalidBinary));
    }

    #[test]
    fn test_binary_json_depth() {
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
    }

    #[test]
    fn test_binary_json_copy() {
        for source in [
            r#"{"a":[1,"2",{"aa":"bb"},4,null],"b":true,"c":null}"#,
            r#"{"aaaaaaaaaaa":[1,"2",{"aa":"bb"},4.1],"bbbbbbbbbb":true,"ccccccccc":"d"}"#,
            r#"[{"a":1,"b":true},3,3.5,"hello, world",null,true]"#,
        ] {
            let value = json(source);
            assert_eq!(value.clone().to_string(), value.to_string());
        }
    }

    /// Complete translation of `pkg/types/json_binary_test.go::TestBinaryJSONMerge`.
    #[test]
    fn test_binary_json_merge() {
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
            (
                vec!["[1]", r#"{"a":1}"#, r#"{"a":2}"#],
                r#"[1,{"a":[1,2]}]"#,
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
            merge_patch_binary_json(&[Some(target), Some(patch)])
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
    fn merge_preserve_reports_depth_added_by_autowrapping() {
        let text = format!(r#"{{"x":{}0{}}}"#, "[".repeat(99), "]".repeat(99));
        let document = json(&text);
        assert_eq!(
            merge_binary_json(&[document.clone(), json("1")]),
            Err(BinaryJSONError::TooDeep)
        );
        assert!(merge_patch_binary_json(&[Some(document), Some(json(r#"{"tag":1}"#))]).is_ok());
    }

    #[test]
    fn merge_patch_keeps_deletions_and_replacements_for_unsorted_input() {
        let mut payload = json(r#"{"a":1,"b":[2]}"#).value().to_vec();
        payload[8..20].rotate_left(6);
        payload[20..30].rotate_left(5);
        let unsorted = BinaryJSON::from_encoded_parts(JSON_TYPE_CODE_OBJECT, payload);
        let patch = json(r#"{"a":null,"c":{"remove":null,"keep":[3]}}"#);
        assert_eq!(
            merge_patch_binary_json(&[Some(unsorted), Some(patch)]).unwrap(),
            Some(json(r#"{"b":[2],"c":{"keep":[3]}}"#))
        );
    }

    #[test]
    fn test_binary_json_contains() {
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
    }

    /// Complete translation of `pkg/types/json_binary_test.go::TestBinaryJSONModify`.
    #[test]
    fn test_binary_json_modify() {
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
    fn remove_autowraps_intermediate_nonarrays() {
        let document = json(r#"{"a":{"x":1},"b":2}"#);
        let path = parse_json_path_expr("$[0].a[last].x").unwrap();
        let expected = json(r#"{"a":{},"b":2}"#);

        let mut payload = document.value().to_vec();
        payload[8..20].rotate_left(6);
        payload[20..30].rotate_left(5);
        let unsorted = BinaryJSON::from_encoded_parts(document.type_code(), payload);
        assert!(unsorted.canonical_view().is_none());
        for value in [&document, &unsorted] {
            assert_eq!(value.remove(std::slice::from_ref(&path)).unwrap(), expected);
        }
    }

    #[test]
    fn binary_edit_keeps_unmodified_storage_and_checks_invalid_values() {
        let document = json(r#"{"a":[1,2],"b":3}"#);
        let absent = parse_json_path_expr("$.missing").unwrap();
        let result = document.remove(std::slice::from_ref(&absent)).unwrap();
        assert_eq!(result.value().as_ptr(), document.value().as_ptr());
        let result = document
            .modify(
                std::slice::from_ref(&absent),
                &[json("4")],
                JSONModifyType::Replace,
            )
            .unwrap();
        assert_eq!(result.value().as_ptr(), document.value().as_ptr());

        let invalid_value =
            BinaryJSON::from_encoded_parts(crate::JSON_TYPE_CODE_STRING, vec![2, b'a']);
        assert_eq!(
            document.modify(&[absent], &[invalid_value], JSONModifyType::Replace),
            Err(BinaryJSONError::InvalidBinary),
        );
    }

    #[test]
    fn test_binary_json_remove() {
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
        assert!(json(r#"{"a":[3]}"#)
            .remove(&[parse_json_path_expr("$.a[*]").unwrap()])
            .is_err());
    }

    #[test]
    fn test_binary_json_peek_and_numeric_hash_contract() {
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
    fn binary_json_hash_append_preserves_key_order_and_buffer_prefix() {
        let canonical = json(r#"{"a":3,"b":[3.0,false]}"#);
        let mut expected = vec![1, 2, 0, 0, 0, 1, b'a', 11];
        expected.extend_from_slice(&3.0_f64.to_le_bytes());
        expected.extend_from_slice(&[1, b'b', 3, 2, 0, 0, 0, 11]);
        expected.extend_from_slice(&3.0_f64.to_le_bytes());
        expected.extend_from_slice(&[4, 2]);

        let mut unsorted = canonical.value().to_vec();
        // Swap the key entries and their associated value entries.
        unsorted[8..20].rotate_left(6);
        unsorted[20..30].rotate_left(5);
        let unsorted = BinaryJSON::from_encoded_parts(JSON_TYPE_CODE_OBJECT, unsorted);
        assert!(unsorted.canonical_view().is_none());
        for value in [canonical, unsorted] {
            let mut output = b"prefix".to_vec();
            value.append_hash_value(&mut output).unwrap();
            assert_eq!(&output[..6], b"prefix");
            assert_eq!(&output[6..], expected);

            let mut cell = vec![value.type_code()];
            cell.extend_from_slice(value.value());
            let borrowed = ValidatedBinaryJSONRef::from_encoded(&cell).unwrap();
            assert_eq!(borrowed.hash_value_size().unwrap(), expected.len());
            assert_eq!(borrowed.hash_value_as_encoded(), None);
            output.truncate(6);
            borrowed.append_hash_value(&mut output).unwrap();
            assert_eq!(&output[..6], b"prefix");
            assert_eq!(&output[6..], expected);
        }
    }

    #[test]
    fn binary_json_hash_append_rolls_back_on_invalid_input() {
        let mut value = json("[1,2]").value().to_vec();
        value.pop();
        let invalid = BinaryJSON::from_encoded_parts(JSON_TYPE_CODE_ARRAY, value);
        let mut output = b"prefix".to_vec();
        assert!(invalid.append_hash_value(&mut output).is_err());
        assert_eq!(output, b"prefix");

        let text = json(&format!("\"{}\"", "ü".repeat(512)));
        let mut payload = text.value().to_vec();
        *payload.last_mut().unwrap() = 0xff;
        let invalid = BinaryJSON::from_encoded_parts(text.type_code(), payload);
        assert!(invalid.append_hash_value(&mut output).is_err());
        assert_eq!(output, b"prefix");
        let mut cell = vec![invalid.type_code()];
        cell.extend_from_slice(invalid.value());
        assert!(matches!(
            ValidatedBinaryJSONRef::from_encoded(&cell),
            Err(BinaryJSONError::InvalidBinary)
        ));
    }

    /// Complete translation of `pkg/types/json_binary_test.go::TestFunctions`.
    #[test]
    fn test_functions() {
        let source = b"\\bfnrtuz0";
        assert_eq!(
            crate::unquote_json_string("\\bfnrtuz0").unwrap(),
            "\u{8}fnrtuz0"
        );
        assert!(peek_binary_json_len(source).is_err());
        assert!(peek_binary_json_len(b"").is_err());
    }

    #[test]
    fn test_hash_value() {
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
    }

    #[test]
    fn binary_json_hash_size_counts_nested_keys_and_payloads() {
        for key_len in [0, 127, 128, 16_383, 16_384] {
            let key = "a".repeat(key_len);
            let value = json(&format!(r#"[{{"{key}":[null,-1,18446744073709551615,0.5,{{}}]}}]"#));
            assert_eq!(
                value.hash_value_size().unwrap(),
                value.hash_value().unwrap().len()
            );
        }
    }

    #[test]
    fn test_binary_json_extract_callback() {
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
    }

    #[test]
    fn search_matches_go_callback_paths_and_escapes() {
        let document = json(r#"["no","slash\\"]"#);
        let last = parse_json_path_expr("$[last]").unwrap();
        assert_eq!(
            document.search(
                JSONSearchMode::One,
                "slash\\",
                '\\',
                std::slice::from_ref(&last)
            ),
            Ok(Some(json(r#""$[last-0]""#))),
        );
        let extracted = document.extract_matches(&last).unwrap();
        assert_eq!(extracted[0].0.to_string(), "$[last-0]");

        let document = json(r#"["needle","needle"]"#);
        let paths = ["$[last]", "$[1]", "$[*]"].map(|path| parse_json_path_expr(path).unwrap());
        assert_eq!(
            document.search(JSONSearchMode::All, "needle", '\\', &paths),
            Ok(Some(json(r#"["$[last-0]","$[1]","$[0]"]"#))),
        );
        for range in ["$[last-50 to last]", "$[0 to last-50]"] {
            let path = parse_json_path_expr(range).unwrap();
            assert_eq!(
                document.search(JSONSearchMode::All, "needle", '\\', &[path]),
                Ok(None),
            );
        }

        let document = json(r#"{"a\u0001b":"needle"}"#);
        let path = document
            .search(JSONSearchMode::One, "needle", '\\', &[])
            .unwrap()
            .unwrap();
        assert_eq!(path.as_string().unwrap(), b"$.\"a\x01b\"");
    }

    #[test]
    fn binary_walk_shares_subtrees_and_keeps_full_storage_validation() {
        let document = json(r#"[{"a":"needle"},2]"#);
        let selected = document.array_get(0).unwrap().unwrap();
        let walked = document
            .walk(&[parse_json_path_expr("$[0]").unwrap()])
            .unwrap();
        assert_eq!(walked[0].1.value().as_ptr(), selected.value().as_ptr());

        let mut payload = document.value().to_vec();
        payload[13] = 0xff;
        let malformed = BinaryJSON::from_encoded_parts(document.type_code(), payload);
        assert_eq!(
            malformed.search(JSONSearchMode::One, "needle", '\\', &[]),
            Err(BinaryJSONError::InvalidBinary),
        );
    }

    /// Complete translation of `pkg/types/json_binary_test.go::TestBinaryJSONWalk`.
    #[test]
    fn test_binary_json_walk() {
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

        let expected_subtree = ["$[1]", "$[1][0]", "$[1][0].k", "$[1][1]"];
        let subtree = document
            .walk(&[parse_json_path_expr("$[1]").unwrap()])
            .unwrap();
        assert_eq!(
            subtree
                .iter()
                .map(|(path, _)| path.to_string())
                .collect::<Vec<_>>(),
            expected_subtree
        );

        let duplicate_subtree = document
            .walk(&[
                parse_json_path_expr("$[1]").unwrap(),
                parse_json_path_expr("$[1]").unwrap(),
            ])
            .unwrap();
        assert_eq!(
            duplicate_subtree
                .iter()
                .map(|(path, _)| path.to_string())
                .collect::<Vec<_>>(),
            expected_subtree
        );
        assert!(document
            .walk(&[parse_json_path_expr("$.m").unwrap()])
            .unwrap()
            .is_empty());
        let empty_object = json("{}").walk(&[]).unwrap();
        assert_eq!(empty_object.len(), 1);
        assert_eq!(empty_object[0].0.to_string(), "$");
        assert_eq!(empty_object[0].1.to_string(), "{}");

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
