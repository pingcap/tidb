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

//! Self-contained pieces of `pkg/meta/model/column.go`: the column-info
//! version constants, `ChangeStateInfo`, and the modify-column name-mangling
//! helpers (changing/removing prefixes).
//!
//! The `ColumnInfo` struct and its FieldType-delegating accessors, generated-
//! /changing-column predicates, and `FindColumnInfo` helpers are ported below.
//!
//! The default-value accessors (`Set`/`GetDefaultValue` and the origin
//! variants) are ported too, over a [`ColumnDefaultValue`] value type that
//! models a Go string as `Vec<u8>`, so the BIT-type byte default (Go's
//! invalid-UTF-8 case in `TestDefaultValue`) is a normal case.
//!
//! The `NewExtra*ColInfo` constructors and `GetTypeDesc` are ported too.
//!
//! The generic JSON domain of Go's `interface{}` is retained recursively,
//! including arrays, objects, nulls, and float64-decoded JSON numbers.

use std::collections::{BTreeMap, BTreeSet};

use serde::de::{DeserializeSeed, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Serialize};
use tidb_ast::CiString;
use tidb_datatype::{
    FieldType, FieldTypeCode, FieldTypeFlags, GoString, ERR_INVALID_DEFAULT,
    STRICT_INTEGER_DISPLAY_WIDTH,
};
use tidb_error::mysql::{errname, FormatArg};
use tidb_error::terror::TerrorError;

use crate::go_runtime::{GoShared, GoSharedPointerSlice, GoSharedSlice};
use crate::schema_state::SchemaState;
use crate::serde_helpers::{
    deserialize_go_object, go_json_field_matches, ignore_unknown, impl_go_json_deserialize,
    impl_go_json_merge_object, AtomicReplaceSeed, FatalSeed, NullNoopSeed, OptionBytesSeed,
    OptionMergeSeed, OptionScalarSeed, ValueMergeSeed,
};
use crate::table_info::TableInfo;

/// Go `ColumnInfoVersion0`.
pub const COLUMN_INFO_VERSION0: u64 = 0;
/// Go `ColumnInfoVersion1`.
pub const COLUMN_INFO_VERSION1: u64 = 1;
/// Go `ColumnInfoVersion2`: fixes a utf8/utf8mb4 charset compatibility issue.
pub const COLUMN_INFO_VERSION2: u64 = 2;
/// Go `CurrLatestColumnInfoVersion`: the latest column-info version.
pub const CURR_LATEST_COLUMN_INFO_VERSION: u64 = COLUMN_INFO_VERSION2;

/// Go `changingColumnPrefix`: prefixes the temporary name of a column being
/// modified (`_Col$_<old_name>_<n>`).
pub const CHANGING_COLUMN_PREFIX: &str = "_Col$_";
/// Go `removingObjPrefix`: prefixes the tombstone name of a column/index
/// being removed during a modify-column.
pub const REMOVING_OBJ_PREFIX: &str = "_Tombstone$_";

/// Go `GenUniqueChangingColumnName`: generates the first unused temporary
/// changing-column name, comparing candidates case-insensitively.
#[must_use]
pub fn gen_unique_changing_column_name(table: &TableInfo, old_column: &ColumnInfo) -> String {
    let used: std::collections::HashSet<String> = table
        .columns
        .iter_deref()
        .map(|column| column.read().name.lowercase().to_owned())
        .collect();
    let mut suffix = 0_u64;
    loop {
        let candidate = format!(
            "{CHANGING_COLUMN_PREFIX}{}_{}",
            old_column.name.original(),
            suffix
        );
        if !used.contains(tidb_mysql::to_lowercase(&candidate).as_str()) {
            return candidate;
        }
        suffix += 1;
    }
}

/// Go `map[string]struct{}` with nil/allocation identity preserved.
///
/// A nil map serializes as `null`, an allocated empty map as `{}`, and keys in
/// an allocated map sort lexically through the underlying [`BTreeSet`].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct GoStringSet(Option<BTreeSet<String>>);

impl GoStringSet {
    /// Returns whether the source map has an allocation, independently of its
    /// length.
    #[must_use]
    pub fn is_allocated(&self) -> bool {
        self.0.is_some()
    }

    /// Returns whether the map has no keys (`len(nil)` is zero in Go).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.as_ref().is_none_or(BTreeSet::is_empty)
    }

    /// Iterates keys in the same lexical order used by `encoding/json`.
    pub fn iter(&self) -> impl Iterator<Item = &String> {
        self.0.iter().flatten()
    }

    /// Reports whether a dependency is present.
    #[must_use]
    pub fn contains(&self, value: &str) -> bool {
        self.0.as_ref().is_some_and(|values| values.contains(value))
    }

    /// Inserts a dependency, allocating the map just as a Go map assignment
    /// requires.
    pub fn insert(&mut self, value: String) -> bool {
        self.0.get_or_insert_with(BTreeSet::new).insert(value)
    }

    /// Clears keys without changing nil versus allocated-empty identity.
    pub fn clear(&mut self) {
        if let Some(values) = &mut self.0 {
            values.clear();
        }
    }

    /// Constructs an allocated map, including the allocated-empty case.
    pub fn allocated(values: impl IntoIterator<Item = String>) -> Self {
        Self(Some(values.into_iter().collect()))
    }
}

#[derive(Default, Deserialize, Serialize)]
struct EmptyObject {}

impl Serialize for GoStringSet {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match &self.0 {
            None => serializer.serialize_none(),
            Some(values) => serializer.collect_map(values.iter().map(|key| (key, EmptyObject {}))),
        }
    }
}

struct GoStringSetSeed<'a>(&'a mut GoStringSet);

impl<'de> DeserializeSeed<'de> for GoStringSetSeed<'_> {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct SetOrNullVisitor<'a>(&'a mut GoStringSet);

        impl<'de> Visitor<'de> for SetOrNullVisitor<'_> {
            type Value = ();

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("null or an object with empty-object values")
            }

            fn visit_none<E>(self) -> Result<Self::Value, E> {
                self.0 .0 = None;
                Ok(())
            }

            fn visit_unit<E>(self) -> Result<Self::Value, E> {
                self.0 .0 = None;
                Ok(())
            }

            fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct SetVisitor<'a>(&'a mut GoStringSet);

                impl<'de> Visitor<'de> for SetVisitor<'_> {
                    type Value = ();

                    fn expecting(
                        &self,
                        formatter: &mut std::fmt::Formatter<'_>,
                    ) -> std::fmt::Result {
                        formatter.write_str("an object with empty-object values")
                    }

                    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
                    where
                        A: MapAccess<'de>,
                    {
                        let values = self.0 .0.get_or_insert_with(BTreeSet::new);
                        let mut first_error = None;
                        while let Some(key) = map.next_key::<String>()? {
                            let mut empty = EmptyObject::default();
                            if let Err(error) = map.next_value_seed(NullNoopSeed(&mut empty)) {
                                first_error.get_or_insert(error);
                            }
                            values.insert(key);
                        }
                        if let Some(error) = first_error {
                            return Err(error);
                        }
                        Ok(())
                    }
                }

                deserialize_go_object(deserializer, SetVisitor(self.0))
            }
        }

        deserializer.deserialize_option(SetOrNullVisitor(self.0))
    }
}

impl<'de> Deserialize<'de> for GoStringSet {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let mut value = Self::default();
        GoStringSetSeed(&mut value).deserialize(deserializer)?;
        Ok(value)
    }
}

/// Go `ChangeStateInfo`: records schema-change information for a column.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub struct ChangeStateInfo {
    /// The offset of the changing column this column depends on during a
    /// modify/change column.
    #[serde(rename = "relative_col_offset", default)]
    pub dependency_column_offset: i64,
}

impl_go_json_merge_object!(ChangeStateInfo, destination, map, key, {
    if go_json_field_matches(&key, "relative_col_offset") {
        map.next_value_seed(NullNoopSeed(&mut destination.dependency_column_offset))?;
    } else {
        ignore_unknown(&mut map)?;
    }
});

impl_go_json_deserialize!(ChangeStateInfo);

/// Go `GenRemovingObjName`: the tombstone name for `name` (idempotent).
#[must_use]
pub fn gen_removing_obj_name(name: &str) -> String {
    if name.starts_with(REMOVING_OBJ_PREFIX) {
        name.to_owned()
    } else {
        format!("{REMOVING_OBJ_PREFIX}{name}")
    }
}

/// Go `ColumnInfo.IsChanging` over a column's original-case name.
#[must_use]
pub fn is_changing_name(name: &str) -> bool {
    name.starts_with(CHANGING_COLUMN_PREFIX)
}

/// Go `ColumnInfo.IsRemoving` over a column's original-case name.
#[must_use]
pub fn is_removing_name(name: &str) -> bool {
    name.starts_with(REMOVING_OBJ_PREFIX)
}

/// Go `ColumnInfo.GetRemovingOriginName`: the original name of a removing
/// column (strips the tombstone prefix).
#[must_use]
pub fn removing_origin_name(name: &str) -> String {
    name.strip_prefix(REMOVING_OBJ_PREFIX)
        .unwrap_or(name)
        .to_owned()
}

/// Go `ColumnInfo.GetChangingOriginName`: the original name of a changing
/// column (strips the changing prefix and the trailing `_<n>` suffix).
#[must_use]
pub fn changing_origin_name(name: &str) -> String {
    let column_name = name.strip_prefix(CHANGING_COLUMN_PREFIX).unwrap_or(name);
    match column_name.rfind('_') {
        None => column_name.to_owned(),
        Some(pos) => column_name[..pos].to_owned(),
    }
}

/// A column default value (Go's `any`). Go strings are byte sequences that
/// need not be valid UTF-8, so the string variant holds `Vec<u8>`; this makes
/// the BIT-type default (which can be arbitrary bytes) a normal case rather
/// than a special one. Go `nil` is represented by `Option::None` at the field.
#[derive(Clone, Debug, PartialEq)]
pub enum ColumnDefaultValue {
    /// A nested JSON null in an array or object.
    Null,
    /// A signed integer default.
    Int(i64),
    /// An unsigned integer default.
    Uint(u64),
    /// A floating-point default.
    Float(f64),
    /// A boolean default.
    Bool(bool),
    /// A string default (Go string = arbitrary byte sequence).
    Str(Vec<u8>),
    /// A Go `[]any` default.
    Array(Vec<ColumnDefaultValue>),
    /// A Go `map[string]any` default, whose JSON keys sort lexically.
    Object(BTreeMap<String, ColumnDefaultValue>),
}

impl ColumnDefaultValue {
    /// A string default from a UTF-8 `str`.
    #[must_use]
    pub fn str(s: &str) -> Self {
        ColumnDefaultValue::Str(s.as_bytes().to_vec())
    }
}

// Go's `any` marshals as the bare JSON value, so this enum is written
// untagged. A Go string that is not valid UTF-8 is emitted by `encoding/json`
// with each invalid byte replaced by U+FFFD. Rust's standard lossy conversion
// can consume a whole malformed subsequence for one replacement, while Go's
// `utf8.DecodeRuneInString` consumes exactly one invalid byte at a time.
fn go_json_string_fragment(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() + 2);
    output.push('"');
    let mut remaining = bytes;
    loop {
        match std::str::from_utf8(remaining) {
            Ok(valid) => {
                let encoded = crate::serde_helpers::to_go_json(&valid)
                    .expect("a Rust string is valid Go JSON");
                output.push_str(&encoded[1..encoded.len() - 1]);
                output.push('"');
                return output;
            }
            Err(error) => {
                let valid_length = error.valid_up_to();
                // SAFETY is avoided deliberately: `valid_up_to` identifies a
                // prefix that `from_utf8` has already validated.
                let valid = std::str::from_utf8(&remaining[..valid_length]).unwrap();
                let encoded = crate::serde_helpers::to_go_json(&valid)
                    .expect("a Rust string is valid Go JSON");
                output.push_str(&encoded[1..encoded.len() - 1]);
                output.push_str(r"\ufffd");
                remaining = &remaining[valid_length + 1..];
            }
        }
    }
}

impl Serialize for ColumnDefaultValue {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            ColumnDefaultValue::Null => serializer.serialize_none(),
            ColumnDefaultValue::Int(v) => serializer.serialize_i64(*v),
            ColumnDefaultValue::Uint(v) => serializer.serialize_u64(*v),
            ColumnDefaultValue::Float(v) if v.is_finite() => serializer.serialize_f64(*v),
            ColumnDefaultValue::Float(v) => {
                Err(serde::ser::Error::custom(format!("unsupported value: {v}")))
            }
            ColumnDefaultValue::Bool(v) => serializer.serialize_bool(*v),
            ColumnDefaultValue::Str(bytes) => {
                serde_json::value::RawValue::from_string(go_json_string_fragment(bytes))
                    .map_err(serde::ser::Error::custom)?
                    .serialize(serializer)
            }
            ColumnDefaultValue::Array(values) => values.serialize(serializer),
            ColumnDefaultValue::Object(values) => values.serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for ColumnDefaultValue {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct AnyVisitor;

        impl<'de> serde::de::Visitor<'de> for AnyVisitor {
            type Value = ColumnDefaultValue;

            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("a JSON value (Go any)")
            }

            fn visit_none<E>(self) -> Result<Self::Value, E> {
                Ok(ColumnDefaultValue::Null)
            }

            fn visit_unit<E>(self) -> Result<Self::Value, E> {
                Ok(ColumnDefaultValue::Null)
            }

            fn visit_bool<E>(self, v: bool) -> Result<Self::Value, E> {
                Ok(ColumnDefaultValue::Bool(v))
            }
            fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E> {
                // `encoding/json` decodes every JSON number stored in an
                // `any` field as float64, even when its lexical form is an
                // integer.
                Ok(ColumnDefaultValue::Float(v as f64))
            }
            fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E> {
                Ok(ColumnDefaultValue::Float(v as f64))
            }
            fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E> {
                Ok(ColumnDefaultValue::Float(v))
            }
            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E> {
                Ok(ColumnDefaultValue::str(v))
            }

            fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut values = Vec::with_capacity(sequence.size_hint().unwrap_or(0));
                while let Some(value) = sequence.next_element()? {
                    values.push(value);
                }
                Ok(ColumnDefaultValue::Array(values))
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut values = BTreeMap::new();
                while let Some((key, value)) = map.next_entry()? {
                    values.insert(key, value);
                }
                Ok(ColumnDefaultValue::Object(values))
            }
        }

        deserializer.deserialize_any(AnyVisitor)
    }
}

/// Go's zero-value `types.FieldType`, i.e. what Go produces for a missing
/// `"type"` key. Spelled as the empty JSON object so it stays defined by the
/// same decode path as any other field type.
fn zero_field_type() -> FieldType {
    FieldType::from_json(b"{}").expect("the empty object decodes to the zero field type")
}

/// Go `ColumnInfo`: metadata describing a table column.
///
/// The `any`-typed `OriginDefaultValue`/`DefaultValue` are modelled as
/// `Option<ColumnDefaultValue>` (`None` = Go `nil`); the accompanying `*_bit`
/// byte fields (`Option<Vec<u8>>`, `None` = Go `nil` slice) hold the BIT-type
/// default.
#[derive(Clone, Debug, Serialize)]
pub struct ColumnInfo {
    /// The column ID.
    #[serde(rename = "id", default)]
    pub id: i64,
    /// The column name.
    #[serde(rename = "name", default)]
    pub name: CiString,
    /// The column's position in the table.
    #[serde(rename = "offset", default)]
    pub offset: i64,
    /// The original default value (`any`); `None` = Go `nil`.
    #[serde(rename = "origin_default", default)]
    pub origin_default_value: Option<ColumnDefaultValue>,
    /// The BIT-type original default value bytes; `None` = Go `nil` slice.
    #[serde(
        rename = "origin_default_bit",
        default,
        with = "crate::serde_helpers::go_bytes"
    )]
    pub origin_default_value_bit: Option<Vec<u8>>,
    /// The default value (`any`); `None` = Go `nil`.
    #[serde(rename = "default", default)]
    pub default_value: Option<ColumnDefaultValue>,
    /// The BIT-type default value bytes; `None` = Go `nil` slice.
    #[serde(
        rename = "default_bit",
        default,
        with = "crate::serde_helpers::go_bytes"
    )]
    pub default_value_bit: Option<Vec<u8>>,
    /// Whether the default value string is an expression.
    #[serde(rename = "default_is_expr", default)]
    pub default_is_expr: bool,
    /// The generated-column expression, if any.
    #[serde(
        rename = "generated_expr_string",
        default,
        deserialize_with = "crate::serde_helpers::null_default"
    )]
    pub generated_expr_string: String,
    /// Whether a generated column is stored.
    #[serde(rename = "generated_stored", default)]
    pub generated_stored: bool,
    /// The columns a generated column depends on.
    #[serde(rename = "dependences", default)]
    pub dependences: GoStringSet,
    /// The column type.
    #[serde(rename = "type", default = "zero_field_type")]
    pub field_type: FieldType,
    /// The new type when modifying the column.
    #[serde(
        rename = "changing_type",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub changing_field_type: Option<Box<FieldType>>,
    /// The online-DDL state of the column.
    #[serde(rename = "state", default)]
    pub state: SchemaState,
    /// The column comment.
    #[serde(
        rename = "comment",
        default,
        deserialize_with = "crate::serde_helpers::null_default"
    )]
    pub comment: String,
    /// Whether the column is hidden (internal, e.g. expression indexes).
    #[serde(rename = "hidden", default)]
    pub hidden: bool,
    /// Schema-change info (Go's embedded `*ChangeStateInfo`). Go tags the
    /// anonymous field, which makes it a plain named field rather than an
    /// inlined one, so it serializes as `"change_state_info": {...}` / `null`.
    #[serde(rename = "change_state_info", default)]
    pub change_state_info: Option<ChangeStateInfo>,
    /// The column-info version (see the `COLUMN_INFO_VERSION*` constants).
    #[serde(rename = "version", default)]
    pub version: u64,
}

impl_go_json_merge_object!(ColumnInfo, destination, map, key, {
    if go_json_field_matches(&key, "id") {
        map.next_value_seed(NullNoopSeed(&mut destination.id))?;
    } else if go_json_field_matches(&key, "name") {
        map.next_value_seed(FatalSeed(ValueMergeSeed(&mut destination.name)))?;
    } else if go_json_field_matches(&key, "offset") {
        map.next_value_seed(NullNoopSeed(&mut destination.offset))?;
    } else if go_json_field_matches(&key, "origin_default") {
        destination.origin_default_value = map.next_value()?;
    } else if go_json_field_matches(&key, "origin_default_bit") {
        map.next_value_seed(OptionBytesSeed(&mut destination.origin_default_value_bit))?;
    } else if go_json_field_matches(&key, "default") {
        destination.default_value = map.next_value()?;
    } else if go_json_field_matches(&key, "default_bit") {
        map.next_value_seed(OptionBytesSeed(&mut destination.default_value_bit))?;
    } else if go_json_field_matches(&key, "default_is_expr") {
        map.next_value_seed(NullNoopSeed(&mut destination.default_is_expr))?;
    } else if go_json_field_matches(&key, "generated_expr_string") {
        map.next_value_seed(NullNoopSeed(&mut destination.generated_expr_string))?;
    } else if go_json_field_matches(&key, "generated_stored") {
        map.next_value_seed(NullNoopSeed(&mut destination.generated_stored))?;
    } else if go_json_field_matches(&key, "dependences") {
        map.next_value_seed(GoStringSetSeed(&mut destination.dependences))?;
    } else if go_json_field_matches(&key, "type") {
        map.next_value_seed(FatalSeed(AtomicReplaceSeed(&mut destination.field_type)))?;
    } else if go_json_field_matches(&key, "changing_type") {
        map.next_value_seed(FatalSeed(OptionScalarSeed(
            &mut destination.changing_field_type,
        )))?;
    } else if go_json_field_matches(&key, "state") {
        map.next_value_seed(NullNoopSeed(&mut destination.state))?;
    } else if go_json_field_matches(&key, "comment") {
        map.next_value_seed(NullNoopSeed(&mut destination.comment))?;
    } else if go_json_field_matches(&key, "hidden") {
        map.next_value_seed(NullNoopSeed(&mut destination.hidden))?;
    } else if go_json_field_matches(&key, "change_state_info") {
        map.next_value_seed(OptionMergeSeed(&mut destination.change_state_info))?;
    } else if go_json_field_matches(&key, "version") {
        map.next_value_seed(NullNoopSeed(&mut destination.version))?;
    } else {
        ignore_unknown(&mut map)?;
    }
});

impl_go_json_deserialize!(ColumnInfo);

impl Default for ColumnInfo {
    fn default() -> Self {
        Self {
            id: 0,
            name: CiString::default(),
            offset: 0,
            origin_default_value: None,
            origin_default_value_bit: None,
            default_value: None,
            default_value_bit: None,
            default_is_expr: false,
            generated_expr_string: String::new(),
            generated_stored: false,
            dependences: GoStringSet::default(),
            field_type: zero_field_type(),
            changing_field_type: None,
            state: SchemaState::NONE,
            comment: String::new(),
            hidden: false,
            change_state_info: None,
            version: 0,
        }
    }
}

impl ColumnInfo {
    // FieldType-delegating accessors (Go `ColumnInfo.Get*`/`Set*`, which just
    // forward to the embedded FieldType). get_type returns the typed
    // FieldTypeCode; Go returns the raw byte, but comparing against
    // FieldTypeCode::Bit/... is the faithful Rust equivalent.

    /// Go `GetType`.
    #[must_use]
    pub fn get_type(&self) -> FieldTypeCode {
        self.field_type.code()
    }
    /// Go `GetFlag`, including every bit in the target's `uint` word.
    #[must_use]
    pub fn get_flag(&self) -> u64 {
        self.field_type.raw_flags()
    }
    /// Go `GetFlen`.
    #[must_use]
    pub fn get_flen(&self) -> i64 {
        self.field_type.flen()
    }
    /// Go `GetDecimal`.
    #[must_use]
    pub fn get_decimal(&self) -> i64 {
        self.field_type.decimal()
    }
    /// Go `GetCharset`.
    #[must_use]
    pub fn get_charset(&self) -> &str {
        self.field_type.charset_name()
    }
    /// Go `GetCollate`.
    #[must_use]
    pub fn get_collate(&self) -> &str {
        self.field_type.collation_name()
    }
    /// Go `GetElems`.
    #[must_use]
    pub fn get_elems(&self) -> GoSharedSlice<GoString> {
        self.field_type.elems()
    }
    /// Go `SetType`.
    pub fn set_type(&mut self, code: FieldTypeCode) {
        self.field_type.set_code(code);
    }
    /// Go `SetFlag`.
    pub fn set_flag(&mut self, flag: u64) {
        self.field_type.set_raw_flags(flag);
    }
    /// Go `AddFlag`.
    pub fn add_flag(&mut self, flag: u64) {
        self.field_type.add_raw_flags(flag);
    }
    /// Go `AndFlag`.
    pub fn and_flag(&mut self, flag: u64) {
        self.field_type.and_raw_flags(flag);
    }
    /// Go `ToggleFlag`.
    pub fn toggle_flag(&mut self, flag: u64) {
        self.field_type.toggle_raw_flags(flag);
    }
    /// Go `DelFlag`.
    pub fn del_flag(&mut self, flag: u64) {
        self.field_type.del_raw_flags(flag);
    }
    /// Go `SetFlen`.
    pub fn set_flen(&mut self, flen: i64) {
        self.field_type.set_flen(flen);
    }
    /// Go `SetDecimal`.
    pub fn set_decimal(&mut self, decimal: i64) {
        self.field_type.set_decimal(decimal);
    }
    /// Go `SetCharset`.
    pub fn set_charset(&mut self, charset: impl Into<String>) {
        self.field_type.set_charset_name(charset);
    }
    /// Go `SetCollate`.
    pub fn set_collate(&mut self, collate: impl Into<String>) {
        self.field_type.set_collation_name(collate);
    }
    /// Go `SetElems`.
    pub fn set_elems(&mut self, elems: impl Into<GoSharedSlice<GoString>>) {
        self.field_type.set_elems(elems);
    }

    /// Go `SetOriginDefaultValue`. The value is always stored; for a BIT
    /// column, a string value is additionally kept as raw bytes, a `nil` is
    /// accepted, and any other type is rejected (Go `ErrInvalidDefault`).
    pub fn set_origin_default_value(
        &mut self,
        value: Option<ColumnDefaultValue>,
    ) -> Result<(), TerrorError> {
        self.origin_default_value = value.clone();
        if self.get_type() == FieldTypeCode::Bit {
            return self.store_bit_default(value, /* origin */ true);
        }
        Ok(())
    }

    /// Go `GetOriginDefaultValue`: for a BIT column with the bit bytes set,
    /// returns them as a string; otherwise the stored value.
    #[must_use]
    pub fn get_origin_default_value(&self) -> Option<ColumnDefaultValue> {
        if self.get_type() == FieldTypeCode::Bit {
            if let Some(bytes) = &self.origin_default_value_bit {
                return Some(ColumnDefaultValue::Str(bytes.clone()));
            }
        }
        self.origin_default_value.clone()
    }

    /// Go `SetDefaultValue` (see [`set_origin_default_value`](Self::set_origin_default_value)).
    pub fn set_default_value(
        &mut self,
        value: Option<ColumnDefaultValue>,
    ) -> Result<(), TerrorError> {
        self.default_value = value.clone();
        if self.get_type() == FieldTypeCode::Bit {
            return self.store_bit_default(value, /* origin */ false);
        }
        Ok(())
    }

    /// Go `GetDefaultValue`: the BIT bytes as a string when set, else the
    /// stored value.
    #[must_use]
    pub fn get_default_value(&self) -> Option<ColumnDefaultValue> {
        if self.get_type() == FieldTypeCode::Bit {
            if let Some(bytes) = &self.default_value_bit {
                return Some(ColumnDefaultValue::Str(bytes.clone()));
            }
        }
        self.default_value.clone()
    }

    // The shared BIT default-value rule: nil is accepted (no bytes), a string
    // is stored as raw bytes, anything else is invalid.
    fn store_bit_default(
        &mut self,
        value: Option<ColumnDefaultValue>,
        origin: bool,
    ) -> Result<(), TerrorError> {
        match value {
            None => Ok(()),
            Some(ColumnDefaultValue::Str(bytes)) => {
                if origin {
                    self.origin_default_value_bit = Some(bytes);
                } else {
                    self.default_value_bit = Some(bytes);
                }
                Ok(())
            }
            Some(_) => {
                let formatted = ERR_INVALID_DEFAULT.fast_generate(
                    errname::ErrInvalidDefault.raw,
                    &[FormatArg::from(self.name.original())],
                );
                Err(ERR_INVALID_DEFAULT.generate_with_stack(formatted.message().to_owned()))
            }
        }
    }

    /// Go `GetTypeDesc`: the column type description.
    ///
    /// Go reads the process-wide `TiDBStrictIntegerDisplayWidth` inside
    /// `FieldType.CompactStr()`. The datatype crate owns the corresponding
    /// process policy, so callers cannot accidentally format the same column
    /// under a different node policy.
    #[must_use]
    pub fn get_type_desc(&self) -> String {
        self.field_type.type_desc(STRICT_INTEGER_DISPLAY_WIDTH)
    }

    /// Go `IsGenerated`: whether the column is a generated column.
    #[must_use]
    pub fn is_generated(&self) -> bool {
        !self.generated_expr_string.is_empty()
    }
    /// Go `IsVirtualGenerated`.
    #[must_use]
    pub fn is_virtual_generated(&self) -> bool {
        self.is_generated() && !self.generated_stored
    }
    /// Go `IsChanging`.
    #[must_use]
    pub fn is_changing(&self) -> bool {
        is_changing_name(self.name.original())
    }
    /// Go `IsRemoving`.
    #[must_use]
    pub fn is_removing(&self) -> bool {
        is_removing_name(self.name.original())
    }
    /// Go `GetRemovingOriginName`.
    #[must_use]
    pub fn get_removing_origin_name(&self) -> String {
        removing_origin_name(self.name.original())
    }
    /// Go `GetChangingOriginName`.
    #[must_use]
    pub fn get_changing_origin_name(&self) -> String {
        changing_origin_name(self.name.original())
    }
}

// The extra hidden-column identifiers (Go defines these in table.go; kept
// here with the constructors that use them, to be consolidated when table.go
// is ported). CharsetBin/CollationBin are "binary".
/// Go `ExtraHandleID` (the `_tidb_rowid` handle column).
pub const EXTRA_HANDLE_ID: i64 = -1;
/// Go `ExtraPhysTblID` (the `_tidb_tid` physical-table-id column).
pub const EXTRA_PHYS_TBL_ID: i64 = -3;
/// Go `ExtraRowChecksumID`.
pub const EXTRA_ROW_CHECKSUM_ID: i64 = -4;
/// Go `ExtraCommitTSID` (the `_tidb_commit_ts` column).
pub const EXTRA_COMMIT_TS_ID: i64 = -5;
/// Go `VirtualColVecSearchDistanceID`.
pub const VIRTUAL_COL_VEC_SEARCH_DISTANCE_ID: i64 = -2000;
/// Go `VirtualColFTSScoreID`.
pub const VIRTUAL_COL_FTS_SCORE_ID: i64 = -2050;
/// Go `ExtraHandleName`.
pub const EXTRA_HANDLE_NAME: &str = "_tidb_rowid";
/// Go `ExtraPhysTblIDName`.
pub const EXTRA_PHYS_TBL_ID_NAME: &str = "_tidb_tid";
/// Go `ExtraCommitTSName`.
pub const EXTRA_COMMIT_TS_NAME: &str = "_tidb_commit_ts";

const CHARSET_BIN: &str = "binary";
const COLLATION_BIN: &str = "binary";

impl ColumnInfo {
    /// A plain public column with the given id, name, and type; every other
    /// field takes its zero value (offset set by the caller). This is the
    /// construction DDL performs when building a table's columns (Go builds
    /// the literal in `buildColumnAndConstraint`).
    #[must_use]
    pub fn new(id: i64, name: &str, field_type: FieldType) -> ColumnInfo {
        ColumnInfo {
            id,
            name: CiString::new(name),
            offset: 0,
            origin_default_value: None,
            origin_default_value_bit: None,
            default_value: None,
            default_value_bit: None,
            default_is_expr: false,
            generated_expr_string: String::new(),
            generated_stored: false,
            dependences: GoStringSet::default(),
            field_type,
            changing_field_type: None,
            state: SchemaState::PUBLIC,
            comment: String::new(),
            hidden: false,
            change_state_info: None,
            version: 0,
        }
    }

    // A base BIGINT/binary extra column: id + name, type LongLong, default
    // flen/decimal, binary charset/collation. Callers then set the flags.
    fn extra_long_long_bin(id: i64, name: &str) -> ColumnInfo {
        let mut c = ColumnInfo {
            id,
            name: CiString::new(name),
            offset: 0,
            origin_default_value: None,
            origin_default_value_bit: None,
            default_value: None,
            default_value_bit: None,
            default_is_expr: false,
            generated_expr_string: String::new(),
            generated_stored: false,
            dependences: GoStringSet::default(),
            field_type: FieldType::new(FieldTypeCode::LongLong),
            changing_field_type: None,
            state: SchemaState::NONE,
            comment: String::new(),
            hidden: false,
            change_state_info: None,
            version: 0,
        };
        let (flen, decimal) = FieldTypeCode::LongLong.default_length_and_decimal();
        c.set_flen(flen);
        c.set_decimal(decimal);
        c.set_charset(CHARSET_BIN);
        c.set_collate(COLLATION_BIN);
        c
    }

    /// Go `NewExtraHandleColInfo`: the `_tidb_rowid` handle column.
    #[must_use]
    pub fn new_extra_handle_col_info() -> ColumnInfo {
        let mut c = Self::extra_long_long_bin(EXTRA_HANDLE_ID, EXTRA_HANDLE_NAME);
        c.set_flag(u64::from(
            FieldTypeFlags::PRI_KEY | FieldTypeFlags::NOT_NULL,
        ));
        c
    }

    /// Go `NewExtraPhysTblIDColInfo`: the extra physical-table-id column.
    #[must_use]
    pub fn new_extra_phys_tbl_id_col_info() -> ColumnInfo {
        let mut c = Self::extra_long_long_bin(EXTRA_PHYS_TBL_ID, EXTRA_PHYS_TBL_ID_NAME);
        c.set_flag(u64::from(FieldTypeFlags::NOT_NULL));
        c
    }

    /// Go `NewExtraCommitTSColInfo`: the extra commit-timestamp column.
    #[must_use]
    pub fn new_extra_commit_ts_col_info() -> ColumnInfo {
        let mut c = Self::extra_long_long_bin(EXTRA_COMMIT_TS_ID, EXTRA_COMMIT_TS_NAME);
        c.set_flag(c.get_flag() | u64::from(FieldTypeFlags::UNSIGNED));
        c
    }
}

/// Go `FindColumnInfo`: finds a column by (case-insensitive) name.
#[must_use]
pub fn find_column_info(
    cols: &GoSharedPointerSlice<ColumnInfo>,
    name: &str,
) -> Option<GoShared<ColumnInfo>> {
    let name = tidb_mysql::to_lowercase(name);
    cols.iter_deref()
        .find(|column| column.read().name.lowercase() == name)
}

/// Go `FindColumnInfoByID`: finds a column by ID.
#[must_use]
pub fn find_column_info_by_id(
    cols: &GoSharedPointerSlice<ColumnInfo>,
    id: i64,
) -> Option<GoShared<ColumnInfo>> {
    cols.iter_deref().find(|column| column.read().id == id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn versions_and_change_state() {
        assert_eq!(CURR_LATEST_COLUMN_INFO_VERSION, COLUMN_INFO_VERSION2);
        assert_eq!(VIRTUAL_COL_VEC_SEARCH_DISTANCE_ID, -2000);
        assert_eq!(VIRTUAL_COL_FTS_SCORE_ID, -2050);
        assert_eq!(ChangeStateInfo::default().dependency_column_offset, 0);
    }

    #[test]
    fn removing_name() {
        assert_eq!(gen_removing_obj_name("c1"), "_Tombstone$_c1");
        // Idempotent.
        assert_eq!(gen_removing_obj_name("_Tombstone$_c1"), "_Tombstone$_c1");
        assert!(is_removing_name("_Tombstone$_c1"));
        assert!(!is_removing_name("c1"));
        assert_eq!(removing_origin_name("_Tombstone$_c1"), "c1");
        assert_eq!(removing_origin_name("c1"), "c1");
    }

    #[test]
    fn unique_changing_column_name_is_case_insensitive_and_fills_first_gap() {
        let mut table = TableInfo {
            columns: vec![
                col("_col$_Old_0", FieldTypeCode::Long),
                col("_COL$_OLD_2", FieldTypeCode::Long),
            ]
            .into(),
            ..Default::default()
        };
        let old = col("Old", FieldTypeCode::Long);
        assert_eq!(gen_unique_changing_column_name(&table, &old), "_Col$_Old_1");

        table.columns = vec![col("_Col$_i_0", FieldTypeCode::Long)].into();
        let old = col("\u{130}", FieldTypeCode::Long);
        assert_eq!(
            gen_unique_changing_column_name(&table, &old),
            "_Col$_\u{130}_1"
        );
    }

    // A minimal ColumnInfo for accessor tests.
    fn col(name: &str, code: FieldTypeCode) -> ColumnInfo {
        ColumnInfo {
            id: 0,
            name: CiString::new(name),
            offset: 0,
            origin_default_value: None,
            origin_default_value_bit: None,
            default_value: None,
            default_value_bit: None,
            default_is_expr: false,
            generated_expr_string: String::new(),
            generated_stored: false,
            dependences: GoStringSet::default(),
            field_type: FieldType::new(code),
            changing_field_type: None,
            state: SchemaState::NONE,
            comment: String::new(),
            hidden: false,
            change_state_info: None,
            version: 0,
        }
    }

    #[test]
    fn field_type_delegators() {
        use tidb_datatype::FieldTypeFlags;
        let mut c = col("c1", FieldTypeCode::Long);
        assert_eq!(c.get_type(), FieldTypeCode::Long);

        c.set_flag(u64::from(FieldTypeFlags::NOT_NULL));
        assert_eq!(c.get_flag(), u64::from(FieldTypeFlags::NOT_NULL));
        c.add_flag(u64::from(FieldTypeFlags::UNSIGNED));
        assert!(c.get_flag() & u64::from(FieldTypeFlags::UNSIGNED) != 0);
        c.del_flag(u64::from(FieldTypeFlags::NOT_NULL));
        assert_eq!(c.get_flag() & u64::from(FieldTypeFlags::NOT_NULL), 0);

        c.set_flen(20);
        assert_eq!(c.get_flen(), 20);
        c.set_decimal(4);
        assert_eq!(c.get_decimal(), 4);
        c.set_charset("utf8mb4");
        assert_eq!(c.get_charset(), "utf8mb4");
        c.set_collate("utf8mb4_bin");
        assert_eq!(c.get_collate(), "utf8mb4_bin");

        c.set_type(FieldTypeCode::Bit);
        assert_eq!(c.get_type(), FieldTypeCode::Bit);

        assert!(!c.get_elems().is_allocated());
        c.set_elems(Some(vec![GoString::from("a"), GoString::from("b")]));
        assert_eq!(c.get_elems().snapshot(), ["a", "b"]);
        c.get_elems().set(0, GoString::from("mutated"));
        assert_eq!(c.get_elems().get(0), "mutated");
        c.set_elems(Some(Vec::<GoString>::new()));
        assert!(c.get_elems().is_allocated());
        assert!(c.get_elems().is_empty());
        c.set_elems(None::<Vec<GoString>>);
        assert!(!c.get_elems().is_allocated());
    }

    #[test]
    fn generated_and_find() {
        let mut c = col("g", FieldTypeCode::Long);
        assert!(!c.is_generated());
        c.generated_expr_string = "a+1".into();
        assert!(c.is_generated());
        assert!(c.is_virtual_generated());
        c.generated_stored = true;
        assert!(!c.is_virtual_generated());

        let cols: GoSharedPointerSlice<_> = vec![
            col("Foo", FieldTypeCode::Long),
            col("bar", FieldTypeCode::Long),
        ]
        .into();
        // Case-insensitive name lookup.
        assert!(find_column_info(&cols, "FOO").is_some());
        assert!(find_column_info(&cols, "baz").is_none());
        let simple_case: GoSharedPointerSlice<_> = vec![col("i", FieldTypeCode::Long)].into();
        assert!(find_column_info(&simple_case, "\u{130}").is_some());

        cols.get(1).unwrap().write().id = 7;
        let found = find_column_info_by_id(&cols, 7).unwrap();
        assert!(found.ptr_eq(&cols.get(1).unwrap()));
        assert_eq!(found.read().name.original(), "bar");
        assert!(find_column_info_by_id(&cols, 99).is_none());

        let nullable =
            GoSharedPointerSlice::from_nullable(vec![None, Some(col("bar", FieldTypeCode::Long))]);
        assert!(std::panic::catch_unwind(|| find_column_info(&nullable, "bar")).is_err());
        assert!(std::panic::catch_unwind(|| find_column_info_by_id(&nullable, 0)).is_err());
    }

    // Go TestDefaultValue (the non-JSON assertions): plain and BIT columns,
    // including the invalid-UTF-8 bit string.
    #[test]
    fn default_value() {
        let rand_plain = ColumnDefaultValue::str("random_plain_string");
        // A BIT default of raw, non-UTF-8 bytes (Go string([]byte{25, 185})).
        let rand_bit = ColumnDefaultValue::Str(vec![25, 185]);

        // Plain column: any value round-trips as-is.
        let mut plain = col("plain", FieldTypeCode::Long);
        plain
            .set_default_value(Some(ColumnDefaultValue::Int(1)))
            .unwrap();
        assert_eq!(plain.get_default_value(), Some(ColumnDefaultValue::Int(1)));
        plain.set_default_value(Some(rand_plain.clone())).unwrap();
        assert_eq!(plain.get_default_value(), Some(rand_plain));

        // BIT column: only strings (and nil) are allowed.
        let mut bit = col("bit", FieldTypeCode::Bit);
        let err = bit
            .set_default_value(Some(ColumnDefaultValue::Int(1)))
            .unwrap_err();
        assert_eq!(err.class(), tidb_error::terror::TerrorClass::Types);
        assert_eq!(err.code().value(), 1067);
        assert_eq!(err.to_sql_error().code, 1067);
        assert_eq!(err.to_sql_error().state, "42000");
        assert_eq!(err.message(), "Invalid default value for 'bit'");
        assert!(err.stack().is_some());
        // The value was still stored before the error (as in Go).
        assert_eq!(bit.get_default_value(), Some(ColumnDefaultValue::Int(1)));
        bit.set_default_value(Some(rand_bit.clone())).unwrap();
        assert_eq!(bit.get_default_value(), Some(rand_bit));

        // BIT column with a nil origin default.
        let mut null_bit = col("nullBit", FieldTypeCode::Bit);
        null_bit.set_origin_default_value(None).unwrap();
        assert_eq!(null_bit.get_origin_default_value(), None);
    }

    #[test]
    fn default_string_json_replaces_each_invalid_go_byte() {
        let truncated = ColumnDefaultValue::Str(vec![0xe2, 0x82]);
        assert_eq!(
            serde_json::to_string(&truncated).unwrap(),
            r#""\ufffd\ufffd""#
        );

        let mixed = ColumnDefaultValue::Str(vec![b'a', 0xf0, 0x9f, b'b']);
        assert_eq!(
            serde_json::to_string(&mixed).unwrap(),
            r#""a\ufffd\ufffdb""#
        );
    }

    // Go TestDefaultValue's constructor assertions + the other extra columns.
    #[test]
    fn type_desc_suffixes() {
        // Unsigned int -> " unsigned"; zerofill adds " zerofill".
        let mut c = col("n", FieldTypeCode::Long);
        c.set_flag(u64::from(FieldTypeFlags::UNSIGNED));
        assert!(c.get_type_desc().ends_with(" unsigned"));
        c.add_flag(u64::from(FieldTypeFlags::ZEROFILL));
        let d = c.get_type_desc();
        assert!(d.contains(" unsigned"));
        assert!(d.ends_with(" zerofill"));

        // BIT excludes the unsigned suffix; YEAR excludes both.
        let mut bit = col("b", FieldTypeCode::Bit);
        bit.set_flag(u64::from(FieldTypeFlags::UNSIGNED));
        assert!(!bit.get_type_desc().contains("unsigned"));
        let mut year = col("y", FieldTypeCode::Year);
        year.set_flag(u64::from(
            FieldTypeFlags::UNSIGNED | FieldTypeFlags::ZEROFILL,
        ));
        let d = year.get_type_desc();
        assert!(!d.contains("unsigned"));
        assert!(!d.contains("zerofill"));
    }

    #[test]
    fn extra_column_constructors() {
        let phys = ColumnInfo::new_extra_phys_tbl_id_col_info();
        assert_eq!(phys.get_flag(), u64::from(FieldTypeFlags::NOT_NULL));
        assert_eq!(phys.get_type(), FieldTypeCode::LongLong);
        assert_eq!(phys.id, EXTRA_PHYS_TBL_ID);
        assert_eq!(phys.name.original(), "_tidb_tid");
        assert_eq!(phys.get_charset(), "binary");

        let handle = ColumnInfo::new_extra_handle_col_info();
        assert_eq!(
            handle.get_flag(),
            u64::from(FieldTypeFlags::PRI_KEY | FieldTypeFlags::NOT_NULL)
        );
        assert_eq!(handle.id, EXTRA_HANDLE_ID);

        let commit_ts = ColumnInfo::new_extra_commit_ts_col_info();
        assert_eq!(commit_ts.get_flag(), u64::from(FieldTypeFlags::UNSIGNED));
        assert_eq!(commit_ts.name.original(), "_tidb_commit_ts");
    }

    #[test]
    fn changing_removing_methods() {
        let c = col("_Col$_orig_0", FieldTypeCode::Long);
        assert!(c.is_changing());
        assert_eq!(c.get_changing_origin_name(), "orig");
        let c = col("_Tombstone$_orig", FieldTypeCode::Long);
        assert!(c.is_removing());
        assert_eq!(c.get_removing_origin_name(), "orig");
    }

    // Byte-for-byte fixtures captured from Go `json.Marshal(*model.ColumnInfo)`
    // (pkg/meta/model/column.go) so the Rust encoding stays pinned to
    // encoding/json's field order, base64 []byte form, and nil-as-null.
    const GO_POPULATED: &str = r#"{"id":3,"name":{"O":"Col1","L":"col1"},"offset":2,"origin_default":"abc","origin_default_bit":"GbkA","default":7,"default_bit":null,"default_is_expr":true,"generated_expr_string":"a+1","generated_stored":false,"dependences":{"a":{},"b":{}},"type":{"Tp":15,"Flag":0,"Flen":20,"Decimal":0,"Charset":"utf8mb4","Collate":"utf8mb4_bin","Elems":null,"ElemsIsBinaryLit":null,"Array":false},"changing_type":{"Tp":3,"Flag":0,"Flen":-1,"Decimal":-1,"Charset":"","Collate":"","Elems":null,"ElemsIsBinaryLit":null,"Array":false},"state":2,"comment":"hi","hidden":true,"change_state_info":{"relative_col_offset":4},"version":2}"#;

    // Go's zero-value ColumnInfo. `changing_type` is omitempty, so it is
    // absent; every other nil field is an explicit null.
    const GO_ZERO: &str = r#"{"id":0,"name":{"O":"","L":""},"offset":0,"origin_default":null,"origin_default_bit":null,"default":null,"default_bit":null,"default_is_expr":false,"generated_expr_string":"","generated_stored":false,"dependences":null,"type":{"Tp":0,"Flag":0,"Flen":0,"Decimal":0,"Charset":"","Collate":"","Elems":null,"ElemsIsBinaryLit":null,"Array":false},"state":0,"comment":"","hidden":false,"change_state_info":null,"version":0}"#;

    #[test]
    fn json_round_trips_byte_identically_with_go() {
        for fixture in [GO_POPULATED, GO_ZERO] {
            let col: ColumnInfo = serde_json::from_str(fixture).unwrap();
            assert_eq!(
                String::from_utf8(crate::serde_helpers::to_go_json(&col).unwrap()).unwrap(),
                fixture
            );
        }

        // Spot-check the decoded values, so a symmetric encode/decode bug
        // cannot hide behind the round trip.
        let col: ColumnInfo = serde_json::from_str(GO_POPULATED).unwrap();
        assert_eq!(col.id, 3);
        assert_eq!(col.name.lowercase(), "col1");
        assert_eq!(
            col.origin_default_value,
            Some(ColumnDefaultValue::str("abc"))
        );
        assert_eq!(col.origin_default_value_bit, Some(vec![25, 185, 0]));
        assert_eq!(col.default_value, Some(ColumnDefaultValue::Float(7.0)));
        assert_eq!(col.dependences.iter().collect::<Vec<_>>(), ["a", "b"]);
        assert_eq!(col.get_type(), FieldTypeCode::Varchar);
        assert_eq!(col.get_flen(), 20);
        assert_eq!(col.state, SchemaState::WRITE_ONLY);
        assert_eq!(col.change_state_info.unwrap().dependency_column_offset, 4);

        let mut allocated_empty = ColumnInfo::default();
        allocated_empty.dependences = GoStringSet::allocated(std::iter::empty());
        let encoded = serde_json::to_value(&allocated_empty).unwrap();
        assert_eq!(encoded["dependences"], serde_json::json!({}));
        let decoded: ColumnInfo = serde_json::from_value(encoded).unwrap();
        assert!(decoded.dependences.is_allocated());
        assert!(decoded.dependences.is_empty());

        // Both signs and integers beyond float64's exact-integer range follow
        // the concrete type produced by Go's `any` decoder.
        assert_eq!(
            serde_json::from_str::<ColumnDefaultValue>("-7").unwrap(),
            ColumnDefaultValue::Float(-7.0)
        );
        assert_eq!(
            serde_json::from_str::<ColumnDefaultValue>("9007199254740993").unwrap(),
            ColumnDefaultValue::Float(9_007_199_254_740_992.0)
        );
        assert_eq!(
            serde_json::from_str::<ColumnDefaultValue>("2.2250738585072012e-308").unwrap(),
            ColumnDefaultValue::Float(f64::from_bits(0x0010_0000_0000_0000))
        );
        assert_eq!(
            serde_json::from_str::<ColumnDefaultValue>(r#"{"a":[1,null,{"z":true}]}"#).unwrap(),
            ColumnDefaultValue::Object(BTreeMap::from([(
                "a".to_owned(),
                ColumnDefaultValue::Array(vec![
                    ColumnDefaultValue::Float(1.0),
                    ColumnDefaultValue::Null,
                    ColumnDefaultValue::Object(BTreeMap::from([(
                        "z".to_owned(),
                        ColumnDefaultValue::Bool(true),
                    )])),
                ]),
            )]))
        );
        for non_finite in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            assert!(
                serde_json::to_string(&ColumnDefaultValue::Float(non_finite)).is_err(),
                "Go rejects non-finite float64 value {non_finite}"
            );
        }

        for invalid in [
            r#"{"origin_default_bit":"A"}"#,
            r#"{"origin_default_bit":"AA"}"#,
            r#"{"origin_default_bit":"AAA"}"#,
            r#"{"origin_default_bit":"AA=A"}"#,
            r#"{"origin_default_bit":"AA==AAAA"}"#,
            r#"{"origin_default_bit":"AA$="}"#,
        ] {
            assert!(serde_json::from_str::<ColumnInfo>(invalid).is_err());
        }
        let with_newline: ColumnInfo =
            serde_json::from_str(r#"{"origin_default_bit":"Gbk\nA"}"#).unwrap();
        assert_eq!(
            with_newline.origin_default_value_bit,
            Some(vec![25, 185, 0])
        );

        let html = ColumnInfo {
            generated_expr_string: "a < 1 && b > 0".to_owned(),
            ..Default::default()
        };
        let encoded = String::from_utf8(crate::serde_helpers::to_go_json(&html).unwrap()).unwrap();
        assert!(encoded.contains(r#""generated_expr_string":"a \u003c 1 \u0026\u0026 b \u003e 0""#));
    }

    #[test]
    fn column_json_merges_maps_pointers_and_later_members_like_go() {
        use crate::serde_helpers::GoJsonMerge;

        let mut column = ColumnInfo {
            id: 7,
            default_value_bit: Some(vec![1]),
            dependences: GoStringSet::allocated(["kept".to_owned()]),
            change_state_info: Some(ChangeStateInfo {
                dependency_column_offset: 5,
            }),
            ..Default::default()
        };
        let mut decoder = serde_json::Deserializer::from_str(
            r#"{
                "id":null,
                "default_bit":"not base64",
                "DEPENDENCES":{"added":{}},
                "change_state_info":{},
                "COMMENT":"later"
            }"#,
        );
        assert!(column.go_json_merge(&mut decoder).is_err());
        assert_eq!(column.id, 7);
        assert_eq!(column.default_value_bit, Some(vec![1]));
        assert!(column.dependences.contains("kept"));
        assert!(column.dependences.contains("added"));
        assert_eq!(
            column
                .change_state_info
                .as_ref()
                .unwrap()
                .dependency_column_offset,
            5
        );
        assert_eq!(column.comment, "later");

        let mut decoder = serde_json::Deserializer::from_str(
            r#"{"dependences":{"temporary":{}},"DEPENDENCES":null}"#,
        );
        column.go_json_merge(&mut decoder).unwrap();
        assert!(!column.dependences.is_allocated());
        assert!(column.dependences.is_empty());

        column.name = CiString::new("Old");
        let mut decoder =
            serde_json::Deserializer::from_str(r#"{"name":{"O":"First"},"NAME":{"L":"folded"}}"#);
        column.go_json_merge(&mut decoder).unwrap();
        assert_eq!(column.name.original(), "First");
        assert_eq!(column.name.lowercase(), "folded");

        let mut decoder =
            serde_json::Deserializer::from_str(r#"{"name":"Single","comment":"after"}"#);
        column.go_json_merge(&mut decoder).unwrap();
        assert_eq!(column.name.original(), "Single");
        assert_eq!(column.name.lowercase(), "single");
        assert_eq!(column.comment, "after");

        column.name = serde_json::from_str(r#"{"O":"First","L":"folded"}"#).unwrap();
        let mut decoder = serde_json::Deserializer::from_str(
            r#"{"name":{"O":1,"L":"partial"},"comment":"unreached"}"#,
        );
        assert!(column.go_json_merge(&mut decoder).is_err());
        assert_eq!(column.name.original(), "First");
        assert_eq!(column.name.lowercase(), "partial");
        assert_eq!(column.comment, "after");

        column.field_type = FieldType::new(FieldTypeCode::LongLong);
        column.set_flen(99);
        let mut decoder = serde_json::Deserializer::from_str(r#"{"type":null}"#);
        column.go_json_merge(&mut decoder).unwrap();
        assert_eq!(column.field_type, zero_field_type());

        column.field_type = FieldType::new(FieldTypeCode::LongLong);
        column.set_flen(99);
        let previous = column.field_type.clone();
        let mut decoder = serde_json::Deserializer::from_str(r#"{"type":{"Tp":"bad"}}"#);
        assert!(column.go_json_merge(&mut decoder).is_err());
        assert_eq!(column.field_type, previous);
    }

    #[test]
    fn changing_name() {
        assert!(is_changing_name("_Col$_a_0"));
        assert!(!is_changing_name("a"));
        // Strips prefix and the trailing _<n>.
        assert_eq!(changing_origin_name("_Col$_mycol_0"), "mycol");
        assert_eq!(changing_origin_name("_Col$_my_col_3"), "my_col");
        // No trailing underscore -> whole remaining name.
        assert_eq!(changing_origin_name("_Col$_mycol"), "mycol");
    }
}
