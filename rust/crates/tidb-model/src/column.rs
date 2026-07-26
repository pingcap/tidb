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
//! DEFERRED to a focused follow-up (documented at the methods): the `any`
//! default-value logic with bit-type byte encoding (`SetDefaultValue`/
//! `GetDefaultValue`), which relies on Go `interface{}` JSON round-trip
//! semantics and invalid-UTF-8 bit strings (Go's `TestDefaultValue`);
//! `GetTypeDesc`; the `NewExtra*ColInfo` constructors; and
//! `GenUniqueChangingColumnName` (needs the unported `TableInfo`).

use std::collections::BTreeSet;

use tidb_ast::CiString;
use tidb_datatype::{FieldType, FieldTypeCode};

use crate::schema_state::SchemaState;

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

/// Go `ChangeStateInfo`: records schema-change information for a column.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ChangeStateInfo {
    /// The offset of the changing column this column depends on during a
    /// modify/change column.
    pub dependency_column_offset: i32,
}

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

/// Go `ColumnInfo`: metadata describing a table column.
///
/// The `any`-typed `OriginDefaultValue`/`DefaultValue` are modelled as
/// [`serde_json::Value`]; the accompanying `*_bit` byte fields hold the
/// BIT-type default. The default-value accessors are a deferred follow-up
/// (see the module note).
#[derive(Clone, Debug)]
pub struct ColumnInfo {
    /// The column ID.
    pub id: i64,
    /// The column name.
    pub name: CiString,
    /// The column's position in the table.
    pub offset: i32,
    /// The original default value (`any`).
    pub origin_default_value: Option<serde_json::Value>,
    /// The BIT-type original default value bytes.
    pub origin_default_value_bit: Vec<u8>,
    /// The default value (`any`).
    pub default_value: Option<serde_json::Value>,
    /// The BIT-type default value bytes.
    pub default_value_bit: Vec<u8>,
    /// Whether the default value string is an expression.
    pub default_is_expr: bool,
    /// The generated-column expression, if any.
    pub generated_expr_string: String,
    /// Whether a generated column is stored.
    pub generated_stored: bool,
    /// The columns a generated column depends on.
    pub dependences: BTreeSet<String>,
    /// The column type.
    pub field_type: FieldType,
    /// The new type when modifying the column.
    pub changing_field_type: Option<Box<FieldType>>,
    /// The online-DDL state of the column.
    pub state: SchemaState,
    /// The column comment.
    pub comment: String,
    /// Whether the column is hidden (internal, e.g. expression indexes).
    pub hidden: bool,
    /// Schema-change info (Go's embedded `*ChangeStateInfo`).
    pub change_state_info: Option<ChangeStateInfo>,
    /// The column-info version (see the `COLUMN_INFO_VERSION*` constants).
    pub version: u64,
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
    /// Go `GetFlag`.
    #[must_use]
    pub fn get_flag(&self) -> u32 {
        self.field_type.flags()
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
    pub fn get_elems(&self) -> &[String] {
        self.field_type.elems()
    }
    /// Go `SetType`.
    pub fn set_type(&mut self, code: FieldTypeCode) {
        self.field_type.set_code(code);
    }
    /// Go `SetFlag`.
    pub fn set_flag(&mut self, flag: u32) {
        self.field_type.set_flags(flag);
    }
    /// Go `AddFlag`.
    pub fn add_flag(&mut self, flag: u32) {
        self.field_type.add_flags(flag);
    }
    /// Go `AndFlag`.
    pub fn and_flag(&mut self, flag: u32) {
        self.field_type.and_flags(flag);
    }
    /// Go `ToggleFlag`.
    pub fn toggle_flag(&mut self, flag: u32) {
        self.field_type.toggle_flags(flag);
    }
    /// Go `DelFlag`.
    pub fn del_flag(&mut self, flag: u32) {
        self.field_type.del_flags(flag);
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
    pub fn set_elems(&mut self, elems: Vec<String>) {
        self.field_type.set_elems(elems);
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

/// Go `FindColumnInfo`: finds a column by (case-insensitive) name.
#[must_use]
pub fn find_column_info<'a>(cols: &'a [ColumnInfo], name: &str) -> Option<&'a ColumnInfo> {
    let name = name.to_lowercase();
    cols.iter().find(|col| col.name.lowercase() == name)
}

/// Go `FindColumnInfoByID`: finds a column by ID.
#[must_use]
pub fn find_column_info_by_id(cols: &[ColumnInfo], id: i64) -> Option<&ColumnInfo> {
    cols.iter().find(|col| col.id == id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn versions_and_change_state() {
        assert_eq!(CURR_LATEST_COLUMN_INFO_VERSION, COLUMN_INFO_VERSION2);
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

    // A minimal ColumnInfo for accessor tests.
    fn col(name: &str, code: FieldTypeCode) -> ColumnInfo {
        ColumnInfo {
            id: 0,
            name: CiString::new(name),
            offset: 0,
            origin_default_value: None,
            origin_default_value_bit: Vec::new(),
            default_value: None,
            default_value_bit: Vec::new(),
            default_is_expr: false,
            generated_expr_string: String::new(),
            generated_stored: false,
            dependences: BTreeSet::new(),
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

        c.set_flag(FieldTypeFlags::NOT_NULL);
        assert_eq!(c.get_flag(), FieldTypeFlags::NOT_NULL);
        c.add_flag(FieldTypeFlags::UNSIGNED);
        assert!(c.get_flag() & FieldTypeFlags::UNSIGNED != 0);
        c.del_flag(FieldTypeFlags::NOT_NULL);
        assert_eq!(c.get_flag() & FieldTypeFlags::NOT_NULL, 0);

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

        c.set_elems(vec!["a".into(), "b".into()]);
        assert_eq!(c.get_elems(), &["a".to_string(), "b".to_string()]);
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

        let cols = vec![
            col("Foo", FieldTypeCode::Long),
            col("bar", FieldTypeCode::Long),
        ];
        // Case-insensitive name lookup.
        assert!(find_column_info(&cols, "FOO").is_some());
        assert!(find_column_info(&cols, "baz").is_none());

        let mut cols = cols;
        cols[1].id = 7;
        assert_eq!(
            find_column_info_by_id(&cols, 7).unwrap().name.original(),
            "bar"
        );
        assert!(find_column_info_by_id(&cols, 99).is_none());
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
