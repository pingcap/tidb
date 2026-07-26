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
//! DEFERRED to a focused follow-up: the `ColumnInfo` struct itself and its
//! ~30 methods. They delegate heavily to `types.FieldType` (whose Rust API in
//! tidb-datatype differs from Go's and needs reconciliation), carry `any`
//! default-value fields with bit-type encoding (`SetDefaultValue`/
//! `GetDefaultValue`, exercised by Go's `TestDefaultValue`), format types
//! (`GetTypeDesc`), and include constructors and `GenUniqueChangingColumnName`
//! (which needs the not-yet-ported `TableInfo`).

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
