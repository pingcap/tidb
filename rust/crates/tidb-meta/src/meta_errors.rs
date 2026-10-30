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

//! Error prototypes defined by the Go package(s) named per entry.
//!
//! GENERATED mechanically from the Go sources by scanning every
//! package-level `Class*.NewStd` / `NewStdErr` definition; the (class,
//! code) set of every module is cross-checked against the real Go
//! registry (`terror.ErrClassToMySQLCodes`) by
//! `tests/catalog_source.rs`. Do not hand-edit entries.

use std::sync::LazyLock;

use tidb_dbterror::catalog_support::{std_err, Class};
use tidb_terror::TerrorError;
use tidb_errno::errcode;

/// Source `meta.ErrDBExists` (`pkg/meta/meta.go:151`).
pub static ERR_DB_EXISTS: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Meta, errcode::ErrDBCreateExists));
/// Source `meta.ErrDBNotExists` (`pkg/meta/meta.go:153`).
pub static ERR_DB_NOT_EXISTS: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Meta, errcode::ErrBadDB));
/// Source `meta.ErrPolicyExists` (`pkg/meta/meta.go:155`).
pub static ERR_POLICY_EXISTS: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Meta, errcode::ErrPlacementPolicyExists));
/// Source `meta.ErrPolicyNotExists` (`pkg/meta/meta.go:157`).
pub static ERR_POLICY_NOT_EXISTS: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Meta, errcode::ErrPlacementPolicyNotExists));
/// Source `meta.ErrMaskingPolicyExists` (`pkg/meta/meta.go:159`).
pub static ERR_MASKING_POLICY_EXISTS: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Meta, errcode::ErrMaskingPolicyExists));
/// Source `meta.ErrMaskingPolicyNotExists` (`pkg/meta/meta.go:161`).
pub static ERR_MASKING_POLICY_NOT_EXISTS: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Meta, errcode::ErrMaskingPolicyNotExists));
/// Source `meta.ErrMaskingPolicyExprInvalidColumn` (`pkg/meta/meta.go:163`).
pub static ERR_MASKING_POLICY_EXPR_INVALID_COLUMN: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Meta, errcode::ErrMaskingPolicyExprInvalidColumn));
/// Source `meta.ErrResourceGroupExists` (`pkg/meta/meta.go:165`).
pub static ERR_RESOURCE_GROUP_EXISTS: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Meta, errcode::ErrResourceGroupExists));
/// Source `meta.ErrResourceGroupNotExists` (`pkg/meta/meta.go:167`).
pub static ERR_RESOURCE_GROUP_NOT_EXISTS: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Meta, errcode::ErrResourceGroupNotExists));
/// Source `meta.ErrTableExists` (`pkg/meta/meta.go:169`).
pub static ERR_TABLE_EXISTS: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Meta, errcode::ErrTableExists));
/// Source `meta.ErrTableNotExists` (`pkg/meta/meta.go:171`).
pub static ERR_TABLE_NOT_EXISTS: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Meta, errcode::ErrNoSuchTable));
/// Source `meta.ErrDDLReorgElementNotExist` (`pkg/meta/meta.go:173`).
pub static ERR_DDL_REORG_ELEMENT_NOT_EXIST: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Meta, errcode::ErrDDLReorgElementNotExist));
/// Source `meta.ErrInvalidString` (`pkg/meta/meta.go:175`).
pub static ERR_INVALID_STRING: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Meta, errcode::ErrInvalidCharacterString));

#[cfg(test)]
mod go_class_code_tests {
    use tidb_errno::errcode;
    use tidb_terror::TerrorClass;

    /// Each prototype carries the class and code of the Go definition it
    /// transcribes (`Class.NewStd(code)`), so `Error()` prints Go's
    /// `[class:code]`.
    #[test]
    fn prototypes_carry_their_go_class_and_code() {
        assert_eq!(
            super::ERR_DB_EXISTS.class(),
            TerrorClass::Meta,
            "meta::ERR_DB_EXISTS"
        );
        assert_eq!(
            super::ERR_DB_EXISTS.code().value(),
            errcode::ErrDBCreateExists as isize,
            "meta::ERR_DB_EXISTS"
        );
        assert_eq!(
            super::ERR_DB_NOT_EXISTS.class(),
            TerrorClass::Meta,
            "meta::ERR_DB_NOT_EXISTS"
        );
        assert_eq!(
            super::ERR_DB_NOT_EXISTS.code().value(),
            errcode::ErrBadDB as isize,
            "meta::ERR_DB_NOT_EXISTS"
        );
        assert_eq!(
            super::ERR_POLICY_EXISTS.class(),
            TerrorClass::Meta,
            "meta::ERR_POLICY_EXISTS"
        );
        assert_eq!(
            super::ERR_POLICY_EXISTS.code().value(),
            errcode::ErrPlacementPolicyExists as isize,
            "meta::ERR_POLICY_EXISTS"
        );
        assert_eq!(
            super::ERR_POLICY_NOT_EXISTS.class(),
            TerrorClass::Meta,
            "meta::ERR_POLICY_NOT_EXISTS"
        );
        assert_eq!(
            super::ERR_POLICY_NOT_EXISTS.code().value(),
            errcode::ErrPlacementPolicyNotExists as isize,
            "meta::ERR_POLICY_NOT_EXISTS"
        );
        assert_eq!(
            super::ERR_MASKING_POLICY_EXISTS.class(),
            TerrorClass::Meta,
            "meta::ERR_MASKING_POLICY_EXISTS"
        );
        assert_eq!(
            super::ERR_MASKING_POLICY_EXISTS.code().value(),
            errcode::ErrMaskingPolicyExists as isize,
            "meta::ERR_MASKING_POLICY_EXISTS"
        );
        assert_eq!(
            super::ERR_MASKING_POLICY_NOT_EXISTS.class(),
            TerrorClass::Meta,
            "meta::ERR_MASKING_POLICY_NOT_EXISTS"
        );
        assert_eq!(
            super::ERR_MASKING_POLICY_NOT_EXISTS.code().value(),
            errcode::ErrMaskingPolicyNotExists as isize,
            "meta::ERR_MASKING_POLICY_NOT_EXISTS"
        );
        assert_eq!(
            super::ERR_MASKING_POLICY_EXPR_INVALID_COLUMN.class(),
            TerrorClass::Meta,
            "meta::ERR_MASKING_POLICY_EXPR_INVALID_COLUMN"
        );
        assert_eq!(
            super::ERR_MASKING_POLICY_EXPR_INVALID_COLUMN
                .code()
                .value(),
            errcode::ErrMaskingPolicyExprInvalidColumn as isize,
            "meta::ERR_MASKING_POLICY_EXPR_INVALID_COLUMN"
        );
        assert_eq!(
            super::ERR_RESOURCE_GROUP_EXISTS.class(),
            TerrorClass::Meta,
            "meta::ERR_RESOURCE_GROUP_EXISTS"
        );
        assert_eq!(
            super::ERR_RESOURCE_GROUP_EXISTS.code().value(),
            errcode::ErrResourceGroupExists as isize,
            "meta::ERR_RESOURCE_GROUP_EXISTS"
        );
        assert_eq!(
            super::ERR_RESOURCE_GROUP_NOT_EXISTS.class(),
            TerrorClass::Meta,
            "meta::ERR_RESOURCE_GROUP_NOT_EXISTS"
        );
        assert_eq!(
            super::ERR_RESOURCE_GROUP_NOT_EXISTS.code().value(),
            errcode::ErrResourceGroupNotExists as isize,
            "meta::ERR_RESOURCE_GROUP_NOT_EXISTS"
        );
        assert_eq!(
            super::ERR_TABLE_EXISTS.class(),
            TerrorClass::Meta,
            "meta::ERR_TABLE_EXISTS"
        );
        assert_eq!(
            super::ERR_TABLE_EXISTS.code().value(),
            errcode::ErrTableExists as isize,
            "meta::ERR_TABLE_EXISTS"
        );
        assert_eq!(
            super::ERR_TABLE_NOT_EXISTS.class(),
            TerrorClass::Meta,
            "meta::ERR_TABLE_NOT_EXISTS"
        );
        assert_eq!(
            super::ERR_TABLE_NOT_EXISTS.code().value(),
            errcode::ErrNoSuchTable as isize,
            "meta::ERR_TABLE_NOT_EXISTS"
        );
        assert_eq!(
            super::ERR_DDL_REORG_ELEMENT_NOT_EXIST.class(),
            TerrorClass::Meta,
            "meta::ERR_DDL_REORG_ELEMENT_NOT_EXIST"
        );
        assert_eq!(
            super::ERR_DDL_REORG_ELEMENT_NOT_EXIST
                .code()
                .value(),
            errcode::ErrDDLReorgElementNotExist as isize,
            "meta::ERR_DDL_REORG_ELEMENT_NOT_EXIST"
        );
        assert_eq!(
            super::ERR_INVALID_STRING.class(),
            TerrorClass::Meta,
            "meta::ERR_INVALID_STRING"
        );
        assert_eq!(
            super::ERR_INVALID_STRING.code().value(),
            errcode::ErrInvalidCharacterString as isize,
            "meta::ERR_INVALID_STRING"
        );
    }
}
