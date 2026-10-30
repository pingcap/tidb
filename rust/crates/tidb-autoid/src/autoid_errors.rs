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

/// Source `meta/autoid.errInvalidTableID` (`pkg/meta/autoid/errors.go:24`).
pub static ERR_INVALID_TABLE_ID: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Autoid, errcode::ErrInvalidTableID));
/// Source `meta/autoid.errInvalidIncrementAndOffset` (`pkg/meta/autoid/errors.go:25`).
pub static ERR_INVALID_INCREMENT_AND_OFFSET: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Autoid, errcode::ErrInvalidIncrementAndOffset));
/// Source `meta/autoid.errNotImplemented` (`pkg/meta/autoid/errors.go:26`).
pub static ERR_NOT_IMPLEMENTED: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Autoid, errcode::ErrNotImplemented));
/// Source `meta/autoid.ErrAutoincReadFailed` (`pkg/meta/autoid/errors.go:27`).
pub static ERR_AUTOINC_READ_FAILED: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Autoid, errcode::ErrAutoincReadFailed));
/// Source `meta/autoid.ErrWrongAutoKey` (`pkg/meta/autoid/errors.go:28`).
pub static ERR_WRONG_AUTO_KEY: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Autoid, errcode::ErrWrongAutoKey));
/// Source `meta/autoid.ErrInvalidAllocatorType` (`pkg/meta/autoid/errors.go:29`).
pub static ERR_INVALID_ALLOCATOR_TYPE: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Autoid, errcode::ErrUnknownAllocatorType));
/// Source `meta/autoid.ErrAutoRandReadFailed` (`pkg/meta/autoid/errors.go:30`).
pub static ERR_AUTO_RAND_READ_FAILED: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Autoid, errcode::ErrAutoRandReadFailed));

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
            super::ERR_INVALID_TABLE_ID.class(),
            TerrorClass::Autoid,
            "autoid::ERR_INVALID_TABLE_ID"
        );
        assert_eq!(
            super::ERR_INVALID_TABLE_ID.code().value(),
            errcode::ErrInvalidTableID as isize,
            "autoid::ERR_INVALID_TABLE_ID"
        );
        assert_eq!(
            super::ERR_INVALID_INCREMENT_AND_OFFSET.class(),
            TerrorClass::Autoid,
            "autoid::ERR_INVALID_INCREMENT_AND_OFFSET"
        );
        assert_eq!(
            super::ERR_INVALID_INCREMENT_AND_OFFSET
                .code()
                .value(),
            errcode::ErrInvalidIncrementAndOffset as isize,
            "autoid::ERR_INVALID_INCREMENT_AND_OFFSET"
        );
        assert_eq!(
            super::ERR_NOT_IMPLEMENTED.class(),
            TerrorClass::Autoid,
            "autoid::ERR_NOT_IMPLEMENTED"
        );
        assert_eq!(
            super::ERR_NOT_IMPLEMENTED.code().value(),
            errcode::ErrNotImplemented as isize,
            "autoid::ERR_NOT_IMPLEMENTED"
        );
        assert_eq!(
            super::ERR_AUTOINC_READ_FAILED.class(),
            TerrorClass::Autoid,
            "autoid::ERR_AUTOINC_READ_FAILED"
        );
        assert_eq!(
            super::ERR_AUTOINC_READ_FAILED.code().value(),
            errcode::ErrAutoincReadFailed as isize,
            "autoid::ERR_AUTOINC_READ_FAILED"
        );
        assert_eq!(
            super::ERR_WRONG_AUTO_KEY.class(),
            TerrorClass::Autoid,
            "autoid::ERR_WRONG_AUTO_KEY"
        );
        assert_eq!(
            super::ERR_WRONG_AUTO_KEY.code().value(),
            errcode::ErrWrongAutoKey as isize,
            "autoid::ERR_WRONG_AUTO_KEY"
        );
        assert_eq!(
            super::ERR_INVALID_ALLOCATOR_TYPE.class(),
            TerrorClass::Autoid,
            "autoid::ERR_INVALID_ALLOCATOR_TYPE"
        );
        assert_eq!(
            super::ERR_INVALID_ALLOCATOR_TYPE.code().value(),
            errcode::ErrUnknownAllocatorType as isize,
            "autoid::ERR_INVALID_ALLOCATOR_TYPE"
        );
        assert_eq!(
            super::ERR_AUTO_RAND_READ_FAILED.class(),
            TerrorClass::Autoid,
            "autoid::ERR_AUTO_RAND_READ_FAILED"
        );
        assert_eq!(
            super::ERR_AUTO_RAND_READ_FAILED.code().value(),
            errcode::ErrAutoRandReadFailed as isize,
            "autoid::ERR_AUTO_RAND_READ_FAILED"
        );
    }
}
