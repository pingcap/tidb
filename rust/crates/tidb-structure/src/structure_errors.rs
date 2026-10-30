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

/// Source `structure.ErrInvalidHashKeyFlag` (`pkg/structure/structure.go:25`).
pub static ERR_INVALID_HASH_KEY_FLAG: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Structure, errcode::ErrInvalidHashKeyFlag));
/// Source `structure.ErrInvalidListIndex` (`pkg/structure/structure.go:27`).
pub static ERR_INVALID_LIST_INDEX: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Structure, errcode::ErrInvalidListIndex));
/// Source `structure.ErrInvalidListMetaData` (`pkg/structure/structure.go:29`).
pub static ERR_INVALID_LIST_META_DATA: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Structure, errcode::ErrInvalidListMetaData));
/// Source `structure.ErrWriteOnSnapshot` (`pkg/structure/structure.go:31`).
pub static ERR_WRITE_ON_SNAPSHOT: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Structure, errcode::ErrWriteOnSnapshot));

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
            super::ERR_INVALID_HASH_KEY_FLAG.class(),
            TerrorClass::Structure,
            "structure::ERR_INVALID_HASH_KEY_FLAG"
        );
        assert_eq!(
            super::ERR_INVALID_HASH_KEY_FLAG.code().value(),
            errcode::ErrInvalidHashKeyFlag as isize,
            "structure::ERR_INVALID_HASH_KEY_FLAG"
        );
        assert_eq!(
            super::ERR_INVALID_LIST_INDEX.class(),
            TerrorClass::Structure,
            "structure::ERR_INVALID_LIST_INDEX"
        );
        assert_eq!(
            super::ERR_INVALID_LIST_INDEX.code().value(),
            errcode::ErrInvalidListIndex as isize,
            "structure::ERR_INVALID_LIST_INDEX"
        );
        assert_eq!(
            super::ERR_INVALID_LIST_META_DATA.class(),
            TerrorClass::Structure,
            "structure::ERR_INVALID_LIST_META_DATA"
        );
        assert_eq!(
            super::ERR_INVALID_LIST_META_DATA
                .code()
                .value(),
            errcode::ErrInvalidListMetaData as isize,
            "structure::ERR_INVALID_LIST_META_DATA"
        );
        assert_eq!(
            super::ERR_WRITE_ON_SNAPSHOT.class(),
            TerrorClass::Structure,
            "structure::ERR_WRITE_ON_SNAPSHOT"
        );
        assert_eq!(
            super::ERR_WRITE_ON_SNAPSHOT.code().value(),
            errcode::ErrWriteOnSnapshot as isize,
            "structure::ERR_WRITE_ON_SNAPSHOT"
        );
    }
}
