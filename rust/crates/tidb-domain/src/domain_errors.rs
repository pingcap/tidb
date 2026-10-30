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

use tidb_dbterror::catalog_support::{std_err, std_err_msg, Class};
use tidb_terror::TerrorError;
use tidb_errno::errcode;

/// Source `domain.ErrInfoSchemaExpired` (`pkg/domain/domain.go:3020`).
pub static ERR_INFO_SCHEMA_EXPIRED: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Domain, errcode::ErrInfoSchemaExpired));
/// Source `domain.ErrInfoSchemaChanged` (`pkg/domain/domain.go:3022`).
pub static ERR_INFO_SCHEMA_CHANGED: LazyLock<TerrorError> = LazyLock::new(|| {
    std_err_msg(
        Class::Domain,
        errcode::ErrInfoSchemaChanged,
        errcode::ErrInfoSchemaChanged,
        ". [try again later]",
    )
});

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
            super::ERR_INFO_SCHEMA_EXPIRED.class(),
            TerrorClass::Domain,
            "domain::ERR_INFO_SCHEMA_EXPIRED"
        );
        assert_eq!(
            super::ERR_INFO_SCHEMA_EXPIRED.code().value(),
            errcode::ErrInfoSchemaExpired as isize,
            "domain::ERR_INFO_SCHEMA_EXPIRED"
        );
        assert_eq!(
            super::ERR_INFO_SCHEMA_CHANGED.class(),
            TerrorClass::Domain,
            "domain::ERR_INFO_SCHEMA_CHANGED"
        );
        assert_eq!(
            super::ERR_INFO_SCHEMA_CHANGED.code().value(),
            errcode::ErrInfoSchemaChanged as isize,
            "domain::ERR_INFO_SCHEMA_CHANGED"
        );
    }
}
