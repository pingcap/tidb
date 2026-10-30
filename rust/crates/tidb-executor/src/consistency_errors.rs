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

/// Source `util/logutil/consistency.ErrAdminCheckInconsistent` (`pkg/util/logutil/consistency/reporter.go:43`).
pub static ERR_ADMIN_CHECK_INCONSISTENT: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Admin, errcode::ErrDataInconsistent));
/// Source `util/logutil/consistency.ErrLookupInconsistent` (`pkg/util/logutil/consistency/reporter.go:45`).
pub static ERR_LOOKUP_INCONSISTENT: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Executor, errcode::ErrDataInconsistentMismatchCount));
/// Source `util/logutil/consistency.ErrAdminCheckInconsistentWithColInfo` (`pkg/util/logutil/consistency/reporter.go:47`).
pub static ERR_ADMIN_CHECK_INCONSISTENT_WITH_COL_INFO: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Executor, errcode::ErrDataInconsistentMismatchIndex));

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
            super::ERR_ADMIN_CHECK_INCONSISTENT.class(),
            TerrorClass::Admin,
            "consistency::ERR_ADMIN_CHECK_INCONSISTENT"
        );
        assert_eq!(
            super::ERR_ADMIN_CHECK_INCONSISTENT
                .code()
                .value(),
            errcode::ErrDataInconsistent as isize,
            "consistency::ERR_ADMIN_CHECK_INCONSISTENT"
        );
        assert_eq!(
            super::ERR_LOOKUP_INCONSISTENT.class(),
            TerrorClass::Executor,
            "consistency::ERR_LOOKUP_INCONSISTENT"
        );
        assert_eq!(
            super::ERR_LOOKUP_INCONSISTENT.code().value(),
            errcode::ErrDataInconsistentMismatchCount as isize,
            "consistency::ERR_LOOKUP_INCONSISTENT"
        );
        assert_eq!(
            super::ERR_ADMIN_CHECK_INCONSISTENT_WITH_COL_INFO.class(),
            TerrorClass::Executor,
            "consistency::ERR_ADMIN_CHECK_INCONSISTENT_WITH_COL_INFO"
        );
        assert_eq!(
            super::ERR_ADMIN_CHECK_INCONSISTENT_WITH_COL_INFO
                .code()
                .value(),
            errcode::ErrDataInconsistentMismatchIndex as isize,
            "consistency::ERR_ADMIN_CHECK_INCONSISTENT_WITH_COL_INFO"
        );
    }
}
