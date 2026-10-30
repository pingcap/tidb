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

/// Source `session.ErrForUpdateCantRetry` (`pkg/session/tidb.go:452`).
pub static ERR_FOR_UPDATE_CANT_RETRY: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Session, errcode::ErrForUpdateCantRetry));
/// Source `session.ErrNonTransactionalJobFailure` (`pkg/session/nontransactional.go:53`).
pub static ERR_NON_TRANSACTIONAL_JOB_FAILURE: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Session, errcode::ErrNonTransactionalJobFailure));
/// Source `session.errResultIsEmpty` (`pkg/session/session.go:4102`).
pub static ERR_RESULT_IS_EMPTY: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Executor, errcode::ErrResultIsEmpty));

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
            super::ERR_FOR_UPDATE_CANT_RETRY.class(),
            TerrorClass::Session,
            "session::ERR_FOR_UPDATE_CANT_RETRY"
        );
        assert_eq!(
            super::ERR_FOR_UPDATE_CANT_RETRY.code().value(),
            errcode::ErrForUpdateCantRetry as isize,
            "session::ERR_FOR_UPDATE_CANT_RETRY"
        );
        assert_eq!(
            super::ERR_NON_TRANSACTIONAL_JOB_FAILURE.class(),
            TerrorClass::Session,
            "session::ERR_NON_TRANSACTIONAL_JOB_FAILURE"
        );
        assert_eq!(
            super::ERR_NON_TRANSACTIONAL_JOB_FAILURE
                .code()
                .value(),
            errcode::ErrNonTransactionalJobFailure as isize,
            "session::ERR_NON_TRANSACTIONAL_JOB_FAILURE"
        );
        assert_eq!(
            super::ERR_RESULT_IS_EMPTY.class(),
            TerrorClass::Executor,
            "session::ERR_RESULT_IS_EMPTY"
        );
        assert_eq!(
            super::ERR_RESULT_IS_EMPTY.code().value(),
            errcode::ErrResultIsEmpty as isize,
            "session::ERR_RESULT_IS_EMPTY"
        );
    }
}
