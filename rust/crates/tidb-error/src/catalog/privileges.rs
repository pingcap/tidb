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

use super::{std_err, Class};
use crate::terror::TerrorError;
use crate::tidb::errcode;

/// Source `privilege/privileges.errInvalidPrivilegeType` (`pkg/privilege/privileges/errors.go:24`).
pub static ERR_INVALID_PRIVILEGE_TYPE: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Privilege, errcode::ErrInvalidPrivilegeType));
/// Source `privilege/privileges.ErrNonexistingGrant` (`pkg/privilege/privileges/errors.go:25`).
pub static ERR_NONEXISTING_GRANT: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Privilege, errcode::ErrNonexistingGrant));
/// Source `privilege/privileges.errLoadPrivilege` (`pkg/privilege/privileges/errors.go:26`).
pub static ERR_LOAD_PRIVILEGE: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Privilege, errcode::ErrLoadPrivilege));
/// Source `privilege/privileges.ErrAccessDenied` (`pkg/privilege/privileges/errors.go:27`).
pub static ERR_ACCESS_DENIED: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Privilege, errcode::ErrAccessDenied));
/// Source `privilege/privileges.errAccountHasBeenLocked` (`pkg/privilege/privileges/errors.go:28`).
pub static ERR_ACCOUNT_HAS_BEEN_LOCKED: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Privilege, errcode::ErrAccountHasBeenLocked));
/// Source `privilege/privileges.ErUserAccessDeniedForUserAccountBlockedByPasswordLock` (`pkg/privilege/privileges/errors.go:29`).
pub static ER_USER_ACCESS_DENIED_FOR_USER_ACCOUNT_BLOCKED_BY_PASSWORD_LOCK: LazyLock<TerrorError> =
    LazyLock::new(|| {
        std_err(
            Class::Privilege,
            errcode::ErUserAccessDeniedForUserAccountBlockedByPasswordLock,
        )
    });
/// Source `privilege/privileges.ErrMustChangePasswordLogin` (`pkg/privilege/privileges/errors.go:30`).
pub static ERR_MUST_CHANGE_PASSWORD_LOGIN: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Privilege, errcode::ErrMustChangePasswordLogin));
