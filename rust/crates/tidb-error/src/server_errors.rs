// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Server-class error prototypes from `pkg/server/err`.
//!
//! The numeric codes and message templates remain owned by the shared error
//! catalogs. This module supplies the package-level `server:<code>` identities
//! that Go creates with `dbterror.ClassServer.NewStd`.

use std::sync::LazyLock;

use crate::terror::{TerrorClass, TerrorCode, TerrorError};

fn prototype(code: u16) -> TerrorError {
    let message = crate::tidb::message_by_code(code)
        .copied()
        .expect("pkg/server/err code must exist in the pkg/errno catalog");
    TerrorError::registered_standard(
        TerrorClass::Server,
        TerrorCode::new(isize::try_from(code).expect("u16 error code must fit isize")),
        message,
    )
}

/// Go `err.ErrInvalidType`.
pub static ERR_INVALID_TYPE: LazyLock<TerrorError> =
    LazyLock::new(|| prototype(crate::tidb::errcode::ErrInvalidType));
/// Go `err.ErrInvalidSequence`.
pub static ERR_INVALID_SEQUENCE: LazyLock<TerrorError> =
    LazyLock::new(|| prototype(crate::tidb::errcode::ErrInvalidSequence));
/// Go `err.ErrNotAllowedCommand`.
pub static ERR_NOT_ALLOWED_COMMAND: LazyLock<TerrorError> =
    LazyLock::new(|| prototype(crate::tidb::errcode::ErrNotAllowedCommand));
/// Go `err.ErrAccessDenied`.
pub static ERR_ACCESS_DENIED: LazyLock<TerrorError> =
    LazyLock::new(|| prototype(crate::tidb::errcode::ErrAccessDenied));
/// Go `err.ErrAccessDeniedNoPassword`.
pub static ERR_ACCESS_DENIED_NO_PASSWORD: LazyLock<TerrorError> =
    LazyLock::new(|| prototype(crate::tidb::errcode::ErrAccessDeniedNoPassword));
/// Go `err.ErrConCount`.
pub static ERR_CON_COUNT: LazyLock<TerrorError> =
    LazyLock::new(|| prototype(crate::tidb::errcode::ErrConCount));
/// Go `err.ErrTooManyUserConnections`.
pub static ERR_TOO_MANY_USER_CONNECTIONS: LazyLock<TerrorError> =
    LazyLock::new(|| prototype(crate::tidb::errcode::ErrTooManyUserConnections));
/// Go `err.ErrSecureTransportRequired`.
pub static ERR_SECURE_TRANSPORT_REQUIRED: LazyLock<TerrorError> =
    LazyLock::new(|| prototype(crate::tidb::errcode::ErrSecureTransportRequired));
/// Go `err.ErrUserPrefixMismatch`.
pub static ERR_USER_PREFIX_MISMATCH: LazyLock<TerrorError> =
    LazyLock::new(|| prototype(crate::tidb::errcode::ErrUserPrefixMismatch));
/// Go `err.ErrMultiStatementDisabled`.
pub static ERR_MULTI_STATEMENT_DISABLED: LazyLock<TerrorError> =
    LazyLock::new(|| prototype(crate::tidb::errcode::ErrMultiStatementDisabled));
/// Go `err.ErrNewAbortingConnection`.
pub static ERR_NEW_ABORTING_CONNECTION: LazyLock<TerrorError> =
    LazyLock::new(|| prototype(crate::tidb::errcode::ErrNewAbortingConnection));
/// Go `err.ErrNotSupportedAuthMode`.
pub static ERR_NOT_SUPPORTED_AUTH_MODE: LazyLock<TerrorError> =
    LazyLock::new(|| prototype(crate::tidb::errcode::ErrNotSupportedAuthMode));
/// Go `err.ErrNetPacketTooLarge`.
pub static ERR_NET_PACKET_TOO_LARGE: LazyLock<TerrorError> =
    LazyLock::new(|| prototype(crate::tidb::errcode::ErrNetPacketTooLarge));
/// Go `err.ErrMustChangePassword`.
pub static ERR_MUST_CHANGE_PASSWORD: LazyLock<TerrorError> =
    LazyLock::new(|| prototype(crate::tidb::errcode::ErrMustChangePassword));
/// Go `err.ErrServerShutdown`.
pub static ERR_SERVER_SHUTDOWN: LazyLock<TerrorError> =
    LazyLock::new(|| prototype(crate::tidb::errcode::ErrServerShutdown));
