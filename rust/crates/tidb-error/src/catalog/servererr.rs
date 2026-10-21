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

/// Source `server/err.ErrInvalidType` (`pkg/server/err/error.go:24`).
pub static ERR_INVALID_TYPE: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Server, errcode::ErrInvalidType));
/// Source `server/err.ErrInvalidSequence` (`pkg/server/err/error.go:26`).
pub static ERR_INVALID_SEQUENCE: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Server, errcode::ErrInvalidSequence));
/// Source `server/err.ErrNotAllowedCommand` (`pkg/server/err/error.go:28`).
pub static ERR_NOT_ALLOWED_COMMAND: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Server, errcode::ErrNotAllowedCommand));
/// Source `server/err.ErrAccessDenied` (`pkg/server/err/error.go:30`).
pub static ERR_ACCESS_DENIED: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Server, errcode::ErrAccessDenied));
/// Source `server/err.ErrAccessDeniedNoPassword` (`pkg/server/err/error.go:32`).
pub static ERR_ACCESS_DENIED_NO_PASSWORD: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Server, errcode::ErrAccessDeniedNoPassword));
/// Source `server/err.ErrConCount` (`pkg/server/err/error.go:34`).
pub static ERR_CON_COUNT: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Server, errcode::ErrConCount));
/// Source `server/err.ErrTooManyUserConnections` (`pkg/server/err/error.go:36`).
pub static ERR_TOO_MANY_USER_CONNECTIONS: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Server, errcode::ErrTooManyUserConnections));
/// Source `server/err.ErrSecureTransportRequired` (`pkg/server/err/error.go:38`).
pub static ERR_SECURE_TRANSPORT_REQUIRED: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Server, errcode::ErrSecureTransportRequired));
/// Source `server/err.ErrUserPrefixMismatch` (`pkg/server/err/error.go:40`).
pub static ERR_USER_PREFIX_MISMATCH: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Server, errcode::ErrUserPrefixMismatch));
/// Source `server/err.ErrMultiStatementDisabled` (`pkg/server/err/error.go:42`).
pub static ERR_MULTI_STATEMENT_DISABLED: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Server, errcode::ErrMultiStatementDisabled));
/// Source `server/err.ErrNewAbortingConnection` (`pkg/server/err/error.go:44`).
pub static ERR_NEW_ABORTING_CONNECTION: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Server, errcode::ErrNewAbortingConnection));
/// Source `server/err.ErrNotSupportedAuthMode` (`pkg/server/err/error.go:46`).
pub static ERR_NOT_SUPPORTED_AUTH_MODE: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Server, errcode::ErrNotSupportedAuthMode));
/// Source `server/err.ErrNetPacketTooLarge` (`pkg/server/err/error.go:48`).
pub static ERR_NET_PACKET_TOO_LARGE: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Server, errcode::ErrNetPacketTooLarge));
/// Source `server/err.ErrMustChangePassword` (`pkg/server/err/error.go:50`).
pub static ERR_MUST_CHANGE_PASSWORD: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Server, errcode::ErrMustChangePassword));
/// Source `server/err.ErrServerShutdown` (`pkg/server/err/error.go:52`).
pub static ERR_SERVER_SHUTDOWN: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Server, errcode::ErrServerShutdown));
