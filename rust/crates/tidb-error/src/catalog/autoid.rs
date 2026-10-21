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
