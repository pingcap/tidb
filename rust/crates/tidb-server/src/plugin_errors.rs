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

use tidb_terror::catalog_support::{std_err, Class};
use tidb_terror::TerrorError;
use tidb_errno::errcode;

/// Source `plugin.errInvalidPluginID` (`pkg/plugin/errors.go:23`).
pub static ERR_INVALID_PLUGIN_ID: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Plugin, errcode::ErrInvalidPluginID));
/// Source `plugin.errInvalidPluginManifest` (`pkg/plugin/errors.go:24`).
pub static ERR_INVALID_PLUGIN_MANIFEST: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Plugin, errcode::ErrInvalidPluginManifest));
/// Source `plugin.errInvalidPluginName` (`pkg/plugin/errors.go:25`).
pub static ERR_INVALID_PLUGIN_NAME: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Plugin, errcode::ErrInvalidPluginName));
/// Source `plugin.errInvalidPluginVersion` (`pkg/plugin/errors.go:26`).
pub static ERR_INVALID_PLUGIN_VERSION: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Plugin, errcode::ErrInvalidPluginVersion));
/// Source `plugin.errDuplicatePlugin` (`pkg/plugin/errors.go:27`).
pub static ERR_DUPLICATE_PLUGIN: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Plugin, errcode::ErrDuplicatePlugin));
/// Source `plugin.errRequireVersionCheckFail` (`pkg/plugin/errors.go:28`).
pub static ERR_REQUIRE_VERSION_CHECK_FAIL: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Plugin, errcode::ErrRequireVersionCheckFail));
