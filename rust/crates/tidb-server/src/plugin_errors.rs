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
            super::ERR_INVALID_PLUGIN_ID.class(),
            TerrorClass::Plugin,
            "plugin::ERR_INVALID_PLUGIN_ID"
        );
        assert_eq!(
            super::ERR_INVALID_PLUGIN_ID.code().value(),
            errcode::ErrInvalidPluginID as isize,
            "plugin::ERR_INVALID_PLUGIN_ID"
        );
        assert_eq!(
            super::ERR_INVALID_PLUGIN_MANIFEST.class(),
            TerrorClass::Plugin,
            "plugin::ERR_INVALID_PLUGIN_MANIFEST"
        );
        assert_eq!(
            super::ERR_INVALID_PLUGIN_MANIFEST.code().value(),
            errcode::ErrInvalidPluginManifest as isize,
            "plugin::ERR_INVALID_PLUGIN_MANIFEST"
        );
        assert_eq!(
            super::ERR_INVALID_PLUGIN_NAME.class(),
            TerrorClass::Plugin,
            "plugin::ERR_INVALID_PLUGIN_NAME"
        );
        assert_eq!(
            super::ERR_INVALID_PLUGIN_NAME.code().value(),
            errcode::ErrInvalidPluginName as isize,
            "plugin::ERR_INVALID_PLUGIN_NAME"
        );
        assert_eq!(
            super::ERR_INVALID_PLUGIN_VERSION.class(),
            TerrorClass::Plugin,
            "plugin::ERR_INVALID_PLUGIN_VERSION"
        );
        assert_eq!(
            super::ERR_INVALID_PLUGIN_VERSION.code().value(),
            errcode::ErrInvalidPluginVersion as isize,
            "plugin::ERR_INVALID_PLUGIN_VERSION"
        );
        assert_eq!(
            super::ERR_DUPLICATE_PLUGIN.class(),
            TerrorClass::Plugin,
            "plugin::ERR_DUPLICATE_PLUGIN"
        );
        assert_eq!(
            super::ERR_DUPLICATE_PLUGIN.code().value(),
            errcode::ErrDuplicatePlugin as isize,
            "plugin::ERR_DUPLICATE_PLUGIN"
        );
        assert_eq!(
            super::ERR_REQUIRE_VERSION_CHECK_FAIL.class(),
            TerrorClass::Plugin,
            "plugin::ERR_REQUIRE_VERSION_CHECK_FAIL"
        );
        assert_eq!(
            super::ERR_REQUIRE_VERSION_CHECK_FAIL
                .code()
                .value(),
            errcode::ErrRequireVersionCheckFail as isize,
            "plugin::ERR_REQUIRE_VERSION_CHECK_FAIL"
        );
    }
}
