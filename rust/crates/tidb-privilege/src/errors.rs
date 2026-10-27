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

//! The errors Go's `pkg/privilege/privileges` and the account statements
//! raise over it, each the `MysqlError` Go's `ErrX.GenWithStackByArgs(...)`
//! renders.

use tidb_error::mysql::MysqlError;

/// Go `exeerrors.ErrPasswordFormat` (1827).
#[must_use]
pub fn password_format() -> MysqlError {
    MysqlError::terror(
        &tidb_util::dbterror::exeerrors::ERR_PASSWORD_FORMAT,
        "The password hash doesn't have the expected format. Check if the correct \
         password algorithm is being used with the PASSWORD() function."
            .to_owned(),
    )
}

/// Go `exeerrors.ErrPluginIsNotLoaded` (1524).
#[must_use]
pub fn plugin_is_not_loaded(plugin: &str) -> MysqlError {
    MysqlError::terror(
        &tidb_util::dbterror::exeerrors::ERR_PLUGIN_IS_NOT_LOADED,
        format!("Plugin '{plugin}' is not loaded"),
    )
}

/// Go `exeerrors.ErrExistsInHistoryPassword` (3638).
#[must_use]
pub fn exists_in_history_password(user: &str, host: &str) -> MysqlError {
    let message = tidb_error::tidb::errname::ErrExistsInHistoryPassword;
    let error = tidb_error::mysql::SqlError::new_f(
        tidb_error::tidb::errcode::ErrExistsInHistoryPassword,
        message.raw,
        message.redact_arg_pos,
        &[
            tidb_error::mysql::FormatArg::from(user),
            tidb_error::mysql::FormatArg::from(host),
        ],
    );
    MysqlError::terror(
        &tidb_util::dbterror::exeerrors::ERR_EXISTS_IN_HISTORY_PASSWORD,
        error.message,
    )
}

/// Go `kv.ErrKeyExists` (1062).
#[must_use]
pub fn duplicate_entry(value: &str, key: &str) -> MysqlError {
    MysqlError::terror(
        &tidb_error::catalog::kv::ERR_KEY_EXISTS,
        format!("Duplicate entry '{value}' for key '{key}'"),
    )
}
