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
    MysqlError::from_terror(
        &tidb_util::dbterror::exeerrors::ERR_EXISTS_IN_HISTORY_PASSWORD
            .generate_with_stack(error.message),
    )
}
