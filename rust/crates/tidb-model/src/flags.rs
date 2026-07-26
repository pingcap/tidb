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

//! `pkg/meta/model/flags.go`: the `tipb.SelectRequest.Flags` bit flags that
//! control execution mode (how truncate/overflow/zero-date errors are
//! handled, and which statement kind is running).

/// Ignore truncate errors (read-only statements do; writes don't).
pub const FLAG_IGNORE_TRUNCATE: u64 = 1;
/// Return truncate errors as warnings (only when [`FLAG_IGNORE_TRUNCATE`] is
/// unset): a warning in non-strict SQL mode, an error in strict mode.
pub const FLAG_TRUNCATE_AS_WARNING: u64 = 1 << 1;
/// `sql_mode` `PAD_CHAR_TO_FULL_LENGTH` is set.
pub const FLAG_PAD_CHAR_TO_FULL_LENGTH: u64 = 1 << 2;
/// This is an `INSERT` statement.
pub const FLAG_IN_INSERT_STMT: u64 = 1 << 3;
/// This is an `UPDATE` or `DELETE` statement.
pub const FLAG_IN_UPDATE_OR_DELETE_STMT: u64 = 1 << 4;
/// This is a `SELECT` statement.
pub const FLAG_IN_SELECT_STMT: u64 = 1 << 5;
/// Return overflow errors as warnings (non-strict SQL mode) rather than
/// errors (strict mode).
pub const FLAG_OVERFLOW_AS_WARNING: u64 = 1 << 6;
/// Ignore zero-in-date errors (read-only statements do).
pub const FLAG_IGNORE_ZERO_IN_DATE: u64 = 1 << 7;
/// Return divided-by-zero as a warning.
pub const FLAG_DIVIDED_BY_ZERO_AS_WARNING: u64 = 1 << 8;
/// This is a `UNION`/`EXCEPT`/`INTERSECT` statement.
pub const FLAG_IN_SET_OPR_STMT: u64 = 1 << 9;
/// This is a `LOAD DATA` statement.
pub const FLAG_IN_LOAD_DATA_STMT: u64 = 1 << 10;
/// This request is a restricted (internal) SQL, e.g. auto-analyze.
pub const FLAG_IN_RESTRICTED_SQL: u64 = 1 << 11;
