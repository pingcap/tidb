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

//! Bootstrap schema-filter policy from `pkg/session/global_init.go`.
//!
//! Global-variable bootstrap loads only the `mysql` system database and does
//! not skip schema diffs. The Go filter receives `DBInfo.Name.L`, which is
//! already lower-cased by the metadata layer; this leaf keeps that exact
//! lower-case comparison and leaves domain/schema loading external.

/// The system database name used by `metadef.IsSystemDB`.
pub const SYSTEM_DB_NAME: &str = "mysql";

/// Returns whether a lower-cased database name is TiDB's system database.
#[must_use]
pub fn is_system_db(lower_name: &str) -> bool {
    lower_name == SYSTEM_DB_NAME
}

/// Returns whether the global bootstrap filter skips a schema.
///
/// `systemDBFilter.SkipLoadSchema` returns the inverse of `IsSystemDB`.
#[must_use]
pub fn skip_load_schema(lower_name: &str) -> bool {
    !is_system_db(lower_name)
}

/// Returns whether the global bootstrap filter skips a schema diff.
///
/// The source filter deliberately returns false because the domain is not
/// started while global variables are initialized.
#[must_use]
pub const fn skip_load_diff() -> bool {
    false
}
