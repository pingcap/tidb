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

//! Go `UserPrivileges.RequestVerification`'s early arms (`privileges.go`):
//! the answers SEM and the virtual schemas give before any stored grant is
//! consulted.

use tidb_hack::{GoCaseCompare, GoToLower};
use tidb_mysql::PrivilegeType;

use crate::PrivilegeTypeExt;

/// Go `metadef.IsMemDB`: the three virtual schemas whose privilege answers
/// are decided by `UserPrivileges.RequestVerification`'s own early arms
/// (`privileges.go` around line 194) rather than by any stored grant.
fn is_mem_db(database: &str) -> bool {
    database.go_lower_eq("information_schema")
        || database.go_lower_eq("performance_schema")
        || database.go_lower_eq("metrics_schema")
}

fn is_mem_or_sys_db(database: &str) -> bool {
    database.go_lower_eq("mysql") || is_mem_db(database)
}

/// SEM's hard table rule, evaluated before stored grants and before the
/// ordinary virtual-schema rule below. `None` means SEM has no opinion and
/// the normal privilege path decides.
pub fn sem_verdict_mask(
    database: &str,
    table: &str,
    mask: u64,
    has_restricted_tables_admin: bool,
) -> Option<bool> {
    if !tidb_sem_compat::is_enabled() || has_restricted_tables_admin {
        return None;
    }
    let database_lower = database.go_to_lower();
    let table_lower = table.go_to_lower();
    if tidb_sem_compat::is_invisible_table(&database_lower, &table_lower) {
        return Some(false);
    }
    const SEM_REFUSED_WRITES: &[PrivilegeType] = &[
        tidb_mysql::CreatePriv,
        tidb_mysql::AlterPriv,
        tidb_mysql::DropPriv,
        tidb_mysql::IndexPriv,
        tidb_mysql::CreateViewPriv,
        tidb_mysql::InsertPriv,
        tidb_mysql::UpdatePriv,
        tidb_mysql::DeletePriv,
    ];
    if is_mem_or_sys_db(&database_lower)
        && SEM_REFUSED_WRITES
            .iter()
            .any(|privilege| privilege.bit() == mask)
    {
        return Some(false);
    }
    None
}

/// The fixed answer `RequestVerification` gives for a virtual schema before
/// it consults a single grant, or `None` when the stored grants decide.
///
/// `mask` is Go's `priv` argument, which is a `mysql.PrivilegeType` and may
/// carry several bits. That matters here: Go's write refusal is a
/// `switch priv { case mysql.CreatePriv, ...: }`, an EQUALITY test, so a
/// multi-bit mask -- the "any privilege" question `SHOW TABLES` and the
/// `information_schema` retrievers ask -- never enters it and falls straight
/// through to the `information_schema` admission below. Matching on the
/// whole mask rather than on a decoded privilege is what keeps that true
/// without a second rule.
///
/// Go refuses every write-shaped privilege on all three virtual schemas
/// (`privileges.go` around line 194) and then admits EVERYTHING on
/// `information_schema` (around line 201), which is what makes `SELECT ...
/// FROM information_schema.*` need no grant at all while
/// `performance_schema` and `metrics_schema` still consult stored grants.
pub fn mem_db_verdict_mask(database: &str, mask: u64) -> Option<bool> {
    if !is_mem_db(database) {
        return None;
    }
    const REFUSED: &[PrivilegeType] = &[
        tidb_mysql::CreatePriv,
        tidb_mysql::AlterPriv,
        tidb_mysql::DropPriv,
        tidb_mysql::IndexPriv,
        tidb_mysql::CreateViewPriv,
        tidb_mysql::InsertPriv,
        tidb_mysql::UpdatePriv,
        tidb_mysql::DeletePriv,
        tidb_mysql::ReferencesPriv,
        tidb_mysql::ExecutePriv,
        tidb_mysql::ShowViewPriv,
        tidb_mysql::LockTablesPriv,
    ];
    if REFUSED.iter().any(|priv_| priv_.bit() == mask) {
        return Some(false);
    }
    database.go_lower_eq("information_schema").then_some(true)
}
