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

//! Loading a cluster's `SET GLOBAL` overrides out of `mysql.global_variables`.
//!
//! Go's `GetGlobalSysVar` reads this table lazily, one row at a time, through
//! `GlobalVarAccessor`; this node instead reads the whole table once, at
//! startup, into [`tidb_session::GlobalSysvars`] -- the same one-shot
//! trade-off [`crate::cluster_privilege_load`] already documents for
//! accounts. A node that only does this one-shot startup load (no
//! `crate::cluster_sysvar_write` seam wired in) never sees a peer's later
//! `SET GLOBAL` until it restarts; the convergence node closes that gap for
//! its OWN `SET GLOBAL` statements the same way `cluster_account_write`
//! closes it for account statements -- see `tidb_server::cluster_sysvar_seam`
//! -- but still relies on its 30-second etcd-backed reload tick (mirroring
//! Go's `LoadSysVarCacheLoop`) to notice a Go peer's write, since this
//! module's own read is snapshot-once by design.
//!
//! `mysql.global_variables` carries exactly two columns, `VARIABLE_NAME` and
//! `VARIABLE_VALUE` (see
//! `tidb_metadef::system_tables_def::CREATE_GLOBAL_VARIABLES_TABLE`), the same
//! shape `mysql.tidb` has -- and this reader is the same shape as
//! [`crate::cluster_privilege_load::read_bootstrap_state`] for exactly that
//! reason.

use crate::cluster_catalog::{ClusterCatalog, MetaSnapshot};
use crate::mysql_system_tables::{scan_system_table, SystemRow, SystemTableError, SystemTableView};

/// The `mysql.global_variables` columns this loader reads.
const GLOBAL_VARIABLES_COLUMNS: &[&str] = &["variable_name", "variable_value"];

/// Reads every `SET GLOBAL` override a cluster's `mysql.global_variables`
/// currently holds, as `(name, value)` pairs.
///
/// A cluster with no such table at all -- not yet bootstrapped, or
/// bootstrapped by a Go version too old to have created it -- reads as no
/// overrides rather than an error: every variable then answers with its
/// build default, which is exactly what a keyspace with no stored row means
/// for [`tidb_session::GlobalSysvars::get`].
pub fn load_cluster_sysvars<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
) -> Result<Vec<(String, String)>, SystemTableError> {
    let Ok(view) = SystemTableView::locate(catalog, "global_variables", GLOBAL_VARIABLES_COLUMNS)
    else {
        return Ok(Vec::new());
    };
    let mut loaded = Vec::new();
    for value in scan_system_table(snapshot, &view)? {
        let row = SystemRow::parse(&view, &value)?;
        let Some(name) = row.text("variable_name")? else {
            continue;
        };
        let value = row.text("variable_value")?.unwrap_or_default();
        loaded.push((name, value));
    }
    Ok(loaded)
}

#[cfg(test)]
mod tests {
    use crate::cluster_catalog::{ClusterCatalogError, MetaPairs};

    use super::*;

    /// A snapshot of an empty keyspace: no meta keys, no record ranges.
    struct EmptySnapshot;

    impl MetaSnapshot for EmptySnapshot {
        fn get(&mut self, _key: &[u8]) -> Result<Option<Vec<u8>>, ClusterCatalogError> {
            Ok(None)
        }

        fn scan_prefix(&mut self, _prefix: &[u8]) -> Result<MetaPairs, ClusterCatalogError> {
            Ok(Vec::new())
        }
    }

    #[test]
    fn a_cluster_with_no_global_variables_table_loads_as_no_overrides() {
        let catalog = ClusterCatalog {
            schema_version: 0,
            databases: Vec::new(),
        };
        let loaded = load_cluster_sysvars(&mut EmptySnapshot, &catalog)
            .expect("a missing table reads as empty, not an error");
        assert!(loaded.is_empty());
    }
}
