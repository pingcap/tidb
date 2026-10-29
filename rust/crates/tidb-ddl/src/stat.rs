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

//! Go `pkg/ddl/stat.go`: the `ddl` provider of `SHOW STATUS`, which the
//! domain registers as it starts the DDL.

use std::collections::BTreeMap;
use std::sync::Arc;

use tidb_infoschema_issyncer::catalog_watch::SharedCatalog;
use tidb_variable::statusvar::{Statistics, DEFAULT_STATUS_VAR_SCOPE_FLAG};
use tidb_variable::SessionVars;

/// Go `ddl`'s `Statistics`: the server ID and the loaded schema version.
pub struct DdlStatistics {
    /// This node's server ID.
    pub id: String,
    /// The node's loaded catalog, whose schema version is reported.
    pub catalog: Arc<SharedCatalog>,
}

impl Statistics for DdlStatistics {
    fn get_scope(&self, _status: &str) -> u8 {
        // Now ddl status variables scope are all default scope.
        DEFAULT_STATUS_VAR_SCOPE_FLAG
    }

    fn stats(&self, _vars: Option<&SessionVars>) -> Result<BTreeMap<String, String>, String> {
        Ok(BTreeMap::from([
            ("server_id".to_owned(), self.id.clone()),
            (
                "ddl_schema_version".to_owned(),
                self.catalog.load().schema_version.to_string(),
            ),
        ]))
    }
}
