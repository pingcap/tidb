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

//! The node's `SHOW STATUS` providers: Go `pkg/ddl/stat.go`'s `ddl` and
//! `pkg/bindinfo/binding_handle.go`'s `bindingHandle`, which the domain
//! registers as it starts them and unregisters as it closes them.

use std::collections::BTreeMap;
use std::sync::Arc;

use tidb_infoschema_issyncer::catalog_watch::SharedCatalog;

use tidb_bindinfo::binding_cache::SharedBindingCache;
use tidb_variable::statusvar::{Statistics, DEFAULT_STATUS_VAR_SCOPE_FLAG};
use tidb_variable::SessionVars;

/// Go `ddl`'s statuses: this instance's DDL id, which is its server id, and
/// the schema version it follows.
pub(super) struct DdlStatistics {
    pub(super) id: String,
    pub(super) catalog: Arc<SharedCatalog>,
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

/// Go `bindingHandle`'s status: `LastUpdateTime().String()`, the zero time
/// until a binding was loaded.
pub(super) struct BindingStatistics(pub(super) SharedBindingCache);

impl Statistics for BindingStatistics {
    fn get_scope(&self, _status: &str) -> u8 {
        tidb_variable::sysvar::SCOPE_SESSION
    }

    fn stats(&self, _vars: Option<&SessionVars>) -> Result<BTreeMap<String, String>, String> {
        let last_update_time = Some(self.0.load().last_update_time())
            .filter(|time| !time.is_empty())
            .unwrap_or_else(|| "0000-00-00 00:00:00".to_owned());
        Ok(BTreeMap::from([(
            "last_plan_binding_update_time".to_owned(),
            last_update_time,
        )]))
    }
}
