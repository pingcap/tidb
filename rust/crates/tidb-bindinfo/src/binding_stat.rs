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

//! Go `pkg/bindinfo/binding_handle.go`'s `bindingHandle` as a `SHOW STATUS`
//! provider, which the domain registers as it starts the binding handle.

use std::collections::BTreeMap;

use tidb_variable::statusvar::Statistics;
use tidb_variable::SessionVars;

use crate::binding_cache::SharedBindingCache;

/// Go `bindingHandle`'s `Statistics`: the last binding update time.
pub struct BindingStatistics(pub SharedBindingCache);

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
