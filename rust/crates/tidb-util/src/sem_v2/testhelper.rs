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

//! Go `testhelper.go`: the helpers other packages' tests use to drive SEM v2.

use std::collections::BTreeMap;

use super::{
    config::parse_sem_config_from_file, disable, enable, get_sys_var, load_global_sem, set_sys_var,
};

/// Go `EnableFromPathForTest`: enables SEM v2 from a configuration file and
/// returns the cleanup that disables it and restores the variables the config
/// overrode.
///
/// # Errors
///
/// Propagates the parse and validation errors.
pub fn enable_from_path_for_test(config_path: &str) -> Result<impl FnOnce(), String> {
    let sem_config = parse_sem_config_from_file(config_path)?;

    let mut variable_def_value = BTreeMap::new();
    for var in &sem_config.restricted_variables {
        if !var.value.is_empty() {
            let Some(sys_var) = get_sys_var(&var.name) else {
                continue;
            };
            variable_def_value.insert(var.name.clone(), sys_var.value);
        }
    }

    enable(config_path)?;

    Ok(move || {
        disable();
        for (name, value) in &variable_def_value {
            set_sys_var(name, value);
        }
    })
}

/// Go `AddRestrictedPrivilegesForTest`. Not safe to use while SEM is being read
/// from multiple threads.
pub fn add_restricted_privileges_for_test(privilege: &str) {
    if let Some(sem) = load_global_sem() {
        sem.add_restricted_privilege(privilege.to_uppercase());
    }
}

/// Go `RemoveRestrictedPrivilegesForTest`. Not safe to use while SEM is being
/// read from multiple threads.
pub fn remove_restricted_privileges_for_test(privilege: &str) {
    if let Some(sem) = load_global_sem() {
        sem.remove_restricted_privilege(&privilege.to_uppercase());
    }
}
