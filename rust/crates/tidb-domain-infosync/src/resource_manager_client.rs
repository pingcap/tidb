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

//! The resource-manager half of Go `pkg/domain/infosync` that the DDL job
//! workers call (a seed of that package, not its port): the
//! `ResourceManagerClient` writes `infosync.AddResourceGroup`,
//! `ModifyResourceGroup` and `DeleteResourceGroup` make, and
//! `resource_manager_client.go`'s `mockResourceManagerClient`, which a store
//! without PD gets (`InfoSyncer.initResourceManagerClient(nil)`).

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError, RwLock};

use tidb_proto::resource_manager as rmpb;

/// Go `constants.NullKeyspaceID`.
pub const NULL_KEYSPACE_ID: u32 = 0xFFFF_FFFF;

/// The writes of Go's `pd.ResourceManagerClient` the DDL job workers make.
pub trait ResourceManagerClient: Send + Sync {
    /// Go `AddResourceGroup`.
    fn add_resource_group(&self, group: rmpb::ResourceGroup) -> Result<String, String>;
    /// Go `ModifyResourceGroup`.
    fn modify_resource_group(&self, group: rmpb::ResourceGroup) -> Result<String, String>;
    /// Go `DeleteResourceGroup`.
    fn delete_resource_group(&self, name: &str) -> Result<String, String>;
    /// Go `GetResourceGroup`, for the resource-groups controller of a store
    /// whose PD client is this one (unistore's `pdClient`).
    fn get_resource_group(&self, name: &str) -> Result<rmpb::ResourceGroup, String> {
        Err(format!(
            "the group {name} cannot be read through this client"
        ))
    }
    /// Go `ListResourceGroups`, as [`Self::get_resource_group`].
    fn list_resource_groups(&self) -> Result<Vec<rmpb::ResourceGroup>, String> {
        Err("resource groups cannot be listed through this client".to_owned())
    }
}

/// Go `mockResourceManagerClient`: groups held in memory, starting with the
/// `default` group Go seeds.
pub struct MockResourceManagerClient {
    groups: RwLock<HashMap<String, rmpb::ResourceGroup>>,
}

impl MockResourceManagerClient {
    /// Go `NewMockResourceManagerClient`.
    #[must_use]
    pub fn new() -> Self {
        let default = rmpb::ResourceGroup {
            name: tidb_model::DEFAULT_RESOURCE_GROUP_NAME.to_owned(),
            mode: rmpb::GroupMode::RuMode as i32,
            r_u_settings: Some(rmpb::GroupRequestUnitSettings {
                r_u: Some(rmpb::TokenBucket {
                    settings: Some(rmpb::TokenLimitSettings {
                        fill_rate: i32::MAX as u64,
                        burst_limit: -1,
                        ..rmpb::TokenLimitSettings::default()
                    }),
                    ..rmpb::TokenBucket::default()
                }),
                ..rmpb::GroupRequestUnitSettings::default()
            }),
            priority: 8,
            ..rmpb::ResourceGroup::default()
        };
        Self {
            groups: RwLock::new([(default.name.clone(), default)].into_iter().collect()),
        }
    }

    /// Go `GetResourceGroup`.
    pub fn get_resource_group(&self, name: &str) -> Result<rmpb::ResourceGroup, String> {
        self.groups
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(name)
            .cloned()
            .ok_or_else(|| format!("the group {name} does not exist"))
    }

    /// Go `ListResourceGroups`.
    #[must_use]
    pub fn list_resource_groups(&self) -> Vec<rmpb::ResourceGroup> {
        self.groups
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .values()
            .cloned()
            .collect()
    }
}

impl Default for MockResourceManagerClient {
    fn default() -> Self {
        Self::new()
    }
}

impl ResourceManagerClient for MockResourceManagerClient {
    fn get_resource_group(&self, name: &str) -> Result<rmpb::ResourceGroup, String> {
        MockResourceManagerClient::get_resource_group(self, name)
    }

    fn list_resource_groups(&self) -> Result<Vec<rmpb::ResourceGroup>, String> {
        Ok(MockResourceManagerClient::list_resource_groups(self))
    }

    fn add_resource_group(&self, group: rmpb::ResourceGroup) -> Result<String, String> {
        let mut groups = self.groups.write().unwrap_or_else(PoisonError::into_inner);
        if groups.contains_key(&group.name) {
            return Err(format!("the group {} already exists", group.name));
        }
        groups.insert(group.name.clone(), group);
        Ok("Success!".to_owned())
    }

    fn modify_resource_group(&self, group: rmpb::ResourceGroup) -> Result<String, String> {
        self.groups
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(group.name.clone(), group);
        Ok("Success!".to_owned())
    }

    fn delete_resource_group(&self, name: &str) -> Result<String, String> {
        self.groups
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(name);
        Ok("Success!".to_owned())
    }
}

/// The `InfoSyncer`'s `resourceManagerClient`.
static CLIENT: Mutex<Option<Arc<dyn ResourceManagerClient>>> = Mutex::new(None);

fn client() -> Arc<dyn ResourceManagerClient> {
    Arc::clone(
        CLIENT
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get_or_insert_with(|| Arc::new(MockResourceManagerClient::new())),
    )
}

/// Go `InfoSyncer.initResourceManagerClient`: PD's client, or the mock when
/// there is none.
pub fn init_resource_manager_client(client: Option<Arc<dyn ResourceManagerClient>>) {
    *CLIENT.lock().unwrap_or_else(PoisonError::into_inner) =
        Some(client.unwrap_or_else(|| Arc::new(MockResourceManagerClient::new())));
}

/// Go `infosync.GetResourceGroup`.
pub fn get_resource_group(name: &str) -> Result<rmpb::ResourceGroup, String> {
    client().get_resource_group(name)
}

/// Go `infosync.ListResourceGroups`.
pub fn list_resource_groups() -> Result<Vec<rmpb::ResourceGroup>, String> {
    client().list_resource_groups()
}

/// Go `infosync.AddResourceGroup`.
pub fn add_resource_group(group: rmpb::ResourceGroup) -> Result<(), String> {
    client().add_resource_group(group).map(|_| ())
}

/// Go `infosync.ModifyResourceGroup`.
pub fn modify_resource_group(group: rmpb::ResourceGroup) -> Result<(), String> {
    client().modify_resource_group(group).map(|_| ())
}

/// Go `infosync.DeleteResourceGroup`.
pub fn delete_resource_group(name: &str) -> Result<(), String> {
    client().delete_resource_group(name).map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mock_seeds_default_and_refuses_a_second_add() {
        let mock = MockResourceManagerClient::new();
        assert_eq!(mock.get_resource_group("default").unwrap().priority, 8);
        let group = rmpb::ResourceGroup {
            name: "rg1".to_owned(),
            ..rmpb::ResourceGroup::default()
        };
        mock.add_resource_group(group.clone()).unwrap();
        assert_eq!(
            mock.add_resource_group(group),
            Err("the group rg1 already exists".to_owned())
        );
        mock.delete_resource_group("rg1").unwrap();
        assert_eq!(
            mock.get_resource_group("rg1"),
            Err("the group rg1 does not exist".to_owned())
        );
    }
}
