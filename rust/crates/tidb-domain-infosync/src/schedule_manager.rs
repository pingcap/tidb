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

//! Go `pkg/domain/infosync/schedule_manager.go`: PD's schedule configuration,
//! which FLASHBACK CLUSTER closes and restores.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

use tikv_client::pd_http;

use tidb_store_driver_txn::cluster_http::block_on_pd_http;

/// Go `ScheduleManager`: manages schedule configs.
pub trait ScheduleManager: Send + Sync + std::fmt::Debug {
    /// Go `GetScheduleConfig`.
    fn get_schedule_config(&self) -> Result<HashMap<String, serde_json::Value>, String>;
    /// Go `SetScheduleConfig`.
    fn set_schedule_config(&self, config: &HashMap<String, serde_json::Value>) -> Result<(), String>;
}

/// Go `PDScheduleManager`: manages schedule with PD.
pub struct PdScheduleManager {
    pd_http_client: pd_http::Client,
}

impl std::fmt::Debug for PdScheduleManager {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PdScheduleManager")
            .finish_non_exhaustive()
    }
}

impl PdScheduleManager {
    /// Go `initScheduleManager`'s `&PDScheduleManager{is.pdHTTPCli}`.
    #[must_use]
    pub fn new(pd_http_client: &pd_http::Client) -> Self {
        Self {
            pd_http_client: crate::pd_http::info_syncer_pd_http_client(pd_http_client),
        }
    }
}

impl ScheduleManager for PdScheduleManager {
    fn get_schedule_config(&self) -> Result<HashMap<String, serde_json::Value>, String> {
        block_on_pd_http(self.pd_http_client.get_schedule_config())
    }

    fn set_schedule_config(&self, config: &HashMap<String, serde_json::Value>) -> Result<(), String> {
        block_on_pd_http(self.pd_http_client.set_schedule_config(config))
    }
}

/// Go `mockScheduleManager`: the schedule configs of a node without PD.
#[derive(Debug, Default)]
pub struct MockScheduleManager {
    schedules: Mutex<HashMap<String, serde_json::Value>>,
}

impl ScheduleManager for MockScheduleManager {
    fn get_schedule_config(&self) -> Result<HashMap<String, serde_json::Value>, String> {
        Ok(self
            .schedules
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone())
    }

    fn set_schedule_config(&self, config: &HashMap<String, serde_json::Value>) -> Result<(), String> {
        self.schedules
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .extend(config.iter().map(|(key, value)| (key.clone(), value.clone())));
        Ok(())
    }
}

/// The `InfoSyncer`'s `scheduleManager`.
static SCHEDULE_MANAGER: Mutex<Option<Arc<dyn ScheduleManager>>> = Mutex::new(None);

/// Go `InfoSyncer.initScheduleManager`: PD's manager, or a fresh mock when
/// the node has no PD.
pub fn init_schedule_manager(manager: Option<Arc<dyn ScheduleManager>>) {
    *SCHEDULE_MANAGER
        .lock()
        .unwrap_or_else(PoisonError::into_inner) =
        Some(manager.unwrap_or_else(|| Arc::new(MockScheduleManager::default())));
}

fn schedule_manager() -> Arc<dyn ScheduleManager> {
    Arc::clone(
        SCHEDULE_MANAGER
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get_or_insert_with(|| Arc::new(MockScheduleManager::default())),
    )
}

/// Go `GetPDScheduleConfig`: the schedule information from PD.
pub fn get_pd_schedule_config() -> Result<HashMap<String, serde_json::Value>, String> {
    schedule_manager().get_schedule_config()
}

/// Go `SetPDScheduleConfig`: sets the schedule information for PD.
pub fn set_pd_schedule_config(config: &HashMap<String, serde_json::Value>) -> Result<(), String> {
    schedule_manager().set_schedule_config(config)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_mock_manager_merges_sets_and_copies_gets_like_go() {
        let manager = MockScheduleManager::default();
        assert!(manager.get_schedule_config().unwrap().is_empty());
        manager
            .set_schedule_config(&HashMap::from([
                ("merge-schedule-limit".to_owned(), serde_json::json!(1)),
                ("other".to_owned(), serde_json::json!(8)),
            ]))
            .unwrap();
        manager
            .set_schedule_config(&HashMap::from([("merge-schedule-limit".to_owned(), serde_json::json!(0))]))
            .unwrap();
        let mut copy = manager.get_schedule_config().unwrap();
        assert_eq!(copy["merge-schedule-limit"], serde_json::json!(0));
        assert_eq!(copy["other"], serde_json::json!(8));
        copy.clear();
        assert_eq!(manager.get_schedule_config().unwrap().len(), 2);
    }
}
