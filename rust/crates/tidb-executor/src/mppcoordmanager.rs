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

//! Go `pkg/executor/mppcoordmanager`: the instance's registry of running
//! MPP coordinators, which routes each TiFlash `ReportMPPTaskStatus` to the
//! coordinator that dispatched the task and drops the closed coordinators
//! that outlived every TiFlash read.

use std::collections::HashMap;
use std::sync::mpsc::{RecvTimeoutError, Sender};
use std::sync::{Arc, LazyLock, Mutex, PoisonError};
use std::thread::JoinHandle;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tidb_proto::mpp::{Error as MppError, ReportTaskStatusRequest, ReportTaskStatusResponse};
use tidb_txnkv::MppQueryId;

/// Go `InstanceMPPCoordinatorManager`: the global MPP coordinator manager.
pub static INSTANCE_MPP_COORDINATOR_MANAGER: LazyLock<MppCoordinatorManager> =
    LazyLock::new(MppCoordinatorManager::new);

/// Go `detectFrequency`.
const DETECT_FREQUENCY: Duration = Duration::from_secs(5 * 60);

/// Go `copr.TiFlashReadTimeoutUltraLong`.
const TIFLASH_READ_TIMEOUT_ULTRA_LONG: Duration = Duration::from_secs(3600);

/// What the manager asks of the Go `kv.MppCoordinator` it holds.
pub trait MppCoordinator: Send + Sync {
    /// Go `MppCoordinator.ReportStatus`: one task's report from TiFlash.
    ///
    /// # Errors
    /// The report is not one this coordinator awaits, or it does not decode.
    fn report_status(&self, request: &ReportTaskStatusRequest) -> Result<(), String>;

    /// Go `MppCoordinator.IsClosed`.
    fn is_closed(&self) -> bool;
}

/// Go `CoordinatorUniqueID`: identifies a unique coordinator.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct CoordinatorUniqueId {
    /// Go `MPPQueryID`.
    pub mpp_query_id: MppQueryId,
    /// Go `GatherID`.
    pub gather_id: u64,
}

#[derive(Default)]
struct Registry {
    server_on: bool,
    /// Empty when the status server is off.
    server_addr: String,
    coordinator_map: HashMap<CoordinatorUniqueId, Arc<dyn MppCoordinator>>,
    /// The running detector: its stop signal and its thread.
    detector: Option<(Sender<()>, JoinHandle<()>)>,
    /// Nanoseconds.
    max_life_time: u64,
}

/// Go `MPPCoordinatorManager`: manages all mpp coordinator instances.
pub struct MppCoordinatorManager {
    mu: Mutex<Registry>,
}

impl Default for MppCoordinatorManager {
    fn default() -> Self {
        Self::new()
    }
}

fn unix_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_nanos() as u64)
}

impl MppCoordinatorManager {
    /// Go `newMPPCoordinatorManger`.
    #[must_use]
    pub fn new() -> Self {
        Self {
            mu: Mutex::new(Registry::default()),
        }
    }

    fn registry(&self) -> std::sync::MutexGuard<'_, Registry> {
        self.mu.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Go `MPPCoordinatorManager.Run`: use a loop to detect and delete
    /// out-of-time coordinators.
    pub fn run(&'static self) {
        let (stop, stopped) = std::sync::mpsc::channel();
        let mut registry = self.registry();
        registry.max_life_time = (TIFLASH_READ_TIMEOUT_ULTRA_LONG + DETECT_FREQUENCY).as_nanos() as u64;
        let detector = std::thread::spawn(move || {
            while stopped.recv_timeout(DETECT_FREQUENCY) == Err(RecvTimeoutError::Timeout) {
                self.detect_and_delete(unix_nanos());
            }
        });
        registry.detector = Some((stop, detector));
    }

    /// Go `MPPCoordinatorManager.detectAndDelete`.
    fn detect_and_delete(&self, now_ts: u64) {
        let mut out_of_time_ids = Vec::new();
        {
            let mut registry = self.registry();
            let max_life_time = registry.max_life_time;
            registry.coordinator_map.retain(|id, coord| {
                // out of time: need to be deleted
                let out_of_time = now_ts > id.mpp_query_id.query_ts + max_life_time && coord.is_closed();
                if out_of_time {
                    out_of_time_ids.push(*id);
                }
                !out_of_time
            });
        }
        for delete_id in out_of_time_ids {
            tidb_metrics::executor::MPP_COORDINATOR_STATS
                .with_label_values(&["overTime"])
                .inc();
            tidb_logutil::bg_logger().error(
                "Delete MppCoordinator due to OutOfTime",
                &[
                    tidb_log::Field::new("QueryID", tidb_log::Value::U64(delete_id.mpp_query_id.local_query_id)),
                    tidb_log::Field::new("QueryTs", tidb_log::Value::U64(delete_id.mpp_query_id.query_ts)),
                ],
            );
        }
    }

    /// Go `MPPCoordinatorManager.Stop`: stop the background detector.
    pub fn stop(&self) {
        let detector = self.registry().detector.take();
        if let Some((stop, detector)) = detector {
            drop(stop);
            let _ = detector.join();
        }
    }

    /// Go `MPPCoordinatorManager.InitServerAddr`: init the grpc server
    /// address of the coordinator manager.
    pub fn init_server_addr(&self, server_on: bool, server_addr: &str) {
        let mut registry = self.registry();
        registry.server_on = server_on;
        if server_on {
            server_addr.clone_into(&mut registry.server_addr);
        }
    }

    /// Go `MPPCoordinatorManager.GetServerAddr`: whether the grpc server is
    /// on, and its address.
    #[must_use]
    pub fn get_server_addr(&self) -> (bool, String) {
        let registry = self.registry();
        (registry.server_on, registry.server_addr.clone())
    }

    /// Go `MPPCoordinatorManager.Register`: register a coordinator to the
    /// manager.
    ///
    /// # Errors
    /// A coordinator is already registered under `coord_id`.
    pub fn register(&self, coord_id: CoordinatorUniqueId, mpp_coord: Arc<dyn MppCoordinator>) -> Result<(), String> {
        let mut registry = self.registry();
        if registry.coordinator_map.contains_key(&coord_id) {
            return Err(format!(
                "Mpp coordinator already registered: {} {} {} {}",
                coord_id.mpp_query_id.query_ts,
                coord_id.mpp_query_id.local_query_id,
                coord_id.mpp_query_id.server_id,
                coord_id.gather_id
            ));
        }
        registry.coordinator_map.insert(coord_id, mpp_coord);
        tidb_metrics::executor::MPP_COORDINATOR_STATS
            .with_label_values(&["total"])
            .inc();
        tidb_metrics::executor::MPP_COORDINATOR_STATS
            .with_label_values(&["active"])
            .inc();
        Ok(())
    }

    /// Go `MPPCoordinatorManager.Unregister`: unregister a coordinator.
    pub fn unregister(&self, coord_id: CoordinatorUniqueId) {
        if self.registry().coordinator_map.remove(&coord_id).is_some() {
            tidb_metrics::executor::MPP_COORDINATOR_STATS
                .with_label_values(&["active"])
                .dec();
        }
    }

    /// Go `MPPCoordinatorManager.GetCoordCount`: the number of registered
    /// coordinators.
    #[must_use]
    pub fn get_coord_count(&self) -> usize {
        self.registry().coordinator_map.len()
    }

    /// Go `MPPCoordinatorManager.ReportStatus`: report mpp task status to
    /// the coordinator.
    #[must_use]
    pub fn report_status(&self, request: &ReportTaskStatusRequest) -> ReportTaskStatusResponse {
        let meta = request.meta.clone().unwrap_or_default();
        let mpp_query_id = MppQueryId {
            query_ts: meta.query_ts,
            local_query_id: meta.local_query_id,
            server_id: meta.server_id,
        };
        let coord_id = CoordinatorUniqueId {
            mpp_query_id,
            gather_id: meta.gather_id,
        };
        let coord = self.registry().coordinator_map.get(&coord_id).cloned();
        let mut resp = ReportTaskStatusResponse::default();
        let Some(coord) = coord else {
            resp.error = Some(MppError {
                mpp_version: meta.mpp_version,
                msg: "MppCoordinator not exists".to_owned(),
                ..Default::default()
            });
            return resp;
        };
        if let Err(err) = coord.report_status(request) {
            tidb_logutil::bg_logger().warn(
                &format!("Mpp coordinator handles ReportMPPTaskStatus met error: {err}"),
                &[
                    tidb_log::Field::new("QueryID", tidb_log::Value::U64(mpp_query_id.local_query_id)),
                    tidb_log::Field::new("QueryTs", tidb_log::Value::U64(mpp_query_id.query_ts)),
                    tidb_log::Field::new("TaskID", tidb_log::Value::I64(meta.task_id)),
                ],
            );
            resp.error = Some(MppError {
                mpp_version: meta.mpp_version,
                msg: err,
                ..Default::default()
            });
        }
        resp
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct IdleCoordinator;

    impl MppCoordinator for IdleCoordinator {
        fn report_status(&self, _request: &ReportTaskStatusRequest) -> Result<(), String> {
            Ok(())
        }

        fn is_closed(&self) -> bool {
            true
        }
    }

    fn insert(manager: &MppCoordinatorManager, query_ts: u64, gather_id: u64) {
        let unique_id = CoordinatorUniqueId {
            mpp_query_id: MppQueryId {
                query_ts,
                ..Default::default()
            },
            gather_id,
        };
        manager
            .registry()
            .coordinator_map
            .insert(unique_id, Arc::new(IdleCoordinator));
    }

    /// Go `TestDetectAndDelete`.
    #[test]
    fn test_detect_and_delete() {
        let manager = MppCoordinatorManager::new();
        let start_ts = unix_nanos();
        let max_life_time = (TIFLASH_READ_TIMEOUT_ULTRA_LONG + DETECT_FREQUENCY).as_nanos() as u64;
        manager.registry().max_life_time = max_life_time;

        // OverTime One with GatherID 1
        insert(&manager, start_ts, 1);
        // Not OverTime One with GatherID 2
        insert(&manager, start_ts + max_life_time, 2);
        // Not OverTime One with GatherID 3
        insert(&manager, start_ts + DETECT_FREQUENCY.as_nanos() as u64, 3);
        // OverTime One with GatherID 4
        insert(&manager, start_ts + Duration::from_secs(60).as_nanos() as u64, 4);
        // OverTime One with GatherID 5
        insert(&manager, start_ts + Duration::from_secs(20).as_nanos() as u64, 5);

        manager.detect_and_delete(start_ts + max_life_time + Duration::from_secs(120).as_nanos() as u64);

        let mut gather_ids = manager
            .registry()
            .coordinator_map
            .keys()
            .map(|id| id.gather_id)
            .collect::<Vec<_>>();
        gather_ids.sort_unstable();
        assert_eq!(gather_ids, [2, 3]);
    }

    /// Go `MPPCoordinatorManager.Register`, `Unregister` and
    /// `ReportStatus`: a second registration under one ID is refused, and a
    /// report reaches only a registered coordinator.
    #[test]
    fn a_report_reaches_the_registered_coordinator_only() {
        struct Refusing;
        impl MppCoordinator for Refusing {
            fn report_status(&self, _request: &ReportTaskStatusRequest) -> Result<(), String> {
                Err("ReportMPPTaskStatus task not exists taskID: 7".to_owned())
            }
            fn is_closed(&self) -> bool {
                false
            }
        }
        let manager = MppCoordinatorManager::new();
        let coord_id = CoordinatorUniqueId {
            mpp_query_id: MppQueryId {
                query_ts: 11,
                local_query_id: 12,
                server_id: 13,
            },
            gather_id: 14,
        };
        let request = ReportTaskStatusRequest {
            meta: Some(tidb_proto::mpp::TaskMeta {
                query_ts: 11,
                local_query_id: 12,
                server_id: 13,
                gather_id: 14,
                task_id: 7,
                mpp_version: 2,
                ..Default::default()
            }),
            ..Default::default()
        };
        let error = |msg: &str| {
            Some(MppError {
                mpp_version: 2,
                msg: msg.to_owned(),
                ..Default::default()
            })
        };
        assert_eq!(manager.report_status(&request).error, error("MppCoordinator not exists"));

        manager.register(coord_id, Arc::new(IdleCoordinator)).unwrap();
        assert_eq!(
            manager.register(coord_id, Arc::new(IdleCoordinator)),
            Err("Mpp coordinator already registered: 11 12 13 14".to_owned())
        );
        assert_eq!(manager.get_coord_count(), 1);
        assert_eq!(manager.report_status(&request).error, None);

        manager.unregister(coord_id);
        assert_eq!(manager.get_coord_count(), 0);
        manager.register(coord_id, Arc::new(Refusing)).unwrap();
        assert_eq!(
            manager.report_status(&request).error,
            error("ReportMPPTaskStatus task not exists taskID: 7")
        );
    }

    /// Go `MPPCoordinatorManager.InitServerAddr`: the address is kept only
    /// when the server is on.
    #[test]
    fn the_server_address_is_kept_only_when_the_server_is_on() {
        let manager = MppCoordinatorManager::new();
        manager.init_server_addr(false, "10.0.0.1:10080");
        assert_eq!(manager.get_server_addr(), (false, String::new()));
        manager.init_server_addr(true, "10.0.0.1:10080");
        assert_eq!(manager.get_server_addr(), (true, "10.0.0.1:10080".to_owned()));
    }
}
