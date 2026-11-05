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

//! Go `pkg/server/handler/tikvhandler/tikv_handler.go`'s
//! `FlashReplicaDeprecatedHandler`: the status port's
//! `/tiflash/replica-deprecated`, which TiFlash polls for the tables to
//! replicate and posts each table's sync progress to.

use std::collections::HashSet;
use std::sync::{Arc, Weak};

use serde::{Deserialize, Serialize};
use tidb_ddl::cluster_ddl::DdlStatement;
use tidb_ddl_build::plan_error::DdlPlanError;
use tidb_domain_core::domain::Domain;
use tidb_model::table_info::TableInfo;
use tidb_model::serde_helpers::GoValueSlice;

/// Go `FlashReplicaDeprecatedHandler`'s store as the handler reaches it.
#[derive(Clone)]
pub struct FlashReplicaDeprecated {
    /// Go `session.GetDomain(h.Store)`.
    pub domain: Weak<Domain>,
    /// Go `session.CreateSession(h.Store)`.
    pub sess_pool: Arc<dyn tidb_ddl_build::session_pool::DdlSessionPool>,
}

/// Go `TableFlashReplicaInfo` is the replica information of a table.
#[derive(Serialize)]
struct TableFlashReplicaInfo {
    // Modifying the field name needs to negotiate with TiFlash colleague.
    id: i64,
    replica_count: u64,
    location_labels: GoValueSlice<String>,
    available: bool,
    high_priority: bool,
}

/// Go `tableFlashReplicaStatus`.
#[derive(Deserialize)]
struct TableFlashReplicaStatus {
    // Modifying the field name needs to negotiate with TiFlash colleague.
    #[serde(default)]
    id: i64,
    /// RegionCount is the number of regions that need sync.
    #[serde(default)]
    region_count: u64,
    /// FlashRegionCount is the number of regions that already sync completed.
    #[serde(default)]
    flash_region_count: u64,
}

impl TableFlashReplicaStatus {
    /// Go `checkTableFlashReplicaAvailable` uses to check the available
    /// status of table flash replica.
    fn check_table_flash_replica_available(&self) -> bool {
        self.flash_region_count == self.region_count
    }
}

/// Go `FlashReplicaDeprecatedHandler.ServeHTTP`; `None` for another route.
/// A GET answers the replica list as JSON, a POST the status report's
/// error, if any.
pub fn flash_replica_deprecated_response(
    path: &str,
    request: &str,
    handler: Option<&FlashReplicaDeprecated>,
) -> Option<Result<Option<String>, String>> {
    let route = path.split_once('?').map_or(path, |(route, _)| route);
    if route != "/tiflash/replica-deprecated" {
        return None;
    }
    let Some(handler) = handler else {
        return Some(Err("the domain is not started".to_owned()));
    };
    let Some(domain) = handler.domain.upgrade() else {
        return Some(Err("the domain is closed".to_owned()));
    };
    if request.split_whitespace().next() == Some("POST") {
        let body = request.split_once("\r\n\r\n").map_or("", |(_, body)| body);
        return Some(handler.handle_status_report(&domain, body).map(|()| None));
    }
    let schema = domain.catalog_snapshot();
    let mut replica_infos = Vec::new();
    // Go `ListTablesWithSpecialAttribute(TiFlashAttribute)`.
    for database in schema.stored_schemas() {
        for tbl in database.read().deprecated_tables.iter_deref() {
            append_tiflash_replica_info(&mut replica_infos, &tbl.read());
        }
    }
    let dropped_or_truncate_replica_infos = match handler.get_drop_or_truncate_table_tiflash(&domain, &schema) {
        Ok(infos) => infos,
        Err(error) => return Some(Err(error)),
    };
    replica_infos.extend(dropped_or_truncate_replica_infos);
    Some(
        serde_json::to_string(&replica_infos)
            .map(Some)
            .map_err(|error| error.to_string()),
    )
}

fn append_tiflash_replica_info(replica_infos: &mut Vec<TableFlashReplicaInfo>, tbl_info: &TableInfo) {
    let Some(replica) = &tbl_info.tiflash_replica else {
        return;
    };
    let replica = replica.read();
    if let Some(pi) = tbl_info.get_partition_info() {
        let pi = pi.read();
        for (definitions, high_priority) in [(&pi.definitions, false), (&pi.adding_definitions, true)] {
            for p in definitions.snapshot() {
                let id = p.id;
                replica_infos.push(TableFlashReplicaInfo {
                    id,
                    replica_count: replica.count,
                    location_labels: replica.location_labels.clone(),
                    available: replica.is_partition_available(id),
                    high_priority,
                });
            }
        }
        return;
    }
    replica_infos.push(TableFlashReplicaInfo {
        id: tbl_info.id,
        replica_count: replica.count,
        location_labels: replica.location_labels.clone(),
        available: replica.available,
        high_priority: false,
    });
}

impl FlashReplicaDeprecated {
    fn get_drop_or_truncate_table_tiflash(
        &self,
        domain: &Domain,
        current_schema: &tidb_infoschema::SessionExtendedInfoSchema,
    ) -> Result<Vec<TableFlashReplicaInfo>, String> {
        let gc_safe_point = tidb_ddl::cluster_ddl::get_gc_safe_point(self.sess_pool.as_ref())
            .map_err(|error| error.to_string())?;
        let mut replica_infos = Vec::new();
        let mut unique_id_map = HashSet::new();
        let mut handle_job_and_table_info = |_: &tidb_model::Job, tbl_info: TableInfo| {
            // Avoid duplicate table ID info.
            if current_schema.table_by_id(tbl_info.id).is_some() {
                return Ok(false);
            }
            if !unique_id_map.insert(tbl_info.id) {
                return Ok(false);
            }
            append_tiflash_replica_info(&mut replica_infos, &tbl_info);
            Ok(false)
        };
        // Go `executor.GetDropOrTruncateTableInfoFromJobs`' `getTable`.
        let get_table = |start_ts: u64, schema_id: i64, table_id: i64| {
            let mut snap_meta = domain
                .get_snapshot_meta(start_ts)
                .map_err(|error| DdlPlanError::from_mysql_error(tidb_terror::MysqlError::unknown(error)))?;
            tidb_infoschema::cluster_catalog::with_meta_reader(&mut snap_meta, |reader| {
                match reader.table(schema_id, table_id) {
                    Err(tidb_meta::MetaError::DatabaseNotExists) => Ok(None),
                    other => other,
                }
            })
            .map_err(DdlPlanError::Meta)
        };
        let history = domain
            .ddl_job_history_store
            .get()
            .ok_or_else(|| "the DDL job history is unavailable".to_owned())?;
        // Go `ddl.IterAllDDLJobs`: the running jobs, then the history.
        let (running, history) = history.all_ddl_jobs()?;
        for jobs in [running, history] {
            match tidb_ddl::cluster_ddl::get_drop_or_truncate_table_info_from_jobs_by_store(
                &jobs,
                gc_safe_point,
                &get_table,
                &mut handle_job_and_table_info,
            ) {
                Ok(true) => break,
                Ok(false) => {}
                // The err indicate that current ddl job and remain DDL jobs
                // was been deleted by GC, just ignore the error and return
                // directly.
                Err(error) if error.equal(&tidb_variable::variable_errors::ERR_SNAPSHOT_TOO_OLD) => break,
                Err(error) => return Err(error.to_string()),
            }
        }
        Ok(replica_infos)
    }

    fn handle_status_report(&self, domain: &Domain, body: &str) -> Result<(), String> {
        let status: TableFlashReplicaStatus = serde_json::from_str(body).map_err(|error| error.to_string())?;
        let available = status.check_table_flash_replica_available();
        // Go writes the DDL's error and still records the progress.
        let ddl_result = domain
            .ddl
            .execute(
                &DdlStatement::UpdateTiFlashReplicaStatus {
                    table_id: status.id,
                    available,
                },
                &*self.sess_pool.system_session(),
            )
            .map(drop)
            .map_err(|error| error.error_string());
        if available {
            let table_info = TableInfo {
                id: status.id,
                ..TableInfo::default()
            };
            tidb_domain_infosync::tiflash_manager::delete_tiflash_table_sync_progress(&table_info);
        } else {
            let progress = status.flash_region_count as f64 / status.region_count as f64;
            tidb_domain_infosync::tiflash_manager::update_tiflash_progress_cache(status.id, progress);
        }
        // Go's `zap.Error(err)` carries the progress update's error, which
        // the process-wide manager never returns.
        tidb_logutil::bg_logger().info(
            "handle flash replica report",
            &[
                tidb_log::Field::new("table ID", tidb_log::Value::I64(status.id)),
                tidb_log::Field::new("region count", tidb_log::Value::U64(status.region_count)),
                tidb_log::Field::new("flash region count", tidb_log::Value::U64(status.flash_region_count)),
            ],
        );
        ddl_result
    }
}
