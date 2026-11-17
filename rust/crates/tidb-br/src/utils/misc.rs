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

//! Go `br/pkg/utils/schema.go` and `misc.go`: the helpers the backup and
//! restore clients share.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tidb_model::TableInfo;
use tidb_proto::backup::File;
use tidb_proto::metapb;

use crate::errors::{self, Result, ERR_KV_STORAGE};

/// Go `temporaryDBNamePrefix`.
pub const TEMPORARY_DB_NAME_PREFIX: &str = "__TiDB_BR_Temporary_";
/// Go `TidbNewCollationEnabled`.
pub const TIDB_NEW_COLLATION_ENABLED: &str = "new_collation_enabled";
/// Go `LabelRuleBatchSize`.
pub const LABEL_RULE_BATCH_SIZE: usize = 50;
/// Go `storeDisconnectionDuration`.
const STORE_DISCONNECTION_DURATION: Duration = Duration::from_secs(100);

/// Go `mysql.SystemDB`.
pub const SYSTEM_DB: &str = "mysql";
/// Go `mysql.SysDB`.
pub const SYS_DB: &str = "sys";
/// Go `mysql.WorkloadSchema`.
pub const WORKLOAD_SCHEMA: &str = "workload_schema";

/// Go `NeedAutoID`.
#[must_use]
pub fn need_auto_id(t: &TableInfo) -> bool {
    let has_row_id = !t.pk_is_handle && !t.is_common_handle;
    has_row_id || t.get_auto_increment_col_info().is_some()
}

/// Go `EncloseName`.
#[must_use]
pub fn enclose_name(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

/// Go `IsTemplateSysDB`.
#[must_use]
pub fn is_template_sys_db(db_name_o: &str) -> bool {
    db_name_o == format!("{TEMPORARY_DB_NAME_PREFIX}{SYSTEM_DB}")
        || db_name_o == format!("{TEMPORARY_DB_NAME_PREFIX}{SYS_DB}")
}

/// Go `IsSysDB`.
#[must_use]
pub fn is_sys_db(db_lower_name: &str) -> bool {
    db_lower_name == SYSTEM_DB || db_lower_name == SYS_DB || db_lower_name == WORKLOAD_SCHEMA
}

/// Go `TemporaryDBName`.
#[must_use]
pub fn temporary_db_name(db: &str) -> String {
    format!("{TEMPORARY_DB_NAME_PREFIX}{db}")
}

/// Go `StripTempDBPrefixIfNeeded`.
#[must_use]
pub fn strip_temp_db_prefix_if_needed(temp_db: &str) -> &str {
    temp_db
        .strip_prefix(TEMPORARY_DB_NAME_PREFIX)
        .unwrap_or(temp_db)
}

/// Go `StripTempDBPrefix`.
#[must_use]
pub fn strip_temp_db_prefix(temp_db: &str) -> (&str, bool) {
    match temp_db.strip_prefix(TEMPORARY_DB_NAME_PREFIX) {
        Some(s) => (s, true),
        None => (temp_db, false),
    }
}

/// Go `IsSysOrTempSysDB`.
#[must_use]
pub fn is_sys_or_temp_sys_db(db: &str) -> bool {
    is_sys_db(strip_temp_db_prefix_if_needed(db))
}

/// Go `metapb.StoreState.String`.
fn store_state_name(state: i32) -> String {
    match metapb::StoreState::try_from(state) {
        Ok(metapb::StoreState::Up) => "Up".into(),
        Ok(metapb::StoreState::Offline) => "Offline".into(),
        Ok(metapb::StoreState::Tombstone) => "Tombstone".into(),
        Err(_) => state.to_string(),
    }
}

/// Go `CheckStoreLiveness`: up or going offline, with a heartbeat in the
/// last 100 seconds when PD reports one.
///
/// # Errors
/// [`ERR_KV_STORAGE`].
pub fn check_store_liveness(s: &metapb::Store) -> Result<()> {
    if s.state != metapb::StoreState::Up as i32 && s.state != metapb::StoreState::Offline as i32 {
        return Err(errors::annotatef(
            &ERR_KV_STORAGE,
            format!(
                "the store state isn't up, it is {}",
                store_state_name(s.state)
            ),
        ));
    }
    if s.last_heartbeat > 0 {
        let last = UNIX_EPOCH + Duration::from_nanos(s.last_heartbeat as u64);
        let since = SystemTime::now().duration_since(last).unwrap_or_default();
        if since > STORE_DISCONNECTION_DURATION {
            return Err(errors::annotatef(
                &ERR_KV_STORAGE,
                format!(
                    "the store last heartbeat is too far, at {}",
                    tidb_model::go_duration::format_go_duration(since.as_nanos() as i64)
                ),
            ));
        }
    }
    Ok(())
}

/// Go `SummaryFiles`: the files' checksum, and their KV and size totals
/// recorded in the summary.
#[must_use]
pub fn summary_files(files: &[File]) -> (u64, u64, u64) {
    let (mut crc, mut kvs, mut bytes) = (0_u64, 0_u64, 0_u64);
    let mut cf_count: std::collections::BTreeMap<String, i64> = std::collections::BTreeMap::new();
    for f in files {
        *cf_count.entry(f.cf.clone()).or_default() += 1;
        crate::summary::collect_success_unit(
            crate::summary::TOTAL_KV,
            1,
            crate::summary::SuccessArg::Data(f.total_kvs),
        );
        crate::summary::collect_success_unit(
            crate::summary::TOTAL_BYTES,
            1,
            crate::summary::SuccessArg::Data(f.total_bytes),
        );
        crc ^= f.crc64xor;
        kvs = kvs.wrapping_add(f.total_kvs);
        bytes = bytes.wrapping_add(f.total_bytes);
    }
    for (cf, count) in cf_count {
        crate::summary::collect_int(&format!("{cf} CF files"), count);
    }
    (crc, kvs, bytes)
}

#[cfg(test)]
mod tests;
