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

//! Go `pkg/executor/distribute.go` (`DistributeTableExec`,
//! `CancelDistributionJobExec`) and `show.go`'s `fetchShowDistributionJobs`:
//! PD's `balance-range-scheduler` jobs through the info syncer.

use std::collections::HashMap;

use tidb_datatype::Datum;
use tidb_planner::common_plans::DistributeTable;
use tikv_client::pd_http;

use crate::DriverError;

/// Go `schedulerName`.
pub const SCHEDULER_NAME: &str = "balance-range-scheduler";

fn unknown(message: String) -> DriverError {
    DriverError::Mysql(tidb_terror::MysqlError::unknown(message))
}

/// Go `DistributeTableExec`.
pub struct DistributeTableExec {
    p: DistributeTable,
    key_ranges: Vec<pd_http::KeyRange>,
}

impl DistributeTableExec {
    /// Go `executorBuilder.buildDistributeTable` and `Open`: the key ranges
    /// of the selected physical tables, the partition names sorted.
    ///
    /// # Errors
    /// A partition the table does not have.
    pub fn open(mut p: DistributeTable) -> Result<Self, DriverError> {
        let key_ranges = get_key_ranges(&p)?;
        p.partition_names.sort_by(|i, j| i.lowercase().cmp(j.lowercase()));
        Ok(Self { p, key_ranges })
    }

    /// Go `DistributeTableExec.Next`: creates the scheduler job, then reads
    /// its ID back (three tries, 500ms apart; the scheduler service may not
    /// have seen the config yet). `None` when it was never found.
    ///
    /// # Errors
    /// PD's refusal of the job.
    pub fn next(&self) -> Result<Option<u64>, DriverError> {
        self.distribute_table()?;
        let mut job_id = None;
        for i in 0..3 {
            if let Some(id) = self.get_scheduler_job() {
                job_id = Some(id);
                break;
            }
            if i < 2 {
                std::thread::sleep(std::time::Duration::from_millis(500));
            }
        }
        Ok(job_id.map(|id| id as u64))
    }

    /// Go `getSchedulerJob`: the newest unfinished job of this alias, engine
    /// and rule.
    fn get_scheduler_job(&self) -> Option<f64> {
        let config = match tidb_domain_infosync::region::get_scheduler_config(SCHEDULER_NAME) {
            Ok(config) => config,
            Err(error) => {
                tidb_logutil::bg_logger().info(
                    "get scheduler config failed",
                    &[tidb_logutil::zap_error(error)],
                );
                return None;
            }
        };
        let Some(configs) = config.as_array() else {
            tidb_logutil::bg_logger().info("get empty scheduler config", &[]);
            return None;
        };
        let alias = self.get_alias();
        let mut job_id = -1.0;
        for job in configs {
            let Some(job) = job.as_object() else {
                tidb_logutil::bg_logger().info("get invalid scheduler config", &[]);
                return None;
            };
            let field = |name: &str| job.get(name).and_then(serde_json::Value::as_str);
            // PD ensures the aliases of the uncompleted jobs differ.
            if field("alias") == Some(alias.as_str())
                && field("engine") == Some(self.p.engine.as_str())
                && field("rule") == Some(self.p.rule.as_str())
                && field("status") != Some("finished")
            {
                if let Some(id) = job.get("job-id").and_then(serde_json::Value::as_f64) {
                    if id > job_id {
                        job_id = id;
                    }
                }
            }
        }
        (job_id > -1.0).then_some(job_id)
    }

    /// Go `distributeTable`.
    fn distribute_table(&self) -> Result<(), DriverError> {
        let mut input = HashMap::new();
        input.insert("alias".to_owned(), self.get_alias().into());
        input.insert("engine".to_owned(), self.p.engine.clone().into());
        input.insert("rule".to_owned(), self.p.rule.clone().into());
        if !self.p.timeout.is_empty() {
            input.insert("timeout".to_owned(), self.p.timeout.clone().into());
        }
        let (start_keys, end_keys): (Vec<String>, Vec<String>) =
            self.key_ranges.iter().map(pd_http::KeyRange::escape_as_utf8_str).unzip();
        input.insert("start-key".to_owned(), start_keys.join(",").into());
        input.insert("end-key".to_owned(), end_keys.join(",").into());
        tidb_domain_infosync::region::create_scheduler_config_with_input(SCHEDULER_NAME, input)
            .map_err(unknown)
    }

    /// Go `getAlias`: `{db}.{table}.{partition(p1,p2)}`.
    fn get_alias(&self) -> String {
        let partition_str = if self.p.partition_names.is_empty() {
            String::new()
        } else {
            let names: Vec<&str> = self.p.partition_names.iter().map(|name| name.original()).collect();
            format!("partition({})", names.join(","))
        };
        [self.p.db_name.as_str(), self.p.table_info.read().name.original(), partition_str.as_str()].join(".")
    }
}

/// Go `getKeyRanges`: one range per run of consecutive physical IDs.
fn get_key_ranges(p: &DistributeTable) -> Result<Vec<pd_http::KeyRange>, DriverError> {
    let info = p.table_info.read();
    let mut physical_ids = Vec::new();
    match info.get_partition_info() {
        None => physical_ids.push(info.id),
        Some(pi) => {
            let definitions = pi.read().definitions.snapshot();
            for name in &p.partition_names {
                // Go `tables.FindPartitionByName`.
                let definition = definitions
                    .iter()
                    .find(|definition| definition.name.lowercase() == name.lowercase())
                    .ok_or_else(|| {
                        DriverError::from(tidb_table::table_errors::ERR_UNKNOWN_PARTITION.gen_with_stack_by_args(&[
                            name.lowercase().to_owned().into(),
                            info.name.original().to_owned().into(),
                        ]))
                    })?;
                physical_ids.push(definition.id);
            }
            if physical_ids.is_empty() {
                physical_ids.extend(definitions.iter().map(|definition| definition.id));
            }
        }
    }
    physical_ids.sort_unstable();
    let key = |pid: i64| {
        let mut key = Vec::new();
        tidb_codec::encode_bytes(&mut key, &tidb_tablecodec::table_key::gen_table_prefix(pid));
        key
    };
    let mut ranges: Vec<pd_http::KeyRange> = Vec::with_capacity(physical_ids.len());
    for (i, pid) in physical_ids.iter().enumerate() {
        if i == 0 || *pid != physical_ids[i - 1] + 1 {
            ranges.push(pd_http::KeyRange::new(key(*pid), key(pid + 1)));
        } else if let Some(last) = ranges.last_mut() {
            last.end_key = key(pid + 1);
        }
    }
    Ok(ranges)
}

/// Go `CancelDistributionJobExec.Next`.
///
/// # Errors
/// PD's.
pub fn cancel_distribution_job(job_id: u64) -> Result<(), DriverError> {
    tidb_domain_infosync::region::cancel_scheduler_job(SCHEDULER_NAME, job_id).map_err(unknown)
}

/// Go `ShowExec.fetchShowDistributionJobs`: every job of the scheduler, or
/// only `job_id`'s.
///
/// # Errors
/// PD's, or a job PD answered in another shape.
pub fn fetch_show_distribution_jobs(job_id: Option<i64>) -> Result<Vec<Vec<Datum>>, DriverError> {
    let config = tidb_domain_infosync::region::get_scheduler_config(SCHEDULER_NAME).map_err(unknown)?;
    let Some(configs) = config.as_array() else {
        // it means that no any jobs
        return Ok(Vec::new());
    };
    let mut jobs = Vec::with_capacity(configs.len());
    for cfg in configs {
        let Some(job) = cfg.as_object() else {
            return Err(unknown(format!(
                "[PD:proto:ErrClientProtoUnmarshal]failed to unmarshal proto, {cfg}"
            )));
        };
        jobs.push(job);
    }
    let mut rows = Vec::new();
    for job in jobs {
        if let Some(wanted) = job_id {
            let id = job.get("job-id").and_then(serde_json::Value::as_f64);
            if id.is_some_and(|id| wanted == id as i64) {
                rows.push(fill_distribution_job_to_chunk(job)?);
                break;
            }
        } else {
            rows.push(fill_distribution_job_to_chunk(job)?);
        }
    }
    Ok(rows)
}

/// Go `fillDistributionJobToChunk`.
fn fill_distribution_job_to_chunk(job: &serde_json::Map<String, serde_json::Value>) -> Result<Vec<Datum>, DriverError> {
    let text = |name: &str| job.get(name).and_then(serde_json::Value::as_str).unwrap_or_default().to_owned();
    // alias is {db_name}.{table_name}.{partition_name}
    let alias_text = text("alias");
    let alias: Vec<&str> = alias_text.split('.').collect();
    if alias.len() != 3 {
        return Err(unknown(format!(
            "[PD:proto:ErrClientProtoUnmarshal]failed to unmarshal proto, alias:{alias_text} is invalid"
        )));
    }
    let number = |name: &str| job.get(name).and_then(serde_json::Value::as_f64).unwrap_or_default();
    let time = |name: &str| -> Result<Datum, DriverError> {
        let Some(value) = job.get(name).and_then(serde_json::Value::as_str) else {
            return Ok(Datum::Null);
        };
        // Go `time.Time.UnmarshalText` (RFC 3339), its wall clock kept.
        let parsed = chrono::DateTime::parse_from_rfc3339(value).map_err(|error| unknown(error.to_string()))?;
        let local = parsed.naive_local();
        use chrono::{Datelike, Timelike};
        let core = tidb_datatype::CoreTime::from_date(
            local.year() as u16,
            local.month() as u8,
            local.day() as u8,
            local.hour() as u8,
            local.minute() as u8,
            local.second() as u8,
            local.nanosecond() / 1000,
        );
        tidb_datatype::Time::new(core, tidb_datatype::TimeType::DateTime, 0)
            .map(Datum::Time)
            .map_err(|error| unknown(error.to_string()))
    };
    Ok(vec![
        Datum::UInt(number("job-id") as u64),
        Datum::new_string_datum(alias[0]),
        Datum::new_string_datum(alias[1]),
        // partition name maybe empty when the table is not partitioned
        if alias[2].is_empty() { Datum::Null } else { Datum::new_string_datum(alias[2]) },
        Datum::new_string_datum(text("engine")),
        Datum::new_string_datum(text("rule")),
        Datum::new_string_datum(text("status")),
        Datum::new_string_datum(tidb_config_configtypes::Duration(number("timeout") as u64 as i64).to_string()),
        time("create")?,
        time("start")?,
        time("finish")?,
    ])
}
