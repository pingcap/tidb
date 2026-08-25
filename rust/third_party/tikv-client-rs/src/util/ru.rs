// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt;
use std::sync::Mutex;
use std::time::Duration;

use crate::proto::kvrpcpb::{ExecutorInputs, Ruv2};
use crate::proto::resource_manager::Consumption;

/// Concurrent resource-unit details accumulated by TiKV responses.
#[derive(Debug, Default)]
pub struct RuDetails {
    read_ru: Mutex<f64>,
    write_ru: Mutex<f64>,
    ru_wait_duration: Mutex<Duration>,
    tiflash_ru: Mutex<f64>,
    tikv_ru_v2: Mutex<f64>,
    raw_ru_v2: Mutex<Option<Ruv2>>,
}

impl RuDetails {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn new_with(read_ru: f64, write_ru: f64, wait_duration: Duration) -> Self {
        Self {
            read_ru: Mutex::new(read_ru),
            write_ru: Mutex::new(write_ru),
            ru_wait_duration: Mutex::new(wait_duration),
            tiflash_ru: Mutex::new(0.0),
            tikv_ru_v2: Mutex::new(0.0),
            raw_ru_v2: Mutex::new(None),
        }
    }

    pub fn cloned(&self) -> Self {
        Self {
            read_ru: Mutex::new(self.read_ru()),
            write_ru: Mutex::new(self.write_ru()),
            ru_wait_duration: Mutex::new(self.ru_wait_duration()),
            tiflash_ru: Mutex::new(self.tiflash_ru()),
            tikv_ru_v2: Mutex::new(self.tikv_ru_v2()),
            raw_ru_v2: Mutex::new(self.raw_ru_v2.lock().unwrap().clone()),
        }
    }

    pub fn merge(&self, other: &Self) {
        *self.read_ru.lock().unwrap() += other.read_ru();
        *self.write_ru.lock().unwrap() += other.write_ru();
        *self.ru_wait_duration.lock().unwrap() += other.ru_wait_duration();
        *self.tiflash_ru.lock().unwrap() += other.tiflash_ru();
        *self.tikv_ru_v2.lock().unwrap() += other.tikv_ru_v2();
        let raw = other.raw_ru_v2.lock().unwrap().clone();
        self.add_ru_v2(raw.as_ref());
    }

    pub fn tikv_ru_v2(&self) -> f64 {
        *self.tikv_ru_v2.lock().unwrap()
    }

    /// Source `RUDetails.RRU`.
    pub fn read_ru(&self) -> f64 {
        *self.read_ru.lock().unwrap()
    }

    /// Source `RUDetails.WRU`.
    pub fn write_ru(&self) -> f64 {
        *self.write_ru.lock().unwrap()
    }

    /// Source `RUDetails.RUWaitDuration`.
    pub fn ru_wait_duration(&self) -> Duration {
        *self.ru_wait_duration.lock().unwrap()
    }

    pub fn tiflash_ru(&self) -> f64 {
        *self.tiflash_ru.lock().unwrap()
    }

    /// Source `RUDetails.Update`: accumulate resource-controller consumption
    /// and the time spent waiting for resource tokens.
    pub fn update(&self, consumption: &Consumption, wait_duration: Duration) {
        *self.read_ru.lock().unwrap() += consumption.r_r_u;
        *self.write_ru.lock().unwrap() += consumption.w_r_u;
        *self.ru_wait_duration.lock().unwrap() += wait_duration;
    }

    pub fn update_tiflash(&self, consumption: &Consumption) {
        *self.read_ru.lock().unwrap() += consumption.r_r_u;
        *self.write_ru.lock().unwrap() += consumption.w_r_u;
        *self.tiflash_ru.lock().unwrap() += consumption.r_r_u + consumption.w_r_u;
    }

    pub fn add_tikv_ru_v2(&self, delta: f64) {
        if delta != 0.0 {
            *self.tikv_ru_v2.lock().unwrap() += delta;
        }
    }

    pub fn add_ru_v2(&self, delta: Option<&Ruv2>) {
        let Some(delta) = delta else {
            return;
        };
        let mut raw = self.raw_ru_v2.lock().unwrap();
        match raw.as_mut() {
            Some(current) => merge_ru_v2(current, delta),
            None => *raw = Some(delta.clone()),
        }
    }

    pub fn drain_ru_v2(&self) -> Option<Ruv2> {
        self.raw_ru_v2.lock().unwrap().take()
    }
}

impl fmt::Display for RuDetails {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "RRU:{:.6}, WRU:{:.6}, WaitDuration:{}",
            self.read_ru(),
            self.write_ru(),
            crate::util::format_duration(self.ru_wait_duration())
        )
    }
}

fn merge_ru_v2(dst: &mut Ruv2, src: &Ruv2) {
    dst.kv_engine_cache_miss = dst
        .kv_engine_cache_miss
        .wrapping_add(src.kv_engine_cache_miss);
    dst.coprocessor_executor_iterations = dst
        .coprocessor_executor_iterations
        .wrapping_add(src.coprocessor_executor_iterations);
    dst.coprocessor_response_bytes = dst
        .coprocessor_response_bytes
        .wrapping_add(src.coprocessor_response_bytes);
    dst.raftstore_store_write_trigger_wb_bytes = dst
        .raftstore_store_write_trigger_wb_bytes
        .wrapping_add(src.raftstore_store_write_trigger_wb_bytes);
    dst.storage_processed_keys_batch_get = dst
        .storage_processed_keys_batch_get
        .wrapping_add(src.storage_processed_keys_batch_get);
    dst.storage_processed_keys_get = dst
        .storage_processed_keys_get
        .wrapping_add(src.storage_processed_keys_get);
    dst.read_rpc_count = dst.read_rpc_count.wrapping_add(src.read_rpc_count);
    dst.write_rpc_count = dst.write_rpc_count.wrapping_add(src.write_rpc_count);
    if let Some(src) = &src.executor_inputs {
        let dst = dst
            .executor_inputs
            .get_or_insert_with(ExecutorInputs::default);
        dst.tikv_coprocessor_executor_work_total_batch_index_scan = dst
            .tikv_coprocessor_executor_work_total_batch_index_scan
            .wrapping_add(src.tikv_coprocessor_executor_work_total_batch_index_scan);
        dst.tikv_coprocessor_executor_work_total_batch_table_scan = dst
            .tikv_coprocessor_executor_work_total_batch_table_scan
            .wrapping_add(src.tikv_coprocessor_executor_work_total_batch_table_scan);
        dst.tikv_coprocessor_executor_work_total_batch_selection = dst
            .tikv_coprocessor_executor_work_total_batch_selection
            .wrapping_add(src.tikv_coprocessor_executor_work_total_batch_selection);
        dst.tikv_coprocessor_executor_work_total_batch_top_n = dst
            .tikv_coprocessor_executor_work_total_batch_top_n
            .wrapping_add(src.tikv_coprocessor_executor_work_total_batch_top_n);
        dst.tikv_coprocessor_executor_work_total_batch_limit = dst
            .tikv_coprocessor_executor_work_total_batch_limit
            .wrapping_add(src.tikv_coprocessor_executor_work_total_batch_limit);
        dst.tikv_coprocessor_executor_work_total_batch_simple_aggr = dst
            .tikv_coprocessor_executor_work_total_batch_simple_aggr
            .wrapping_add(src.tikv_coprocessor_executor_work_total_batch_simple_aggr);
        dst.tikv_coprocessor_executor_work_total_batch_fast_hash_aggr = dst
            .tikv_coprocessor_executor_work_total_batch_fast_hash_aggr
            .wrapping_add(src.tikv_coprocessor_executor_work_total_batch_fast_hash_aggr);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_resource_consumption_updates_accumulate() {
        let details = RuDetails::new();
        details.update(
            &Consumption {
                r_r_u: 1.5,
                w_r_u: 2.25,
                ..Default::default()
            },
            Duration::from_millis(3),
        );
        details.update(
            &Consumption {
                r_r_u: 0.5,
                w_r_u: 0.75,
                ..Default::default()
            },
            Duration::from_millis(5),
        );
        assert_eq!(details.read_ru(), 2.0);
        assert_eq!(details.write_ru(), 3.0);
        assert_eq!(details.ru_wait_duration(), Duration::from_millis(8));
    }

    #[test]
    fn source_clone_merge_tiflash_and_string_contract() {
        let details = RuDetails::new_with(1.0, 2.0, Duration::from_millis(3));
        details.update_tiflash(&Consumption {
            r_r_u: 3.0,
            w_r_u: 4.0,
            ..Default::default()
        });
        details.add_tikv_ru_v2(5.0);
        details.add_ru_v2(Some(&Ruv2 {
            read_rpc_count: 1,
            ..Default::default()
        }));
        let cloned = details.cloned();
        assert_eq!(cloned.read_ru(), 4.0);
        assert_eq!(cloned.write_ru(), 6.0);
        assert_eq!(cloned.tiflash_ru(), 7.0);
        assert_eq!(cloned.tikv_ru_v2(), 5.0);
        assert_eq!(cloned.drain_ru_v2().unwrap().read_rpc_count, 1);
        assert_eq!(details.drain_ru_v2().unwrap().read_rpc_count, 1);
        assert_eq!(
            details.to_string(),
            "RRU:4.000000, WRU:6.000000, WaitDuration:3ms"
        );

        let merged = RuDetails::new();
        merged.merge(&details);
        assert_eq!(merged.read_ru(), 4.0);
        assert_eq!(merged.write_ru(), 6.0);
        assert_eq!(merged.tiflash_ru(), 7.0);
    }
}
