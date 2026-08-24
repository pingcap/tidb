// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Mutex;

use crate::proto::kvrpcpb::{ExecutorInputs, Ruv2};

/// Concurrent resource-unit details accumulated by TiKV responses.
#[derive(Debug, Default)]
pub struct RuDetails {
    tikv_ru_v2: Mutex<f64>,
    raw_ru_v2: Mutex<Option<Ruv2>>,
}

impl RuDetails {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn tikv_ru_v2(&self) -> f64 {
        *self.tikv_ru_v2.lock().unwrap()
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
