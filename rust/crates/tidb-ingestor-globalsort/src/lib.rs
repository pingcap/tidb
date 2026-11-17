// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `pkg/ingestor/globalsort`: global sort over object storage. The
//! encode step writes sorted KV files with `simplesst`; this package merges
//! overlapping files (`MergeOverlappingFiles`), splits the sorted data into
//! ingest subtasks (`RangeSplitter`), and loads a subtask's ranges into
//! memory as an ingest engine (`Engine`).

mod engine;
mod kvgroup;
mod merge;
mod reader;
mod split;
mod util;

#[cfg(test)]
mod tests;

pub use engine::{Engine, EngineConfig, MemoryIngestData};
pub use kvgroup::{index_id_2_kv_group, kv_group_2_index_id, DATA_KV_GROUP};
pub use merge::{merge_overlapping_files, MergeCollector, MergeOperator, MAX_MERGING_FILES_PER_THREAD};
pub use reader::read_kv_files_async;
pub use split::{cal_range_size, RangeSplitter, SplitGroup};
pub use util::{
    bytes_max, bytes_min, clean_up_files, divide_merge_sort_data_files, plan_meta_path, prepared_meta_path,
    subtask_meta_path, SortedKVMeta,
};

/// The package's errors: a Go `*errors.Error` keeps its identity.
pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// The package's result.
pub type Result<T> = std::result::Result<T, Error>;

/// A Go `errors.New` / `errors.Errorf` error.
pub(crate) fn other(message: impl Into<String>) -> Error {
    Box::new(tidb_ingestor_simplesst::Error::Other(message.into()))
}

/// A `simplesst` error, kept as itself.
pub(crate) fn sst(e: tidb_ingestor_simplesst::Error) -> Error {
    Box::new(e)
}

pub(crate) fn str_field(key: &'static str, value: &str) -> tidb_log::Field {
    tidb_log::Field::new(key, tidb_log::Value::Str(value.to_owned()))
}

pub(crate) fn int_field(key: &'static str, value: i64) -> tidb_log::Field {
    tidb_log::Field::new(key, tidb_log::Value::I64(value))
}
