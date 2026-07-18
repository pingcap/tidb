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

//! Dependency-closed statistics primitives.
//!
//! The leaves port Count-Min Sketch/TopN, raw-hash
//! Flajolet-Martin geometry, and statistics loading metadata from
//! `pkg/statistics/{cmsketch,fmsketch,histogram}.go`.
//! They own only source-shaped arithmetic and metadata at encoded-byte or
//! already-hashed boundaries. The CMSketch family also owns sampled TopN
//! construction and the tipb message boundary. Datum encoding, histograms,
//! storage persistence, session tracing, and a statistics handle remain
//! explicit future owners.

#![allow(missing_docs)]

pub mod analysis_interval;
pub mod analysis_policy;
pub mod analyze_jobs;
pub mod analyze_table_id;
pub mod analyze_version_policy;
pub mod async_load;
pub mod auto_analyze_job;
pub mod auto_analyze_policy;
pub mod auto_analyze_process_set;
pub mod auto_analyze_ratio;
pub mod auto_analyze_runtime;
pub mod auto_analyze_window;
pub mod average_count;
pub mod batch_update;
pub mod bootstrap_sql;
pub mod bounded_min_heap;
pub mod cache_metrics_labels;
pub mod cmsketch;
pub mod constants;
pub mod correlation;
pub mod count_metrics;
pub mod datum_map_cache;
pub mod ddl_event_match;
pub mod ddl_physical_ids;
pub mod ddl_queue_gate;
pub mod ddl_stats_delta;
pub mod dynamic_partition_helpers;
pub mod estimate;
pub mod existence_map;
pub mod fmsketch;
pub mod gc_batch_count;
pub mod global_stats_layout;
pub mod global_stats_sql_index;
pub mod global_topn;
pub mod healthy_metrics;
pub mod historical_stats;
pub mod index_query;
pub mod index_usage;
pub mod index_usage_key;
pub mod init_stats_concurrency;
pub mod init_stats_progress;
pub mod json_metadata;
pub mod json_stats_version;
pub mod lock_messages;
pub mod locked_tables;
pub mod map_cache;
pub mod memory_cost;
pub mod memory_usage;
pub mod mock_statistics_shape;
pub mod non_partitioned_analysis;
pub mod overlap_geometry;
pub mod partition_table_id_cache;
pub mod pending_delta_ids;
pub mod predicate_column_queries;
pub mod predicate_column_query_mode;
pub mod priority_calculator;
pub mod priority_heap;
pub mod pseudo_cache_policy;
pub mod queue_gate;
pub mod refresher_state;
pub mod row_estimate;
pub mod sample_bytes;
pub mod scalar_geometry;
pub mod special_global_index;
pub mod static_partitioned_analysis;
pub mod stats_cache_inner;
pub mod stats_cache_version;
pub mod stats_delta;
pub mod stats_key_set;
pub mod stats_key_set_shards;
pub mod stats_lease;
pub mod stats_lock_table;
pub mod stats_meta;
pub mod stats_meta_save_sql;
pub mod stats_meta_update;
pub mod stats_pool;
pub mod stats_read_writer;
pub mod stats_request_matcher;
pub mod stats_table_snapshot;
pub mod stats_version;
pub mod status;
pub mod sync_load_concurrency;
pub mod table_id_filter;
pub mod topn_merge_task;
pub mod usage_collector;
pub mod weighted_reservoir;
pub mod worker_capacity;

pub use analysis_interval::{
    average_analysis_duration_from_seconds, average_duration_query,
    last_failed_analysis_duration_from_seconds, last_failed_duration_query,
    AVG_DURATION_QUERY_FOR_PARTITION, AVG_DURATION_QUERY_FOR_TABLE,
    DEFAULT_FAILED_ANALYSIS_WAIT_NANOS, JUST_FAILED, LAST_FAILED_DURATION_QUERY_FOR_PARTITION,
    LAST_FAILED_DURATION_QUERY_FOR_TABLE, NO_RECORD,
};
pub use analysis_policy::{
    is_eligible_for_analysis, meets_auto_analyze_min_count, table_is_analyzed,
    DEFAULT_AUTO_ANALYZE_MIN_COUNT,
};
pub use analyze_jobs::{
    AnalyzeJob, AnalyzeProgress, JobType, ANALYZE_FAILED, ANALYZE_FINISHED, ANALYZE_PENDING,
    ANALYZE_RUNNING, DUMP_TIME_INTERVAL, MAX_DELTA,
};
pub use analyze_table_id::{AnalyzeTableId, NON_PARTITION_TABLE_ID};
pub use analyze_version_policy::analyze_version_matches;
pub use async_load::{NeededStatsMap, StatsLoadItem, TableItemId, SHARD_COUNT};
pub use auto_analyze_job::{
    as_json_indicators, is_dynamic_partitioned_table_analysis_job, AnalysisIndicators,
    AnalysisJobKind, Indicators, IndicatorsJSON, IndicatorsJson,
};
pub use auto_analyze_policy::need_analyze_table;
pub use auto_analyze_process_set::AutoAnalyzeProcessSet;
pub use auto_analyze_ratio::{parse_auto_analyze_ratio, DEFAULT_AUTO_ANALYZE_RATIO};
pub use auto_analyze_runtime::{
    AnalysisJobFactory, AnalysisJobRuntime,
    AutoAnalysisTimeWindow as RuntimeAutoAnalysisTimeWindow, ClockPort, DdlEvent, DdlHandleOutcome,
    DdlRuntime, InfoSchemaPort, JobHookPort, PartitionPruneMode, QueueMutationPort, RuntimeError,
    SessionPort, SqlPort, StatisticsPort,
};
pub use auto_analyze_window::{AutoAnalysisTimeWindow, UtcDayMinute};
pub use average_count::avg_count_per_not_null_value;
pub use batch_update::BatchUpdate;
pub use bootstrap_sql::{gen_init_stats_histograms_sql, gen_init_stats_meta_sql, HistSqlOptions};
pub use bounded_min_heap::BoundedMinHeap;
pub use cache_metrics_labels::{
    stats_cache_counter_labels, stats_cache_gauge_labels, STATS_CACHE_COUNTER_LABELS,
    STATS_CACHE_GAUGE_LABELS,
};
pub use cmsketch::{
    check_empty_topns, decode_cmsketch, decode_cmsketch_and_embedded_topn,
    decode_cmsketch_and_topn, decode_topn_rows, encode_cmsketch_and_topn,
    encode_cmsketch_without_topn, get_merged_topn_from_sorted_slice, merge_topn,
    merge_topn_and_update_cmsketch, new_cmsketch_and_topn,
    new_cmsketch_and_topn_with_tie_stabilization, sort_topn_meta, topn_meta_compare, CodecError,
};
pub use cmsketch::{hash_bytes, CmsSketch, Hash128, MergeError, TopN, TopNEntry};
pub use constants::{DEFAULT_HISTOGRAM_BUCKETS, DEFAULT_TOP_N_VALUE};
pub use correlation::calc_correlation;
pub use count_metrics::HistogramCountSummary;
pub use datum_map_cache::DatumMapCache;
pub use ddl_event_match::find_event_with_timeout;
pub use ddl_physical_ids::physical_ids_for_stats_ddl;
pub use ddl_queue_gate::{ddl_queue_disposition, DdlQueueDisposition};
pub use ddl_stats_delta::{
    ddl_stats_delta_update, DdlStatsDeltaUpdate, EXISTING_STATS_META_DELTA_QUERY,
    LOCKED_STATS_DELTA_QUERY, MISSING_STATS_META_DELTA_QUERY,
};
pub use dynamic_partition_helpers::{flatten_partition_names, get_partition_sql};
pub use estimate::{estimate_global_singleton_by_sketches, estimate_ndv_by_gee};
pub use existence_map::ColAndIdxExistenceMap;
pub use fmsketch::{FmSketch, MAX_SKETCH_SIZE};
pub use gc_batch_count::gc_batch_count;
pub use global_stats_layout::{new_global_stats_layout, GlobalStatsLayout};
pub use global_stats_sql_index::to_sql_index;
pub use global_topn::{merge_histogram_free_topn, GlobalTopNMerge};
pub use healthy_metrics::{
    healthy_bucket_configs, HealthyBucketConfig, HEALTHY_BUCKET_CONFIGS,
    STATS_HEALTHY_BUCKET_0_TO_50, STATS_HEALTHY_BUCKET_100_TO_100, STATS_HEALTHY_BUCKET_50_TO_55,
    STATS_HEALTHY_BUCKET_55_TO_60, STATS_HEALTHY_BUCKET_60_TO_70, STATS_HEALTHY_BUCKET_70_TO_80,
    STATS_HEALTHY_BUCKET_80_TO_100, STATS_HEALTHY_BUCKET_COUNT, STATS_HEALTHY_BUCKET_PSEUDO,
    STATS_HEALTHY_BUCKET_TOTAL, STATS_HEALTHY_BUCKET_UNNEEDED_ANALYZE,
};
pub use historical_stats::historical_stats_version;
pub use index_query::query_index_bytes;
pub use index_usage::{
    index_usage_access_bucket, new_index_usage_sample, IndexUsageSample, INDEX_USAGE_BUCKET_BOUNDS,
    INDEX_USAGE_BUCKET_COUNT,
};
pub use index_usage_key::IndexUsageKey;
pub use init_stats_concurrency::init_stats_concurrency;
pub use init_stats_progress::init_stats_progress;
pub use json_metadata::{JsonPredicateColumn, JsonTable, TIDB_GLOBAL_STATS};
pub use json_stats_version::{json_stats_version, JSON_STATS_VERSION_0, JSON_STATS_VERSION_1};
pub use lock_messages::{
    generate_stable_skipped_partitions_message, generate_stable_skipped_tables_message,
};
pub use locked_tables::{get_locked_tables, SELECT_LOCKED_TABLES_SQL};
pub use map_cache::MapCache;
pub use memory_cost::{
    add_memory_cost, adjust_mem_cost, effective_mem_cost, MemoryCostError, MEMORY_COST_PERCENT,
    TEST_MODE_MEMORY_COST,
};
pub use memory_usage::{ColumnMemUsage, IndexMemUsage};
pub use mock_statistics_shape::MockStatisticsTableShape;
pub use non_partitioned_analysis::{
    analyze_type, gen_sql_for_analyze_index, gen_sql_for_analyze_table, has_newly_added_index,
    ANALYZE_INDEX, ANALYZE_TABLE,
};
pub use overlap_geometry::{left_overlap_percent, right_overlap_percent};
pub use partition_table_id_cache::PartitionTableIdCache;
pub use pending_delta_ids::collect_pending_delta_ids;
pub use predicate_column_queries::{
    cleanup_column_ids_argument, CLEANUP_DROPPED_COLUMN_STATS_USAGE_QUERY,
    GET_PREDICATE_COLUMNS_QUERY, LOAD_COLUMN_STATS_USAGE_FOR_TABLE_QUERY,
    LOAD_COLUMN_STATS_USAGE_QUERY,
};
pub use predicate_column_query_mode::PredicateColumnOperation;
pub use priority_calculator::{
    calculate_priority_weight, special_event_weight, EVENT_NEW_INDEX, EVENT_NONE,
};
pub use priority_heap::{PriorityHeap, PriorityHeapError, PriorityHeapItem};
pub use pseudo_cache_policy::{should_cache_pseudo_stats, PSEUDO_CACHE_PARTITION_LIMIT};
pub use queue_gate::{
    is_empty_for_test, queue_len, require_initialized, running_jobs, QueueNotInitialized,
    NOT_INITIALIZED_ERROR_MSG,
};
pub use refresher_state::should_rebuild_queue;
pub use row_estimate::{calculate_skew_ratio_counts, default_row_est, RowEstimate};
pub use sample_bytes::{
    calc_total_size, sample_value_is_usable, MAX_FIELD_VARCHAR_LENGTH, MAX_SAMPLE_VALUE_LENGTH,
};
pub use scalar_geometry::{calc_fraction, common_prefix_length, convert_bytes_to_scalar};
pub use special_global_index::{is_special_global_index, IndexColumnInfo};
pub use static_partitioned_analysis::{
    gen_sql_for_analyze_static_partition, gen_sql_for_analyze_static_partition_index,
    has_newly_added_static_partition_index, static_partition_analyze_type,
    static_partition_table_id, ANALYZE_STATIC_PARTITION, ANALYZE_STATIC_PARTITION_INDEX,
};
pub use stats_cache_inner::StatsCacheInner;
pub use stats_cache_version::max_stats_cache_version;
pub use stats_delta::{stats_delta_from_rows, StatsDelta, SELECT_DELTA_SQL};
pub use stats_key_set::StatsKeySet;
pub use stats_key_set_shards::{StatsKeySetShards, KEY_SET_SHARD_COUNT};
pub use stats_lease::StatsLease;
pub use stats_lock_table::StatsLockTable;
pub use stats_meta::{stats_meta_counts, stats_meta_query, StatsMetaCounts};
pub use stats_meta_save_sql::{stats_meta_save_sql, StatsMetaSaveUpdate};
pub use stats_meta_update::{
    stats_meta_update_sql, DeltaUpdate, StatsMetaUpdateSql, StatsMetaVersionUpdate,
    UPDATE_STATS_META_VERSION_QUERY,
};
pub use stats_pool::StatsPool;
pub use stats_read_writer::{
    historical_stats_meta_record_required, slow_stats_saving_requires_meta_update, LEASE_OFFSET,
    SLOW_STATS_SAVE_ERROR_MESSAGE,
};
pub use stats_request_matcher::{
    is_internal_stats_foreground_source, CTX_MATCHER_DESCRIPTION,
    INTERNAL_STATS_FOREGROUND_PRIORITY_SOURCE,
};
pub use stats_table_snapshot::{
    stats_table_snapshots_equal, StatsItemSnapshot, StatsTableSnapshot,
};
pub use stats_version::{
    is_analyzed, is_column_analyzed_or_synthesized, VERSION_0, VERSION_1, VERSION_2,
};
pub use status::{StatsLoadedStatus, ALL_EVICTED, ALL_LOADED};
pub use sync_load_concurrency::sync_load_concurrency_for_cpu;
pub use table_id_filter::build_in_table_ids_string;
pub use topn_merge_task::TopnStatsMergeTask;
pub use usage_collector::{
    GlobalCollector, SessionCollector, DEFAULT_CHANNEL_SIZE, DEFAULT_TIMEOUT,
};
pub use weighted_reservoir::{WeightedReservoir, WeightedSample};
pub use worker_capacity::{worker_capacity_available, worker_concurrency_changed};
