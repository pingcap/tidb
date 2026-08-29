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
//! Flajolet-Martin geometry, statistics loading metadata, and histogram
//! estimation/merge behavior from
//! `pkg/statistics/{cmsketch,fmsketch,histogram}.go`.
//! They own only source-shaped arithmetic and metadata at encoded-byte or
//! already-hashed boundaries. The CMSketch family also owns sampled TopN
//! construction and the tipb message boundary. Datum encoding, histogram
//! loading, storage persistence, session tracing, and a statistics handle
//! remain explicit future owners.

#![allow(missing_docs)]

pub mod analysis_policy;
pub mod analyze_jobs;
pub mod analyze_results;
pub mod analyze_table_id;
pub mod analyze_version_policy;
pub mod average_count;
pub mod builder;
pub mod cmsketch;
pub mod column;
pub mod constants;
pub mod correlation;
pub mod count_metrics;
pub mod datum_map_cache;
pub mod estimate;
pub mod existence_map;
pub mod fmsketch;
pub mod fmsketch_codec;
mod go_pdqsort;
mod go_stable_sort;
pub mod histogram;
pub mod index;
pub mod index_query;
pub mod json_metadata;
pub mod memory_usage;
pub mod overlap_geometry;
pub mod row_estimate;
pub mod row_sample_collector;
pub mod sample_bytes;
pub mod sample_collector;
pub mod scalar_enum;
pub mod scalar_geometry;
pub mod sorted_builder;
pub mod stats_lock_table;
pub mod stats_version;
pub mod status;
pub mod table;
pub mod weighted_reservoir;

pub use analysis_policy::{
    is_eligible_for_analysis, meets_auto_analyze_min_count, table_is_analyzed,
    DEFAULT_AUTO_ANALYZE_MIN_COUNT,
};
pub use analyze_jobs::{
    go_zero_time, AnalyzeJob, AnalyzeProgress, JobType, ANALYZE_FAILED, ANALYZE_FINISHED,
    ANALYZE_PENDING, ANALYZE_RUNNING, DUMP_TIME_INTERVAL, MAX_DELTA,
};
pub use analyze_results::{AnalyzeError, AnalyzeHistogramLifecycle, AnalyzeResult, AnalyzeResults};
pub use analyze_table_id::{AnalyzeTableId, NON_PARTITION_TABLE_ID};
pub use analyze_version_policy::analyze_version_matches;
pub use average_count::avg_count_per_not_null_value;
pub use builder::{
    build_column, build_column_histogram, build_hist_and_topn, try_build_column_histogram,
    try_build_column_histogram_in_place, try_build_hist_and_topn, try_build_hist_and_topn_in_place,
    try_build_hist_and_topn_tracked, BuildOptions, BuilderMemoryBuffer, ComparedBytesResult,
    HistogramAndTopN, HistogramBuildError, SampleCollector, SampleItem, SequentialRangeChecker,
};
pub use cmsketch::{
    check_empty_topns, cmsketch_and_topn_from_proto, decode_cmsketch,
    decode_cmsketch_and_embedded_topn, decode_cmsketch_and_topn, decode_topn_rows,
    encode_cmsketch_and_topn, encode_cmsketch_without_topn, find_topn,
    get_merged_topn_from_sorted_slice, merge_topn, merge_topn_and_update_cmsketch,
    new_cmsketch_and_topn, new_cmsketch_and_topn_with_tie_stabilization, query_topn,
    sort_topn_meta, topn_between_count, topn_lower_bound, topn_meta_compare, topn_min_count,
    topn_total_count, CodecError,
};
pub use cmsketch::{
    hash_bytes, query_value, query_value_with_encoder, topn_decoded_string, topn_display_string,
    CmsSketch, CmsSketchProto, CmsSketchProtoRow, CmsSketchProtoTopN, Hash128, MergeError,
    SharedTopNBytes, TopN, TopNEntry,
};
pub use column::{column_is_all_evicted, copy_column, empty_column, Column, ColumnInfo};
pub use constants::{DEFAULT_HISTOGRAM_BUCKETS, DEFAULT_TOP_N_VALUE};
pub use correlation::calc_correlation;
pub use count_metrics::HistogramCountSummary;
pub use datum_map_cache::DatumMapCache;
pub use estimate::{estimate_global_singleton_by_sketches, estimate_ndv_by_gee};
pub use existence_map::ColAndIdxExistenceMap;
pub use fmsketch::{copy_fm_sketch, fm_sketch_ndv, merge_fm_sketch, FmSketch, MAX_SKETCH_SIZE};
pub use fmsketch_codec::{
    decode_fm_sketch, encode_fm_sketch, fm_sketch_from_proto, fm_sketch_to_proto, hash_datum,
    hash_datum_with_error_policy, hash_row, hash_row_with_error_policy, insert_encoded_row,
    insert_encoded_value, insert_row_value, insert_row_value_with_error_policy, insert_value,
    insert_value_with_error_policy, FmSketchCodecError, FmSketchProto,
};
pub use histogram::{Bucket, Histogram};
pub use index::{copy_index, index_is_all_evicted, Index, IndexInfo};
pub use index_query::query_index_bytes;
pub use json_metadata::{JsonPredicateColumn, JsonTable, TIDB_GLOBAL_STATS};
pub use memory_usage::{ColumnMemUsage, IndexMemUsage};
pub use overlap_geometry::{left_overlap_percent, right_overlap_percent};
pub use row_estimate::{calculate_skew_ratio_counts, default_row_est, RowEstimate};
pub use row_sample_collector::{
    adjusted_sample_rate, RowSampleCollector, RowSampleCollectorProto, RowSampleProto,
    SamplePolicy, SampledRow, ScannedRow, SlotStats, SlotValue, DEF_ROWS_FOR_SAMPLE_RATE,
};
pub use sample_bytes::{
    calc_total_size, sample_value_is_usable, MAX_FIELD_VARCHAR_LENGTH, MAX_SAMPLE_VALUE_LENGTH,
};
pub use sample_collector::{
    legacy_row_to_datums, legacy_sample_collector_from_proto, legacy_sample_collector_to_proto,
    sort_legacy_sample_items, LegacyRecordChunk, LegacySampleBuilder, LegacySampleBuilderError,
    LegacySampleCollector, LegacySampleCollectorProto, LegacySampleItem, LegacySampleRng,
    EMPTY_SAMPLE_ITEM_SIZE,
};
pub use scalar_enum::{enum_range_values, MAX_NUM_STEP};
pub use scalar_geometry::{
    calc_fraction, calc_fraction_from_datums, common_prefix_length, convert_bytes_to_scalar,
    convert_datum_to_scalar,
};
pub use sorted_builder::SortedHistogramBuilder;
pub use stats_lock_table::StatsLockTable;
pub use stats_version::{
    is_analyzed, is_column_analyzed_or_synthesized, VERSION_0, VERSION_1, VERSION_2,
};
pub use status::{StatsLoadedStatus, ALL_EVICTED, ALL_LOADED};
pub use table::{
    pseudo_hist_coll, pseudo_table, CopyIntent, HistColl, PseudoColumnInfo, PseudoIndexInfo,
    PseudoTableInfo, QueryColumn, QueryIndexInfo, QueryTableInfo, SharedColumn, SharedIndex,
    StatsInfo, Table, TableMemoryUsage, PSEUDO_ROW_COUNT, PSEUDO_VERSION, RATIO_OF_PSEUDO_ESTIMATE,
};
pub use tidb_stats_handle_util::*;
pub use weighted_reservoir::{WeightedReservoir, WeightedSample};
