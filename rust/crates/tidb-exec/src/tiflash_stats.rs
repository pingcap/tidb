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

//! SEED of Go `pkg/util/execdetails`, covering `tiflash_stats.go`:
//! [`TiflashStats`], [`TiFlashScanContext`], [`TiFlashColumnarScanContext`],
//! [`TiFlashWaitSummary`], and [`TiFlashNetworkTrafficSummary`] with their
//! byte-exact `String()` renderings and their `Clone`/`Merge`/
//! `mergeExecSummary`/`Empty`/`CanBeIgnored` behavior, plus
//! `TiFlashNetworkTrafficSummary.UpdateTiKVExecDetails` and
//! `GetInterZoneTrafficBytes`.
//!
//! Narrowings (Go input → Rust snapshot shape):
//! - `*tipb.TiFlashScanContext` → [`TiFlashScanContextSnapshot`] (with
//!   `*tipb.RegionsOfInstance` → [`RegionOfInstanceSnapshot`]),
//!   `*tipb.ColumnarScanContext` → [`ColumnarScanContextSnapshot`],
//!   `*tipb.TiFlashWaitSummary` → [`TiFlashWaitSummarySnapshot`], and
//!   `*tipb.TiFlashNetWorkSummary` → [`TiFlashNetworkSummarySnapshot`]: the
//!   tipb protobufs are not available, so each snapshot carries exactly the
//!   fields the ported `mergeExecSummary` bodies read, with the proto
//!   optionals collapsed to the `Get*` accessors' zero-default results. The
//!   Go nil-summary early return survives as the `Option` parameter.
//!   `TiFlashNetworkTrafficSummary.mergeExecSummary` is the one Go body that
//!   dereferences the proto pointers directly (`*summary.InnerZoneSendBytes`,
//!   no getters — a nil field would panic in Go); the snapshot's plain `u64`
//!   fields make that arm total here.
//! - client-go `*util.ExecDetails` (atomic `int64` counters) →
//!   [`crate::slow_log_format::TikvExecDetailsSnapshot`]:
//!   `UpdateTiKVExecDetails`'s `atomic.AddInt64` calls — the only
//!   `sync/atomic` use in `tiflash_stats.go` — collapse to plain `+=` on the
//!   already-loaded snapshot value, single-threaded on this seed's side.
//! - Go `Clone` methods (field-by-field deep copies, including the
//!   `regionsOfInstance` map copy) are the `#[derive(Clone)]` impls here;
//!   the derives copy exactly the same fields.
//! - Go `fmt.Sprintf("%f", ...)` in the region-balance rendering is Rust
//!   `{:.6}`; both fix six decimals and round half-to-even. Go's `+Inf`/`NaN`
//!   spellings differ but are unreachable (`maxNum > 0` forces
//!   `minNum >= 1`).
//! - The Go guard sums (`vectorIdxLoadFromS3 + ... > 0` and friends) use
//!   wrapping `uint64`/`uint32` addition; `wrapping_add` preserves that
//!   instead of panicking in debug builds.
//!
//! Boundaries:
//! - `MergeTiFlashRUConsumption` is NOT ported: it unmarshals a
//!   `resource_manager.Consumption` protobuf out of
//!   `summary.GetRuConsumption()` bytes and feeds client-go
//!   `util.RUDetails.UpdateTiFlash`/`Merge`; neither the kvproto
//!   `resource_manager` decoder nor the live `RUDetails` accumulator exists
//!   here.
//! - The Go tests (`TestCopRuntimeStatsForTiFlash`, `TestVectorSearchStats`,
//!   `TestColumnarScanContextStats`) drive these types through
//!   `runtime_stats.go`'s `RuntimeStatsColl.RecordOneCopTask`/
//!   `RecordCopStats`, `CopRuntimeStats.String`,
//!   `basicCopRuntimeStats.String`, and `StmtCopRuntimeStats` — whose
//!   TiFlash arms [`crate::runtime_stats`] deliberately left open. Those
//!   arms are mirrored locally inside this module's `#[cfg(test)]` harness
//!   (see the `// boundary:` comments there) so the Go assertion literals
//!   stay byte-exact; production wiring into `runtime_stats.rs` remains
//!   open.

use std::collections::HashMap;

use crate::slow_log_format::TikvExecDetailsSnapshot;

/// Go `time.Millisecond` in nanoseconds, the [`TiFlashWaitSummary`]
/// significance threshold.
const MILLISECOND_NS: u64 = 1_000_000;

/// One `*tipb.RegionsOfInstance` entry as
/// `TiFlashScanContext.mergeExecSummary` reads it (`GetInstanceId()`,
/// `GetRegionNum()`), zero-defaulted.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RegionOfInstanceSnapshot {
    /// Go `instance.GetInstanceId()` (`""` when unset).
    pub instance_id: String,
    /// Go `instance.GetRegionNum()` (0 when unset).
    pub region_num: u64,
}

/// The fields `TiFlashScanContext.mergeExecSummary` reads off a
/// `*tipb.TiFlashScanContext`, with proto optionals collapsed to Go's
/// zero-defaulted getter results.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TiFlashScanContextSnapshot {
    /// Go `summary.GetDmfileDataScannedRows()`.
    pub dmfile_data_scanned_rows: u64,
    /// Go `summary.GetDmfileDataSkippedRows()`.
    pub dmfile_data_skipped_rows: u64,
    /// Go `summary.GetDmfileMvccScannedRows()`.
    pub dmfile_mvcc_scanned_rows: u64,
    /// Go `summary.GetDmfileMvccSkippedRows()`.
    pub dmfile_mvcc_skipped_rows: u64,
    /// Go `summary.GetDmfileLmFilterScannedRows()`.
    pub dmfile_lm_filter_scanned_rows: u64,
    /// Go `summary.GetDmfileLmFilterSkippedRows()`.
    pub dmfile_lm_filter_skipped_rows: u64,
    /// Go `summary.GetTotalDmfileRsCheckMs()`.
    pub total_dmfile_rs_check_ms: u64,
    /// Go `summary.GetTotalDmfileReadMs()`.
    pub total_dmfile_read_ms: u64,
    /// Go `summary.GetTotalBuildSnapshotMs()`.
    pub total_build_snapshot_ms: u64,
    /// Go `summary.GetLocalRegions()`.
    pub local_regions: u64,
    /// Go `summary.GetRemoteRegions()`.
    pub remote_regions: u64,
    /// Go `summary.GetTotalLearnerReadMs()`.
    pub total_learner_read_ms: u64,
    /// Go `summary.GetDisaggReadCacheHitBytes()`.
    pub disagg_read_cache_hit_bytes: u64,
    /// Go `summary.GetDisaggReadCacheMissBytes()`.
    pub disagg_read_cache_miss_bytes: u64,
    /// Go `summary.GetSegments()`.
    pub segments: u64,
    /// Go `summary.GetReadTasks()`.
    pub read_tasks: u64,
    /// Go `summary.GetDeltaRows()`.
    pub delta_rows: u64,
    /// Go `summary.GetDeltaBytes()`.
    pub delta_bytes: u64,
    /// Go `summary.GetMvccInputRows()`.
    pub mvcc_input_rows: u64,
    /// Go `summary.GetMvccInputBytes()`.
    pub mvcc_input_bytes: u64,
    /// Go `summary.GetMvccOutputRows()`.
    pub mvcc_output_rows: u64,
    /// Go `summary.GetTotalBuildBitmapMs()`.
    pub total_build_bitmap_ms: u64,
    /// Go `summary.GetTotalBuildInputstreamMs()`.
    pub total_build_inputstream_ms: u64,
    /// Go `summary.GetStaleReadRegions()`.
    pub stale_read_regions: u64,
    /// Go `summary.GetVectorIdxLoadFromS3()`.
    pub vector_idx_load_from_s3: u64,
    /// Go `summary.GetVectorIdxLoadFromDisk()`.
    pub vector_idx_load_from_disk: u64,
    /// Go `summary.GetVectorIdxLoadFromCache()`.
    pub vector_idx_load_from_cache: u64,
    /// Go `summary.GetVectorIdxLoadTimeMs()`.
    pub vector_idx_load_time_ms: u64,
    /// Go `summary.GetVectorIdxSearchTimeMs()`.
    pub vector_idx_search_time_ms: u64,
    /// Go `summary.GetVectorIdxSearchVisitedNodes()`.
    pub vector_idx_search_visited_nodes: u64,
    /// Go `summary.GetVectorIdxSearchDiscardedNodes()`.
    pub vector_idx_search_discarded_nodes: u64,
    /// Go `summary.GetVectorIdxReadVecTimeMs()`.
    pub vector_idx_read_vec_time_ms: u64,
    /// Go `summary.GetVectorIdxReadOthersTimeMs()`.
    pub vector_idx_read_others_time_ms: u64,
    /// Go `summary.GetFtsNFromInmemoryNoindex()`.
    pub fts_n_from_inmemory_noindex: u32,
    /// Go `summary.GetFtsNFromTinyIndex()`.
    pub fts_n_from_tiny_index: u32,
    /// Go `summary.GetFtsNFromTinyNoindex()`.
    pub fts_n_from_tiny_noindex: u32,
    /// Go `summary.GetFtsNFromDmfIndex()`.
    pub fts_n_from_dmf_index: u32,
    /// Go `summary.GetFtsNFromDmfNoindex()`.
    pub fts_n_from_dmf_noindex: u32,
    /// Go `summary.GetFtsRowsFromInmemoryNoindex()`.
    pub fts_rows_from_inmemory_noindex: u64,
    /// Go `summary.GetFtsRowsFromTinyIndex()`.
    pub fts_rows_from_tiny_index: u64,
    /// Go `summary.GetFtsRowsFromTinyNoindex()`.
    pub fts_rows_from_tiny_noindex: u64,
    /// Go `summary.GetFtsRowsFromDmfIndex()`.
    pub fts_rows_from_dmf_index: u64,
    /// Go `summary.GetFtsRowsFromDmfNoindex()`.
    pub fts_rows_from_dmf_noindex: u64,
    /// Go `summary.GetFtsIdxLoadTotalMs()`.
    pub fts_idx_load_total_ms: u64,
    /// Go `summary.GetFtsIdxLoadFromCache()`.
    pub fts_idx_load_from_cache: u32,
    /// Go `summary.GetFtsIdxLoadFromColumnFile()`.
    pub fts_idx_load_from_column_file: u32,
    /// Go `summary.GetFtsIdxLoadFromStableS3()`.
    pub fts_idx_load_from_stable_s3: u32,
    /// Go `summary.GetFtsIdxLoadFromStableDisk()`.
    pub fts_idx_load_from_stable_disk: u32,
    /// Go `summary.GetFtsIdxSearchN()`.
    pub fts_idx_search_n: u32,
    /// Go `summary.GetFtsIdxSearchTotalMs()`.
    pub fts_idx_search_total_ms: u64,
    /// Go `summary.GetFtsIdxDmSearchRows()`.
    pub fts_idx_dm_search_rows: u64,
    /// Go `summary.GetFtsIdxDmTotalReadFtsMs()`.
    pub fts_idx_dm_total_read_fts_ms: u64,
    /// Go `summary.GetFtsIdxDmTotalReadOthersMs()`.
    pub fts_idx_dm_total_read_others_ms: u64,
    /// Go `summary.GetFtsIdxTinySearchRows()`.
    pub fts_idx_tiny_search_rows: u64,
    /// Go `summary.GetFtsIdxTinyTotalReadFtsMs()`.
    pub fts_idx_tiny_total_read_fts_ms: u64,
    /// Go `summary.GetFtsIdxTinyTotalReadOthersMs()`.
    pub fts_idx_tiny_total_read_others_ms: u64,
    /// Go `summary.GetFtsBruteTotalReadMs()`.
    pub fts_brute_total_read_ms: u64,
    /// Go `summary.GetFtsBruteTotalSearchMs()`.
    pub fts_brute_total_search_ms: u64,
    /// Go `summary.GetInvertedIdxLoadFromS3()`.
    pub inverted_idx_load_from_s3: u32,
    /// Go `summary.GetInvertedIdxLoadFromDisk()`.
    pub inverted_idx_load_from_disk: u32,
    /// Go `summary.GetInvertedIdxLoadFromCache()`.
    pub inverted_idx_load_from_cache: u32,
    /// Go `summary.GetInvertedIdxLoadTimeMs()`.
    pub inverted_idx_load_time_ms: u64,
    /// Go `summary.GetInvertedIdxSearchTimeMs()`.
    pub inverted_idx_search_time_ms: u64,
    /// Go `summary.GetInvertedIdxSearchSkippedPacks()`.
    pub inverted_idx_search_skipped_packs: u32,
    /// Go `summary.GetInvertedIdxIndexedRows()`.
    pub inverted_idx_indexed_rows: u64,
    /// Go `summary.GetInvertedIdxSearchSelectedRows()`.
    pub inverted_idx_search_selected_rows: u64,
    /// Go `summary.GetMinLocalStreamMs()`.
    pub min_local_stream_ms: u64,
    /// Go `summary.GetMaxLocalStreamMs()`.
    pub max_local_stream_ms: u64,
    /// Go `summary.GetMinRemoteStreamMs()`.
    pub min_remote_stream_ms: u64,
    /// Go `summary.GetMaxRemoteStreamMs()`.
    pub max_remote_stream_ms: u64,
    /// Go `summary.GetRegionsOfInstance()` (empty when unset).
    pub regions_of_instance: Vec<RegionOfInstanceSnapshot>,
}

/// The fields `TiFlashColumnarScanContext.mergeExecSummary` reads off a
/// `*tipb.ColumnarScanContext`, zero-defaulted like Go's getters.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ColumnarScanContextSnapshot {
    /// Go `summary.GetRegions()`.
    pub regions: u64,
    /// Go `summary.GetReadTasks()`.
    pub read_tasks: u64,
    /// Go `summary.GetPhysicalTables()`.
    pub physical_tables: u64,
    /// Go `summary.GetColumns()`.
    pub columns: u64,
    /// Go `summary.GetUserReadBytes()`.
    pub user_read_bytes: u64,
    /// Go `summary.GetMvccInputRows()`.
    pub mvcc_input_rows: u64,
    /// Go `summary.GetMvccInputBytes()`.
    pub mvcc_input_bytes: u64,
    /// Go `summary.GetMvccOutputRows()`.
    pub mvcc_output_rows: u64,
    /// Go `summary.GetTotalReadBlockMs()`.
    pub total_read_block_ms: u64,
    /// Go `summary.GetTotalSerializeBlockMs()`.
    pub total_serialize_block_ms: u64,
    /// Go `summary.GetTotalInitReaderMs()`.
    pub total_init_reader_ms: u64,
    /// Go `summary.GetTotalPrefetchMs()`.
    pub total_prefetch_ms: u64,
    /// Go `summary.GetRoughCheckTotalPacks()`.
    pub rough_check_total_packs: u64,
    /// Go `summary.GetRoughCheckSelectedPacks()`.
    pub rough_check_selected_packs: u64,
    /// Go `summary.GetRoughCheckSkippedPacks()`.
    pub rough_check_skipped_packs: u64,
    /// Go `summary.GetRoughCheckUnknownPacks()`.
    pub rough_check_unknown_packs: u64,
    /// Go `summary.GetRemoteSegments()`.
    pub remote_segments: u64,
    /// Go `summary.GetTotalSegments()`.
    pub total_segments: u64,
    /// Go `summary.GetTotalDeserializeBlockMs()`.
    pub total_deserialize_block_ms: u64,
}

/// The fields `TiFlashWaitSummary.mergeExecSummary` reads off a
/// `*tipb.TiFlashWaitSummary`, zero-defaulted like Go's getters.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TiFlashWaitSummarySnapshot {
    /// Go `summary.GetMinTSOWaitNs()`.
    pub min_tso_wait_ns: u64,
    /// Go `summary.GetPipelineBreakerWaitNs()`.
    pub pipeline_breaker_wait_ns: u64,
    /// Go `summary.GetPipelineQueueWaitNs()`.
    pub pipeline_queue_wait_ns: u64,
}

/// The fields `TiFlashNetworkTrafficSummary.mergeExecSummary` reads off a
/// `*tipb.TiFlashNetWorkSummary`. Go dereferences the proto pointers
/// directly here (no `Get*` guards); the plain `u64` fields make that total.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TiFlashNetworkSummarySnapshot {
    /// Go `*summary.InnerZoneSendBytes`.
    pub inner_zone_send_bytes: u64,
    /// Go `*summary.InterZoneSendBytes`.
    pub inter_zone_send_bytes: u64,
    /// Go `*summary.InnerZoneReceiveBytes`.
    pub inner_zone_receive_bytes: u64,
    /// Go `*summary.InterZoneReceiveBytes`.
    pub inter_zone_receive_bytes: u64,
}

/// Go `TiflashStats`: contains tiflash execution stats. The Go fields are
/// package-private; they are `pub` here for the pending `runtime_stats`
/// wiring.
#[derive(Clone, Debug, Default)]
pub struct TiflashStats {
    /// Go `TiflashStats.scanContext`.
    pub scan_context: TiFlashScanContext,
    /// Go `TiflashStats.columnarScanContext`.
    pub columnar_scan_context: TiFlashColumnarScanContext,
    /// Go `TiflashStats.waitSummary`.
    pub wait_summary: TiFlashWaitSummary,
    /// Go `TiflashStats.networkSummary`.
    pub network_summary: TiFlashNetworkTrafficSummary,
}

/// Go `TiFlashScanContext`: the table scan information in tiflash. Go
/// `Clone` (a field-by-field deep copy including the `regionsOfInstance`
/// map) is the derived `Clone` here.
#[derive(Clone, Debug, Default)]
pub struct TiFlashScanContext {
    /// Go `dmfileDataScannedRows`.
    pub dmfile_data_scanned_rows: u64,
    /// Go `dmfileDataSkippedRows`.
    pub dmfile_data_skipped_rows: u64,
    /// Go `dmfileMvccScannedRows`.
    pub dmfile_mvcc_scanned_rows: u64,
    /// Go `dmfileMvccSkippedRows`.
    pub dmfile_mvcc_skipped_rows: u64,
    /// Go `dmfileLmFilterScannedRows`.
    pub dmfile_lm_filter_scanned_rows: u64,
    /// Go `dmfileLmFilterSkippedRows`.
    pub dmfile_lm_filter_skipped_rows: u64,
    /// Go `totalDmfileRsCheckMs`.
    pub total_dmfile_rs_check_ms: u64,
    /// Go `totalDmfileReadMs`.
    pub total_dmfile_read_ms: u64,
    /// Go `totalBuildSnapshotMs`.
    pub total_build_snapshot_ms: u64,
    /// Go `localRegions`.
    pub local_regions: u64,
    /// Go `remoteRegions`.
    pub remote_regions: u64,
    /// Go `totalLearnerReadMs`.
    pub total_learner_read_ms: u64,
    /// Go `disaggReadCacheHitBytes`.
    pub disagg_read_cache_hit_bytes: u64,
    /// Go `disaggReadCacheMissBytes`.
    pub disagg_read_cache_miss_bytes: u64,
    /// Go `segments`.
    pub segments: u64,
    /// Go `readTasks`.
    pub read_tasks: u64,
    /// Go `deltaRows`.
    pub delta_rows: u64,
    /// Go `deltaBytes`.
    pub delta_bytes: u64,
    /// Go `mvccInputRows`.
    pub mvcc_input_rows: u64,
    /// Go `mvccInputBytes`.
    pub mvcc_input_bytes: u64,
    /// Go `mvccOutputRows`.
    pub mvcc_output_rows: u64,
    /// Go `totalBuildBitmapMs`.
    pub total_build_bitmap_ms: u64,
    /// Go `totalBuildInputStreamMs`.
    pub total_build_input_stream_ms: u64,
    /// Go `staleReadRegions`.
    pub stale_read_regions: u64,
    /// Go `minLocalStreamMs`.
    pub min_local_stream_ms: u64,
    /// Go `maxLocalStreamMs`.
    pub max_local_stream_ms: u64,
    /// Go `minRemoteStreamMs`.
    pub min_remote_stream_ms: u64,
    /// Go `maxRemoteStreamMs`.
    pub max_remote_stream_ms: u64,
    /// Go `regionsOfInstance` (a nil Go map is the empty map here).
    pub regions_of_instance: HashMap<String, u64>,
    /// Go `vectorIdxLoadFromS3`.
    pub vector_idx_load_from_s3: u64,
    /// Go `vectorIdxLoadFromDisk`.
    pub vector_idx_load_from_disk: u64,
    /// Go `vectorIdxLoadFromCache`.
    pub vector_idx_load_from_cache: u64,
    /// Go `vectorIdxLoadTimeMs`.
    pub vector_idx_load_time_ms: u64,
    /// Go `vectorIdxSearchTimeMs`.
    pub vector_idx_search_time_ms: u64,
    /// Go `vectorIdxSearchVisitedNodes`.
    pub vector_idx_search_visited_nodes: u64,
    /// Go `vectorIdxSearchDiscardedNodes`.
    pub vector_idx_search_discarded_nodes: u64,
    /// Go `vectorIdxReadVecTimeMs`.
    pub vector_idx_read_vec_time_ms: u64,
    /// Go `vectorIdxReadOthersTimeMs`.
    pub vector_idx_read_others_time_ms: u64,
    /// Go `ftsNFromInmemoryNoindex`.
    pub fts_n_from_inmemory_noindex: u32,
    /// Go `ftsNFromTinyIndex`.
    pub fts_n_from_tiny_index: u32,
    /// Go `ftsNFromTinyNoindex`.
    pub fts_n_from_tiny_noindex: u32,
    /// Go `ftsNFromDmfIndex`.
    pub fts_n_from_dmf_index: u32,
    /// Go `ftsNFromDmfNoindex`.
    pub fts_n_from_dmf_noindex: u32,
    /// Go `ftsRowsFromInmemoryNoindex`.
    pub fts_rows_from_inmemory_noindex: u64,
    /// Go `ftsRowsFromTinyIndex`.
    pub fts_rows_from_tiny_index: u64,
    /// Go `ftsRowsFromTinyNoindex`.
    pub fts_rows_from_tiny_noindex: u64,
    /// Go `ftsRowsFromDmfIndex`.
    pub fts_rows_from_dmf_index: u64,
    /// Go `ftsRowsFromDmfNoindex`.
    pub fts_rows_from_dmf_noindex: u64,
    /// Go `ftsIdxLoadTotalMs`.
    pub fts_idx_load_total_ms: u64,
    /// Go `ftsIdxLoadFromCache`.
    pub fts_idx_load_from_cache: u32,
    /// Go `ftsIdxLoadFromColumnFile`.
    pub fts_idx_load_from_column_file: u32,
    /// Go `ftsIdxLoadFromStableS3`.
    pub fts_idx_load_from_stable_s3: u32,
    /// Go `ftsIdxLoadFromStableDisk`.
    pub fts_idx_load_from_stable_disk: u32,
    /// Go `ftsIdxSearchN`.
    pub fts_idx_search_n: u32,
    /// Go `ftsIdxSearchTotalMs`.
    pub fts_idx_search_total_ms: u64,
    /// Go `ftsIdxDmSearchRows`.
    pub fts_idx_dm_search_rows: u64,
    /// Go `ftsIdxDmTotalReadFtsMs`.
    pub fts_idx_dm_total_read_fts_ms: u64,
    /// Go `ftsIdxDmTotalReadOthersMs`.
    pub fts_idx_dm_total_read_others_ms: u64,
    /// Go `ftsIdxTinySearchRows`.
    pub fts_idx_tiny_search_rows: u64,
    /// Go `ftsIdxTinyTotalReadFtsMs`.
    pub fts_idx_tiny_total_read_fts_ms: u64,
    /// Go `ftsIdxTinyTotalReadOthersMs`.
    pub fts_idx_tiny_total_read_others_ms: u64,
    /// Go `ftsBruteTotalReadMs`.
    pub fts_brute_total_read_ms: u64,
    /// Go `ftsBruteTotalSearchMs`.
    pub fts_brute_total_search_ms: u64,
    /// Go `invertedIdxLoadFromS3`.
    pub inverted_idx_load_from_s3: u32,
    /// Go `invertedIdxLoadFromDisk`.
    pub inverted_idx_load_from_disk: u32,
    /// Go `invertedIdxLoadFromCache`.
    pub inverted_idx_load_from_cache: u32,
    /// Go `invertedIdxLoadTimeMs`.
    pub inverted_idx_load_time_ms: u64,
    /// Go `invertedIdxSearchTimeMs`.
    pub inverted_idx_search_time_ms: u64,
    /// Go `invertedIdxSearchSkippedPacks`.
    pub inverted_idx_search_skipped_packs: u32,
    /// Go `invertedIdxIndexedRows`.
    pub inverted_idx_indexed_rows: u64,
    /// Go `invertedIdxSearchSelectedRows`.
    pub inverted_idx_search_selected_rows: u64,
}

impl TiFlashScanContext {
    /// Go `TiFlashScanContext.String`.
    #[must_use]
    #[expect(
        clippy::too_many_lines,
        reason = "one Go String body, kept in source order"
    )]
    #[expect(clippy::cast_precision_loss, reason = "Go float64(uint64) conversions")]
    pub fn string(&self) -> String {
        let mut output: Vec<String> = Vec::new();
        if self
            .vector_idx_load_from_s3
            .wrapping_add(self.vector_idx_load_from_disk)
            .wrapping_add(self.vector_idx_load_from_cache)
            > 0
        {
            let mut items: Vec<String> = Vec::new();
            items.push(format!(
                "load:{{total:{}ms,from_s3:{},from_disk:{},from_cache:{}}}",
                self.vector_idx_load_time_ms,
                self.vector_idx_load_from_s3,
                self.vector_idx_load_from_disk,
                self.vector_idx_load_from_cache
            ));
            items.push(format!(
                "search:{{total:{}ms,visited_nodes:{},discarded_nodes:{}}}",
                self.vector_idx_search_time_ms,
                self.vector_idx_search_visited_nodes,
                self.vector_idx_search_discarded_nodes
            ));
            items.push(format!(
                "read:{{vec_total:{}ms,others_total:{}ms}}",
                self.vector_idx_read_vec_time_ms, self.vector_idx_read_others_time_ms
            ));
            output.push(format!("vector_idx:{{{}}}", items.join(",")));
        }
        if self
            .inverted_idx_load_from_s3
            .wrapping_add(self.inverted_idx_load_from_disk)
            .wrapping_add(self.inverted_idx_load_from_cache)
            > 0
        {
            let mut items: Vec<String> = Vec::new();
            items.push(format!(
                "load:{{total:{}ms,from_s3:{},from_disk:{},from_cache:{}}}",
                self.inverted_idx_load_time_ms,
                self.inverted_idx_load_from_s3,
                self.inverted_idx_load_from_disk,
                self.inverted_idx_load_from_cache
            ));
            items.push(format!(
                "search:{{total:{}ms,skipped_packs:{},indexed_rows:{},selected_rows:{}}}",
                self.inverted_idx_search_time_ms,
                self.inverted_idx_search_skipped_packs,
                self.inverted_idx_indexed_rows,
                self.inverted_idx_search_selected_rows
            ));
            output.push(format!("inverted_idx:{{{}}}", items.join(",")));
        }
        if self
            .fts_n_from_inmemory_noindex
            .wrapping_add(self.fts_n_from_tiny_index)
            .wrapping_add(self.fts_n_from_tiny_noindex)
            .wrapping_add(self.fts_n_from_dmf_index)
            .wrapping_add(self.fts_n_from_dmf_noindex)
            > 0
        {
            let mut items: Vec<String> = Vec::new();
            items.push(format!(
                "hit_rows:{{delta:{},dmf:{}}}",
                self.fts_rows_from_tiny_index, self.fts_rows_from_dmf_index
            ));
            items.push(format!(
                "miss_rows:{{mem:{},delta:{},dmf:{}}}",
                self.fts_rows_from_inmemory_noindex,
                self.fts_rows_from_tiny_noindex,
                self.fts_rows_from_dmf_noindex
            ));
            items.push(format!(
                "idx_load:{{total:{}ms,from:{{s3:{},disk:{},cache:{}}}}}",
                self.fts_idx_load_total_ms,
                self.fts_idx_load_from_stable_s3,
                self.fts_idx_load_from_stable_disk
                    .wrapping_add(self.fts_idx_load_from_column_file),
                self.fts_idx_load_from_cache
            ));
            let mut avg = 0u64;
            if self.fts_idx_search_n > 0 {
                avg = self.fts_idx_search_total_ms / u64::from(self.fts_idx_search_n);
            }
            items.push(format!(
                "idx_search:{{total:{}ms,avg:{avg}ms}}",
                self.fts_idx_search_total_ms
            ));
            items.push(format!(
                "idx_read:{{rows:{},fts_total:{}ms,others_total:{}ms}}",
                self.fts_idx_dm_search_rows
                    .wrapping_add(self.fts_idx_tiny_search_rows),
                self.fts_idx_dm_total_read_fts_ms
                    .wrapping_add(self.fts_idx_tiny_total_read_fts_ms),
                self.fts_idx_dm_total_read_others_ms
                    .wrapping_add(self.fts_idx_tiny_total_read_others_ms)
            ));
            items.push(format!(
                "miss:{{read:{}ms,search:{}ms}}",
                self.fts_brute_total_read_ms, self.fts_brute_total_search_ms
            ));
            output.push(format!("fts:{{{}}}", items.join(",")));
        }

        let mut region_balance_info = "none".to_owned();
        if !self.regions_of_instance.is_empty() {
            let mut max_num = 0u64;
            let mut min_num = u64::MAX;
            for &v in self.regions_of_instance.values() {
                if v > max_num {
                    max_num = v;
                }
                if v > 0 && v < min_num {
                    min_num = v;
                }
            }
            // Go `%f`: fixed six decimals, matched by `{:.6}` at every
            // reachable point.
            region_balance_info = format!(
                "{{instance_num: {}, max/min: {}/{}={:.6}}}",
                self.regions_of_instance.len(),
                max_num,
                min_num,
                max_num as f64 / min_num as f64
            );
        }
        let mut dmfile_disagg_info = String::new();
        if self.disagg_read_cache_hit_bytes != 0 || self.disagg_read_cache_miss_bytes != 0 {
            dmfile_disagg_info = format!(
                ", disagg_cache_hit_bytes: {}, disagg_cache_miss_bytes: {}",
                self.disagg_read_cache_hit_bytes, self.disagg_read_cache_miss_bytes
            );
        }
        let mut remote_stream_info = String::new();
        if self.min_remote_stream_ms != 0 || self.max_remote_stream_ms != 0 {
            remote_stream_info = format!(
                "min_remote_stream:{}ms, max_remote_stream:{}ms, ",
                self.min_remote_stream_ms, self.max_remote_stream_ms
            );
        }

        // note: "tot" is short for "total"
        output.push(format!(
            "tiflash_scan:{{\
             mvcc_input_rows:{}, \
             mvcc_input_bytes:{}, \
             mvcc_output_rows:{}, \
             local_regions:{}, \
             remote_regions:{}, \
             tot_learner_read:{}ms, \
             region_balance:{}, \
             delta_rows:{}, \
             delta_bytes:{}, \
             segments:{}, \
             stale_read_regions:{}, \
             tot_build_snapshot:{}ms, \
             tot_build_bitmap:{}ms, \
             tot_build_inputstream:{}ms, \
             min_local_stream:{}ms, \
             max_local_stream:{}ms, \
             {}\
             dtfile:{{\
             data_scanned_rows:{}, \
             data_skipped_rows:{}, \
             mvcc_scanned_rows:{}, \
             mvcc_skipped_rows:{}, \
             lm_filter_scanned_rows:{}, \
             lm_filter_skipped_rows:{}, \
             tot_rs_index_check:{}ms, \
             tot_read:{}ms\
             {}}}\
             }}",
            self.mvcc_input_rows,
            self.mvcc_input_bytes,
            self.mvcc_output_rows,
            self.local_regions,
            self.remote_regions,
            self.total_learner_read_ms,
            region_balance_info,
            self.delta_rows,
            self.delta_bytes,
            self.segments,
            self.stale_read_regions,
            self.total_build_snapshot_ms,
            self.total_build_bitmap_ms,
            self.total_build_input_stream_ms,
            self.min_local_stream_ms,
            self.max_local_stream_ms,
            remote_stream_info,
            self.dmfile_data_scanned_rows,
            self.dmfile_data_skipped_rows,
            self.dmfile_mvcc_scanned_rows,
            self.dmfile_mvcc_skipped_rows,
            self.dmfile_lm_filter_scanned_rows,
            self.dmfile_lm_filter_skipped_rows,
            self.total_dmfile_rs_check_ms,
            self.total_dmfile_read_ms,
            dmfile_disagg_info,
        ));

        output.join(", ")
    }

    /// Go `TiFlashScanContext.Merge`: makes sum to merge the information.
    pub fn merge(&mut self, other: &TiFlashScanContext) {
        self.dmfile_data_scanned_rows += other.dmfile_data_scanned_rows;
        self.dmfile_data_skipped_rows += other.dmfile_data_skipped_rows;
        self.dmfile_mvcc_scanned_rows += other.dmfile_mvcc_scanned_rows;
        self.dmfile_mvcc_skipped_rows += other.dmfile_mvcc_skipped_rows;
        self.dmfile_lm_filter_scanned_rows += other.dmfile_lm_filter_scanned_rows;
        self.dmfile_lm_filter_skipped_rows += other.dmfile_lm_filter_skipped_rows;
        self.total_dmfile_rs_check_ms += other.total_dmfile_rs_check_ms;
        self.total_dmfile_read_ms += other.total_dmfile_read_ms;
        self.total_build_snapshot_ms += other.total_build_snapshot_ms;
        self.local_regions += other.local_regions;
        self.remote_regions += other.remote_regions;
        self.total_learner_read_ms += other.total_learner_read_ms;
        self.disagg_read_cache_hit_bytes += other.disagg_read_cache_hit_bytes;
        self.disagg_read_cache_miss_bytes += other.disagg_read_cache_miss_bytes;
        self.segments += other.segments;
        self.read_tasks += other.read_tasks;
        self.delta_rows += other.delta_rows;
        self.delta_bytes += other.delta_bytes;
        self.mvcc_input_rows += other.mvcc_input_rows;
        self.mvcc_input_bytes += other.mvcc_input_bytes;
        self.mvcc_output_rows += other.mvcc_output_rows;
        self.total_build_bitmap_ms += other.total_build_bitmap_ms;
        self.total_build_input_stream_ms += other.total_build_input_stream_ms;
        self.stale_read_regions += other.stale_read_regions;

        self.vector_idx_load_from_s3 += other.vector_idx_load_from_s3;
        self.vector_idx_load_from_disk += other.vector_idx_load_from_disk;
        self.vector_idx_load_from_cache += other.vector_idx_load_from_cache;
        self.vector_idx_load_time_ms += other.vector_idx_load_time_ms;
        self.vector_idx_search_time_ms += other.vector_idx_search_time_ms;
        self.vector_idx_search_visited_nodes += other.vector_idx_search_visited_nodes;
        self.vector_idx_search_discarded_nodes += other.vector_idx_search_discarded_nodes;
        self.vector_idx_read_vec_time_ms += other.vector_idx_read_vec_time_ms;
        self.vector_idx_read_others_time_ms += other.vector_idx_read_others_time_ms;

        self.fts_n_from_inmemory_noindex += other.fts_n_from_inmemory_noindex;
        self.fts_n_from_tiny_index += other.fts_n_from_tiny_index;
        self.fts_n_from_tiny_noindex += other.fts_n_from_tiny_noindex;
        self.fts_n_from_dmf_index += other.fts_n_from_dmf_index;
        self.fts_n_from_dmf_noindex += other.fts_n_from_dmf_noindex;
        self.fts_rows_from_inmemory_noindex += other.fts_rows_from_inmemory_noindex;
        self.fts_rows_from_tiny_index += other.fts_rows_from_tiny_index;
        self.fts_rows_from_tiny_noindex += other.fts_rows_from_tiny_noindex;
        self.fts_rows_from_dmf_index += other.fts_rows_from_dmf_index;
        self.fts_rows_from_dmf_noindex += other.fts_rows_from_dmf_noindex;
        self.fts_idx_load_total_ms += other.fts_idx_load_total_ms;
        self.fts_idx_load_from_cache += other.fts_idx_load_from_cache;
        self.fts_idx_load_from_column_file += other.fts_idx_load_from_column_file;
        self.fts_idx_load_from_stable_s3 += other.fts_idx_load_from_stable_s3;
        self.fts_idx_load_from_stable_disk += other.fts_idx_load_from_stable_disk;
        self.fts_idx_search_n += other.fts_idx_search_n;
        self.fts_idx_search_total_ms += other.fts_idx_search_total_ms;
        self.fts_idx_dm_search_rows += other.fts_idx_dm_search_rows;
        self.fts_idx_dm_total_read_fts_ms += other.fts_idx_dm_total_read_fts_ms;
        self.fts_idx_dm_total_read_others_ms += other.fts_idx_dm_total_read_others_ms;
        self.fts_idx_tiny_search_rows += other.fts_idx_tiny_search_rows;
        self.fts_idx_tiny_total_read_fts_ms += other.fts_idx_tiny_total_read_fts_ms;
        self.fts_idx_tiny_total_read_others_ms += other.fts_idx_tiny_total_read_others_ms;
        self.fts_brute_total_read_ms += other.fts_brute_total_read_ms;
        self.fts_brute_total_search_ms += other.fts_brute_total_search_ms;

        self.inverted_idx_load_from_s3 += other.inverted_idx_load_from_s3;
        self.inverted_idx_load_from_disk += other.inverted_idx_load_from_disk;
        self.inverted_idx_load_from_cache += other.inverted_idx_load_from_cache;
        self.inverted_idx_load_time_ms += other.inverted_idx_load_time_ms;
        self.inverted_idx_search_time_ms += other.inverted_idx_search_time_ms;
        self.inverted_idx_search_skipped_packs += other.inverted_idx_search_skipped_packs;
        self.inverted_idx_indexed_rows += other.inverted_idx_indexed_rows;
        self.inverted_idx_search_selected_rows += other.inverted_idx_search_selected_rows;

        if self.min_local_stream_ms == 0 || other.min_local_stream_ms < self.min_local_stream_ms {
            self.min_local_stream_ms = other.min_local_stream_ms;
        }
        if other.max_local_stream_ms > self.max_local_stream_ms {
            self.max_local_stream_ms = other.max_local_stream_ms;
        }
        if self.min_remote_stream_ms == 0 || other.min_remote_stream_ms < self.min_remote_stream_ms
        {
            self.min_remote_stream_ms = other.min_remote_stream_ms;
        }
        if other.max_remote_stream_ms > self.max_remote_stream_ms {
            self.max_remote_stream_ms = other.max_remote_stream_ms;
        }

        for (k, v) in &other.regions_of_instance {
            *self.regions_of_instance.entry(k.clone()).or_insert(0) += v;
        }
    }

    /// Go `TiFlashScanContext.mergeExecSummary`: merges a
    /// `tipb.TiFlashScanContext` (its snapshot narrowing) directly; a `None`
    /// summary is Go's nil early return.
    pub fn merge_exec_summary(&mut self, summary: Option<&TiFlashScanContextSnapshot>) {
        let Some(summary) = summary else {
            return;
        };
        self.dmfile_data_scanned_rows += summary.dmfile_data_scanned_rows;
        self.dmfile_data_skipped_rows += summary.dmfile_data_skipped_rows;
        self.dmfile_mvcc_scanned_rows += summary.dmfile_mvcc_scanned_rows;
        self.dmfile_mvcc_skipped_rows += summary.dmfile_mvcc_skipped_rows;
        self.dmfile_lm_filter_scanned_rows += summary.dmfile_lm_filter_scanned_rows;
        self.dmfile_lm_filter_skipped_rows += summary.dmfile_lm_filter_skipped_rows;
        self.total_dmfile_rs_check_ms += summary.total_dmfile_rs_check_ms;
        self.total_dmfile_read_ms += summary.total_dmfile_read_ms;
        self.total_build_snapshot_ms += summary.total_build_snapshot_ms;
        self.local_regions += summary.local_regions;
        self.remote_regions += summary.remote_regions;
        self.total_learner_read_ms += summary.total_learner_read_ms;
        self.disagg_read_cache_hit_bytes += summary.disagg_read_cache_hit_bytes;
        self.disagg_read_cache_miss_bytes += summary.disagg_read_cache_miss_bytes;
        self.segments += summary.segments;
        self.read_tasks += summary.read_tasks;
        self.delta_rows += summary.delta_rows;
        self.delta_bytes += summary.delta_bytes;
        self.mvcc_input_rows += summary.mvcc_input_rows;
        self.mvcc_input_bytes += summary.mvcc_input_bytes;
        self.mvcc_output_rows += summary.mvcc_output_rows;
        self.total_build_bitmap_ms += summary.total_build_bitmap_ms;
        self.total_build_input_stream_ms += summary.total_build_inputstream_ms;
        self.stale_read_regions += summary.stale_read_regions;

        self.vector_idx_load_from_s3 += summary.vector_idx_load_from_s3;
        self.vector_idx_load_from_disk += summary.vector_idx_load_from_disk;
        self.vector_idx_load_from_cache += summary.vector_idx_load_from_cache;
        self.vector_idx_load_time_ms += summary.vector_idx_load_time_ms;
        self.vector_idx_search_time_ms += summary.vector_idx_search_time_ms;
        self.vector_idx_search_visited_nodes += summary.vector_idx_search_visited_nodes;
        self.vector_idx_search_discarded_nodes += summary.vector_idx_search_discarded_nodes;
        self.vector_idx_read_vec_time_ms += summary.vector_idx_read_vec_time_ms;
        self.vector_idx_read_others_time_ms += summary.vector_idx_read_others_time_ms;

        self.fts_n_from_inmemory_noindex += summary.fts_n_from_inmemory_noindex;
        self.fts_n_from_tiny_index += summary.fts_n_from_tiny_index;
        self.fts_n_from_tiny_noindex += summary.fts_n_from_tiny_noindex;
        self.fts_n_from_dmf_index += summary.fts_n_from_dmf_index;
        self.fts_n_from_dmf_noindex += summary.fts_n_from_dmf_noindex;
        self.fts_rows_from_inmemory_noindex += summary.fts_rows_from_inmemory_noindex;
        self.fts_rows_from_tiny_index += summary.fts_rows_from_tiny_index;
        self.fts_rows_from_tiny_noindex += summary.fts_rows_from_tiny_noindex;
        self.fts_rows_from_dmf_index += summary.fts_rows_from_dmf_index;
        self.fts_rows_from_dmf_noindex += summary.fts_rows_from_dmf_noindex;
        self.fts_idx_load_total_ms += summary.fts_idx_load_total_ms;
        self.fts_idx_load_from_cache += summary.fts_idx_load_from_cache;
        self.fts_idx_load_from_column_file += summary.fts_idx_load_from_column_file;
        self.fts_idx_load_from_stable_s3 += summary.fts_idx_load_from_stable_s3;
        self.fts_idx_load_from_stable_disk += summary.fts_idx_load_from_stable_disk;
        self.fts_idx_search_n += summary.fts_idx_search_n;
        self.fts_idx_search_total_ms += summary.fts_idx_search_total_ms;
        self.fts_idx_dm_search_rows += summary.fts_idx_dm_search_rows;
        self.fts_idx_dm_total_read_fts_ms += summary.fts_idx_dm_total_read_fts_ms;
        self.fts_idx_dm_total_read_others_ms += summary.fts_idx_dm_total_read_others_ms;
        self.fts_idx_tiny_search_rows += summary.fts_idx_tiny_search_rows;
        self.fts_idx_tiny_total_read_fts_ms += summary.fts_idx_tiny_total_read_fts_ms;
        self.fts_idx_tiny_total_read_others_ms += summary.fts_idx_tiny_total_read_others_ms;
        self.fts_brute_total_read_ms += summary.fts_brute_total_read_ms;
        self.fts_brute_total_search_ms += summary.fts_brute_total_search_ms;

        self.inverted_idx_load_from_s3 += summary.inverted_idx_load_from_s3;
        self.inverted_idx_load_from_disk += summary.inverted_idx_load_from_disk;
        self.inverted_idx_load_from_cache += summary.inverted_idx_load_from_cache;
        self.inverted_idx_load_time_ms += summary.inverted_idx_load_time_ms;
        self.inverted_idx_search_time_ms += summary.inverted_idx_search_time_ms;
        self.inverted_idx_search_skipped_packs += summary.inverted_idx_search_skipped_packs;
        self.inverted_idx_indexed_rows += summary.inverted_idx_indexed_rows;
        self.inverted_idx_search_selected_rows += summary.inverted_idx_search_selected_rows;

        if self.min_local_stream_ms == 0 || summary.min_local_stream_ms < self.min_local_stream_ms {
            self.min_local_stream_ms = summary.min_local_stream_ms;
        }
        if summary.max_local_stream_ms > self.max_local_stream_ms {
            self.max_local_stream_ms = summary.max_local_stream_ms;
        }
        if self.min_remote_stream_ms == 0
            || summary.min_remote_stream_ms < self.min_remote_stream_ms
        {
            self.min_remote_stream_ms = summary.min_remote_stream_ms;
        }
        if summary.max_remote_stream_ms > self.max_remote_stream_ms {
            self.max_remote_stream_ms = summary.max_remote_stream_ms;
        }

        for instance in &summary.regions_of_instance {
            *self
                .regions_of_instance
                .entry(instance.instance_id.clone())
                .or_insert(0) += instance.region_num;
        }
    }

    /// Go `TiFlashScanContext.Empty`: if scan no pack and skip no pack, we
    /// regard it as empty.
    #[must_use]
    pub fn empty(&self) -> bool {
        self.dmfile_data_scanned_rows == 0
            && self.dmfile_data_skipped_rows == 0
            && self.dmfile_mvcc_scanned_rows == 0
            && self.dmfile_mvcc_skipped_rows == 0
            && self.dmfile_lm_filter_scanned_rows == 0
            && self.dmfile_lm_filter_skipped_rows == 0
            && self.local_regions == 0
            && self.remote_regions == 0
            && self.vector_idx_load_from_disk == 0
            && self.vector_idx_load_from_cache == 0
            && self.vector_idx_load_from_s3 == 0
            && self.inverted_idx_load_from_disk == 0
            && self.inverted_idx_load_from_cache == 0
            && self.inverted_idx_load_from_s3 == 0
            && self.fts_n_from_inmemory_noindex == 0
            && self.fts_n_from_tiny_index == 0
            && self.fts_n_from_tiny_noindex == 0
            && self.fts_n_from_dmf_index == 0
            && self.fts_n_from_dmf_noindex == 0
    }
}

/// Go `TiFlashColumnarScanContext`: the table scan information in the
/// tiflash columnar read path. Go `Clone` is the derived `Clone`.
#[derive(Clone, Debug, Default)]
pub struct TiFlashColumnarScanContext {
    /// Go `hasStats`.
    pub has_stats: bool,
    /// Go `regions`.
    pub regions: u64,
    /// Go `readTasks`.
    pub read_tasks: u64,
    /// Go `physicalTables`.
    pub physical_tables: u64,
    /// Go `columns`.
    pub columns: u64,
    /// Go `userReadBytes`.
    pub user_read_bytes: u64,
    /// Go `mvccInputRows`.
    pub mvcc_input_rows: u64,
    /// Go `mvccInputBytes`.
    pub mvcc_input_bytes: u64,
    /// Go `mvccOutputRows`.
    pub mvcc_output_rows: u64,
    /// Go `totalReadBlockMs`.
    pub total_read_block_ms: u64,
    /// Go `totalSerializeBlockMs`.
    pub total_serialize_block_ms: u64,
    /// Go `totalInitReaderMs`.
    pub total_init_reader_ms: u64,
    /// Go `totalPrefetchMs`.
    pub total_prefetch_ms: u64,
    /// Go `roughCheckTotalPacks`.
    pub rough_check_total_packs: u64,
    /// Go `roughCheckSelectedPacks`.
    pub rough_check_selected_packs: u64,
    /// Go `roughCheckSkippedPacks`.
    pub rough_check_skipped_packs: u64,
    /// Go `roughCheckUnknownPacks`.
    pub rough_check_unknown_packs: u64,
    /// Go `remoteSegments`.
    pub remote_segments: u64,
    /// Go `totalSegments`.
    pub total_segments: u64,
    /// Go `totalDeserializeBlockMs`.
    pub total_deserialize_block_ms: u64,
}

impl TiFlashColumnarScanContext {
    /// Go `TiFlashColumnarScanContext.String`.
    #[must_use]
    pub fn string(&self) -> String {
        format!(
            "columnar_scan:{{\
             mvcc_input_rows:{}, \
             mvcc_input_bytes:{}, \
             mvcc_output_rows:{}, \
             regions:{}, \
             read_tasks:{}, \
             physical_tables:{}, \
             columns:{}, \
             user_read_bytes:{}, \
             read_block:{}ms, \
             serialize_block:{}ms, \
             init_reader:{}ms, \
             prefetch:{}ms, \
             deserialize_block:{}ms, \
             rough_check:{{total:{}, selected:{}, skipped:{}, unknown:{}}}, \
             remote_segments:{}, \
             total_segments:{}}}",
            self.mvcc_input_rows,
            self.mvcc_input_bytes,
            self.mvcc_output_rows,
            self.regions,
            self.read_tasks,
            self.physical_tables,
            self.columns,
            self.user_read_bytes,
            self.total_read_block_ms,
            self.total_serialize_block_ms,
            self.total_init_reader_ms,
            self.total_prefetch_ms,
            self.total_deserialize_block_ms,
            self.rough_check_total_packs,
            self.rough_check_selected_packs,
            self.rough_check_skipped_packs,
            self.rough_check_unknown_packs,
            self.remote_segments,
            self.total_segments
        )
    }

    /// Go `TiFlashColumnarScanContext.Merge`: makes sum to merge the
    /// information.
    pub fn merge(&mut self, other: &TiFlashColumnarScanContext) {
        self.has_stats = self.has_stats || other.has_stats;
        self.regions += other.regions;
        self.read_tasks += other.read_tasks;
        if other.physical_tables > self.physical_tables {
            self.physical_tables = other.physical_tables;
        }
        if other.columns > self.columns {
            self.columns = other.columns;
        }
        self.user_read_bytes += other.user_read_bytes;
        self.mvcc_input_rows += other.mvcc_input_rows;
        self.mvcc_input_bytes += other.mvcc_input_bytes;
        self.mvcc_output_rows += other.mvcc_output_rows;
        self.total_read_block_ms += other.total_read_block_ms;
        self.total_serialize_block_ms += other.total_serialize_block_ms;
        self.total_init_reader_ms += other.total_init_reader_ms;
        self.total_prefetch_ms += other.total_prefetch_ms;
        self.rough_check_total_packs += other.rough_check_total_packs;
        self.rough_check_selected_packs += other.rough_check_selected_packs;
        self.rough_check_skipped_packs += other.rough_check_skipped_packs;
        self.rough_check_unknown_packs += other.rough_check_unknown_packs;
        self.remote_segments += other.remote_segments;
        self.total_segments += other.total_segments;
        self.total_deserialize_block_ms += other.total_deserialize_block_ms;
    }

    /// Go `TiFlashColumnarScanContext.mergeExecSummary`: merges a
    /// `tipb.ColumnarScanContext` (its snapshot narrowing) directly; a
    /// `None` summary is Go's nil early return.
    pub fn merge_exec_summary(&mut self, summary: Option<&ColumnarScanContextSnapshot>) {
        let Some(summary) = summary else {
            return;
        };
        self.has_stats = true;
        self.regions += summary.regions;
        self.read_tasks += summary.read_tasks;
        if summary.physical_tables > self.physical_tables {
            self.physical_tables = summary.physical_tables;
        }
        if summary.columns > self.columns {
            self.columns = summary.columns;
        }
        self.user_read_bytes += summary.user_read_bytes;
        self.mvcc_input_rows += summary.mvcc_input_rows;
        self.mvcc_input_bytes += summary.mvcc_input_bytes;
        self.mvcc_output_rows += summary.mvcc_output_rows;
        self.total_read_block_ms += summary.total_read_block_ms;
        self.total_serialize_block_ms += summary.total_serialize_block_ms;
        self.total_init_reader_ms += summary.total_init_reader_ms;
        self.total_prefetch_ms += summary.total_prefetch_ms;
        self.rough_check_total_packs += summary.rough_check_total_packs;
        self.rough_check_selected_packs += summary.rough_check_selected_packs;
        self.rough_check_skipped_packs += summary.rough_check_skipped_packs;
        self.rough_check_unknown_packs += summary.rough_check_unknown_packs;
        self.remote_segments += summary.remote_segments;
        self.total_segments += summary.total_segments;
        self.total_deserialize_block_ms += summary.total_deserialize_block_ms;
    }

    /// Go `TiFlashColumnarScanContext.Empty`.
    #[must_use]
    pub fn empty(&self) -> bool {
        !self.has_stats
            && self.regions == 0
            && self.read_tasks == 0
            && self.physical_tables == 0
            && self.columns == 0
            && self.user_read_bytes == 0
            && self.mvcc_input_rows == 0
            && self.mvcc_input_bytes == 0
            && self.mvcc_output_rows == 0
            && self.total_read_block_ms == 0
            && self.total_serialize_block_ms == 0
            && self.total_init_reader_ms == 0
            && self.total_prefetch_ms == 0
            && self.rough_check_total_packs == 0
            && self.rough_check_selected_packs == 0
            && self.rough_check_skipped_packs == 0
            && self.rough_check_unknown_packs == 0
            && self.remote_segments == 0
            && self.total_segments == 0
            && self.total_deserialize_block_ms == 0
    }
}

/// Go `TiFlashWaitSummary`: all kinds of wait information in tiflash. Go
/// `Clone` is the derived `Clone`.
#[derive(Clone, Copy, Debug, Default)]
pub struct TiFlashWaitSummary {
    /// Go `executionTime`: keeps execution time to do merge work, always
    /// record the wait time with largest execution time.
    pub execution_time: u64,
    /// Go `minTSOWaitTime` (nanoseconds).
    pub min_tso_wait_time: u64,
    /// Go `pipelineBreakerWaitTime` (nanoseconds).
    pub pipeline_breaker_wait_time: u64,
    /// Go `pipelineQueueWaitTime` (nanoseconds).
    pub pipeline_queue_wait_time: u64,
}

impl TiFlashWaitSummary {
    /// Go `TiFlashWaitSummary.String`: dumps the wait summary info as a
    /// string. Go's `time.Duration.Milliseconds()` is the truncating
    /// nanoseconds-over-a-million division.
    #[must_use]
    pub fn string(&self) -> String {
        if self.can_be_ignored() {
            return String::new();
        }
        let mut buf = String::with_capacity(32);
        buf.push_str("tiflash_wait: {");
        let mut empty = true;
        if self.min_tso_wait_time >= MILLISECOND_NS {
            buf.push_str("minTSO_wait: ");
            buf.push_str(&(self.min_tso_wait_time / MILLISECOND_NS).to_string());
            buf.push_str("ms");
            empty = false;
        }
        if self.pipeline_breaker_wait_time >= MILLISECOND_NS {
            if !empty {
                buf.push_str(", ");
            }
            buf.push_str("pipeline_breaker_wait: ");
            buf.push_str(&(self.pipeline_breaker_wait_time / MILLISECOND_NS).to_string());
            buf.push_str("ms");
            empty = false;
        }
        if self.pipeline_queue_wait_time >= MILLISECOND_NS {
            if !empty {
                buf.push_str(", ");
            }
            buf.push_str("pipeline_queue_wait: ");
            buf.push_str(&(self.pipeline_queue_wait_time / MILLISECOND_NS).to_string());
            buf.push_str("ms");
        }
        buf.push('}');
        buf
    }

    /// Go `TiFlashWaitSummary.Merge`: keeps the wait times of the side with
    /// the larger execution time.
    pub fn merge(&mut self, other: &TiFlashWaitSummary) {
        if self.execution_time < other.execution_time {
            self.execution_time = other.execution_time;
            self.min_tso_wait_time = other.min_tso_wait_time;
            self.pipeline_breaker_wait_time = other.pipeline_breaker_wait_time;
            self.pipeline_queue_wait_time = other.pipeline_queue_wait_time;
        }
    }

    /// Go `TiFlashWaitSummary.mergeExecSummary`: merges a
    /// `tipb.TiFlashWaitSummary` (its snapshot narrowing) directly; a
    /// `None` summary is Go's nil early return.
    pub fn merge_exec_summary(
        &mut self,
        summary: Option<&TiFlashWaitSummarySnapshot>,
        execution_time: u64,
    ) {
        let Some(summary) = summary else {
            return;
        };
        if self.execution_time < execution_time {
            self.execution_time = execution_time;
            self.min_tso_wait_time = summary.min_tso_wait_ns;
            self.pipeline_breaker_wait_time = summary.pipeline_breaker_wait_ns;
            self.pipeline_queue_wait_time = summary.pipeline_queue_wait_ns;
        }
    }

    /// Go `TiFlashWaitSummary.CanBeIgnored`: not all tidb executors have
    /// significant tiflash wait summary.
    #[must_use]
    pub fn can_be_ignored(&self) -> bool {
        self.min_tso_wait_time < MILLISECOND_NS
            && self.pipeline_breaker_wait_time < MILLISECOND_NS
            && self.pipeline_queue_wait_time < MILLISECOND_NS
    }
}

/// Go `TiFlashNetworkTrafficSummary`: network traffic in tiflash. Go
/// `Clone` is the derived `Clone`.
#[derive(Clone, Copy, Debug, Default)]
pub struct TiFlashNetworkTrafficSummary {
    /// Go `innerZoneSendBytes`.
    pub inner_zone_send_bytes: u64,
    /// Go `interZoneSendBytes`.
    pub inter_zone_send_bytes: u64,
    /// Go `innerZoneReceiveBytes`.
    pub inner_zone_receive_bytes: u64,
    /// Go `interZoneReceiveBytes`.
    pub inter_zone_receive_bytes: u64,
}

impl TiFlashNetworkTrafficSummary {
    /// Go `TiFlashNetworkTrafficSummary.UpdateTiKVExecDetails`: updates
    /// `tikvDetails` with this summary's values. Go's nil-`tikvDetails`
    /// early return is the caller's `Option` here, and Go's
    /// `atomic.AddInt64` on the live client-go `util.ExecDetails` collapses
    /// to plain `+=` on the already-loaded snapshot.
    #[expect(
        clippy::cast_possible_wrap,
        reason = "Go int64(uint64) conversions before atomic.AddInt64"
    )]
    pub fn update_tikv_exec_details(&self, tikv_details: &mut TikvExecDetailsSnapshot) {
        tikv_details.unpacked_bytes_sent_mpp_cross_zone += self.inter_zone_send_bytes as i64;
        tikv_details.unpacked_bytes_sent_mpp_total += self.inter_zone_send_bytes as i64;
        tikv_details.unpacked_bytes_sent_mpp_total += self.inner_zone_send_bytes as i64;

        tikv_details.unpacked_bytes_received_mpp_cross_zone += self.inter_zone_receive_bytes as i64;
        tikv_details.unpacked_bytes_received_mpp_total += self.inter_zone_receive_bytes as i64;
        tikv_details.unpacked_bytes_received_mpp_total += self.inner_zone_receive_bytes as i64;
    }

    /// Go `TiFlashNetworkTrafficSummary.Empty`: if no any network traffic,
    /// we regard it as empty.
    #[must_use]
    pub fn empty(&self) -> bool {
        self.inner_zone_send_bytes == 0
            && self.inter_zone_send_bytes == 0
            && self.inner_zone_receive_bytes == 0
            && self.inter_zone_receive_bytes == 0
    }

    /// Go `TiFlashNetworkTrafficSummary.String`: dumps the network traffic
    /// info as a string (Go renders each `uint64` through
    /// `strconv.FormatInt(int64(...))`).
    #[must_use]
    #[expect(
        clippy::cast_possible_wrap,
        reason = "Go strconv.FormatInt(int64(uint64)) rendering"
    )]
    pub fn string(&self) -> String {
        let mut buf = String::with_capacity(32);
        buf.push_str("tiflash_network: {");
        let mut empty = true;
        if self.inner_zone_send_bytes != 0 {
            buf.push_str("inner_zone_send_bytes: ");
            buf.push_str(&(self.inner_zone_send_bytes as i64).to_string());
            empty = false;
        }
        if self.inter_zone_send_bytes != 0 {
            if !empty {
                buf.push_str(", ");
            }
            buf.push_str("inter_zone_send_bytes: ");
            buf.push_str(&(self.inter_zone_send_bytes as i64).to_string());
            empty = false;
        }
        if self.inner_zone_receive_bytes != 0 {
            if !empty {
                buf.push_str(", ");
            }
            buf.push_str("inner_zone_receive_bytes: ");
            buf.push_str(&(self.inner_zone_receive_bytes as i64).to_string());
            empty = false;
        }
        if self.inter_zone_receive_bytes != 0 {
            if !empty {
                buf.push_str(", ");
            }
            buf.push_str("inter_zone_receive_bytes: ");
            buf.push_str(&(self.inter_zone_receive_bytes as i64).to_string());
        }
        buf.push('}');
        buf
    }

    /// Go `TiFlashNetworkTrafficSummary.Merge`: makes sum to merge the
    /// information.
    pub fn merge(&mut self, other: &TiFlashNetworkTrafficSummary) {
        self.inner_zone_send_bytes += other.inner_zone_send_bytes;
        self.inter_zone_send_bytes += other.inter_zone_send_bytes;
        self.inner_zone_receive_bytes += other.inner_zone_receive_bytes;
        self.inter_zone_receive_bytes += other.inter_zone_receive_bytes;
    }

    /// Go `TiFlashNetworkTrafficSummary.mergeExecSummary`: merges a
    /// `tipb.TiFlashNetWorkSummary` (its snapshot narrowing) directly; a
    /// `None` summary is Go's nil early return. Go dereferences the proto
    /// field pointers without getters here; the snapshot's plain fields
    /// make that total.
    pub fn merge_exec_summary(&mut self, summary: Option<&TiFlashNetworkSummarySnapshot>) {
        let Some(summary) = summary else {
            return;
        };
        self.inner_zone_send_bytes += summary.inner_zone_send_bytes;
        self.inter_zone_send_bytes += summary.inter_zone_send_bytes;
        self.inner_zone_receive_bytes += summary.inner_zone_receive_bytes;
        self.inter_zone_receive_bytes += summary.inter_zone_receive_bytes;
    }

    /// Go `TiFlashNetworkTrafficSummary.GetInterZoneTrafficBytes`: the inter
    /// zone network traffic bytes involved between tiflash instances. Go's
    /// nil-receiver zero branch is the caller's `Option` here.
    ///
    /// NOTE: we only count the inter zone sent bytes here because tiflash
    /// count the traffic bytes of all sub request. For each sub request,
    /// both side with count the send and recv traffic. So here, we only use
    /// the send bytes as the overall traffic to avoid count the traffic
    /// twice. While this statistics logic seems a bit weird to me, but this
    /// is the tiflash side desicion.
    #[must_use]
    pub fn get_inter_zone_traffic_bytes(&self) -> u64 {
        self.inter_zone_send_bytes
    }
}

// boundary: Go `MergeTiFlashRUConsumption` (tiflash_stats.go) is not ported —
// it protobuf-unmarshals `resource_manager.Consumption` out of
// `summary.GetRuConsumption()` bytes and feeds client-go
// `util.RUDetails.UpdateTiFlash`/`Merge`, neither of which exists here.

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec_details::ScanDetail;
    use crate::runtime_stats::{
        format_duration, CopScanDetail, CopTimeDetail, Duration, Percentile, StoreType,
    };
    use std::collections::HashSet;
    use std::time::Duration as StdDuration;

    // boundary: the Go tests drive `tiflash_stats.go` through
    // `runtime_stats.go`'s TiFlash arms, which `runtime_stats.rs`
    // deliberately left open. The harness below mirrors exactly those arms —
    // `basicCopRuntimeStats` (its `tiflashStats` field, `String`, and
    // `mergeExecSummary`; the unread `rows` field is dropped),
    // `CopRuntimeStats` (`String` with `printTiFlashSpecificInfo`),
    // `StmtCopRuntimeStats` (+ `mergeExecSummary`), and
    // `RuntimeStatsColl.RecordOneCopTask`/`RecordCopStats`/`GetCopStats`/
    // `GetRootStats`/`ExistsRootStats`/`ExistsCopStats`/
    // `GetStmtCopRuntimeStats`/`getPlanIDFromExecutionSummary` — reusing
    // `runtime_stats.rs`'s public `Percentile`/`Duration`/
    // `format_duration`/`StoreType`/`CopScanDetail`/`CopTimeDetail`.

    /// Go `tipb.ExecutorExecutionSummary.DetailInfo` oneof, narrowed to the
    /// two variants these tests build.
    #[expect(
        clippy::large_enum_variant,
        reason = "test-only mirror of the Go oneof; the wide scan snapshot \
                  variant is intentional"
    )]
    enum FlashDetailInfo {
        Scan(TiFlashScanContextSnapshot),
        Columnar(ColumnarScanContextSnapshot),
    }

    /// The `*tipb.ExecutorExecutionSummary` slice the TiFlash recording
    /// path reads, mirroring Go's getters (`GetTiflashScanContext` yields
    /// nil unless `DetailInfo` holds the scan variant, etc.).
    struct FlashExecSummary {
        time_processed_ns: u64,
        #[expect(
            dead_code,
            reason = "Go NumProducedRows feeds basicCopRuntimeStats.rows, which \
                      these tests never read; kept for the mock's Go signature"
        )]
        num_produced_rows: u64,
        num_iterations: u64,
        concurrency: u64,
        executor_id: String,
        detail_info: Option<FlashDetailInfo>,
        tiflash_wait_summary: Option<TiFlashWaitSummarySnapshot>,
        tiflash_network_summary: Option<TiFlashNetworkSummarySnapshot>,
    }

    /// Go `basicCopRuntimeStats`' TiFlash slice.
    #[derive(Default)]
    struct FlashBasicCopStats {
        loops: i32,
        threads: i32,
        proc_times: Percentile<Duration>,
        tiflash_stats: Option<TiflashStats>,
    }

    impl FlashBasicCopStats {
        /// Go `basicCopRuntimeStats.mergeExecSummary` (TiFlash arms
        /// included).
        #[expect(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
        fn merge_exec_summary(&mut self, summary: &FlashExecSummary) {
            self.loops += summary.num_iterations as i32;
            // Go also accumulates `rows` from NumProducedRows; these tests
            // never read it, so the harness drops the field.
            self.threads += summary.concurrency as i32;
            self.proc_times
                .add(Duration(summary.time_processed_ns as i64));
            match &summary.detail_info {
                Some(FlashDetailInfo::Scan(scan)) => {
                    self.tiflash_stats
                        .get_or_insert_with(TiflashStats::default)
                        .scan_context
                        .merge_exec_summary(Some(scan));
                }
                Some(FlashDetailInfo::Columnar(columnar)) => {
                    self.tiflash_stats
                        .get_or_insert_with(TiflashStats::default)
                        .columnar_scan_context
                        .merge_exec_summary(Some(columnar));
                }
                None => {}
            }
            if let Some(wait) = &summary.tiflash_wait_summary {
                self.tiflash_stats
                    .get_or_insert_with(TiflashStats::default)
                    .wait_summary
                    .merge_exec_summary(Some(wait), summary.time_processed_ns);
            }
            if let Some(network) = &summary.tiflash_network_summary {
                self.tiflash_stats
                    .get_or_insert_with(TiflashStats::default)
                    .network_summary
                    .merge_exec_summary(Some(network));
            }
        }

        /// Go `basicCopRuntimeStats.String` (TiFlash arms included).
        #[expect(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        fn string(&self) -> String {
            let mut buf = String::with_capacity(16);
            buf.push_str("time:");
            buf.push_str(&format_duration(StdDuration::from_nanos(
                (self.proc_times.sum() as i64).max(0) as u64,
            )));
            buf.push_str(", loops:");
            buf.push_str(&self.loops.to_string());
            if let Some(tiflash_stats) = &self.tiflash_stats {
                buf.push_str(", threads:");
                buf.push_str(&self.threads.to_string());
                if !tiflash_stats.wait_summary.can_be_ignored() {
                    buf.push_str(", ");
                    buf.push_str(&tiflash_stats.wait_summary.string());
                }
                if !tiflash_stats.network_summary.empty() {
                    buf.push_str(", ");
                    buf.push_str(&tiflash_stats.network_summary.string());
                }
                buf.push_str(", ");
                buf.push_str(&tiflash_stats.scan_context.string());
            }
            buf
        }
    }

    /// Go `CopRuntimeStats`' TiFlash slice.
    struct FlashCopRuntimeStats {
        stats: FlashBasicCopStats,
        scan_detail: CopScanDetail,
        time_detail: CopTimeDetail,
        store_type: StoreType,
    }

    impl FlashCopRuntimeStats {
        fn new(store_type: StoreType) -> FlashCopRuntimeStats {
            FlashCopRuntimeStats {
                stats: FlashBasicCopStats::default(),
                scan_detail: CopScanDetail::default(),
                time_detail: CopTimeDetail::default(),
                store_type,
            }
        }

        /// Go `CopRuntimeStats.String` (with `printTiFlashSpecificInfo`).
        #[expect(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        fn string(&self) -> String {
            let mut proc_times = self.stats.proc_times.clone();
            let total_tasks = proc_times.size();
            let is_tiflash_cop = self.store_type == StoreType::TiFlash;
            let mut buf = String::with_capacity(16);
            let print_tiflash_specific_info = |buf: &mut String| {
                if is_tiflash_cop {
                    buf.push_str(", ");
                    buf.push_str("threads:");
                    buf.push_str(&self.stats.threads.to_string());
                    buf.push('}');
                    if let Some(tiflash_stats) = &self.stats.tiflash_stats {
                        if !tiflash_stats.wait_summary.can_be_ignored() {
                            buf.push_str(", ");
                            buf.push_str(&tiflash_stats.wait_summary.string());
                        }
                        if !tiflash_stats.network_summary.empty() {
                            buf.push_str(", ");
                            buf.push_str(&tiflash_stats.network_summary.string());
                        }
                        if !tiflash_stats.columnar_scan_context.empty() {
                            buf.push_str(", ");
                            buf.push_str(&tiflash_stats.columnar_scan_context.string());
                        } else if !tiflash_stats.scan_context.empty() {
                            buf.push_str(", ");
                            buf.push_str(&tiflash_stats.scan_context.string());
                        }
                    }
                } else {
                    buf.push('}');
                }
            };
            if total_tasks == 1 {
                buf.push_str(self.store_type.name());
                buf.push_str("_task:{time:");
                buf.push_str(&format_duration(StdDuration::from_nanos(
                    (proc_times.get_percentile(0.0) as i64).max(0) as u64,
                )));
                buf.push_str(", loops:");
                buf.push_str(&self.stats.loops.to_string());
                print_tiflash_specific_info(&mut buf);
            } else if total_tasks > 0 {
                buf.push_str(self.store_type.name());
                buf.push_str("_task:{proc max:");
                buf.push_str(&format_duration(StdDuration::from_nanos(
                    (proc_times.get_max().0).max(0) as u64,
                )));
                buf.push_str(", min:");
                buf.push_str(&format_duration(StdDuration::from_nanos(
                    (proc_times.get_min().0).max(0) as u64,
                )));
                buf.push_str(", avg: ");
                buf.push_str(&format_duration(StdDuration::from_nanos(
                    ((proc_times.sum() as i64) / (total_tasks as i64)).max(0) as u64,
                )));
                buf.push_str(", p80:");
                buf.push_str(&format_duration(StdDuration::from_nanos(
                    (proc_times.get_percentile(0.8) as i64).max(0) as u64,
                )));
                buf.push_str(", p95:");
                buf.push_str(&format_duration(StdDuration::from_nanos(
                    (proc_times.get_percentile(0.95) as i64).max(0) as u64,
                )));
                buf.push_str(", iters:");
                buf.push_str(&self.stats.loops.to_string());
                buf.push_str(", tasks:");
                buf.push_str(&total_tasks.to_string());
                print_tiflash_specific_info(&mut buf);
            }
            if !is_tiflash_cop {
                let detail = self.scan_detail.string();
                if !detail.is_empty() {
                    buf.push_str(", ");
                    buf.push_str(&detail);
                }
                if self.time_detail != CopTimeDetail::default() {
                    let time_detail_str = self.time_detail.string();
                    if !time_detail_str.is_empty() {
                        buf.push_str(", ");
                        buf.push_str(&time_detail_str);
                    }
                }
            }
            buf
        }
    }

    /// Go `StmtCopRuntimeStats`.
    #[derive(Default)]
    struct StmtCopRuntimeStats {
        tiflash_network_stats: Option<TiFlashNetworkTrafficSummary>,
    }

    impl StmtCopRuntimeStats {
        /// Go `StmtCopRuntimeStats.mergeExecSummary`.
        fn merge_exec_summary(&mut self, summary: &FlashExecSummary) {
            if let Some(network) = &summary.tiflash_network_summary {
                self.tiflash_network_stats
                    .get_or_insert_with(TiFlashNetworkTrafficSummary::default)
                    .merge_exec_summary(Some(network));
            }
        }
    }

    /// Go `RuntimeStatsColl`'s TiFlash-recording slice (root stats narrowed
    /// to the existence set these tests check).
    #[derive(Default)]
    struct FlashRuntimeStatsColl {
        root_stats: HashSet<i64>,
        cop_stats: std::collections::HashMap<i64, FlashCopRuntimeStats>,
        stmt_cop_stats: StmtCopRuntimeStats,
    }

    impl FlashRuntimeStatsColl {
        /// Go `getPlanIDFromExecutionSummary`.
        fn plan_id_from_summary(summary: &FlashExecSummary) -> Option<i64> {
            if summary.executor_id.is_empty() {
                return None;
            }
            summary
                .executor_id
                .split('_')
                .next_back()
                .and_then(|last| last.parse::<i64>().ok())
        }

        /// Go `RuntimeStatsColl.RecordOneCopTask`.
        fn record_one_cop_task(
            &mut self,
            mut plan_id: i64,
            store_type: StoreType,
            summary: &FlashExecSummary,
        ) -> i64 {
            if let Some(id) = Self::plan_id_from_summary(summary) {
                plan_id = id;
            }
            let cop_stats = self
                .cop_stats
                .entry(plan_id)
                .or_insert_with(|| FlashCopRuntimeStats::new(store_type));
            cop_stats.stats.merge_exec_summary(summary);
            self.stmt_cop_stats.merge_exec_summary(summary);
            plan_id
        }

        /// Go `RuntimeStatsColl.RecordCopStats`.
        fn record_cop_stats(
            &mut self,
            mut plan_id: i64,
            store_type: StoreType,
            scan: Option<&CopScanDetail>,
            time: CopTimeDetail,
            summary: Option<&FlashExecSummary>,
        ) -> i64 {
            if let Some(cop_stats) = self.cop_stats.get_mut(&plan_id) {
                if let Some(scan) = scan {
                    cop_stats.scan_detail.merge(scan);
                }
                cop_stats.time_detail.merge(&time);
            } else {
                let mut cop_stats = FlashCopRuntimeStats::new(store_type);
                cop_stats.time_detail = time;
                if let Some(scan) = scan {
                    cop_stats.scan_detail = scan.clone();
                }
                self.cop_stats.insert(plan_id, cop_stats);
            }
            if let Some(summary) = summary {
                if let Some(id) = Self::plan_id_from_summary(summary) {
                    if id != plan_id {
                        plan_id = id;
                        self.cop_stats
                            .entry(plan_id)
                            .or_insert_with(|| FlashCopRuntimeStats::new(store_type));
                    }
                }
                if let Some(cop_stats) = self.cop_stats.get_mut(&plan_id) {
                    cop_stats.stats.merge_exec_summary(summary);
                }
                self.stmt_cop_stats.merge_exec_summary(summary);
            }
            plan_id
        }

        /// Go `RuntimeStatsColl.GetCopStats`.
        fn get_cop_stats(&self, plan_id: i64) -> Option<&FlashCopRuntimeStats> {
            self.cop_stats.get(&plan_id)
        }

        /// Go `RuntimeStatsColl.GetRootStats` (creates when missing; the Go
        /// `require.NotNil` on its result collapses to the existence set).
        fn get_root_stats(&mut self, plan_id: i64) {
            self.root_stats.insert(plan_id);
        }

        /// Go `RuntimeStatsColl.ExistsRootStats`.
        fn exists_root_stats(&self, plan_id: i64) -> bool {
            self.root_stats.contains(&plan_id)
        }

        /// Go `RuntimeStatsColl.ExistsCopStats`.
        fn exists_cop_stats(&self, plan_id: i64) -> bool {
            self.cop_stats.contains_key(&plan_id)
        }
    }

    /// Go `mockExecutorExecutionSummaryForTiFlash` (execdetails_test.go).
    /// NOTE: as in Go, `pipeline_breaker_wait_time` feeds
    /// `PipelineQueueWaitNs` and `pipeline_queue_time` feeds
    /// `PipelineBreakerWaitNs` — the swap is preserved verbatim.
    #[expect(clippy::too_many_arguments)]
    fn mock_executor_execution_summary_for_tiflash(
        time_processed_ns: u64,
        num_produced_rows: u64,
        num_iterations: u64,
        concurrency: u64,
        dmfile_scanned_rows: u64,
        dmfile_skipped_rows: u64,
        total_dmfile_rs_check_ms: u64,
        total_dmfile_read_time_ms: u64,
        total_build_snapshot_ms: u64,
        local_regions: u64,
        remote_regions: u64,
        total_learner_read_ms: u64,
        disagg_read_cache_hit_bytes: u64,
        disagg_read_cache_miss_bytes: u64,
        min_tso_wait_time: u64,
        pipeline_breaker_wait_time: u64,
        pipeline_queue_time: u64,
        inner_zone_send_bytes: u64,
        inter_zone_send_bytes: u64,
        inner_zone_receive_bytes: u64,
        inter_zone_receive_bytes: u64,
        executor_id: &str,
    ) -> FlashExecSummary {
        let tiflash_scan_context = TiFlashScanContextSnapshot {
            dmfile_data_scanned_rows: dmfile_scanned_rows,
            dmfile_data_skipped_rows: dmfile_skipped_rows,
            total_dmfile_rs_check_ms,
            total_dmfile_read_ms: total_dmfile_read_time_ms,
            total_build_snapshot_ms,
            local_regions,
            remote_regions,
            total_learner_read_ms,
            disagg_read_cache_hit_bytes,
            disagg_read_cache_miss_bytes,
            ..TiFlashScanContextSnapshot::default()
        };
        let tiflash_wait_summary = TiFlashWaitSummarySnapshot {
            min_tso_wait_ns: min_tso_wait_time,
            pipeline_queue_wait_ns: pipeline_breaker_wait_time,
            pipeline_breaker_wait_ns: pipeline_queue_time,
        };
        let tiflash_network_summary = TiFlashNetworkSummarySnapshot {
            inner_zone_send_bytes,
            inter_zone_send_bytes,
            inner_zone_receive_bytes,
            inter_zone_receive_bytes,
        };
        FlashExecSummary {
            time_processed_ns,
            num_produced_rows,
            num_iterations,
            concurrency,
            executor_id: executor_id.to_owned(),
            detail_info: Some(FlashDetailInfo::Scan(tiflash_scan_context)),
            tiflash_wait_summary: Some(tiflash_wait_summary),
            tiflash_network_summary: Some(tiflash_network_summary),
        }
    }

    /// Go `mockExecutorExecutionSummaryForTiFlashColumnar`
    /// (execdetails_test.go).
    #[expect(clippy::too_many_arguments)]
    fn mock_executor_execution_summary_for_tiflash_columnar(
        time_processed_ns: u64,
        num_produced_rows: u64,
        num_iterations: u64,
        concurrency: u64,
        regions: u64,
        read_tasks: u64,
        physical_tables: u64,
        columns: u64,
        user_read_bytes: u64,
        mvcc_input_rows: u64,
        mvcc_input_bytes: u64,
        mvcc_output_rows: u64,
        total_read_block_ms: u64,
        total_serialize_block_ms: u64,
        total_init_reader_ms: u64,
        total_prefetch_ms: u64,
        rough_check_total_packs: u64,
        rough_check_selected_packs: u64,
        rough_check_skipped_packs: u64,
        rough_check_unknown_packs: u64,
        remote_segments: u64,
        total_segments: u64,
        total_deserialize_block_ms: u64,
        executor_id: &str,
    ) -> FlashExecSummary {
        let columnar_scan_context = ColumnarScanContextSnapshot {
            regions,
            read_tasks,
            physical_tables,
            columns,
            user_read_bytes,
            mvcc_input_rows,
            mvcc_input_bytes,
            mvcc_output_rows,
            total_read_block_ms,
            total_serialize_block_ms,
            total_init_reader_ms,
            total_prefetch_ms,
            rough_check_total_packs,
            rough_check_selected_packs,
            rough_check_skipped_packs,
            rough_check_unknown_packs,
            remote_segments,
            total_segments,
            total_deserialize_block_ms,
        };
        FlashExecSummary {
            time_processed_ns,
            num_produced_rows,
            num_iterations,
            concurrency,
            executor_id: executor_id.to_owned(),
            detail_info: Some(FlashDetailInfo::Columnar(columnar_scan_context)),
            tiflash_wait_summary: None,
            tiflash_network_summary: None,
        }
    }

    /// Go `TestCopRuntimeStatsForTiFlash`, driven through the local harness
    /// mirror of `runtime_stats.go`'s TiFlash arms (see the boundary note
    /// above); the assertion literals are byte-exact copies of the Go
    /// test's. `require.NotNil(copStats)` collapses to the direct field
    /// access, and the extra scan/time-detail equality checks replace the
    /// unread Go fields to pin `RecordCopStats`' merge-into-empty behavior.
    #[test]
    fn test_cop_runtime_stats_for_tiflash() {
        let mut stats = FlashRuntimeStatsColl::default();
        let table_scan_id = 1i64;
        let agg_id = 2i64;
        let table_reader_id = 3i64;
        stats.record_one_cop_task(
            table_scan_id,
            StoreType::TiFlash,
            &mock_executor_execution_summary_for_tiflash(
                1,
                1,
                1,
                1,
                8192,
                0,
                15,
                200,
                40,
                10,
                4,
                1,
                100,
                50,
                30_000_000,
                20_000_000,
                10_000_000,
                1000,
                2000,
                3000,
                4000,
                &format!("tablescan_{table_scan_id}"),
            ),
        );
        stats.record_one_cop_task(
            table_scan_id,
            StoreType::TiFlash,
            &mock_executor_execution_summary_for_tiflash(
                2,
                2,
                2,
                1,
                0,
                0,
                0,
                2,
                0,
                0,
                0,
                0,
                0,
                0,
                20_000_000,
                10_000_000,
                5_000_000,
                10000,
                20000,
                30000,
                40000,
                &format!("tablescan_{table_scan_id}"),
            ),
        );
        stats.record_one_cop_task(
            agg_id,
            StoreType::TiFlash,
            &mock_executor_execution_summary_for_tiflash(
                3,
                3,
                3,
                1,
                12000,
                6000,
                60,
                1000,
                20,
                5,
                1,
                0,
                20,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                &format!("aggregation_{agg_id}"),
            ),
        );
        stats.record_one_cop_task(
            agg_id,
            StoreType::TiFlash,
            &mock_executor_execution_summary_for_tiflash(
                4,
                4,
                4,
                1,
                8192,
                80000,
                40,
                2000,
                30,
                1,
                1,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                &format!("aggregation_{agg_id}"),
            ),
        );
        let scan_detail = CopScanDetail {
            base: ScanDetail {
                total_keys: 10,
                processed_keys: 10,
                rocksdb_delete_skipped_count: 10,
                rocksdb_key_skipped_count: 1,
                rocksdb_block_cache_hit_count: 10,
                rocksdb_block_read_count: 10,
                rocksdb_block_read_byte: 100,
                ..ScanDetail::default()
            },
            ..CopScanDetail::default()
        };
        stats.record_cop_stats(
            table_scan_id,
            StoreType::TiFlash,
            Some(&scan_detail),
            CopTimeDetail::default(),
            None,
        );
        assert!(stats.exists_cop_stats(table_scan_id));

        let cop = stats.get_cop_stats(table_scan_id).unwrap();
        assert_eq!(
            "tiflash_task:{proc max:2ns, min:1ns, avg: 1ns, p80:2ns, p95:2ns, iters:3, tasks:2, threads:2}, tiflash_wait: {minTSO_wait: 20ms, pipeline_breaker_wait: 5ms, pipeline_queue_wait: 10ms}, tiflash_network: {inner_zone_send_bytes: 11000, inter_zone_send_bytes: 22000, inner_zone_receive_bytes: 33000, inter_zone_receive_bytes: 44000}, tiflash_scan:{mvcc_input_rows:0, mvcc_input_bytes:0, mvcc_output_rows:0, local_regions:10, remote_regions:4, tot_learner_read:1ms, region_balance:none, delta_rows:0, delta_bytes:0, segments:0, stale_read_regions:0, tot_build_snapshot:40ms, tot_build_bitmap:0ms, tot_build_inputstream:0ms, min_local_stream:0ms, max_local_stream:0ms, dtfile:{data_scanned_rows:8192, data_skipped_rows:0, mvcc_scanned_rows:0, mvcc_skipped_rows:0, lm_filter_scanned_rows:0, lm_filter_skipped_rows:0, tot_rs_index_check:15ms, tot_read:202ms, disagg_cache_hit_bytes: 100, disagg_cache_miss_bytes: 50}}",
            cop.string()
        );

        // Go: `copStats := cop.stats; require.NotNil(t, copStats)`.
        let cop_stats = &cop.stats;
        assert_eq!(
            "time:3ns, loops:3, threads:2, tiflash_wait: {minTSO_wait: 20ms, pipeline_breaker_wait: 5ms, pipeline_queue_wait: 10ms}, tiflash_network: {inner_zone_send_bytes: 11000, inter_zone_send_bytes: 22000, inner_zone_receive_bytes: 33000, inter_zone_receive_bytes: 44000}, tiflash_scan:{mvcc_input_rows:0, mvcc_input_bytes:0, mvcc_output_rows:0, local_regions:10, remote_regions:4, tot_learner_read:1ms, region_balance:none, delta_rows:0, delta_bytes:0, segments:0, stale_read_regions:0, tot_build_snapshot:40ms, tot_build_bitmap:0ms, tot_build_inputstream:0ms, min_local_stream:0ms, max_local_stream:0ms, dtfile:{data_scanned_rows:8192, data_skipped_rows:0, mvcc_scanned_rows:0, mvcc_skipped_rows:0, lm_filter_scanned_rows:0, lm_filter_skipped_rows:0, tot_rs_index_check:15ms, tot_read:202ms, disagg_cache_hit_bytes: 100, disagg_cache_miss_bytes: 50}}",
            cop_stats.string()
        );
        // Not in the Go test: pins the harness `RecordCopStats` merge (the
        // Go fields are recorded but unread there).
        assert_eq!(cop.scan_detail, scan_detail);
        assert_eq!(cop.time_detail, CopTimeDetail::default());

        let expected = "tiflash_task:{proc max:4ns, min:3ns, avg: 3ns, p80:4ns, p95:4ns, iters:7, tasks:2, threads:2}, tiflash_scan:{mvcc_input_rows:0, mvcc_input_bytes:0, mvcc_output_rows:0, local_regions:6, remote_regions:2, tot_learner_read:0ms, region_balance:none, delta_rows:0, delta_bytes:0, segments:0, stale_read_regions:0, tot_build_snapshot:50ms, tot_build_bitmap:0ms, tot_build_inputstream:0ms, min_local_stream:0ms, max_local_stream:0ms, dtfile:{data_scanned_rows:20192, data_skipped_rows:86000, mvcc_scanned_rows:0, mvcc_skipped_rows:0, lm_filter_scanned_rows:0, lm_filter_skipped_rows:0, tot_rs_index_check:100ms, tot_read:3000ms, disagg_cache_hit_bytes: 20, disagg_cache_miss_bytes: 0}}";
        assert_eq!(expected, stats.get_cop_stats(agg_id).unwrap().string());

        stats.get_root_stats(table_reader_id);
        assert!(stats.exists_root_stats(table_reader_id));

        let stmt_network_stats = stats.stmt_cop_stats.tiflash_network_stats.as_ref().unwrap();
        assert_eq!(stmt_network_stats.inner_zone_send_bytes, 11000u64);
        assert_eq!(stmt_network_stats.inter_zone_send_bytes, 22000u64);
        assert_eq!(stmt_network_stats.inner_zone_receive_bytes, 33000u64);
        assert_eq!(stmt_network_stats.inter_zone_receive_bytes, 44000u64);
    }

    /// Go `TestVectorSearchStats`, driven through the local harness mirror
    /// (see the boundary note above); the assertion literal is a byte-exact
    /// copy of the Go test's. Go's
    /// `execSummary.DetailInfo.(*tipb.ExecutorExecutionSummary_TiflashScanContext)`
    /// type assertion collapses to the harness enum match.
    #[test]
    fn test_vector_search_stats() {
        let mut stats = FlashRuntimeStatsColl::default();

        let v = 1u64;

        let mut exec_summary = mock_executor_execution_summary_for_tiflash(
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "",
        );
        match &mut exec_summary.detail_info {
            Some(FlashDetailInfo::Scan(scan)) => scan.vector_idx_load_from_s3 = v,
            _ => unreachable!("mock builds the scan variant"),
        }
        stats.record_one_cop_task(1, StoreType::TiFlash, &exec_summary);
        let s = stats.get_cop_stats(1).unwrap();
        assert_eq!(
            "tiflash_task:{time:0s, loops:0, threads:0}, vector_idx:{load:{total:0ms,from_s3:1,from_disk:0,from_cache:0},search:{total:0ms,visited_nodes:0,discarded_nodes:0},read:{vec_total:0ms,others_total:0ms}}, tiflash_scan:{mvcc_input_rows:0, mvcc_input_bytes:0, mvcc_output_rows:0, local_regions:0, remote_regions:0, tot_learner_read:0ms, region_balance:none, delta_rows:0, delta_bytes:0, segments:0, stale_read_regions:0, tot_build_snapshot:0ms, tot_build_bitmap:0ms, tot_build_inputstream:0ms, min_local_stream:0ms, max_local_stream:0ms, dtfile:{data_scanned_rows:0, data_skipped_rows:0, mvcc_scanned_rows:0, mvcc_skipped_rows:0, lm_filter_scanned_rows:0, lm_filter_skipped_rows:0, tot_rs_index_check:0ms, tot_read:0ms}}",
            s.string()
        );
    }

    /// Go `TestColumnarScanContextStats`, driven through the local harness
    /// mirror (see the boundary note above); the assertion literals are
    /// byte-exact copies of the Go test's.
    #[test]
    fn test_columnar_scan_context_stats() {
        let mut stats = FlashRuntimeStatsColl::default();
        let exec_summary = mock_executor_execution_summary_for_tiflash_columnar(
            1,
            10,
            2,
            1,
            2,
            4,
            3,
            5,
            2048,
            100,
            4096,
            80,
            7,
            8,
            9,
            10,
            11,
            12,
            13,
            14,
            15,
            16,
            17,
            "tablescan_1",
        );
        stats.record_one_cop_task(1, StoreType::TiFlash, &exec_summary);
        stats.record_one_cop_task(
            1,
            StoreType::TiFlash,
            &mock_executor_execution_summary_for_tiflash_columnar(
                2,
                20,
                3,
                2,
                4,
                6,
                2,
                4,
                1024,
                10,
                2048,
                8,
                1,
                2,
                3,
                4,
                5,
                6,
                7,
                8,
                9,
                10,
                11,
                "tablescan_1",
            ),
        );
        let s = stats.get_cop_stats(1).unwrap();
        assert_eq!(
            "tiflash_task:{proc max:2ns, min:1ns, avg: 1ns, p80:2ns, p95:2ns, iters:5, tasks:2, threads:3}, columnar_scan:{mvcc_input_rows:110, mvcc_input_bytes:6144, mvcc_output_rows:88, regions:6, read_tasks:10, physical_tables:3, columns:5, user_read_bytes:3072, read_block:8ms, serialize_block:10ms, init_reader:12ms, prefetch:14ms, deserialize_block:28ms, rough_check:{total:16, selected:18, skipped:20, unknown:22}, remote_segments:24, total_segments:26}",
            s.string()
        );

        let mut zero_stats = FlashRuntimeStatsColl::default();
        let zero_exec_summary = mock_executor_execution_summary_for_tiflash_columnar(
            1,
            0,
            1,
            1,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            "tablescan_1",
        );
        zero_stats.record_one_cop_task(1, StoreType::TiFlash, &zero_exec_summary);
        let zero_string = zero_stats.get_cop_stats(1).unwrap().string();
        assert!(zero_string.contains("columnar_scan:{"));
        assert!(!zero_string.contains("tiflash_scan:{"));
    }
}
