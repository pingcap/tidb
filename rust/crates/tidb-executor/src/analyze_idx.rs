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

//! Go `pkg/executor/analyze_idx.go`: `AnalyzeIndexExec`, which has the
//! store summarize an index's keys -- the independent index tasks
//! (multi-valued and special global indexes) and the NDV of a column task's
//! special indexes.

use prost::Message;
use tidb_proto::tipb;
use tidb_store_driver_txn::transaction::{RealOptimisticTransactionOpener, StoreClient};

use crate::cluster_analyze::sampling::{native_transport, read_policy};
use crate::cluster_analyze::AnalyzeError;

/// Go `AnalyzeIndexExec` with its `baseAnalyzeExec`.
pub(crate) struct AnalyzeIndexExec<'a, S: StoreClient> {
    pub(crate) opener: &'a RealOptimisticTransactionOpener<S>,
    /// Go `tableID.GetStatisticsID()`.
    pub(crate) physical_id: i64,
    /// Go `idxInfo.ID`.
    pub(crate) index_id: i64,
    /// Go `len(idxInfo.Columns)`.
    pub(crate) num_columns: usize,
    /// Go `isCommonHandle && idxInfo.Primary`: the clustered primary key,
    /// whose keys are the records'.
    pub(crate) is_common_handle_primary: bool,
    /// Go `analyzePB`.
    pub(crate) analyze_pb: tipb::AnalyzeReq,
    /// Go `opts[ast.AnalyzeOptNumBuckets]`.
    pub(crate) num_buckets: usize,
    /// Go `opts[ast.AnalyzeOptNumTopN]`.
    pub(crate) num_topn: u32,
    /// Go `concurrency`.
    pub(crate) concurrency: u64,
    /// Go `snapshot`.
    pub(crate) snapshot: u64,
    /// Go `SessionVars.EnableAnalyzeSnapshot`.
    pub(crate) analyze_snapshot: bool,
    pub(crate) timeout: std::time::Duration,
    pub(crate) cancellation: std::sync::Arc<tidb_txnkv::CancelHandle>,
    /// Go `StmtCtx.ResourceGroupName`.
    pub(crate) resource_group: &'a str,
}

/// What `buildStatsFromResult` merges from the responses.
struct IndexResult {
    built: tidb_stats::AnalyzeIndexStatistics,
}

impl<S: StoreClient> AnalyzeIndexExec<'_, S> {
    /// Go `analyzeIndexPushdown`: the index's histogram, TopN and FM sketch,
    /// with Go's `Count` (the NULLs, the histogram's rows and the TopN's).
    pub(crate) fn analyze_index_pushdown(
        &self,
        progress: &dyn Fn(i64),
    ) -> Result<(tidb_stats::AnalyzeIndexStatistics, i64), AnalyzeError> {
        let mut built = self.build_stats(true, progress)?;
        built.histogram.id = self.index_id;
        let count = built
            .histogram
            .null_count
            .wrapping_add(built.histogram.buckets.last().map_or(0, |bucket| bucket.count))
            .wrapping_add(built.topn.as_ref().map_or(0, |topn| topn.total_count() as i64));
        Ok((built, count))
    }

    /// Go `buildStats`: a single-column index reads its non-NULL keys, then
    /// its NULL keys for the NULL count; any other index reads every key.
    fn build_stats(
        &self,
        consider_null: bool,
        progress: &dyn Fn(i64),
    ) -> Result<tidb_stats::AnalyzeIndexStatistics, AnalyzeError> {
        let mut hist = self.build_stats_from_result(false, true, progress)?.built;
        if consider_null && self.num_columns == 1 {
            let null_hist = self.build_stats_from_result(true, false, progress)?.built;
            if let Some(bucket) = null_hist.histogram.buckets.last() {
                hist.histogram.null_count = bucket.count;
            }
        }
        Ok(hist)
    }

    /// Go `buildSimpleStats`: the FM sketch of the keys and the NULL count
    /// of a single-column index.
    fn build_simple_stats(&self, consider_null: bool) -> Result<(tidb_stats::FmSketch, i64), AnalyzeError> {
        let mut sketch = tidb_stats::FmSketch::new(tidb_stats::MAX_SKETCH_SIZE);
        self.read_responses(false, |response| {
            let fm = response
                .collector
                .and_then(|collector| collector.fm_sketch)
                .ok_or_else(|| AnalyzeError::unsupported("TiKV index ANALYZE response has no FM sketch".to_owned()))?;
            sketch.merge(&tidb_stats::FmSketch::from_raw_parts(
                fm.mask.unwrap_or_default(),
                tidb_stats::MAX_SKETCH_SIZE,
                fm.hashset,
            ));
            Ok(())
        })?;
        let mut null_count = 0;
        if consider_null && self.num_columns == 1 {
            self.read_responses(true, |response| {
                null_count += response
                    .hist
                    .as_ref()
                    .and_then(|hist| hist.buckets.last())
                    .map_or(0, |bucket| bucket.count());
                Ok(())
            })?;
        }
        Ok((sketch, null_count))
    }

    /// Go `analyzeIndexNDVPushDown`: the NULL count and FM sketch of a
    /// special index's keys.
    pub(crate) fn analyze_index_ndv_push_down(&self) -> Result<(i64, tidb_stats::FmSketch), AnalyzeError> {
        let (sketch, null_count) = self.build_simple_stats(self.num_columns == 1)?;
        Ok((null_count, sketch))
    }

    /// Go `buildStatsFromResult`: merges each response (`updateIndexResult`).
    fn build_stats_from_result(
        &self,
        is_null_range: bool,
        need_cms: bool,
        progress: &dyn Fn(i64),
    ) -> Result<IndexResult, AnalyzeError> {
        if tidb_failpoint::eval("github.com/pingcap/tidb/pkg/executor/buildStatsFromResult").is_some_and(|value| value.bool())
        {
            return Err(AnalyzeError::unsupported("mock buildStatsFromResult error".to_owned()));
        }
        let mut result = tidb_stats::AnalyzeIndexResult::new(
            need_cms,
            tidb_stats::DEFAULT_INDEX_CMS_DEPTH,
            tidb_stats::DEFAULT_INDEX_CMS_WIDTH,
            self.num_buckets,
            self.num_topn,
        );
        self.read_responses(is_null_range, |response| update_index_result(response, &mut result, progress))?;
        let built = result
            .finish()
            .map_err(|error| AnalyzeError::unsupported(error.to_string()))?;
        Ok(IndexResult { built })
    }

    /// Go `fetchAnalyzeResult` and the response loop over the result:
    /// `FullRange`, `FullNotNullRange` (single column) or `NullRange` keys of
    /// the index, or of the records for the clustered primary key.
    fn read_responses(
        &self,
        is_null_range: bool,
        mut handle: impl FnMut(tipb::AnalyzeIndexResp) -> Result<(), AnalyzeError>,
    ) -> Result<(), AnalyzeError> {
        let error = |e: String| AnalyzeError::unsupported(e);
        let prefix = if self.is_common_handle_primary {
            tidb_tablecodec::table_key::gen_table_record_prefix(self.physical_id)
        } else {
            tidb_tablecodec::table_key::encode_index_seek_key(self.physical_id, self.index_id, &[])
        };
        let Some((_, start, end)) = index_key_ranges(&prefix, self.num_columns == 1)
            .into_iter()
            .find(|(null_range, ..)| *null_range == is_null_range)
        else {
            return Ok(());
        };
        let transport = native_transport(self.opener, self.timeout).map_err(error)?;
        let mut runtime = tidb_distsql::InjectedQueryRuntime::new(transport);
        let (read_ts, isolation) = read_policy(self.analyze_snapshot, self.snapshot);
        let mut builder = tidb_distsql::RequestBuilder::new();
        builder
            .set_analyze_request(self.analyze_pb.encode_to_vec(), isolation)
            .set_paging(false)
            .set_start_ts(read_ts)
            .set_keep_order(true)
            .set_non_partitioned_key_ranges(vec![tidb_txnkv::KeyRange {
                start_key: start.into(),
                end_key: end.into(),
            }])
            .set_concurrency(self.concurrency)
            .set_resource_group_name(self.resource_group);
        let transport_request = builder
            .build_transport_request(self.cancellation.clone())
            .map_err(|e| error(format!("{e:?}")))?;
        let mut result = runtime.analyze(&transport_request, true).map_err(|e| error(e.to_string()))?;
        let outcome = (|| {
            while let Some(bytes) = result.next_raw().map_err(|e| error(e.to_string()))? {
                let response = tipb::AnalyzeIndexResp::decode(bytes.as_slice()).map_err(|e| error(e.to_string()))?;
                handle(response)?;
            }
            Ok(())
        })();
        let _ = result.close();
        outcome
    }
}

/// Go `updateIndexResult`: merges one region's histogram, CM sketch with
/// its TopN, and FM sketch, and reports the region's rows as progress.
fn update_index_result(
    response: tipb::AnalyzeIndexResp,
    result: &mut tidb_stats::AnalyzeIndexResult,
    progress: &dyn Fn(i64),
) -> Result<(), AnalyzeError> {
    let error = |e: String| AnalyzeError::unsupported(e);
    let histogram = histogram_from_proto(response.hist.as_ref());
    progress(histogram.buckets.last().map_or(0, |bucket| bucket.count));
    let cms_and_topn = response
        .cms
        .map(|cms| tidb_stats::decode_cmsketch_and_embedded_topn(&cms.encode_to_vec()))
        .transpose()
        .map_err(|e| error(format!("{e:?}")))?;
    let fm_sketch = response.collector.and_then(|collector| collector.fm_sketch).map(|fm| {
        tidb_stats::FmSketch::from_raw_parts(fm.mask.unwrap_or_default(), tidb_stats::MAX_SKETCH_SIZE, fm.hashset)
    });
    result
        .update(histogram, cms_and_topn, fm_sketch)
        .map_err(|e| error(e.to_string()))
}

/// Go `buildAnalyzeIndexPushdown`'s request for the statistics of an index.
pub(crate) fn analyze_index_req(
    num_columns: usize,
    num_buckets: i64,
    num_topn: u32,
    flags: u64,
    time_zone_offset: i64,
) -> tipb::AnalyzeReq {
    tipb::AnalyzeReq {
        tp: Some(tipb::AnalyzeType::TypeIndex as i32),
        flags: Some(flags),
        time_zone_offset: Some(time_zone_offset),
        idx_req: Some(tipb::AnalyzeIndexReq {
            bucket_size: Some(num_buckets),
            num_columns: Some(num_columns as i32),
            top_n_size: Some(num_topn as i32),
            version: Some(2),
            sketch_size: Some(tidb_stats::MAX_SKETCH_SIZE as i64),
            cmsketch_depth: Some(tidb_stats::DEFAULT_INDEX_CMS_DEPTH as i32),
            cmsketch_width: Some(tidb_stats::DEFAULT_INDEX_CMS_WIDTH as i32),
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// Go `buildSubIndexJobForSpecialIndex`'s request: only the NDV of the keys.
pub(crate) fn analyze_index_ndv_req(
    num_columns: usize,
    common_handle_primary: bool,
    flags: u64,
    time_zone_offset: i64,
) -> tipb::AnalyzeReq {
    tipb::AnalyzeReq {
        tp: Some(if common_handle_primary {
            tipb::AnalyzeType::TypeCommonHandle
        } else {
            tipb::AnalyzeType::TypeIndex
        } as i32),
        flags: Some(flags),
        time_zone_offset: Some(time_zone_offset),
        idx_req: Some(tipb::AnalyzeIndexReq {
            bucket_size: Some(1),
            num_columns: Some(num_columns as i32),
            cmsketch_depth: Some(0),
            cmsketch_width: Some(0),
            top_n_size: Some(0),
            version: Some(2),
            sketch_size: Some(tidb_stats::MAX_SKETCH_SIZE as i64),
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// Go `EncodeIndexKey` over the ranges `AnalyzeIndexExec.open` reads: a
/// single-column index reads `FullNotNullRange` -- `[MinNotNull, MaxValue]`,
/// the codec's `bytesFlag` (1) through `maxFlag` (250) -- and then
/// `NullRange` (`[NULL, NULL]`, `NilFlag` 0) for its NULL count; any other
/// index reads `FullRange`, `[NULL, MaxValue]`. The high bound is
/// `PrefixNext` of the inclusive high key. Each entry is `(is_null_range,
/// start, end)` in Go's request order.
pub(crate) fn index_key_ranges(prefix: &[u8], single_column: bool) -> Vec<(bool, Vec<u8>, Vec<u8>)> {
    const NIL_FLAG: u8 = 0;
    const BYTES_FLAG: u8 = 1;
    const MAX_FLAG: u8 = 250;
    let key = |flag: u8| [prefix, &[flag]].concat();
    if single_column {
        vec![
            (false, key(BYTES_FLAG), key(MAX_FLAG + 1)),
            (true, key(NIL_FLAG), key(NIL_FLAG + 1)),
        ]
    } else {
        vec![(false, key(NIL_FLAG), key(MAX_FLAG + 1))]
    }
}

/// Go `statistics.HistogramFromProto`: BLOB bounds, each bucket's NDV when
/// the store sent one.
fn histogram_from_proto(proto: Option<&tipb::Histogram>) -> tidb_stats::Histogram {
    let empty = tipb::Histogram::default();
    let proto = proto.unwrap_or(&empty);
    let mut histogram = tidb_stats::Histogram::new(
        0,
        proto.ndv(),
        0,
        0,
        &tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::Blob),
        proto.buckets.len(),
        0,
    );
    for bucket in &proto.buckets {
        let lower = tidb_datatype::Datum::Bytes(bucket.lower_bound().to_vec());
        let upper = tidb_datatype::Datum::Bytes(bucket.upper_bound().to_vec());
        match bucket.ndv {
            Some(ndv) => histogram.append_bucket_with_ndv(lower, upper, bucket.count(), bucket.repeats(), ndv),
            None => histogram.append_bucket(lower, upper, bucket.count(), bucket.repeats()),
        }
    }
    histogram
}

#[cfg(test)]
mod tests {
    /// Go `EncodeIndexKey` over `FullNotNullRange`/`NullRange` (single
    /// column) and `FullRange`: the high bound is `PrefixNext` of the
    /// `maxFlag` byte, `prefix\xfb`, never `PrefixNext(prefix)`.
    #[test]
    fn index_ndv_key_ranges_match_go_encode_index_key() {
        let prefix = b"t\x80\x00\x00\x00\x00\x00\x00\x01_i\x80\x00\x00\x00\x00\x00\x00\x02";
        let key = |flag: u8| [&prefix[..], &[flag]].concat();
        assert_eq!(
            super::index_key_ranges(prefix, true),
            vec![(false, key(0x01), key(0xfb)), (true, key(0x00), key(0x01))]
        );
        assert_eq!(super::index_key_ranges(prefix, false), vec![(false, key(0x00), key(0xfb))]);
    }
}
