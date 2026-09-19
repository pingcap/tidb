// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Schema projection for Go master buildAnalyzeSamplingPushdown.
use super::{AnalyzeError, AnalyzeOptions, AnalyzePlan};
use tidb_executor::remote_scan::PushdownScanColumn;
use tidb_model::table_info::TableInfo;
use tidb_proto::tipb;

pub(super) struct SamplingSchema {
    pub columns: Vec<PushdownScanColumn>,
    pub handle_positions: Vec<usize>,
    pub common_handle: bool,
    primary_ids: Vec<i64>,
    primary_prefix_ids: Vec<i64>,
    primary_lengths: Vec<i64>,
}

impl SamplingSchema {
    pub fn new(table: &TableInfo, plan: &AnalyzePlan) -> Result<Self, AnalyzeError> {
        let integer_pk = table
            .pk_is_handle
            .then(|| table.get_pk_col_info())
            .flatten()
            .map(|column| column.read().id);
        let mut columns: Vec<_> = plan
            .columns()
            .iter()
            .map(|column| PushdownScanColumn {
                id: column.id,
                field_type: column.field_type.clone(),
                is_handle: integer_pk == Some(column.id),
                origin_default: Some(column.absent_value.clone()),
            })
            .collect();
        let mut primary_ids = Vec::new();
        let mut primary_prefix_ids = Vec::new();
        let mut primary_lengths = Vec::new();
        if table.is_common_handle {
            let primary = table.get_primary_key().ok_or_else(|| {
                AnalyzeError::unsupported("common handle has no primary index".to_owned())
            })?;
            for part in primary.read().columns.iter_deref() {
                let part = part.read();
                let source = table.cols().get(part.offset as usize).ok_or_else(|| {
                    AnalyzeError::unsupported("primary index offset is invalid".to_owned())
                })?;
                let source = source.read();
                primary_ids.push(source.id);
                primary_lengths.push(part.length);
                if part.length != tidb_datatype::UNSPECIFIED_LENGTH {
                    primary_prefix_ids.push(source.id);
                }
            }
        } else if let Some(id) = integer_pk {
            primary_ids.push(id);
        } else {
            columns.push(PushdownScanColumn {
                id: -1,
                field_type: tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
                is_handle: true,
                origin_default: None,
            });
            primary_ids.push(-1);
        }
        let mut handle_positions = Vec::new();
        for id in &primary_ids {
            if !columns.iter().any(|column| column.id == *id) {
                let column = table
                    .cols()
                    .iter_deref()
                    .find(|column| column.read().id == *id)
                    .ok_or_else(|| {
                        AnalyzeError::unsupported("handle column is missing".to_owned())
                    })?;
                let column = column.read();
                columns.push(PushdownScanColumn {
                    id: *id,
                    field_type: column.field_type.clone(),
                    is_handle: integer_pk == Some(*id),
                    origin_default: Some(
                        super::origin_default(&column, table.name.original())
                            .map_err(|e| AnalyzeError::unsupported(e.to_string()))?,
                    ),
                });
            }
            handle_positions.push(columns.iter().position(|column| column.id == *id).unwrap());
        }
        Ok(Self {
            columns,
            handle_positions,
            common_handle: table.is_common_handle,
            primary_ids,
            primary_prefix_ids,
            primary_lengths,
        })
    }

    pub fn statistics_slots(&self, plan: &AnalyzePlan) -> Vec<usize> {
        (0..plan.columns().len())
            .chain(
                plan.indexes()
                    .iter()
                    .enumerate()
                    .filter(|(_, index)| index.needs_own_slot())
                    .map(|(i, _)| self.columns.len() + i),
            )
            .collect()
    }

    pub fn decode_handle(
        &self,
        row: &mut Vec<tidb_datatype::Datum>,
        timezone: &tidb_datatype::SessionTimeZone,
    ) -> Result<tidb_txnkv::Handle, tidb_executor::analyze::AnalyzeError> {
        use tidb_datatype::Datum;
        use tidb_executor::analyze::AnalyzeError as Error;
        if row.len() != self.columns.len() {
            return Err(Error::Unsupported(
                "ANALYZE sample column count mismatch".to_owned(),
            ));
        }
        for (datum, column) in row.iter_mut().zip(&self.columns) {
            let Datum::Bytes(bytes) = datum else {
                return Err(Error::Unsupported(
                    "ANALYZE sample is not encoded".to_owned(),
                ));
            };
            *datum =
                tidb_tablecodec::decode_column_value(bytes, &column.field_type, Some(timezone))
                    .map_err(|e| Error::Encode(e.to_string()))?;
        }
        if !self.common_handle {
            return match row[self.handle_positions[0]] {
                Datum::Int(value) => Ok(tidb_txnkv::IntHandle::new(value).into()),
                Datum::UInt(value) => Ok(tidb_txnkv::IntHandle::new(value as i64).into()),
                _ => Err(Error::Unsupported(
                    "ANALYZE integer handle is not an integer".to_owned(),
                )),
            };
        }
        let mut values = Vec::with_capacity(self.handle_positions.len());
        for (key_position, position) in self.handle_positions.iter().copied().enumerate() {
            let column = &self.columns[position];
            let mut value = row[position].clone();
            tidb_tablecodec::truncate_index_value(
                &mut value,
                &tidb_tablecodec::IndexColumn {
                    offset: position,
                    length: self.primary_lengths[key_position],
                    use_changing_type: false,
                },
                &tidb_tablecodec::TableColumn {
                    id: column.id,
                    offset: position,
                    field_type: column.field_type.clone(),
                    primary_key: true,
                    changing_field_type: None,
                },
            )
            .map_err(|e| Error::Encode(e.to_string()))?;
            if let Some(collation) = tidb_executor::analyze::AnalyzedColumn::sampling_collation(
                &column.field_type,
                "primary key",
            )? {
                value = match value {
                    Datum::Bytes(bytes) => Datum::Bytes(collation.key(&bytes)),
                    Datum::String(string) => Datum::Bytes(collation.key(string.bytes())),
                    other => other,
                };
            }
            values.push(value);
        }
        let bytes = tidb_codec::encode_key(&values).map_err(|e| Error::Encode(e.to_string()))?;
        tidb_txnkv::CommonHandle::new(bytes)
            .map(Into::into)
            .map_err(|e| Error::Encode(e.to_string()))
    }

    pub fn project_collector(
        &self,
        plan: &AnalyzePlan,
        collector: tipb::RowSampleCollector,
    ) -> Result<tidb_stats::row_sample_collector::RowSampleCollectorProto, AnalyzeError> {
        let expected = self.columns.len() + plan.indexes().len();
        if collector.null_counts.len() != expected
            || collector.total_size.len() != expected
            || collector.fm_sketch.len() != expected
            || collector
                .samples
                .iter()
                .any(|sample| sample.row.len() != self.columns.len())
            || collector.count.unwrap_or_default() < 0
        {
            return Err(AnalyzeError::unsupported(
                "TiKV ANALYZE collector does not match requested schema".to_owned(),
            ));
        }
        let slots = self.statistics_slots(plan);
        Ok(tidb_stats::row_sample_collector::RowSampleCollectorProto {
            count: collector.count.unwrap_or_default(),
            null_counts: slots.iter().map(|i| collector.null_counts[*i]).collect(),
            total_sizes: slots.iter().map(|i| collector.total_size[*i]).collect(),
            fm_sketches: slots
                .iter()
                .map(|i| {
                    Some(tidb_stats::FmSketchProto {
                        mask: collector.fm_sketch[*i].mask.unwrap_or_default(),
                        hashset: collector.fm_sketch[*i].hashset.clone(),
                    })
                })
                .collect(),
            samples: collector
                .samples
                .into_iter()
                .map(|sample| tidb_stats::row_sample_collector::RowSampleProto {
                    row: sample.row,
                    weight: sample.weight.unwrap_or_default(),
                })
                .collect(),
        })
    }

    pub fn request(
        &self,
        plan: &AnalyzePlan,
        options: &AnalyzeOptions,
        rate: f64,
        flags: u64,
        offset: i64,
    ) -> Result<tipb::AnalyzeReq, AnalyzeError> {
        let columns_info = self
            .columns
            .iter()
            .map(|column| {
                crate::cop_scan::scan_column(column)
                    .map(|column| crate::dag_request::column_to_pb(&column))
                    .ok_or_else(|| {
                        AnalyzeError::unsupported(format!(
                            "ANALYZE cannot encode column {}",
                            column.id
                        ))
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(tipb::AnalyzeReq {
            tp: Some(tipb::AnalyzeType::TypeFullSampling as i32),
            flags: Some(flags),
            time_zone_offset: Some(offset),
            col_req: Some(tipb::AnalyzeColumnsReq {
                bucket_size: Some(options.num_buckets as i64),
                sample_size: Some(options.num_samples as i64),
                sketch_size: Some(tidb_stats::MAX_SKETCH_SIZE as i64),
                sample_rate: Some(if options.num_samples > 0 { 0.0 } else { rate }),
                columns_info,
                primary_column_ids: if self.common_handle {
                    self.primary_ids.clone()
                } else {
                    Vec::new()
                },
                primary_prefix_column_ids: self.primary_prefix_ids.clone(),
                column_groups: plan
                    .indexes()
                    .iter()
                    .map(|index| tipb::AnalyzeColumnGroup {
                        column_offsets: index
                            .column_positions
                            .iter()
                            .map(|position| *position as i64)
                            .collect(),
                        prefix_lengths: Vec::new(),
                    })
                    .collect(),
                ..Default::default()
            }),
            ..Default::default()
        })
    }
}

fn concurrency_for_stores(stores: u64) -> u64 {
    match stores {
        0..=5 => tidb_vardef::defaults::DEF_ANALYZE_DIST_SQL_SCAN_CONCURRENCY as u64,
        6..=10 => stores,
        11..=20 => stores.saturating_mul(2),
        21..=50 => stores.saturating_mul(3),
        _ => stores.saturating_mul(4),
    }
}

pub(crate) fn scan_concurrency<P: tidb_txnkv::transaction::StorePdCapability>(
    configured: i64,
    pd: &P,
    timeout: std::time::Duration,
) -> u64 {
    if configured > 0 {
        return configured as u64;
    }
    let count = (|| -> Result<u64, String> {
        let endpoint = pd
            .http_endpoint()
            .ok_or_else(|| "no PD HTTP endpoint".to_owned())?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| e.to_string())?;
        runtime.block_on(async {
            let response = reqwest::Client::builder()
                .timeout(timeout)
                .build()
                .map_err(|e| e.to_string())?
                .get(format!(
                    "{}/pd/api/v1/stores",
                    endpoint.trim_end_matches('/')
                ))
                .send()
                .await
                .map_err(|e| e.to_string())?
                .error_for_status()
                .map_err(|e| e.to_string())?;
            let json: serde_json::Value = response.json().await.map_err(|e| e.to_string())?;
            json.get("count")
                .and_then(serde_json::Value::as_u64)
                .ok_or_else(|| "PD stores response omitted count".to_owned())
        })
    })();
    match count {
        Ok(count) => concurrency_for_stores(count),
        Err(error) => {
            eprintln!("cannot determine adaptive ANALYZE concurrency: {error}");
            concurrency_for_stores(0)
        }
    }
}

struct SamplingCancellation {
    done: crossbeam_channel::Sender<()>,
    worker: Option<std::thread::JoinHandle<()>>,
}

impl SamplingCancellation {
    fn start(
        killer: &tidb_util::sqlkiller::SqlKiller,
        cancellation: std::sync::Arc<tidb_distsql::CancelHandle>,
    ) -> Result<Self, AnalyzeError> {
        let killed = killer.get_kill_event_chan();
        let (done, finished) = crossbeam_channel::bounded(1);
        let worker = std::thread::Builder::new()
            .name("analyze-cancel".to_owned())
            .spawn(move || {
                crossbeam_channel::select! {
                    recv(killed) -> _ => cancellation.cancel(),
                    recv(finished) -> _ => {},
                }
            })
            .map_err(|e| AnalyzeError::unsupported(e.to_string()))?;
        Ok(Self {
            done,
            worker: Some(worker),
        })
    }
}

impl Drop for SamplingCancellation {
    fn drop(&mut self) {
        let _ = self.done.try_send(());
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

pub(crate) fn analyze_table<C, L, P>(
    opener: &tidb_txnkv::transaction::RealOptimisticTransactionOpener<C, L, P>,
    table: &TableInfo,
    physical_id: i64,
    statement: &super::AnalyzeStatement,
    realtime_count: Option<i64>,
    snapshot: u64,
    selected_columns: Option<&std::collections::HashSet<i64>>,
    concurrency: u64,
    flags: u64,
    timeout: std::time::Duration,
    killer: &tidb_util::sqlkiller::SqlKiller,
    resource_group: &str,
    progress: &dyn Fn(i64),
) -> Result<super::AnalyzeReport, AnalyzeError>
where
    C: tidb_txnkv::transaction::StoreWriteClient + tidb_txnkv::DirectUnaryClient,
    L: tidb_txnkv::transaction::StoreWriteLoader,
    P: tidb_txnkv::transaction::StorePdCapability,
{
    let cancellation = std::sync::Arc::new(tidb_distsql::CancelHandle::default());
    let _cancel_guard = SamplingCancellation::start(killer, cancellation.clone())?;
    let plan = super::cluster_analyze_plan(table, selected_columns)?;
    let schema = SamplingSchema::new(table, &plan)?;
    let options = &statement.options;
    let rate = options.sample_rate.unwrap_or_else(|| {
        tidb_stats::row_sample_collector::adjusted_sample_rate(realtime_count, None)
    });
    let request = schema.request(
        &plan,
        options,
        rate,
        flags,
        statement.time_zone.dag_zone().1,
    )?;
    let mut collector = collect_regions(
        opener,
        &schema,
        &plan,
        request,
        physical_id,
        snapshot,
        statement.analyze_snapshot,
        options,
        concurrency,
        timeout,
        cancellation.clone(),
        resource_group,
        progress,
    )?;
    for (position, index) in plan.indexes().iter().enumerate() {
        if !index
            .prefix_lengths
            .iter()
            .any(|length| *length != tidb_datatype::UNSPECIFIED_LENGTH)
        {
            continue;
        }
        let primary = table.is_common_handle
            && table
                .get_primary_key()
                .is_some_and(|primary| primary.read().id == index.id);
        let (null_count, sketch) = collect_index_ndv(
            opener,
            physical_id,
            index,
            primary,
            statement,
            snapshot,
            concurrency,
            timeout,
            cancellation.clone(),
            resource_group,
        )?;
        collector.replace_special_index_stats(plan.index_slot(position), null_count, sketch);
    }
    let run = tidb_executor::analyze::AnalyzeRun::start(&plan, options, realtime_count)?;
    let analyzed = run.finish_collector(collector, |row| {
        schema.decode_handle(row, &statement.time_zone)
    })?;
    Ok(super::report_from_analyzed(analyzed, physical_id, snapshot))
}

fn read_policy(snapshot_enabled: bool, snapshot: u64) -> (u64, tidb_distsql::IsolationLevel) {
    if snapshot_enabled {
        (snapshot, tidb_distsql::IsolationLevel::Snapshot)
    } else {
        (u64::MAX, tidb_distsql::IsolationLevel::ReadCommitted)
    }
}

/// Uses the process-owned region cache and lock recovery; only sampled
/// collector responses cross the TiKV boundary.
pub(super) fn collect_regions<C, L, P>(
    opener: &tidb_txnkv::transaction::RealOptimisticTransactionOpener<C, L, P>,
    schema: &SamplingSchema,
    plan: &AnalyzePlan,
    request: tipb::AnalyzeReq,
    physical_id: i64,
    snapshot: u64,
    snapshot_enabled: bool,
    options: &AnalyzeOptions,
    concurrency: u64,
    timeout: std::time::Duration,
    cancellation: std::sync::Arc<tidb_distsql::CancelHandle>,
    resource_group: &str,
    progress: &dyn Fn(i64),
) -> Result<tidb_stats::row_sample_collector::RowSampleCollector, AnalyzeError>
where
    C: tidb_txnkv::transaction::StoreWriteClient + tidb_txnkv::DirectUnaryClient,
    L: tidb_txnkv::transaction::StoreWriteLoader,
    P: tidb_txnkv::transaction::StorePdCapability,
{
    use prost::Message;
    use tidb_stats::row_sample_collector::{RowSampleCollector, SamplePolicy};
    let error = |e: String| AnalyzeError::unsupported(e);
    let rate = request
        .col_req
        .as_ref()
        .and_then(|r| r.sample_rate)
        .unwrap_or_default();
    let policy = SamplePolicy::choose(options.num_samples, rate)
        .ok_or_else(|| error("invalid ANALYZE sample policy".to_owned()))?;
    let runtime = opener
        .open_read_runtime()
        .map_err(|e| error(e.to_string()))?;
    let transport = tidb_distsql::DirectUnaryQueryTransport::with_shared_runtime(
        runtime,
        tidb_distsql::DirectUnaryRuntimeConfig {
            default_timeout: timeout,
            ..Default::default()
        },
        tidb_txnkv::pd_capability::CapabilityTimestampSource(opener.pd().clone()),
    )
    .map_err(|e| error(e.to_string()))?
    .with_concurrent_workers()
    .map_err(|e| error(e.to_string()))?;
    let start = tidb_codec::table_key::gen_table_record_prefix(physical_id);
    let end = super::finite_successor(&start)?;
    let (read_ts, isolation) = read_policy(snapshot_enabled, snapshot);
    let mut builder = tidb_distsql::RequestBuilder::new();
    builder
        .set_analyze_request(request.encode_to_vec(), isolation)
        .set_paging(false)
        .set_start_ts(read_ts)
        .set_non_partitioned_key_ranges(vec![tidb_distsql::RequestKeyRange {
            start_key: start.into(),
            end_key: end.into(),
        }])
        .set_concurrency(concurrency)
        .set_resource_group_name(resource_group);
    let request = builder
        .build_transport_request(cancellation)
        .map_err(|e| error(format!("{e:?}")))?;
    let mut runtime = tidb_distsql::InjectedQueryRuntime::new(transport);
    let mut result = runtime
        .analyze(&request, true)
        .map_err(|e| error(e.to_string()))?;
    let mut merged =
        RowSampleCollector::with_memory_quota(plan.slot_count(), policy, options.memory_quota);
    let outcome = (|| {
        while let Some(bytes) = result.next_raw().map_err(|e| error(e.to_string()))? {
            let response = tipb::AnalyzeColumnsResp::decode(bytes.as_slice())
                .map_err(|e| error(e.to_string()))?;
            let raw = response
                .row_collector
                .ok_or_else(|| error("TiKV ANALYZE response has no row collector".to_owned()))?;
            let projected = schema.project_collector(plan, raw)?;
            progress(projected.count);
            let fragment = RowSampleCollector::from_proto(&projected, policy, options.memory_quota)
                .map_err(|e| error(e.to_string()))?;
            merged.merge(fragment);
            if options
                .memory_quota
                .bytes()
                .is_some_and(|limit| merged.consumed_memory_bytes() > limit)
            {
                return Err(error(
                    "ANALYZE merged samples exceed tidb_mem_quota_analyze".to_owned(),
                ));
            }
        }
        Ok(merged)
    })();
    result.close();
    outcome
}

/// Go analyzeIndexNDVPushDown uses index keys for prefix NDV. A single
/// column's NULL range has a separate histogram and never enters its sketch.
fn collect_index_ndv<C, L, P>(
    opener: &tidb_txnkv::transaction::RealOptimisticTransactionOpener<C, L, P>,
    physical_id: i64,
    index: &tidb_executor::analyze::AnalyzedIndex,
    common_handle: bool,
    statement: &super::AnalyzeStatement,
    snapshot: u64,
    concurrency: u64,
    timeout: std::time::Duration,
    cancellation: std::sync::Arc<tidb_distsql::CancelHandle>,
    resource_group: &str,
) -> Result<(i64, tidb_stats::FmSketch), AnalyzeError>
where
    C: tidb_txnkv::transaction::StoreWriteClient + tidb_txnkv::DirectUnaryClient,
    L: tidb_txnkv::transaction::StoreWriteLoader,
    P: tidb_txnkv::transaction::StorePdCapability,
{
    use prost::Message;
    let error = |e: String| AnalyzeError::unsupported(e);
    let prefix = if common_handle {
        tidb_codec::table_key::gen_table_record_prefix(physical_id)
    } else {
        tidb_codec::table_key::encode_index_seek_key(physical_id, index.id, &[])
    };
    let end = super::finite_successor(&prefix)?;
    let mut non_null = prefix.clone();
    let mut ranges = Vec::new();
    if index.column_positions.len() == 1 {
        // codec.NilFlag is 0. PrefixNext(prefix ++ NilFlag) starts non-NULLs.
        let mut null_start = prefix.clone();
        null_start.push(0);
        non_null = super::finite_successor(&null_start)?;
        ranges.push((true, null_start, non_null.clone()));
    }
    ranges.push((false, non_null, end));
    let request = tipb::AnalyzeReq {
        tp: Some(if common_handle {
            tipb::AnalyzeType::TypeCommonHandle
        } else {
            tipb::AnalyzeType::TypeIndex
        } as i32),
        flags: Some(statement.push_down_flags),
        time_zone_offset: Some(statement.time_zone.dag_zone().1),
        idx_req: Some(tipb::AnalyzeIndexReq {
            bucket_size: Some(1),
            num_columns: Some(index.column_positions.len() as i32),
            cmsketch_depth: Some(0),
            cmsketch_width: Some(0),
            top_n_size: Some(0),
            version: Some(2),
            sketch_size: Some(tidb_stats::MAX_SKETCH_SIZE as i64),
            ..Default::default()
        }),
        ..Default::default()
    };
    let transport = tidb_distsql::DirectUnaryQueryTransport::with_shared_runtime(
        opener
            .open_read_runtime()
            .map_err(|e| error(e.to_string()))?,
        tidb_distsql::DirectUnaryRuntimeConfig {
            default_timeout: timeout,
            ..Default::default()
        },
        tidb_txnkv::pd_capability::CapabilityTimestampSource(opener.pd().clone()),
    )
    .map_err(|e| error(e.to_string()))?
    .with_concurrent_workers()
    .map_err(|e| error(e.to_string()))?;
    let mut runtime = tidb_distsql::InjectedQueryRuntime::new(transport);
    let (read_ts, isolation) = read_policy(statement.analyze_snapshot, snapshot);
    let mut sketch = tidb_stats::FmSketch::new(tidb_stats::MAX_SKETCH_SIZE);
    let mut null_count = 0;
    for (is_null, start, end) in ranges {
        let mut builder = tidb_distsql::RequestBuilder::new();
        builder
            .set_analyze_request(request.encode_to_vec(), isolation)
            .set_paging(false)
            .set_start_ts(read_ts)
            .set_keep_order(true)
            .set_non_partitioned_key_ranges(vec![tidb_distsql::RequestKeyRange {
                start_key: start.into(),
                end_key: end.into(),
            }])
            .set_concurrency(concurrency)
            .set_resource_group_name(resource_group);
        let transport_request = builder
            .build_transport_request(cancellation.clone())
            .map_err(|e| error(format!("{e:?}")))?;
        let mut result = runtime
            .analyze(&transport_request, true)
            .map_err(|e| error(e.to_string()))?;
        let outcome: Result<(), AnalyzeError> = (|| {
            while let Some(bytes) = result.next_raw().map_err(|e| error(e.to_string()))? {
                let response = tipb::AnalyzeIndexResp::decode(bytes.as_slice())
                    .map_err(|e| error(e.to_string()))?;
                if is_null {
                    null_count += response
                        .hist
                        .as_ref()
                        .and_then(|hist| hist.buckets.last())
                        .map_or(0, |bucket| bucket.count());
                } else {
                    let fm = response
                        .collector
                        .and_then(|collector| collector.fm_sketch)
                        .ok_or_else(|| {
                            error("TiKV index ANALYZE response has no FM sketch".to_owned())
                        })?;
                    sketch.merge(&tidb_stats::FmSketch::from_raw_parts(
                        fm.mask.unwrap_or_default(),
                        tidb_stats::MAX_SKETCH_SIZE,
                        fm.hashset,
                    ));
                }
            }
            Ok(())
        })();
        result.close();
        outcome?;
    }
    Ok((null_count, sketch))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn adaptive_concurrency_matches_go_store_count_boundaries() {
        for (stores, expected) in [
            (0, 4),
            (5, 4),
            (6, 6),
            (10, 10),
            (11, 22),
            (20, 40),
            (21, 63),
            (50, 150),
            (51, 204),
        ] {
            assert_eq!(super::concurrency_for_stores(stores), expected);
        }
    }

    #[test]
    fn kill_cancels_sampling_and_completed_guard_does_not_wait_for_kill() {
        let killer = tidb_util::sqlkiller::SqlKiller::default();
        let cancellation = std::sync::Arc::new(tidb_distsql::CancelHandle::default());
        let guard = super::SamplingCancellation::start(&killer, cancellation.clone()).unwrap();
        killer.send_kill_signal(tidb_util::sqlkiller::KillSignal::QueryInterrupted);
        for _ in 0..100 {
            if cancellation.is_cancelled() {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        assert!(cancellation.is_cancelled());
        drop(guard);
        killer.reset();
        let cancellation = std::sync::Arc::new(tidb_distsql::CancelHandle::default());
        drop(super::SamplingCancellation::start(&killer, cancellation.clone()).unwrap());
        assert!(!cancellation.is_cancelled());
    }

    #[test]
    fn analyze_snapshot_controls_both_timestamp_and_isolation() {
        assert_eq!(
            super::read_policy(false, 123),
            (u64::MAX, tidb_distsql::IsolationLevel::ReadCommitted)
        );
        assert_eq!(
            super::read_policy(true, 123),
            (123, tidb_distsql::IsolationLevel::Snapshot)
        );
    }

    #[test]
    fn sample_decoding_restores_unsigned_integer_handle() {
        use tidb_datatype::{Datum, FieldType, FieldTypeCode};
        let mut schema = SamplingSchema::new(
            &TableInfo::default(),
            &AnalyzePlan::new(Vec::new(), Vec::new(), "t").unwrap(),
        )
        .unwrap();
        schema.columns[0].field_type = FieldType::new(FieldTypeCode::LongLong)
            .with_flags(tidb_datatype::FieldTypeFlags::UNSIGNED);
        let mut row = vec![Datum::Bytes(
            tidb_codec::encode_value(&[Datum::UInt(u64::MAX)]).unwrap(),
        )];
        let handle = schema
            .decode_handle(&mut row, &tidb_datatype::SessionTimeZone::utc())
            .unwrap();
        assert_eq!(handle.encoded(), tidb_txnkv::IntHandle::new(-1).encoded());
        assert_eq!(row, vec![Datum::UInt(u64::MAX)]);
    }

    #[test]
    fn collector_projection_skips_handle_stats_and_preserves_index_stats() {
        let plan = AnalyzePlan::new(
            vec![tidb_executor::analyze::AnalyzedColumn {
                id: 7,
                name: "a".to_owned(),
                field_type: tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
                absent_value: tidb_datatype::Datum::Null,
                collation: None,
            }],
            vec![tidb_executor::analyze::AnalyzedIndex {
                id: 3,
                column_positions: vec![0],
                prefix_lengths: vec![-1],
                single_column_unique: false,
            }],
            "t",
        )
        .unwrap();
        let schema = SamplingSchema::new(&TableInfo::default(), &plan).unwrap();
        let raw = tipb::RowSampleCollector {
            count: Some(100),
            null_counts: vec![2, 0, 3],
            total_size: vec![800, 999, 700],
            fm_sketch: vec![tipb::FmSketch::default(); 3],
            samples: vec![tipb::RowSample {
                row: vec![vec![0], vec![8, 2]],
                weight: Some(91),
            }],
            ..Default::default()
        };
        let projected = schema.project_collector(&plan, raw.clone()).unwrap();
        assert_eq!(projected.count, 100);
        assert_eq!(projected.null_counts, vec![2]);
        assert_eq!(projected.total_sizes, vec![800]);
        assert_eq!(projected.samples[0].weight, 91);
        assert_eq!(projected.samples[0].row.len(), 2);
        let mut malformed = raw;
        malformed.fm_sketch.pop();
        assert!(schema.project_collector(&plan, malformed).is_err());
    }
}
