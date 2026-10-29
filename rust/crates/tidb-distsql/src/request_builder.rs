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

//! Canonical pre-transport request builder translated from
//! `pkg/distsql/request_builder.go`.
//!
//! This owner joins request defaults, session projection, DAG concurrency,
//! table/index range encoding, and the immutable transport handoff. Protobuf
//! payloads remain caller-owned bytes and TiKV routing/RPC remains explicitly
//! outside this crate.

use tidb_ranger::points::{full_int_range, full_not_null_range, full_range};
use tidb_ranger::types::Range;
use std::sync::Arc;
use tidb_tablecodec::table_key::encode_index_seek_key;
use tidb_codec::encode_key;
use tidb_tablecodec::table_key::encode_row_key;
use tidb_datatype::Datum;
use tidb_txnkv::{Handle, Key, ResourceGroupTagBuilder};

use crate::{
    DistSqlContext, IsolationLevel, KvPriority, KvRequestMetadata, PartitionIdAndRanges,
    ReadRequestBuilder, ReplicaReadType, RequestEnvelope, RequestKeyRange, RequestKeyRanges,
    RequestSource, RequestType, StoreLabel, StoreType, TransportRequest, DC_LABEL_KEY,
    DEFAULT_DIST_SQL_CONCURRENCY, GLOBAL_REPLICA_SCOPE,
};

/// Index metadata required by [`build_table_ranges`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TableIndexRangeSpec {
    /// Physical index identifier.
    pub id: i64,
    /// Only public indexes are readable.
    pub public: bool,
    /// Global indexes belong to the logical table ID, not each partition.
    pub global: bool,
}

/// Dependency-closed table metadata required to build full scan ranges.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TableRangeSpec {
    /// Logical table identifier.
    pub table_id: i64,
    /// Physical partition identifiers in source order; empty means no
    /// partitioning.
    pub partition_ids: Vec<i64>,
    /// Whether records use a common handle.
    pub common_handle: bool,
    /// Public/global index metadata.
    pub indexes: Vec<TableIndexRangeSpec>,
}

/// Pre-transport encoding/build failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KvRequestBuildError {
    /// The one-use builder was consumed already.
    AlreadyBuilt,
    /// A currently supported Datum tuple could not be mem-comparably encoded.
    RangeEncoding,
}

/// The single canonical owner of Go `RequestBuilder` state transitions.
#[derive(Debug)]
pub struct RequestBuilder {
    request: KvRequestMetadata,
    dag: Option<RequestEnvelope>,
    error: Option<KvRequestBuildError>,
    used: bool,
}

/// Compatibility name retained while callers migrate to [`RequestBuilder`].
pub type KvRequestBuilder = RequestBuilder;

impl Default for RequestBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl RequestBuilder {
    /// Creates the exact Go zero-value builder state.
    #[must_use]
    pub fn new() -> Self {
        Self {
            request: KvRequestMetadata::default(),
            dag: None,
            error: None,
            used: false,
        }
    }

    /// Creates a builder with session metadata projected once.
    #[must_use]
    pub fn from_context(context: &DistSqlContext) -> Self {
        let mut builder = Self::new();
        builder.set_from_context(context);
        builder
    }

    /// Sets the raw request type.
    pub fn set_request_type(&mut self, request_type: RequestType) -> &mut Self {
        self.request.request_type = request_type;
        self
    }

    /// Sets the transaction start timestamp.
    pub fn set_start_ts(&mut self, start_ts: u64) -> &mut Self {
        self.request.start_ts = start_ts;
        self
    }

    /// Stores opaque protobuf bytes unchanged.
    pub fn set_data(&mut self, data: impl Into<Vec<u8>>) -> &mut Self {
        self.request.data = Some(data.into());
        self
    }

    /// Replaces ranges with a complete canonical envelope.
    pub fn set_key_ranges(&mut self, key_ranges: RequestKeyRanges) -> &mut Self {
        self.request.key_ranges = Some(Arc::new(key_ranges));
        self
    }

    /// Wraps ordinary ranges as one non-partitioned source group.
    pub fn set_non_partitioned_key_ranges(
        &mut self,
        key_ranges: Vec<RequestKeyRange>,
    ) -> &mut Self {
        self.request.key_ranges = Some(Arc::new(RequestKeyRanges::new_non_partitioned(key_ranges)));
        self
    }

    /// Wraps ordinary ranges and aligned row-count hints.
    pub fn set_key_ranges_with_hints(
        &mut self,
        key_ranges: Vec<RequestKeyRange>,
        hints: Vec<usize>,
    ) -> &mut Self {
        self.request.key_ranges = Some(Arc::new(RequestKeyRanges::new_non_partitioned_with_hints(
            key_ranges, hints,
        )));
        self
    }

    /// Replaces ranges with partitioned groups.
    pub fn set_partition_key_ranges(&mut self, key_ranges: Vec<Vec<RequestKeyRange>>) -> &mut Self {
        self.request.key_ranges = Some(Arc::new(RequestKeyRanges::new_partitioned(key_ranges)));
        self
    }

    /// Converts sorted table handles and their exact hints.
    pub fn set_table_handles(&mut self, table_id: i64, handles: &[Handle]) -> &mut Self {
        let (ranges, hints) = crate::table_handles_to_kv_ranges(table_id, handles);
        self.set_key_ranges_with_hints(ranges, hints)
    }

    /// Converts partition-wrapped handles. The embedded partition IDs are the
    /// physical table authority, matching Go `SetPartitionsAndHandles`.
    pub fn set_partitions_and_handles(&mut self, handles: &[Handle]) -> &mut Self {
        let (ranges, hints) = crate::table_handles_to_kv_ranges(0, handles);
        self.set_key_ranges_with_hints(ranges, hints)
    }

    /// Converts signed table-handle ranges into one non-partitioned group.
    pub fn set_table_ranges(&mut self, table_id: i64, ranges: &[Range]) -> &mut Self {
        if self.error.is_some() {
            return self;
        }
        match table_ranges_to_kv_ranges(table_id, ranges) {
            Ok(ranges) => self.set_non_partitioned_key_ranges(ranges),
            Err(error) => {
                self.remember_error(error);
                self
            }
        }
    }

    /// Go `SetHandleRanges`: table handle ranges through
    /// `TableHandleRangesToKVRanges` into one non-partitioned group -- signed
    /// integer handles for an int-handle table, mem-comparable tuples for a
    /// common-handle one (`CommonHandleRangesToKVRanges`).
    pub fn set_handle_ranges(
        &mut self,
        table_id: i64,
        is_common_handle: bool,
        ranges: &[Range],
    ) -> &mut Self {
        if !is_common_handle {
            return self.set_table_ranges(table_id, ranges);
        }
        if self.error.is_some() {
            return self;
        }
        match common_handle_ranges_to_kv_ranges(table_id, ranges) {
            Ok(ranges) => self.set_non_partitioned_key_ranges(ranges),
            Err(error) => {
                self.remember_error(error);
                self
            }
        }
    }

    /// Converts mem-comparable index tuples into one non-partitioned group.
    pub fn set_index_ranges(
        &mut self,
        table_id: i64,
        index_id: i64,
        ranges: &[Range],
    ) -> &mut Self {
        if self.error.is_some() {
            return self;
        }
        match index_ranges_to_kv_ranges(&[table_id], index_id, ranges) {
            Ok(mut ranges) => self.set_non_partitioned_key_ranges(ranges.remove(0)),
            Err(error) => {
                self.remember_error(error);
                self
            }
        }
    }

    /// Sets TiFlash partition ID/range metadata without flattening it.
    pub fn set_partition_id_and_ranges(
        &mut self,
        partition_id_and_ranges: Vec<PartitionIdAndRanges>,
    ) -> &mut Self {
        self.request.partition_id_and_ranges = partition_id_and_ranges;
        self
    }

    /// Attaches a DAG shape and its already-marshalled source payload.
    pub fn set_dag_request(
        &mut self,
        mut dag: RequestEnvelope,
        data: impl Into<Vec<u8>>,
    ) -> &mut Self {
        if self.error.is_some() {
            return self;
        }
        dag.keep_order = self.request.keep_order;
        if dag.partition_count == 0 {
            dag.partition_count = self
                .request
                .key_ranges
                .as_ref()
                .map_or(0, |ranges| ranges.partition_count());
        }
        if let Some(concurrency) = dag.small_limit_concurrency() {
            self.request.concurrency = concurrency as isize;
        }
        self.request.request_type = RequestType::Dag;
        self.request.cacheable = true;
        self.request.data = Some(data.into());
        self.request.limit_size = dag.limit_size().unwrap_or(0);
        self.dag = Some(dag);
        self
    }

    /// Compatibility setter for callers that attach payload separately.
    pub fn set_dag_envelope(&mut self, dag: RequestEnvelope) -> &mut Self {
        if self.error.is_some() {
            return self;
        }
        let data = self.request.data.take().unwrap_or_default();
        self.set_dag_request(dag, data)
    }

    /// Applies Analyze's source request defaults to opaque encoded bytes.
    pub fn set_analyze_request(
        &mut self,
        data: impl Into<Vec<u8>>,
        isolation_level: IsolationLevel,
    ) -> &mut Self {
        if self.error.is_some() {
            return self;
        }
        self.request.request_type = RequestType::Analyze;
        self.request.data = Some(data.into());
        self.request.not_fill_cache = true;
        self.request.isolation_level = isolation_level;
        self.request.priority = KvPriority::Low;
        self
    }

    /// Applies Checksum's source request defaults to opaque encoded bytes.
    pub fn set_checksum_request(&mut self, data: impl Into<Vec<u8>>) -> &mut Self {
        if self.error.is_some() {
            return self;
        }
        self.request.request_type = RequestType::Checksum;
        self.request.data = Some(data.into());
        self.request.not_fill_cache = true;
        self
    }

    /// Projects source session settings, preserving an already selected DAG
    /// concurrency long enough for the source upper-bound clamp.
    pub fn set_from_context(&mut self, context: &DistSqlContext) -> &mut Self {
        let current_concurrency = self.request.concurrency;
        let session = ReadRequestBuilder::new()
            .set_concurrency(u64::try_from(current_concurrency).unwrap_or(0))
            .from_context(context)
            .build();
        crate::kv_request::apply_session_metadata(&mut self.request, session);
        if !context.request.weak_consistency
            && !context.request.rc_check_ts
            && self.request.request_type == RequestType::Analyze
        {
            self.request.isolation_level = IsolationLevel::ReadCommitted;
        }
        self.request.connection_id = context.request.session.connection_id;
        self.request.connection_alias = context.request.session.alias.clone();
        self
    }

    /// Sets response ordering and keeps the attached DAG shape synchronized.
    pub fn set_keep_order(&mut self, keep_order: bool) -> &mut Self {
        self.request.keep_order = keep_order;
        if let Some(dag) = self.dag.as_mut() {
            dag.keep_order = keep_order;
        }
        self
    }

    /// Allows the coprocessor transport to use its unordered response path.
    ///
    /// This remains separate from `set_keep_order` so requests that merge
    /// staged rows keep the source's ordered-response validation by default.
    pub fn set_allow_unordered_response(&mut self, allow: bool) -> &mut Self {
        self.request.allow_unordered_response = allow;
        self
    }

    /// Sets descending range order.
    pub fn set_desc(&mut self, desc: bool) -> &mut Self {
        self.request.desc = desc;
        self
    }

    /// Sets request concurrency directly.
    pub fn set_concurrency(&mut self, concurrency: u64) -> &mut Self {
        self.request.concurrency = concurrency as isize;
        self
    }

    /// Sets replica-read mode.
    pub fn set_replica_read(&mut self, replica_read: ReplicaReadType) -> &mut Self {
        self.request.replica_read = replica_read;
        self
    }

    /// Sets only paging admission, preserving all configured paging sizes.
    pub fn set_paging(&mut self, enabled: bool) -> &mut Self {
        self.request.paging.enabled = enabled;
        self
    }

    /// Sets store engine metadata.
    pub fn set_store_type(&mut self, store_type: StoreType) -> &mut Self {
        self.request.store_type = store_type;
        self
    }

    /// Sets batch-coprocessor admission.
    pub fn set_allow_batch_cop(&mut self, batch_cop: bool) -> &mut Self {
        self.request.batch_cop = batch_cop;
        self
    }

    /// Sets the source opt-in for merging batched child data into the main response.
    pub fn set_allow_batch_task_data_merge(&mut self, allow: bool) -> &mut Self {
        self.request.allow_batch_task_data_merge = allow;
        self
    }

    /// Sets the source opt-in for serial execution of batched child tasks.
    pub fn set_execute_batch_tasks_serially(&mut self, execute_serially: bool) -> &mut Self {
        self.request.execute_batch_tasks_serially = execute_serially;
        self
    }

    /// Sets the per-store batching limit. A non-positive value disables batching.
    pub fn set_store_batch_size(&mut self, store_batch_size: isize) -> &mut Self {
        self.request.store_batch_size = store_batch_size;
        self
    }

    /// Sets TiDB server identity.
    pub fn set_tidb_server_id(&mut self, server_id: u64) -> &mut Self {
        self.request.tidb_server_id = server_id;
        self
    }

    /// Sets schema version already obtained from infoschema.
    pub fn set_schema_version(&mut self, schema_version: i64) -> &mut Self {
        self.request.schema_version = schema_version;
        self
    }

    /// Sets transaction scope metadata. Infoschema placement verification is
    /// intentionally a separate unresolved boundary.
    pub fn set_txn_scope(&mut self, scope: impl Into<String>) -> &mut Self {
        self.request.txn_scope = scope.into();
        self
    }

    /// Sets replica scope before global-default normalization.
    pub fn set_read_replica_scope(&mut self, scope: impl Into<String>) -> &mut Self {
        self.request.read_replica_scope = scope.into();
        self
    }

    /// Sets staleness metadata.
    pub fn set_is_staleness(&mut self, is_staleness: bool) -> &mut Self {
        self.request.is_staleness = is_staleness;
        self
    }

    /// Sets connection identity explicitly.
    pub fn set_connection(&mut self, id: u64, alias: impl Into<String>) -> &mut Self {
        self.request.connection_id = id;
        self.request.connection_alias = alias.into();
        self
    }

    /// Sets the resource group name independently of session projection.
    pub fn set_resource_group_name(&mut self, name: impl Into<String>) -> &mut Self {
        self.request.resource_group_name = name.into();
        self
    }

    /// Replaces all request-source metadata.
    pub fn set_request_source(&mut self, source: RequestSource) -> &mut Self {
        self.request.request_source = source;
        self
    }

    /// Replaces only the explicit request-source type.
    pub fn set_explicit_request_source_type(
        &mut self,
        source_type: impl Into<String>,
    ) -> &mut Self {
        self.request.request_source.explicit_source_type = source_type.into();
        self
    }

    /// Retains a real source-compatible tagger for the transport consumer.
    pub fn set_resource_group_tagger(&mut self, tagger: ResourceGroupTagBuilder) -> &mut Self {
        self.request.resource_group_tagger = Some(tagger);
        self
    }

    /// Clears a tagger, matching Go's ability to pass a nil tag builder.
    pub fn clear_resource_group_tagger(&mut self) -> &mut Self {
        self.request.resource_group_tagger = None;
        self
    }

    /// Attach Go's request adjuster; it runs after region tasks are built.
    pub fn set_closest_replica_read_adjuster(
        &mut self,
        adjuster: Option<Arc<dyn tidb_txnkv::CoprocessorRequestAdjuster>>,
    ) -> &mut Self {
        self.request.closest_replica_read_adjuster = adjuster;
        self
    }

    /// Builds immutable request metadata once.
    pub fn build(&mut self) -> Result<KvRequestMetadata, KvRequestBuildError> {
        if self.used {
            return Err(KvRequestBuildError::AlreadyBuilt);
        }
        self.used = true;
        if let Some(error) = self.error {
            return Err(error);
        }

        let mut request = self.request.clone();
        if request.read_replica_scope.is_empty() {
            request.read_replica_scope = GLOBAL_REPLICA_SCOPE.to_owned();
        }
        if request.replica_read.is_closest_read()
            && request.read_replica_scope != GLOBAL_REPLICA_SCOPE
        {
            request.match_store_labels = vec![StoreLabel {
                key: DC_LABEL_KEY.to_owned(),
                value: request.read_replica_scope.clone(),
            }];
        }
        if request.key_ranges.is_none() {
            request.key_ranges = Some(Arc::new(RequestKeyRanges::new_non_partitioned(Vec::new())));
        }

        if let Some(dag) = &self.dag {
            let mut shape = dag.clone();
            shape.keep_order = request.keep_order;
            request.concurrency = shape.build_concurrency(
                u64::try_from(request.concurrency).unwrap_or(0),
                DEFAULT_DIST_SQL_CONCURRENCY,
            ) as isize;
        }

        Ok(request)
    }

    /// Builds and hands the immutable snapshot to the real pre-transport
    /// consumer without fabricating a client or RPC.
    pub fn build_transport_request(
        &mut self,
        execution_cancellation: std::sync::Arc<crate::CancelHandle>,
    ) -> Result<TransportRequest, KvRequestBuildError> {
        self.build()
            .map(|metadata| TransportRequest::new(metadata, execution_cancellation))
    }

    fn remember_error(&mut self, error: KvRequestBuildError) {
        if self.error.is_none() {
            self.error = Some(error);
        }
    }
}

/// Encodes signed table-handle ranges with exact exclusion adjustments.
pub fn table_ranges_to_kv_ranges(
    table_id: i64,
    ranges: &[Range],
) -> Result<Vec<RequestKeyRange>, KvRequestBuildError> {
    ranges
        .iter()
        .map(|range| {
            let low = range
                .low_val
                .first()
                .and_then(int_bits)
                .ok_or(KvRequestBuildError::RangeEncoding)?;
            let high = range
                .high_val
                .first()
                .and_then(int_bits)
                .ok_or(KvRequestBuildError::RangeEncoding)?;
            Ok(crate::signed_handle_range::encode_signed_handle_range(
                table_id,
                low,
                high,
                range.low_exclude,
                range.high_exclude,
            ))
        })
        .collect()
}

/// Encodes index ranges for every physical table without flattening groups.
pub fn index_ranges_to_kv_ranges(
    table_ids: &[i64],
    index_id: i64,
    ranges: &[Range],
) -> Result<Vec<Vec<RequestKeyRange>>, KvRequestBuildError> {
    let encoded = ranges
        .iter()
        .map(encode_index_key)
        .collect::<Result<Vec<_>, KvRequestBuildError>>()?;

    Ok(table_ids
        .iter()
        .map(|table_id| {
            encoded
                .iter()
                .map(|(low, high)| RequestKeyRange {
                    start_key: encode_index_seek_key(*table_id, index_id, low).into(),
                    end_key: encode_index_seek_key(*table_id, index_id, high).into(),
                })
                .collect()
        })
        .collect())
}

/// Go `EncodeIndexKey`: the mem-comparable bounds of one range, with the
/// exclusion adjustments applied.
fn encode_index_key(range: &Range) -> Result<(Vec<u8>, Vec<u8>), KvRequestBuildError> {
    let mut low = encode_key(&range.low_val).map_err(|_| KvRequestBuildError::RangeEncoding)?;
    if range.low_exclude {
        low = Key::from_bytes(low).prefix_next().into_bytes();
    }
    let mut high = encode_key(&range.high_val).map_err(|_| KvRequestBuildError::RangeEncoding)?;
    if !range.high_exclude {
        high = Key::from_bytes(high).prefix_next().into_bytes();
    }
    Ok((low, high))
}

/// Go `CommonHandleRangesToKVRanges` for one physical table: each range's
/// `EncodeIndexKey` bounds under the table's record prefix.
pub fn common_handle_ranges_to_kv_ranges(
    table_id: i64,
    ranges: &[Range],
) -> Result<Vec<RequestKeyRange>, KvRequestBuildError> {
    ranges
        .iter()
        .map(|range| {
            let (low, high) = encode_index_key(range)?;
            Ok(RequestKeyRange {
                start_key: encode_row_key(table_id, &low).into(),
                end_key: encode_row_key(table_id, &high).into(),
            })
        })
        .collect()
}

/// Go `BuildTableRanges`: full record and public-index ranges for a
/// table/partition layout. Global indexes exist only on a partitioned table,
/// under the logical table ID, ahead of every partition's own ranges.
pub fn build_table_ranges(
    table: &TableRangeSpec,
) -> Result<Vec<RequestKeyRange>, KvRequestBuildError> {
    if table.partition_ids.is_empty() {
        // Short path, no partition.
        return append_ranges(table, table.table_id);
    }
    let mut output = Vec::new();
    for index in table
        .indexes
        .iter()
        .filter(|index| index.public && index.global)
    {
        output.extend(
            index_ranges_to_kv_ranges(&[table.table_id], index.id, &full_range())?
                .into_iter()
                .flatten(),
        );
    }
    for table_id in &table.partition_ids {
        output.extend(append_ranges(table, *table_id)?);
    }
    Ok(output)
}

/// Go `appendRanges`: one physical table's full record range
/// (`FullNotNullRange` for a common handle, `FullIntRange(false)` otherwise)
/// followed by `FullRange` over each public local index.
fn append_ranges(
    table: &TableRangeSpec,
    table_id: i64,
) -> Result<Vec<RequestKeyRange>, KvRequestBuildError> {
    let mut output = if table.common_handle {
        common_handle_ranges_to_kv_ranges(table_id, &full_not_null_range())?
    } else {
        table_ranges_to_kv_ranges(table_id, &full_int_range(false))?
    };
    for index in table
        .indexes
        .iter()
        .filter(|index| index.public && !index.global)
    {
        output.extend(
            index_ranges_to_kv_ranges(&[table_id], index.id, &full_range())?
                .into_iter()
                .flatten(),
        );
    }
    Ok(output)
}

fn int_bits(value: &Datum) -> Option<i64> {
    match value {
        Datum::Int(value) => Some(*value),
        Datum::UInt(value) => Some(*value as i64),
        _ => None,
    }
}

/// Go `SplitRangesAcrossInt64Boundary`
/// (`pkg/distsql/request_builder.go:597`): divides an UNSIGNED handle's
/// ranges at the point where the key encoding flips sign.
///
/// A row handle is encoded as a signed 64-bit integer, so an unsigned value
/// above `i64::MAX` encodes NEGATIVE and sorts, in key order, before every
/// ordinary handle. A single range spanning that point would be read as one
/// contiguous key interval and would miss everything on the far side.
///
/// The result is two range lists in the order they must be READ. When order
/// does not matter Go concatenates them into the first list and leaves the
/// second empty, which is why the signature returns a pair rather than two
/// separate calls: the caller must not reorder them itself.
///
/// Returns `(signed, unsigned)` for an ascending ordered read and
/// `(unsigned, signed)` for a descending one, because the unsigned half
/// occupies the LOWER key range.
#[must_use]
pub fn split_ranges_across_int64_boundary(
    ranges: Vec<Range>,
    keep_order: bool,
    desc: bool,
    is_common_handle: bool,
) -> (Vec<Range>, Vec<Range>) {
    /// The unsigned reading of a bound, or `None` when it is not an integer
    /// this split understands.
    fn unsigned(datum: Option<&Datum>) -> Option<u64> {
        match datum? {
            Datum::UInt(value) => Some(*value),
            Datum::Int(value) => Some(*value as u64),
            _ => None,
        }
    }
    // Go returns the ranges untouched for a common handle, an empty set, or a
    // SIGNED leading bound -- the last being how it detects that the handle is
    // not unsigned at all.
    if is_common_handle
        || ranges.is_empty()
        || matches!(ranges[0].low_val.first(), Some(Datum::Int(_)))
    {
        return (ranges, Vec::new());
    }
    let boundary = i64::MAX as u64;
    let Some(index) = ranges
        .iter()
        .position(|range| unsigned(range.high_val.first()).is_some_and(|high| high > boundary))
    else {
        // Nothing reaches past the boundary.
        return (ranges, Vec::new());
    };
    let order = |signed: Vec<Range>, mut unsigned_half: Vec<Range>| {
        if !keep_order {
            // Go appends the SIGNED half onto the unsigned one and returns a
            // single list: with no order to keep, one scan covers both.
            unsigned_half.extend(signed);
            return (unsigned_half, Vec::new());
        }
        if desc {
            (unsigned_half, signed)
        } else {
            (signed, unsigned_half)
        }
    };
    let straddles = unsigned(ranges[index].low_val.first()).is_some_and(|low| low <= boundary);
    if !straddles {
        // A clean cut: every range from `index` on is wholly above the
        // boundary.
        let mut ranges = ranges;
        let unsigned_half = ranges.split_off(index);
        return order(ranges, unsigned_half);
    }
    // The range at `index` spans the boundary and has to be cut in two.
    let mut ranges = ranges;
    let tail = ranges.split_off(index);
    let mut signed = ranges;
    let (crossing, rest) = tail.split_first().expect("the straddling range");
    // Go skips the signed piece when the cut point is EXCLUDED at exactly
    // `MaxInt64`, because that piece would then be empty.
    if !(unsigned(crossing.low_val.first()) == Some(boundary) && crossing.low_exclude) {
        signed.push(Range {
            low_val: crossing.low_val.clone(),
            high_val: vec![Datum::UInt(boundary)],
            collators: crossing.collators.clone(),
            low_exclude: crossing.low_exclude,
            high_exclude: false,
        });
    }
    let mut unsigned_half = Vec::with_capacity(rest.len() + 1);
    // ... and the unsigned piece when it is excluded at exactly
    // `MaxInt64 + 1`, for the same reason.
    if !(unsigned(crossing.high_val.first()) == Some(boundary + 1) && crossing.high_exclude) {
        unsigned_half.push(Range {
            low_val: vec![Datum::UInt(boundary + 1)],
            high_val: crossing.high_val.clone(),
            collators: crossing.collators.clone(),
            low_exclude: false,
            high_exclude: crossing.high_exclude,
        });
    }
    unsigned_half.extend(rest.iter().cloned());
    order(signed, unsigned_half)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sticky_range_error_blocks_guarded_request_family_mutations() {
        let invalid = Range {
            low_val: vec![Datum::new_string("not an integer")],
            high_val: vec![Datum::new_string("not an integer")],
            collators: vec![tidb_datatype::Collation::Binary],
            ..Range::default()
        };
        let mut builder = RequestBuilder::new();
        builder.set_table_ranges(1, &[invalid]);
        assert_eq!(builder.error, Some(KvRequestBuildError::RangeEncoding));

        builder
            .set_dag_request(RequestEnvelope::new(Vec::new()), [1, 2, 3])
            .set_analyze_request([4], IsolationLevel::ReadCommitted)
            .set_checksum_request([5]);
        assert_eq!(builder.request.request_type, RequestType::Unknown);
        assert_eq!(builder.request.data, None);
        assert!(!builder.request.cacheable);

        assert!(matches!(
            builder.build(),
            Err(KvRequestBuildError::RangeEncoding)
        ));
        assert!(matches!(
            builder.build(),
            Err(KvRequestBuildError::AlreadyBuilt)
        ));
    }
}

#[cfg(test)]
mod int64_boundary_split_tests {
    use super::*;

    const MAX: u64 = i64::MAX as u64;

    fn range(low: u64, high: u64) -> Range {
        Range {
            low_val: vec![Datum::UInt(low)],
            high_val: vec![Datum::UInt(high)],
            collators: vec![tidb_datatype::Collation::Binary],
            low_exclude: false,
            high_exclude: false,
        }
    }

    fn bounds(ranges: &[Range]) -> Vec<(u64, u64)> {
        ranges
            .iter()
            .map(|range| match (range.low_val.first(), range.high_val.first()) {
                (Some(Datum::UInt(low)), Some(Datum::UInt(high))) => (*low, *high),
                other => panic!("unsigned bounds, got {other:?}"),
            })
            .collect()
    }

    #[test]
    fn a_signed_leading_bound_is_left_alone() {
        // Go detects a non-unsigned handle by the leading bound's KIND and
        // returns the ranges untouched.
        let input = vec![Range {
            low_val: vec![Datum::Int(1)],
            high_val: vec![Datum::Int(9)],
            collators: vec![tidb_datatype::Collation::Binary],
            low_exclude: false,
            high_exclude: false,
        }];
        let (first, second) = split_ranges_across_int64_boundary(input, true, false, false);
        assert_eq!(first.len(), 1);
        assert!(second.is_empty());
    }

    #[test]
    fn a_common_handle_is_left_alone() {
        // Go's first test: a common handle has no int64 boundary to split at.
        let (first, second) =
            split_ranges_across_int64_boundary(vec![range(1, MAX + 5)], true, false, true);
        assert_eq!(bounds(&first), vec![(1, MAX + 5)]);
        assert!(second.is_empty());
    }

    #[test]
    fn ranges_below_the_boundary_are_left_alone() {
        let (first, second) =
            split_ranges_across_int64_boundary(vec![range(1, 9), range(20, MAX)], true, false, false);
        assert_eq!(bounds(&first), vec![(1, 9), (20, MAX)]);
        assert!(second.is_empty(), "nothing reaches past the boundary");
    }

    #[test]
    fn a_clean_cut_splits_between_ranges() {
        // No range straddles: the split falls between them.
        let (signed, unsigned) = split_ranges_across_int64_boundary(
            vec![range(1, 9), range(MAX + 1, MAX + 5)],
            true,
            false,
            false,
        );
        assert_eq!(bounds(&signed), vec![(1, 9)]);
        assert_eq!(bounds(&unsigned), vec![(MAX + 1, MAX + 5)]);
    }

    #[test]
    fn a_straddling_range_is_cut_at_the_boundary() {
        let (signed, unsigned) =
            split_ranges_across_int64_boundary(vec![range(1, MAX + 5)], true, false, false);
        assert_eq!(
            bounds(&signed),
            vec![(1, MAX)],
            "the signed piece ends AT i64::MAX, inclusive"
        );
        assert_eq!(
            bounds(&unsigned),
            vec![(MAX + 1, MAX + 5)],
            "and the unsigned piece starts one past it"
        );
    }

    #[test]
    fn an_empty_signed_piece_is_not_emitted() {
        // Go skips the signed half when the low bound is EXCLUSIVE at exactly
        // i64::MAX, because `(MAX, MAX]` selects nothing.
        let mut input = range(MAX, MAX + 5);
        input.low_exclude = true;
        let (signed, unsigned) = split_ranges_across_int64_boundary(vec![input], true, false, false);
        assert!(signed.is_empty(), "an empty signed piece is omitted");
        assert_eq!(bounds(&unsigned), vec![(MAX + 1, MAX + 5)]);
    }

    #[test]
    fn an_empty_unsigned_piece_is_not_emitted() {
        // The mirror image: exclusive at exactly i64::MAX + 1.
        let mut input = range(1, MAX + 1);
        input.high_exclude = true;
        let (signed, unsigned) = split_ranges_across_int64_boundary(vec![input], true, false, false);
        assert_eq!(bounds(&signed), vec![(1, MAX)]);
        assert!(unsigned.is_empty(), "an empty unsigned piece is omitted");
    }

    #[test]
    fn descending_order_reads_the_unsigned_half_first() {
        // The unsigned half occupies the LOWER key range, so a descending
        // ordered read takes it first.
        let (first, second) =
            split_ranges_across_int64_boundary(vec![range(1, MAX + 5)], true, true, false);
        assert_eq!(bounds(&first), vec![(MAX + 1, MAX + 5)]);
        assert_eq!(bounds(&second), vec![(1, MAX)]);
    }

    #[test]
    fn an_unordered_read_gets_one_list_with_the_unsigned_half_first() {
        // Go concatenates rather than returning a pair when order is free, so
        // ONE scan covers both halves -- and it puts the unsigned half first
        // because that is where the keys start.
        let (all, second) =
            split_ranges_across_int64_boundary(vec![range(1, MAX + 5)], false, false, false);
        assert_eq!(bounds(&all), vec![(MAX + 1, MAX + 5), (1, MAX)]);
        assert!(second.is_empty(), "an unordered read gets a single list");
    }
}
