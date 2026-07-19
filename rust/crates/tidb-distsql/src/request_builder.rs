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

use tidb_codec::table_key::encode_index_seek_key;
use tidb_codec::{encode_key, encode_row_key};
use tidb_datatype::Datum;
use tidb_txnkv::{Handle, Key, ResourceGroupTagBuilder};

use crate::{
    DistSqlContext, IsolationLevel, KvPriority, KvRequestMetadata, PartitionIdAndRanges,
    ReadRequestBuilder, ReplicaReadType, RequestEnvelope, RequestKeyRange, RequestKeyRanges,
    RequestSource, RequestType, StoreLabel, StoreType, TransportRequest, DC_LABEL_KEY,
    DEFAULT_DIST_SQL_CONCURRENCY, GLOBAL_REPLICA_SCOPE,
};

/// One source ranger boundary represented by already typed Datum values.
#[derive(Clone, Debug, PartialEq)]
pub struct DatumRange {
    /// Inclusive value tuple before `low_exclude` adjustment.
    pub low: Vec<Datum>,
    /// Inclusive value tuple before `high_exclude` adjustment.
    pub high: Vec<Datum>,
    /// Whether the low tuple is excluded.
    pub low_exclude: bool,
    /// Whether the high tuple is excluded.
    pub high_exclude: bool,
}

impl DatumRange {
    /// Creates the common inclusive source range shape.
    #[must_use]
    pub fn inclusive(low: Vec<Datum>, high: Vec<Datum>) -> Self {
        Self {
            low,
            high,
            low_exclude: false,
            high_exclude: false,
        }
    }
}

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
        self.request.key_ranges = Some(key_ranges);
        self
    }

    /// Wraps ordinary ranges as one non-partitioned source group.
    pub fn set_non_partitioned_key_ranges(
        &mut self,
        key_ranges: Vec<RequestKeyRange>,
    ) -> &mut Self {
        self.request.key_ranges = Some(RequestKeyRanges::new_non_partitioned(key_ranges));
        self
    }

    /// Wraps ordinary ranges and aligned row-count hints.
    pub fn set_key_ranges_with_hints(
        &mut self,
        key_ranges: Vec<RequestKeyRange>,
        hints: Vec<usize>,
    ) -> &mut Self {
        self.request.key_ranges = Some(RequestKeyRanges::new_non_partitioned_with_hints(
            key_ranges, hints,
        ));
        self
    }

    /// Replaces ranges with partitioned groups.
    pub fn set_partition_key_ranges(&mut self, key_ranges: Vec<Vec<RequestKeyRange>>) -> &mut Self {
        self.request.key_ranges = Some(RequestKeyRanges::new_partitioned(key_ranges));
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
    pub fn set_table_ranges(&mut self, table_id: i64, ranges: &[DatumRange]) -> &mut Self {
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

    /// Converts mem-comparable index tuples into one non-partitioned group.
    pub fn set_index_ranges(
        &mut self,
        table_id: i64,
        index_id: i64,
        ranges: &[DatumRange],
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
                .map_or(0, RequestKeyRanges::partition_count);
        }
        if let Some(concurrency) = dag.small_limit_concurrency() {
            self.request.session.concurrency = concurrency;
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
        self.request.session.not_fill_cache = true;
        self.request.session.isolation_level = isolation_level;
        self.request.session.priority = KvPriority::Low;
        self
    }

    /// Applies Checksum's source request defaults to opaque encoded bytes.
    pub fn set_checksum_request(&mut self, data: impl Into<Vec<u8>>) -> &mut Self {
        if self.error.is_some() {
            return self;
        }
        self.request.request_type = RequestType::Checksum;
        self.request.data = Some(data.into());
        self.request.session.not_fill_cache = true;
        self
    }

    /// Projects source session settings, preserving an already selected DAG
    /// concurrency long enough for the source upper-bound clamp.
    pub fn set_from_context(&mut self, context: &DistSqlContext) -> &mut Self {
        let current_concurrency = self.request.session.concurrency;
        self.request.session = ReadRequestBuilder::new()
            .set_concurrency(current_concurrency)
            .from_context(context)
            .build();
        if !context.request.weak_consistency
            && !context.request.rc_check_ts
            && self.request.request_type == RequestType::Analyze
        {
            self.request.session.isolation_level = IsolationLevel::ReadCommitted;
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

    /// Sets descending range order.
    pub fn set_desc(&mut self, desc: bool) -> &mut Self {
        self.request.desc = desc;
        self
    }

    /// Sets request concurrency directly.
    pub fn set_concurrency(&mut self, concurrency: u64) -> &mut Self {
        self.request.session.concurrency = concurrency;
        self
    }

    /// Sets replica-read mode.
    pub fn set_replica_read(&mut self, replica_read: ReplicaReadType) -> &mut Self {
        self.request.session.replica_read = replica_read;
        self
    }

    /// Sets only paging admission, preserving all configured paging sizes.
    pub fn set_paging(&mut self, enabled: bool) -> &mut Self {
        self.request.session.paging.enabled = enabled;
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
        self.request.session.resource_group_name = name.into();
        self
    }

    /// Replaces all request-source metadata.
    pub fn set_request_source(&mut self, source: RequestSource) -> &mut Self {
        self.request.session.request_source = source;
        self
    }

    /// Replaces only the explicit request-source type.
    pub fn set_explicit_request_source_type(
        &mut self,
        source_type: impl Into<String>,
    ) -> &mut Self {
        self.request.session.request_source.explicit_source_type = source_type.into();
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
        if request.session.replica_read.is_closest_read()
            && request.read_replica_scope != GLOBAL_REPLICA_SCOPE
        {
            request.match_store_labels = vec![StoreLabel {
                key: DC_LABEL_KEY.to_owned(),
                value: request.read_replica_scope.clone(),
            }];
        }
        if request.key_ranges.is_none() {
            request.key_ranges = Some(RequestKeyRanges::new_non_partitioned(Vec::new()));
        }

        if let Some(dag) = &self.dag {
            let mut shape = dag.clone();
            shape.keep_order = request.keep_order;
            request.session.concurrency =
                shape.build_concurrency(request.session.concurrency, DEFAULT_DIST_SQL_CONCURRENCY);
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
    ranges: &[DatumRange],
) -> Result<Vec<RequestKeyRange>, KvRequestBuildError> {
    ranges
        .iter()
        .map(|range| {
            let low = range
                .low
                .first()
                .and_then(int_bits)
                .ok_or(KvRequestBuildError::RangeEncoding)?;
            let high = range
                .high
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
    ranges: &[DatumRange],
) -> Result<Vec<Vec<RequestKeyRange>>, KvRequestBuildError> {
    let encoded = ranges
        .iter()
        .map(|range| {
            let mut low = encode_key(&range.low).map_err(|_| KvRequestBuildError::RangeEncoding)?;
            let mut high =
                encode_key(&range.high).map_err(|_| KvRequestBuildError::RangeEncoding)?;
            if range.low_exclude {
                low = Key::from_bytes(low).prefix_next().into_bytes();
            }
            if !range.high_exclude {
                high = Key::from_bytes(high).prefix_next().into_bytes();
            }
            Ok((low, high))
        })
        .collect::<Result<Vec<_>, KvRequestBuildError>>()?;

    Ok(table_ids
        .iter()
        .map(|table_id| {
            encoded
                .iter()
                .map(|(low, high)| RequestKeyRange {
                    start_key: encode_index_seek_key(*table_id, index_id, low),
                    end_key: encode_index_seek_key(*table_id, index_id, high),
                })
                .collect()
        })
        .collect())
}

/// Builds full record and public-index ranges for a table/partition layout.
pub fn build_table_ranges(
    table: &TableRangeSpec,
) -> Result<Vec<RequestKeyRange>, KvRequestBuildError> {
    let physical_ids = if table.partition_ids.is_empty() {
        vec![table.table_id]
    } else {
        table.partition_ids.clone()
    };
    let record_range = if table.common_handle {
        DatumRange::inclusive(vec![Datum::MinNotNull], vec![Datum::MaxValue])
    } else {
        DatumRange::inclusive(vec![Datum::Int(i64::MIN)], vec![Datum::Int(i64::MAX)])
    };
    let mut output = Vec::new();

    for index in table
        .indexes
        .iter()
        .filter(|index| index.public && index.global)
    {
        output.extend(
            index_ranges_to_kv_ranges(
                &[table.table_id],
                index.id,
                &[DatumRange::inclusive(
                    vec![Datum::MinNotNull],
                    vec![Datum::MaxValue],
                )],
            )?
            .into_iter()
            .flatten(),
        );
    }

    for table_id in physical_ids {
        if table.common_handle {
            let mut low =
                encode_key(&record_range.low).map_err(|_| KvRequestBuildError::RangeEncoding)?;
            let mut high =
                encode_key(&record_range.high).map_err(|_| KvRequestBuildError::RangeEncoding)?;
            if !record_range.high_exclude {
                high = Key::from_bytes(high).prefix_next().into_bytes();
            }
            if record_range.low_exclude {
                low = Key::from_bytes(low).prefix_next().into_bytes();
            }
            output.push(RequestKeyRange {
                start_key: encode_row_key(table_id, &low),
                end_key: encode_row_key(table_id, &high),
            });
        } else {
            output.extend(table_ranges_to_kv_ranges(
                table_id,
                std::slice::from_ref(&record_range),
            )?);
        }
        for index in table
            .indexes
            .iter()
            .filter(|index| index.public && !index.global)
        {
            output.extend(
                index_ranges_to_kv_ranges(
                    &[table_id],
                    index.id,
                    &[DatumRange::inclusive(
                        vec![Datum::MinNotNull],
                        vec![Datum::MaxValue],
                    )],
                )?
                .into_iter()
                .flatten(),
            );
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sticky_range_error_blocks_guarded_request_family_mutations() {
        let invalid = DatumRange::inclusive(
            vec![Datum::new_string("not an integer")],
            vec![Datum::new_string("not an integer")],
        );
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
