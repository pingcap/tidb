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

//! Source-shaped DistSQL context primitives.
//!
//! This crate now owns bounded request construction, protobuf encoding,
//! checked caller-supplied region-task projection, response decoding, and
//! result iteration. Its production adapter now retains the PD-backed region
//! cache and BatchCommands-first TiKV transport used by the bounded read-only
//! SQL node; topology, selection, retry, lock recovery, and cancellation stay
//! in their existing single owners. The shared [`DistSqlContext::request`]
//! remains the single source of session request metadata rather than
//! inventing a second replica-read or paging contract.

mod channel_iter;
mod chblock;
mod chunk_decode;
mod context;
pub mod cop_paging;
mod copr_cache;
mod coprocessor_request;
mod distsql_runtime;
mod envelope;
mod execution;
mod kv_request;
mod paging;
pub mod query_runtime;
mod read_bytes_ema;
mod region_location;
mod region_task;
mod request;
mod request_builder;
mod response_channel;
mod select_iter;
mod signed_handle_range;
mod stream_decode;
mod table_handle_ranges;
mod tiflash_replica_read;
mod transport;
mod warning;

pub use channel_iter::{ChannelIter, ChannelIterError, ChannelIterUnsupported, ChannelRow};
pub use chblock::{decode_ch_block, RawChBlockChunk};
pub use chunk_decode::{
    decode_chunk, decode_response_chunks, decode_select_response, ChunkDecodeError, RawChunk,
    RawChunkRow, RawColumnarChunk, TypedColumnarChunk,
};
pub use context::{
    DistSqlContext, PagingConfig, Priority, RequestContext, SessionContext,
    DEFAULT_DIST_SQL_CONCURRENCY,
};
pub use cop_paging::{
    calculate_paging_remain, calculate_paging_retry, paging_response_read_bytes,
    BatchBucketVersionUpdate, CopPagingError, CopPagingOutcome, CopPagingState, DirectUnaryClient,
    DirectUnaryClientError, DirectUnaryQueryResponse, DirectUnaryQueryTransport,
    DirectUnaryRequest, DirectUnaryResponse, DirectUnaryRuntimeConfig, DirectUnaryTransportError,
    DirectUnaryTransportEvidence, DirectUnaryTransportEvidenceHandle, LockedResponseAction,
    LockedResponseDelegate, LockedResponseObservation, OptimisticLockRecovery,
    PublicationObserverAlreadyInstalled, PublishedDispatchEvidence, ReadEngineGeneration,
    RegionRetryWaiter,
};
pub use copr_cache::{
    build_copr_cache_key, CoprCache, CoprCacheAdmission, CoprCacheConfig, CoprCacheError,
    CoprCacheLookup, CoprCacheRequestContext, CoprCacheResponseContext, CoprCacheResponseOutcome,
    CoprCacheValue,
};
pub use coprocessor_request::CoprocessorRequestEnvelope;
pub use distsql_runtime::{
    analyze_request_source, analyze_result_metadata, can_use_chunk_rpc, checksum_result_metadata,
    mpp_result_metadata, select_result_metadata, select_with_runtime_stats, set_encode_type,
    system_endian, tiflash_conf_metadata, with_sql_kv_exec_counter_interceptor, EncodeType,
    OutgoingMetadata, SelectInput, SelectResultMetadata, SelectResultRuntimeStats, SystemEndian,
    TiFlashSettings, ANALYZE_RESULT_LABEL, CHECKSUM_RESULT_LABEL, DAG_RESULT_LABEL,
    GENERAL_SQL_TYPE, INTERNAL_SQL_TYPE, INTERNAL_TXN_STATS_SOURCE, MPP_RESULT_LABEL,
};
pub use envelope::{ExecutorKind, ExecutorShape, RequestEnvelope, ESTIMATED_REGION_ROW_COUNT};
pub use execution::{CancelHandle, CpuUsage, ExecutionState, KillHandle, KvVariables};
pub use kv_request::{
    KvRequestMetadata, PartitionIdAndRanges, RequestKeyRange, RequestKeyRanges, DC_LABEL_KEY,
    GLOBAL_REPLICA_SCOPE,
};
pub use paging::{
    calculate_seek_count, grow_paging_size, MIN_ALLOWED_MAX_PAGING_SIZE, MIN_PAGING_SIZE,
    PAGING_THRESHOLD,
};
pub use query_runtime::{
    InjectedQueryRuntime, QueryDispatch, QueryOperation, QueryResponseError, QueryResultContext,
    QueryRuntimeError, QueryTransport,
};
pub use read_bytes_ema::ReadBytesEma;
pub use region_location::RegionTaskLocation;
pub use region_task::{
    RegionTaskEnvelope, RegionTaskEpoch, RegionTaskPeer, RegionTaskTopology,
    VersionedRegionKeyRange,
};
pub use request::{ReadRequestBuilder, ReadRequestMetadata};
pub use request_builder::{
    build_table_ranges, index_ranges_to_kv_ranges, table_ranges_to_kv_ranges, DatumRange,
    KvRequestBuildError, KvRequestBuilder, RequestBuilder, TableIndexRangeSpec, TableRangeSpec,
};
pub use response_channel::{
    unsupported_raw_tipb_response, unsupported_tikv_response_channel, ResponseChannel,
    ResponseChannelError, ResponseChannelEvent, ResponseChannelState, ResponseChannelUnsupported,
    ResponseRuntimeStats, SelectResponseIter,
};
pub use select_iter::{
    unsupported_chunk, unsupported_next_raw, unsupported_sorted_heap, unsupported_tikv_transport,
    SelectResultError, SelectResultRow, SelectResultSource, SerialSelectResults,
    UnsupportedCapability,
};
pub use signed_handle_range::{signed_handle_ranges_to_kv_ranges, SignedHandleRange};
pub use stream_decode::{decode_stream_response, RawStreamResponse};
pub use table_handle_ranges::table_handles_to_kv_ranges;
pub use tidb_txnkv::lock::{FixedTimestampSource, LockRecoveryClient, TimestampSource};
pub use tidb_txnkv::region;
pub use tidb_txnkv::{
    IsolationLevel, Priority as KvPriority, ReplicaReadType, RequestSource, RequestType,
    StoreLabel, StoreType, UnaryCallContext,
};
pub use tiflash_replica_read::{
    TiFlashReplicaRead, ALL_REPLICAS, CLOSEST_ADAPTIVE, CLOSEST_REPLICAS,
    MAX_REMOTE_READ_COUNT_PER_NODE_FOR_CLOSEST_REPLICAS,
};
pub use transport::{
    TransportBinding, TransportRequest, TransportRequestError, TransportRequestState,
};
pub use warning::{Warning, WarningClass, WarningCollector, WarningLevel};

#[cfg(test)]
mod tests;
