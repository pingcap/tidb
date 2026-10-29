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

mod chblock;
mod chunk_decode;
mod context;
pub mod select_result_stats;
mod distsql_runtime;
mod envelope;
mod execution;
mod kv_request;
pub mod query_runtime;
mod request;
mod request_builder;
mod select_iter;
pub mod select_response;
mod signed_handle_range;
mod table_handle_ranges;
mod transport;

pub use chblock::{decode_ch_block, RawChBlockChunk};
pub use chunk_decode::{
    decode_chunk, decode_response_chunks, decode_select_response, ChunkDecodeError, RawChunk,
    RawChunkRow, RawColumnarChunk, TypedColumnarChunk,
};
pub use context::{DistSqlContext, PagingConfig, RequestContext, SessionContext, DEFAULT_DIST_SQL_CONCURRENCY};
use tidb_mysql::Priority;
pub use distsql_runtime::{analyze_request_source, analyze_result_metadata, checksum_result_metadata, mpp_result_metadata, select_result_metadata, select_with_runtime_stats, system_endian, EncodeType, SelectInput, SelectResultMetadata, SelectResultRuntimeStats, SystemEndian, ANALYZE_RESULT_LABEL, CHECKSUM_RESULT_LABEL, DAG_RESULT_LABEL, GENERAL_SQL_TYPE, INTERNAL_SQL_TYPE, INTERNAL_TXN_STATS_SOURCE, MPP_RESULT_LABEL};
pub use envelope::{ExecutorKind, ExecutorShape, RequestEnvelope, ESTIMATED_REGION_ROW_COUNT};
pub use execution::{CpuUsage, ExecutionState, KillHandle};
use tidb_txnkv::CancelHandle;
pub use kv_request::{request_from_context, RequestKeyRange, RequestKeyRanges, DC_LABEL_KEY, GLOBAL_REPLICA_SCOPE};
use tidb_txnkv::PartitionIdAndRanges;
use tidb_txnkv::KvRequestMetadata;
pub use query_runtime::{checksum, select_with_runtime_stats_over_client, InjectedQueryRuntime, QueryDispatch, QueryResponse, QueryResultContext};
pub use request::{ReadRequestBuilder, ReadRequestMetadata};
pub use request_builder::{
    build_table_ranges, common_handle_ranges_to_kv_ranges, index_ranges_to_kv_ranges,
    split_ranges_across_int64_boundary, table_ranges_to_kv_ranges,
    KvRequestBuildError, KvRequestBuilder, RequestBuilder, TableIndexRangeSpec, TableRangeSpec,
};
pub use select_iter::SelectResultRow;
pub use signed_handle_range::{signed_handle_ranges_to_kv_ranges, SignedHandleRange};
pub use table_handle_ranges::table_handles_to_kv_ranges;
use tidb_txnkv::IsolationLevel;
use tidb_tiflash::ReplicaRead;
use tidb_txnkv::ReplicaReadType;
use tidb_txnkv::RequestSource;
use tidb_txnkv::RequestType;
use tidb_txnkv::StoreLabel;
use tidb_txnkv::StoreType;
pub use tidb_txnkv::Priority as KvPriority;
pub use tidb_paging::calculate_seek_cnt as calculate_seek_count;
pub use tidb_paging::THRESHOLD as PAGING_THRESHOLD;
pub use transport::RegionTaskRequest;
use tidb_txnkv::TransportRequest;
use tidb_context::{Warning, WarningCollector};

pub mod remote_scan;
#[cfg(test)]
mod tests;
