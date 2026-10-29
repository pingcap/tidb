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

//! Dependency-closed `kv.Request` metadata from `pkg/kv/kv.go`.
//!
//! [`KvRequestBuilder`] stops at the same pre-transport boundary as Go's
//! `distsql.RequestBuilder.Build`: it normalizes defaults, initializes the
//! key-range envelope, applies closest-read labels, and projects DAG limit
//! metadata. It deliberately does not marshal protobuf data, route regions,
//! acquire a TiKV client, or issue RPCs.

use crate::{DistSqlContext, ReadRequestMetadata};
pub use tidb_txnkv::KeyRange as RequestKeyRange;
pub use tidb_txnkv::PartitionedKeyRanges as RequestKeyRanges;

/// Go's `kv.GlobalReplicaScope` / client-go `oracle.GlobalTxnScope` value.
pub const GLOBAL_REPLICA_SCOPE: &str = "global";
/// Go placement's data-center label key used for closest reads.
pub const DC_LABEL_KEY: &str = "zone";

use tidb_txnkv::KvRequestMetadata;

/// Projects the source context fields into a new request metadata value.
#[must_use]
pub fn request_from_context(context: &DistSqlContext) -> KvRequestMetadata {
    let mut request = KvRequestMetadata::default();
    apply_session_metadata(&mut request, ReadRequestMetadata::from_context(context));
    request.connection_id = context.request.session.connection_id;
    request.connection_alias = context.request.session.alias.clone();
    request
}

pub(crate) fn apply_session_metadata(
    request: &mut KvRequestMetadata,
    session: ReadRequestMetadata,
) {
    request.concurrency = session.concurrency as isize;
    request.isolation_level = session.isolation_level;
    request.priority = session.priority;
    request.not_fill_cache = session.not_fill_cache;
    request.task_id = session.task_id;
    request.replica_read = session.replica_read;
    request.tiflash_replica_read = session.tiflash_replica_read;
    request.paging = tidb_txnkv::Paging {
        enabled: session.paging.enabled,
        min_size: session.paging.min_size,
        max_size: session.paging.max_size,
        size_bytes: session.paging.size_bytes,
    };
    request.request_source = session.request_source;
    request.store_batch_size = session.store_batch_size as isize;
    request.allow_batch_task_data_merge = session.allow_batch_task_data_merge;
    request.execute_batch_tasks_serially = session.execute_batch_tasks_serially;
    request.resource_group_name = session.resource_group_name;
    request.store_busy_threshold_ns = session.store_busy_threshold_ns;
    request.tikv_client_read_timeout_ms = session.tikv_client_read_timeout_ms;
    request.max_execution_time_ms = session.max_execution_time_ms;
    request.max_keys_read = session.max_keys_read;
    request.max_keys_read_counter = session.max_keys_read_counter;
    request.query_cop_store_limiter = session.query_cop_store_limiter;
}
