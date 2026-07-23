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

//! Wire-ready unary TiKV request/response contract without a transport.

use prost::Message;
use tidb_proto::{
    CoprocessorResponse, KvrpcContext, KvrpcPeer, KvrpcRegionEpoch, KvrpcRequestOrigin,
    KvrpcResourceControlContext,
};
use tidb_txnkv::region::{LeaderRequest, ReplicaReadMode};
use tidb_txnkv::{
    endpoint_type, inject_source_stmt, map_replica_read_type, ClientReplicaReadType, EndpointType,
    TraceInfo,
};

use super::{CopReadTaskResponse, PreparedCopReadTask};
use crate::{KvRequestMetadata, RequestSource};

/// Fully encoded unary coprocessor request plus wrapper-owned send metadata.
#[derive(Clone, Debug, PartialEq)]
pub struct TikvUnaryRequest {
    /// Endpoint selected by TiDB's store-type mapping.
    pub endpoint: EndpointType,
    /// Client-go replica-read selection.
    pub replica_read_type: ClientReplicaReadType,
    /// Whether follower-capable replica routing is active.
    pub replica_read: bool,
    /// Whether this is a stale-read attempt.
    pub stale_read: bool,
    /// Input request source before client-go appends retry metadata.
    pub input_request_source: String,
    /// Caller-supplied predicted read bytes for resource control.
    pub predicted_read_bytes: u64,
    /// Replica scope retained by the request wrapper.
    pub read_replica_scope: String,
    /// Transaction scope retained by the request wrapper.
    pub txn_scope: String,
    /// Task-local timeout override; `None` means the process-global default.
    pub timeout_override_ms: Option<u64>,
    /// Exact protobuf context encoded into the request body.
    pub context: KvrpcContext,
    /// Exact encoded `coprocessor.Request` bytes.
    pub encoded_request: Vec<u8>,
}

/// Constructs the complete unary request owned before network dispatch.
///
/// `predicted_read_bytes` is supplied by the runtime's shared EMA owner. This
/// function does not choose an address, query PD/RegionCache, allocate a
/// replica seed, send bytes, or schedule retries.
#[must_use]
pub fn build_tikv_unary_request(
    prepared: &PreparedCopReadTask,
    metadata: &KvRequestMetadata,
    predicted_read_bytes: u64,
    trace: Option<&TraceInfo>,
    cluster_id: u64,
) -> TikvUnaryRequest {
    build_tikv_unary_request_inner(
        prepared,
        metadata,
        predicted_read_bytes,
        trace,
        cluster_id,
        None,
    )
}

/// Constructs a request whose peer and leader flags come from the selection
/// made immediately before dispatch.
#[must_use]
pub fn build_tikv_unary_request_for_dispatch(
    prepared: &PreparedCopReadTask,
    metadata: &KvRequestMetadata,
    predicted_read_bytes: u64,
    trace: Option<&TraceInfo>,
    cluster_id: u64,
    selected: &LeaderRequest,
) -> TikvUnaryRequest {
    build_tikv_unary_request_inner(
        prepared,
        metadata,
        predicted_read_bytes,
        trace,
        cluster_id,
        Some(selected),
    )
}

fn build_tikv_unary_request_inner(
    prepared: &PreparedCopReadTask,
    metadata: &KvRequestMetadata,
    predicted_read_bytes: u64,
    trace: Option<&TraceInfo>,
    cluster_id: u64,
    selected: Option<&LeaderRequest>,
) -> TikvUnaryRequest {
    let task = prepared.task();
    let mut replica_read_type = map_replica_read_type(metadata.replica_read.raw());
    let mut replica_read = replica_read_type != ClientReplicaReadType::Leader;
    let mut stale_read = metadata.is_staleness;
    if stale_read {
        replica_read_type = ClientReplicaReadType::Mixed;
        replica_read = false;
    }
    if let Some(selected) = selected {
        replica_read_type = match selected.read_mode {
            ReplicaReadMode::Leader => ClientReplicaReadType::Leader,
            ReplicaReadMode::Follower => ClientReplicaReadType::Follower,
            ReplicaReadMode::Mixed => ClientReplicaReadType::Mixed,
            ReplicaReadMode::Learner => ClientReplicaReadType::Learner,
            ReplicaReadMode::PreferLeader => ClientReplicaReadType::PreferLeader,
        };
        replica_read = selected.replica_read;
        stale_read = selected.stale_read;
    }

    let input_request_source = request_source(&metadata.request_source);
    let resource_group_tag = metadata
        .resource_group_tagger
        .as_ref()
        .map(|tagger| {
            let key = task
                .ranges
                .first()
                .map_or(&[][..], |range| range.start_key.as_slice());
            tagger.encode_tag_with_key(key)
        })
        .unwrap_or_default();
    let mut context = KvrpcContext {
        region_id: task.region_id,
        region_epoch: task.region_epoch.map(|epoch| KvrpcRegionEpoch {
            conf_ver: epoch.conf_ver,
            version: epoch.version,
        }),
        peer: selected.map_or_else(
            || {
                task.peer.map(|peer| KvrpcPeer {
                    id: peer.id,
                    store_id: peer.store_id,
                    role: peer.role,
                    is_witness: peer.is_witness,
                })
            },
            |selected| {
                Some(KvrpcPeer {
                    id: selected.attempt.peer_id,
                    store_id: selected.attempt.store_id,
                    role: selected.role.as_i32(),
                    is_witness: selected.is_witness,
                })
            },
        ),
        priority: metadata.priority.raw() as i32,
        isolation_level: metadata.isolation_level.raw() as i32,
        not_fill_cache: metadata.not_fill_cache,
        record_time_stat: true,
        record_scan_stat: true,
        replica_read,
        task_id: metadata.task_id,
        stale_read,
        resource_group_tag,
        request_source: input_request_source.clone(),
        busy_threshold_ms: task.store_busy_threshold_ms as u32,
        resource_control_context: Some(KvrpcResourceControlContext {
            resource_group_name: metadata.resource_group_name.clone(),
            override_priority: 0,
        }),
        // cmd/tidb-server sets client-go's process default to TiDB, and
        // tikvrpc.NewRequest fills an unknown context from that default before
        // the request is attached. This builder is TiDB-owned, so encode the
        // resulting wire value directly rather than leaking the pre-fill zero.
        request_origin: KvrpcRequestOrigin::TiDb as i32,
        buckets_version: task.buckets_version,
        cluster_id,
        ..KvrpcContext::default()
    };
    inject_source_stmt(&mut context, trace);

    // RegionRequestSender/client-go keeps Context on the request wrapper and
    // RPCClient attaches it to the command body immediately before the send.
    // Preserve that single authority so a real transport cannot observe two
    // independently mutable context copies.
    let request = prepared.request().clone();
    TikvUnaryRequest {
        // PreparedCopReadTask is produced only after the coordinator rejects
        // every non-TiKV store. TiFlash resource-group clearing and
        // disaggregated endpoint selection therefore remain outside this API.
        endpoint: endpoint_type(metadata.store_type.raw(), false),
        replica_read_type,
        replica_read,
        stale_read,
        input_request_source,
        predicted_read_bytes,
        read_replica_scope: metadata.read_replica_scope.clone(),
        txn_scope: metadata.txn_scope.clone(),
        timeout_override_ms: (task.tikv_client_read_timeout_ms > 0)
            .then_some(task.tikv_client_read_timeout_ms),
        context,
        encoded_request: request.encode_to_vec(),
    }
}

/// Decodes all response bytes before returning a coordinator envelope.
///
/// Decode failure has no coordinator reference and therefore cannot mutate
/// task/cache/paging state. Error precedence matches Go's unary response path:
/// region, lock, other error, then batch or ordinary success.
pub fn decode_tikv_unary_response(
    raw_response: &[u8],
) -> Result<CopReadTaskResponse, prost::DecodeError> {
    let response = CoprocessorResponse::decode(raw_response)?;
    if response.region_error.is_some() {
        return Ok(CopReadTaskResponse::region_error(response));
    }
    if response.locked.is_some() {
        return Ok(CopReadTaskResponse::lock_error(response));
    }
    if !response.other_error.is_empty() {
        let message = response.other_error.clone();
        return Ok(CopReadTaskResponse::other_error(response, message));
    }
    if !response.batch_responses.is_empty() {
        return Ok(CopReadTaskResponse::batch(response));
    }
    Ok(CopReadTaskResponse::success(response))
}

fn request_source(source: &RequestSource) -> String {
    if source.source_type.is_empty() && source.explicit_source_type.is_empty() {
        return "unknown".to_owned();
    }
    let origin = if source.internal {
        "internal"
    } else {
        "external"
    };
    let source_type = if source.source_type.is_empty() {
        "unknown"
    } else {
        source.source_type.as_str()
    };
    let mut result = format!("{origin}_{source_type}");
    if !source.explicit_source_type.is_empty() && source.explicit_source_type != source.source_type
    {
        result.push('_');
        result.push_str(&source.explicit_source_type);
    }
    result
}
