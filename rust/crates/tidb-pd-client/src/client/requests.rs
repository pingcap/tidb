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

//! One PD gRPC method per function, plus the request/response header contract
//! every one of them owes.
//!
//! Go boundary: the request wrappers in `pd/client`'s `client.go`
//! (`GetMembers`, `GetRegion`, `GetPrevRegion`, `GetRegionByID`, `ScanRegions`,
//! `BatchScanRegions`, `GetStore`) and `gc_client.go` (`GetGCState`). Each
//! builds one request, awaits one response, and rejects a response whose header
//! names a different cluster or reports an error — no retry and no failover
//! decision lives here.

use std::time::Duration;

use tidb_proto::pdpb;
use tokio::sync::watch;

use crate::{PdClientError, PdGcState, PdOperation, PdRegion, PdSplitAndScatterRegions, PdStore};

use super::failover::{tonic_client, PdChannelCache};
use super::topology::{
    invalid_topology, project_extended_region, project_member_set, project_region,
    project_scan_regions, project_store,
};
use super::{block_on_rpc, PdMemberObservation, RpcCompletion, RpcControl};

pub(super) fn get_members(
    runtime: &tokio::runtime::Runtime,
    clients: &mut PdChannelCache,
    endpoint: &str,
    timeout: Duration,
    shutdown: &watch::Receiver<bool>,
    expected_cluster_id: Option<u64>,
) -> Result<PdMemberObservation, PdClientError> {
    let client = tonic_client(runtime, clients, endpoint)?;
    let response = block_on_rpc(
        runtime,
        timeout,
        shutdown,
        client.get_members(pdpb::GetMembersRequest { header: None }),
    );
    let response = map_rpc_result(response, PdOperation::GetMembers, endpoint, timeout)?;
    let response = response.into_inner();
    let header = response
        .header
        .as_ref()
        .ok_or(PdClientError::MissingHeader(PdOperation::GetMembers))?;
    reject_header_error(PdOperation::GetMembers, header)?;
    if header.cluster_id == 0 {
        return Err(PdClientError::ZeroClusterId);
    }
    if let Some(expected) = expected_cluster_id {
        if header.cluster_id != expected {
            return Err(PdClientError::ClusterMismatch {
                operation: PdOperation::GetMembers,
                expected,
                actual: header.cluster_id,
            });
        }
    }
    let cluster_id = header.cluster_id;
    Ok(PdMemberObservation {
        cluster_id,
        projected: project_member_set(response),
    })
}

pub(super) fn get_region(
    runtime: &tokio::runtime::Runtime,
    clients: &mut PdChannelCache,
    endpoint: &str,
    control: RpcControl<'_>,
    cluster_id: u64,
    encoded_key: &[u8],
    need_buckets: bool,
) -> Result<PdRegion, PdClientError> {
    let client = tonic_client(runtime, clients, endpoint)?;
    let response = block_on_rpc(
        runtime,
        control.timeout,
        control.shutdown,
        client.get_region(pdpb::GetRegionRequest {
            header: Some(request_header(cluster_id)),
            region_key: encoded_key.to_vec(),
            need_buckets,
        }),
    );
    let response =
        map_rpc_result(response, PdOperation::GetRegion, endpoint, control.timeout)?.into_inner();
    validate_response_header(PdOperation::GetRegion, response.header.as_ref(), cluster_id)?;
    project_region(response, need_buckets)
}

pub(super) fn get_prev_region(
    runtime: &tokio::runtime::Runtime,
    clients: &mut PdChannelCache,
    endpoint: &str,
    control: RpcControl<'_>,
    cluster_id: u64,
    encoded_key: &[u8],
    need_buckets: bool,
) -> Result<PdRegion, PdClientError> {
    let client = tonic_client(runtime, clients, endpoint)?;
    let response = block_on_rpc(
        runtime,
        control.timeout,
        control.shutdown,
        client.get_prev_region(pdpb::GetRegionRequest {
            header: Some(request_header(cluster_id)),
            region_key: encoded_key.to_vec(),
            need_buckets,
        }),
    );
    let response = map_rpc_result(
        response,
        PdOperation::GetPrevRegion,
        endpoint,
        control.timeout,
    )?
    .into_inner();
    validate_response_header(
        PdOperation::GetPrevRegion,
        response.header.as_ref(),
        cluster_id,
    )?;
    project_region(response, need_buckets)
}

pub(super) fn get_region_by_id(
    runtime: &tokio::runtime::Runtime,
    clients: &mut PdChannelCache,
    endpoint: &str,
    control: RpcControl<'_>,
    cluster_id: u64,
    region_id: u64,
    need_buckets: bool,
) -> Result<PdRegion, PdClientError> {
    let client = tonic_client(runtime, clients, endpoint)?;
    let response = block_on_rpc(
        runtime,
        control.timeout,
        control.shutdown,
        client.get_region_by_id(pdpb::GetRegionByIdRequest {
            header: Some(request_header(cluster_id)),
            region_id,
            need_buckets,
        }),
    );
    let response = map_rpc_result(
        response,
        PdOperation::GetRegionById,
        endpoint,
        control.timeout,
    )?
    .into_inner();
    validate_response_header(
        PdOperation::GetRegionById,
        response.header.as_ref(),
        cluster_id,
    )?;
    project_region(response, need_buckets)
}

pub(super) fn scan_regions(
    runtime: &tokio::runtime::Runtime,
    clients: &mut PdChannelCache,
    endpoint: &str,
    timeout: Duration,
    shutdown: &watch::Receiver<bool>,
    cluster_id: u64,
    request: &pdpb::ScanRegionsRequest,
) -> Result<Vec<PdRegion>, PdClientError> {
    let client = tonic_client(runtime, clients, endpoint)?;
    let mut request = request.clone();
    request.header = Some(request_header(cluster_id));
    let response = block_on_rpc(runtime, timeout, shutdown, client.scan_regions(request));
    let response =
        map_rpc_result(response, PdOperation::ScanRegions, endpoint, timeout)?.into_inner();
    validate_response_header(
        PdOperation::ScanRegions,
        response.header.as_ref(),
        cluster_id,
    )?;
    project_scan_regions(response)
}

pub(super) fn batch_scan_regions(
    runtime: &tokio::runtime::Runtime,
    clients: &mut PdChannelCache,
    endpoint: &str,
    timeout: Duration,
    shutdown: &watch::Receiver<bool>,
    cluster_id: u64,
    request: &pdpb::BatchScanRegionsRequest,
) -> Result<Vec<PdRegion>, PdClientError> {
    let client = tonic_client(runtime, clients, endpoint)?;
    let need_buckets = request.need_buckets;
    let mut request = request.clone();
    request.header = Some(request_header(cluster_id));
    let response = block_on_rpc(
        runtime,
        timeout,
        shutdown,
        client.batch_scan_regions(request),
    );
    let response =
        map_rpc_result(response, PdOperation::BatchScanRegions, endpoint, timeout)?.into_inner();
    validate_response_header(
        PdOperation::BatchScanRegions,
        response.header.as_ref(),
        cluster_id,
    )?;
    response
        .regions
        .into_iter()
        .map(|region| project_extended_region(region, need_buckets))
        .collect()
}

pub(super) fn get_store(
    runtime: &tokio::runtime::Runtime,
    clients: &mut PdChannelCache,
    endpoint: &str,
    timeout: Duration,
    shutdown: &watch::Receiver<bool>,
    cluster_id: u64,
    store_id: u64,
) -> Result<Option<PdStore>, PdClientError> {
    let client = tonic_client(runtime, clients, endpoint)?;
    let response = block_on_rpc(
        runtime,
        timeout,
        shutdown,
        client.get_store(pdpb::GetStoreRequest {
            header: Some(request_header(cluster_id)),
            store_id,
        }),
    );
    let response = map_rpc_result(response, PdOperation::GetStore, endpoint, timeout)?.into_inner();
    if store_is_removed(response.header.as_ref(), cluster_id)? {
        return Ok(None);
    }
    project_store(response.store, store_id)
}

pub(super) fn get_gc_state(
    runtime: &tokio::runtime::Runtime,
    clients: &mut PdChannelCache,
    endpoint: &str,
    timeout: Duration,
    shutdown: &watch::Receiver<bool>,
    cluster_id: u64,
    keyspace_id: Option<u32>,
) -> Result<PdGcState, PdClientError> {
    let client = tonic_client(runtime, clients, endpoint)?;
    let response = block_on_rpc(
        runtime,
        timeout,
        shutdown,
        client.get_gc_state(pdpb::GetGcStateRequest {
            header: Some(request_header(cluster_id)),
            keyspace_scope: keyspace_id.map(|keyspace_id| pdpb::KeyspaceScope { keyspace_id }),
            // The barriers describe which components still hold GC back. A
            // reading client only needs the resulting txn safe point.
            exclude_gc_barriers: true,
        }),
    );
    let response =
        map_rpc_result(response, PdOperation::GetGcState, endpoint, timeout)?.into_inner();
    validate_response_header(
        PdOperation::GetGcState,
        response.header.as_ref(),
        cluster_id,
    )?;
    let state = response.gc_state.ok_or_else(|| {
        invalid_topology("missing_gc_state", "GetGCState omitted the GC state body")
    })?;
    Ok(PdGcState {
        is_keyspace_level_gc: state.is_keyspace_level_gc,
        txn_safe_point: state.txn_safe_point,
        gc_safe_point: state.gc_safe_point,
    })
}

pub(super) fn split_and_scatter_regions(
    runtime: &tokio::runtime::Runtime,
    clients: &mut PdChannelCache,
    endpoint: &str,
    timeout: Duration,
    shutdown: &watch::Receiver<bool>,
    cluster_id: u64,
    split_keys: &[Vec<u8>],
    group: &str,
) -> Result<PdSplitAndScatterRegions, PdClientError> {
    let client = tonic_client(runtime, clients, endpoint)?;
    let response = block_on_rpc(
        runtime,
        timeout,
        shutdown,
        client.split_and_scatter_regions(pdpb::SplitAndScatterRegionsRequest {
            header: Some(request_header(cluster_id)),
            split_keys: split_keys.to_vec(),
            group: group.to_owned(),
            // Go pd/client's RegionsOp has a zero default, which delegates
            // the retry policy to PD.
            retry_limit: 0,
        }),
    );
    let response = map_rpc_result(
        response,
        PdOperation::SplitAndScatterRegions,
        endpoint,
        timeout,
    )?
    .into_inner();
    validate_response_header(
        PdOperation::SplitAndScatterRegions,
        response.header.as_ref(),
        cluster_id,
    )?;
    Ok(PdSplitAndScatterRegions {
        split_finished_percentage: response.split_finished_percentage,
        scatter_finished_percentage: response.scatter_finished_percentage,
        region_ids: response.regions_id,
    })
}

pub(super) fn is_region_scattering(
    runtime: &tokio::runtime::Runtime,
    clients: &mut PdChannelCache,
    endpoint: &str,
    timeout: Duration,
    shutdown: &watch::Receiver<bool>,
    cluster_id: u64,
    region_id: u64,
) -> Result<bool, PdClientError> {
    let client = tonic_client(runtime, clients, endpoint)?;
    let response = block_on_rpc(
        runtime,
        timeout,
        shutdown,
        client.get_operator(pdpb::GetOperatorRequest {
            header: Some(request_header(cluster_id)),
            region_id,
        }),
    );
    let response =
        map_rpc_result(response, PdOperation::GetOperator, endpoint, timeout)?.into_inner();
    // client-go tikv/split_region.go:WaitScatterRegionFinish treats any
    // response that is not a running scatter operator as completion before it
    // inspects the response-header error. In particular, PD uses an empty
    // operator plus REGION_NOT_FOUND after the operator has disappeared.
    let is_scattering = response.desc.as_slice() == b"scatter-region"
        && response.status == pdpb::OperatorStatus::Running as i32;
    if !is_scattering {
        return Ok(false);
    }
    validate_response_header(
        PdOperation::GetOperator,
        response.header.as_ref(),
        cluster_id,
    )?;
    Ok(true)
}

pub(super) fn map_rpc_result<T>(
    result: RpcCompletion<T>,
    operation: PdOperation,
    endpoint: &str,
    timeout: Duration,
) -> Result<tonic::Response<T>, PdClientError> {
    match result {
        RpcCompletion::Completed(Ok(response)) => Ok(response),
        RpcCompletion::Completed(Err(status)) if status.code() == tonic::Code::DeadlineExceeded => {
            Err(timeout_error(operation, endpoint, timeout))
        }
        RpcCompletion::Completed(Err(status)) => Err(PdClientError::Transport {
            operation,
            endpoint: endpoint.to_owned(),
            code: format!("{:?}", status.code()),
            message: status.message().to_owned(),
        }),
        RpcCompletion::Timeout => Err(timeout_error(operation, endpoint, timeout)),
        RpcCompletion::Shutdown => Err(PdClientError::Closed),
    }
}

pub(super) fn timeout_error(
    operation: PdOperation,
    endpoint: &str,
    timeout: Duration,
) -> PdClientError {
    PdClientError::Timeout {
        operation,
        endpoint: endpoint.to_owned(),
        timeout_ms: u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX),
    }
}

pub(super) fn request_header(cluster_id: u64) -> pdpb::RequestHeader {
    pdpb::RequestHeader {
        cluster_id,
        sender_id: 0,
        caller_id: String::new(),
        caller_component: "codec-pd-client".to_owned(),
    }
}

pub(super) fn validate_response_header(
    operation: PdOperation,
    header: Option<&pdpb::ResponseHeader>,
    cluster_id: u64,
) -> Result<(), PdClientError> {
    let header = header.ok_or(PdClientError::MissingHeader(operation))?;
    reject_header_error(operation, header)?;
    if header.cluster_id != cluster_id {
        return Err(PdClientError::ClusterMismatch {
            operation,
            expected: cluster_id,
            actual: header.cluster_id,
        });
    }
    Ok(())
}

pub(super) fn reject_header_error(
    operation: PdOperation,
    header: &pdpb::ResponseHeader,
) -> Result<(), PdClientError> {
    if let Some(error) = &header.error {
        return Err(PdClientError::HeaderError {
            operation,
            error_type: error.r#type,
            message: error.message.clone(),
        });
    }
    Ok(())
}

pub(super) fn store_is_removed(
    header: Option<&pdpb::ResponseHeader>,
    cluster_id: u64,
) -> Result<bool, PdClientError> {
    let header = header.ok_or(PdClientError::MissingHeader(PdOperation::GetStore))?;
    if header.cluster_id != cluster_id {
        return Err(PdClientError::ClusterMismatch {
            operation: PdOperation::GetStore,
            expected: cluster_id,
            actual: header.cluster_id,
        });
    }
    if let Some(error) = &header.error {
        let store_not_found = error.r#type == pdpb::ErrorType::StoreTombstone as i32
            || (error.message.contains("invalid store ID") && error.message.contains("not found"));
        if store_not_found {
            return Ok(true);
        }
        reject_header_error(PdOperation::GetStore, header)?;
    }
    Ok(false)
}
