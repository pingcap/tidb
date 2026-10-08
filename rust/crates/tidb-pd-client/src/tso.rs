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

use std::time::{Duration, Instant};

use tidb_proto::pdpb;
use tikv_client::pd_service_discovery::{TsoRoute, TsoStreamSet};
use tokio::sync::watch;
use tonic::transport::Channel;

use crate::{PdClientError, PdOperation};
pub(crate) use tikv_client::pd_tso_batch::{TimestampTracker, TsoBatch};

pub(crate) fn batch_error(error: tikv_client::pd_tso_batch::BatchError) -> PdClientError {
    PdClientError::InvalidTopology {
        kind: error.kind,
        message: error.message,
    }
}

pub(crate) fn request_batch(
    streams: &mut TsoStreamSet,
    runtime: &tokio::runtime::Runtime,
    channel: Channel,
    route: TsoRoute,
    forwarding: &tikv_client::pd_service_discovery::TsoForwarding,
    cluster_id: u64,
    deadline: Instant,
    shutdown: &watch::Receiver<bool>,
    routes: &watch::Sender<Vec<TsoRoute>>,
    count: u32,
) -> Result<TsoBatch, PdClientError> {
    let endpoint = route.endpoint.clone();
    let timeout = remaining(deadline, &endpoint)?;
    let request = pdpb::TsoRequest {
        header: Some(pdpb::RequestHeader {
            cluster_id,
            ..Default::default()
        }),
        count,
        dc_location: String::new(),
    };
    let mut cancellation = shutdown.clone();
    let mut routes = routes.subscribe();
    runtime.block_on(async {
        tokio::select! {
            biased;
            () = shutdown_requested(&mut cancellation) => Err(PdClientError::Closed),
            () = route_retired(&mut routes, &route) => Err(map_status(&endpoint, tonic::Status::cancelled("TSO route retired"))),
            response = tokio::time::timeout(timeout, streams.request(route.clone(), channel, request, forwarding)) => {
                match response {
                    Ok(Ok(response)) => TsoBatch::from_response(&response, count).map_err(batch_error),
                    Ok(Err(status)) => Err(map_status(&endpoint, status)),
                    Err(_) => Err(timeout_error(&endpoint, timeout)),
                }
            },
        }
    })
}

async fn route_retired(routes: &mut watch::Receiver<Vec<TsoRoute>>, route: &TsoRoute) {
    loop {
        if !routes.borrow_and_update().contains(route) {
            return;
        }
        if routes.changed().await.is_err() {
            return;
        }
    }
}

async fn shutdown_requested(shutdown: &mut watch::Receiver<bool>) {
    if *shutdown.borrow() {
        return;
    }
    let _ = shutdown.changed().await;
}

pub(crate) fn retry_delay(_retry_index: usize) -> Duration {
    Duration::from_millis(500)
}

pub(crate) fn remaining(deadline: Instant, endpoint: &str) -> Result<Duration, PdClientError> {
    deadline
        .checked_duration_since(Instant::now())
        .filter(|remaining| !remaining.is_zero())
        .ok_or_else(|| timeout_error(endpoint, Duration::ZERO))
}

fn map_status(endpoint: &str, status: tonic::Status) -> PdClientError {
    PdClientError::Transport {
        operation: PdOperation::Tso,
        endpoint: endpoint.to_owned(),
        code: format!("{:?}", status.code()),
        message: status.message().to_owned(),
    }
}

pub(crate) fn timeout_error(endpoint: &str, timeout: Duration) -> PdClientError {
    PdClientError::Timeout {
        operation: PdOperation::Tso,
        endpoint: endpoint.to_owned(),
        timeout_ms: u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX),
    }
}
