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

//! One-shot TiKV gRPC health checking.
//!
//! This leaf creates an independent foreground Health/Check channel, maps the
//! wire result into shared store liveness, and returns. It deliberately owns no
//! retry, background loop, store mutation, TLS policy, or channel-pool close.

use std::time::Duration;

use prost::Message;
use tonic::transport::Endpoint;

use crate::region::StoreLiveness;

use super::tonic_coprocessor::RawProtobufCodec;

/// Pinned client-go's default foreground store-liveness timeout.
pub const DEFAULT_STORE_LIVENESS_TIMEOUT: Duration = Duration::from_secs(1);

const HEALTH_CHECK_PATH: &str = "/grpc.health.v1.Health/Check";
const HEALTH_UNKNOWN: i32 = 0;
const HEALTH_SERVING: i32 = 1;
const HEALTH_SERVICE_UNKNOWN: i32 = 3;

#[derive(Clone, PartialEq, Message)]
struct HealthCheckRequest {
    #[prost(string, tag = "1")]
    service: String,
}

#[derive(Clone, Copy, PartialEq, Message)]
struct HealthCheckResponse {
    #[prost(enumeration = "ServingStatus", tag = "1")]
    status: i32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
#[repr(i32)]
enum ServingStatus {
    Unknown = 0,
    Serving = 1,
    NotServing = 2,
    ServiceUnknown = 3,
}

pub(super) fn check_liveness(
    runtime: &tokio::runtime::Runtime,
    address: &str,
    timeout: Duration,
) -> StoreLiveness {
    if timeout.is_zero() {
        return StoreLiveness::Unreachable;
    }
    let uri = if address.contains("://") {
        address.to_owned()
    } else {
        format!("http://{address}")
    };
    let Ok(endpoint) = Endpoint::from_shared(uri) else {
        return StoreLiveness::Unreachable;
    };
    let channel = {
        let _runtime = runtime.enter();
        endpoint.connect_lazy()
    };
    let request = HealthCheckRequest {
        service: String::new(),
    }
    .encode_to_vec();
    let path = tonic::codegen::http::uri::PathAndQuery::from_static(HEALTH_CHECK_PATH);
    let result = runtime.block_on(async {
        tokio::time::timeout(timeout, async {
            let mut client = tonic::client::Grpc::new(channel);
            client.ready().await.map_err(|_| ())?;
            client
                .unary(tonic::Request::new(request), path, RawProtobufCodec)
                .await
                .map_err(|_| ())
        })
        .await
    });
    let Ok(Ok(response)) = result else {
        return StoreLiveness::Unreachable;
    };
    let Ok(response) = HealthCheckResponse::decode(response.into_inner().as_slice()) else {
        return StoreLiveness::Unreachable;
    };
    match response.status {
        HEALTH_SERVING => StoreLiveness::Reachable,
        HEALTH_UNKNOWN | HEALTH_SERVICE_UNKNOWN => StoreLiveness::Unknown,
        _ => StoreLiveness::Unreachable,
    }
}
