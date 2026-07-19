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

#![allow(missing_docs)]

use tidb_distsql::cop_paging::{classify_transport_failure, TransportFailureAction};
use tidb_txnkv::{
    DirectUnaryClientError, DirectUnaryConnectionError, DirectUnaryGrpcCode,
    DirectUnaryTransportClass,
};

fn connection(
    class: DirectUnaryTransportClass,
    grpc_code: Option<DirectUnaryGrpcCode>,
) -> DirectUnaryClientError {
    let error = match class {
        DirectUnaryTransportClass::Connection => {
            DirectUnaryConnectionError::connection("tikv-1:20160", 17, "scripted".to_owned())
        }
        DirectUnaryTransportClass::LocalDeadline => {
            DirectUnaryConnectionError::local_deadline("tikv-1:20160", 17, "scripted".to_owned())
        }
        DirectUnaryTransportClass::RemoteGrpc => DirectUnaryConnectionError::remote_grpc(
            "tikv-1:20160",
            17,
            grpc_code.expect("remote gRPC scripts require a code"),
            "scripted".to_owned(),
        ),
        DirectUnaryTransportClass::CallerCancelled => {
            panic!("caller cancellation has no selected connection")
        }
    };
    DirectUnaryClientError::Connection(error)
}

#[test]
fn remote_canceled_is_the_only_exact_generation_close() {
    let canceled = connection(
        DirectUnaryTransportClass::RemoteGrpc,
        Some(DirectUnaryGrpcCode::Canceled),
    );
    let unavailable = connection(
        DirectUnaryTransportClass::RemoteGrpc,
        Some(DirectUnaryGrpcCode::Unavailable),
    );
    let local = connection(DirectUnaryTransportClass::Connection, None);

    assert!(matches!(
        classify_transport_failure(&canceled),
        TransportFailureAction::RetryConnection {
            connection,
            close_generation: true,
        } if connection.address() == "tikv-1:20160" && connection.version() == 17
    ));
    assert!(matches!(
        classify_transport_failure(&unavailable),
        TransportFailureAction::RetryConnection {
            close_generation: false,
            ..
        }
    ));
    assert!(matches!(
        classify_transport_failure(&local),
        TransportFailureAction::RetryConnection {
            close_generation: false,
            ..
        }
    ));
}

#[test]
fn errors_without_an_observed_connection_are_terminal() {
    for error in [
        DirectUnaryClientError::CallerCancelled,
        DirectUnaryClientError::Closed,
        DirectUnaryClientError::InvalidAddress {
            address: "not an address".to_owned(),
            message: "invalid".to_owned(),
        },
        DirectUnaryClientError::InvalidRequest("invalid".to_owned()),
        DirectUnaryClientError::AdmissionBusy {
            address: "tikv-1:20160".to_owned(),
        },
        DirectUnaryClientError::Runtime("runtime".to_owned()),
    ] {
        assert_eq!(
            classify_transport_failure(&error),
            TransportFailureAction::Terminal,
            "{}",
            error.kind()
        );
    }
}

#[test]
fn local_deadline_retains_connection_identity_for_liveness() {
    let connection =
        DirectUnaryConnectionError::local_deadline("tikv-timeout:20160", 23, "deadline".to_owned());
    let timeout = DirectUnaryClientError::Timeout {
        connection,
        timeout_ms: 500,
    };

    assert!(matches!(
        classify_transport_failure(&timeout),
        TransportFailureAction::RetryConnection {
            connection,
            close_generation: false,
        } if connection.address() == "tikv-timeout:20160" && connection.version() == 23
    ));
}
