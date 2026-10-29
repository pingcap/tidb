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

//! Native implementation of the pinned `github.com/pingcap/sysutil`
//! (v1.0.1-0.20240311050922-ae81ee01f3a5) root package: the diagnostics gRPC
//! service every TiDB node serves on its status port.
//!
//! `ServerInfo` answers the hardware, load and system facts the
//! `CLUSTER_HARDWARE`, `CLUSTER_LOAD` and `CLUSTER_SYSTEMINFO` tables read;
//! `SearchLog` streams the node's own log lines for `CLUSTER_LOG`.
//!
//! The upstream package reads the host through `gopsutil/v3` (v3.24.5, the
//! version TiDB's module graph selects). `host` carries exactly the gopsutil
//! calls this package makes, with their Linux and Darwin (cgo) behavior.

#![deny(unsafe_code)]
#![cfg_attr(test, feature(test))]

mod hardware_info;
mod host;
mod load_info;
mod search_log;
mod system_info;

#[cfg(test)]
mod search_log_test;
#[cfg(test)]
mod service_test;

pub use hardware_info::{get_memory_capacity, register_get_memory_capacity};
pub use search_log::{parse_log_level, TIME_STAMP_LAYOUT};

use std::pin::Pin;

use tidb_proto::diagnosticspb as pb;

/// Go `DiagnosticsServer`: serves `SearchLog` over the log file it was built
/// with, and `ServerInfo` over the host.
#[derive(Clone, Debug, Default)]
pub struct DiagnosticsServer {
    log_file: String,
}

impl DiagnosticsServer {
    /// Go `NewDiagnosticsServer`.
    #[must_use]
    pub fn new(log_file: impl Into<String>) -> Self {
        Self {
            log_file: log_file.into(),
        }
    }
}

type SearchLogStream =
    Pin<Box<dyn futures::Stream<Item = Result<pb::SearchLogResponse, tonic::Status>> + Send>>;

#[tonic::async_trait]
impl pb::diagnostics_server::Diagnostics for DiagnosticsServer {
    type search_logStream = SearchLogStream;

    /// Go `DiagnosticsServer.SearchLog`: one response per 1024 matching
    /// lines, always at least one, then the stream ends; an error ends the
    /// stream with that error.
    async fn search_log(
        &self,
        request: tonic::Request<pb::SearchLogRequest>,
    ) -> Result<tonic::Response<Self::search_logStream>, tonic::Status> {
        let request = request.into_inner();
        let log_file = self.log_file.clone();
        // Go's stream.Send blocks until the transport takes the message; a
        // one-slot channel keeps the reader exactly one batch ahead, and a
        // closed receiver is Go's `stream.Context().Done()`.
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        tokio::task::spawn_blocking(move || {
            let send = tx.clone();
            let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                search_log::search_log(&log_file, &request, &|| send.is_closed(), &mut |res| {
                    send.blocking_send(Ok(res)).map_err(|_| ())
                })
            }));
            let error = match outcome {
                Ok(Ok(())) => None,
                Ok(Err(search_log::SearchLogError::Canceled)) => None,
                Ok(Err(search_log::SearchLogError::Failed(message))) => Some(message),
                Err(panic) => {
                    let reason = panic
                        .downcast_ref::<&str>()
                        .map(|s| (*s).to_owned())
                        .or_else(|| panic.downcast_ref::<String>().cloned())
                        .unwrap_or_default();
                    let message = format!(
                        "search log panic, {reason}, stack is {}",
                        std::backtrace::Backtrace::force_capture()
                    );
                    tidb_log::error(&message, &[]);
                    Some(message)
                }
            };
            if let Some(message) = error {
                // A Go handler's plain error reaches the client as Unknown.
                let _ = tx.blocking_send(Err(tonic::Status::unknown(message)));
            }
        });
        let stream = futures::stream::unfold(rx, |mut rx| async move {
            rx.recv().await.map(|item| (item, rx))
        });
        Ok(tonic::Response::new(Box::pin(stream)))
    }

    /// Go `DiagnosticsServer.ServerInfo`.
    async fn server_info(
        &self,
        request: tonic::Request<pb::ServerInfoRequest>,
    ) -> Result<tonic::Response<pb::ServerInfoResponse>, tonic::Status> {
        let tp = request.into_inner().tp;
        let items = tokio::task::spawn_blocking(move || server_info_items(tp))
            .await
            .map_err(|error| tonic::Status::unknown(error.to_string()))?;
        Ok(tonic::Response::new(pb::ServerInfoResponse { items }))
    }
}

/// The items Go `ServerInfo` answers for `tp`, ordered by type then name.
fn server_info_items(tp: i32) -> Vec<pb::ServerInfoItem> {
    let mut items = Vec::new();
    match pb::ServerInfoType::try_from(tp) {
        Ok(pb::ServerInfoType::LoadInfo) => items = load_info::get_load_info(),
        Ok(pb::ServerInfoType::HardwareInfo) => items = hardware_info::get_hardware_info(),
        Ok(pb::ServerInfoType::SystemInfo) => items = system_info::get_system_info(),
        Ok(pb::ServerInfoType::All) => {
            items.extend(load_info::get_load_info());
            items.extend(hardware_info::get_hardware_info());
            items.extend(system_info::get_system_info());
        }
        Err(_) => {}
    }
    // Go's sort.Slice is not stable; keys repeat only for a device mounted
    // twice, where any order is one Go may produce.
    items.sort_by(|lhs, rhs| lhs.tp.cmp(&rhs.tp).then_with(|| lhs.name.cmp(&rhs.name)));
    items
}

/// One `ServerInfoItem`, as Go's composite literals build them.
fn item(tp: &str, name: &str, pairs: Vec<(&str, String)>) -> pb::ServerInfoItem {
    pb::ServerInfoItem {
        tp: tp.to_owned(),
        name: name.to_owned(),
        pairs: pairs
            .into_iter()
            .map(|(key, value)| pb::ServerInfoPair {
                key: key.to_owned(),
                value,
            })
            .collect(),
    }
}

/// Go `fmt.Sprintf("%.2f", v)`.
fn fmt2(value: f64) -> String {
    tidb_goruntime::go_strconv::format_float(value, b'f', 2, 64)
}
