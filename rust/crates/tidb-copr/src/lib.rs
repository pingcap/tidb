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
// See the License for the specific language governing permissions and
// limitations under the License.
//! The cluster and session subsystems: catalog load and watch, DDL job and

//! Go `pkg/store/copr`: the store's coprocessor client (`CopClient`), the
//! batch-cop task building for TiFlash, and the MPP client (`MPPClient`).

pub mod key_ranges;
pub mod range_diagnostics;
pub mod mpp_probe;
pub mod region_location;
pub mod copr_cache;
pub mod coprocessor_request;
pub mod read_bytes_ema;
pub mod region_task;
pub mod response_channel;
pub mod cop_paging;
pub mod query_transport;
pub mod batch_cop_tasks;
pub mod cop_client;
pub mod mpp_client;
