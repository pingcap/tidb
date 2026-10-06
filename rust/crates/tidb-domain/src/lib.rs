// Copyright 2025 PingCAP, Inc.
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

//! Domain services and helpers used by the Rust server and session owners.
//!
//! This crate is not complete Go `pkg/domain` parity. Consult the structural
//! audit under `rust/docs/parity/current-audit` for current integration and
//! lifecycle obligations; historical file-level claims are not package acceptance.
//!
//! Live configuration hooks belong to the session/server owners. The unused
//! `DomainSysVarEnv` facade was retired. [`sysvar_cache`] retains shared cache
//! behavior. [`topn_slow_query`] retains Go's heap, expiry and FIFO contracts;
//! integrating them with the live session slow-query recorder remains open.
//! Preserve the original Go cases alongside these retained algorithms.

pub mod cdcutil;
pub mod cluster_topology;
pub mod disttask;
pub mod domainutil;
/// Complete Go `pkg/domain/globalconfigsync` queue/store contract.
pub mod globalconfigsync;
pub mod historical_stats;
pub mod metrics;
pub mod plan_replayer;
pub mod replayer;
pub mod ru_stats;
pub mod schema_checker;
pub mod server_id;
pub mod serverinfo;
pub mod serverinfo_syncer;
pub mod status_endpoint_claim;
pub mod sysvar_cache;
pub mod topn_slow_query;
