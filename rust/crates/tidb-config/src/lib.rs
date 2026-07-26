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

//! Transcreation of Go `pkg/config`'s leaf packages: `configtypes`
//! (TOML/JSON byte-size and duration wrappers), `deploymode` (process-wide
//! deployment mode), and `kerneltype` (compile-time kernel selection).
//!
//! The top-level `pkg/config` package (the `Config` struct tree) lands on
//! top of these as its own unit.

#![warn(missing_docs)]

pub mod config_tree;
pub mod configtypes;
pub mod deploymode;
pub mod external_workload;
pub mod kerneltype;
pub mod keyspace_observability;
pub mod store;
pub mod tiflash;
pub mod tikvcfg;

/// Default sample-rate row count (Go `pkg/config/const.go`
/// `DefRowsForSampleRate`).
pub const DEF_ROWS_FOR_SAMPLE_RATE: i64 = 110000;
