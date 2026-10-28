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

//! Go `pkg/metrics`: the Prometheus families every layer bumps, one module
//! per Go file (`executor.go`, `session.go`, ...), each with the
//! `init*Metrics` registration of its file.

pub mod bindinfo;
pub mod ddl;
pub mod distsql;
pub mod domain;
pub mod executor;
pub mod gc_worker;
pub mod globalsort;
pub mod memory;
pub mod meta;
pub mod owner;
pub mod panic;
pub mod server;
pub mod session;
pub mod stats;
pub mod stmtsummary;
pub mod topsql;
