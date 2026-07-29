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

//! The window-function partition RUNTIME: peer geometry and the per-partition
//! cursor the ranking states walk.
//!
//! This is state, not an operator. The window PLANNING and evaluation that used
//! to live here belonged to the retired `Database` engine; the live consumer is
//! `tidb-executor`'s `window`. What remains is shared by this crate's own
//! source-shaped ranking states (`cume_dist`, `ntile`, `lead_lag`,
//! `percentile`).

pub(crate) mod ranking_runtime;
