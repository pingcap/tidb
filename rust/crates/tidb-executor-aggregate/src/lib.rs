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

//! `pkg/executor/aggregate`: Go's `HashAggExec` (the serial
//! `unparallelExec` and the partial/final-worker `parallelExec`, with
//! `parallelHashAggSpillHelper` spill) and `StreamAggExec`, over Go's
//! `aggfuncs` (`tidb-executor-aggfuncs`).

pub mod agg_spill;
pub(crate) mod agg_hash_base_worker;
pub mod agg_hash_executor;
pub(crate) mod agg_hash_final_worker;
pub(crate) mod agg_hash_partial_worker;
pub mod agg_stream_executor;
pub(crate) mod agg_util;
mod group_key;
pub(crate) mod stats;
