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

//! Go master `pkg/executor/mviewdeltamergeagg`: the sink executor that merges
//! a materialized view's aggregate deltas into its rows.

mod exec;
mod merge;
mod merge_count;
mod merge_sum;
mod writer;

#[cfg(test)]
mod exec_test;

pub use exec::{
    ChunkResult, Exec, Mapping, MergeRuntimeStats, MergeWorkerData, MergeWriterStats, NoopWriter, ResultWriter, RowOp,
    RowOpType,
};
pub use writer::{TargetColumn, TargetTable};
