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


//! Go `br/pkg/streamhelper`: the TiDB side of log backup. Every TiDB node
//! runs the checkpoint advancer under an etcd owner election (Go
//! `domain.initLogBackup`): the owner collects the region checkpoints of the
//! running log backup task from TiKV, uploads the global checkpoint to PD's
//! etcd and holds the GC safe point at it. The `br log` command side (task
//! creation, status, truncation) has no TiDB caller and is not ported.

pub mod advancer;
pub mod advancer_cliext;
pub mod advancer_env;
pub mod client;
pub mod collector;
pub mod config;
pub mod daemon;
pub mod flush_subscriber;
pub mod models;
pub mod prefix_scanner;
pub mod regioniter;

pub use advancer::CheckpointAdvancer;
pub use advancer_cliext::{AdvancerExt, EventType, TaskEvent};
pub use advancer_env::{Env, LogBackupService, StreamMeta, TiKVClusterMeta};
pub use client::{MetaDataClient, MetaStore, Task};
pub use models::*;
pub use regioniter::{RegionWithLeader, Store};

#[cfg(test)]
mod tests;
