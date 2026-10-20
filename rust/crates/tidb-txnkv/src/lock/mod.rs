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

//! The lock-resolution state TiDB's reads keep around client-rust's resolver
//! (the store's `GetLockResolver()`): the snapshot's resolved and committed
//! locks, and the timestamp source a read session draws from.

mod resolver;

pub use resolver::{FixedTimestampSource, LockRecoveryResult, SnapshotLockSet, TimestampSource};
