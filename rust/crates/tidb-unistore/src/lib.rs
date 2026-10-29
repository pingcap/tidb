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

//! Go `pkg/store/mockstore/unistore`: the in-process TiKV and PD that tests
//! and the embedded node run against -- the root package (`cluster`, `mock`,
//! `pd`, `rpc`, `tso`), its `tikv` server (`mvcc_store`, `kv_handler`,
//! `detector`, `region_manager`, `tikv_util`) and `cophandler`.
//!
//! Its leaf packages are their own crates: `lockstore`
//! (`tidb-unistore-lockstore`), `util/lockwaiter` (`tidb-unistore-lockwaiter`)
//! and `tikv/mvcc` (`tidb-unistore-mvcc`). `cophandler` and the `tikv` modules
//! stay here: the coprocessor scans `tikv`'s `MvccStore` (Go reads through
//! `dbreader`), and `tikv` and the root reference each other.

pub mod cluster;
pub mod cophandler;
pub mod detector;
pub mod kv_handler;
pub mod mock;
pub mod mock_pd;
pub mod mvcc_store;
pub mod pd;
pub mod redirector;
pub mod region_manager;
pub mod rpc;
#[cfg(test)]
mod tests_mockstore_part1_go_parity;
#[cfg(test)]
mod tests_tikv_util_go_parity;
pub(crate) mod tikv_util;
pub mod tso;

