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

//! Go `pkg/store/mockstore/unistore`, the root package: the in-process
//! cluster (`cluster`, the single-store bootstrap), `mock`, the PD client
//! (`pd`), the RPC client (`rpc`) and its `redirector`.
//!
//! Its subpackages are their own crates: `tikv` (`tidb-unistore-tikv`),
//! `cophandler` (`tidb-unistore-cophandler`), `tikv/dbreader`
//! (`tidb-unistore-dbreader`), `tikv/kverrors` (`tidb-unistore-kverrors`),
//! `lockstore` (`tidb-unistore-lockstore`), `util/lockwaiter`
//! (`tidb-unistore-lockwaiter`) and `tikv/mvcc` (`tidb-unistore-mvcc`).

pub mod cluster;
pub mod mock;
pub mod pd;
pub mod redirector;
pub mod rpc;
#[cfg(test)]
mod tests_mockstore_part1_go_parity;

