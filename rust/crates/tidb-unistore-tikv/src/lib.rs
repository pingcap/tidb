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

//! TiDB pkg/store/mockstore/unistore/tikv: the in-process TiKV server (Rust).

//! Go `pkg/store/mockstore/unistore/tikv`: the in-process TiKV server -- the
//! MVCC store (`mvcc_store`), the request handler (`kv_handler`, Go
//! `Server`), the region manager and mock PD (`region_manager`, `mock_pd`,
//! Go `mock_region.go`, with its timestamp oracle `tso`), the deadlock
//! `detector` and `tikv_util`. The coprocessor reads the store through
//! `tidb-unistore-dbreader`.

pub mod detector;
pub mod kv_handler;
pub mod mock_pd;
pub mod mvcc_store;
pub mod region_manager;
#[cfg(test)]
mod tests_tikv_util_go_parity;
pub(crate) mod tikv_util;
pub mod tso;
