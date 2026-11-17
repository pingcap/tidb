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

//! Go `br/pkg/restore`: the snapshot restore's region split, SST import and
//! the restorers that drive them.
//!
//! Log restore (`log_client`), PiTR's split strategies (`split/splitter.go`,
//! `split/sum_sorted.go`), raw/txn KV restore and the compacted-SST download
//! paths have no SQL caller and are not ported.

pub mod import_client;
pub mod import_mode_switcher;
pub mod restorer;
pub mod snap_client;
pub mod split;
