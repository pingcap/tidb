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

//! Go `pkg/statistics/handle/lockstats`: `LOCK STATS` / `UNLOCK STATS` over
//! `mysql.stats_table_locked` -- the request (`cluster_stats_lock`), Go's
//! lock policy (`lock_stats`) and its statements on a statistics session
//! (`session_stats_lock`).

pub mod cluster_stats_lock;
pub mod lock_stats;
pub mod session_stats_lock;
