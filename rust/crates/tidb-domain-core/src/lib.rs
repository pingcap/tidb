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

//! Go `pkg/domain`'s `Domain` type and the cluster services it owns, above
//! the executor. Go's leaf `domain` subpackages (`infosync`, the server-info
//! syncer, the label manager) are tidb-domain, which the DDL builder imports;
//! this crate is the domain the session package opens sessions over.

pub mod cluster_account_seam;
pub mod cluster_analyze_seam;
pub mod cluster_privileges;
pub mod cluster_stats_lock_seam;
pub mod cluster_sysvar_seam;
pub mod cluster_transactions;
pub mod errors;
