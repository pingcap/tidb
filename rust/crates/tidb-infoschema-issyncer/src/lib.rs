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

//! Go `pkg/infoschema/issyncer`: the loader that brings a node's schema image
//! up to the cluster's current version (`loader.go`), the syncer that keeps it
//! current while the node serves (`syncer.go`), and the `mysql.tidb_mdl_info`
//! read the syncer does after every reload (`refreshMDLCheckTableInfo`).

pub mod catalog_reload;
pub mod catalog_watch;
pub mod real_tikv_catalog;
pub mod mdl_info_load;
