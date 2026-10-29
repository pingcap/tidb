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
// See the License for the specific language governing permissions and
// limitations under the License.
//! The cluster and session subsystems: catalog load and watch, DDL job and
//! metadata plumbing, real-TiKV read/write, privileges, sysvars, `mysql.*`
//! bootstrap, statistics, statement summaries, process info, DAG/coprocessor request
//! building, and the MySQL result-metadata contracts those paths publish.
//!
//! IT IS NOT THE QUERY ENGINE. The live operator tree -- the one every TCP
//! connection and every in-process session executes -- is `tidb-executor`
//! (`Executor` trait, chunk-based, pull-driven; `hash_agg`/`sort`/`limit`/
//! `join`/`window`). `tidb-session` reaches it directly and uses this crate
//! for cluster and session support. `tidb-exec` also uses shared executor
//! storage/scan types; it does not provide a second query engine.
//!
//! SQL aggregation, DISTINCT, sorting and window execution are owned by
//! `tidb-executor`. This crate provides cluster storage and result metadata;
//! it does not keep a second operator or aggregate-state implementation.

pub mod bootstrap_publish;
pub mod cluster_analyze;
pub mod cluster_discovery;
use tidb_cluster::cluster_http;
pub mod cluster_load_stats;
pub mod cluster_predicate_column;
pub mod cluster_sequence;
pub mod cluster_stats_dump;
pub mod cluster_stats_load;
pub mod cluster_stats_lock;
pub mod cluster_stats_write;
pub mod ddl_session_pool;
pub mod in_memory_meta;
pub mod explain;
pub mod label_delivery;
pub mod meta_txn;
pub mod mysql_bootstrap;
pub mod pd_affinity_client;
pub mod pd_approximate_count;
pub mod pre_split;
pub mod real_tikv_analyze;
pub mod real_tikv_load_stats;
pub mod real_tikv_privileges;
pub mod real_tikv_stats;
pub mod real_tikv_stats_lock;
mod result_metadata;
pub mod stats_watch;
pub mod table_info_build;
pub mod tikv_region_store;

pub use result_metadata::{
    col_names_to_result_fields, AdaptedResultField, FieldNameMetadata, IdentifierMetadata, ResultFieldMetadata,
    ResultFieldTypeMetadata, MAX_ALIAS_IDENTIFIER_LEN, NOT_FIXED_DEC, NOT_NULL_FLAG, UNSIGNED_FLAG,
};
