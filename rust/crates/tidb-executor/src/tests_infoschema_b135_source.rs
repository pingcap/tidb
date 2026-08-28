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

//! Source-mapped ports of Go `pkg/executor/test/infoschema` items 962–966.
//!
//! These tests exercise information_schema retrievers, DDL schema-state
//! callbacks, statistics usage reporting, and keyspace metadata. Those
//! surfaces are owned by the session/DDL/statistics layers rather than this
//! executor crate, so each source test is retained as an explicit compiled
//! gap instead of being replaced by a weaker catalog assertion.

/// Go `infoschema_test.go:995::TestInfoSchemaExcludeNonPublicColumns`: query
/// `information_schema.columns` while a MODIFY COLUMN reorg is in progress.
#[test]
#[ignore = "go-parity-gap: DDL reorg failpoint and cross-session information_schema.columns visibility are unported here"]
fn info_schema_exclude_non_public_columns() {}

/// Go `infoschema_test.go:1019::TestIndexUsageWithData`: index usage counters,
/// statistics loading, and LAST_ACCESS_TIME reporting after indexed scans.
#[test]
#[ignore = "go-parity-gap: statistics handle, execution-info accounting, and information_schema.tidb_index_usage retriever are unported here"]
fn index_usage_with_data() {}

/// Go `infoschema_test.go:1172::TestKeyspaceMeta`: current keyspace metadata
/// is exposed as one `information_schema.keyspace_meta` row.
#[test]
#[ignore = "go-parity-gap: current keyspace metadata is supplied by the mock store/kernel and has no tidb-executor catalog surface"]
fn keyspace_meta() {}

/// Go `infoschema_test.go:1201::TestStatisticShowPublicIndexes`: an index is
/// absent from INFORMATION_SCHEMA.STATISTICS until its asynchronous DDL job
/// reaches PUBLIC.
#[test]
#[ignore = "go-parity-gap: asynchronous DDL schema-state callback and statistics retriever are unported here"]
fn statistic_show_public_indexes() {}

/// Go `infoschema/main_test.go:26::TestMain`: suite configuration and goleak
/// bootstrap only; it carries no product assertion.
#[test]
#[ignore = "skipped-reason: Go infoschema TestMain only configures auto-ID/failpoints/goleak"]
fn infoschema_suite_main_is_bootstrap() {}
