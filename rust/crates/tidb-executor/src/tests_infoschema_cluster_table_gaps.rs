// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache 2.0 license (see the License file at the crate root).

//! Gap tests for Go `pkg/executor/infoschema_cluster_table_test.go`: the
//! cluster-backed virtual tables (`information_schema.cluster_info`,
//! `TIKV_REGION_STATUS`, `TABLE_STORAGE_STATS`, `cluster_slow_query`).
//! On this tier the data sources live in Go's
//! `pkg/executor/infoschema_reader.go` (`dataForTiDBClusterInfo` :1842,
//! `setDataForTiKVRegionStatus` :2082, `tableStorageStatsRetriever` :2375)
//! and need a PD HTTP endpoint plus the diagnostics RPC server. This tier has
//! no wired cluster memtable reader or those remote transports.

/// Go `pkg/executor/infoschema_cluster_table_test.go:240::TestSkipEmptyIPNodesForTiDBTypeCoprocessor`:
/// with `AdvertiseAddress` set to `config.UnavailableIP`, a cluster-scoped
/// query over `information_schema.cluster_slow_query` returns no rows and no
/// warnings -- the local TiDB node is skipped because it has no usable IP.
#[test]
#[ignore = "go-parity-gap: the cluster_slow_query retriever and the AdvertiseAddress node filter are unported; no slow-log memtable reader on this tier"]
fn cluster_slow_query_skips_tidb_nodes_without_a_usable_ip() {}

/// Go `pkg/executor/infoschema_cluster_table_test.go:253::TestTiDBClusterInfo`:
/// `information_schema.cluster_info` merges the local TiDB row (status addr
/// from the status port), PD rows from the PD HTTP `/stores` endpoint
/// (version/git-hash/start-time), and TiKV store rows; the
/// `mockStoreTombstone` failpoint (`pkg/infoschema/tables.go:2308`) empties
/// the TiKV rows; the `mockClusterInfo` failpoint
/// (`pkg/infoschema/tables.go:1989`) replaces the instance list with
/// 6-field entries (type,address,status,version,githash,server_id) served in
/// order, and `cluster_config` fans config fetches out to those instances
/// with hidden-key filtering and per-key sorting.
#[test]
#[ignore = "go-parity-gap: dataForTiDBClusterInfo (infoschema_reader.go:1842) needs the PD HTTP client and the infoschema failpoints; unported"]
fn cluster_info_merges_tidb_pd_tikv_rows_and_honors_failpoints() {}

/// Go `pkg/executor/infoschema_cluster_table_test.go:342::TestTikvRegionStatus`:
/// `information_schema.TIKV_REGION_STATUS` lists one row per table/index (and
/// per partition for a RANGE-partitioned table, with global indexes as
/// non-partitioned rows), joins region metadata through the PD HTTP API
/// (`setDataForTiKVRegionStatus`, `infoschema_reader.go:2082`), supports
/// filtering by `TABLE_ID`, and excludes virtual schemas.
#[test]
#[ignore = "go-parity-gap: setDataForTiKVRegionStatus (infoschema_reader.go:2082) needs the PD region HTTP API and region cache; unported"]
fn tikv_region_status_lists_table_index_and_partition_rows() {}

/// Go `pkg/executor/infoschema_cluster_table_test.go:415::TestTableStorageStats`:
/// `information_schema.TABLE_STORAGE_STATS` errors with "pd unavailable"
/// without a PD address, demands a `TABLE_SCHEMA` predicate ("Please add where
/// clause to filter the column TABLE_SCHEMA."), returns an empty set for
/// system schemas, reports TABLE_SIZE per table/index from the PD region API,
/// and enforces per-schema privileges (a user without access sees zero rows;
/// grant-all or global-select users see the full count).
#[test]
#[ignore = "go-parity-gap: tableStorageStatsRetriever (infoschema_reader.go:2375/:2495) needs the PD HTTP API and privilege manager; unported"]
fn table_storage_stats_requires_schema_predicate_and_respects_privileges() {}

/// Go `pkg/executor/infoschema_cluster_table_test.go:492::TestIssue42619`:
/// a partitioned table contributes one TABLE_STORAGE_STATS row per partition,
/// each with peer/region/empty-region counts of 1 and size/keys of 1 under
/// the mock PD (`setDataForTableStorageStats`,
/// `infoschema_reader.go:2495`).
#[test]
#[ignore = "go-parity-gap: setDataForTableStorageStats (infoschema_reader.go:2495) needs the PD HTTP API; unported"]
fn table_storage_stats_emits_one_row_per_partition() {}
