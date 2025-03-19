// Copyright 2024 PingCAP, Inc.
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

package utils

import "github.com/pingcap/tidb/pkg/parser/mysql"

var planPeplayerTables = map[string]map[string]struct{}{
	"mysql": {
		"plan_replayer_status": {},
		"plan_replayer_task":   {},
	},
}

var statsTables = map[string]map[string]struct{}{
	"mysql": {
		"stats_buckets":      {},
		"stats_extended":     {},
		"stats_feedback":     {},
		"stats_fm_sketch":    {},
		"stats_histograms":   {},
		"stats_history":      {},
		"stats_meta":         {},
		"stats_meta_history": {},
		"stats_table_locked": {},
		"stats_top_n":        {},
		"column_stats_usage": {},
	},
}

// tables in this map is restored when fullClusterRestore=true
var sysPrivilegeTableMap = map[string]string{
	"user":          "(user = '%s' and host = '%%')",       // since v1.0.0
	"db":            "(user = '%s' and host = '%%')",       // since v1.0.0
	"tables_priv":   "(user = '%s' and host = '%%')",       // since v1.0.0
	"columns_priv":  "(user = '%s' and host = '%%')",       // since v1.0.0
	"default_roles": "(user = '%s' and host = '%%')",       // since v3.0.0
	"role_edges":    "(to_user = '%s' and to_host = '%%')", // since v3.0.0
	"global_priv":   "(user = '%s' and host = '%%')",       // since v3.0.8
	"global_grants": "(user = '%s' and host = '%%')",       // since v5.0.3
}

var unRecoverableTable = map[string]map[string]struct{}{
	"mysql": {
		// some variables in tidb (e.g. gc_safe_point) cannot be recovered.
		"tidb":                             {},
		"global_variables":                 {},
		"capture_plan_baselines_blacklist": {},
		// GET_LOCK() or IS_USED_LOCK() try to insert a lock into the table in a pessimistic transaction but finally rollback.
		// Therefore actually the table is empty.
		"advisory_locks": {},
		// Table ID is recorded in the column `job_info` so that the table cannot be recovered simply.
		"analyze_jobs": {},
		// Table ID is recorded in the column `table_id` so that the table cannot be recovered simply.
		"analyze_options": {},
		// Distributed eXecution Framework
		// Records the tidb node information, no need to recovered.
		"dist_framework_meta":             {},
		"tidb_global_task":                {},
		"tidb_global_task_history":        {},
		"tidb_background_subtask":         {},
		"tidb_background_subtask_history": {},
		// DDL internal system tables.
		"tidb_ddl_history": {},
		"tidb_ddl_job":     {},
		"tidb_ddl_reorg":   {},
		// Table ID is recorded in the column `schema_change` so that the table cannot be recovered simply.
		"tidb_ddl_notifier": {},
		// v7.2.0. Based on Distributed eXecution Framework, records running import jobs.
		"tidb_import_jobs": {},

		"help_topic": {},
		// records the RU for each resource group temporary, no need to recovered.
		"request_unit_by_group": {},
		// load the table data into the memory.
		"table_cache_meta": {},

		// TiDB runaway internal information.
		"tidb_runaway_queries":    {},
		"tidb_runaway_watch":      {},
		"tidb_runaway_watch_done": {},

		// TiDB internal ttl information.
		"tidb_ttl_job_history":  {},
		"tidb_ttl_table_status": {},
		"tidb_ttl_task":         {},

		// TiDB internal timers.
		"tidb_timers": {},

		// gc info don't need to recover.
		"gc_delete_range":       {},
		"gc_delete_range_done":  {},
		"index_advisor_results": {},

		// TiDB internal system table to synchronize metadata locks across nodes.
		"tidb_mdl_info": {},
		// replace into view is not supported now
		"tidb_mdl_view": {},

		"tidb_pitr_id_map": {},
	},
	"sys": {
		// replace into view is not supported now
		"schema_unused_indexes": {},
	},
}

var unRecoverableSchema = map[string]struct{}{
	mysql.WorkloadSchema: {},
}

func IsUnrecoverableTable(schemaName string, tableName string) bool {
	if _, ok := unRecoverableSchema[schemaName]; ok {
		return true
	}
	tableMap, ok := unRecoverableTable[schemaName]
	if !ok {
		return false
	}
	_, ok = tableMap[tableName]
	return ok
}

func IsStatsTable(schemaName string, tableName string) bool {
	tableMap, ok := statsTables[schemaName]
	if !ok {
		return false
	}
	_, ok = tableMap[tableName]
	return ok
}

func IsPlanReplayerTables(schemaName string, tableName string) bool {
	tableMap, ok := planPeplayerTables[schemaName]
	if !ok {
		return false
	}
	_, ok = tableMap[tableName]
	return ok
}

func IsSysPrivilegeTable(tableName string) bool {
	return sysPrivilegeTableMap[tableName] != ""
}
