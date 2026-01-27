// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package session

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/tidb/pkg/bindinfo"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/metadef"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/session/sessionapi"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/logutil"
	utilparser "github.com/pingcap/tidb/pkg/util/parser"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/pingcap/tidb/pkg/util/timeutil"
	"go.uber.org/zap"
)

const (
	// Const for TiDB server version 2.
	version2  = 2
	version3  = 3
	version4  = 4
	version5  = 5
	version6  = 6
	version7  = 7
	version8  = 8
	version9  = 9
	version10 = 10
	version11 = 11
	version12 = 12
	version13 = 13
	version14 = 14
	version15 = 15
	version16 = 16
	version17 = 17
	version18 = 18
	version19 = 19
	version20 = 20
	version21 = 21
	version22 = 22
	version23 = 23
	version24 = 24
	version25 = 25
	version26 = 26
	version27 = 27
	version28 = 28
	// version29 only need to be run when the current version is 28.
	version29 = 29
	version30 = 30
	version31 = 31
	version32 = 32
	version33 = 33
	version34 = 34
	version35 = 35
	version36 = 36
	version37 = 37
	version38 = 38
	// version39 will be redone in version46 so it's skipped here.
	// version40 is the version that introduce new collation in TiDB,
	// see https://github.com/pingcap/tidb/pull/14574 for more details.
	version40 = 40
	version41 = 41
	// version42 add storeType and reason column in expr_pushdown_blacklist
	version42 = 42
	// version43 updates global variables related to statement summary.
	version43 = 43
	// version44 delete tidb_isolation_read_engines from mysql.global_variables to avoid unexpected behavior after upgrade.
	version44 = 44
	// version45 introduces CONFIG_PRIV for SET CONFIG statements.
	version45 = 45
	// version46 fix a bug in v3.1.1.
	version46 = 46
	// version47 add Source to bindings to indicate the way binding created.
	version47 = 47
	// version48 reset all deprecated concurrency related system-variables if they were all default value.
	// version49 introduces mysql.stats_extended table.
	// Both version48 and version49 will be redone in version55 and version56 so they're skipped here.
	// version50 add mysql.schema_index_usage table.
	version50 = 50
	// version51 introduces CreateTablespacePriv to mysql.user.
	// version51 will be redone in version63 so it's skipped here.
	// version52 change mysql.stats_histograms cm_sketch column from blob to blob(6291456)
	version52 = 52
	// version53 introduce Global variable tidb_enable_strict_double_type_check
	version53 = 53
	// version54 writes a variable `mem_quota_query` to mysql.tidb if it's a cluster upgraded from v3.0.x to v4.0.9+.
	version54 = 54
	// version55 fixes the bug that upgradeToVer48 would be missed when upgrading from v4.0 to a new version
	version55 = 55
	// version56 fixes the bug that upgradeToVer49 would be missed when upgrading from v4.0 to a new version
	version56 = 56
	// version57 fixes the bug of concurrent create / drop binding
	version57 = 57
	// version58 add `Repl_client_priv` and `Repl_slave_priv` to `mysql.user`
	// version58 will be redone in version64 so it's skipped here.
	// version59 add writes a variable `oom-action` to mysql.tidb if it's a cluster upgraded from v3.0.x to v4.0.11+.
	version59 = 59
	// version60 redesigns `mysql.stats_extended`
	version60 = 60
	// version61 will be redone in version67
	// version62 add column ndv for mysql.stats_buckets.
	version62 = 62
	// version63 fixes the bug that upgradeToVer51 would be missed when upgrading from v4.0 to a new version
	version63 = 63
	// version64 is redone upgradeToVer58 after upgradeToVer63, this is to preserve the order of the columns in mysql.user
	version64 = 64
	// version65 add mysql.stats_fm_sketch table.
	version65 = 65
	// version66 enables the feature `track_aggregate_memory_usage` by default.
	version66 = 66
	// version67 restore all SQL bindings.
	version67 = 67
	// version68 update the global variable 'tidb_enable_clustered_index' from 'off' to 'int_only'.
	version68 = 68
	// version69 adds mysql.global_grants for DYNAMIC privileges
	version69 = 69
	// version70 adds mysql.user.plugin to allow multiple authentication plugins
	version70 = 70
	// version71 forces tidb_multi_statement_mode=OFF when tidb_multi_statement_mode=WARN
	// This affects upgrades from v4.0 where the default was WARN.
	version71 = 71
	// version72 adds snapshot column for mysql.stats_meta
	version72 = 72
	// version73 adds mysql.capture_plan_baselines_blacklist table
	version73 = 73
	// version74 changes global variable `tidb_stmt_summary_max_stmt_count` value from 200 to 3000.
	version74 = 74
	// version75 update mysql.*.host from char(60) to char(255)
	version75 = 75
	// version76 update mysql.columns_priv from SET('Select','Insert','Update') to SET('Select','Insert','Update','References')
	version76 = 76
	// version77 adds mysql.column_stats_usage table
	version77 = 77
	// version78 updates mysql.stats_buckets.lower_bound, mysql.stats_buckets.upper_bound and mysql.stats_histograms.last_analyze_pos from BLOB to LONGBLOB.
	version78 = 78
	// version79 adds the mysql.table_cache_meta table
	version79 = 79
	// version80 fixes the issue https://github.com/pingcap/tidb/issues/25422.
	// If the TiDB upgrading from the 4.x to a newer version, we keep the tidb_analyze_version to 1.
	version80 = 80
	// version81 insert "tidb_enable_index_merge|off" to mysql.GLOBAL_VARIABLES if there is no tidb_enable_index_merge.
	// This will only happens when we upgrade a cluster before 4.0.0 to 4.0.0+.
	version81 = 81
	// version82 adds the mysql.analyze_options table
	version82 = 82
	// version83 adds the tables mysql.stats_history
	version83 = 83
	// version84 adds the tables mysql.stats_meta_history
	version84 = 84
	// version85 updates bindings with status 'using' in mysql.bind_info table to 'enabled' status
	version85 = 85
	// version86 update mysql.tables_priv from SET('Select','Insert','Update') to SET('Select','Insert','Update','References').
	version86 = 86
	// version87 adds the mysql.analyze_jobs table
	version87 = 87
	// version88 fixes the issue https://github.com/pingcap/tidb/issues/33650.
	version88 = 88
	// version89 adds the tables mysql.advisory_locks
	version89 = 89
	// version90 converts enable-batch-dml, mem-quota-query, query-log-max-len, committer-concurrency, run-auto-analyze, and oom-action to a sysvar
	version90 = 90
	// version91 converts prepared-plan-cache to sysvars
	version91 = 91
	// version92 for concurrent ddl.
	version92 = 92
	// version93 converts oom-use-tmp-storage to a sysvar
	version93 = 93
	version94 = 94
	// version95 add a column `User_attributes` to `mysql.user`
	version95 = 95
	// version97 sets tidb_opt_range_max_size to 0 when a cluster upgrades from some version lower than v6.4.0 to v6.4.0+.
	// It promises the compatibility of building ranges behavior.
	version97 = 97
	// version98 add a column `Token_issuer` to `mysql.user`
	version98 = 98
	version99 = 99
	// version100 converts server-memory-quota to a sysvar
	version100 = 100
	// version101 add mysql.plan_replayer_status table
	version101 = 101
	// version102 add mysql.plan_replayer_task table
	version102 = 102
	// version103 adds the tables mysql.stats_table_locked
	version103 = 103
	// version104 add `sql_digest` and `plan_digest` to `bind_info`
	version104 = 104
	// version105 insert "tidb_cost_model_version|1" to mysql.GLOBAL_VARIABLES if there is no tidb_cost_model_version.
	// This will only happens when we upgrade a cluster before 6.0.
	version105 = 105
	// version106 add mysql.password_history, and Password_reuse_history, Password_reuse_time into mysql.user.
	version106 = 106
	// version107 add columns related to password expiration into mysql.user
	version107 = 107
	// version108 adds the table tidb_ttl_table_status
	version108 = 108
	// version109 sets tidb_enable_gc_aware_memory_track to off when a cluster upgrades from some version lower than v6.5.0.
	version109 = 109
	// ...
	// [version110, version129] is the version range reserved for patches of 6.5.x
	// ...
	// version110 sets tidb_stats_load_pseudo_timeout to ON when a cluster upgrades from some version lower than v6.5.0.
	version110 = 110
	// version130 add column source to mysql.stats_meta_history
	version130 = 130
	// version131 adds the table tidb_ttl_task and tidb_ttl_job_history
	version131 = 131
	// version132 modifies the view tidb_mdl_view
	version132 = 132
	// version133 sets tidb_server_memory_limit to "80%"
	version133 = 133
	// version134 modifies the following global variables default value:
	// - foreign_key_checks: off -> on
	// - tidb_enable_foreign_key: off -> on
	// - tidb_store_batch_size: 0 -> 4
	version134 = 134
	// version135 sets tidb_opt_advanced_join_hint to off when a cluster upgrades from some version lower than v7.0.
	version135 = 135
	// version136 prepare the tables for the distributed task.
	version136 = 136
	// version137 introduces some reserved resource groups
	version137 = 137
	// version 138 set tidb_enable_null_aware_anti_join to true
	version138 = 138
	// version 139 creates mysql.load_data_jobs table for LOAD DATA statement
	// deprecated in version184
	version139 = 139
	// version 140 add column task_key to mysql.tidb_global_task
	version140 = 140
	// version 141
	//   set the value of `tidb_session_plan_cache_size` to "tidb_prepared_plan_cache_size" if there is no `tidb_session_plan_cache_size`.
	//   update tidb_load_based_replica_read_threshold from 0 to 4
	// This will only happens when we upgrade a cluster before 7.1.
	version141 = 141
	// version 142 insert "tidb_enable_non_prepared_plan_cache|0" to mysql.GLOBAL_VARIABLES if there is no tidb_enable_non_prepared_plan_cache.
	// This will only happens when we upgrade a cluster before 6.5.
	version142 = 142
	// version 143 add column `error` to `mysql.tidb_global_task` and `mysql.tidb_background_subtask`
	version143 = 143
	// version 144 turn off `tidb_plan_cache_invalidation_on_fresh_stats`, which is introduced in 7.1-rc,
	// if it's upgraded from an existing old version cluster.
	version144 = 144
	// version 145 to only add a version make we know when we support upgrade state.
	version145 = 145
	// version 146 add index for mysql.stats_meta_history and mysql.stats_history.
	version146 = 146
	// ...
	// [version147, version166] is the version range reserved for patches of 7.1.x
	// ...
	// version 167 add column `step` to `mysql.tidb_background_subtask`
	version167 = 167
	version168 = 168
	// version 169
	// 	 create table `mysql.tidb_runaway_quarantined_watch` and table `mysql.tidb_runaway_queries`
	//   to save runaway query records and persist runaway watch at 7.2 version.
	//   but due to ver171 recreate `mysql.tidb_runaway_watch`,
	//   no need to create table `mysql.tidb_runaway_quarantined_watch`, so delete it.
	version169 = 169
	version170 = 170
	// version 171
	//   keep the tidb_server length same as instance in other tables.
	version171 = 171
	// version 172
	//   create table `mysql.tidb_runaway_watch` and table `mysql.tidb_runaway_watch_done`
	//   to persist runaway watch and deletion of runaway watch at 7.3.
	version172 = 172
	// version 173 add column `summary` to `mysql.tidb_background_subtask`.
	version173 = 173
	// version 174
	//   add column `step`, `error`; delete unique key; and add key idx_state_update_time
	//   to `mysql.tidb_background_subtask_history`.
	version174 = 174

	// version 175
	//   update normalized bindings of `in (?)` to `in (...)` to solve #44298.
	version175 = 175

	// version 176
	//   add `mysql.tidb_global_task_history`
	version176 = 176

	// version 177
	//   add `mysql.dist_framework_meta`
	version177 = 177

	// version 178
	//   write mDDLTableVersion into `mysql.tidb` table
	version178 = 178

	// version 179
	//   enlarge `VARIABLE_VALUE` of `mysql.global_variables` from `varchar(1024)` to `varchar(16383)`.
	version179 = 179

	// ...
	// [version180, version189] is the version range reserved for patches of 7.5.x
	// ...

	// version 190
	//   add priority/create_time/end_time to `mysql.tidb_global_task`/`mysql.tidb_global_task_history`
	//   add concurrency/create_time/end_time/digest to `mysql.tidb_background_subtask`/`mysql.tidb_background_subtask_history`
	//   add idx_exec_id(exec_id), uk_digest to `mysql.tidb_background_subtask`
	//   add cpu_count to mysql.dist_framework_meta
	//   modify `mysql.dist_framework_meta` host from VARCHAR(100) to VARCHAR(261)
	//   modify `mysql.tidb_background_subtask`/`mysql.tidb_background_subtask_history` exec_id from varchar(256) to VARCHAR(261)
	//   modify `mysql.tidb_global_task`/`mysql.tidb_global_task_history` dispatcher_id from varchar(256) to VARCHAR(261)
	version190 = 190

	// version 191
	//   set tidb_txn_mode to Optimistic when tidb_txn_mode is not set.
	version191 = 191

	// version 192
	//   add new system table `mysql.request_unit_by_group`, which is used for
	//   historical RU consumption by resource group per day.
	version192 = 192

	// version 193
	//   replace `mysql.tidb_mdl_view` table
	version193 = 193

	// version 194
	//   remove `mysql.load_data_jobs` table
	version194 = 194

	// version 195
	//   drop `mysql.schema_index_usage` table
	//   create `sys` schema
	//   create `sys.schema_unused_indexes` table
	version195 = 195

	// version 196
	//   add column `target_scope` for 'mysql.tidb_global_task` table
	//   add column `target_scope` for 'mysql.tidb_global_task_history` table
	version196 = 196

	// version 197
	//   replace `mysql.tidb_mdl_view` table
	version197 = 197

	// version 198
	//   add column `owner_id` for `mysql.tidb_mdl_info` table
	version198 = 198

	// ...
	// [version199, version208] is the version range reserved for patches of 8.1.x
	// ...

	// version 209
	//   sets `tidb_resource_control_strict_mode` to off when a cluster upgrades from some version lower than v8.2.
	version209 = 209
	// version210 indicates that if TiDB is upgraded from a lower version(lower than 8.3.0), the tidb_analyze_column_options will be set to ALL.
	version210 = 210

	// version211 add column `summary` to `mysql.tidb_background_subtask_history`.
	version211 = 211

	// version212 changed a lots of runaway related table.
	// 1. switchGroup: add column `switch_group_name` to `mysql.tidb_runaway_watch` and `mysql.tidb_runaway_watch_done`.
	// 2. modify column `plan_digest` type, modify column `time` to `start_time,
	// modify column `original_sql` to `sample_sql` to `mysql.tidb_runaway_queries`.
	// 3. modify column length of `action`.
	// 4. add column `rule` to `mysql.tidb_runaway_watch`, `mysql.tidb_runaway_watch_done` and `mysql.tidb_runaway_queries`.
	version212 = 212

	// version 213
	//   create `mysql.tidb_pitr_id_map` table
	version213 = 213

	// version 214
	//   create `mysql.index_advisor_results` table
	version214 = 214

	// If the TiDB upgrading from the a version before v7.0 to a newer version, we keep the tidb_enable_inl_join_inner_multi_pattern to 0.
	version215 = 215

	// version 216
	//   changes variable `tidb_scatter_region` value from ON to "table" and OFF to "".
	version216 = 216

	// version 217
	// Keep tidb_schema_cache_size to 0 if this variable does not exist (upgrading from old version pre 8.1).
	version217 = 217

	// version 218
	// enable fast_create_table on default
	version218 = 218

	// ...
	// [version219, version238] is the version range reserved for patches of 8.5.x
	// ...

	// next version should start with 239

	// version 239
	// add modify_params to tidb_global_task and tidb_global_task_history.
	version239 = 239

	// version 240
	// Add indexes to mysql.analyze_jobs to speed up the query.
	version240 = 240

	// Add index on user field for some mysql tables.
	version241 = 241

	// version 242
	//   insert `cluster_id` into the `mysql.tidb` table.
	//   Add workload-based learning system tables
	version242 = 242

	// Add max_node_count column to tidb_global_task and tidb_global_task_history.
	// Add extra_params to tidb_global_task and tidb_global_task_history.
	version243 = 243

	// version244 add Max_user_connections into mysql.user.
	version244 = 244

	// version245 updates column types of mysql.bind_info.
	version245 = 245

	// version246 adds new unique index for mysql.bind_info.
	version246 = 246

	// version 247
	// Add last_stats_histograms_version to mysql.stats_meta.
	version247 = 247

	// version 248
	// Update mysql.tidb_pitr_id_map to add restore_id as a primary key field
	version248 = 248
	version249 = 249

	// version250 add keyspace to tidb_global_task and tidb_global_task_history.
	version250 = 250

	// version 251
	// Add group_key to mysql.tidb_import_jobs.
	version251 = 251

	// version 252
	// Update FSP of mysql.bind_info timestamp columns to microsecond precision.
	version252 = 252

	// version253
	// Add last_used_date to mysql.bind_info
	version253 = 253

	// version254
	// Add index on start_time for mysql.tidb_runaway_watch and done_time for mysql.tidb_runaway_watch_done
	// to improve the performance of runaway watch sync loop.
	version254 = 254
)

// versionedUpgradeFunction is a struct that holds the upgrade function related
// to a specific bootstrap version.
// we will run the upgrade function fn when the current bootstrapped version is
// less than the version in this struct
type versionedUpgradeFunction struct {
	version int64
	fn      func(sessionapi.Session, int64)
}

// currentBootstrapVersion is defined as a variable, so we can modify its value for testing.
// please make sure this is the largest version
var currentBootstrapVersion int64 = version254

var (
	// this list must be ordered by version in ascending order, and the function
	// name must follow the same pattern as `upgradeToVer<version>`.
	upgradeToVerFunctions = []versionedUpgradeFunction{
		{version: version2, fn: upgradeToVer2},
		{version: version3, fn: upgradeToVer3},
		{version: version4, fn: upgradeToVer4},
		{version: version5, fn: upgradeToVer5},
		{version: version6, fn: upgradeToVer6},
		{version: version7, fn: upgradeToVer7},
		{version: version8, fn: upgradeToVer8},
		{version: version9, fn: upgradeToVer9},
		{version: version10, fn: upgradeToVer10},
		{version: version11, fn: upgradeToVer11},
		{version: version12, fn: upgradeToVer12},
		{version: version13, fn: upgradeToVer13},
		{version: version14, fn: upgradeToVer14},
		{version: version15, fn: upgradeToVer15},
		{version: version16, fn: upgradeToVer16},
		{version: version17, fn: upgradeToVer17},
		{version: version18, fn: upgradeToVer18},
		{version: version19, fn: upgradeToVer19},
		{version: version20, fn: upgradeToVer20},
		{version: version21, fn: upgradeToVer21},
		{version: version22, fn: upgradeToVer22},
		{version: version23, fn: upgradeToVer23},
		{version: version24, fn: upgradeToVer24},
		{version: version25, fn: upgradeToVer25},
		{version: version26, fn: upgradeToVer26},
		{version: version27, fn: upgradeToVer27},
		{version: version28, fn: upgradeToVer28},
		{version: version29, fn: upgradeToVer29},
		{version: version30, fn: upgradeToVer30},
		{version: version31, fn: upgradeToVer31},
		{version: version32, fn: upgradeToVer32},
		{version: version33, fn: upgradeToVer33},
		{version: version34, fn: upgradeToVer34},
		{version: version35, fn: upgradeToVer35},
		{version: version36, fn: upgradeToVer36},
		{version: version37, fn: upgradeToVer37},
		{version: version38, fn: upgradeToVer38},
		// We will redo upgradeToVer39 in upgradeToVer46,
		// so upgradeToVer39 is skipped here.
		{version: version40, fn: upgradeToVer40},
		{version: version41, fn: upgradeToVer41},
		{version: version42, fn: upgradeToVer42},
		{version: version43, fn: upgradeToVer43},
		{version: version44, fn: upgradeToVer44},
		{version: version45, fn: upgradeToVer45},
		{version: version46, fn: upgradeToVer46},
		{version: version47, fn: upgradeToVer47},
		// We will redo upgradeToVer48 and upgradeToVer49 in upgradeToVer55 and upgradeToVer56,
		// so upgradeToVer48 and upgradeToVer49 is skipped here.
		{version: version50, fn: upgradeToVer50},
		// We will redo upgradeToVer51 in upgradeToVer63, it is skipped here.
		{version: version52, fn: upgradeToVer52},
		{version: version53, fn: upgradeToVer53},
		{version: version54, fn: upgradeToVer54},
		{version: version55, fn: upgradeToVer55},
		{version: version56, fn: upgradeToVer56},
		{version: version57, fn: upgradeToVer57},
		// We will redo upgradeToVer58 in upgradeToVer64, it is skipped here.
		{version: version59, fn: upgradeToVer59},
		{version: version60, fn: upgradeToVer60},
		// We will redo upgradeToVer61 in upgradeToVer67, it is skipped here.
		{version: version62, fn: upgradeToVer62},
		{version: version63, fn: upgradeToVer63},
		{version: version64, fn: upgradeToVer64},
		{version: version65, fn: upgradeToVer65},
		{version: version66, fn: upgradeToVer66},
		{version: version67, fn: upgradeToVer67},
		{version: version68, fn: upgradeToVer68},
		{version: version69, fn: upgradeToVer69},
		{version: version70, fn: upgradeToVer70},
		{version: version71, fn: upgradeToVer71},
		{version: version72, fn: upgradeToVer72},
		{version: version73, fn: upgradeToVer73},
		{version: version74, fn: upgradeToVer74},
		{version: version75, fn: upgradeToVer75},
		{version: version76, fn: upgradeToVer76},
		{version: version77, fn: upgradeToVer77},
		{version: version78, fn: upgradeToVer78},
		{version: version79, fn: upgradeToVer79},
		{version: version80, fn: upgradeToVer80},
		{version: version81, fn: upgradeToVer81},
		{version: version82, fn: upgradeToVer82},
		{version: version83, fn: upgradeToVer83},
		{version: version84, fn: upgradeToVer84},
		{version: version85, fn: upgradeToVer85},
		{version: version86, fn: upgradeToVer86},
		{version: version87, fn: upgradeToVer87},
		{version: version88, fn: upgradeToVer88},
		{version: version89, fn: upgradeToVer89},
		{version: version90, fn: upgradeToVer90},
		{version: version91, fn: upgradeToVer91},
		{version: version93, fn: upgradeToVer93},
		{version: version94, fn: upgradeToVer94},
		{version: version95, fn: upgradeToVer95},
		// We will redo upgradeToVer96 in upgradeToVer100, it is skipped here.
		{version: version97, fn: upgradeToVer97},
		{version: version98, fn: upgradeToVer98},
		{version: version100, fn: upgradeToVer100},
		{version: version101, fn: upgradeToVer101},
		{version: version102, fn: upgradeToVer102},
		{version: version103, fn: upgradeToVer103},
		{version: version104, fn: upgradeToVer104},
		{version: version105, fn: upgradeToVer105},
		{version: version106, fn: upgradeToVer106},
		{version: version107, fn: upgradeToVer107},
		{version: version108, fn: upgradeToVer108},
		{version: version109, fn: upgradeToVer109},
		{version: version110, fn: upgradeToVer110},
		{version: version130, fn: upgradeToVer130},
		{version: version131, fn: upgradeToVer131},
		{version: version132, fn: upgradeToVer132},
		{version: version133, fn: upgradeToVer133},
		{version: version134, fn: upgradeToVer134},
		{version: version135, fn: upgradeToVer135},
		{version: version136, fn: upgradeToVer136},
		{version: version137, fn: upgradeToVer137},
		{version: version138, fn: upgradeToVer138},
		{version: version139, fn: upgradeToVer139},
		{version: version140, fn: upgradeToVer140},
		{version: version141, fn: upgradeToVer141},
		{version: version142, fn: upgradeToVer142},
		{version: version143, fn: upgradeToVer143},
		{version: version144, fn: upgradeToVer144},
		// We will only use Ver145 to differentiate versions, so it is skipped here.
		{version: version146, fn: upgradeToVer146},
		{version: version167, fn: upgradeToVer167},
		{version: version168, fn: upgradeToVer168},
		{version: version169, fn: upgradeToVer169},
		{version: version170, fn: upgradeToVer170},
		{version: version171, fn: upgradeToVer171},
		{version: version172, fn: upgradeToVer172},
		{version: version173, fn: upgradeToVer173},
		{version: version174, fn: upgradeToVer174},
		{version: version175, fn: upgradeToVer175},
		{version: version176, fn: upgradeToVer176},
		{version: version177, fn: upgradeToVer177},
		{version: version178, fn: upgradeToVer178},
		{version: version179, fn: upgradeToVer179},
		{version: version190, fn: upgradeToVer190},
		{version: version191, fn: upgradeToVer191},
		{version: version192, fn: upgradeToVer192},
		{version: version193, fn: upgradeToVer193},
		{version: version194, fn: upgradeToVer194},
		{version: version195, fn: upgradeToVer195},
		{version: version196, fn: upgradeToVer196},
		{version: version197, fn: upgradeToVer197},
		{version: version198, fn: upgradeToVer198},
		{version: version209, fn: upgradeToVer209},
		{version: version210, fn: upgradeToVer210},
		{version: version211, fn: upgradeToVer211},
		{version: version212, fn: upgradeToVer212},
		{version: version213, fn: upgradeToVer213},
		{version: version214, fn: upgradeToVer214},
		{version: version215, fn: upgradeToVer215},
		{version: version216, fn: upgradeToVer216},
		{version: version217, fn: upgradeToVer217},
		{version: version218, fn: upgradeToVer218},
		{version: version239, fn: upgradeToVer239},
		{version: version240, fn: upgradeToVer240},
		{version: version241, fn: upgradeToVer241},
		{version: version242, fn: upgradeToVer242},
		{version: version243, fn: upgradeToVer243},
		{version: version244, fn: upgradeToVer244},
		{version: version245, fn: upgradeToVer245},
		{version: version246, fn: upgradeToVer246},
		{version: version247, fn: upgradeToVer247},
		{version: version248, fn: upgradeToVer248},
		{version: version249, fn: upgradeToVer249},
		{version: version250, fn: upgradeToVer250},
		{version: version251, fn: upgradeToVer251},
		{version: version252, fn: upgradeToVer252},
		{version: version253, fn: upgradeToVer253},
		{version: version254, fn: upgradeToVer254},
	}
)

// upgradeToVer2 updates to version 2.
func upgradeToVer2(s sessionapi.Session, _ int64) {
	// Version 2 add two system variable for DistSQL concurrency controlling.
	// Insert distsql related system variable.
	distSQLVars := []string{vardef.TiDBDistSQLScanConcurrency}
	values := make([]string, 0, len(distSQLVars))
	for _, v := range distSQLVars {
		value := fmt.Sprintf(`("%s", "%s")`, v, variable.GetSysVar(v).Value)
		values = append(values, value)
	}
	sql := fmt.Sprintf("INSERT HIGH_PRIORITY IGNORE INTO %s.%s VALUES %s;", mysql.SystemDB, mysql.GlobalVariablesTable,
		strings.Join(values, ", "))
	mustExecute(s, sql)
}

// upgradeToVer3 updates to version 3.
func upgradeToVer3(s sessionapi.Session, _ int64) {
	// Version 3 fix tx_read_only variable value.
	mustExecute(s, "UPDATE HIGH_PRIORITY %n.%n SET variable_value = '0' WHERE variable_name = 'tx_read_only';", mysql.SystemDB, mysql.GlobalVariablesTable)
}

// upgradeToVer4 updates to version 4.
func upgradeToVer4(s sessionapi.Session, _ int64) {
	mustExecute(s, metadef.CreateStatsMetaTable)
}

func upgradeToVer5(s sessionapi.Session, _ int64) {
	mustExecute(s, metadef.CreateStatsHistogramsTable)
	mustExecute(s, metadef.CreateStatsBucketsTable)
}

func upgradeToVer6(s sessionapi.Session, _ int64) {
	doReentrantDDL(s, "ALTER TABLE mysql.user ADD COLUMN `Super_priv` ENUM('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Show_db_priv`", infoschema.ErrColumnExists)
	// For reasons of compatibility, set the non-exists privilege column value to 'Y', as TiDB doesn't check them in older versions.
	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.user SET Super_priv='Y'")
}

func upgradeToVer7(s sessionapi.Session, _ int64) {
	doReentrantDDL(s, "ALTER TABLE mysql.user ADD COLUMN `Process_priv` ENUM('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Drop_priv`", infoschema.ErrColumnExists)
	// For reasons of compatibility, set the non-exists privilege column value to 'Y', as TiDB doesn't check them in older versions.
	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.user SET Process_priv='Y'")
}

func upgradeToVer8(s sessionapi.Session, ver int64) {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnBootstrap)
	// This is a dummy upgrade, it checks whether upgradeToVer7 success, if not, do it again.
	if _, err := s.ExecuteInternal(ctx, "SELECT HIGH_PRIORITY `Process_priv` FROM mysql.user LIMIT 0"); err == nil {
		return
	}
	upgradeToVer7(s, ver)
}

func upgradeToVer9(s sessionapi.Session, _ int64) {
	doReentrantDDL(s, "ALTER TABLE mysql.user ADD COLUMN `Trigger_priv` ENUM('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Create_user_priv`", infoschema.ErrColumnExists)
	// For reasons of compatibility, set the non-exists privilege column value to 'Y', as TiDB doesn't check them in older versions.
	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.user SET Trigger_priv='Y'")
}

func doReentrantDDL(s sessionapi.Session, sql string, ignorableErrs ...error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(internalSQLTimeout)*time.Second)
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnBootstrap)
	_, err := s.ExecuteInternal(ctx, sql)
	defer cancel()
	for _, ignorableErr := range ignorableErrs {
		if terror.ErrorEqual(err, ignorableErr) {
			return
		}
	}
	if err != nil {
		logutil.BgLogger().Fatal("doReentrantDDL error", zap.Error(err))
	}
}

func upgradeToVer10(s sessionapi.Session, _ int64) {
	doReentrantDDL(s, "ALTER TABLE mysql.stats_buckets CHANGE COLUMN `value` `upper_bound` BLOB NOT NULL", infoschema.ErrColumnNotExists, infoschema.ErrColumnExists)
	doReentrantDDL(s, "ALTER TABLE mysql.stats_buckets ADD COLUMN `lower_bound` BLOB", infoschema.ErrColumnExists)
	doReentrantDDL(s, "ALTER TABLE mysql.stats_histograms ADD COLUMN `null_count` BIGINT(64) NOT NULL DEFAULT 0", infoschema.ErrColumnExists)
	doReentrantDDL(s, "ALTER TABLE mysql.stats_histograms DROP COLUMN distinct_ratio", dbterror.ErrCantDropFieldOrKey)
	doReentrantDDL(s, "ALTER TABLE mysql.stats_histograms DROP COLUMN use_count_to_estimate", dbterror.ErrCantDropFieldOrKey)
}

func upgradeToVer11(s sessionapi.Session, _ int64) {
	doReentrantDDL(s, "ALTER TABLE mysql.user ADD COLUMN `References_priv` ENUM('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Grant_priv`", infoschema.ErrColumnExists)
	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.user SET References_priv='Y'")
}

func upgradeToVer12(s sessionapi.Session, _ int64) {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnBootstrap)
	_, err := s.ExecuteInternal(ctx, "BEGIN")
	terror.MustNil(err)
	sql := "SELECT HIGH_PRIORITY user, host, password FROM mysql.user WHERE password != ''"
	rs, err := s.ExecuteInternal(ctx, sql)
	if terror.ErrorEqual(err, plannererrors.ErrUnknownColumn) {
		sql := "SELECT HIGH_PRIORITY user, host, authentication_string FROM mysql.user WHERE authentication_string != ''"
		rs, err = s.ExecuteInternal(ctx, sql)
	}
	terror.MustNil(err)
	sqls := make([]string, 0, 1)
	defer terror.Call(rs.Close)
	req := rs.NewChunk(nil)
	it := chunk.NewIterator4Chunk(req)
	err = rs.Next(ctx, req)
	for err == nil && req.NumRows() != 0 {
		for row := it.Begin(); row != it.End(); row = it.Next() {
			user := row.GetString(0)
			host := row.GetString(1)
			pass := row.GetString(2)
			var newPass string
			newPass, err = oldPasswordUpgrade(pass)
			terror.MustNil(err)
			updateSQL := fmt.Sprintf(`UPDATE HIGH_PRIORITY mysql.user SET password = "%s" WHERE user="%s" AND host="%s"`, newPass, user, host)
			sqls = append(sqls, updateSQL)
		}
		err = rs.Next(ctx, req)
	}
	terror.MustNil(err)

	for _, sql := range sqls {
		mustExecute(s, sql)
	}

	sql = fmt.Sprintf(`INSERT HIGH_PRIORITY INTO %s.%s VALUES ("%s", "%d", "TiDB bootstrap version.") ON DUPLICATE KEY UPDATE VARIABLE_VALUE="%d"`,
		mysql.SystemDB, mysql.TiDBTable, tidbServerVersionVar, version12, version12)
	mustExecute(s, sql)

	mustExecute(s, "COMMIT")
}

func upgradeToVer13(s sessionapi.Session, _ int64) {
	sqls := []string{
		"ALTER TABLE mysql.user ADD COLUMN `Create_tmp_table_priv` ENUM('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Super_priv`",
		"ALTER TABLE mysql.user ADD COLUMN `Lock_tables_priv` ENUM('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Create_tmp_table_priv`",
		"ALTER TABLE mysql.user ADD COLUMN `Create_view_priv` ENUM('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Execute_priv`",
		"ALTER TABLE mysql.user ADD COLUMN `Show_view_priv` ENUM('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Create_view_priv`",
		"ALTER TABLE mysql.user ADD COLUMN `Create_routine_priv` ENUM('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Show_view_priv`",
		"ALTER TABLE mysql.user ADD COLUMN `Alter_routine_priv` ENUM('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Create_routine_priv`",
		"ALTER TABLE mysql.user ADD COLUMN `Event_priv` ENUM('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Create_user_priv`",
	}
	for _, sql := range sqls {
		doReentrantDDL(s, sql, infoschema.ErrColumnExists)
	}
	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.user SET Create_tmp_table_priv='Y',Lock_tables_priv='Y',Create_routine_priv='Y',Alter_routine_priv='Y',Event_priv='Y' WHERE Super_priv='Y'")
	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.user SET Create_view_priv='Y',Show_view_priv='Y' WHERE Create_priv='Y'")
}

func upgradeToVer14(s sessionapi.Session, _ int64) {
	sqls := []string{
		"ALTER TABLE mysql.db ADD COLUMN `References_priv` ENUM('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Grant_priv`",
		"ALTER TABLE mysql.db ADD COLUMN `Create_tmp_table_priv` ENUM('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Alter_priv`",
		"ALTER TABLE mysql.db ADD COLUMN `Lock_tables_priv` ENUM('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Create_tmp_table_priv`",
		"ALTER TABLE mysql.db ADD COLUMN `Create_view_priv` ENUM('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Lock_tables_priv`",
		"ALTER TABLE mysql.db ADD COLUMN `Show_view_priv` ENUM('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Create_view_priv`",
		"ALTER TABLE mysql.db ADD COLUMN `Create_routine_priv` ENUM('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Show_view_priv`",
		"ALTER TABLE mysql.db ADD COLUMN `Alter_routine_priv` ENUM('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Create_routine_priv`",
		"ALTER TABLE mysql.db ADD COLUMN `Event_priv` ENUM('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Execute_priv`",
		"ALTER TABLE mysql.db ADD COLUMN `Trigger_priv` ENUM('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Event_priv`",
	}
	for _, sql := range sqls {
		doReentrantDDL(s, sql, infoschema.ErrColumnExists)
	}
}

func upgradeToVer15(s sessionapi.Session, _ int64) {
	doReentrantDDL(s, metadef.CreateGCDeleteRangeTable)
}

func upgradeToVer16(s sessionapi.Session, _ int64) {
	doReentrantDDL(s, "ALTER TABLE mysql.stats_histograms ADD COLUMN `cm_sketch` BLOB", infoschema.ErrColumnExists)
}

func upgradeToVer17(s sessionapi.Session, _ int64) {
	doReentrantDDL(s, "ALTER TABLE mysql.user MODIFY User CHAR(32)")
}

func upgradeToVer18(s sessionapi.Session, _ int64) {
	doReentrantDDL(s, "ALTER TABLE mysql.stats_histograms ADD COLUMN `tot_col_size` BIGINT(64) NOT NULL DEFAULT 0", infoschema.ErrColumnExists)
}

func upgradeToVer19(s sessionapi.Session, _ int64) {
	doReentrantDDL(s, "ALTER TABLE mysql.db MODIFY User CHAR(32)")
	doReentrantDDL(s, "ALTER TABLE mysql.tables_priv MODIFY User CHAR(32)")
	doReentrantDDL(s, "ALTER TABLE mysql.columns_priv MODIFY User CHAR(32)")
}

func upgradeToVer20(s sessionapi.Session, _ int64) {
	// NOTE: Feedback is deprecated, but we still need to create this table for compatibility.
	doReentrantDDL(s, metadef.CreateStatsFeedbackTable)
}

func upgradeToVer21(s sessionapi.Session, _ int64) {
	mustExecute(s, metadef.CreateGCDeleteRangeDoneTable)

	doReentrantDDL(s, "ALTER TABLE mysql.gc_delete_range DROP INDEX job_id", dbterror.ErrCantDropFieldOrKey)
	doReentrantDDL(s, "ALTER TABLE mysql.gc_delete_range ADD UNIQUE INDEX delete_range_index (job_id, element_id)", dbterror.ErrDupKeyName)
	doReentrantDDL(s, "ALTER TABLE mysql.gc_delete_range DROP INDEX element_id", dbterror.ErrCantDropFieldOrKey)
}

func upgradeToVer22(s sessionapi.Session, _ int64) {
	doReentrantDDL(s, "ALTER TABLE mysql.stats_histograms ADD COLUMN `stats_ver` BIGINT(64) NOT NULL DEFAULT 0", infoschema.ErrColumnExists)
}

func upgradeToVer23(s sessionapi.Session, _ int64) {
	doReentrantDDL(s, "ALTER TABLE mysql.stats_histograms ADD COLUMN `flag` BIGINT(64) NOT NULL DEFAULT 0", infoschema.ErrColumnExists)
}

// writeSystemTZ writes system timezone info into mysql.tidb
func writeSystemTZ(s sessionapi.Session) {
	mustExecute(s, `INSERT HIGH_PRIORITY INTO %n.%n VALUES (%?, %?, "TiDB Global System Timezone.") ON DUPLICATE KEY UPDATE VARIABLE_VALUE= %?`,
		mysql.SystemDB,
		mysql.TiDBTable,
		tidbSystemTZ,
		timeutil.InferSystemTZ(),
		timeutil.InferSystemTZ(),
	)
}

// upgradeToVer24 initializes `System` timezone according to docs/design/2018-09-10-adding-tz-env.md
func upgradeToVer24(s sessionapi.Session, _ int64) {
	writeSystemTZ(s)
}

// upgradeToVer25 updates tidb_max_chunk_size to new low bound value 32 if previous value is small than 32.
func upgradeToVer25(s sessionapi.Session, _ int64) {
	sql := fmt.Sprintf("UPDATE HIGH_PRIORITY %[1]s.%[2]s SET VARIABLE_VALUE = '%[4]d' WHERE VARIABLE_NAME = '%[3]s' AND VARIABLE_VALUE < %[4]d",
		mysql.SystemDB, mysql.GlobalVariablesTable, vardef.TiDBMaxChunkSize, vardef.DefInitChunkSize)
	mustExecute(s, sql)
}

func upgradeToVer26(s sessionapi.Session, _ int64) {
	mustExecute(s, metadef.CreateRoleEdgesTable)
	mustExecute(s, metadef.CreateDefaultRolesTable)
	doReentrantDDL(s, "ALTER TABLE mysql.user ADD COLUMN `Create_role_priv` ENUM('N','Y') DEFAULT 'N'", infoschema.ErrColumnExists)
	doReentrantDDL(s, "ALTER TABLE mysql.user ADD COLUMN `Drop_role_priv` ENUM('N','Y') DEFAULT 'N'", infoschema.ErrColumnExists)
	doReentrantDDL(s, "ALTER TABLE mysql.user ADD COLUMN `Account_locked` ENUM('N','Y') DEFAULT 'N'", infoschema.ErrColumnExists)
	// user with Create_user_Priv privilege should have Create_view_priv and Show_view_priv after upgrade to v3.0
	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.user SET Create_role_priv='Y',Drop_role_priv='Y' WHERE Create_user_priv='Y'")
	// user with Create_Priv privilege should have Create_view_priv and Show_view_priv after upgrade to v3.0
	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.user SET Create_view_priv='Y',Show_view_priv='Y' WHERE Create_priv='Y'")
}

func upgradeToVer27(s sessionapi.Session, _ int64) {
	doReentrantDDL(s, "ALTER TABLE mysql.stats_histograms ADD COLUMN `correlation` DOUBLE NOT NULL DEFAULT 0", infoschema.ErrColumnExists)
}

func upgradeToVer28(s sessionapi.Session, _ int64) {
	doReentrantDDL(s, metadef.CreateBindInfoTable)
}

func upgradeToVer29(s sessionapi.Session, ver int64) {
	// upgradeToVer29 only need to be run when the current version is 28.
	if ver != version28 {
		return
	}
	doReentrantDDL(s, "ALTER TABLE mysql.bind_info CHANGE create_time create_time TIMESTAMP(3)")
	doReentrantDDL(s, "ALTER TABLE mysql.bind_info CHANGE update_time update_time TIMESTAMP(3)")
	doReentrantDDL(s, "ALTER TABLE mysql.bind_info ADD INDEX sql_index (original_sql(1024),default_db(1024))", dbterror.ErrDupKeyName)
}

func upgradeToVer30(s sessionapi.Session, _ int64) {
	mustExecute(s, metadef.CreateStatsTopNTable)
}

func upgradeToVer31(s sessionapi.Session, _ int64) {
	doReentrantDDL(s, "ALTER TABLE mysql.stats_histograms ADD COLUMN `last_analyze_pos` BLOB DEFAULT NULL", infoschema.ErrColumnExists)
}

func upgradeToVer32(s sessionapi.Session, _ int64) {
	doReentrantDDL(s, "ALTER TABLE mysql.tables_priv MODIFY table_priv SET('Select','Insert','Update','Delete','Create','Drop','Grant', 'Index', 'Alter', 'Create View', 'Show View', 'Trigger', 'References')")
}

func upgradeToVer33(s sessionapi.Session, _ int64) {
	doReentrantDDL(s, metadef.CreateExprPushdownBlacklistTable)
}

func upgradeToVer34(s sessionapi.Session, _ int64) {
	doReentrantDDL(s, metadef.CreateOptRuleBlacklistTable)
}

func upgradeToVer35(s sessionapi.Session, _ int64) {
	sql := fmt.Sprintf("UPDATE HIGH_PRIORITY %s.%s SET VARIABLE_NAME = '%s' WHERE VARIABLE_NAME = 'tidb_back_off_weight'",
		mysql.SystemDB, mysql.GlobalVariablesTable, vardef.TiDBBackOffWeight)
	mustExecute(s, sql)
}

func upgradeToVer36(s sessionapi.Session, _ int64) {
	doReentrantDDL(s, "ALTER TABLE mysql.user ADD COLUMN `Shutdown_priv` ENUM('N','Y') DEFAULT 'N'", infoschema.ErrColumnExists)
	// A root user will have those privileges after upgrading.
	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.user SET Shutdown_priv='Y' WHERE Super_priv='Y'")
	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.user SET Create_tmp_table_priv='Y',Lock_tables_priv='Y',Create_routine_priv='Y',Alter_routine_priv='Y',Event_priv='Y' WHERE Super_priv='Y'")
}

func upgradeToVer37(s sessionapi.Session, _ int64) {
	// when upgrade from old tidb and no 'tidb_enable_window_function' in GLOBAL_VARIABLES, init it with 0.
	sql := fmt.Sprintf("INSERT IGNORE INTO  %s.%s (`VARIABLE_NAME`, `VARIABLE_VALUE`) VALUES ('%s', '%d')",
		mysql.SystemDB, mysql.GlobalVariablesTable, vardef.TiDBEnableWindowFunction, 0)
	mustExecute(s, sql)
}

func upgradeToVer38(s sessionapi.Session, _ int64) {
	doReentrantDDL(s, metadef.CreateGlobalPrivTable)
}

func writeNewCollationParameter(s sessionapi.Session, flag bool) {
	comment := "If the new collations are enabled. Do not edit it."
	b := varFalse
	if flag {
		b = varTrue
	}
	mustExecute(s, `INSERT HIGH_PRIORITY INTO %n.%n VALUES (%?, %?, %?) ON DUPLICATE KEY UPDATE VARIABLE_VALUE=%?`,
		mysql.SystemDB, mysql.TiDBTable, TidbNewCollationEnabled, b, comment, b,
	)
}

func upgradeToVer40(s sessionapi.Session, _ int64) {
	// There is no way to enable new collation for an existing TiDB cluster.
	writeNewCollationParameter(s, false)
}

func upgradeToVer41(s sessionapi.Session, _ int64) {
	doReentrantDDL(s, "ALTER TABLE mysql.user CHANGE `password` `authentication_string` TEXT", infoschema.ErrColumnExists, infoschema.ErrColumnNotExists)
	doReentrantDDL(s, "ALTER TABLE mysql.user ADD COLUMN `password` TEXT as (`authentication_string`)", infoschema.ErrColumnExists)
}

// writeDefaultExprPushDownBlacklist writes default expr pushdown blacklist into mysql.expr_pushdown_blacklist
func writeDefaultExprPushDownBlacklist(s sessionapi.Session) {
	mustExecute(s, "INSERT HIGH_PRIORITY INTO mysql.expr_pushdown_blacklist VALUES"+
		"('date_add','tiflash', 'DST(daylight saving time) does not take effect in TiFlash date_add')")
}

func upgradeToVer42(s sessionapi.Session, _ int64) {
	doReentrantDDL(s, "ALTER TABLE mysql.expr_pushdown_blacklist ADD COLUMN `store_type` CHAR(100) NOT NULL DEFAULT 'tikv,tiflash,tidb'", infoschema.ErrColumnExists)
	doReentrantDDL(s, "ALTER TABLE mysql.expr_pushdown_blacklist ADD COLUMN `reason` VARCHAR(200)", infoschema.ErrColumnExists)
	writeDefaultExprPushDownBlacklist(s)
}

// Convert statement summary global variables to non-empty values.
func writeStmtSummaryVars(s sessionapi.Session) {
	sql := "UPDATE %n.%n SET variable_value= %? WHERE variable_name= %? AND variable_value=''"
	mustExecute(s, sql, mysql.SystemDB, mysql.GlobalVariablesTable, variable.BoolToOnOff(vardef.DefTiDBEnableStmtSummary), vardef.TiDBEnableStmtSummary)
	mustExecute(s, sql, mysql.SystemDB, mysql.GlobalVariablesTable, variable.BoolToOnOff(vardef.DefTiDBStmtSummaryInternalQuery), vardef.TiDBStmtSummaryInternalQuery)
	mustExecute(s, sql, mysql.SystemDB, mysql.GlobalVariablesTable, strconv.Itoa(vardef.DefTiDBStmtSummaryRefreshInterval), vardef.TiDBStmtSummaryRefreshInterval)
	mustExecute(s, sql, mysql.SystemDB, mysql.GlobalVariablesTable, strconv.Itoa(vardef.DefTiDBStmtSummaryHistorySize), vardef.TiDBStmtSummaryHistorySize)
	mustExecute(s, sql, mysql.SystemDB, mysql.GlobalVariablesTable, strconv.FormatUint(uint64(vardef.DefTiDBStmtSummaryMaxStmtCount), 10), vardef.TiDBStmtSummaryMaxStmtCount)
	mustExecute(s, sql, mysql.SystemDB, mysql.GlobalVariablesTable, strconv.FormatUint(uint64(vardef.DefTiDBStmtSummaryMaxSQLLength), 10), vardef.TiDBStmtSummaryMaxSQLLength)
}

func upgradeToVer43(s sessionapi.Session, _ int64) {
	writeStmtSummaryVars(s)
}

func upgradeToVer44(s sessionapi.Session, _ int64) {
	mustExecute(s, "DELETE FROM mysql.global_variables where variable_name = \"tidb_isolation_read_engines\"")
}

func upgradeToVer45(s sessionapi.Session, _ int64) {
	doReentrantDDL(s, "ALTER TABLE mysql.user ADD COLUMN `Config_priv` ENUM('N','Y') DEFAULT 'N'", infoschema.ErrColumnExists)
	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.user SET Config_priv='Y' WHERE Super_priv='Y'")
}

// In v3.1.1, we wrongly replace the context of upgradeToVer39 with upgradeToVer44. If we upgrade from v3.1.1 to a newer version,
// upgradeToVer39 will be missed. So we redo upgradeToVer39 here to make sure the upgrading from v3.1.1 succeed.
func upgradeToVer46(s sessionapi.Session, _ int64) {
	doReentrantDDL(s, "ALTER TABLE mysql.user ADD COLUMN `Reload_priv` ENUM('N','Y') DEFAULT 'N'", infoschema.ErrColumnExists)
	doReentrantDDL(s, "ALTER TABLE mysql.user ADD COLUMN `File_priv` ENUM('N','Y') DEFAULT 'N'", infoschema.ErrColumnExists)
	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.user SET Reload_priv='Y' WHERE Super_priv='Y'")
	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.user SET File_priv='Y' WHERE Super_priv='Y'")
}

func upgradeToVer47(s sessionapi.Session, _ int64) {
	doReentrantDDL(s, "ALTER TABLE mysql.bind_info ADD COLUMN `source` varchar(10) NOT NULL default 'unknown'", infoschema.ErrColumnExists)
}

func upgradeToVer50(s sessionapi.Session, _ int64) {
	doReentrantDDL(s, metadef.CreateSchemaIndexUsageTable)
}

func upgradeToVer52(s sessionapi.Session, _ int64) {
	doReentrantDDL(s, "ALTER TABLE mysql.stats_histograms MODIFY cm_sketch BLOB(6291456)")
}

func upgradeToVer53(s sessionapi.Session, _ int64) {
	// when upgrade from old tidb and no `tidb_enable_strict_double_type_check` in GLOBAL_VARIABLES, init it with 1`
	sql := fmt.Sprintf("INSERT IGNORE INTO %s.%s (`VARIABLE_NAME`, `VARIABLE_VALUE`) VALUES ('%s', '%d')",
		mysql.SystemDB, mysql.GlobalVariablesTable, vardef.TiDBEnableStrictDoubleTypeCheck, 0)
	mustExecute(s, sql)
}

func upgradeToVer54(s sessionapi.Session, ver int64) {
	// The mem-query-quota default value is 32GB by default in v3.0, and 1GB by
	// default in v4.0.
	// If a cluster is upgraded from v3.0.x (bootstrapVer <= version38) to
	// v4.0.9+, we'll write the default value to mysql.tidb. Thus we can get the
	// default value of mem-quota-query, and promise the compatibility even if
	// the tidb-server restarts.
	// If it's a newly deployed cluster, we do not need to write the value into
	// mysql.tidb, since no compatibility problem will happen.

	// This bootstrap task becomes obsolete in TiDB 5.0+, because it appears that the
	// default value of mem-quota-query changes back to 1GB. In TiDB 6.1+ mem-quota-query
	// is no longer a config option, but instead a system variable (tidb_mem_quota_query).

	if ver <= version38 {
		writeMemoryQuotaQuery(s)
	}
}

// When cherry-pick upgradeToVer52 to v4.0, we wrongly name it upgradeToVer48.
// If we upgrade from v4.0 to a newer version, the real upgradeToVer48 will be missed.
// So we redo upgradeToVer48 here to make sure the upgrading from v4.0 succeeds.
func upgradeToVer55(s sessionapi.Session, _ int64) {
	defValues := map[string]string{
		vardef.TiDBIndexLookupConcurrency:     "4",
		vardef.TiDBIndexLookupJoinConcurrency: "4",
		vardef.TiDBHashAggFinalConcurrency:    "4",
		vardef.TiDBHashAggPartialConcurrency:  "4",
		vardef.TiDBWindowConcurrency:          "4",
		vardef.TiDBProjectionConcurrency:      "4",
		vardef.TiDBHashJoinConcurrency:        "5",
	}
	names := make([]string, 0, len(defValues))
	for n := range defValues {
		names = append(names, n)
	}

	selectSQL := "select HIGH_PRIORITY * from mysql.global_variables where variable_name in ('" + strings.Join(names, quoteCommaQuote) + "')"
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnBootstrap)
	rs, err := s.ExecuteInternal(ctx, selectSQL)
	terror.MustNil(err)
	defer terror.Call(rs.Close)
	req := rs.NewChunk(nil)
	it := chunk.NewIterator4Chunk(req)
	err = rs.Next(ctx, req)
	for err == nil && req.NumRows() != 0 {
		for row := it.Begin(); row != it.End(); row = it.Next() {
			n := strings.ToLower(row.GetString(0))
			v := row.GetString(1)
			if defValue, ok := defValues[n]; !ok || defValue != v {
				return
			}
		}
		err = rs.Next(ctx, req)
	}
	terror.MustNil(err)

	mustExecute(s, "BEGIN")
	v := strconv.Itoa(vardef.ConcurrencyUnset)
	sql := fmt.Sprintf("UPDATE %s.%s SET variable_value='%%s' WHERE variable_name='%%s'", mysql.SystemDB, mysql.GlobalVariablesTable)
	for _, name := range names {
		mustExecute(s, fmt.Sprintf(sql, v, name))
	}
	mustExecute(s, "COMMIT")
}

// When cherry-pick upgradeToVer54 to v4.0, we wrongly name it upgradeToVer49.
// If we upgrade from v4.0 to a newer version, the real upgradeToVer49 will be missed.
// So we redo upgradeToVer49 here to make sure the upgrading from v4.0 succeeds.
func upgradeToVer56(s sessionapi.Session, _ int64) {
	doReentrantDDL(s, metadef.CreateStatsExtendedTable)
}

func upgradeToVer57(s sessionapi.Session, _ int64) {
	insertBuiltinBindInfoRow(s)
}

func insertBuiltinBindInfoRow(s sessionapi.Session) {
	mustExecute(s, `INSERT HIGH_PRIORITY INTO mysql.bind_info(original_sql, bind_sql, default_db, status, create_time, update_time, charset, collation, source)
						VALUES (%?, %?, "mysql", %?, "0000-00-00 00:00:00", "0000-00-00 00:00:00", "", "", %?)`,
		bindinfo.BuiltinPseudoSQL4BindLock, bindinfo.BuiltinPseudoSQL4BindLock, bindinfo.StatusBuiltin, bindinfo.StatusBuiltin,
	)
}

func upgradeToVer59(s sessionapi.Session, _ int64) {
	// The oom-action default value is log by default in v3.0, and cancel by
	// default in v4.0.11+.
	// If a cluster is upgraded from v3.0.x (bootstrapVer <= version59) to
	// v4.0.11+, we'll write the default value to mysql.tidb. Thus we can get
	// the default value of oom-action, and promise the compatibility even if
	// the tidb-server restarts.
	// If it's a newly deployed cluster, we do not need to write the value into
	// mysql.tidb, since no compatibility problem will happen.
	writeOOMAction(s)
}

func upgradeToVer60(s sessionapi.Session, _ int64) {
	mustExecute(s, "DROP TABLE IF EXISTS mysql.stats_extended")
	doReentrantDDL(s, metadef.CreateStatsExtendedTable)
}

type bindInfo struct {
	bindSQL    string
	status     string
	createTime types.Time
	charset    string
	collation  string
	source     string
}

func upgradeToVer67(s sessionapi.Session, _ int64) {
	bindMap := make(map[string]bindInfo)
	var err error
	mustExecute(s, "BEGIN PESSIMISTIC")

	defer func() {
		if err != nil {
			mustExecute(s, "ROLLBACK")
			return
		}

		mustExecute(s, "COMMIT")
	}()
	mustExecute(s, bindinfo.LockBindInfoSQL)
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnBootstrap)
	var rs sqlexec.RecordSet
	rs, err = s.ExecuteInternal(ctx,
		`SELECT bind_sql, default_db, status, create_time, charset, collation, source
			FROM mysql.bind_info
			WHERE source != 'builtin'
			ORDER BY update_time DESC`)
	if err != nil {
		logutil.BgLogger().Fatal("upgradeToVer67 error", zap.Error(err))
	}
	req := rs.NewChunk(nil)
	iter := chunk.NewIterator4Chunk(req)
	p := parser.New()
	now := types.NewTime(types.FromGoTime(time.Now()), mysql.TypeTimestamp, 3)
	for {
		err = rs.Next(context.TODO(), req)
		if err != nil {
			logutil.BgLogger().Fatal("upgradeToVer67 error", zap.Error(err))
		}
		if req.NumRows() == 0 {
			break
		}
		updateBindInfo(iter, p, bindMap)
	}
	terror.Call(rs.Close)

	mustExecute(s, "DELETE FROM mysql.bind_info where source != 'builtin'")
	for original, bind := range bindMap {
		mustExecute(s, fmt.Sprintf("INSERT INTO mysql.bind_info VALUES(%s, %s, '', %s, %s, %s, %s, %s, %s)",
			expression.Quote(original),
			expression.Quote(bind.bindSQL),
			expression.Quote(bind.status),
			expression.Quote(bind.createTime.String()),
			expression.Quote(now.String()),
			expression.Quote(bind.charset),
			expression.Quote(bind.collation),
			expression.Quote(bind.source),
		))
	}
}

func updateBindInfo(iter *chunk.Iterator4Chunk, p *parser.Parser, bindMap map[string]bindInfo) {
	for row := iter.Begin(); row != iter.End(); row = iter.Next() {
		bind := row.GetString(0)
		db := row.GetString(1)
		status := row.GetString(2)

		if status != bindinfo.StatusEnabled && status != bindinfo.StatusUsing && status != bindinfo.StatusBuiltin {
			continue
		}

		charset := row.GetString(4)
		collation := row.GetString(5)
		stmt, err := p.ParseOneStmt(bind, charset, collation)
		if err != nil {
			logutil.BgLogger().Fatal("updateBindInfo error", zap.Error(err))
		}
		originWithDB := parser.Normalize(utilparser.RestoreWithDefaultDB(stmt, db, bind), "ON")
		if _, ok := bindMap[originWithDB]; ok {
			// The results are sorted in descending order of time.
			// And in the following cases, duplicate originWithDB may occur
			//      originalText         	|bindText                                   	|DB
			//		`select * from t` 		|`select /*+ use_index(t, idx) */ * from t` 	|`test`
			// 		`select * from test.t`  |`select /*+ use_index(t, idx) */ * from test.t`|``
			// Therefore, if repeated, we can skip to keep the latest binding.
			continue
		}
		bindMap[originWithDB] = bindInfo{
			bindSQL:    utilparser.RestoreWithDefaultDB(stmt, db, bind),
			status:     status,
			createTime: row.GetTime(3),
			charset:    charset,
			collation:  collation,
			source:     row.GetString(6),
		}
	}
}

func writeMemoryQuotaQuery(s sessionapi.Session) {
	comment := "memory_quota_query is 32GB by default in v3.0.x, 1GB by default in v4.0.x+"
	mustExecute(s, `INSERT HIGH_PRIORITY INTO %n.%n VALUES (%?, %?, %?) ON DUPLICATE KEY UPDATE VARIABLE_VALUE=%?`,
		mysql.SystemDB, mysql.TiDBTable, tidbDefMemoryQuotaQuery, 32<<30, comment, 32<<30,
	)
}

func upgradeToVer62(s sessionapi.Session, _ int64) {
	doReentrantDDL(s, "ALTER TABLE mysql.stats_buckets ADD COLUMN `ndv` bigint not null default 0", infoschema.ErrColumnExists)
}

func upgradeToVer63(s sessionapi.Session, _ int64) {
	doReentrantDDL(s, "ALTER TABLE mysql.user ADD COLUMN `Create_tablespace_priv` ENUM('N','Y') DEFAULT 'N'", infoschema.ErrColumnExists)
	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.user SET Create_tablespace_priv='Y' where Super_priv='Y'")
}

func upgradeToVer64(s sessionapi.Session, _ int64) {
	doReentrantDDL(s, "ALTER TABLE mysql.user ADD COLUMN `Repl_slave_priv` ENUM('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Execute_priv`", infoschema.ErrColumnExists)
	doReentrantDDL(s, "ALTER TABLE mysql.user ADD COLUMN `Repl_client_priv` ENUM('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Repl_slave_priv`", infoschema.ErrColumnExists)
	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.user SET Repl_slave_priv='Y',Repl_client_priv='Y' where Super_priv='Y'")
}

func upgradeToVer65(s sessionapi.Session, _ int64) {
	doReentrantDDL(s, metadef.CreateStatsFMSketchTable)
}

func upgradeToVer66(s sessionapi.Session, _ int64) {
	mustExecute(s, "set @@global.tidb_track_aggregate_memory_usage = 1")
}

func upgradeToVer68(s sessionapi.Session, _ int64) {
	mustExecute(s, "DELETE FROM mysql.global_variables where VARIABLE_NAME = 'tidb_enable_clustered_index' and VARIABLE_VALUE = 'OFF'")
}

func upgradeToVer69(s sessionapi.Session, _ int64) {
	doReentrantDDL(s, metadef.CreateGlobalGrantsTable)
}

func upgradeToVer70(s sessionapi.Session, _ int64) {
	doReentrantDDL(s, "ALTER TABLE mysql.user ADD COLUMN plugin CHAR(64) AFTER authentication_string", infoschema.ErrColumnExists)
	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.user SET plugin='mysql_native_password'")
}

func upgradeToVer71(s sessionapi.Session, _ int64) {
	mustExecute(s, "UPDATE mysql.global_variables SET VARIABLE_VALUE='OFF' WHERE VARIABLE_NAME = 'tidb_multi_statement_mode' AND VARIABLE_VALUE = 'WARN'")
}

func upgradeToVer72(s sessionapi.Session, _ int64) {
	doReentrantDDL(s, "ALTER TABLE mysql.stats_meta ADD COLUMN snapshot BIGINT(64) UNSIGNED NOT NULL DEFAULT 0", infoschema.ErrColumnExists)
}

func upgradeToVer73(s sessionapi.Session, _ int64) {
	doReentrantDDL(s, metadef.CreateCapturePlanBaselinesBlacklistTable)
}

func upgradeToVer74(s sessionapi.Session, _ int64) {
	// The old default value of `tidb_stmt_summary_max_stmt_count` is 200, we want to enlarge this to the new default value when TiDB upgrade.
	mustExecute(s, fmt.Sprintf("UPDATE mysql.global_variables SET VARIABLE_VALUE='%[1]v' WHERE VARIABLE_NAME = 'tidb_stmt_summary_max_stmt_count' AND CAST(VARIABLE_VALUE AS SIGNED) = 200", vardef.DefTiDBStmtSummaryMaxStmtCount))
}

func upgradeToVer75(s sessionapi.Session, _ int64) {
	doReentrantDDL(s, "ALTER TABLE mysql.user MODIFY COLUMN Host CHAR(255)")
	doReentrantDDL(s, "ALTER TABLE mysql.global_priv MODIFY COLUMN Host CHAR(255)")
	doReentrantDDL(s, "ALTER TABLE mysql.db MODIFY COLUMN Host CHAR(255)")
	doReentrantDDL(s, "ALTER TABLE mysql.tables_priv MODIFY COLUMN Host CHAR(255)")
	doReentrantDDL(s, "ALTER TABLE mysql.columns_priv MODIFY COLUMN Host CHAR(255)")
}

func upgradeToVer76(s sessionapi.Session, _ int64) {
	doReentrantDDL(s, "ALTER TABLE mysql.columns_priv MODIFY COLUMN Column_priv SET('Select','Insert','Update','References')")
}

func upgradeToVer77(s sessionapi.Session, _ int64) {
	doReentrantDDL(s, metadef.CreateColumnStatsUsageTable)
}

func upgradeToVer78(s sessionapi.Session, _ int64) {
	doReentrantDDL(s, "ALTER TABLE mysql.stats_buckets MODIFY upper_bound LONGBLOB NOT NULL")
	doReentrantDDL(s, "ALTER TABLE mysql.stats_buckets MODIFY lower_bound LONGBLOB")
	doReentrantDDL(s, "ALTER TABLE mysql.stats_histograms MODIFY last_analyze_pos LONGBLOB DEFAULT NULL")
}

func upgradeToVer79(s sessionapi.Session, _ int64) {
	doReentrantDDL(s, metadef.CreateTableCacheMetaTable)
}

func upgradeToVer80(s sessionapi.Session, _ int64) {
	// Check if tidb_analyze_version exists in mysql.GLOBAL_VARIABLES.
	// If not, insert "tidb_analyze_version | 1" since this is the old behavior before we introduce this variable.
	initGlobalVariableIfNotExists(s, vardef.TiDBAnalyzeVersion, 1)
}

// For users that upgrade TiDB from a pre-4.0 version, we want to disable index merge by default.
// This helps minimize query plan regressions.
func upgradeToVer81(s sessionapi.Session, _ int64) {
	// Check if tidb_enable_index_merge exists in mysql.GLOBAL_VARIABLES.
	// If not, insert "tidb_enable_index_merge | off".
	initGlobalVariableIfNotExists(s, vardef.TiDBEnableIndexMerge, vardef.Off)
}

func upgradeToVer82(s sessionapi.Session, _ int64) {
	doReentrantDDL(s, metadef.CreateAnalyzeOptionsTable)
}

func upgradeToVer83(s sessionapi.Session, _ int64) {
	doReentrantDDL(s, metadef.CreateStatsHistoryTable)
}

func upgradeToVer84(s sessionapi.Session, _ int64) {
	doReentrantDDL(s, metadef.CreateStatsMetaHistoryTable)
}

func upgradeToVer85(s sessionapi.Session, _ int64) {
	mustExecute(s, fmt.Sprintf("UPDATE HIGH_PRIORITY mysql.bind_info SET status= '%s' WHERE status = '%s'", bindinfo.StatusEnabled, bindinfo.StatusUsing))
}

func upgradeToVer86(s sessionapi.Session, _ int64) {
	doReentrantDDL(s, "ALTER TABLE mysql.tables_priv MODIFY COLUMN Column_priv SET('Select','Insert','Update','References')")
}

func upgradeToVer87(s sessionapi.Session, _ int64) {
	doReentrantDDL(s, metadef.CreateAnalyzeJobsTable)
}

func upgradeToVer88(s sessionapi.Session, _ int64) {
	doReentrantDDL(s, "ALTER TABLE mysql.user CHANGE `Repl_slave_priv` `Repl_slave_priv` ENUM('N','Y') NOT NULL DEFAULT 'N' AFTER `Execute_priv`")
	doReentrantDDL(s, "ALTER TABLE mysql.user CHANGE `Repl_client_priv` `Repl_client_priv` ENUM('N','Y') NOT NULL DEFAULT 'N' AFTER `Repl_slave_priv`")
}

func upgradeToVer89(s sessionapi.Session, _ int64) {
	doReentrantDDL(s, metadef.CreateAdvisoryLocksTable)
}

// importConfigOption is a one-time import.
// It is intended to be used to convert a config option to a sysvar.
// It reads the config value from the tidb-server executing the bootstrap
// (not guaranteed to be the same on all servers), and writes a message
// to the error log. The message is important since the behavior is weird
// (changes to the config file will no longer take effect past this point).
func importConfigOption(s sessionapi.Session, configName, svName, valStr string) {
	message := fmt.Sprintf("%s is now configured by the system variable %s. One-time importing the value specified in tidb.toml file", configName, svName)
	logutil.BgLogger().Warn(message, zap.String("value", valStr))
	// We use insert ignore, since if its a duplicate we don't want to overwrite any user-set values.
	sql := fmt.Sprintf("INSERT IGNORE INTO  %s.%s (`VARIABLE_NAME`, `VARIABLE_VALUE`) VALUES ('%s', '%s')",
		mysql.SystemDB, mysql.GlobalVariablesTable, svName, valStr)
	mustExecute(s, sql)
}

func upgradeToVer90(s sessionapi.Session, _ int64) {
	valStr := variable.BoolToOnOff(config.GetGlobalConfig().EnableBatchDML)
	importConfigOption(s, "enable-batch-dml", vardef.TiDBEnableBatchDML, valStr)
	valStr = fmt.Sprint(config.GetGlobalConfig().MemQuotaQuery)
	importConfigOption(s, "mem-quota-query", vardef.TiDBMemQuotaQuery, valStr)
	valStr = fmt.Sprint(config.GetGlobalConfig().Log.QueryLogMaxLen)
	importConfigOption(s, "query-log-max-len", vardef.TiDBQueryLogMaxLen, valStr)
	valStr = fmt.Sprint(config.GetGlobalConfig().Performance.CommitterConcurrency)
	importConfigOption(s, "committer-concurrency", vardef.TiDBCommitterConcurrency, valStr)
	valStr = variable.BoolToOnOff(config.GetGlobalConfig().Performance.RunAutoAnalyze)
	importConfigOption(s, "run-auto-analyze", vardef.TiDBEnableAutoAnalyze, valStr)
	valStr = config.GetGlobalConfig().OOMAction
	importConfigOption(s, "oom-action", vardef.TiDBMemOOMAction, valStr)
}

func upgradeToVer91(s sessionapi.Session, _ int64) {
	valStr := variable.BoolToOnOff(config.GetGlobalConfig().PreparedPlanCache.Enabled)
	importConfigOption(s, "prepared-plan-cache.enable", vardef.TiDBEnablePrepPlanCache, valStr)

	valStr = strconv.Itoa(int(config.GetGlobalConfig().PreparedPlanCache.Capacity))
	importConfigOption(s, "prepared-plan-cache.capacity", vardef.TiDBPrepPlanCacheSize, valStr)

	valStr = strconv.FormatFloat(config.GetGlobalConfig().PreparedPlanCache.MemoryGuardRatio, 'f', -1, 64)
	importConfigOption(s, "prepared-plan-cache.memory-guard-ratio", vardef.TiDBPrepPlanCacheMemoryGuardRatio, valStr)
}

func upgradeToVer93(s sessionapi.Session, _ int64) {
	valStr := variable.BoolToOnOff(config.GetGlobalConfig().OOMUseTmpStorage)
	importConfigOption(s, "oom-use-tmp-storage", vardef.TiDBEnableTmpStorageOnOOM, valStr)
}

func upgradeToVer94(s sessionapi.Session, _ int64) {
	mustExecute(s, metadef.CreateTiDBMDLView)
}

func upgradeToVer95(s sessionapi.Session, _ int64) {
	doReentrantDDL(s, "ALTER TABLE mysql.user ADD COLUMN IF NOT EXISTS `User_attributes` JSON")
}

func upgradeToVer97(s sessionapi.Session, _ int64) {
	// Check if tidb_opt_range_max_size exists in mysql.GLOBAL_VARIABLES.
	// If not, insert "tidb_opt_range_max_size | 0" since this is the old behavior before we introduce this variable.
	initGlobalVariableIfNotExists(s, vardef.TiDBOptRangeMaxSize, 0)
}

func upgradeToVer98(s sessionapi.Session, _ int64) {
	doReentrantDDL(s, "ALTER TABLE mysql.user ADD COLUMN IF NOT EXISTS `Token_issuer` varchar(255)")
}

func upgradeToVer99Before(s sessionapi.Session) {
	mustExecute(s, "INSERT HIGH_PRIORITY IGNORE INTO %n.%n VALUES (%?, %?);",
		mysql.SystemDB, mysql.GlobalVariablesTable, vardef.TiDBEnableMDL, 0)
}

func upgradeToVer99After(s sessionapi.Session) {
	sql := fmt.Sprintf("UPDATE HIGH_PRIORITY %[1]s.%[2]s SET VARIABLE_VALUE = %[4]d WHERE VARIABLE_NAME = '%[3]s'",
		mysql.SystemDB, mysql.GlobalVariablesTable, vardef.TiDBEnableMDL, 1)
	mustExecute(s, sql)
	err := kv.RunInNewTxn(kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL), s.GetStore(), true, func(_ context.Context, txn kv.Transaction) error {
		t := meta.NewMutator(txn)
		return t.SetMetadataLock(true)
	})
	terror.MustNil(err)
}

func upgradeToVer100(s sessionapi.Session, _ int64) {
	valStr := strconv.Itoa(int(config.GetGlobalConfig().Performance.ServerMemoryQuota))
	importConfigOption(s, "performance.server-memory-quota", vardef.TiDBServerMemoryLimit, valStr)
}

func upgradeToVer101(s sessionapi.Session, _ int64) {
	doReentrantDDL(s, metadef.CreatePlanReplayerStatusTable)
}

func upgradeToVer102(s sessionapi.Session, _ int64) {
	doReentrantDDL(s, metadef.CreatePlanReplayerTaskTable)
}

func upgradeToVer103(s sessionapi.Session, _ int64) {
	doReentrantDDL(s, metadef.CreateStatsTableLockedTable)
}

func upgradeToVer104(s sessionapi.Session, _ int64) {
	doReentrantDDL(s, "ALTER TABLE mysql.bind_info ADD COLUMN IF NOT EXISTS `sql_digest` varchar(64)")
	doReentrantDDL(s, "ALTER TABLE mysql.bind_info ADD COLUMN IF NOT EXISTS `plan_digest` varchar(64)")
}

// For users that upgrade TiDB from a pre-6.0 version, we want to disable tidb cost model2 by default to keep plans unchanged.
func upgradeToVer105(s sessionapi.Session, _ int64) {
	initGlobalVariableIfNotExists(s, vardef.TiDBCostModelVersion, "1")
}

func upgradeToVer106(s sessionapi.Session, _ int64) {
	doReentrantDDL(s, metadef.CreatePasswordHistoryTable)
	doReentrantDDL(s, "Alter table mysql.user add COLUMN IF NOT EXISTS `Password_reuse_history` smallint unsigned  DEFAULT NULL AFTER `Create_Tablespace_Priv` ")
	doReentrantDDL(s, "Alter table mysql.user add COLUMN IF NOT EXISTS `Password_reuse_time` smallint unsigned DEFAULT NULL AFTER `Password_reuse_history`")
}

func upgradeToVer107(s sessionapi.Session, _ int64) {
	doReentrantDDL(s, "ALTER TABLE mysql.user ADD COLUMN IF NOT EXISTS `Password_expired` ENUM('N','Y') NOT NULL DEFAULT 'N'")
	doReentrantDDL(s, "ALTER TABLE mysql.user ADD COLUMN IF NOT EXISTS `Password_last_changed` TIMESTAMP DEFAULT CURRENT_TIMESTAMP()")
	doReentrantDDL(s, "ALTER TABLE mysql.user ADD COLUMN IF NOT EXISTS `Password_lifetime` SMALLINT UNSIGNED DEFAULT NULL")
}

func upgradeToVer108(s sessionapi.Session, _ int64) {
	doReentrantDDL(s, metadef.CreateTiDBTTLTableStatusTable)
}

// For users that upgrade TiDB from a 6.2-6.4 version, we want to disable tidb gc_aware_memory_track by default.
func upgradeToVer109(s sessionapi.Session, _ int64) {
	mustExecute(s, "REPLACE HIGH_PRIORITY INTO %n.%n VALUES (%?, %?);",
		mysql.SystemDB, mysql.GlobalVariablesTable, vardef.TiDBEnableGCAwareMemoryTrack, 0)
}

// For users that upgrade TiDB from a 5.4-6.4 version, we want to enable tidb tidb_stats_load_pseudo_timeout by default.
func upgradeToVer110(s sessionapi.Session, _ int64) {
	mustExecute(s, "REPLACE HIGH_PRIORITY INTO %n.%n VALUES (%?, %?);",
		mysql.SystemDB, mysql.GlobalVariablesTable, vardef.TiDBStatsLoadPseudoTimeout, 1)
}

func upgradeToVer130(s sessionapi.Session, _ int64) {
	doReentrantDDL(s, "ALTER TABLE mysql.stats_meta_history ADD COLUMN IF NOT EXISTS `source` varchar(40) NOT NULL after `version`;")
}

func upgradeToVer131(s sessionapi.Session, _ int64) {
	doReentrantDDL(s, metadef.CreateTiDBTTLTaskTable)
	doReentrantDDL(s, metadef.CreateTiDBTTLJobHistoryTable)
}

func upgradeToVer132(s sessionapi.Session, _ int64) {
	doReentrantDDL(s, metadef.CreateTiDBMDLView)
}

func upgradeToVer133(s sessionapi.Session, _ int64) {
	mustExecute(s, "UPDATE HIGH_PRIORITY %n.%n set VARIABLE_VALUE = %? where VARIABLE_NAME = %? and VARIABLE_VALUE = %?;",
		mysql.SystemDB, mysql.GlobalVariablesTable, vardef.DefTiDBServerMemoryLimit, vardef.TiDBServerMemoryLimit, "0")
}

func upgradeToVer134(s sessionapi.Session, _ int64) {
	mustExecute(s, "REPLACE HIGH_PRIORITY INTO %n.%n VALUES (%?, %?);", mysql.SystemDB, mysql.GlobalVariablesTable, vardef.ForeignKeyChecks, vardef.On)
	mustExecute(s, "REPLACE HIGH_PRIORITY INTO %n.%n VALUES (%?, %?);", mysql.SystemDB, mysql.GlobalVariablesTable, vardef.TiDBEnableForeignKey, vardef.On)
	mustExecute(s, "REPLACE HIGH_PRIORITY INTO %n.%n VALUES (%?, %?);", mysql.SystemDB, mysql.GlobalVariablesTable, vardef.TiDBEnableHistoricalStats, vardef.On)
	mustExecute(s, "REPLACE HIGH_PRIORITY INTO %n.%n VALUES (%?, %?);", mysql.SystemDB, mysql.GlobalVariablesTable, vardef.TiDBEnablePlanReplayerCapture, vardef.On)
	mustExecute(s, "UPDATE HIGH_PRIORITY %n.%n SET VARIABLE_VALUE = %? WHERE VARIABLE_NAME = %? AND VARIABLE_VALUE = %?;", mysql.SystemDB, mysql.GlobalVariablesTable, "4", vardef.TiDBStoreBatchSize, "0")
}

// For users that upgrade TiDB from a pre-7.0 version, we want to set tidb_opt_advanced_join_hint to off by default to keep plans unchanged.
func upgradeToVer135(s sessionapi.Session, _ int64) {
	initGlobalVariableIfNotExists(s, vardef.TiDBOptAdvancedJoinHint, false)
}

func upgradeToVer136(s sessionapi.Session, _ int64) {
	mustExecute(s, metadef.CreateTiDBGlobalTaskTable)
	doReentrantDDL(s, "ALTER TABLE mysql.tidb_background_subtask DROP INDEX namespace", dbterror.ErrCantDropFieldOrKey)
	doReentrantDDL(s, "ALTER TABLE mysql.tidb_background_subtask ADD INDEX idx_task_key(task_key)", dbterror.ErrDupKeyName)
}

func upgradeToVer137(_ sessionapi.Session, _ int64) {
	// NOOP, we don't depend on ddl to init the default group due to backward compatible issue.
}

// For users that upgrade TiDB from a version below 7.0, we want to enable tidb tidb_enable_null_aware_anti_join by default.
func upgradeToVer138(s sessionapi.Session, _ int64) {
	mustExecute(s, "REPLACE HIGH_PRIORITY INTO %n.%n VALUES (%?, %?);", mysql.SystemDB, mysql.GlobalVariablesTable, vardef.TiDBOptimizerEnableNAAJ, vardef.On)
}

func upgradeToVer139(sessionapi.Session, int64) {}

func upgradeToVer140(s sessionapi.Session, _ int64) {
	doReentrantDDL(s, "ALTER TABLE mysql.tidb_global_task ADD COLUMN `task_key` VARCHAR(256) NOT NULL AFTER `id`", infoschema.ErrColumnExists)
	doReentrantDDL(s, "ALTER TABLE mysql.tidb_global_task ADD UNIQUE KEY task_key(task_key)", dbterror.ErrDupKeyName)
}

// upgradeToVer141 sets the value of `tidb_session_plan_cache_size` as `tidb_prepared_plan_cache_size` for compatibility,
// and update tidb_load_based_replica_read_threshold from 0 to 4.
func upgradeToVer141(s sessionapi.Session, _ int64) {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnBootstrap)
	rs, err := s.ExecuteInternal(ctx, "SELECT VARIABLE_VALUE FROM %n.%n WHERE VARIABLE_NAME=%?;",
		mysql.SystemDB, mysql.GlobalVariablesTable, vardef.TiDBPrepPlanCacheSize)
	terror.MustNil(err)
	req := rs.NewChunk(nil)
	err = rs.Next(ctx, req)
	if err != nil || req.NumRows() == 0 {
		return
	}
	row := req.GetRow(0)
	if row.IsNull(0) {
		return
	}
	val := row.GetString(0)

	mustExecute(s, "INSERT HIGH_PRIORITY IGNORE INTO %n.%n VALUES (%?, %?);",
		mysql.SystemDB, mysql.GlobalVariablesTable, vardef.TiDBSessionPlanCacheSize, val)
	mustExecute(s, "REPLACE HIGH_PRIORITY INTO %n.%n VALUES (%?, %?);", mysql.SystemDB, mysql.GlobalVariablesTable, vardef.TiDBLoadBasedReplicaReadThreshold, vardef.DefTiDBLoadBasedReplicaReadThreshold.String())
}

func upgradeToVer142(s sessionapi.Session, _ int64) {
	initGlobalVariableIfNotExists(s, vardef.TiDBEnableNonPreparedPlanCache, vardef.Off)
}

func upgradeToVer143(s sessionapi.Session, _ int64) {
	doReentrantDDL(s, "ALTER TABLE mysql.tidb_global_task ADD COLUMN `error` BLOB", infoschema.ErrColumnExists)
	doReentrantDDL(s, "ALTER TABLE mysql.tidb_background_subtask ADD COLUMN `error` BLOB", infoschema.ErrColumnExists)
}

func upgradeToVer144(s sessionapi.Session, _ int64) {
	initGlobalVariableIfNotExists(s, vardef.TiDBPlanCacheInvalidationOnFreshStats, vardef.Off)
}

func upgradeToVer146(s sessionapi.Session, _ int64) {
	doReentrantDDL(s, "ALTER TABLE mysql.stats_meta_history ADD INDEX idx_create_time (create_time)", dbterror.ErrDupKeyName)
	doReentrantDDL(s, "ALTER TABLE mysql.stats_history ADD INDEX idx_create_time (create_time)", dbterror.ErrDupKeyName)
}

func upgradeToVer167(s sessionapi.Session, _ int64) {
	doReentrantDDL(s, "ALTER TABLE mysql.tidb_background_subtask ADD COLUMN `step` INT AFTER `id`", infoschema.ErrColumnExists)
}

func upgradeToVer168(s sessionapi.Session, _ int64) {
	mustExecute(s, metadef.CreateTiDBImportJobsTable)
}

func upgradeToVer169(s sessionapi.Session, _ int64) {
	mustExecute(s, metadef.CreateTiDBRunawayQueriesTable)
}

func upgradeToVer170(s sessionapi.Session, _ int64) {
	mustExecute(s, metadef.CreateTiDBTimersTable)
}

func upgradeToVer171(s sessionapi.Session, _ int64) {
	mustExecute(s, "ALTER TABLE mysql.tidb_runaway_queries CHANGE COLUMN `tidb_server` `tidb_server` varchar(512)")
}

func upgradeToVer172(s sessionapi.Session, _ int64) {
	mustExecute(s, "DROP TABLE IF EXISTS mysql.tidb_runaway_quarantined_watch")
	mustExecute(s, metadef.CreateTiDBRunawayWatchTable)
	mustExecute(s, metadef.CreateTiDBRunawayWatchDoneTable)
}

func upgradeToVer173(s sessionapi.Session, _ int64) {
	doReentrantDDL(s, "ALTER TABLE mysql.tidb_background_subtask ADD COLUMN `summary` JSON", infoschema.ErrColumnExists)
}

func upgradeToVer174(s sessionapi.Session, _ int64) {
	doReentrantDDL(s, "ALTER TABLE mysql.tidb_background_subtask_history ADD COLUMN `step` INT AFTER `id`", infoschema.ErrColumnExists)
	doReentrantDDL(s, "ALTER TABLE mysql.tidb_background_subtask_history ADD COLUMN `error` BLOB", infoschema.ErrColumnExists)
	doReentrantDDL(s, "ALTER TABLE mysql.tidb_background_subtask_history DROP INDEX `namespace`", dbterror.ErrCantDropFieldOrKey)
	doReentrantDDL(s, "ALTER TABLE mysql.tidb_background_subtask_history ADD INDEX `idx_task_key`(`task_key`)", dbterror.ErrDupKeyName)
	doReentrantDDL(s, "ALTER TABLE mysql.tidb_background_subtask_history ADD INDEX `idx_state_update_time`(`state_update_time`)", dbterror.ErrDupKeyName)
}

// upgradeToVer175 updates normalized bindings of `in (?)` to `in (...)` to solve
// the issue #44298 that bindings for `in (?)` can't work for `in (?, ?, ?)`.
// After this update, multiple bindings may have the same `original_sql`, but it's OK, and
// for safety, don't remove duplicated bindings when upgrading.
func upgradeToVer175(s sessionapi.Session, _ int64) {
	var err error
	mustExecute(s, "BEGIN PESSIMISTIC")
	defer func() {
		if err != nil {
			mustExecute(s, "ROLLBACK")
			return
		}
		mustExecute(s, "COMMIT")
	}()
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnBootstrap)
	rs, err := s.ExecuteInternal(ctx, "SELECT original_sql, bind_sql FROM mysql.bind_info WHERE source != 'builtin'")
	if err != nil {
		logutil.BgLogger().Fatal("upgradeToVer175 error", zap.Error(err))
		return
	}
	req := rs.NewChunk(nil)
	updateStmts := make([]string, 0, 4)
	for {
		err = rs.Next(ctx, req)
		if err != nil {
			logutil.BgLogger().Fatal("upgradeToVer175 error", zap.Error(err))
			return
		}
		if req.NumRows() == 0 {
			break
		}
		for i := range req.NumRows() {
			originalNormalizedSQL, bindSQL := req.GetRow(i).GetString(0), req.GetRow(i).GetString(1)
			newNormalizedSQL := parser.NormalizeForBinding(bindSQL, false)
			// update `in (?)` to `in (...)`
			if originalNormalizedSQL == newNormalizedSQL {
				continue // no need to update
			}
			// must run those update statements outside this loop, otherwise may cause some concurrency problems,
			// since the current statement over this session has not been finished yet.
			updateStmts = append(updateStmts, fmt.Sprintf("UPDATE mysql.bind_info SET original_sql='%s' WHERE original_sql='%s'", newNormalizedSQL, originalNormalizedSQL))
		}
		req.Reset()
	}
	if err := rs.Close(); err != nil {
		logutil.BgLogger().Fatal("upgradeToVer175 error", zap.Error(err))
	}
	for _, updateStmt := range updateStmts {
		mustExecute(s, updateStmt)
	}
}

func upgradeToVer176(s sessionapi.Session, _ int64) {
	mustExecute(s, metadef.CreateTiDBGlobalTaskHistoryTable)
}

func upgradeToVer177(s sessionapi.Session, _ int64) {
	// ignore error when upgrading from v7.4 to higher version.
	doReentrantDDL(s, metadef.CreateDistFrameworkMetaTable, infoschema.ErrTableExists)
	err := s.GetSessionVars().GlobalVarsAccessor.SetGlobalSysVar(context.Background(), vardef.TiDBEnableAsyncMergeGlobalStats, vardef.Off)
	if err != nil {
		logutil.BgLogger().Fatal("upgradeToVer177 error", zap.Error(err))
	}
}

// writeDDLTableVersion writes mDDLTableVersion into mysql.tidb
func writeDDLTableVersion(s sessionapi.Session) {
	var err error
	var ddlTableVersion meta.DDLTableVersion
	err = kv.RunInNewTxn(kv.WithInternalSourceType(context.Background(), kv.InternalTxnBootstrap), s.GetStore(), true, func(_ context.Context, txn kv.Transaction) error {
		t := meta.NewMutator(txn)
		ddlTableVersion, err = t.GetDDLTableVersion()
		return err
	})
	terror.MustNil(err)
	mustExecute(s, `INSERT HIGH_PRIORITY INTO %n.%n VALUES (%?, %?, "DDL Table Version. Do not delete.") ON DUPLICATE KEY UPDATE VARIABLE_VALUE= %?`,
		mysql.SystemDB,
		mysql.TiDBTable,
		tidbDDLTableVersion,
		ddlTableVersion,
		ddlTableVersion,
	)
}

func upgradeToVer178(s sessionapi.Session, _ int64) {
	writeDDLTableVersion(s)
}

func upgradeToVer179(s sessionapi.Session, _ int64) {
	doReentrantDDL(s, "ALTER TABLE mysql.global_variables MODIFY COLUMN `VARIABLE_VALUE` varchar(16383)")
}

func upgradeToVer190(s sessionapi.Session, _ int64) {
	doReentrantDDL(s, "ALTER TABLE mysql.tidb_global_task ADD COLUMN `priority` INT DEFAULT 1 AFTER `state`", infoschema.ErrColumnExists)
	doReentrantDDL(s, "ALTER TABLE mysql.tidb_global_task ADD COLUMN `create_time` TIMESTAMP AFTER `priority`", infoschema.ErrColumnExists)
	doReentrantDDL(s, "ALTER TABLE mysql.tidb_global_task ADD COLUMN `end_time` TIMESTAMP AFTER `state_update_time`", infoschema.ErrColumnExists)
	doReentrantDDL(s, "ALTER TABLE mysql.tidb_global_task_history ADD COLUMN `priority` INT DEFAULT 1 AFTER `state`", infoschema.ErrColumnExists)
	doReentrantDDL(s, "ALTER TABLE mysql.tidb_global_task_history ADD COLUMN `create_time` TIMESTAMP AFTER `priority`", infoschema.ErrColumnExists)
	doReentrantDDL(s, "ALTER TABLE mysql.tidb_global_task_history ADD COLUMN `end_time` TIMESTAMP AFTER `state_update_time`", infoschema.ErrColumnExists)

	doReentrantDDL(s, "ALTER TABLE mysql.tidb_background_subtask ADD COLUMN `concurrency` INT AFTER `checkpoint`", infoschema.ErrColumnExists)
	doReentrantDDL(s, "ALTER TABLE mysql.tidb_background_subtask ADD COLUMN `create_time` TIMESTAMP AFTER `concurrency`", infoschema.ErrColumnExists)
	doReentrantDDL(s, "ALTER TABLE mysql.tidb_background_subtask ADD COLUMN `end_time` TIMESTAMP AFTER `state_update_time`", infoschema.ErrColumnExists)
	doReentrantDDL(s, "ALTER TABLE mysql.tidb_background_subtask ADD COLUMN `ordinal` int AFTER `meta`", infoschema.ErrColumnExists)
	doReentrantDDL(s, "ALTER TABLE mysql.tidb_background_subtask_history ADD COLUMN `concurrency` INT AFTER `checkpoint`", infoschema.ErrColumnExists)
	doReentrantDDL(s, "ALTER TABLE mysql.tidb_background_subtask_history ADD COLUMN `create_time` TIMESTAMP AFTER `concurrency`", infoschema.ErrColumnExists)
	doReentrantDDL(s, "ALTER TABLE mysql.tidb_background_subtask_history ADD COLUMN `end_time` TIMESTAMP AFTER `state_update_time`", infoschema.ErrColumnExists)
	doReentrantDDL(s, "ALTER TABLE mysql.tidb_background_subtask_history ADD COLUMN `ordinal` int AFTER `meta`", infoschema.ErrColumnExists)

	doReentrantDDL(s, "ALTER TABLE mysql.tidb_background_subtask ADD INDEX idx_exec_id(exec_id)", dbterror.ErrDupKeyName)
	doReentrantDDL(s, "ALTER TABLE mysql.tidb_background_subtask ADD UNIQUE INDEX uk_task_key_step_ordinal(task_key, step, ordinal)", dbterror.ErrDupKeyName)

	doReentrantDDL(s, "ALTER TABLE mysql.dist_framework_meta ADD COLUMN `cpu_count` INT DEFAULT 0 AFTER `role`", infoschema.ErrColumnExists)

	doReentrantDDL(s, "ALTER TABLE mysql.dist_framework_meta MODIFY COLUMN `host` VARCHAR(261)")
	doReentrantDDL(s, "ALTER TABLE mysql.tidb_background_subtask MODIFY COLUMN `exec_id` VARCHAR(261)")
	doReentrantDDL(s, "ALTER TABLE mysql.tidb_background_subtask_history MODIFY COLUMN `exec_id` VARCHAR(261)")
	doReentrantDDL(s, "ALTER TABLE mysql.tidb_global_task MODIFY COLUMN `dispatcher_id` VARCHAR(261)")
	doReentrantDDL(s, "ALTER TABLE mysql.tidb_global_task_history MODIFY COLUMN `dispatcher_id` VARCHAR(261)")
}

func upgradeToVer191(s sessionapi.Session, _ int64) {
	sql := fmt.Sprintf("INSERT HIGH_PRIORITY IGNORE INTO %s.%s VALUES('%s', '%s')",
		mysql.SystemDB, mysql.GlobalVariablesTable,
		vardef.TiDBTxnMode, vardef.OptimisticTxnMode)
	mustExecute(s, sql)
}

func upgradeToVer192(s sessionapi.Session, _ int64) {
	doReentrantDDL(s, metadef.CreateRequestUnitByGroupTable)
}

func upgradeToVer193(s sessionapi.Session, _ int64) {
	doReentrantDDL(s, metadef.CreateTiDBMDLView)
}

func upgradeToVer194(s sessionapi.Session, _ int64) {
	mustExecute(s, "DROP TABLE IF EXISTS mysql.load_data_jobs")
}

func upgradeToVer195(s sessionapi.Session, _ int64) {
	doReentrantDDL(s, metadef.DropMySQLIndexUsageTable)
}

func upgradeToVer196(s sessionapi.Session, _ int64) {
	doReentrantDDL(s, "ALTER TABLE mysql.tidb_global_task ADD COLUMN target_scope VARCHAR(256) DEFAULT '' AFTER `step`;", infoschema.ErrColumnExists)
	doReentrantDDL(s, "ALTER TABLE mysql.tidb_global_task_history ADD COLUMN target_scope VARCHAR(256) DEFAULT '' AFTER `step`;", infoschema.ErrColumnExists)
}

func upgradeToVer197(s sessionapi.Session, _ int64) {
	doReentrantDDL(s, metadef.CreateTiDBMDLView)
}

func upgradeToVer198(s sessionapi.Session, _ int64) {
	doReentrantDDL(s, "ALTER TABLE mysql.tidb_mdl_info ADD COLUMN owner_id VARCHAR(64) NOT NULL DEFAULT '';", infoschema.ErrColumnExists)
}

func upgradeToVer209(s sessionapi.Session, _ int64) {
	initGlobalVariableIfNotExists(s, vardef.TiDBResourceControlStrictMode, vardef.Off)
}

func upgradeToVer210(s sessionapi.Session, _ int64) {
	// Check if tidb_analyze_column_options exists in mysql.GLOBAL_VARIABLES.
	// If not, set tidb_analyze_column_options to ALL since this is the old behavior before we introduce this variable.
	initGlobalVariableIfNotExists(s, vardef.TiDBAnalyzeColumnOptions, ast.AllColumns.String())

	// Check if tidb_opt_projection_push_down exists in mysql.GLOBAL_VARIABLES.
	// If not, set tidb_opt_projection_push_down to Off since this is the old behavior before we introduce this variable.
	initGlobalVariableIfNotExists(s, vardef.TiDBOptProjectionPushDown, vardef.Off)
}

func upgradeToVer211(s sessionapi.Session, _ int64) {
	doReentrantDDL(s, "ALTER TABLE mysql.tidb_background_subtask_history ADD COLUMN `summary` JSON", infoschema.ErrColumnExists)
}

func upgradeToVer212(s sessionapi.Session, ver int64) {
	// need to ensure curVersion has the column before rename.
	// version169 created `tidb_runaway_queries` table
	// version172 created `tidb_runaway_watch` and `tidb_runaway_watch_done` tables
	if ver < version172 {
		return
	}
	// version212 changed a lots of runaway related table.
	// 1. switchGroup: add column `switch_group_name` to `mysql.tidb_runaway_watch` and `mysql.tidb_runaway_watch_done`.
	doReentrantDDL(s, "ALTER TABLE mysql.tidb_runaway_watch ADD COLUMN `switch_group_name` VARCHAR(32) DEFAULT '' AFTER `action`;", infoschema.ErrColumnExists)
	doReentrantDDL(s, "ALTER TABLE mysql.tidb_runaway_watch_done ADD COLUMN `switch_group_name` VARCHAR(32) DEFAULT '' AFTER `action`;", infoschema.ErrColumnExists)
	// 2. modify column `plan_digest` type, modify column `time` to `start_time,
	// modify column `original_sql` to `sample_sql` and unique union key to `mysql.tidb_runaway_queries`.
	// add column `sql_digest`.
	doReentrantDDL(s, "ALTER TABLE mysql.tidb_runaway_queries ADD COLUMN `sql_digest` varchar(64) DEFAULT '' AFTER `original_sql`;", infoschema.ErrColumnExists)
	// add column `repeats`.
	doReentrantDDL(s, "ALTER TABLE mysql.tidb_runaway_queries ADD COLUMN `repeats` int DEFAULT 1 AFTER `time`;", infoschema.ErrColumnExists)
	// rename column name from `time` to `start_time`, will auto rebuild the index.
	doReentrantDDL(s, "ALTER TABLE mysql.tidb_runaway_queries RENAME COLUMN `time` TO `start_time`", infoschema.ErrColumnNotExists)
	// rename column `original_sql` to `sample_sql`.
	doReentrantDDL(s, "ALTER TABLE mysql.tidb_runaway_queries RENAME COLUMN `original_sql` TO `sample_sql`", infoschema.ErrColumnNotExists)
	// modify column type of `plan_digest`.
	doReentrantDDL(s, "ALTER TABLE mysql.tidb_runaway_queries MODIFY COLUMN `plan_digest` varchar(64) DEFAULT '';", infoschema.ErrColumnExists)
	// 3. modify column length of `action`.
	doReentrantDDL(s, "ALTER TABLE mysql.tidb_runaway_queries MODIFY COLUMN `action` VARCHAR(64) NOT NULL;", infoschema.ErrColumnExists)
	// 4. add column `rule` to `mysql.tidb_runaway_watch`, `mysql.tidb_runaway_watch_done` and `mysql.tidb_runaway_queries`.
	doReentrantDDL(s, "ALTER TABLE mysql.tidb_runaway_watch ADD COLUMN `rule` VARCHAR(512) DEFAULT '' AFTER `switch_group_name`;", infoschema.ErrColumnExists)
	doReentrantDDL(s, "ALTER TABLE mysql.tidb_runaway_watch_done ADD COLUMN `rule` VARCHAR(512) DEFAULT '' AFTER `switch_group_name`;", infoschema.ErrColumnExists)
	doReentrantDDL(s, "ALTER TABLE mysql.tidb_runaway_queries ADD COLUMN `rule` VARCHAR(512) DEFAULT '' AFTER `tidb_server`;", infoschema.ErrColumnExists)
}

func upgradeToVer213(s sessionapi.Session, _ int64) {
	mustExecute(s, metadef.CreateTiDBPITRIDMapTable)
}

func upgradeToVer214(s sessionapi.Session, _ int64) {
	mustExecute(s, metadef.CreateIndexAdvisorResultsTable)
	mustExecute(s, metadef.CreateTiDBKernelOptionsTable)
}

func upgradeToVer215(s sessionapi.Session, _ int64) {
	initGlobalVariableIfNotExists(s, vardef.TiDBEnableINLJoinInnerMultiPattern, vardef.Off)
}

func upgradeToVer216(s sessionapi.Session, _ int64) {
	mustExecute(s, "UPDATE mysql.global_variables SET VARIABLE_VALUE='' WHERE VARIABLE_NAME = 'tidb_scatter_region' AND VARIABLE_VALUE = 'OFF'")
	mustExecute(s, "UPDATE mysql.global_variables SET VARIABLE_VALUE='table' WHERE VARIABLE_NAME = 'tidb_scatter_region' AND VARIABLE_VALUE = 'ON'")
}

func upgradeToVer217(s sessionapi.Session, _ int64) {
	// If tidb_schema_cache_size does not exist, insert a record and set the value to 0
	// Otherwise do nothing.
	mustExecute(s, "INSERT IGNORE INTO mysql.global_variables VALUES ('tidb_schema_cache_size', 0)")
}

func upgradeToVer218(_ sessionapi.Session, _ int64) {
	// empty, just make lint happy.
}

func upgradeToVer239(s sessionapi.Session, _ int64) {
	doReentrantDDL(s, "ALTER TABLE mysql.tidb_global_task ADD COLUMN modify_params json AFTER `error`;", infoschema.ErrColumnExists)
	doReentrantDDL(s, "ALTER TABLE mysql.tidb_global_task_history ADD COLUMN modify_params json AFTER `error`;", infoschema.ErrColumnExists)
}

const (
	// addAnalyzeJobsSchemaTableStateIndex is a DDL statement that adds an index on (table_schema, table_name, state)
	// columns to mysql.analyze_jobs table. This index is currently unused since queries filter on partition_name='',
	// even for non-partitioned tables. It is kept for potential future optimization where queries could use this
	// simpler index directly for non-partitioned tables.
	addAnalyzeJobsSchemaTableStateIndex = "ALTER TABLE mysql.analyze_jobs ADD INDEX idx_schema_table_state (table_schema, table_name, state)"
	// addAnalyzeJobsSchemaTablePartitionStateIndex adds an index on (table_schema, table_name, partition_name, state) to mysql.analyze_jobs
	addAnalyzeJobsSchemaTablePartitionStateIndex = "ALTER TABLE mysql.analyze_jobs ADD INDEX idx_schema_table_partition_state (table_schema, table_name, partition_name, state)"
)

func upgradeToVer240(s sessionapi.Session, _ int64) {
	doReentrantDDL(s, addAnalyzeJobsSchemaTableStateIndex, dbterror.ErrDupKeyName)
	doReentrantDDL(s, addAnalyzeJobsSchemaTablePartitionStateIndex, dbterror.ErrDupKeyName)
}

func upgradeToVer241(s sessionapi.Session, _ int64) {
	doReentrantDDL(s, "ALTER TABLE mysql.user ADD INDEX i_user (user)", dbterror.ErrDupKeyName)
	doReentrantDDL(s, "ALTER TABLE mysql.global_priv ADD INDEX i_user (user)", dbterror.ErrDupKeyName)
	doReentrantDDL(s, "ALTER TABLE mysql.db ADD INDEX i_user (user)", dbterror.ErrDupKeyName)
	doReentrantDDL(s, "ALTER TABLE mysql.tables_priv ADD INDEX i_user (user)", dbterror.ErrDupKeyName)
	doReentrantDDL(s, "ALTER TABLE mysql.columns_priv ADD INDEX i_user (user)", dbterror.ErrDupKeyName)
	doReentrantDDL(s, "ALTER TABLE mysql.global_grants ADD INDEX i_user (user)", dbterror.ErrDupKeyName)
	doReentrantDDL(s, "ALTER TABLE mysql.default_roles ADD INDEX i_user (user)", dbterror.ErrDupKeyName)
}

// writeClusterID writes cluster id into mysql.tidb
func writeClusterID(s sessionapi.Session) {
	clusterID := s.GetStore().GetClusterID()

	mustExecute(s, `INSERT HIGH_PRIORITY INTO %n.%n VALUES (%?, %?, "TiDB Cluster ID.") ON DUPLICATE KEY UPDATE VARIABLE_VALUE= %?`,
		mysql.SystemDB,
		mysql.TiDBTable,
		tidbClusterID,
		clusterID,
		clusterID,
	)
}

func upgradeToVer242(s sessionapi.Session, _ int64) {
	writeClusterID(s)
	mustExecute(s, metadef.CreateTiDBWorkloadValuesTable)
}

func upgradeToVer243(s sessionapi.Session, _ int64) {
	doReentrantDDL(s, "ALTER TABLE mysql.tidb_global_task ADD COLUMN max_node_count INT DEFAULT 0 AFTER `modify_params`;", infoschema.ErrColumnExists)
	doReentrantDDL(s, "ALTER TABLE mysql.tidb_global_task_history ADD COLUMN max_node_count INT DEFAULT 0 AFTER `modify_params`;", infoschema.ErrColumnExists)
	doReentrantDDL(s, "ALTER TABLE mysql.tidb_global_task ADD COLUMN extra_params json AFTER max_node_count;", infoschema.ErrColumnExists)
	doReentrantDDL(s, "ALTER TABLE mysql.tidb_global_task_history ADD COLUMN extra_params json AFTER max_node_count;", infoschema.ErrColumnExists)
}

func upgradeToVer244(s sessionapi.Session, _ int64) {
	doReentrantDDL(s, "ALTER TABLE mysql.user ADD COLUMN IF NOT EXISTS `Max_user_connections` INT UNSIGNED NOT NULL DEFAULT 0 AFTER `Password_lifetime`")
}

func upgradeToVer245(s sessionapi.Session, _ int64) {
	doReentrantDDL(s, "ALTER TABLE mysql.bind_info MODIFY COLUMN original_sql LONGTEXT NOT NULL")
	doReentrantDDL(s, "ALTER TABLE mysql.bind_info MODIFY COLUMN bind_sql LONGTEXT NOT NULL")
}

func upgradeToVer246(s sessionapi.Session, _ int64) {
	// log duplicated digests that will be set to null.
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnBootstrap)
	rs, err := s.ExecuteInternal(ctx,
		`select plan_digest, sql_digest from mysql.bind_info group by plan_digest, sql_digest having count(1) > 1`)
	if err != nil {
		logutil.BgLogger().Fatal("failed to get duplicated plan and sql digests", zap.Error(err))
		return
	}
	req := rs.NewChunk(nil)
	duplicatedDigests := make(map[string]struct{})
	for {
		err = rs.Next(ctx, req)
		if err != nil {
			logutil.BgLogger().Fatal("failed to get duplicated plan and sql digests", zap.Error(err))
			return
		}
		if req.NumRows() == 0 {
			break
		}
		for i := range req.NumRows() {
			planDigest, sqlDigest := req.GetRow(i).GetString(0), req.GetRow(i).GetString(1)
			duplicatedDigests[sqlDigest+", "+planDigest] = struct{}{}
		}
		req.Reset()
	}
	if err := rs.Close(); err != nil {
		logutil.BgLogger().Warn("failed to close record set", zap.Error(err))
	}
	if len(duplicatedDigests) > 0 {
		digestList := make([]string, 0, len(duplicatedDigests))
		for k := range duplicatedDigests {
			digestList = append(digestList, "("+k+")")
		}
		logutil.BgLogger().Warn("set the following (plan digest, sql digest) in mysql.bind_info to null " +
			"for adding new unique index: " + strings.Join(digestList, ", "))
	}

	// to avoid the failure of adding the unique index, remove duplicated rows on these 2 digest columns first.
	// in most cases, there should be no duplicated rows, since now we only store one binding for each sql_digest.
	// compared with upgrading failure, it's OK to set these 2 columns to null.
	doReentrantDDL(s, `UPDATE mysql.bind_info SET plan_digest=null, sql_digest=null
                       WHERE (plan_digest, sql_digest) in (
                         select plan_digest, sql_digest from mysql.bind_info
                         group by plan_digest, sql_digest having count(1) > 1)`)
	doReentrantDDL(s, "ALTER TABLE mysql.bind_info MODIFY COLUMN sql_digest VARCHAR(64) DEFAULT NULL")
	doReentrantDDL(s, "ALTER TABLE mysql.bind_info MODIFY COLUMN plan_digest VARCHAR(64) DEFAULT NULL")
	doReentrantDDL(s, "ALTER TABLE mysql.bind_info ADD UNIQUE INDEX digest_index(plan_digest, sql_digest)", dbterror.ErrDupKeyName)
}

func upgradeToVer247(s sessionapi.Session, _ int64) {
	doReentrantDDL(s, "ALTER TABLE mysql.stats_meta ADD COLUMN last_stats_histograms_version bigint unsigned DEFAULT NULL", infoschema.ErrColumnExists)
}

func upgradeToVer248(s sessionapi.Session, _ int64) {
	doReentrantDDL(s, "ALTER TABLE mysql.tidb_pitr_id_map ADD COLUMN restore_id BIGINT NOT NULL DEFAULT 0", infoschema.ErrColumnExists)
	doReentrantDDL(s, "ALTER TABLE mysql.tidb_pitr_id_map DROP PRIMARY KEY")
	doReentrantDDL(s, "ALTER TABLE mysql.tidb_pitr_id_map ADD PRIMARY KEY(restore_id, restored_ts, upstream_cluster_id, segment_id)")
}

func upgradeToVer249(s sessionapi.Session, _ int64) {
	doReentrantDDL(s, metadef.CreateTiDBRestoreRegistryTable)
}

func upgradeToVer250(s sessionapi.Session, _ int64) {
	doReentrantDDL(s, "ALTER TABLE mysql.tidb_global_task ADD COLUMN `keyspace` varchar(64) DEFAULT '' AFTER `extra_params`", infoschema.ErrColumnExists)
	doReentrantDDL(s, "ALTER TABLE mysql.tidb_global_task ADD INDEX idx_keyspace(keyspace)", dbterror.ErrDupKeyName)
	doReentrantDDL(s, "ALTER TABLE mysql.tidb_global_task_history ADD COLUMN `keyspace` varchar(64) DEFAULT '' AFTER `extra_params`", infoschema.ErrColumnExists)
	doReentrantDDL(s, "ALTER TABLE mysql.tidb_global_task_history ADD INDEX idx_keyspace(keyspace)", dbterror.ErrDupKeyName)
}

func upgradeToVer251(s sessionapi.Session, _ int64) {
	doReentrantDDL(s, "ALTER TABLE mysql.tidb_import_jobs ADD COLUMN `group_key` VARCHAR(256) NOT NULL DEFAULT '' AFTER `created_by`", infoschema.ErrColumnExists)
	doReentrantDDL(s, "ALTER TABLE mysql.tidb_import_jobs ADD INDEX idx_group_key(group_key)", dbterror.ErrDupKeyName)
}

func upgradeToVer252(s sessionapi.Session, _ int64) {
	doReentrantDDL(s, "ALTER TABLE mysql.bind_info CHANGE create_time create_time TIMESTAMP(6)")
	doReentrantDDL(s, "ALTER TABLE mysql.bind_info CHANGE update_time update_time TIMESTAMP(6)")
}

func upgradeToVer253(s sessionapi.Session, _ int64) {
	doReentrantDDL(s, "ALTER TABLE mysql.bind_info ADD COLUMN last_used_date DATE DEFAULT NULL AFTER `plan_digest`", infoschema.ErrColumnExists)
}

func upgradeToVer254(s sessionapi.Session, _ int64) {
	doReentrantDDL(s, "ALTER TABLE mysql.tidb_runaway_watch ADD INDEX idx_start_time(start_time)", dbterror.ErrDupKeyName)
	doReentrantDDL(s, "ALTER TABLE mysql.tidb_runaway_watch_done ADD INDEX idx_done_time(done_time)", dbterror.ErrDupKeyName)
}
