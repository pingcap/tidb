// Copyright 2015 PingCAP, Inc.
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

// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

package session

import (
	"context"
	"encoding/hex"
	"flag"
	"fmt"
	osuser "os/user"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/bindinfo"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/pingcap/tidb/util/logutil"
	utilparser "github.com/pingcap/tidb/util/parser"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/pingcap/tidb/util/timeutil"
	"go.uber.org/zap"
)

const (
	// CreateUserTable is the SQL statement creates User table in system db.
	CreateUserTable = `CREATE TABLE IF NOT EXISTS mysql.user (
		Host					CHAR(255),
		User					CHAR(32),
		authentication_string	TEXT,
		plugin					CHAR(64),
		Select_priv				ENUM('N','Y') NOT NULL DEFAULT 'N',
		Insert_priv				ENUM('N','Y') NOT NULL DEFAULT 'N',
		Update_priv				ENUM('N','Y') NOT NULL DEFAULT 'N',
		Delete_priv				ENUM('N','Y') NOT NULL DEFAULT 'N',
		Create_priv				ENUM('N','Y') NOT NULL DEFAULT 'N',
		Drop_priv				ENUM('N','Y') NOT NULL DEFAULT 'N',
		Process_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Grant_priv				ENUM('N','Y') NOT NULL DEFAULT 'N',
		References_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Alter_priv				ENUM('N','Y') NOT NULL DEFAULT 'N',
		Show_db_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Super_priv				ENUM('N','Y') NOT NULL DEFAULT 'N',
		Create_tmp_table_priv	ENUM('N','Y') NOT NULL DEFAULT 'N',
		Lock_tables_priv		ENUM('N','Y') NOT NULL DEFAULT 'N',
		Execute_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Create_view_priv		ENUM('N','Y') NOT NULL DEFAULT 'N',
		Show_view_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Create_routine_priv		ENUM('N','Y') NOT NULL DEFAULT 'N',
		Alter_routine_priv		ENUM('N','Y') NOT NULL DEFAULT 'N',
		Index_priv				ENUM('N','Y') NOT NULL DEFAULT 'N',
		Create_user_priv		ENUM('N','Y') NOT NULL DEFAULT 'N',
		Event_priv				ENUM('N','Y') NOT NULL DEFAULT 'N',
		Repl_slave_priv	    	ENUM('N','Y') NOT NULL DEFAULT 'N',
		Repl_client_priv		ENUM('N','Y') NOT NULL DEFAULT 'N',
		Trigger_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Create_role_priv		ENUM('N','Y') NOT NULL DEFAULT 'N',
		Drop_role_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Account_locked			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Shutdown_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Reload_priv				ENUM('N','Y') NOT NULL DEFAULT 'N',
		FILE_priv				ENUM('N','Y') NOT NULL DEFAULT 'N',
		Config_priv				ENUM('N','Y') NOT NULL DEFAULT 'N',
		Create_Tablespace_Priv  ENUM('N','Y') NOT NULL DEFAULT 'N',
		PRIMARY KEY (Host, User));`
	// CreateGlobalPrivTable is the SQL statement creates Global scope privilege table in system db.
	CreateGlobalPrivTable = "CREATE TABLE IF NOT EXISTS mysql.global_priv (" +
		"Host CHAR(255) NOT NULL DEFAULT ''," +
		"User CHAR(80) NOT NULL DEFAULT ''," +
		"Priv LONGTEXT NOT NULL DEFAULT ''," +
		"PRIMARY KEY (Host, User)" +
		")"
	// CreateDBPrivTable is the SQL statement creates DB scope privilege table in system db.
	CreateDBPrivTable = `CREATE TABLE IF NOT EXISTS mysql.db (
		Host					CHAR(255),
		DB						CHAR(64),
		User					CHAR(32),
		Select_priv				ENUM('N','Y') NOT NULL DEFAULT 'N',
		Insert_priv				ENUM('N','Y') NOT NULL DEFAULT 'N',
		Update_priv				ENUM('N','Y') NOT NULL DEFAULT 'N',
		Delete_priv				ENUM('N','Y') NOT NULL DEFAULT 'N',
		Create_priv				ENUM('N','Y') NOT NULL DEFAULT 'N',
		Drop_priv				ENUM('N','Y') NOT NULL DEFAULT 'N',
		Grant_priv				ENUM('N','Y') NOT NULL DEFAULT 'N',
		References_priv 		ENUM('N','Y') NOT NULL DEFAULT 'N',
		Index_priv				ENUM('N','Y') NOT NULL DEFAULT 'N',
		Alter_priv				ENUM('N','Y') NOT NULL DEFAULT 'N',
		Create_tmp_table_priv	ENUM('N','Y') NOT NULL DEFAULT 'N',
		Lock_tables_priv		ENUM('N','Y') NOT NULL DEFAULT 'N',
		Create_view_priv		ENUM('N','Y') NOT NULL DEFAULT 'N',
		Show_view_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Create_routine_priv		ENUM('N','Y') NOT NULL DEFAULT 'N',
		Alter_routine_priv		ENUM('N','Y') NOT NULL DEFAULT 'N',
		Execute_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Event_priv				ENUM('N','Y') NOT NULL DEFAULT 'N',
		Trigger_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		PRIMARY KEY (Host, DB, User));`
	// CreateTablePrivTable is the SQL statement creates table scope privilege table in system db.
	CreateTablePrivTable = `CREATE TABLE IF NOT EXISTS mysql.tables_priv (
		Host		CHAR(255),
		DB			CHAR(64),
		User		CHAR(32),
		Table_name	CHAR(64),
		Grantor		CHAR(77),
		Timestamp	TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		Table_priv	SET('Select','Insert','Update','Delete','Create','Drop','Grant','Index','Alter','Create View','Show View','Trigger','References'),
		Column_priv	SET('Select','Insert','Update','References'),
		PRIMARY KEY (Host, DB, User, Table_name));`
	// CreateColumnPrivTable is the SQL statement creates column scope privilege table in system db.
	CreateColumnPrivTable = `CREATE TABLE IF NOT EXISTS mysql.columns_priv(
		Host		CHAR(255),
		DB			CHAR(64),
		User		CHAR(32),
		Table_name	CHAR(64),
		Column_name	CHAR(64),
		Timestamp	TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		Column_priv	SET('Select','Insert','Update','References'),
		PRIMARY KEY (Host, DB, User, Table_name, Column_name));`
	// CreateGlobalVariablesTable is the SQL statement creates global variable table in system db.
	// TODO: MySQL puts GLOBAL_VARIABLES table in INFORMATION_SCHEMA db.
	// INFORMATION_SCHEMA is a virtual db in TiDB. So we put this table in system db.
	// Maybe we will put it back to INFORMATION_SCHEMA.
	CreateGlobalVariablesTable = `CREATE TABLE IF NOT EXISTS mysql.GLOBAL_VARIABLES(
		VARIABLE_NAME  VARCHAR(64) NOT NULL PRIMARY KEY,
		VARIABLE_VALUE VARCHAR(1024) DEFAULT NULL);`
	// CreateTiDBTable is the SQL statement creates a table in system db.
	// This table is a key-value struct contains some information used by TiDB.
	// Currently we only put bootstrapped in it which indicates if the system is already bootstrapped.
	CreateTiDBTable = `CREATE TABLE IF NOT EXISTS mysql.tidb(
		VARIABLE_NAME  	VARCHAR(64) NOT NULL PRIMARY KEY,
		VARIABLE_VALUE 	VARCHAR(1024) DEFAULT NULL,
		COMMENT 		VARCHAR(1024));`

	// CreateHelpTopic is the SQL statement creates help_topic table in system db.
	// See: https://dev.mysql.com/doc/refman/5.5/en/system-database.html#system-database-help-tables
	CreateHelpTopic = `CREATE TABLE IF NOT EXISTS mysql.help_topic (
  		help_topic_id 		INT(10) UNSIGNED NOT NULL,
  		name 				CHAR(64) NOT NULL,
  		help_category_id 	SMALLINT(5) UNSIGNED NOT NULL,
  		description 		TEXT NOT NULL,
  		example 			TEXT NOT NULL,
  		url 				TEXT NOT NULL,
  		PRIMARY KEY (help_topic_id) clustered,
  		UNIQUE KEY name (name)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8 STATS_PERSISTENT=0 COMMENT='help topics';`

	// CreateStatsMetaTable stores the meta of table statistics.
	CreateStatsMetaTable = `CREATE TABLE IF NOT EXISTS mysql.stats_meta (
		version 		BIGINT(64) UNSIGNED NOT NULL,
		table_id 		BIGINT(64) NOT NULL,
		modify_count	BIGINT(64) NOT NULL DEFAULT 0,
		count 			BIGINT(64) UNSIGNED NOT NULL DEFAULT 0,
		snapshot        BIGINT(64) UNSIGNED NOT NULL DEFAULT 0,
		INDEX idx_ver(version),
		UNIQUE INDEX tbl(table_id)
	);`

	// CreateStatsColsTable stores the statistics of table columns.
	CreateStatsColsTable = `CREATE TABLE IF NOT EXISTS mysql.stats_histograms (
		table_id 			BIGINT(64) NOT NULL,
		is_index 			TINYINT(2) NOT NULL,
		hist_id 			BIGINT(64) NOT NULL,
		distinct_count 		BIGINT(64) NOT NULL,
		null_count 			BIGINT(64) NOT NULL DEFAULT 0,
		tot_col_size 		BIGINT(64) NOT NULL DEFAULT 0,
		modify_count 		BIGINT(64) NOT NULL DEFAULT 0,
		version 			BIGINT(64) UNSIGNED NOT NULL DEFAULT 0,
		cm_sketch 			BLOB(6291456),
		stats_ver 			BIGINT(64) NOT NULL DEFAULT 0,
		flag 				BIGINT(64) NOT NULL DEFAULT 0,
		correlation 		DOUBLE NOT NULL DEFAULT 0,
		last_analyze_pos 	LONGBLOB DEFAULT NULL,
		UNIQUE INDEX tbl(table_id, is_index, hist_id)
	);`

	// CreateStatsBucketsTable stores the histogram info for every table columns.
	CreateStatsBucketsTable = `CREATE TABLE IF NOT EXISTS mysql.stats_buckets (
		table_id 	BIGINT(64) NOT NULL,
		is_index 	TINYINT(2) NOT NULL,
		hist_id 	BIGINT(64) NOT NULL,
		bucket_id 	BIGINT(64) NOT NULL,
		count 		BIGINT(64) NOT NULL,
		repeats 	BIGINT(64) NOT NULL,
		upper_bound LONGBLOB NOT NULL,
		lower_bound LONGBLOB ,
		ndv         BIGINT NOT NULL DEFAULT 0,
		UNIQUE INDEX tbl(table_id, is_index, hist_id, bucket_id)
	);`

	// CreateGCDeleteRangeTable stores schemas which can be deleted by DeleteRange.
	CreateGCDeleteRangeTable = `CREATE TABLE IF NOT EXISTS mysql.gc_delete_range (
		job_id 		BIGINT NOT NULL COMMENT "the DDL job ID",
		element_id 	BIGINT NOT NULL COMMENT "the schema element ID",
		start_key 	VARCHAR(255) NOT NULL COMMENT "encoded in hex",
		end_key 	VARCHAR(255) NOT NULL COMMENT "encoded in hex",
		ts 			BIGINT NOT NULL COMMENT "timestamp in uint64",
		UNIQUE KEY delete_range_index (job_id, element_id)
	);`

	// CreateGCDeleteRangeDoneTable stores schemas which are already deleted by DeleteRange.
	CreateGCDeleteRangeDoneTable = `CREATE TABLE IF NOT EXISTS mysql.gc_delete_range_done (
		job_id 		BIGINT NOT NULL COMMENT "the DDL job ID",
		element_id 	BIGINT NOT NULL COMMENT "the schema element ID",
		start_key 	VARCHAR(255) NOT NULL COMMENT "encoded in hex",
		end_key 	VARCHAR(255) NOT NULL COMMENT "encoded in hex",
		ts 			BIGINT NOT NULL COMMENT "timestamp in uint64",
		UNIQUE KEY delete_range_done_index (job_id, element_id)
	);`

	// CreateStatsFeedbackTable stores the feedback info which is used to update stats.
	CreateStatsFeedbackTable = `CREATE TABLE IF NOT EXISTS mysql.stats_feedback (
		table_id 	BIGINT(64) NOT NULL,
		is_index 	TINYINT(2) NOT NULL,
		hist_id 	BIGINT(64) NOT NULL,
		feedback 	BLOB NOT NULL,
		INDEX hist(table_id, is_index, hist_id)
	);`

	// CreateBindInfoTable stores the sql bind info which is used to update globalBindCache.
	CreateBindInfoTable = `CREATE TABLE IF NOT EXISTS mysql.bind_info (
		original_sql TEXT NOT NULL,
		bind_sql TEXT NOT NULL,
		default_db TEXT NOT NULL,
		status TEXT NOT NULL,
		create_time TIMESTAMP(3) NOT NULL,
		update_time TIMESTAMP(3) NOT NULL,
		charset TEXT NOT NULL,
		collation TEXT NOT NULL,
		source VARCHAR(10) NOT NULL DEFAULT 'unknown',
		INDEX sql_index(original_sql(700),default_db(68)) COMMENT "accelerate the speed when add global binding query",
		INDEX time_index(update_time) COMMENT "accelerate the speed when querying with last update time"
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;`

	// CreateRoleEdgesTable stores the role and user relationship information.
	CreateRoleEdgesTable = `CREATE TABLE IF NOT EXISTS mysql.role_edges (
		FROM_HOST 			CHAR(60) COLLATE utf8_bin NOT NULL DEFAULT '',
		FROM_USER 			CHAR(32) COLLATE utf8_bin NOT NULL DEFAULT '',
		TO_HOST 			CHAR(60) COLLATE utf8_bin NOT NULL DEFAULT '',
		TO_USER 			CHAR(32) COLLATE utf8_bin NOT NULL DEFAULT '',
		WITH_ADMIN_OPTION 	ENUM('N','Y') CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'N',
		PRIMARY KEY (FROM_HOST,FROM_USER,TO_HOST,TO_USER)
	);`

	// CreateDefaultRolesTable stores the active roles for a user.
	CreateDefaultRolesTable = `CREATE TABLE IF NOT EXISTS mysql.default_roles (
		HOST 				CHAR(60) COLLATE utf8_bin NOT NULL DEFAULT '',
		USER 				CHAR(32) COLLATE utf8_bin NOT NULL DEFAULT '',
		DEFAULT_ROLE_HOST 	CHAR(60) COLLATE utf8_bin NOT NULL DEFAULT '%',
		DEFAULT_ROLE_USER 	CHAR(32) COLLATE utf8_bin NOT NULL DEFAULT '',
		PRIMARY KEY (HOST,USER,DEFAULT_ROLE_HOST,DEFAULT_ROLE_USER)
	)`

	// CreateStatsTopNTable stores topn data of a cmsketch with top n.
	CreateStatsTopNTable = `CREATE TABLE IF NOT EXISTS mysql.stats_top_n (
		table_id 	BIGINT(64) NOT NULL,
		is_index 	TINYINT(2) NOT NULL,
		hist_id 	BIGINT(64) NOT NULL,
		value 		LONGBLOB,
		count 		BIGINT(64) UNSIGNED NOT NULL,
		INDEX tbl(table_id, is_index, hist_id)
	);`

	// CreateStatsFMSketchTable stores FMSketch data of a column histogram.
	CreateStatsFMSketchTable = `CREATE TABLE IF NOT EXISTS mysql.stats_fm_sketch (
		table_id 	BIGINT(64) NOT NULL,
		is_index 	TINYINT(2) NOT NULL,
		hist_id 	BIGINT(64) NOT NULL,
		value 		LONGBLOB,
		INDEX tbl(table_id, is_index, hist_id)
	);`

	// CreateExprPushdownBlacklist stores the expressions which are not allowed to be pushed down.
	CreateExprPushdownBlacklist = `CREATE TABLE IF NOT EXISTS mysql.expr_pushdown_blacklist (
		name 		CHAR(100) NOT NULL,
		store_type 	CHAR(100) NOT NULL DEFAULT 'tikv,tiflash,tidb',
		reason 		VARCHAR(200)
	);`

	// CreateOptRuleBlacklist stores the list of disabled optimizing operations.
	CreateOptRuleBlacklist = `CREATE TABLE IF NOT EXISTS mysql.opt_rule_blacklist (
		name 	CHAR(100) NOT NULL
	);`

	// CreateStatsExtended stores the registered extended statistics.
	CreateStatsExtended = `CREATE TABLE IF NOT EXISTS mysql.stats_extended (
		name varchar(32) NOT NULL,
		type tinyint(4) NOT NULL,
		table_id bigint(64) NOT NULL,
		column_ids varchar(32) NOT NULL,
		stats blob DEFAULT NULL,
		version bigint(64) unsigned NOT NULL,
		status tinyint(4) NOT NULL,
		PRIMARY KEY(name, table_id),
		KEY idx_1 (table_id, status, version),
		KEY idx_2 (status, version)
	);`

	// CreateSchemaIndexUsageTable stores the index usage information.
	CreateSchemaIndexUsageTable = `CREATE TABLE IF NOT EXISTS mysql.schema_index_usage (
		TABLE_ID bigint(64),
		INDEX_ID bigint(21),
		QUERY_COUNT bigint(64),
		ROWS_SELECTED bigint(64),
		LAST_USED_AT timestamp,
		PRIMARY KEY(TABLE_ID, INDEX_ID)
	);`
	// CreateGlobalGrantsTable stores dynamic privs
	CreateGlobalGrantsTable = `CREATE TABLE IF NOT EXISTS mysql.global_grants (
		USER char(32) NOT NULL DEFAULT '',
		HOST char(255) NOT NULL DEFAULT '',
		PRIV char(32) NOT NULL DEFAULT '',
		WITH_GRANT_OPTION enum('N','Y') NOT NULL DEFAULT 'N',
		PRIMARY KEY (USER,HOST,PRIV)
	);`
	// CreateCapturePlanBaselinesBlacklist stores the baseline capture filter rules.
	CreateCapturePlanBaselinesBlacklist = `CREATE TABLE IF NOT EXISTS mysql.capture_plan_baselines_blacklist (
		id bigint(64) auto_increment,
		filter_type varchar(32) NOT NULL COMMENT "type of the filter, only db, table and frequency supported now",
		filter_value varchar(32) NOT NULL,
		key idx(filter_type),
		primary key(id)
	);`
	// CreateColumnStatsUsageTable stores the column stats usage information.
	CreateColumnStatsUsageTable = `CREATE TABLE IF NOT EXISTS mysql.column_stats_usage (
		table_id BIGINT(64) NOT NULL,
		column_id BIGINT(64) NOT NULL,
		last_used_at TIMESTAMP,
		last_analyzed_at TIMESTAMP,
		PRIMARY KEY (table_id, column_id) CLUSTERED
	);`
	// CreateTableCacheMetaTable stores the cached table meta lock information.
	CreateTableCacheMetaTable = `CREATE TABLE IF NOT EXISTS mysql.table_cache_meta (
		tid bigint(11) NOT NULL DEFAULT 0,
		lock_type enum('NONE','READ', 'INTEND', 'WRITE') NOT NULL DEFAULT 'NONE',
		lease bigint(20) NOT NULL DEFAULT 0,
		oldReadLease bigint(20) NOT NULL DEFAULT 0,
		PRIMARY KEY (tid)
	);`
	// CreateAnalyzeOptionsTable stores the analyze options used by analyze and auto analyze.
	CreateAnalyzeOptionsTable = `CREATE TABLE IF NOT EXISTS mysql.analyze_options (
		table_id BIGINT(64) NOT NULL,
		sample_num BIGINT(64) NOT NULL DEFAULT 0,
		sample_rate DOUBLE NOT NULL DEFAULT -1,
		buckets BIGINT(64) NOT NULL DEFAULT 0,
		topn BIGINT(64) NOT NULL DEFAULT -1,
		column_choice enum('DEFAULT','ALL','PREDICATE','LIST') NOT NULL DEFAULT 'DEFAULT',
		column_ids TEXT(19372),
		PRIMARY KEY (table_id) CLUSTERED
	);`
	// CreateStatsHistory stores the historical stats.
	CreateStatsHistory = `CREATE TABLE IF NOT EXISTS mysql.stats_history (
		table_id bigint(64) NOT NULL,
		stats_data longblob NOT NULL,
		seq_no bigint(64) NOT NULL comment 'sequence number of the gzipped data slice',
		version bigint(64) NOT NULL comment 'stats version which corresponding to stats:version in EXPLAIN',
		create_time datetime(6) NOT NULL,
		UNIQUE KEY table_version_seq (table_id, version, seq_no),
		KEY table_create_time (table_id, create_time, seq_no)
	);`
	// CreateStatsMetaHistory stores the historical meta stats.
	CreateStatsMetaHistory = `CREATE TABLE IF NOT EXISTS mysql.stats_meta_history (
		table_id bigint(64) NOT NULL,
		modify_count bigint(64) NOT NULL,
		count bigint(64) NOT NULL,
		version bigint(64) NOT NULL comment 'stats version which corresponding to stats:version in EXPLAIN',
		create_time datetime(6) NOT NULL,
		UNIQUE KEY table_version (table_id, version),
		KEY table_create_time (table_id, create_time)
	);`
	// CreateAnalyzeJobs stores the analyze jobs.
	CreateAnalyzeJobs = `CREATE TABLE IF NOT EXISTS mysql.analyze_jobs (
		id BIGINT(64) UNSIGNED NOT NULL AUTO_INCREMENT,
		update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
		table_schema CHAR(64) NOT NULL DEFAULT '',
		table_name CHAR(64) NOT NULL DEFAULT '',
		partition_name CHAR(64) NOT NULL DEFAULT '',
		job_info TEXT NOT NULL,
		processed_rows BIGINT(64) UNSIGNED NOT NULL DEFAULT 0,
		start_time TIMESTAMP,
		end_time TIMESTAMP,
		state ENUM('pending', 'running', 'finished', 'failed') NOT NULL,
		fail_reason TEXT,
		instance VARCHAR(512) NOT NULL comment 'address of the TiDB instance executing the analyze job',
		process_id BIGINT(64) UNSIGNED comment 'ID of the process executing the analyze job',
		PRIMARY KEY (id),
		KEY (update_time)
	);`
	// CreateAdvisoryLocks stores the advisory locks (get_lock, release_lock).
	CreateAdvisoryLocks = `CREATE TABLE IF NOT EXISTS mysql.advisory_locks (
		lock_name VARCHAR(64) NOT NULL PRIMARY KEY
	);`
)

// bootstrap initiates system DB for a store.
func bootstrap(s Session) {
	startTime := time.Now()
	dom := domain.GetDomain(s)
	for {
		b, err := checkBootstrapped(s)
		if err != nil {
			logutil.BgLogger().Fatal("check bootstrap error",
				zap.Error(err))
		}
		// For rolling upgrade, we can't do upgrade only in the owner.
		if b {
			upgrade(s)
			logutil.BgLogger().Info("upgrade successful in bootstrap",
				zap.Duration("take time", time.Since(startTime)))
			return
		}
		// To reduce conflict when multiple TiDB-server start at the same time.
		// Actually only one server need to do the bootstrap. So we chose DDL owner to do this.
		if dom.DDL().OwnerManager().IsOwner() {
			doDDLWorks(s)
			doDMLWorks(s)
			logutil.BgLogger().Info("bootstrap successful",
				zap.Duration("take time", time.Since(startTime)))
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
}

const (
	// varTrue is the true value in mysql.TiDB table for boolean columns.
	varTrue = "True"
	// varFalse is the false value in mysql.TiDB table for boolean columns.
	varFalse = "False"
	// The variable name in mysql.TiDB table.
	// It is used for checking if the store is bootstrapped by any TiDB server.
	// If the value is `True`, the store is already bootstrapped by a TiDB server.
	bootstrappedVar = "bootstrapped"
	// The variable name in mysql.TiDB table.
	// It is used for getting the version of the TiDB server which bootstrapped the store.
	tidbServerVersionVar = "tidb_server_version"
	// The variable name in mysql.tidb table and it will be used when we want to know
	// system timezone.
	tidbSystemTZ = "system_tz"
	// The variable name in mysql.tidb table and it will indicate if the new collations are enabled in the TiDB cluster.
	tidbNewCollationEnabled = "new_collation_enabled"
	// The variable name in mysql.tidb table and it records the default value of
	// mem-quota-query when upgrade from v3.0.x to v4.0.9+.
	tidbDefMemoryQuotaQuery = "default_memory_quota_query"
	// The variable name in mysql.tidb table and it records the default value of
	// oom-action when upgrade from v3.0.x to v4.0.11+.
	tidbDefOOMAction = "default_oom_action"
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
	// version29 is not needed.
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
)

// currentBootstrapVersion is defined as a variable, so we can modify its value for testing.
// please make sure this is the largest version
var currentBootstrapVersion int64 = version91

var (
	bootstrapVersion = []func(Session, int64){
		upgradeToVer2,
		upgradeToVer3,
		upgradeToVer4,
		upgradeToVer5,
		upgradeToVer6,
		upgradeToVer7,
		upgradeToVer8,
		upgradeToVer9,
		upgradeToVer10,
		upgradeToVer11,
		upgradeToVer12,
		upgradeToVer13,
		upgradeToVer14,
		upgradeToVer15,
		upgradeToVer16,
		upgradeToVer17,
		upgradeToVer18,
		upgradeToVer19,
		upgradeToVer20,
		upgradeToVer21,
		upgradeToVer22,
		upgradeToVer23,
		upgradeToVer24,
		upgradeToVer25,
		upgradeToVer26,
		upgradeToVer27,
		upgradeToVer28,
		upgradeToVer29,
		upgradeToVer30,
		upgradeToVer31,
		upgradeToVer32,
		upgradeToVer33,
		upgradeToVer34,
		upgradeToVer35,
		upgradeToVer36,
		upgradeToVer37,
		upgradeToVer38,
		// We will redo upgradeToVer39 in upgradeToVer46,
		// so upgradeToVer39 is skipped here.
		upgradeToVer40,
		upgradeToVer41,
		upgradeToVer42,
		upgradeToVer43,
		upgradeToVer44,
		upgradeToVer45,
		upgradeToVer46,
		upgradeToVer47,
		// We will redo upgradeToVer48 and upgradeToVer49 in upgradeToVer55 and upgradeToVer56,
		// so upgradeToVer48 and upgradeToVer49 is skipped here.
		upgradeToVer50,
		// We will redo upgradeToVer51 in upgradeToVer63, it is skipped here.
		upgradeToVer52,
		upgradeToVer53,
		upgradeToVer54,
		upgradeToVer55,
		upgradeToVer56,
		upgradeToVer57,
		// We will redo upgradeToVer58 in upgradeToVer64, it is skipped here.
		upgradeToVer59,
		upgradeToVer60,
		// We will redo upgradeToVer61 in upgradeToVer67, it is skipped here.
		upgradeToVer62,
		upgradeToVer63,
		upgradeToVer64,
		upgradeToVer65,
		upgradeToVer66,
		upgradeToVer67,
		upgradeToVer68,
		upgradeToVer69,
		upgradeToVer70,
		upgradeToVer71,
		upgradeToVer72,
		upgradeToVer73,
		upgradeToVer74,
		upgradeToVer75,
		upgradeToVer76,
		upgradeToVer77,
		upgradeToVer78,
		upgradeToVer79,
		upgradeToVer80,
		upgradeToVer81,
		upgradeToVer82,
		upgradeToVer83,
		upgradeToVer84,
		upgradeToVer85,
		upgradeToVer86,
		upgradeToVer87,
		upgradeToVer88,
		upgradeToVer89,
		upgradeToVer90,
		upgradeToVer91,
	}
)

func checkBootstrapped(s Session) (bool, error) {
	//  Check if system db exists.
	_, err := s.ExecuteInternal(context.Background(), "USE %n", mysql.SystemDB)
	if err != nil && infoschema.ErrDatabaseNotExists.NotEqual(err) {
		logutil.BgLogger().Fatal("check bootstrap error",
			zap.Error(err))
	}
	// Check bootstrapped variable value in TiDB table.
	sVal, _, err := getTiDBVar(s, bootstrappedVar)
	if err != nil {
		if infoschema.ErrTableNotExists.Equal(err) {
			return false, nil
		}
		return false, errors.Trace(err)
	}
	isBootstrapped := sVal == varTrue
	if isBootstrapped {
		// Make sure that doesn't affect the following operations.
		if err = s.CommitTxn(context.Background()); err != nil {
			return false, errors.Trace(err)
		}
	}
	return isBootstrapped, nil
}

// getTiDBVar gets variable value from mysql.tidb table.
// Those variables are used by TiDB server.
func getTiDBVar(s Session, name string) (sVal string, isNull bool, e error) {
	ctx := context.Background()
	rs, err := s.ExecuteInternal(ctx, `SELECT HIGH_PRIORITY VARIABLE_VALUE FROM %n.%n WHERE VARIABLE_NAME= %?`,
		mysql.SystemDB,
		mysql.TiDBTable,
		name,
	)
	if err != nil {
		return "", true, errors.Trace(err)
	}
	if rs == nil {
		return "", true, errors.New("Wrong number of Recordset")
	}
	defer terror.Call(rs.Close)
	req := rs.NewChunk(nil)
	err = rs.Next(ctx, req)
	if err != nil || req.NumRows() == 0 {
		return "", true, errors.Trace(err)
	}
	row := req.GetRow(0)
	if row.IsNull(0) {
		return "", true, nil
	}
	return row.GetString(0), false, nil
}

// upgrade function  will do some upgrade works, when the system is bootstrapped by low version TiDB server
// For example, add new system variables into mysql.global_variables table.
func upgrade(s Session) {
	ver, err := getBootstrapVersion(s)
	terror.MustNil(err)
	if ver >= currentBootstrapVersion {
		// It is already bootstrapped/upgraded by a higher version TiDB server.
		return
	}
	// Do upgrade works then update bootstrap version.
	for _, upgrade := range bootstrapVersion {
		upgrade(s, ver)
	}

	updateBootstrapVer(s)
	_, err = s.ExecuteInternal(context.Background(), "COMMIT")

	if err != nil {
		sleepTime := 1 * time.Second
		logutil.BgLogger().Info("update bootstrap ver failed",
			zap.Error(err), zap.Duration("sleeping time", sleepTime))
		time.Sleep(sleepTime)
		// Check if TiDB is already upgraded.
		v, err1 := getBootstrapVersion(s)
		if err1 != nil {
			logutil.BgLogger().Fatal("upgrade failed", zap.Error(err1))
		}
		if v >= currentBootstrapVersion {
			// It is already bootstrapped/upgraded by a higher version TiDB server.
			return
		}
		logutil.BgLogger().Fatal("[Upgrade] upgrade failed",
			zap.Int64("from", ver),
			zap.Int64("to", currentBootstrapVersion),
			zap.Error(err))
	}
}

// upgradeToVer2 updates to version 2.
func upgradeToVer2(s Session, ver int64) {
	if ver >= version2 {
		return
	}
	// Version 2 add two system variable for DistSQL concurrency controlling.
	// Insert distsql related system variable.
	distSQLVars := []string{variable.TiDBDistSQLScanConcurrency}
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
func upgradeToVer3(s Session, ver int64) {
	if ver >= version3 {
		return
	}
	// Version 3 fix tx_read_only variable value.
	mustExecute(s, "UPDATE HIGH_PRIORITY %n.%n SET variable_value = '0' WHERE variable_name = 'tx_read_only';", mysql.SystemDB, mysql.GlobalVariablesTable)
}

// upgradeToVer4 updates to version 4.
func upgradeToVer4(s Session, ver int64) {
	if ver >= version4 {
		return
	}
	mustExecute(s, CreateStatsMetaTable)
}

func upgradeToVer5(s Session, ver int64) {
	if ver >= version5 {
		return
	}
	mustExecute(s, CreateStatsColsTable)
	mustExecute(s, CreateStatsBucketsTable)
}

func upgradeToVer6(s Session, ver int64) {
	if ver >= version6 {
		return
	}
	doReentrantDDL(s, "ALTER TABLE mysql.user ADD COLUMN `Super_priv` ENUM('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Show_db_priv`", infoschema.ErrColumnExists)
	// For reasons of compatibility, set the non-exists privilege column value to 'Y', as TiDB doesn't check them in older versions.
	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.user SET Super_priv='Y'")
}

func upgradeToVer7(s Session, ver int64) {
	if ver >= version7 {
		return
	}
	doReentrantDDL(s, "ALTER TABLE mysql.user ADD COLUMN `Process_priv` ENUM('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Drop_priv`", infoschema.ErrColumnExists)
	// For reasons of compatibility, set the non-exists privilege column value to 'Y', as TiDB doesn't check them in older versions.
	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.user SET Process_priv='Y'")
}

func upgradeToVer8(s Session, ver int64) {
	if ver >= version8 {
		return
	}
	// This is a dummy upgrade, it checks whether upgradeToVer7 success, if not, do it again.
	if _, err := s.ExecuteInternal(context.Background(), "SELECT HIGH_PRIORITY `Process_priv` FROM mysql.user LIMIT 0"); err == nil {
		return
	}
	upgradeToVer7(s, ver)
}

func upgradeToVer9(s Session, ver int64) {
	if ver >= version9 {
		return
	}
	doReentrantDDL(s, "ALTER TABLE mysql.user ADD COLUMN `Trigger_priv` ENUM('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Create_user_priv`", infoschema.ErrColumnExists)
	// For reasons of compatibility, set the non-exists privilege column value to 'Y', as TiDB doesn't check them in older versions.
	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.user SET Trigger_priv='Y'")
}

func doReentrantDDL(s Session, sql string, ignorableErrs ...error) {
	_, err := s.ExecuteInternal(context.Background(), sql)
	for _, ignorableErr := range ignorableErrs {
		if terror.ErrorEqual(err, ignorableErr) {
			return
		}
	}
	if err != nil {
		logutil.BgLogger().Fatal("doReentrantDDL error", zap.Error(err))
	}
}

func upgradeToVer10(s Session, ver int64) {
	if ver >= version10 {
		return
	}
	doReentrantDDL(s, "ALTER TABLE mysql.stats_buckets CHANGE COLUMN `value` `upper_bound` BLOB NOT NULL", infoschema.ErrColumnNotExists, infoschema.ErrColumnExists)
	doReentrantDDL(s, "ALTER TABLE mysql.stats_buckets ADD COLUMN `lower_bound` BLOB", infoschema.ErrColumnExists)
	doReentrantDDL(s, "ALTER TABLE mysql.stats_histograms ADD COLUMN `null_count` BIGINT(64) NOT NULL DEFAULT 0", infoschema.ErrColumnExists)
	doReentrantDDL(s, "ALTER TABLE mysql.stats_histograms DROP COLUMN distinct_ratio", dbterror.ErrCantDropFieldOrKey)
	doReentrantDDL(s, "ALTER TABLE mysql.stats_histograms DROP COLUMN use_count_to_estimate", dbterror.ErrCantDropFieldOrKey)
}

func upgradeToVer11(s Session, ver int64) {
	if ver >= version11 {
		return
	}
	_, err := s.ExecuteInternal(context.Background(), "ALTER TABLE mysql.user ADD COLUMN `References_priv` ENUM('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Grant_priv`")
	if err != nil {
		if terror.ErrorEqual(err, infoschema.ErrColumnExists) {
			return
		}
		logutil.BgLogger().Fatal("upgradeToVer11 error", zap.Error(err))
	}
	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.user SET References_priv='Y'")
}

func upgradeToVer12(s Session, ver int64) {
	if ver >= version12 {
		return
	}
	ctx := context.Background()
	_, err := s.ExecuteInternal(ctx, "BEGIN")
	terror.MustNil(err)
	sql := "SELECT HIGH_PRIORITY user, host, password FROM mysql.user WHERE password != ''"
	rs, err := s.ExecuteInternal(ctx, sql)
	if terror.ErrorEqual(err, core.ErrUnknownColumn) {
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

func upgradeToVer13(s Session, ver int64) {
	if ver >= version13 {
		return
	}
	sqls := []string{
		"ALTER TABLE mysql.user ADD COLUMN `Create_tmp_table_priv` ENUM('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Super_priv`",
		"ALTER TABLE mysql.user ADD COLUMN `Lock_tables_priv` ENUM('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Create_tmp_table_priv`",
		"ALTER TABLE mysql.user ADD COLUMN `Create_view_priv` ENUM('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Execute_priv`",
		"ALTER TABLE mysql.user ADD COLUMN `Show_view_priv` ENUM('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Create_view_priv`",
		"ALTER TABLE mysql.user ADD COLUMN `Create_routine_priv` ENUM('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Show_view_priv`",
		"ALTER TABLE mysql.user ADD COLUMN `Alter_routine_priv` ENUM('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Create_routine_priv`",
		"ALTER TABLE mysql.user ADD COLUMN `Event_priv` ENUM('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Create_user_priv`",
	}
	ctx := context.Background()
	for _, sql := range sqls {
		_, err := s.ExecuteInternal(ctx, sql)
		if err != nil {
			if terror.ErrorEqual(err, infoschema.ErrColumnExists) {
				continue
			}
			logutil.BgLogger().Fatal("upgradeToVer13 error", zap.Error(err))
		}
	}
	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.user SET Create_tmp_table_priv='Y',Lock_tables_priv='Y',Create_routine_priv='Y',Alter_routine_priv='Y',Event_priv='Y' WHERE Super_priv='Y'")
	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.user SET Create_view_priv='Y',Show_view_priv='Y' WHERE Create_priv='Y'")
}

func upgradeToVer14(s Session, ver int64) {
	if ver >= version14 {
		return
	}
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
	ctx := context.Background()
	for _, sql := range sqls {
		_, err := s.ExecuteInternal(ctx, sql)
		if err != nil {
			if terror.ErrorEqual(err, infoschema.ErrColumnExists) {
				continue
			}
			logutil.BgLogger().Fatal("upgradeToVer14 error", zap.Error(err))
		}
	}
}

func upgradeToVer15(s Session, ver int64) {
	if ver >= version15 {
		return
	}
	var err error
	_, err = s.ExecuteInternal(context.Background(), CreateGCDeleteRangeTable)
	if err != nil {
		logutil.BgLogger().Fatal("upgradeToVer15 error", zap.Error(err))
	}
}

func upgradeToVer16(s Session, ver int64) {
	if ver >= version16 {
		return
	}
	doReentrantDDL(s, "ALTER TABLE mysql.stats_histograms ADD COLUMN `cm_sketch` BLOB", infoschema.ErrColumnExists)
}

func upgradeToVer17(s Session, ver int64) {
	if ver >= version17 {
		return
	}
	doReentrantDDL(s, "ALTER TABLE mysql.user MODIFY User CHAR(32)")
}

func upgradeToVer18(s Session, ver int64) {
	if ver >= version18 {
		return
	}
	doReentrantDDL(s, "ALTER TABLE mysql.stats_histograms ADD COLUMN `tot_col_size` BIGINT(64) NOT NULL DEFAULT 0", infoschema.ErrColumnExists)
}

func upgradeToVer19(s Session, ver int64) {
	if ver >= version19 {
		return
	}
	doReentrantDDL(s, "ALTER TABLE mysql.db MODIFY User CHAR(32)")
	doReentrantDDL(s, "ALTER TABLE mysql.tables_priv MODIFY User CHAR(32)")
	doReentrantDDL(s, "ALTER TABLE mysql.columns_priv MODIFY User CHAR(32)")
}

func upgradeToVer20(s Session, ver int64) {
	if ver >= version20 {
		return
	}
	doReentrantDDL(s, CreateStatsFeedbackTable)
}

func upgradeToVer21(s Session, ver int64) {
	if ver >= version21 {
		return
	}
	mustExecute(s, CreateGCDeleteRangeDoneTable)

	doReentrantDDL(s, "ALTER TABLE mysql.gc_delete_range DROP INDEX job_id", dbterror.ErrCantDropFieldOrKey)
	doReentrantDDL(s, "ALTER TABLE mysql.gc_delete_range ADD UNIQUE INDEX delete_range_index (job_id, element_id)", dbterror.ErrDupKeyName)
	doReentrantDDL(s, "ALTER TABLE mysql.gc_delete_range DROP INDEX element_id", dbterror.ErrCantDropFieldOrKey)
}

func upgradeToVer22(s Session, ver int64) {
	if ver >= version22 {
		return
	}
	doReentrantDDL(s, "ALTER TABLE mysql.stats_histograms ADD COLUMN `stats_ver` BIGINT(64) NOT NULL DEFAULT 0", infoschema.ErrColumnExists)
}

func upgradeToVer23(s Session, ver int64) {
	if ver >= version23 {
		return
	}
	doReentrantDDL(s, "ALTER TABLE mysql.stats_histograms ADD COLUMN `flag` BIGINT(64) NOT NULL DEFAULT 0", infoschema.ErrColumnExists)
}

// writeSystemTZ writes system timezone info into mysql.tidb
func writeSystemTZ(s Session) {
	mustExecute(s, `INSERT HIGH_PRIORITY INTO %n.%n VALUES (%?, %?, "TiDB Global System Timezone.") ON DUPLICATE KEY UPDATE VARIABLE_VALUE= %?`,
		mysql.SystemDB,
		mysql.TiDBTable,
		tidbSystemTZ,
		timeutil.InferSystemTZ(),
		timeutil.InferSystemTZ(),
	)
}

// upgradeToVer24 initializes `System` timezone according to docs/design/2018-09-10-adding-tz-env.md
func upgradeToVer24(s Session, ver int64) {
	if ver >= version24 {
		return
	}
	writeSystemTZ(s)
}

// upgradeToVer25 updates tidb_max_chunk_size to new low bound value 32 if previous value is small than 32.
func upgradeToVer25(s Session, ver int64) {
	if ver >= version25 {
		return
	}
	sql := fmt.Sprintf("UPDATE HIGH_PRIORITY %[1]s.%[2]s SET VARIABLE_VALUE = '%[4]d' WHERE VARIABLE_NAME = '%[3]s' AND VARIABLE_VALUE < %[4]d",
		mysql.SystemDB, mysql.GlobalVariablesTable, variable.TiDBMaxChunkSize, variable.DefInitChunkSize)
	mustExecute(s, sql)
}

func upgradeToVer26(s Session, ver int64) {
	if ver >= version26 {
		return
	}
	mustExecute(s, CreateRoleEdgesTable)
	mustExecute(s, CreateDefaultRolesTable)
	doReentrantDDL(s, "ALTER TABLE mysql.user ADD COLUMN `Create_role_priv` ENUM('N','Y') DEFAULT 'N'", infoschema.ErrColumnExists)
	doReentrantDDL(s, "ALTER TABLE mysql.user ADD COLUMN `Drop_role_priv` ENUM('N','Y') DEFAULT 'N'", infoschema.ErrColumnExists)
	doReentrantDDL(s, "ALTER TABLE mysql.user ADD COLUMN `Account_locked` ENUM('N','Y') DEFAULT 'N'", infoschema.ErrColumnExists)
	// user with Create_user_Priv privilege should have Create_view_priv and Show_view_priv after upgrade to v3.0
	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.user SET Create_role_priv='Y',Drop_role_priv='Y' WHERE Create_user_priv='Y'")
	// user with Create_Priv privilege should have Create_view_priv and Show_view_priv after upgrade to v3.0
	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.user SET Create_view_priv='Y',Show_view_priv='Y' WHERE Create_priv='Y'")
}

func upgradeToVer27(s Session, ver int64) {
	if ver >= version27 {
		return
	}
	doReentrantDDL(s, "ALTER TABLE mysql.stats_histograms ADD COLUMN `correlation` DOUBLE NOT NULL DEFAULT 0", infoschema.ErrColumnExists)
}

func upgradeToVer28(s Session, ver int64) {
	if ver >= version28 {
		return
	}
	doReentrantDDL(s, CreateBindInfoTable)
}

func upgradeToVer29(s Session, ver int64) {
	// upgradeToVer29 only need to be run when the current version is 28.
	if ver != version28 {
		return
	}
	doReentrantDDL(s, "ALTER TABLE mysql.bind_info CHANGE create_time create_time TIMESTAMP(3)")
	doReentrantDDL(s, "ALTER TABLE mysql.bind_info CHANGE update_time update_time TIMESTAMP(3)")
	doReentrantDDL(s, "ALTER TABLE mysql.bind_info ADD INDEX sql_index (original_sql(1024),default_db(1024))", dbterror.ErrDupKeyName)
}

func upgradeToVer30(s Session, ver int64) {
	if ver >= version30 {
		return
	}
	mustExecute(s, CreateStatsTopNTable)
}

func upgradeToVer31(s Session, ver int64) {
	if ver >= version31 {
		return
	}
	doReentrantDDL(s, "ALTER TABLE mysql.stats_histograms ADD COLUMN `last_analyze_pos` BLOB DEFAULT NULL", infoschema.ErrColumnExists)
}

func upgradeToVer32(s Session, ver int64) {
	if ver >= version32 {
		return
	}
	doReentrantDDL(s, "ALTER TABLE mysql.tables_priv MODIFY table_priv SET('Select','Insert','Update','Delete','Create','Drop','Grant', 'Index', 'Alter', 'Create View', 'Show View', 'Trigger', 'References')")
}

func upgradeToVer33(s Session, ver int64) {
	if ver >= version33 {
		return
	}
	doReentrantDDL(s, CreateExprPushdownBlacklist)
}

func upgradeToVer34(s Session, ver int64) {
	if ver >= version34 {
		return
	}
	doReentrantDDL(s, CreateOptRuleBlacklist)
}

func upgradeToVer35(s Session, ver int64) {
	if ver >= version35 {
		return
	}
	sql := fmt.Sprintf("UPDATE HIGH_PRIORITY %s.%s SET VARIABLE_NAME = '%s' WHERE VARIABLE_NAME = 'tidb_back_off_weight'",
		mysql.SystemDB, mysql.GlobalVariablesTable, variable.TiDBBackOffWeight)
	mustExecute(s, sql)
}

func upgradeToVer36(s Session, ver int64) {
	if ver >= version36 {
		return
	}
	doReentrantDDL(s, "ALTER TABLE mysql.user ADD COLUMN `Shutdown_priv` ENUM('N','Y') DEFAULT 'N'", infoschema.ErrColumnExists)
	// A root user will have those privileges after upgrading.
	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.user SET Shutdown_priv='Y' WHERE Super_priv='Y'")
	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.user SET Create_tmp_table_priv='Y',Lock_tables_priv='Y',Create_routine_priv='Y',Alter_routine_priv='Y',Event_priv='Y' WHERE Super_priv='Y'")
}

func upgradeToVer37(s Session, ver int64) {
	if ver >= version37 {
		return
	}
	// when upgrade from old tidb and no 'tidb_enable_window_function' in GLOBAL_VARIABLES, init it with 0.
	sql := fmt.Sprintf("INSERT IGNORE INTO  %s.%s (`VARIABLE_NAME`, `VARIABLE_VALUE`) VALUES ('%s', '%d')",
		mysql.SystemDB, mysql.GlobalVariablesTable, variable.TiDBEnableWindowFunction, 0)
	mustExecute(s, sql)
}

func upgradeToVer38(s Session, ver int64) {
	if ver >= version38 {
		return
	}
	var err error
	_, err = s.ExecuteInternal(context.Background(), CreateGlobalPrivTable)
	if err != nil {
		logutil.BgLogger().Fatal("upgradeToVer38 error", zap.Error(err))
	}
}

func writeNewCollationParameter(s Session, flag bool) {
	comment := "If the new collations are enabled. Do not edit it."
	b := varFalse
	if flag {
		b = varTrue
	}
	mustExecute(s, `INSERT HIGH_PRIORITY INTO %n.%n VALUES (%?, %?, %?) ON DUPLICATE KEY UPDATE VARIABLE_VALUE=%?`,
		mysql.SystemDB, mysql.TiDBTable, tidbNewCollationEnabled, b, comment, b,
	)
}

func upgradeToVer40(s Session, ver int64) {
	if ver >= version40 {
		return
	}
	// There is no way to enable new collation for an existing TiDB cluster.
	writeNewCollationParameter(s, false)
}

func upgradeToVer41(s Session, ver int64) {
	if ver >= version41 {
		return
	}
	doReentrantDDL(s, "ALTER TABLE mysql.user CHANGE `password` `authentication_string` TEXT", infoschema.ErrColumnExists, infoschema.ErrColumnNotExists)
	doReentrantDDL(s, "ALTER TABLE mysql.user ADD COLUMN `password` TEXT as (`authentication_string`)", infoschema.ErrColumnExists)
}

// writeDefaultExprPushDownBlacklist writes default expr pushdown blacklist into mysql.expr_pushdown_blacklist
func writeDefaultExprPushDownBlacklist(s Session) {
	mustExecute(s, "INSERT HIGH_PRIORITY INTO mysql.expr_pushdown_blacklist VALUES"+
		"('date_add','tiflash', 'DST(daylight saving time) does not take effect in TiFlash date_add')")
}

func upgradeToVer42(s Session, ver int64) {
	if ver >= version42 {
		return
	}
	doReentrantDDL(s, "ALTER TABLE mysql.expr_pushdown_blacklist ADD COLUMN `store_type` CHAR(100) NOT NULL DEFAULT 'tikv,tiflash,tidb'", infoschema.ErrColumnExists)
	doReentrantDDL(s, "ALTER TABLE mysql.expr_pushdown_blacklist ADD COLUMN `reason` VARCHAR(200)", infoschema.ErrColumnExists)
	writeDefaultExprPushDownBlacklist(s)
}

// Convert statement summary global variables to non-empty values.
func writeStmtSummaryVars(s Session) {
	sql := "UPDATE %n.%n SET variable_value= %? WHERE variable_name= %? AND variable_value=''"
	mustExecute(s, sql, mysql.SystemDB, mysql.GlobalVariablesTable, variable.BoolToOnOff(variable.DefTiDBEnableStmtSummary), variable.TiDBEnableStmtSummary)
	mustExecute(s, sql, mysql.SystemDB, mysql.GlobalVariablesTable, variable.BoolToOnOff(variable.DefTiDBStmtSummaryInternalQuery), variable.TiDBStmtSummaryInternalQuery)
	mustExecute(s, sql, mysql.SystemDB, mysql.GlobalVariablesTable, strconv.Itoa(variable.DefTiDBStmtSummaryRefreshInterval), variable.TiDBStmtSummaryRefreshInterval)
	mustExecute(s, sql, mysql.SystemDB, mysql.GlobalVariablesTable, strconv.Itoa(variable.DefTiDBStmtSummaryHistorySize), variable.TiDBStmtSummaryHistorySize)
	mustExecute(s, sql, mysql.SystemDB, mysql.GlobalVariablesTable, strconv.FormatUint(uint64(variable.DefTiDBStmtSummaryMaxStmtCount), 10), variable.TiDBStmtSummaryMaxStmtCount)
	mustExecute(s, sql, mysql.SystemDB, mysql.GlobalVariablesTable, strconv.FormatUint(uint64(variable.DefTiDBStmtSummaryMaxSQLLength), 10), variable.TiDBStmtSummaryMaxSQLLength)
}

func upgradeToVer43(s Session, ver int64) {
	if ver >= version43 {
		return
	}
	writeStmtSummaryVars(s)
}

func upgradeToVer44(s Session, ver int64) {
	if ver >= version44 {
		return
	}
	mustExecute(s, "DELETE FROM mysql.global_variables where variable_name = \"tidb_isolation_read_engines\"")
}

func upgradeToVer45(s Session, ver int64) {
	if ver >= version45 {
		return
	}
	doReentrantDDL(s, "ALTER TABLE mysql.user ADD COLUMN `Config_priv` ENUM('N','Y') DEFAULT 'N'", infoschema.ErrColumnExists)
	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.user SET Config_priv='Y' WHERE Super_priv='Y'")
}

// In v3.1.1, we wrongly replace the context of upgradeToVer39 with upgradeToVer44. If we upgrade from v3.1.1 to a newer version,
// upgradeToVer39 will be missed. So we redo upgradeToVer39 here to make sure the upgrading from v3.1.1 succeed.
func upgradeToVer46(s Session, ver int64) {
	if ver >= version46 {
		return
	}
	doReentrantDDL(s, "ALTER TABLE mysql.user ADD COLUMN `Reload_priv` ENUM('N','Y') DEFAULT 'N'", infoschema.ErrColumnExists)
	doReentrantDDL(s, "ALTER TABLE mysql.user ADD COLUMN `File_priv` ENUM('N','Y') DEFAULT 'N'", infoschema.ErrColumnExists)
	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.user SET Reload_priv='Y' WHERE Super_priv='Y'")
	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.user SET File_priv='Y' WHERE Super_priv='Y'")
}

func upgradeToVer47(s Session, ver int64) {
	if ver >= version47 {
		return
	}
	doReentrantDDL(s, "ALTER TABLE mysql.bind_info ADD COLUMN `source` varchar(10) NOT NULL default 'unknown'", infoschema.ErrColumnExists)
}

func upgradeToVer50(s Session, ver int64) {
	if ver >= version50 {
		return
	}
	doReentrantDDL(s, CreateSchemaIndexUsageTable)
}

func upgradeToVer52(s Session, ver int64) {
	if ver >= version52 {
		return
	}
	doReentrantDDL(s, "ALTER TABLE mysql.stats_histograms MODIFY cm_sketch BLOB(6291456)")
}

func upgradeToVer53(s Session, ver int64) {
	if ver >= version53 {
		return
	}
	// when upgrade from old tidb and no `tidb_enable_strict_double_type_check` in GLOBAL_VARIABLES, init it with 1`
	sql := fmt.Sprintf("INSERT IGNORE INTO %s.%s (`VARIABLE_NAME`, `VARIABLE_VALUE`) VALUES ('%s', '%d')",
		mysql.SystemDB, mysql.GlobalVariablesTable, variable.TiDBEnableStrictDoubleTypeCheck, 0)
	mustExecute(s, sql)
}

func upgradeToVer54(s Session, ver int64) {
	if ver >= version54 {
		return
	}
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
func upgradeToVer55(s Session, ver int64) {
	if ver >= version55 {
		return
	}
	defValues := map[string]string{
		variable.TiDBIndexLookupConcurrency:     "4",
		variable.TiDBIndexLookupJoinConcurrency: "4",
		variable.TiDBHashAggFinalConcurrency:    "4",
		variable.TiDBHashAggPartialConcurrency:  "4",
		variable.TiDBWindowConcurrency:          "4",
		variable.TiDBProjectionConcurrency:      "4",
		variable.TiDBHashJoinConcurrency:        "5",
	}
	names := make([]string, 0, len(defValues))
	for n := range defValues {
		names = append(names, n)
	}

	selectSQL := "select HIGH_PRIORITY * from mysql.global_variables where variable_name in ('" + strings.Join(names, quoteCommaQuote) + "')"
	ctx := context.Background()
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
	v := strconv.Itoa(variable.ConcurrencyUnset)
	sql := fmt.Sprintf("UPDATE %s.%s SET variable_value='%%s' WHERE variable_name='%%s'", mysql.SystemDB, mysql.GlobalVariablesTable)
	for _, name := range names {
		mustExecute(s, fmt.Sprintf(sql, v, name))
	}
	mustExecute(s, "COMMIT")
}

// When cherry-pick upgradeToVer54 to v4.0, we wrongly name it upgradeToVer49.
// If we upgrade from v4.0 to a newer version, the real upgradeToVer49 will be missed.
// So we redo upgradeToVer49 here to make sure the upgrading from v4.0 succeeds.
func upgradeToVer56(s Session, ver int64) {
	if ver >= version56 {
		return
	}
	doReentrantDDL(s, CreateStatsExtended)
}

func upgradeToVer57(s Session, ver int64) {
	if ver >= version57 {
		return
	}
	insertBuiltinBindInfoRow(s)
}

func initBindInfoTable(s Session) {
	mustExecute(s, CreateBindInfoTable)
	insertBuiltinBindInfoRow(s)
}

func insertBuiltinBindInfoRow(s Session) {
	mustExecute(s, `INSERT HIGH_PRIORITY INTO mysql.bind_info VALUES (%?, %?, "mysql", %?, "0000-00-00 00:00:00", "0000-00-00 00:00:00", "", "", %?)`,
		bindinfo.BuiltinPseudoSQL4BindLock, bindinfo.BuiltinPseudoSQL4BindLock, bindinfo.Builtin, bindinfo.Builtin,
	)
}

func upgradeToVer59(s Session, ver int64) {
	if ver >= version59 {
		return
	}
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

func upgradeToVer60(s Session, ver int64) {
	if ver >= version60 {
		return
	}
	mustExecute(s, "DROP TABLE IF EXISTS mysql.stats_extended")
	doReentrantDDL(s, CreateStatsExtended)
}

type bindInfo struct {
	bindSQL    string
	status     string
	createTime types.Time
	charset    string
	collation  string
	source     string
}

func upgradeToVer67(s Session, ver int64) {
	if ver >= version67 {
		return
	}
	bindMap := make(map[string]bindInfo)
	h := &bindinfo.BindHandle{}
	var err error
	mustExecute(s, "BEGIN PESSIMISTIC")

	defer func() {
		if err != nil {
			mustExecute(s, "ROLLBACK")
			return
		}

		mustExecute(s, "COMMIT")
	}()
	mustExecute(s, h.LockBindInfoSQL())
	var rs sqlexec.RecordSet
	rs, err = s.ExecuteInternal(context.Background(),
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

		if status != bindinfo.Enabled && status != bindinfo.Using && status != bindinfo.Builtin {
			continue
		}

		charset := row.GetString(4)
		collation := row.GetString(5)
		stmt, err := p.ParseOneStmt(bind, charset, collation)
		if err != nil {
			logutil.BgLogger().Fatal("updateBindInfo error", zap.Error(err))
		}
		originWithDB := parser.Normalize(utilparser.RestoreWithDefaultDB(stmt, db, bind))
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

func writeMemoryQuotaQuery(s Session) {
	comment := "memory_quota_query is 32GB by default in v3.0.x, 1GB by default in v4.0.x+"
	mustExecute(s, `INSERT HIGH_PRIORITY INTO %n.%n VALUES (%?, %?, %?) ON DUPLICATE KEY UPDATE VARIABLE_VALUE=%?`,
		mysql.SystemDB, mysql.TiDBTable, tidbDefMemoryQuotaQuery, 32<<30, comment, 32<<30,
	)
}

func upgradeToVer62(s Session, ver int64) {
	if ver >= version62 {
		return
	}
	doReentrantDDL(s, "ALTER TABLE mysql.stats_buckets ADD COLUMN `ndv` bigint not null default 0", infoschema.ErrColumnExists)
}

func upgradeToVer63(s Session, ver int64) {
	if ver >= version63 {
		return
	}
	doReentrantDDL(s, "ALTER TABLE mysql.user ADD COLUMN `Create_tablespace_priv` ENUM('N','Y') DEFAULT 'N'", infoschema.ErrColumnExists)
	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.user SET Create_tablespace_priv='Y' where Super_priv='Y'")
}

func upgradeToVer64(s Session, ver int64) {
	if ver >= version64 {
		return
	}
	doReentrantDDL(s, "ALTER TABLE mysql.user ADD COLUMN `Repl_slave_priv` ENUM('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Execute_priv`", infoschema.ErrColumnExists)
	doReentrantDDL(s, "ALTER TABLE mysql.user ADD COLUMN `Repl_client_priv` ENUM('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Repl_slave_priv`", infoschema.ErrColumnExists)
	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.user SET Repl_slave_priv='Y',Repl_client_priv='Y' where Super_priv='Y'")
}

func upgradeToVer65(s Session, ver int64) {
	if ver >= version65 {
		return
	}
	doReentrantDDL(s, CreateStatsFMSketchTable)
}

func upgradeToVer66(s Session, ver int64) {
	if ver >= version66 {
		return
	}
	mustExecute(s, "set @@global.tidb_track_aggregate_memory_usage = 1")
}

func upgradeToVer68(s Session, ver int64) {
	if ver >= version68 {
		return
	}
	mustExecute(s, "DELETE FROM mysql.global_variables where VARIABLE_NAME = 'tidb_enable_clustered_index' and VARIABLE_VALUE = 'OFF'")
}

func upgradeToVer69(s Session, ver int64) {
	if ver >= version69 {
		return
	}
	doReentrantDDL(s, CreateGlobalGrantsTable)
}

func upgradeToVer70(s Session, ver int64) {
	if ver >= version70 {
		return
	}
	doReentrantDDL(s, "ALTER TABLE mysql.user ADD COLUMN plugin CHAR(64) AFTER authentication_string", infoschema.ErrColumnExists)
	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.user SET plugin='mysql_native_password'")
}

func upgradeToVer71(s Session, ver int64) {
	if ver >= version71 {
		return
	}
	mustExecute(s, "UPDATE mysql.global_variables SET VARIABLE_VALUE='OFF' WHERE VARIABLE_NAME = 'tidb_multi_statement_mode' AND VARIABLE_VALUE = 'WARN'")
}

func upgradeToVer72(s Session, ver int64) {
	if ver >= version72 {
		return
	}
	doReentrantDDL(s, "ALTER TABLE mysql.stats_meta ADD COLUMN snapshot BIGINT(64) UNSIGNED NOT NULL DEFAULT 0", infoschema.ErrColumnExists)
}

func upgradeToVer73(s Session, ver int64) {
	if ver >= version73 {
		return
	}
	doReentrantDDL(s, CreateCapturePlanBaselinesBlacklist)
}

func upgradeToVer74(s Session, ver int64) {
	if ver >= version74 {
		return
	}
	// The old default value of `tidb_stmt_summary_max_stmt_count` is 200, we want to enlarge this to the new default value when TiDB upgrade.
	mustExecute(s, fmt.Sprintf("UPDATE mysql.global_variables SET VARIABLE_VALUE='%[1]v' WHERE VARIABLE_NAME = 'tidb_stmt_summary_max_stmt_count' AND CAST(VARIABLE_VALUE AS SIGNED) = 200", variable.DefTiDBStmtSummaryMaxStmtCount))
}

func upgradeToVer75(s Session, ver int64) {
	if ver >= version75 {
		return
	}
	doReentrantDDL(s, "ALTER TABLE mysql.user MODIFY COLUMN Host CHAR(255)")
	doReentrantDDL(s, "ALTER TABLE mysql.global_priv MODIFY COLUMN Host CHAR(255)")
	doReentrantDDL(s, "ALTER TABLE mysql.db MODIFY COLUMN Host CHAR(255)")
	doReentrantDDL(s, "ALTER TABLE mysql.tables_priv MODIFY COLUMN Host CHAR(255)")
	doReentrantDDL(s, "ALTER TABLE mysql.columns_priv MODIFY COLUMN Host CHAR(255)")
}

func upgradeToVer76(s Session, ver int64) {
	if ver >= version76 {
		return
	}
	doReentrantDDL(s, "ALTER TABLE mysql.columns_priv MODIFY COLUMN Column_priv SET('Select','Insert','Update','References')")
}

func upgradeToVer77(s Session, ver int64) {
	if ver >= version77 {
		return
	}
	doReentrantDDL(s, CreateColumnStatsUsageTable)
}

func upgradeToVer78(s Session, ver int64) {
	if ver >= version78 {
		return
	}
	doReentrantDDL(s, "ALTER TABLE mysql.stats_buckets MODIFY upper_bound LONGBLOB NOT NULL")
	doReentrantDDL(s, "ALTER TABLE mysql.stats_buckets MODIFY lower_bound LONGBLOB")
	doReentrantDDL(s, "ALTER TABLE mysql.stats_histograms MODIFY last_analyze_pos LONGBLOB DEFAULT NULL")
}

func upgradeToVer79(s Session, ver int64) {
	if ver >= version79 {
		return
	}
	doReentrantDDL(s, CreateTableCacheMetaTable)
}

func upgradeToVer80(s Session, ver int64) {
	if ver >= version80 {
		return
	}
	// Check if tidb_analyze_version exists in mysql.GLOBAL_VARIABLES.
	// If not, insert "tidb_analyze_version | 1" since this is the old behavior before we introduce this variable.
	ctx := context.Background()
	rs, err := s.ExecuteInternal(ctx, "SELECT VARIABLE_VALUE FROM %n.%n WHERE VARIABLE_NAME=%?;",
		mysql.SystemDB, mysql.GlobalVariablesTable, variable.TiDBAnalyzeVersion)
	terror.MustNil(err)
	req := rs.NewChunk(nil)
	err = rs.Next(ctx, req)
	terror.MustNil(err)
	if req.NumRows() != 0 {
		return
	}

	mustExecute(s, "INSERT HIGH_PRIORITY IGNORE INTO %n.%n VALUES (%?, %?);",
		mysql.SystemDB, mysql.GlobalVariablesTable, variable.TiDBAnalyzeVersion, 1)
}

// For users that upgrade TiDB from a pre-4.0 version, we want to disable index merge by default.
// This helps minimize query plan regressions.
func upgradeToVer81(s Session, ver int64) {
	if ver >= version81 {
		return
	}
	// Check if tidb_enable_index_merge exists in mysql.GLOBAL_VARIABLES.
	// If not, insert "tidb_enable_index_merge | off".
	ctx := context.Background()
	rs, err := s.ExecuteInternal(ctx, "SELECT VARIABLE_VALUE FROM %n.%n WHERE VARIABLE_NAME=%?;",
		mysql.SystemDB, mysql.GlobalVariablesTable, variable.TiDBEnableIndexMerge)
	terror.MustNil(err)
	req := rs.NewChunk(nil)
	err = rs.Next(ctx, req)
	terror.MustNil(err)
	if req.NumRows() != 0 {
		return
	}

	mustExecute(s, "INSERT HIGH_PRIORITY IGNORE INTO %n.%n VALUES (%?, %?);",
		mysql.SystemDB, mysql.GlobalVariablesTable, variable.TiDBEnableIndexMerge, variable.Off)
}

func upgradeToVer82(s Session, ver int64) {
	if ver >= version82 {
		return
	}
	doReentrantDDL(s, CreateAnalyzeOptionsTable)
}

func upgradeToVer83(s Session, ver int64) {
	if ver >= version83 {
		return
	}
	doReentrantDDL(s, CreateStatsHistory)
}

func upgradeToVer84(s Session, ver int64) {
	if ver >= version84 {
		return
	}
	doReentrantDDL(s, CreateStatsMetaHistory)
}

func upgradeToVer85(s Session, ver int64) {
	if ver >= version85 {
		return
	}
	mustExecute(s, fmt.Sprintf("UPDATE HIGH_PRIORITY mysql.bind_info SET status= '%s' WHERE status = '%s'", bindinfo.Enabled, bindinfo.Using))
}

func upgradeToVer86(s Session, ver int64) {
	if ver >= version86 {
		return
	}
	doReentrantDDL(s, "ALTER TABLE mysql.tables_priv MODIFY COLUMN Column_priv SET('Select','Insert','Update','References')")
}

func upgradeToVer87(s Session, ver int64) {
	if ver >= version87 {
		return
	}
	doReentrantDDL(s, CreateAnalyzeJobs)
}

func upgradeToVer88(s Session, ver int64) {
	if ver >= version88 {
		return
	}
	doReentrantDDL(s, "ALTER TABLE mysql.user CHANGE `Repl_slave_priv` `Repl_slave_priv` ENUM('N','Y') NOT NULL DEFAULT 'N' AFTER `Execute_priv`")
	doReentrantDDL(s, "ALTER TABLE mysql.user CHANGE `Repl_client_priv` `Repl_client_priv` ENUM('N','Y') NOT NULL DEFAULT 'N' AFTER `Repl_slave_priv`")
}

func upgradeToVer89(s Session, ver int64) {
	if ver >= version89 {
		return
	}
	doReentrantDDL(s, CreateAdvisoryLocks)
}

// importConfigOption is a one-time import.
// It is intended to be used to convert a config option to a sysvar.
// It reads the config value from the tidb-server executing the bootstrap
// (not guaranteed to be the same on all servers), and writes a message
// to the error log. The message is important since the behavior is weird
// (changes to the config file will no longer take effect past this point).
func importConfigOption(s Session, configName, svName, valStr string) {
	message := fmt.Sprintf("%s is now configured by the system variable %s. One-time importing the value specified in tidb.toml file", configName, svName)
	logutil.BgLogger().Warn(message, zap.String("value", valStr))
	// We use insert ignore, since if its a duplicate we don't want to overwrite any user-set values.
	sql := fmt.Sprintf("INSERT IGNORE INTO  %s.%s (`VARIABLE_NAME`, `VARIABLE_VALUE`) VALUES ('%s', '%s')",
		mysql.SystemDB, mysql.GlobalVariablesTable, svName, valStr)
	mustExecute(s, sql)
}

func upgradeToVer90(s Session, ver int64) {
	if ver >= version90 {
		return
	}
	valStr := variable.BoolToOnOff(config.GetGlobalConfig().EnableBatchDML)
	importConfigOption(s, "enable-batch-dml", variable.TiDBEnableBatchDML, valStr)
	valStr = fmt.Sprint(config.GetGlobalConfig().MemQuotaQuery)
	importConfigOption(s, "mem-quota-query", variable.TiDBMemQuotaQuery, valStr)
	valStr = fmt.Sprint(config.GetGlobalConfig().Log.QueryLogMaxLen)
	importConfigOption(s, "query-log-max-len", variable.TiDBQueryLogMaxLen, valStr)
	valStr = fmt.Sprint(config.GetGlobalConfig().Performance.CommitterConcurrency)
	importConfigOption(s, "committer-concurrency", variable.TiDBCommitterConcurrency, valStr)
	valStr = variable.BoolToOnOff(config.GetGlobalConfig().Performance.RunAutoAnalyze)
	importConfigOption(s, "run-auto-analyze", variable.TiDBEnableAutoAnalyze, valStr)
	valStr = config.GetGlobalConfig().OOMAction
	importConfigOption(s, "oom-action", variable.TiDBMemOOMAction, valStr)
}

func upgradeToVer91(s Session, ver int64) {
	if ver >= version91 {
		return
	}
	valStr := variable.BoolToOnOff(config.GetGlobalConfig().PreparedPlanCache.Enabled)
	importConfigOption(s, "prepared-plan-cache.enable", variable.TiDBEnablePrepPlanCache, valStr)

	valStr = strconv.Itoa(int(config.GetGlobalConfig().PreparedPlanCache.Capacity))
	importConfigOption(s, "prepared-plan-cache.capacity", variable.TiDBPrepPlanCacheSize, valStr)

	valStr = strconv.FormatFloat(config.GetGlobalConfig().PreparedPlanCache.MemoryGuardRatio, 'f', -1, 64)
	importConfigOption(s, "prepared-plan-cache.memory-guard-ratio", variable.TiDBPrepPlanCacheMemoryGuardRatio, valStr)
}

func writeOOMAction(s Session) {
	comment := "oom-action is `log` by default in v3.0.x, `cancel` by default in v4.0.11+"
	mustExecute(s, `INSERT HIGH_PRIORITY INTO %n.%n VALUES (%?, %?, %?) ON DUPLICATE KEY UPDATE VARIABLE_VALUE= %?`,
		mysql.SystemDB, mysql.TiDBTable, tidbDefOOMAction, variable.OOMActionLog, comment, variable.OOMActionLog,
	)
}

// updateBootstrapVer updates bootstrap version variable in mysql.TiDB table.
func updateBootstrapVer(s Session) {
	// Update bootstrap version.
	mustExecute(s, `INSERT HIGH_PRIORITY INTO %n.%n VALUES (%?, %?, "TiDB bootstrap version.") ON DUPLICATE KEY UPDATE VARIABLE_VALUE=%?`,
		mysql.SystemDB, mysql.TiDBTable, tidbServerVersionVar, currentBootstrapVersion, currentBootstrapVersion,
	)
}

// getBootstrapVersion gets bootstrap version from mysql.tidb table;
func getBootstrapVersion(s Session) (int64, error) {
	sVal, isNull, err := getTiDBVar(s, tidbServerVersionVar)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if isNull {
		return 0, nil
	}
	return strconv.ParseInt(sVal, 10, 64)
}

// doDDLWorks executes DDL statements in bootstrap stage.
func doDDLWorks(s Session) {
	// Create a test database.
	mustExecute(s, "CREATE DATABASE IF NOT EXISTS test")
	// Create system db.
	mustExecute(s, "CREATE DATABASE IF NOT EXISTS %n", mysql.SystemDB)
	// Create user table.
	mustExecute(s, CreateUserTable)
	// Create privilege tables.
	mustExecute(s, CreateGlobalPrivTable)
	mustExecute(s, CreateDBPrivTable)
	mustExecute(s, CreateTablePrivTable)
	mustExecute(s, CreateColumnPrivTable)
	// Create global system variable table.
	mustExecute(s, CreateGlobalVariablesTable)
	// Create TiDB table.
	mustExecute(s, CreateTiDBTable)
	// Create help table.
	mustExecute(s, CreateHelpTopic)
	// Create stats_meta table.
	mustExecute(s, CreateStatsMetaTable)
	// Create stats_columns table.
	mustExecute(s, CreateStatsColsTable)
	// Create stats_buckets table.
	mustExecute(s, CreateStatsBucketsTable)
	// Create gc_delete_range table.
	mustExecute(s, CreateGCDeleteRangeTable)
	// Create gc_delete_range_done table.
	mustExecute(s, CreateGCDeleteRangeDoneTable)
	// Create stats_feedback table.
	mustExecute(s, CreateStatsFeedbackTable)
	// Create role_edges table.
	mustExecute(s, CreateRoleEdgesTable)
	// Create default_roles table.
	mustExecute(s, CreateDefaultRolesTable)
	// Create bind_info table.
	initBindInfoTable(s)
	// Create stats_topn_store table.
	mustExecute(s, CreateStatsTopNTable)
	// Create expr_pushdown_blacklist table.
	mustExecute(s, CreateExprPushdownBlacklist)
	// Create opt_rule_blacklist table.
	mustExecute(s, CreateOptRuleBlacklist)
	// Create stats_extended table.
	mustExecute(s, CreateStatsExtended)
	// Create schema_index_usage.
	mustExecute(s, CreateSchemaIndexUsageTable)
	// Create stats_fm_sketch table.
	mustExecute(s, CreateStatsFMSketchTable)
	// Create global_grants
	mustExecute(s, CreateGlobalGrantsTable)
	// Create capture_plan_baselines_blacklist
	mustExecute(s, CreateCapturePlanBaselinesBlacklist)
	// Create column_stats_usage table
	mustExecute(s, CreateColumnStatsUsageTable)
	// Create table_cache_meta table.
	mustExecute(s, CreateTableCacheMetaTable)
	// Create analyze_options table.
	mustExecute(s, CreateAnalyzeOptionsTable)
	// Create stats_history table.
	mustExecute(s, CreateStatsHistory)
	// Create stats_meta_history table.
	mustExecute(s, CreateStatsMetaHistory)
	// Create analyze_jobs table.
	mustExecute(s, CreateAnalyzeJobs)
	// Create advisory_locks table.
	mustExecute(s, CreateAdvisoryLocks)
}

// doDMLWorks executes DML statements in bootstrap stage.
// All the statements run in a single transaction.
// TODO: sanitize.
func doDMLWorks(s Session) {
	mustExecute(s, "BEGIN")
	if config.GetGlobalConfig().Security.SecureBootstrap {
		// If secure bootstrap is enabled, we create a root@localhost account which can login with auth_socket.
		// i.e. mysql -S /tmp/tidb.sock -uroot
		// The auth_socket plugin will validate that the user matches $USER.
		u, err := osuser.Current()
		if err != nil {
			logutil.BgLogger().Fatal("failed to read current user. unable to secure bootstrap.", zap.Error(err))
		}
		mustExecute(s, `INSERT HIGH_PRIORITY INTO mysql.user VALUES
		("localhost", "root", %?, "auth_socket", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "N", "Y", "Y", "Y", "Y", "Y")`, u.Username)
	} else {
		mustExecute(s, `INSERT HIGH_PRIORITY INTO mysql.user VALUES
		("%", "root", "", "mysql_native_password", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "N", "Y", "Y", "Y", "Y", "Y")`)
	}

	// Init global system variables table.
	values := make([]string, 0, len(variable.GetSysVars()))
	for k, v := range variable.GetSysVars() {
		// Only global variables should be inserted.
		if v.HasGlobalScope() {
			vVal := v.Value
			if v.Name == variable.TiDBTxnMode && config.GetGlobalConfig().Store == "tikv" {
				vVal = "pessimistic"
			}
			if v.Name == variable.TiDBRowFormatVersion {
				vVal = strconv.Itoa(variable.DefTiDBRowFormatV2)
			}
			if v.Name == variable.TiDBPartitionPruneMode {
				vVal = variable.DefTiDBPartitionPruneMode
				if flag.Lookup("test.v") != nil || flag.Lookup("check.v") != nil || config.CheckTableBeforeDrop {
					// enable Dynamic Prune by default in test case.
					vVal = string(variable.Dynamic)
				}
			}
			if v.Name == variable.TiDBMemOOMAction {
				if flag.Lookup("test.v") != nil || flag.Lookup("check.v") != nil {
					// Change the OOM action to log for the test suite.
					vVal = variable.OOMActionLog
				}
			}
			if v.Name == variable.TiDBEnableChangeMultiSchema {
				vVal = variable.Off
				if flag.Lookup("test.v") != nil || flag.Lookup("check.v") != nil {
					// enable change multi schema in test case for compatibility with old cases.
					vVal = variable.On
				}
			}
			if v.Name == variable.TiDBEnableAsyncCommit && config.GetGlobalConfig().Store == "tikv" {
				vVal = variable.On
			}
			if v.Name == variable.TiDBEnable1PC && config.GetGlobalConfig().Store == "tikv" {
				vVal = variable.On
			}
			if v.Name == variable.TiDBEnableMutationChecker {
				vVal = variable.On
			}
			if v.Name == variable.TiDBEnableAutoAnalyze {
				if flag.Lookup("test.v") != nil || flag.Lookup("check.v") != nil {
					vVal = variable.Off
				}
			}
			if v.Name == variable.TiDBTxnAssertionLevel {
				vVal = variable.AssertionFastStr
			}
			value := fmt.Sprintf(`("%s", "%s")`, strings.ToLower(k), vVal)
			values = append(values, value)
		}
	}
	sql := fmt.Sprintf("INSERT HIGH_PRIORITY INTO %s.%s VALUES %s;", mysql.SystemDB, mysql.GlobalVariablesTable,
		strings.Join(values, ", "))
	mustExecute(s, sql)

	mustExecute(s, `INSERT HIGH_PRIORITY INTO %n.%n VALUES(%?, %?, "Bootstrap flag. Do not delete.") ON DUPLICATE KEY UPDATE VARIABLE_VALUE=%?`,
		mysql.SystemDB, mysql.TiDBTable, bootstrappedVar, varTrue, varTrue,
	)

	mustExecute(s, `INSERT HIGH_PRIORITY INTO %n.%n VALUES(%?, %?, "Bootstrap version. Do not delete.")`,
		mysql.SystemDB, mysql.TiDBTable, tidbServerVersionVar, currentBootstrapVersion,
	)

	writeSystemTZ(s)

	writeNewCollationParameter(s, config.GetGlobalConfig().NewCollationsEnabledOnFirstBootstrap)

	writeDefaultExprPushDownBlacklist(s)

	writeStmtSummaryVars(s)

	_, err := s.ExecuteInternal(context.Background(), "COMMIT")
	if err != nil {
		sleepTime := 1 * time.Second
		logutil.BgLogger().Info("doDMLWorks failed", zap.Error(err), zap.Duration("sleeping time", sleepTime))
		time.Sleep(sleepTime)
		// Check if TiDB is already bootstrapped.
		b, err1 := checkBootstrapped(s)
		if err1 != nil {
			logutil.BgLogger().Fatal("doDMLWorks failed", zap.Error(err1))
		}
		if b {
			return
		}
		logutil.BgLogger().Fatal("doDMLWorks failed", zap.Error(err))
	}
}

func mustExecute(s Session, sql string, args ...interface{}) {
	_, err := s.ExecuteInternal(context.Background(), sql, args...)
	if err != nil {
		debug.PrintStack()
		logutil.BgLogger().Fatal("mustExecute error", zap.Error(err))
	}
}

// oldPasswordUpgrade upgrade password to MySQL compatible format
func oldPasswordUpgrade(pass string) (string, error) {
	hash1, err := hex.DecodeString(pass)
	if err != nil {
		return "", errors.Trace(err)
	}

	hash2 := auth.Sha1Hash(hash1)
	newpass := fmt.Sprintf("*%X", hash2)
	return newpass, nil
}

// rebuildAllPartitionValueMapAndSorted rebuilds all value map and sorted info for list column partitions with InfoSchema.
func rebuildAllPartitionValueMapAndSorted(s *session) {
	type partitionExpr interface {
		PartitionExpr() (*tables.PartitionExpr, error)
	}

	p := parser.New()
	is := s.GetInfoSchema().(infoschema.InfoSchema)
	for _, dbInfo := range is.AllSchemas() {
		for _, t := range is.SchemaTables(dbInfo.Name) {
			pi := t.Meta().GetPartitionInfo()
			if pi == nil || pi.Type != model.PartitionTypeList {
				continue
			}

			pe, err := t.(partitionExpr).PartitionExpr()
			if err != nil {
				panic("partition table gets partition expression failed")
			}
			for _, cp := range pe.ColPrunes {
				if err = cp.RebuildPartitionValueMapAndSorted(p); err != nil {
					logutil.BgLogger().Warn("build list column partition value map and sorted failed")
					break
				}
			}
		}
	}
}
