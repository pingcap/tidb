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
	"fmt"
	"os"
	osuser "os/user"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/infoschema"
	infoschemacontext "github.com/pingcap/tidb/pkg/infoschema/context"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/metadef"
	"github.com/pingcap/tidb/pkg/owner"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/session/sessionapi"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	storepkg "github.com/pingcap/tidb/pkg/store"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlescape"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"go.uber.org/zap"
)

// bootstrapOwnerKey is the key used by ddl owner mutex during boostrap.
var bootstrapOwnerKey = "/tidb/distributeDDLOwnerLock/"

const (
	// CreateUserTable is the SQL statement creates User table in system db.
	// WARNING: There are some limitations on altering the schema of mysql.user table.
	// Adding columns that are nullable or have default values is permitted.
	// But operations like dropping or renaming columns may break the compatibility with BR.
	// REFERENCE ISSUE: https://github.com/pingcap/tidb/issues/38785
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
		Password_reuse_history  smallint unsigned DEFAULT NULL,
		Password_reuse_time     smallint unsigned DEFAULT NULL,
		User_attributes			json,
		Token_issuer			VARCHAR(255),
		Password_expired		ENUM('N','Y') NOT NULL DEFAULT 'N',
		Password_last_changed	TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
		Password_lifetime		SMALLINT UNSIGNED DEFAULT NULL,
		Max_user_connections 	INT UNSIGNED NOT NULL DEFAULT 0,
		PRIMARY KEY (Host, User),
		KEY i_user (User));`
	// CreateGlobalPrivTable is the SQL statement creates Global scope privilege table in system db.
	CreateGlobalPrivTable = `CREATE TABLE IF NOT EXISTS mysql.global_priv (
		Host CHAR(255) NOT NULL DEFAULT '',
		User CHAR(80) NOT NULL DEFAULT '',
		Priv LONGTEXT NOT NULL DEFAULT '',
		PRIMARY KEY (Host, User),
		KEY i_user (User))`

	// For `mysql.db`, `mysql.tables_priv` and `mysql.columns_priv` table, we have a slight different
	// schema definition with MySQL: columns `DB`/`Table_name`/`Column_name` are defined with case-insensitive
	// collation(in MySQL, they are case-sensitive).

	// The reason behind this is that when writing those records, MySQL always converts those names into lower case
	// while TiDB does not do so in early implementations, which makes some 'GRANT'/'REVOKE' operations case-sensitive.

	// In order to fix this, we decide to explicitly set case-insensitive collation for the related columns here, to
	// make sure:
	// * The 'GRANT'/'REVOKE' could be case-insensitive for new clusters(compatible with MySQL).
	// * Keep all behaviors unchanged for upgraded cluster.

	// CreateDBTable is the SQL statement creates DB scope privilege table in system db.
	CreateDBTable = `CREATE TABLE IF NOT EXISTS mysql.db (
		Host					CHAR(255),
		DB						CHAR(64) CHARSET utf8mb4 COLLATE utf8mb4_general_ci,
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
		PRIMARY KEY (Host, DB, User),
		KEY i_user (User));`
	// CreateTablesPrivTable is the SQL statement creates table scope privilege table in system db.
	CreateTablesPrivTable = `CREATE TABLE IF NOT EXISTS mysql.tables_priv (
		Host		CHAR(255),
		DB			CHAR(64) CHARSET utf8mb4 COLLATE utf8mb4_general_ci,
		User		CHAR(32),
		Table_name	CHAR(64) CHARSET utf8mb4 COLLATE utf8mb4_general_ci,
		Grantor		CHAR(77),
		Timestamp	TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		Table_priv	SET('Select','Insert','Update','Delete','Create','Drop','Grant','Index','Alter','Create View','Show View','Trigger','References'),
		Column_priv	SET('Select','Insert','Update','References'),
		PRIMARY KEY (Host, DB, User, Table_name),
		KEY i_user (User));`
	// CreateColumnsPrivTable is the SQL statement creates column scope privilege table in system db.
	CreateColumnsPrivTable = `CREATE TABLE IF NOT EXISTS mysql.columns_priv (
		Host		CHAR(255),
		DB			CHAR(64) CHARSET utf8mb4 COLLATE utf8mb4_general_ci,
		User		CHAR(32),
		Table_name	CHAR(64) CHARSET utf8mb4 COLLATE utf8mb4_general_ci,
		Column_name	CHAR(64) CHARSET utf8mb4 COLLATE utf8mb4_general_ci,
		Timestamp	TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		Column_priv	SET('Select','Insert','Update','References'),
		PRIMARY KEY (Host, DB, User, Table_name, Column_name),
		KEY i_user (User));`
	// CreateGlobalVariablesTable is the SQL statement creates global variable table in system db.
	// TODO: MySQL puts GLOBAL_VARIABLES table in INFORMATION_SCHEMA db.
	// INFORMATION_SCHEMA is a virtual db in TiDB. So we put this table in system db.
	// Maybe we will put it back to INFORMATION_SCHEMA.
	CreateGlobalVariablesTable = `CREATE TABLE IF NOT EXISTS mysql.global_variables (
		VARIABLE_NAME  VARCHAR(64) NOT NULL PRIMARY KEY,
		VARIABLE_VALUE VARCHAR(16383) DEFAULT NULL);`
	// CreateTiDBTable is the SQL statement creates a table in system db.
	// This table is a key-value struct contains some information used by TiDB.
	// Currently we only put bootstrapped in it which indicates if the system is already bootstrapped.
	CreateTiDBTable = `CREATE TABLE IF NOT EXISTS mysql.tidb (
		VARIABLE_NAME  	VARCHAR(64) NOT NULL PRIMARY KEY,
		VARIABLE_VALUE 	VARCHAR(1024) DEFAULT NULL,
		COMMENT 		VARCHAR(1024));`

	// CreateHelpTopicTable is the SQL statement creates help_topic table in system db.
	// See: https://dev.mysql.com/doc/refman/5.5/en/system-database.html#system-database-help-tables
	CreateHelpTopicTable = `CREATE TABLE IF NOT EXISTS mysql.help_topic (
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
		version 					BIGINT(64) UNSIGNED NOT NULL,
		table_id 					BIGINT(64) NOT NULL,
		modify_count				BIGINT(64) NOT NULL DEFAULT 0,
		count 						BIGINT(64) UNSIGNED NOT NULL DEFAULT 0,
		snapshot        			BIGINT(64) UNSIGNED NOT NULL DEFAULT 0,
		last_stats_histograms_version 	BIGINT(64) UNSIGNED DEFAULT NULL,
		INDEX idx_ver(version),
		UNIQUE INDEX tbl(table_id)
	);`

	// CreateStatsHistogramsTable stores the statistics of table columns.
	CreateStatsHistogramsTable = `CREATE TABLE IF NOT EXISTS mysql.stats_histograms (
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
	// NOTE: Feedback is deprecated, but we still need to create this table for compatibility.
	CreateStatsFeedbackTable = `CREATE TABLE IF NOT EXISTS mysql.stats_feedback (
		table_id 	BIGINT(64) NOT NULL,
		is_index 	TINYINT(2) NOT NULL,
		hist_id 	BIGINT(64) NOT NULL,
		feedback 	BLOB NOT NULL,
		INDEX hist(table_id, is_index, hist_id)
	);`

	// CreateBindInfoTable stores the sql bind info which is used to update globalBindCache.
	CreateBindInfoTable = `CREATE TABLE IF NOT EXISTS mysql.bind_info (
		original_sql LONGTEXT NOT NULL,
		bind_sql LONGTEXT NOT NULL,
		default_db TEXT NOT NULL,
		status TEXT NOT NULL,
		create_time TIMESTAMP(6) NOT NULL,
		update_time TIMESTAMP(6) NOT NULL,
		charset TEXT NOT NULL,
		collation TEXT NOT NULL,
		source VARCHAR(10) NOT NULL DEFAULT 'unknown',
		sql_digest varchar(64) DEFAULT NULL,
		plan_digest varchar(64) DEFAULT NULL,
		last_used_date date DEFAULT NULL,
		INDEX sql_index(original_sql(700),default_db(68)) COMMENT "accelerate the speed when add global binding query",
		INDEX time_index(update_time) COMMENT "accelerate the speed when querying with last update time",
		UNIQUE INDEX digest_index(plan_digest, sql_digest) COMMENT "avoid duplicated records"
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
		PRIMARY KEY (HOST,USER,DEFAULT_ROLE_HOST,DEFAULT_ROLE_USER),
		KEY i_user (USER))`

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

	// CreateExprPushdownBlacklistTable stores the expressions which are not allowed to be pushed down.
	CreateExprPushdownBlacklistTable = `CREATE TABLE IF NOT EXISTS mysql.expr_pushdown_blacklist (
		name 		CHAR(100) NOT NULL,
		store_type 	CHAR(100) NOT NULL DEFAULT 'tikv,tiflash,tidb',
		reason 		VARCHAR(200)
	);`

	// CreateOptRuleBlacklistTable stores the list of disabled optimizing operations.
	CreateOptRuleBlacklistTable = `CREATE TABLE IF NOT EXISTS mysql.opt_rule_blacklist (
		name 	CHAR(100) NOT NULL
	);`

	// CreateStatsExtendedTable stores the registered extended statistics.
	CreateStatsExtendedTable = `CREATE TABLE IF NOT EXISTS mysql.stats_extended (
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
		PRIMARY KEY (USER,HOST,PRIV),
		KEY i_user (USER)
	);`
	// CreateCapturePlanBaselinesBlacklistTable stores the baseline capture filter rules.
	CreateCapturePlanBaselinesBlacklistTable = `CREATE TABLE IF NOT EXISTS mysql.capture_plan_baselines_blacklist (
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
	// CreateStatsHistoryTable stores the historical stats.
	CreateStatsHistoryTable = `CREATE TABLE IF NOT EXISTS mysql.stats_history (
		table_id bigint(64) NOT NULL,
		stats_data longblob NOT NULL,
		seq_no bigint(64) NOT NULL comment 'sequence number of the gzipped data slice',
		version bigint(64) NOT NULL comment 'stats version which corresponding to stats:version in EXPLAIN',
		create_time datetime(6) NOT NULL,
		UNIQUE KEY table_version_seq (table_id, version, seq_no),
		KEY table_create_time (table_id, create_time, seq_no),
    	KEY idx_create_time (create_time)
	);`
	// CreateStatsMetaHistoryTable stores the historical meta stats.
	CreateStatsMetaHistoryTable = `CREATE TABLE IF NOT EXISTS mysql.stats_meta_history (
		table_id bigint(64) NOT NULL,
		modify_count bigint(64) NOT NULL,
		count bigint(64) NOT NULL,
		version bigint(64) NOT NULL comment 'stats version which corresponding to stats:version in EXPLAIN',
    	source varchar(40) NOT NULL,
		create_time datetime(6) NOT NULL,
		UNIQUE KEY table_version (table_id, version),
		KEY table_create_time (table_id, create_time),
    	KEY idx_create_time (create_time)
	);`
	// CreateAnalyzeJobsTable stores the analyze jobs.
	CreateAnalyzeJobsTable = `CREATE TABLE IF NOT EXISTS mysql.analyze_jobs (
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
		KEY (update_time),
		INDEX idx_schema_table_state (table_schema, table_name, state),
		INDEX idx_schema_table_partition_state (table_schema, table_name, partition_name, state)
	);`
	// CreateAdvisoryLocksTable stores the advisory locks (get_lock, release_lock).
	CreateAdvisoryLocksTable = `CREATE TABLE IF NOT EXISTS mysql.advisory_locks (
		lock_name VARCHAR(64) NOT NULL PRIMARY KEY
	);`
	// CreateTiDBMDLView is a view about metadata locks.
	CreateTiDBMDLView = `CREATE OR REPLACE SQL SECURITY INVOKER VIEW mysql.tidb_mdl_view as (
		SELECT tidb_mdl_info.job_id,
			JSON_UNQUOTE(JSON_EXTRACT(cast(cast(job_meta as char) as json), "$.schema_name")) as db_name,
			JSON_UNQUOTE(JSON_EXTRACT(cast(cast(job_meta as char) as json), "$.table_name")) as table_name,
			JSON_UNQUOTE(JSON_EXTRACT(cast(cast(job_meta as char) as json), "$.query")) as query,
			session_id,
			cluster_tidb_trx.start_time,
			tidb_decode_sql_digests(all_sql_digests, 4096) AS SQL_DIGESTS
		FROM mysql.tidb_ddl_job,
			mysql.tidb_mdl_info,
			information_schema.cluster_tidb_trx
		WHERE tidb_ddl_job.job_id=tidb_mdl_info.job_id
			AND CONCAT(',', tidb_mdl_info.table_ids, ',') REGEXP CONCAT(',(', REPLACE(cluster_tidb_trx.related_table_ids, ',', '|'), '),') != 0
	);`

	// CreatePlanReplayerStatusTable is a table about plan replayer status
	CreatePlanReplayerStatusTable = `CREATE TABLE IF NOT EXISTS mysql.plan_replayer_status (
		sql_digest VARCHAR(128),
		plan_digest VARCHAR(128),
		origin_sql TEXT,
		token VARCHAR(128),
		update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
		fail_reason TEXT,
		instance VARCHAR(512) NOT NULL comment 'address of the TiDB instance executing the plan replayer job');`

	// CreatePlanReplayerTaskTable is a table about plan replayer capture task
	CreatePlanReplayerTaskTable = `CREATE TABLE IF NOT EXISTS mysql.plan_replayer_task (
		sql_digest VARCHAR(128) NOT NULL,
		plan_digest VARCHAR(128) NOT NULL,
		update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
		PRIMARY KEY (sql_digest,plan_digest));`

	// CreateStatsTableLockedTable stores the locked tables
	CreateStatsTableLockedTable = `CREATE TABLE IF NOT EXISTS mysql.stats_table_locked (
		table_id bigint(64) NOT NULL,
		modify_count bigint(64) NOT NULL DEFAULT 0,
		count bigint(64) NOT NULL DEFAULT 0,
		version bigint(64) UNSIGNED NOT NULL DEFAULT 0,
		PRIMARY KEY (table_id));`

	// CreatePasswordHistoryTable is a table save history passwd.
	CreatePasswordHistoryTable = `CREATE TABLE  IF NOT EXISTS mysql.password_history (
         Host char(255)  NOT NULL DEFAULT '',
         User char(32)  NOT NULL DEFAULT '',
         Password_timestamp timestamp(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
         Password text,
         PRIMARY KEY (Host,User,Password_timestamp )
        ) COMMENT='Password history for user accounts' `

	// CreateTiDBTTLTableStatusTable is a table about TTL job schedule
	CreateTiDBTTLTableStatusTable = `CREATE TABLE IF NOT EXISTS mysql.tidb_ttl_table_status (
		table_id bigint(64) PRIMARY KEY,
        parent_table_id bigint(64),
        table_statistics text DEFAULT NULL,
		last_job_id varchar(64) DEFAULT NULL,
		last_job_start_time timestamp NULL DEFAULT NULL,
		last_job_finish_time timestamp NULL DEFAULT NULL,
		last_job_ttl_expire timestamp NULL DEFAULT NULL,
        last_job_summary text DEFAULT NULL,
		current_job_id varchar(64) DEFAULT NULL,
		current_job_owner_id varchar(64) DEFAULT NULL,
		current_job_owner_addr varchar(256) DEFAULT NULL,
		current_job_owner_hb_time timestamp,
		current_job_start_time timestamp NULL DEFAULT NULL,
		current_job_ttl_expire timestamp NULL DEFAULT NULL,
		current_job_state text DEFAULT NULL,
		current_job_status varchar(64) DEFAULT NULL,
  		current_job_status_update_time timestamp NULL DEFAULT NULL);`

	// CreateTiDBTTLTaskTable is a table about parallel ttl tasks
	CreateTiDBTTLTaskTable = `CREATE TABLE IF NOT EXISTS mysql.tidb_ttl_task (
		job_id varchar(64) NOT NULL,
		table_id bigint(64) NOT NULL,
		scan_id int NOT NULL,
		scan_range_start BLOB,
		scan_range_end BLOB,
		expire_time timestamp NOT NULL,
		owner_id varchar(64) DEFAULT NULL,
		owner_addr varchar(64) DEFAULT NULL,
		owner_hb_time timestamp DEFAULT NULL,
		status varchar(64) DEFAULT 'waiting',
		status_update_time timestamp NULL DEFAULT NULL,
		state text,
		created_time timestamp NOT NULL,
		primary key(job_id, scan_id),
		key(created_time));`

	// CreateTiDBTTLJobHistoryTable is a table that stores ttl job's history
	CreateTiDBTTLJobHistoryTable = `CREATE TABLE IF NOT EXISTS mysql.tidb_ttl_job_history (
		job_id varchar(64) PRIMARY KEY,
		table_id bigint(64) NOT NULL,
        parent_table_id bigint(64) NOT NULL,
    	table_schema varchar(64) NOT NULL,
		table_name varchar(64) NOT NULL,
    	partition_name varchar(64) DEFAULT NULL,
		create_time timestamp NOT NULL,
		finish_time timestamp NOT NULL,
		ttl_expire timestamp NOT NULL,
        summary_text text,
		expired_rows bigint(64) DEFAULT NULL,
    	deleted_rows bigint(64) DEFAULT NULL,
    	error_delete_rows bigint(64) DEFAULT NULL,
    	status varchar(64) NOT NULL,
    	key(table_schema, table_name, create_time),
    	key(parent_table_id, create_time),
    	key(create_time)
	);`

	// CreateTiDBGlobalTaskTable is a table about global task.
	CreateTiDBGlobalTaskTable = `CREATE TABLE IF NOT EXISTS mysql.tidb_global_task (
		id BIGINT(20) NOT NULL AUTO_INCREMENT PRIMARY KEY,
    	task_key VARCHAR(256) NOT NULL,
		type VARCHAR(256) NOT NULL,
		dispatcher_id VARCHAR(261),
		state VARCHAR(64) NOT NULL,
		priority INT DEFAULT 1,
		create_time TIMESTAMP,
		start_time TIMESTAMP,
		state_update_time TIMESTAMP,
		end_time TIMESTAMP,
		meta LONGBLOB,
		concurrency INT(11),
		step INT(11),
		target_scope VARCHAR(256) DEFAULT "",
		error BLOB,
		modify_params json,
		max_node_count INT DEFAULT 0,
		extra_params json,
		keyspace varchar(64) default '',
		key(state),
		key idx_keyspace(keyspace),
		UNIQUE KEY task_key(task_key)
	);`

	// CreateTiDBGlobalTaskHistoryTable is a table about history global task.
	CreateTiDBGlobalTaskHistoryTable = `CREATE TABLE IF NOT EXISTS mysql.tidb_global_task_history (
		id BIGINT(20) NOT NULL AUTO_INCREMENT PRIMARY KEY,
    	task_key VARCHAR(256) NOT NULL,
		type VARCHAR(256) NOT NULL,
		dispatcher_id VARCHAR(261),
		state VARCHAR(64) NOT NULL,
		priority INT DEFAULT 1,
		create_time TIMESTAMP,
		start_time TIMESTAMP,
		state_update_time TIMESTAMP,
		end_time TIMESTAMP,
		meta LONGBLOB,
		concurrency INT(11),
		step INT(11),
		target_scope VARCHAR(256) DEFAULT "",
		error BLOB,
		modify_params json,
		max_node_count INT DEFAULT 0,
		extra_params json,
		keyspace varchar(64) default '',
		key(state),
		key idx_keyspace(keyspace),
		UNIQUE KEY task_key(task_key)
	);`

	// CreateDistFrameworkMetaTable create a system table that distributed task framework use to store meta information
	CreateDistFrameworkMetaTable = `CREATE TABLE IF NOT EXISTS mysql.dist_framework_meta (
        host VARCHAR(261) NOT NULL PRIMARY KEY,
        role VARCHAR(64),
        cpu_count int default 0,
        keyspace_id bigint(8) NOT NULL DEFAULT -1
    );`

	// CreateTiDBRunawayQueriesTable stores the query which is identified as runaway or quarantined because of in watch list.
	CreateTiDBRunawayQueriesTable = `CREATE TABLE IF NOT EXISTS mysql.tidb_runaway_queries (
		resource_group_name varchar(32) not null,
		start_time TIMESTAMP NOT NULL,
		repeats int default 1,
		match_type varchar(12) NOT NULL,
		action varchar(64) NOT NULL,
		sample_sql TEXT NOT NULL,
		sql_digest varchar(64) NOT NULL,
		plan_digest varchar(64) NOT NULL,
		tidb_server varchar(512),
		rule VARCHAR(512) DEFAULT '',
		INDEX plan_index(plan_digest(64)) COMMENT "accelerate the speed when select runaway query",
		INDEX time_index(start_time) COMMENT "accelerate the speed when querying with active watch"
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;`

	// CreateTiDBTimersTable is a table to store all timers for tidb
	CreateTiDBTimersTable = `CREATE TABLE IF NOT EXISTS mysql.tidb_timers (
		ID BIGINT(64) UNSIGNED NOT NULL AUTO_INCREMENT,
		NAMESPACE VARCHAR(256) NOT NULL,
		TIMER_KEY VARCHAR(256) NOT NULL,
		TIMER_DATA BLOB,
		TIMEZONE VARCHAR(64) NOT NULL,
		SCHED_POLICY_TYPE VARCHAR(32) NOT NULL,
		SCHED_POLICY_EXPR VARCHAR(256) NOT NULL,
		HOOK_CLASS VARCHAR(64) NOT NULL,
		WATERMARK TIMESTAMP DEFAULT NULL,
		ENABLE TINYINT(2) NOT NULL,
		TIMER_EXT JSON NOT NULL,
		EVENT_STATUS VARCHAR(32) NOT NULL,
		EVENT_ID VARCHAR(64) NOT NULL,
		EVENT_DATA BLOB,
		EVENT_START TIMESTAMP DEFAULT NULL,
		SUMMARY_DATA BLOB,
		CREATE_TIME TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		UPDATE_TIME TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
		VERSION BIGINT(64) UNSIGNED NOT NULL,
		PRIMARY KEY (ID),
		UNIQUE KEY timer_key(NAMESPACE, TIMER_KEY),
		KEY hook_class(HOOK_CLASS)
	)`

	// CreateTiDBRunawayWatchTable stores the condition which is used to check whether query should be quarantined.
	CreateTiDBRunawayWatchTable = `CREATE TABLE IF NOT EXISTS mysql.tidb_runaway_watch (
		id BIGINT(20) NOT NULL AUTO_INCREMENT PRIMARY KEY,
		resource_group_name varchar(32) not null,
		start_time datetime(6) NOT NULL,
		end_time datetime(6),
		watch bigint(10) NOT NULL,
		watch_text TEXT NOT NULL,
		source varchar(512) NOT NULL,
		action bigint(10),
		switch_group_name VARCHAR(32) DEFAULT '',
		rule VARCHAR(512) DEFAULT '',
		INDEX sql_index(resource_group_name,watch_text(700)) COMMENT "accelerate the speed when select quarantined query",
		INDEX time_index(end_time) COMMENT "accelerate the speed when querying with active watch"
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;`

	// CreateTiDBRunawayWatchDoneTable stores the condition which is used to check whether query should be quarantined.
	CreateTiDBRunawayWatchDoneTable = `CREATE TABLE IF NOT EXISTS mysql.tidb_runaway_watch_done (
		id BIGINT(20) NOT NULL AUTO_INCREMENT PRIMARY KEY,
		record_id BIGINT(20) not null,
		resource_group_name varchar(32) not null,
		start_time datetime(6) NOT NULL,
		end_time datetime(6),
		watch bigint(10) NOT NULL,
		watch_text TEXT NOT NULL,
		source varchar(512) NOT NULL,
		action bigint(10),
		switch_group_name VARCHAR(32) DEFAULT '',
		rule VARCHAR(512) DEFAULT '',
		done_time TIMESTAMP(6) NOT NULL
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;`

	// CreateRequestUnitByGroupTable stores the historical RU consumption by resource group.
	CreateRequestUnitByGroupTable = `CREATE TABLE IF NOT EXISTS mysql.request_unit_by_group (
		start_time TIMESTAMP(6) NOT NULL,
		end_time TIMESTAMP(6) NOT NULL,
		resource_group VARCHAR(32) NOT null,
		total_ru bigint(64) UNSIGNED NOT NULL,
		PRIMARY KEY (start_time, end_time, resource_group),
		KEY (resource_group)
	);`

	// CreateTiDBImportJobsTable is a table that IMPORT INTO uses.
	CreateTiDBImportJobsTable = `CREATE TABLE IF NOT EXISTS mysql.tidb_import_jobs (
		id bigint(64) NOT NULL AUTO_INCREMENT,
		create_time TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
		start_time TIMESTAMP(6) NULL DEFAULT NULL,
		update_time TIMESTAMP(6) NULL DEFAULT NULL,
		end_time TIMESTAMP(6) NULL DEFAULT NULL,
		table_schema VARCHAR(64) NOT NULL,
		table_name VARCHAR(64) NOT NULL,
		table_id bigint(64) NOT NULL,
		created_by VARCHAR(300) NOT NULL,
		group_key VARCHAR(256) NOT NULL DEFAULT "",
		parameters text NOT NULL,
		source_file_size bigint(64) NOT NULL,
		status VARCHAR(64) NOT NULL,
		step VARCHAR(64) NOT NULL,
		summary text DEFAULT NULL,
		error_message TEXT DEFAULT NULL,
		PRIMARY KEY (id),
		KEY (created_by),
		KEY idx_group_key(group_key),
		KEY (status));`

	// CreateTiDBPITRIDMapTable is a table that records the id map from upstream to downstream for PITR.
	// set restore id default to 0 to make it compatible for old BR tool to restore to a new TiDB, such case should be
	// rare though.
	CreateTiDBPITRIDMapTable = `CREATE TABLE IF NOT EXISTS mysql.tidb_pitr_id_map (
		restore_id BIGINT NOT NULL DEFAULT 0,
		restored_ts BIGINT NOT NULL,
		upstream_cluster_id BIGINT NOT NULL,
		segment_id BIGINT NOT NULL,
		id_map BLOB(524288) NOT NULL,
		update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		PRIMARY KEY (restore_id, restored_ts, upstream_cluster_id, segment_id));`

	// CreateTiDBRestoreRegistryTable is a table that tracks active restore tasks to prevent conflicts.
	CreateTiDBRestoreRegistryTable = `CREATE TABLE IF NOT EXISTS mysql.tidb_restore_registry (
		id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
		filter_strings TEXT NOT NULL,
		filter_hash VARCHAR(64) NOT NULL,
		start_ts BIGINT UNSIGNED NOT NULL,
		restored_ts BIGINT UNSIGNED NOT NULL,
		upstream_cluster_id BIGINT UNSIGNED,
		with_sys_table BOOLEAN NOT NULL DEFAULT TRUE,
		status VARCHAR(20) NOT NULL DEFAULT 'running',
		cmd TEXT,
		task_start_time TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6),
		last_heartbeat_time TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6),
		UNIQUE KEY unique_registration_params (
			filter_hash,
			start_ts,
			restored_ts,
			upstream_cluster_id,
			with_sys_table,
			cmd(256)
		)
	) AUTO_INCREMENT = 1;`

	// DropMySQLIndexUsageTable removes the table `mysql.schema_index_usage`
	DropMySQLIndexUsageTable = "DROP TABLE IF EXISTS mysql.schema_index_usage"

	// CreateSchemaUnusedIndexesView creates a view to use `information_schema.tidb_index_usage` to get the unused indexes.
	CreateSchemaUnusedIndexesView = `CREATE OR REPLACE VIEW sys.schema_unused_indexes AS
		SELECT
			table_schema as object_schema,
			table_name as object_name,
			index_name
		FROM information_schema.cluster_tidb_index_usage
		WHERE
			table_schema not in ('sys', 'mysql', 'INFORMATION_SCHEMA', 'PERFORMANCE_SCHEMA') and
			index_name != 'PRIMARY'
		GROUP BY table_schema, table_name, index_name
		HAVING
			sum(last_access_time) is null;`

	// CreateIndexAdvisorResultsTable is a table to store the index advisor results.
	CreateIndexAdvisorResultsTable = `CREATE TABLE IF NOT EXISTS mysql.index_advisor_results (
       id bigint primary key not null auto_increment,
       created_at datetime not null,
       updated_at datetime not null,

       schema_name varchar(64) not null,
       table_name varchar(64) not null,
       index_name varchar(127) not null,
       index_columns varchar(500) not null COMMENT 'split by ",", e.g. "c1", "c1,c2", "c1,c2,c3,c4,c5"',

       index_details json,        -- est_index_size, reason, DDL to create this index, ...
       top_impacted_queries json, -- improvement, plan before and after this index, ...
       workload_impact json,      -- improvement and more details, ...
       extra json,                -- for the cloud env to save more info like RU, cost_saving, ...
       index idx_create(created_at),
       index idx_update(updated_at),
       unique index idx(schema_name, table_name, index_columns));`

	// CreateTiDBKernelOptionsTable is a table to store kernel options for tidb.
	CreateTiDBKernelOptionsTable = `CREATE TABLE IF NOT EXISTS mysql.tidb_kernel_options (
        module varchar(128),
        name varchar(128),
        value varchar(128),
        updated_at datetime,
        status varchar(128),
        description text,
        primary key(module, name));`

	// CreateTiDBWorkloadValuesTable is a table to store workload-based learning values for tidb.
	CreateTiDBWorkloadValuesTable = `CREATE TABLE IF NOT EXISTS mysql.tidb_workload_values (
		id bigint(20) NOT NULL AUTO_INCREMENT PRIMARY KEY,
		version bigint(20) NOT NULL,
		category varchar(64) NOT NULL,
		type varchar(64) NOT NULL,
		table_id bigint(20) NOT NULL,
		value json NOT NULL,
		index idx_version_category_type (version, category, type),
		index idx_table_id (table_id));`

	// CreateMaterializedViewRefreshInfoTable is a system table for materialized views to
	// track refresh progress and scheduling.
	CreateMaterializedViewRefreshInfoTable = `CREATE TABLE IF NOT EXISTS mysql.mv_refresh_info (
		mv_id BIGINT NOT NULL PRIMARY KEY,
		base_table_id BIGINT NOT NULL,
		log_table_id BIGINT NOT NULL,
		refresh_interval_seconds BIGINT NOT NULL,
		next_run_time TIMESTAMP NULL DEFAULT NULL,
		last_refresh_tso BIGINT UNSIGNED NOT NULL DEFAULT 0,
		last_refresh_type ENUM('COMPLETE','FAST') NOT NULL DEFAULT 'COMPLETE',
		last_refresh_result ENUM('SUCCESS','FAILED') NOT NULL DEFAULT 'SUCCESS',
		last_refresh_time TIMESTAMP NULL DEFAULT NULL,
		last_error TEXT DEFAULT NULL,
		INDEX idx_base_table_id (base_table_id),
		INDEX idx_next_run_time (next_run_time));`
)

// bootstrap initiates system DB for a store.
func bootstrap(s sessionapi.Session) {
	startTime := time.Now()
	err := InitMDLVariableForBootstrap(s.GetStore())
	if err != nil {
		logutil.BgLogger().Fatal("init metadata lock failed during bootstrap", zap.Error(err))
	}
	dom := domain.GetDomain(s)
	bootLogger := logutil.SampleLoggerFactory(30*time.Second, 1)()
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
			runBootstrapSQLFile = true
			logutil.BgLogger().Info("bootstrap successful",
				zap.Duration("take time", time.Since(startTime)))
			return
		}
		bootLogger.Info("bootstrap not done yet, waiting for owner to finish")
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
	// TidbNewCollationEnabled The variable name in mysql.tidb table and it will indicate if the new collations are enabled in the TiDB cluster.
	TidbNewCollationEnabled = "new_collation_enabled"
	// The variable name in mysql.tidb table and it records the default value of
	// mem-quota-query when upgrade from v3.0.x to v4.0.9+.
	tidbDefMemoryQuotaQuery = "default_memory_quota_query"
	// The variable name in mysql.tidb table and it records the default value of
	// oom-action when upgrade from v3.0.x to v4.0.11+.
	tidbDefOOMAction = "default_oom_action"
	// The variable name in mysql.tidb table and it records the current DDLTableVersion
	tidbDDLTableVersion = "ddl_table_version"
	// The variable name in mysql.tidb table and it records the cluster id of this cluster
	tidbClusterID = "cluster_id"
)

// DDL owner key's expired time is ManagerSessionTTL seconds, we should wait the time and give more time to have a chance to finish it.
var internalSQLTimeout = owner.ManagerSessionTTL + 15

// whether to run the sql file in bootstrap.
var runBootstrapSQLFile = false

// DisableRunBootstrapSQLFileInTest only used for test
func DisableRunBootstrapSQLFileInTest() {
	if intest.InTest {
		runBootstrapSQLFile = false
	}
}

func checkBootstrapped(s sessionapi.Session) (bool, error) {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnBootstrap)
	//  Check if system db exists.
	_, err := s.ExecuteInternal(ctx, "USE %n", mysql.SystemDB)
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
		if err = s.CommitTxn(ctx); err != nil {
			return false, errors.Trace(err)
		}
	}
	return isBootstrapped, nil
}

// getTiDBVar gets variable value from mysql.tidb table.
// Those variables are used by TiDB server.
func getTiDBVar(s sessionapi.Session, name string) (sVal string, isNull bool, e error) {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnBootstrap)
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

var (
	// SupportUpgradeHTTPOpVer is exported for testing.
	// The minimum version of the upgrade by paused user DDL can be notified through the HTTP API.
	SupportUpgradeHTTPOpVer int64 = version174
)

func acquireLock(store kv.Storage) (func(), error) {
	etcdCli, err := storepkg.NewEtcdCli(store)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if etcdCli == nil {
		// Special handling for test.
		logutil.BgLogger().Warn("skip acquire ddl owner lock for uni-store")
		return func() {
			// do nothing
		}, nil
	}
	releaseFn, err := owner.AcquireDistributedLock(context.Background(), etcdCli, bootstrapOwnerKey, 10)
	if err != nil {
		if err2 := etcdCli.Close(); err2 != nil {
			logutil.BgLogger().Error("failed to close etcd client", zap.Error(err2))
		}
		return nil, errors.Trace(err)
	}
	return func() {
		releaseFn()
		if err2 := etcdCli.Close(); err2 != nil {
			logutil.BgLogger().Error("failed to close etcd client", zap.Error(err2))
		}
	}, nil
}

// upgrade function  will do some upgrade works, when the system is bootstrapped by low version TiDB server
// For example, add new system variables into mysql.global_variables table.
func upgrade(s sessionapi.Session) {
	// Do upgrade works then update bootstrap version.
	isNull, err := InitMDLVariableForUpgrade(s.GetStore())
	if err != nil {
		logutil.BgLogger().Fatal("init metadata lock failed during upgrade", zap.Error(err))
	}

	var ver int64
	ver, err = getBootstrapVersion(s)
	terror.MustNil(err)
	if ver >= currentBootstrapVersion {
		// It is already bootstrapped/upgraded by a higher version TiDB server.
		return
	}

	printClusterState(s, ver)

	// when upgrade from v6.4.0 or earlier, enables metadata lock automatically,
	// but during upgrade we disable it.
	if isNull {
		upgradeToVer99Before(s)
	}

	// It is only used in test.
	upgradeFns := addMockBootstrapVersionForTest(s)
	for _, verFn := range upgradeFns {
		if ver < verFn.version {
			verFn.fn(s, ver)
			logutil.BgLogger().Info("upgrade to bootstrap version.",
				zap.Int64("old-start-version", ver),
				zap.Int64("in-progress-version", verFn.version),
				zap.Int64("target-version", currentBootstrapVersion))
		}
	}
	if isNull {
		upgradeToVer99After(s)
	}

	updateBootstrapVer(s)
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnBootstrap)
	_, err = s.ExecuteInternal(ctx, "COMMIT")

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
		logutil.BgLogger().Fatal("[upgrade] upgrade failed",
			zap.Int64("from", ver),
			zap.Int64("to", currentBootstrapVersion),
			zap.Error(err))
	}
}

// initGlobalVariableIfNotExists initialize a global variable with specific val if it does not exist.
func initGlobalVariableIfNotExists(s sessionapi.Session, name string, val any) {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnBootstrap)
	rows, err := sqlexec.ExecSQL(ctx, s, "SELECT VARIABLE_VALUE FROM %n.%n WHERE VARIABLE_NAME=%?;", mysql.SystemDB, mysql.GlobalVariablesTable, name)
	terror.MustNil(err)
	if len(rows) != 0 {
		return
	}

	mustExecute(s, "INSERT HIGH_PRIORITY IGNORE INTO %n.%n VALUES (%?, %?);",
		mysql.SystemDB, mysql.GlobalVariablesTable, name, val)
}

func writeOOMAction(s sessionapi.Session) {
	comment := "oom-action is `log` by default in v3.0.x, `cancel` by default in v4.0.11+"
	mustExecute(s, `INSERT HIGH_PRIORITY INTO %n.%n VALUES (%?, %?, %?) ON DUPLICATE KEY UPDATE VARIABLE_VALUE= %?`,
		mysql.SystemDB, mysql.TiDBTable, tidbDefOOMAction, vardef.OOMActionLog, comment, vardef.OOMActionLog,
	)
}

// updateBootstrapVer updates bootstrap version variable in mysql.TiDB table.
func updateBootstrapVer(s sessionapi.Session) {
	// Update bootstrap version.
	mustExecute(s, `INSERT HIGH_PRIORITY INTO %n.%n VALUES (%?, %?, "TiDB bootstrap version.") ON DUPLICATE KEY UPDATE VARIABLE_VALUE=%?`,
		mysql.SystemDB, mysql.TiDBTable, tidbServerVersionVar, currentBootstrapVersion, currentBootstrapVersion,
	)
}

// getBootstrapVersion gets bootstrap version from mysql.tidb table;
func getBootstrapVersion(s sessionapi.Session) (int64, error) {
	sVal, isNull, err := getTiDBVar(s, tidbServerVersionVar)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if isNull {
		return 0, nil
	}
	return strconv.ParseInt(sVal, 10, 64)
}

var systemDatabases = []DatabaseBasicInfo{
	{ID: metadef.SystemDatabaseID, Name: mysql.SystemDB},
	{ID: metadef.SysDatabaseID, Name: mysql.SysDB},
}

// tablesInSystemDatabase contains the definitions of system tables in the mysql
// database, or the system database, except DDL related tables, see ddlTableVersionTables.
// TODO: the reserved ID will be used later.
var tablesInSystemDatabase = []TableBasicInfo{
	{ID: metadef.UserTableID, Name: "user", SQL: CreateUserTable},
	{ID: metadef.PasswordHistoryTableID, Name: "password_history", SQL: CreatePasswordHistoryTable},
	{ID: metadef.GlobalPrivTableID, Name: "global_priv", SQL: CreateGlobalPrivTable},
	{ID: metadef.DBTableID, Name: "db", SQL: CreateDBTable},
	{ID: metadef.TablesPrivTableID, Name: "tables_priv", SQL: CreateTablesPrivTable},
	{ID: metadef.ColumnsPrivTableID, Name: "columns_priv", SQL: CreateColumnsPrivTable},
	{ID: metadef.GlobalVariablesTableID, Name: "global_variables", SQL: CreateGlobalVariablesTable},
	{ID: metadef.TiDBTableID, Name: "tidb", SQL: CreateTiDBTable},
	{ID: metadef.HelpTopicTableID, Name: "help_topic", SQL: CreateHelpTopicTable},
	{ID: metadef.StatsMetaTableID, Name: "stats_meta", SQL: CreateStatsMetaTable},
	{ID: metadef.StatsHistogramsTableID, Name: "stats_histograms", SQL: CreateStatsHistogramsTable},
	{ID: metadef.StatsBucketsTableID, Name: "stats_buckets", SQL: CreateStatsBucketsTable},
	{ID: metadef.GCDeleteRangeTableID, Name: "gc_delete_range", SQL: CreateGCDeleteRangeTable},
	{ID: metadef.GCDeleteRangeDoneTableID, Name: "gc_delete_range_done", SQL: CreateGCDeleteRangeDoneTable},
	{ID: metadef.StatsFeedbackTableID, Name: "stats_feedback", SQL: CreateStatsFeedbackTable},
	{ID: metadef.RoleEdgesTableID, Name: "role_edges", SQL: CreateRoleEdgesTable},
	{ID: metadef.DefaultRolesTableID, Name: "default_roles", SQL: CreateDefaultRolesTable},
	{ID: metadef.BindInfoTableID, Name: "bind_info", SQL: CreateBindInfoTable},
	{ID: metadef.StatsTopNTableID, Name: "stats_top_n", SQL: CreateStatsTopNTable},
	{ID: metadef.ExprPushdownBlacklistTableID, Name: "expr_pushdown_blacklist", SQL: CreateExprPushdownBlacklistTable},
	{ID: metadef.OptRuleBlacklistTableID, Name: "opt_rule_blacklist", SQL: CreateOptRuleBlacklistTable},
	{ID: metadef.StatsExtendedTableID, Name: "stats_extended", SQL: CreateStatsExtendedTable},
	{ID: metadef.StatsFMSketchTableID, Name: "stats_fm_sketch", SQL: CreateStatsFMSketchTable},
	{ID: metadef.GlobalGrantsTableID, Name: "global_grants", SQL: CreateGlobalGrantsTable},
	{ID: metadef.CapturePlanBaselinesBlacklistTableID, Name: "capture_plan_baselines_blacklist", SQL: CreateCapturePlanBaselinesBlacklistTable},
	{ID: metadef.ColumnStatsUsageTableID, Name: "column_stats_usage", SQL: CreateColumnStatsUsageTable},
	{ID: metadef.TableCacheMetaTableID, Name: "table_cache_meta", SQL: CreateTableCacheMetaTable},
	{ID: metadef.AnalyzeOptionsTableID, Name: "analyze_options", SQL: CreateAnalyzeOptionsTable},
	{ID: metadef.StatsHistoryTableID, Name: "stats_history", SQL: CreateStatsHistoryTable},
	{ID: metadef.StatsMetaHistoryTableID, Name: "stats_meta_history", SQL: CreateStatsMetaHistoryTable},
	{ID: metadef.AnalyzeJobsTableID, Name: "analyze_jobs", SQL: CreateAnalyzeJobsTable},
	{ID: metadef.AdvisoryLocksTableID, Name: "advisory_locks", SQL: CreateAdvisoryLocksTable},
	{ID: metadef.PlanReplayerStatusTableID, Name: "plan_replayer_status", SQL: CreatePlanReplayerStatusTable},
	{ID: metadef.PlanReplayerTaskTableID, Name: "plan_replayer_task", SQL: CreatePlanReplayerTaskTable},
	{ID: metadef.StatsTableLockedTableID, Name: "stats_table_locked", SQL: CreateStatsTableLockedTable},
	{ID: metadef.TiDBTTLTableStatusTableID, Name: "tidb_ttl_table_status", SQL: CreateTiDBTTLTableStatusTable},
	{ID: metadef.TiDBTTLTaskTableID, Name: "tidb_ttl_task", SQL: CreateTiDBTTLTaskTable},
	{ID: metadef.TiDBTTLJobHistoryTableID, Name: "tidb_ttl_job_history", SQL: CreateTiDBTTLJobHistoryTable},
	{ID: metadef.TiDBGlobalTaskTableID, Name: "tidb_global_task", SQL: CreateTiDBGlobalTaskTable},
	{ID: metadef.TiDBGlobalTaskHistoryTableID, Name: "tidb_global_task_history", SQL: CreateTiDBGlobalTaskHistoryTable},
	{ID: metadef.TiDBImportJobsTableID, Name: "tidb_import_jobs", SQL: CreateTiDBImportJobsTable},
	{ID: metadef.TiDBRunawayWatchTableID, Name: "tidb_runaway_watch", SQL: CreateTiDBRunawayWatchTable},
	{ID: metadef.TiDBRunawayQueriesTableID, Name: "tidb_runaway_queries", SQL: CreateTiDBRunawayQueriesTable},
	{ID: metadef.TiDBTimersTableID, Name: "tidb_timers", SQL: CreateTiDBTimersTable},
	{ID: metadef.TiDBRunawayWatchDoneTableID, Name: "tidb_runaway_watch_done", SQL: CreateTiDBRunawayWatchDoneTable},
	{ID: metadef.DistFrameworkMetaTableID, Name: "dist_framework_meta", SQL: CreateDistFrameworkMetaTable},
	{ID: metadef.RequestUnitByGroupTableID, Name: "request_unit_by_group", SQL: CreateRequestUnitByGroupTable},
	{ID: metadef.TiDBPITRIDMapTableID, Name: "tidb_pitr_id_map", SQL: CreateTiDBPITRIDMapTable},
	{ID: metadef.TiDBRestoreRegistryTableID, Name: "tidb_restore_registry", SQL: CreateTiDBRestoreRegistryTable},
	{ID: metadef.IndexAdvisorResultsTableID, Name: "index_advisor_results", SQL: CreateIndexAdvisorResultsTable},
	{ID: metadef.TiDBKernelOptionsTableID, Name: "tidb_kernel_options", SQL: CreateTiDBKernelOptionsTable},
	{ID: metadef.TiDBWorkloadValuesTableID, Name: "tidb_workload_values", SQL: CreateTiDBWorkloadValuesTable},
	// NOTE: if you need to add more tables to 'mysql' database, please also add
	// an entry to versionedBootstrapSchemas, to make sure the table is created
	// correctly in nextgen kennel.
}

type versionedBootstrapSchema struct {
	ver       meta.NextGenBootTableVersion
	databases []DatabaseBasicInfo
}

const (
	// 52 is the number of system tables as we do this change.
	// as tablesInSystemDatabase is shared with classic kernel, it's simple to
	// use a slice to hold all system tables in classic kernel. but in nextgen,
	// we need to make those tables versioned, as we don't create system tables
	// through DDL, we need this version to avoid create tables again.
	// if we add more system tables later, we should increase the version, and
	// add another versionedBootstrapSchema entry.
	tableCountInFirstVerOnNextGen = 52
)

// used in nextgen, to create system tables directly through meta kv, without
// going through DDL, so we can create them with reversed ID range.
var versionedBootstrapSchemas = []versionedBootstrapSchema{
	{ver: meta.BaseNextGenBootTableVersion, databases: []DatabaseBasicInfo{
		{ID: metadef.SystemDatabaseID, Name: mysql.SystemDB, Tables: tablesInSystemDatabase[:tableCountInFirstVerOnNextGen]},
		{ID: metadef.SysDatabaseID, Name: mysql.SysDB},
	}},
}

func bootstrapSchemas(store kv.Storage) error {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	return kv.RunInNewTxn(ctx, store, true, func(_ context.Context, txn kv.Transaction) error {
		m := meta.NewMutator(txn)
		currVer, err := m.GetNextGenBootTableVersion()
		if err != nil {
			return errors.Trace(err)
		}

		largestVer := currVer
		for _, vt := range versionedBootstrapSchemas {
			if currVer >= vt.ver {
				continue
			}
			logutil.BgLogger().Info("bootstrap tables", zap.Int("currVer", int(currVer)),
				zap.Int("targetVer", int(vt.ver)))
			for _, bdb := range vt.databases {
				if err = m.CreateSysDatabaseByIDIfNotExists(bdb.Name, bdb.ID); err != nil {
					return err
				}
				if len(bdb.Tables) > 0 {
					if err = createAndSplitTables(store, m, bdb.ID, bdb.Tables); err != nil {
						return err
					}
				}
			}
			largestVer = max(largestVer, vt.ver)
		}
		if largestVer > currVer {
			return m.SetNextGenBootTableVersion(largestVer)
		}
		return nil
	})
}

// doDDLWorks executes DDL statements in bootstrap stage.
func doDDLWorks(s sessionapi.Session) {
	// for nextgen, system schemas are created in bootstrapSessionImpl
	if kerneltype.IsClassic() {
		for _, db := range systemDatabases {
			mustExecute(s, "CREATE DATABASE IF NOT EXISTS %n", db.Name)
		}
		for _, tbl := range tablesInSystemDatabase {
			mustExecute(s, tbl.SQL)
		}
	}
	// Materialized view system tables are created by DDL for both classic and nextgen to avoid
	// depending on nextgen reserved IDs/versioned bootstrap tables.
	mustExecute(s, CreateMaterializedViewRefreshInfoTable)
	// Create bind_info table.
	insertBuiltinBindInfoRow(s)
	// Create `mysql.tidb_mdl_view` view.
	mustExecute(s, CreateTiDBMDLView)
	// create `sys.schema_unused_indexes` view
	mustExecute(s, CreateSchemaUnusedIndexesView)
	// Create a test database.
	mustExecute(s, "CREATE DATABASE IF NOT EXISTS test")
}

// doBootstrapSQLFile executes SQL commands in a file as the last stage of bootstrap.
// It is useful for setting the initial value of GLOBAL variables.
func doBootstrapSQLFile(s sessionapi.Session) error {
	sqlFile := config.GetGlobalConfig().InitializeSQLFile
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnBootstrap)
	if sqlFile == "" {
		return nil
	}
	logutil.BgLogger().Info("executing -initialize-sql-file", zap.String("file", sqlFile))
	b, err := os.ReadFile(sqlFile) //nolint:gosec
	if err != nil {
		if intest.InTest {
			return err
		}
		logutil.BgLogger().Fatal("unable to read InitializeSQLFile", zap.Error(err))
	}
	stmts, err := s.Parse(ctx, string(b))
	if err != nil {
		if intest.InTest {
			return err
		}
		logutil.BgLogger().Fatal("unable to parse InitializeSQLFile", zap.Error(err))
	}
	for _, stmt := range stmts {
		rs, err := s.ExecuteStmt(ctx, stmt)
		if err != nil {
			logutil.BgLogger().Warn("InitializeSQLFile error", zap.Error(err))
		}
		if rs != nil {
			// I don't believe we need to drain the result-set in bootstrap mode
			// but if required we can do this here in future.
			if err := rs.Close(); err != nil {
				logutil.BgLogger().Fatal("unable to close result", zap.Error(err))
			}
		}
	}
	return nil
}

// doDMLWorks executes DML statements in bootstrap stage.
// All the statements run in a single transaction.
func doDMLWorks(s sessionapi.Session) {
	mustExecute(s, "BEGIN")
	if config.GetGlobalConfig().Security.SecureBootstrap {
		// If secure bootstrap is enabled, we create a root@localhost account which can login with auth_socket.
		// i.e. mysql -S /tmp/tidb.sock -uroot
		// The auth_socket plugin will validate that the user matches $USER.
		u, err := osuser.Current()
		if err != nil {
			logutil.BgLogger().Fatal("failed to read current user. unable to secure bootstrap.", zap.Error(err))
		}
		mustExecute(s, `INSERT HIGH_PRIORITY INTO mysql.user (Host,User,authentication_string,plugin,Select_priv,Insert_priv,Update_priv,Delete_priv,Create_priv,Drop_priv,Process_priv,Grant_priv,References_priv,Alter_priv,Show_db_priv,
			Super_priv,Create_tmp_table_priv,Lock_tables_priv,Execute_priv,Create_view_priv,Show_view_priv,Create_routine_priv,Alter_routine_priv,Index_priv,Create_user_priv,Event_priv,Repl_slave_priv,Repl_client_priv,Trigger_priv,Create_role_priv,Drop_role_priv,Account_locked,
		    Shutdown_priv,Reload_priv,FILE_priv,Config_priv,Create_Tablespace_Priv,User_attributes,Token_issuer) VALUES
		("localhost", "root", %?, "auth_socket", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "N", "Y", "Y", "Y", "Y", "Y", null, "")`, u.Username)
	} else {
		mustExecute(s, `INSERT HIGH_PRIORITY INTO mysql.user (Host,User,authentication_string,plugin,Select_priv,Insert_priv,Update_priv,Delete_priv,Create_priv,Drop_priv,Process_priv,Grant_priv,References_priv,Alter_priv,Show_db_priv,
			Super_priv,Create_tmp_table_priv,Lock_tables_priv,Execute_priv,Create_view_priv,Show_view_priv,Create_routine_priv,Alter_routine_priv,Index_priv,Create_user_priv,Event_priv,Repl_slave_priv,Repl_client_priv,Trigger_priv,Create_role_priv,Drop_role_priv,Account_locked,
		    Shutdown_priv,Reload_priv,FILE_priv,Config_priv,Create_Tablespace_Priv,User_attributes,Token_issuer) VALUES
		("%", "root", "", "mysql_native_password", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "N", "Y", "Y", "Y", "Y", "Y", null, "")`)
	}

	// For GLOBAL scoped system variables, insert the initial value
	// into the mysql.global_variables table. This is only run on initial
	// bootstrap, and in some cases we will use a different default value
	// for new installs versus existing installs.

	values := make([]string, 0, len(variable.GetSysVars()))
	for k, v := range variable.GetSysVars() {
		if !v.HasGlobalScope() {
			continue
		}
		vVal := variable.GlobalSystemVariableInitialValue(v.Name, v.Value)

		// sanitize k and vVal
		value := fmt.Sprintf(`("%s", "%s")`, sqlescape.EscapeString(k), sqlescape.EscapeString(vVal))
		values = append(values, value)
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

	writeStmtSummaryVars(s)

	writeDDLTableVersion(s)

	writeClusterID(s)

	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnBootstrap)
	_, err := s.ExecuteInternal(ctx, "COMMIT")
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

func mustExecute(s sessionapi.Session, sql string, args ...any) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(internalSQLTimeout)*time.Second)
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnBootstrap)
	_, err := s.ExecuteInternal(ctx, sql, args...)
	defer cancel()
	if err != nil {
		logutil.BgLogger().Fatal("mustExecute error", zap.Error(err), zap.Stack("stack"))
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
func rebuildAllPartitionValueMapAndSorted(ctx context.Context, s *session) {
	type partitionExpr interface {
		PartitionExpr() *tables.PartitionExpr
	}

	p := parser.New()
	is := s.GetInfoSchema().(infoschema.InfoSchema)
	dbs := is.ListTablesWithSpecialAttribute(infoschemacontext.PartitionAttribute)
	for _, db := range dbs {
		for _, t := range db.TableInfos {
			pi := t.GetPartitionInfo()
			if pi == nil || pi.Type != ast.PartitionTypeList {
				continue
			}
			tbl, ok := is.TableByID(ctx, t.ID)
			intest.Assert(ok, "table not found in infoschema")
			pe := tbl.(partitionExpr).PartitionExpr()
			for _, cp := range pe.ColPrunes {
				if err := cp.RebuildPartitionValueMapAndSorted(p, pi.Definitions); err != nil {
					logutil.BgLogger().Warn("build list column partition value map and sorted failed")
					break
				}
			}
		}
	}
}
