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
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/bindinfo"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	infoschemacontext "github.com/pingcap/tidb/pkg/infoschema/context"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/owner"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	sessiontypes "github.com/pingcap/tidb/pkg/session/types"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	storepkg "github.com/pingcap/tidb/pkg/store"
	"github.com/pingcap/tidb/pkg/table/tables"
	timertable "github.com/pingcap/tidb/pkg/timer/tablestore"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	utilparser "github.com/pingcap/tidb/pkg/util/parser"
	"github.com/pingcap/tidb/pkg/util/sqlescape"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/pingcap/tidb/pkg/util/timeutil"
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
	CreateGlobalPrivTable = "CREATE TABLE IF NOT EXISTS mysql.global_priv (" +
		"Host CHAR(255) NOT NULL DEFAULT ''," +
		"User CHAR(80) NOT NULL DEFAULT ''," +
		"Priv LONGTEXT NOT NULL DEFAULT ''," +
		"PRIMARY KEY (Host, User)," +
		"KEY i_user (User))"

	// For `mysql.db`, `mysql.tables_priv` and `mysql.columns_priv` table, we have a slight different
	// schema definition with MySQL: columns `DB`/`Table_name`/`Column_name` are defined with case-insensitive
	// collation(in MySQL, they are case-sensitive).

	// The reason behind this is that when writing those records, MySQL always converts those names into lower case
	// while TiDB does not do so in early implementations, which makes some 'GRANT'/'REVOKE' operations case-sensitive.

	// In order to fix this, we decide to explicitly set case-insensitive collation for the related columns here, to
	// make sure:
	// * The 'GRANT'/'REVOKE' could be case-insensitive for new clusters(compatible with MySQL).
	// * Keep all behaviors unchanged for upgraded cluster.

	// CreateDBPrivTable is the SQL statement creates DB scope privilege table in system db.
	CreateDBPrivTable = `CREATE TABLE IF NOT EXISTS mysql.db (
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
	// CreateTablePrivTable is the SQL statement creates table scope privilege table in system db.
	CreateTablePrivTable = `CREATE TABLE IF NOT EXISTS mysql.tables_priv (
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
	// CreateColumnPrivTable is the SQL statement creates column scope privilege table in system db.
	CreateColumnPrivTable = `CREATE TABLE IF NOT EXISTS mysql.columns_priv(
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
	CreateGlobalVariablesTable = `CREATE TABLE IF NOT EXISTS mysql.GLOBAL_VARIABLES(
		VARIABLE_NAME  VARCHAR(64) NOT NULL PRIMARY KEY,
		VARIABLE_VALUE VARCHAR(16383) DEFAULT NULL);`
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
		version 					BIGINT(64) UNSIGNED NOT NULL,
		table_id 					BIGINT(64) NOT NULL,
		modify_count				BIGINT(64) NOT NULL DEFAULT 0,
		count 						BIGINT(64) UNSIGNED NOT NULL DEFAULT 0,
		snapshot        			BIGINT(64) UNSIGNED NOT NULL DEFAULT 0,
		last_stats_histograms_version 	BIGINT(64) UNSIGNED DEFAULT NULL,
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
		create_time TIMESTAMP(3) NOT NULL,
		update_time TIMESTAMP(3) NOT NULL,
		charset TEXT NOT NULL,
		collation TEXT NOT NULL,
		source VARCHAR(10) NOT NULL DEFAULT 'unknown',
		sql_digest varchar(64) DEFAULT NULL,
		plan_digest varchar(64) DEFAULT NULL,
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
		PRIMARY KEY (USER,HOST,PRIV),
		KEY i_user (USER)
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
		KEY table_create_time (table_id, create_time, seq_no),
    	KEY idx_create_time (create_time)
	);`
	// CreateStatsMetaHistory stores the historical meta stats.
	CreateStatsMetaHistory = `CREATE TABLE IF NOT EXISTS mysql.stats_meta_history (
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
		KEY (update_time),
		INDEX idx_schema_table_state (table_schema, table_name, state),
		INDEX idx_schema_table_partition_state (table_schema, table_name, partition_name, state)
	);`
	// CreateAdvisoryLocks stores the advisory locks (get_lock, release_lock).
	CreateAdvisoryLocks = `CREATE TABLE IF NOT EXISTS mysql.advisory_locks (
		lock_name VARCHAR(64) NOT NULL PRIMARY KEY
	);`
	// CreateMDLView is a view about metadata locks.
	CreateMDLView = `CREATE OR REPLACE SQL SECURITY INVOKER VIEW mysql.tidb_mdl_view as (
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

	// CreateStatsTableLocked stores the locked tables
	CreateStatsTableLocked = `CREATE TABLE IF NOT EXISTS mysql.stats_table_locked(
		table_id bigint(64) NOT NULL,
		modify_count bigint(64) NOT NULL DEFAULT 0,
		count bigint(64) NOT NULL DEFAULT 0,
		version bigint(64) UNSIGNED NOT NULL DEFAULT 0,
		PRIMARY KEY (table_id));`

	// CreatePasswordHistory is a table save history passwd.
	CreatePasswordHistory = `CREATE TABLE  IF NOT EXISTS mysql.password_history (
         Host char(255)  NOT NULL DEFAULT '',
         User char(32)  NOT NULL DEFAULT '',
         Password_timestamp timestamp(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
         Password text,
         PRIMARY KEY (Host,User,Password_timestamp )
        ) COMMENT='Password history for user accounts' `

	// CreateTTLTableStatus is a table about TTL job schedule
	CreateTTLTableStatus = `CREATE TABLE IF NOT EXISTS mysql.tidb_ttl_table_status (
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

	// CreateTTLTask is a table about parallel ttl tasks
	CreateTTLTask = `CREATE TABLE IF NOT EXISTS mysql.tidb_ttl_task (
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

	// CreateTTLJobHistory is a table that stores ttl job's history
	CreateTTLJobHistory = `CREATE TABLE IF NOT EXISTS mysql.tidb_ttl_job_history (
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

	// CreateGlobalTask is a table about global task.
	CreateGlobalTask = `CREATE TABLE IF NOT EXISTS mysql.tidb_global_task (
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
		key(state),
		UNIQUE KEY task_key(task_key)
	);`

	// CreateGlobalTaskHistory is a table about history global task.
	CreateGlobalTaskHistory = `CREATE TABLE IF NOT EXISTS mysql.tidb_global_task_history (
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
		key(state),
		UNIQUE KEY task_key(task_key)
	);`

	// CreateDistFrameworkMeta create a system table that distributed task framework use to store meta information
	CreateDistFrameworkMeta = `CREATE TABLE IF NOT EXISTS mysql.dist_framework_meta (
        host VARCHAR(261) NOT NULL PRIMARY KEY,
        role VARCHAR(64),
        cpu_count int default 0,
        keyspace_id bigint(8) NOT NULL DEFAULT -1
    );`

	// CreateRunawayTable stores the query which is identified as runaway or quarantined because of in watch list.
	CreateRunawayTable = `CREATE TABLE IF NOT EXISTS mysql.tidb_runaway_queries (
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

	// CreateRunawayWatchTable stores the condition which is used to check whether query should be quarantined.
	CreateRunawayWatchTable = `CREATE TABLE IF NOT EXISTS mysql.tidb_runaway_watch (
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

	// CreateDoneRunawayWatchTable stores the condition which is used to check whether query should be quarantined.
	CreateDoneRunawayWatchTable = `CREATE TABLE IF NOT EXISTS mysql.tidb_runaway_watch_done (
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

	// CreateImportJobs is a table that IMPORT INTO uses.
	CreateImportJobs = `CREATE TABLE IF NOT EXISTS mysql.tidb_import_jobs (
		id bigint(64) NOT NULL AUTO_INCREMENT,
		create_time TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
		start_time TIMESTAMP(6) NULL DEFAULT NULL,
		update_time TIMESTAMP(6) NULL DEFAULT NULL,
		end_time TIMESTAMP(6) NULL DEFAULT NULL,
		table_schema VARCHAR(64) NOT NULL,
		table_name VARCHAR(64) NOT NULL,
		table_id bigint(64) NOT NULL,
		created_by VARCHAR(300) NOT NULL,
		parameters text NOT NULL,
		source_file_size bigint(64) NOT NULL,
		status VARCHAR(64) NOT NULL,
		step VARCHAR(64) NOT NULL,
		summary text DEFAULT NULL,
		error_message TEXT DEFAULT NULL,
		PRIMARY KEY (id),
		KEY (created_by),
		KEY (status));`

	// CreatePITRIDMap is a table that records the id map from upstream to downstream for PITR.
	// set restore id default to 0 to make it compatible for old BR tool to restore to a new TiDB, such case should be
	// rare though.
	CreatePITRIDMap = `CREATE TABLE IF NOT EXISTS mysql.tidb_pitr_id_map (
		restore_id BIGINT NOT NULL DEFAULT 0,
		restored_ts BIGINT NOT NULL,
		upstream_cluster_id BIGINT NOT NULL,
		segment_id BIGINT NOT NULL,
		id_map BLOB(524288) NOT NULL,
		update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		PRIMARY KEY (restore_id, restored_ts, upstream_cluster_id, segment_id));`

	// CreateRestoreRegistryTable is a table that tracks active restore tasks to prevent conflicts.
	CreateRestoreRegistryTable = `CREATE TABLE IF NOT EXISTS mysql.tidb_restore_registry (
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

	// CreateSysSchema creates a new schema called `sys`.
	CreateSysSchema = `CREATE DATABASE IF NOT EXISTS sys;`

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

	// CreateIndexAdvisorTable is a table to store the index advisor results.
	CreateIndexAdvisorTable = `CREATE TABLE IF NOT EXISTS mysql.index_advisor_results (
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

	// CreateKernelOptionsTable is a table to store kernel options for tidb.
	CreateKernelOptionsTable = `CREATE TABLE IF NOT EXISTS mysql.tidb_kernel_options (
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
)

// CreateTimers is a table to store all timers for tidb
var CreateTimers = timertable.CreateTimerTableSQL("mysql", "tidb_timers")

// bootstrap initiates system DB for a store.
func bootstrap(s sessiontypes.Session) {
	startTime := time.Now()
	err := InitMDLVariableForBootstrap(s.GetStore())
	if err != nil {
		logutil.BgLogger().Fatal("init metadata lock error",
			zap.Error(err))
	}
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
			runBootstrapSQLFile = true
			logutil.BgLogger().Info("bootstrap successful",
				zap.Duration("take time", time.Since(startTime)))
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func getFunctionName(f func(sessiontypes.Session, int64)) (string, error) {
	if f == nil {
		return "", errors.New("function is nil")
	}

	funcPtr := reflect.ValueOf(f).Pointer()
	if funcPtr == 0 {
		return "", errors.New("invalid function pointer")
	}

	fullName := runtime.FuncForPC(funcPtr).Name()
	if fullName == "" {
		return "", errors.New("unable to retrieve function name")
	}

	parts := strings.Split(fullName, ".")
	if len(parts) == 0 {
		return "", errors.New("invalid function name structure")
	}

	return parts[len(parts)-1], nil
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
)

// currentBootstrapVersion is defined as a variable, so we can modify its value for testing.
// please make sure this is the largest version
var currentBootstrapVersion int64 = version249

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

var (
	bootstrapVersion = []func(sessiontypes.Session, int64){
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
		upgradeToVer93,
		upgradeToVer94,
		upgradeToVer95,
		// We will redo upgradeToVer96 in upgradeToVer100, it is skipped here.
		upgradeToVer97,
		upgradeToVer98,
		upgradeToVer100,
		upgradeToVer101,
		upgradeToVer102,
		upgradeToVer103,
		upgradeToVer104,
		upgradeToVer105,
		upgradeToVer106,
		upgradeToVer107,
		upgradeToVer108,
		upgradeToVer109,
		upgradeToVer110,
		upgradeToVer130,
		upgradeToVer131,
		upgradeToVer132,
		upgradeToVer133,
		upgradeToVer134,
		upgradeToVer135,
		upgradeToVer136,
		upgradeToVer137,
		upgradeToVer138,
		upgradeToVer139,
		upgradeToVer140,
		upgradeToVer141,
		upgradeToVer142,
		upgradeToVer143,
		upgradeToVer144,
		// We will only use Ver145 to differentiate versions, so it is skipped here.
		upgradeToVer146,
		upgradeToVer167,
		upgradeToVer168,
		upgradeToVer169,
		upgradeToVer170,
		upgradeToVer171,
		upgradeToVer172,
		upgradeToVer173,
		upgradeToVer174,
		upgradeToVer175,
		upgradeToVer176,
		upgradeToVer177,
		upgradeToVer178,
		upgradeToVer179,
		upgradeToVer190,
		upgradeToVer191,
		upgradeToVer192,
		upgradeToVer193,
		upgradeToVer194,
		upgradeToVer195,
		upgradeToVer196,
		upgradeToVer197,
		upgradeToVer198,
		upgradeToVer209,
		upgradeToVer210,
		upgradeToVer211,
		upgradeToVer212,
		upgradeToVer213,
		upgradeToVer214,
		upgradeToVer215,
		upgradeToVer216,
		upgradeToVer217,
		upgradeToVer218,
		upgradeToVer239,
		upgradeToVer240,
		upgradeToVer241,
		upgradeToVer242,
		upgradeToVer243,
		upgradeToVer244,
		upgradeToVer245,
		upgradeToVer246,
		upgradeToVer247,
		upgradeToVer248,
		upgradeToVer249,
	}
)

func checkBootstrapped(s sessiontypes.Session) (bool, error) {
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
func getTiDBVar(s sessiontypes.Session, name string) (sVal string, isNull bool, e error) {
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
func upgrade(s sessiontypes.Session) {
	// Do upgrade works then update bootstrap version.
	isNull, err := InitMDLVariableForUpgrade(s.GetStore())
	if err != nil {
		logutil.BgLogger().Fatal("[upgrade] init metadata lock failed", zap.Error(err))
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
	addMockBootstrapVersionForTest(s)
	for _, upgrade := range bootstrapVersion {
		funcName, err := getFunctionName(upgrade)
		terror.MustNil(err)
		upgrade(s, ver)
		logutil.BgLogger().Info("upgrade in progress, a version has just been completed or be skipped.",
			zap.Int64("old-start-version", ver),
			zap.String("in-progress-version", funcName),
			zap.Int64("latest-version", currentBootstrapVersion))
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

// upgradeToVer2 updates to version 2.
func upgradeToVer2(s sessiontypes.Session, ver int64) {
	if ver >= version2 {
		return
	}
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
func upgradeToVer3(s sessiontypes.Session, ver int64) {
	if ver >= version3 {
		return
	}
	// Version 3 fix tx_read_only variable value.
	mustExecute(s, "UPDATE HIGH_PRIORITY %n.%n SET variable_value = '0' WHERE variable_name = 'tx_read_only';", mysql.SystemDB, mysql.GlobalVariablesTable)
}

// upgradeToVer4 updates to version 4.
func upgradeToVer4(s sessiontypes.Session, ver int64) {
	if ver >= version4 {
		return
	}
	mustExecute(s, CreateStatsMetaTable)
}

func upgradeToVer5(s sessiontypes.Session, ver int64) {
	if ver >= version5 {
		return
	}
	mustExecute(s, CreateStatsColsTable)
	mustExecute(s, CreateStatsBucketsTable)
}

func upgradeToVer6(s sessiontypes.Session, ver int64) {
	if ver >= version6 {
		return
	}
	doReentrantDDL(s, "ALTER TABLE mysql.user ADD COLUMN `Super_priv` ENUM('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Show_db_priv`", infoschema.ErrColumnExists)
	// For reasons of compatibility, set the non-exists privilege column value to 'Y', as TiDB doesn't check them in older versions.
	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.user SET Super_priv='Y'")
}

func upgradeToVer7(s sessiontypes.Session, ver int64) {
	if ver >= version7 {
		return
	}
	doReentrantDDL(s, "ALTER TABLE mysql.user ADD COLUMN `Process_priv` ENUM('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Drop_priv`", infoschema.ErrColumnExists)
	// For reasons of compatibility, set the non-exists privilege column value to 'Y', as TiDB doesn't check them in older versions.
	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.user SET Process_priv='Y'")
}

func upgradeToVer8(s sessiontypes.Session, ver int64) {
	if ver >= version8 {
		return
	}
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnBootstrap)
	// This is a dummy upgrade, it checks whether upgradeToVer7 success, if not, do it again.
	if _, err := s.ExecuteInternal(ctx, "SELECT HIGH_PRIORITY `Process_priv` FROM mysql.user LIMIT 0"); err == nil {
		return
	}
	upgradeToVer7(s, ver)
}

func upgradeToVer9(s sessiontypes.Session, ver int64) {
	if ver >= version9 {
		return
	}
	doReentrantDDL(s, "ALTER TABLE mysql.user ADD COLUMN `Trigger_priv` ENUM('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Create_user_priv`", infoschema.ErrColumnExists)
	// For reasons of compatibility, set the non-exists privilege column value to 'Y', as TiDB doesn't check them in older versions.
	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.user SET Trigger_priv='Y'")
}

func doReentrantDDL(s sessiontypes.Session, sql string, ignorableErrs ...error) {
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

func upgradeToVer10(s sessiontypes.Session, ver int64) {
	if ver >= version10 {
		return
	}
	doReentrantDDL(s, "ALTER TABLE mysql.stats_buckets CHANGE COLUMN `value` `upper_bound` BLOB NOT NULL", infoschema.ErrColumnNotExists, infoschema.ErrColumnExists)
	doReentrantDDL(s, "ALTER TABLE mysql.stats_buckets ADD COLUMN `lower_bound` BLOB", infoschema.ErrColumnExists)
	doReentrantDDL(s, "ALTER TABLE mysql.stats_histograms ADD COLUMN `null_count` BIGINT(64) NOT NULL DEFAULT 0", infoschema.ErrColumnExists)
	doReentrantDDL(s, "ALTER TABLE mysql.stats_histograms DROP COLUMN distinct_ratio", dbterror.ErrCantDropFieldOrKey)
	doReentrantDDL(s, "ALTER TABLE mysql.stats_histograms DROP COLUMN use_count_to_estimate", dbterror.ErrCantDropFieldOrKey)
}

func upgradeToVer11(s sessiontypes.Session, ver int64) {
	if ver >= version11 {
		return
	}
	doReentrantDDL(s, "ALTER TABLE mysql.user ADD COLUMN `References_priv` ENUM('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Grant_priv`", infoschema.ErrColumnExists)
	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.user SET References_priv='Y'")
}

func upgradeToVer12(s sessiontypes.Session, ver int64) {
	if ver >= version12 {
		return
	}
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

func upgradeToVer13(s sessiontypes.Session, ver int64) {
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
	for _, sql := range sqls {
		doReentrantDDL(s, sql, infoschema.ErrColumnExists)
	}
	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.user SET Create_tmp_table_priv='Y',Lock_tables_priv='Y',Create_routine_priv='Y',Alter_routine_priv='Y',Event_priv='Y' WHERE Super_priv='Y'")
	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.user SET Create_view_priv='Y',Show_view_priv='Y' WHERE Create_priv='Y'")
}

func upgradeToVer14(s sessiontypes.Session, ver int64) {
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
	for _, sql := range sqls {
		doReentrantDDL(s, sql, infoschema.ErrColumnExists)
	}
}

func upgradeToVer15(s sessiontypes.Session, ver int64) {
	if ver >= version15 {
		return
	}
	doReentrantDDL(s, CreateGCDeleteRangeTable)
}

func upgradeToVer16(s sessiontypes.Session, ver int64) {
	if ver >= version16 {
		return
	}
	doReentrantDDL(s, "ALTER TABLE mysql.stats_histograms ADD COLUMN `cm_sketch` BLOB", infoschema.ErrColumnExists)
}

func upgradeToVer17(s sessiontypes.Session, ver int64) {
	if ver >= version17 {
		return
	}
	doReentrantDDL(s, "ALTER TABLE mysql.user MODIFY User CHAR(32)")
}

func upgradeToVer18(s sessiontypes.Session, ver int64) {
	if ver >= version18 {
		return
	}
	doReentrantDDL(s, "ALTER TABLE mysql.stats_histograms ADD COLUMN `tot_col_size` BIGINT(64) NOT NULL DEFAULT 0", infoschema.ErrColumnExists)
}

func upgradeToVer19(s sessiontypes.Session, ver int64) {
	if ver >= version19 {
		return
	}
	doReentrantDDL(s, "ALTER TABLE mysql.db MODIFY User CHAR(32)")
	doReentrantDDL(s, "ALTER TABLE mysql.tables_priv MODIFY User CHAR(32)")
	doReentrantDDL(s, "ALTER TABLE mysql.columns_priv MODIFY User CHAR(32)")
}

func upgradeToVer20(s sessiontypes.Session, ver int64) {
	if ver >= version20 {
		return
	}
	// NOTE: Feedback is deprecated, but we still need to create this table for compatibility.
	doReentrantDDL(s, CreateStatsFeedbackTable)
}

func upgradeToVer21(s sessiontypes.Session, ver int64) {
	if ver >= version21 {
		return
	}
	mustExecute(s, CreateGCDeleteRangeDoneTable)

	doReentrantDDL(s, "ALTER TABLE mysql.gc_delete_range DROP INDEX job_id", dbterror.ErrCantDropFieldOrKey)
	doReentrantDDL(s, "ALTER TABLE mysql.gc_delete_range ADD UNIQUE INDEX delete_range_index (job_id, element_id)", dbterror.ErrDupKeyName)
	doReentrantDDL(s, "ALTER TABLE mysql.gc_delete_range DROP INDEX element_id", dbterror.ErrCantDropFieldOrKey)
}

func upgradeToVer22(s sessiontypes.Session, ver int64) {
	if ver >= version22 {
		return
	}
	doReentrantDDL(s, "ALTER TABLE mysql.stats_histograms ADD COLUMN `stats_ver` BIGINT(64) NOT NULL DEFAULT 0", infoschema.ErrColumnExists)
}

func upgradeToVer23(s sessiontypes.Session, ver int64) {
	if ver >= version23 {
		return
	}
	doReentrantDDL(s, "ALTER TABLE mysql.stats_histograms ADD COLUMN `flag` BIGINT(64) NOT NULL DEFAULT 0", infoschema.ErrColumnExists)
}

// writeSystemTZ writes system timezone info into mysql.tidb
func writeSystemTZ(s sessiontypes.Session) {
	mustExecute(s, `INSERT HIGH_PRIORITY INTO %n.%n VALUES (%?, %?, "TiDB Global System Timezone.") ON DUPLICATE KEY UPDATE VARIABLE_VALUE= %?`,
		mysql.SystemDB,
		mysql.TiDBTable,
		tidbSystemTZ,
		timeutil.InferSystemTZ(),
		timeutil.InferSystemTZ(),
	)
}

// upgradeToVer24 initializes `System` timezone according to docs/design/2018-09-10-adding-tz-env.md
func upgradeToVer24(s sessiontypes.Session, ver int64) {
	if ver >= version24 {
		return
	}
	writeSystemTZ(s)
}

// upgradeToVer25 updates tidb_max_chunk_size to new low bound value 32 if previous value is small than 32.
func upgradeToVer25(s sessiontypes.Session, ver int64) {
	if ver >= version25 {
		return
	}
	sql := fmt.Sprintf("UPDATE HIGH_PRIORITY %[1]s.%[2]s SET VARIABLE_VALUE = '%[4]d' WHERE VARIABLE_NAME = '%[3]s' AND VARIABLE_VALUE < %[4]d",
		mysql.SystemDB, mysql.GlobalVariablesTable, vardef.TiDBMaxChunkSize, vardef.DefInitChunkSize)
	mustExecute(s, sql)
}

func upgradeToVer26(s sessiontypes.Session, ver int64) {
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

func upgradeToVer27(s sessiontypes.Session, ver int64) {
	if ver >= version27 {
		return
	}
	doReentrantDDL(s, "ALTER TABLE mysql.stats_histograms ADD COLUMN `correlation` DOUBLE NOT NULL DEFAULT 0", infoschema.ErrColumnExists)
}

func upgradeToVer28(s sessiontypes.Session, ver int64) {
	if ver >= version28 {
		return
	}
	doReentrantDDL(s, CreateBindInfoTable)
}

func upgradeToVer29(s sessiontypes.Session, ver int64) {
	// upgradeToVer29 only need to be run when the current version is 28.
	if ver != version28 {
		return
	}
	doReentrantDDL(s, "ALTER TABLE mysql.bind_info CHANGE create_time create_time TIMESTAMP(3)")
	doReentrantDDL(s, "ALTER TABLE mysql.bind_info CHANGE update_time update_time TIMESTAMP(3)")
	doReentrantDDL(s, "ALTER TABLE mysql.bind_info ADD INDEX sql_index (original_sql(1024),default_db(1024))", dbterror.ErrDupKeyName)
}

func upgradeToVer30(s sessiontypes.Session, ver int64) {
	if ver >= version30 {
		return
	}
	mustExecute(s, CreateStatsTopNTable)
}

func upgradeToVer31(s sessiontypes.Session, ver int64) {
	if ver >= version31 {
		return
	}
	doReentrantDDL(s, "ALTER TABLE mysql.stats_histograms ADD COLUMN `last_analyze_pos` BLOB DEFAULT NULL", infoschema.ErrColumnExists)
}

func upgradeToVer32(s sessiontypes.Session, ver int64) {
	if ver >= version32 {
		return
	}
	doReentrantDDL(s, "ALTER TABLE mysql.tables_priv MODIFY table_priv SET('Select','Insert','Update','Delete','Create','Drop','Grant', 'Index', 'Alter', 'Create View', 'Show View', 'Trigger', 'References')")
}

func upgradeToVer33(s sessiontypes.Session, ver int64) {
	if ver >= version33 {
		return
	}
	doReentrantDDL(s, CreateExprPushdownBlacklist)
}

func upgradeToVer34(s sessiontypes.Session, ver int64) {
	if ver >= version34 {
		return
	}
	doReentrantDDL(s, CreateOptRuleBlacklist)
}

func upgradeToVer35(s sessiontypes.Session, ver int64) {
	if ver >= version35 {
		return
	}
	sql := fmt.Sprintf("UPDATE HIGH_PRIORITY %s.%s SET VARIABLE_NAME = '%s' WHERE VARIABLE_NAME = 'tidb_back_off_weight'",
		mysql.SystemDB, mysql.GlobalVariablesTable, vardef.TiDBBackOffWeight)
	mustExecute(s, sql)
}

func upgradeToVer36(s sessiontypes.Session, ver int64) {
	if ver >= version36 {
		return
	}
	doReentrantDDL(s, "ALTER TABLE mysql.user ADD COLUMN `Shutdown_priv` ENUM('N','Y') DEFAULT 'N'", infoschema.ErrColumnExists)
	// A root user will have those privileges after upgrading.
	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.user SET Shutdown_priv='Y' WHERE Super_priv='Y'")
	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.user SET Create_tmp_table_priv='Y',Lock_tables_priv='Y',Create_routine_priv='Y',Alter_routine_priv='Y',Event_priv='Y' WHERE Super_priv='Y'")
}

func upgradeToVer37(s sessiontypes.Session, ver int64) {
	if ver >= version37 {
		return
	}
	// when upgrade from old tidb and no 'tidb_enable_window_function' in GLOBAL_VARIABLES, init it with 0.
	sql := fmt.Sprintf("INSERT IGNORE INTO  %s.%s (`VARIABLE_NAME`, `VARIABLE_VALUE`) VALUES ('%s', '%d')",
		mysql.SystemDB, mysql.GlobalVariablesTable, vardef.TiDBEnableWindowFunction, 0)
	mustExecute(s, sql)
}

func upgradeToVer38(s sessiontypes.Session, ver int64) {
	if ver >= version38 {
		return
	}
	doReentrantDDL(s, CreateGlobalPrivTable)
}

func writeNewCollationParameter(s sessiontypes.Session, flag bool) {
	comment := "If the new collations are enabled. Do not edit it."
	b := varFalse
	if flag {
		b = varTrue
	}
	mustExecute(s, `INSERT HIGH_PRIORITY INTO %n.%n VALUES (%?, %?, %?) ON DUPLICATE KEY UPDATE VARIABLE_VALUE=%?`,
		mysql.SystemDB, mysql.TiDBTable, TidbNewCollationEnabled, b, comment, b,
	)
}

func upgradeToVer40(s sessiontypes.Session, ver int64) {
	if ver >= version40 {
		return
	}
	// There is no way to enable new collation for an existing TiDB cluster.
	writeNewCollationParameter(s, false)
}

func upgradeToVer41(s sessiontypes.Session, ver int64) {
	if ver >= version41 {
		return
	}
	doReentrantDDL(s, "ALTER TABLE mysql.user CHANGE `password` `authentication_string` TEXT", infoschema.ErrColumnExists, infoschema.ErrColumnNotExists)
	doReentrantDDL(s, "ALTER TABLE mysql.user ADD COLUMN `password` TEXT as (`authentication_string`)", infoschema.ErrColumnExists)
}

// writeDefaultExprPushDownBlacklist writes default expr pushdown blacklist into mysql.expr_pushdown_blacklist
func writeDefaultExprPushDownBlacklist(s sessiontypes.Session) {
	mustExecute(s, "INSERT HIGH_PRIORITY INTO mysql.expr_pushdown_blacklist VALUES"+
		"('date_add','tiflash', 'DST(daylight saving time) does not take effect in TiFlash date_add')")
}

func upgradeToVer42(s sessiontypes.Session, ver int64) {
	if ver >= version42 {
		return
	}
	doReentrantDDL(s, "ALTER TABLE mysql.expr_pushdown_blacklist ADD COLUMN `store_type` CHAR(100) NOT NULL DEFAULT 'tikv,tiflash,tidb'", infoschema.ErrColumnExists)
	doReentrantDDL(s, "ALTER TABLE mysql.expr_pushdown_blacklist ADD COLUMN `reason` VARCHAR(200)", infoschema.ErrColumnExists)
	writeDefaultExprPushDownBlacklist(s)
}

// Convert statement summary global variables to non-empty values.
func writeStmtSummaryVars(s sessiontypes.Session) {
	sql := "UPDATE %n.%n SET variable_value= %? WHERE variable_name= %? AND variable_value=''"
	mustExecute(s, sql, mysql.SystemDB, mysql.GlobalVariablesTable, variable.BoolToOnOff(vardef.DefTiDBEnableStmtSummary), vardef.TiDBEnableStmtSummary)
	mustExecute(s, sql, mysql.SystemDB, mysql.GlobalVariablesTable, variable.BoolToOnOff(vardef.DefTiDBStmtSummaryInternalQuery), vardef.TiDBStmtSummaryInternalQuery)
	mustExecute(s, sql, mysql.SystemDB, mysql.GlobalVariablesTable, strconv.Itoa(vardef.DefTiDBStmtSummaryRefreshInterval), vardef.TiDBStmtSummaryRefreshInterval)
	mustExecute(s, sql, mysql.SystemDB, mysql.GlobalVariablesTable, strconv.Itoa(vardef.DefTiDBStmtSummaryHistorySize), vardef.TiDBStmtSummaryHistorySize)
	mustExecute(s, sql, mysql.SystemDB, mysql.GlobalVariablesTable, strconv.FormatUint(uint64(vardef.DefTiDBStmtSummaryMaxStmtCount), 10), vardef.TiDBStmtSummaryMaxStmtCount)
	mustExecute(s, sql, mysql.SystemDB, mysql.GlobalVariablesTable, strconv.FormatUint(uint64(vardef.DefTiDBStmtSummaryMaxSQLLength), 10), vardef.TiDBStmtSummaryMaxSQLLength)
}

func upgradeToVer43(s sessiontypes.Session, ver int64) {
	if ver >= version43 {
		return
	}
	writeStmtSummaryVars(s)
}

func upgradeToVer44(s sessiontypes.Session, ver int64) {
	if ver >= version44 {
		return
	}
	mustExecute(s, "DELETE FROM mysql.global_variables where variable_name = \"tidb_isolation_read_engines\"")
}

func upgradeToVer45(s sessiontypes.Session, ver int64) {
	if ver >= version45 {
		return
	}
	doReentrantDDL(s, "ALTER TABLE mysql.user ADD COLUMN `Config_priv` ENUM('N','Y') DEFAULT 'N'", infoschema.ErrColumnExists)
	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.user SET Config_priv='Y' WHERE Super_priv='Y'")
}

// In v3.1.1, we wrongly replace the context of upgradeToVer39 with upgradeToVer44. If we upgrade from v3.1.1 to a newer version,
// upgradeToVer39 will be missed. So we redo upgradeToVer39 here to make sure the upgrading from v3.1.1 succeed.
func upgradeToVer46(s sessiontypes.Session, ver int64) {
	if ver >= version46 {
		return
	}
	doReentrantDDL(s, "ALTER TABLE mysql.user ADD COLUMN `Reload_priv` ENUM('N','Y') DEFAULT 'N'", infoschema.ErrColumnExists)
	doReentrantDDL(s, "ALTER TABLE mysql.user ADD COLUMN `File_priv` ENUM('N','Y') DEFAULT 'N'", infoschema.ErrColumnExists)
	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.user SET Reload_priv='Y' WHERE Super_priv='Y'")
	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.user SET File_priv='Y' WHERE Super_priv='Y'")
}

func upgradeToVer47(s sessiontypes.Session, ver int64) {
	if ver >= version47 {
		return
	}
	doReentrantDDL(s, "ALTER TABLE mysql.bind_info ADD COLUMN `source` varchar(10) NOT NULL default 'unknown'", infoschema.ErrColumnExists)
}

func upgradeToVer50(s sessiontypes.Session, ver int64) {
	if ver >= version50 {
		return
	}
	doReentrantDDL(s, CreateSchemaIndexUsageTable)
}

func upgradeToVer52(s sessiontypes.Session, ver int64) {
	if ver >= version52 {
		return
	}
	doReentrantDDL(s, "ALTER TABLE mysql.stats_histograms MODIFY cm_sketch BLOB(6291456)")
}

func upgradeToVer53(s sessiontypes.Session, ver int64) {
	if ver >= version53 {
		return
	}
	// when upgrade from old tidb and no `tidb_enable_strict_double_type_check` in GLOBAL_VARIABLES, init it with 1`
	sql := fmt.Sprintf("INSERT IGNORE INTO %s.%s (`VARIABLE_NAME`, `VARIABLE_VALUE`) VALUES ('%s', '%d')",
		mysql.SystemDB, mysql.GlobalVariablesTable, vardef.TiDBEnableStrictDoubleTypeCheck, 0)
	mustExecute(s, sql)
}

func upgradeToVer54(s sessiontypes.Session, ver int64) {
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
func upgradeToVer55(s sessiontypes.Session, ver int64) {
	if ver >= version55 {
		return
	}
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
func upgradeToVer56(s sessiontypes.Session, ver int64) {
	if ver >= version56 {
		return
	}
	doReentrantDDL(s, CreateStatsExtended)
}

func upgradeToVer57(s sessiontypes.Session, ver int64) {
	if ver >= version57 {
		return
	}
	insertBuiltinBindInfoRow(s)
}

func initBindInfoTable(s sessiontypes.Session) {
	mustExecute(s, CreateBindInfoTable)
	insertBuiltinBindInfoRow(s)
}

func insertBuiltinBindInfoRow(s sessiontypes.Session) {
	mustExecute(s, `INSERT HIGH_PRIORITY INTO mysql.bind_info(original_sql, bind_sql, default_db, status, create_time, update_time, charset, collation, source)
						VALUES (%?, %?, "mysql", %?, "0000-00-00 00:00:00", "0000-00-00 00:00:00", "", "", %?)`,
		bindinfo.BuiltinPseudoSQL4BindLock, bindinfo.BuiltinPseudoSQL4BindLock, bindinfo.StatusBuiltin, bindinfo.StatusBuiltin,
	)
}

func upgradeToVer59(s sessiontypes.Session, ver int64) {
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

func upgradeToVer60(s sessiontypes.Session, ver int64) {
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

func upgradeToVer67(s sessiontypes.Session, ver int64) {
	if ver >= version67 {
		return
	}
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

func writeMemoryQuotaQuery(s sessiontypes.Session) {
	comment := "memory_quota_query is 32GB by default in v3.0.x, 1GB by default in v4.0.x+"
	mustExecute(s, `INSERT HIGH_PRIORITY INTO %n.%n VALUES (%?, %?, %?) ON DUPLICATE KEY UPDATE VARIABLE_VALUE=%?`,
		mysql.SystemDB, mysql.TiDBTable, tidbDefMemoryQuotaQuery, 32<<30, comment, 32<<30,
	)
}

func upgradeToVer62(s sessiontypes.Session, ver int64) {
	if ver >= version62 {
		return
	}
	doReentrantDDL(s, "ALTER TABLE mysql.stats_buckets ADD COLUMN `ndv` bigint not null default 0", infoschema.ErrColumnExists)
}

func upgradeToVer63(s sessiontypes.Session, ver int64) {
	if ver >= version63 {
		return
	}
	doReentrantDDL(s, "ALTER TABLE mysql.user ADD COLUMN `Create_tablespace_priv` ENUM('N','Y') DEFAULT 'N'", infoschema.ErrColumnExists)
	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.user SET Create_tablespace_priv='Y' where Super_priv='Y'")
}

func upgradeToVer64(s sessiontypes.Session, ver int64) {
	if ver >= version64 {
		return
	}
	doReentrantDDL(s, "ALTER TABLE mysql.user ADD COLUMN `Repl_slave_priv` ENUM('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Execute_priv`", infoschema.ErrColumnExists)
	doReentrantDDL(s, "ALTER TABLE mysql.user ADD COLUMN `Repl_client_priv` ENUM('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Repl_slave_priv`", infoschema.ErrColumnExists)
	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.user SET Repl_slave_priv='Y',Repl_client_priv='Y' where Super_priv='Y'")
}

func upgradeToVer65(s sessiontypes.Session, ver int64) {
	if ver >= version65 {
		return
	}
	doReentrantDDL(s, CreateStatsFMSketchTable)
}

func upgradeToVer66(s sessiontypes.Session, ver int64) {
	if ver >= version66 {
		return
	}
	mustExecute(s, "set @@global.tidb_track_aggregate_memory_usage = 1")
}

func upgradeToVer68(s sessiontypes.Session, ver int64) {
	if ver >= version68 {
		return
	}
	mustExecute(s, "DELETE FROM mysql.global_variables where VARIABLE_NAME = 'tidb_enable_clustered_index' and VARIABLE_VALUE = 'OFF'")
}

func upgradeToVer69(s sessiontypes.Session, ver int64) {
	if ver >= version69 {
		return
	}
	doReentrantDDL(s, CreateGlobalGrantsTable)
}

func upgradeToVer70(s sessiontypes.Session, ver int64) {
	if ver >= version70 {
		return
	}
	doReentrantDDL(s, "ALTER TABLE mysql.user ADD COLUMN plugin CHAR(64) AFTER authentication_string", infoschema.ErrColumnExists)
	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.user SET plugin='mysql_native_password'")
}

func upgradeToVer71(s sessiontypes.Session, ver int64) {
	if ver >= version71 {
		return
	}
	mustExecute(s, "UPDATE mysql.global_variables SET VARIABLE_VALUE='OFF' WHERE VARIABLE_NAME = 'tidb_multi_statement_mode' AND VARIABLE_VALUE = 'WARN'")
}

func upgradeToVer72(s sessiontypes.Session, ver int64) {
	if ver >= version72 {
		return
	}
	doReentrantDDL(s, "ALTER TABLE mysql.stats_meta ADD COLUMN snapshot BIGINT(64) UNSIGNED NOT NULL DEFAULT 0", infoschema.ErrColumnExists)
}

func upgradeToVer73(s sessiontypes.Session, ver int64) {
	if ver >= version73 {
		return
	}
	doReentrantDDL(s, CreateCapturePlanBaselinesBlacklist)
}

func upgradeToVer74(s sessiontypes.Session, ver int64) {
	if ver >= version74 {
		return
	}
	// The old default value of `tidb_stmt_summary_max_stmt_count` is 200, we want to enlarge this to the new default value when TiDB upgrade.
	mustExecute(s, fmt.Sprintf("UPDATE mysql.global_variables SET VARIABLE_VALUE='%[1]v' WHERE VARIABLE_NAME = 'tidb_stmt_summary_max_stmt_count' AND CAST(VARIABLE_VALUE AS SIGNED) = 200", vardef.DefTiDBStmtSummaryMaxStmtCount))
}

func upgradeToVer75(s sessiontypes.Session, ver int64) {
	if ver >= version75 {
		return
	}
	doReentrantDDL(s, "ALTER TABLE mysql.user MODIFY COLUMN Host CHAR(255)")
	doReentrantDDL(s, "ALTER TABLE mysql.global_priv MODIFY COLUMN Host CHAR(255)")
	doReentrantDDL(s, "ALTER TABLE mysql.db MODIFY COLUMN Host CHAR(255)")
	doReentrantDDL(s, "ALTER TABLE mysql.tables_priv MODIFY COLUMN Host CHAR(255)")
	doReentrantDDL(s, "ALTER TABLE mysql.columns_priv MODIFY COLUMN Host CHAR(255)")
}

func upgradeToVer76(s sessiontypes.Session, ver int64) {
	if ver >= version76 {
		return
	}
	doReentrantDDL(s, "ALTER TABLE mysql.columns_priv MODIFY COLUMN Column_priv SET('Select','Insert','Update','References')")
}

func upgradeToVer77(s sessiontypes.Session, ver int64) {
	if ver >= version77 {
		return
	}
	doReentrantDDL(s, CreateColumnStatsUsageTable)
}

func upgradeToVer78(s sessiontypes.Session, ver int64) {
	if ver >= version78 {
		return
	}
	doReentrantDDL(s, "ALTER TABLE mysql.stats_buckets MODIFY upper_bound LONGBLOB NOT NULL")
	doReentrantDDL(s, "ALTER TABLE mysql.stats_buckets MODIFY lower_bound LONGBLOB")
	doReentrantDDL(s, "ALTER TABLE mysql.stats_histograms MODIFY last_analyze_pos LONGBLOB DEFAULT NULL")
}

func upgradeToVer79(s sessiontypes.Session, ver int64) {
	if ver >= version79 {
		return
	}
	doReentrantDDL(s, CreateTableCacheMetaTable)
}

func upgradeToVer80(s sessiontypes.Session, ver int64) {
	if ver >= version80 {
		return
	}
	// Check if tidb_analyze_version exists in mysql.GLOBAL_VARIABLES.
	// If not, insert "tidb_analyze_version | 1" since this is the old behavior before we introduce this variable.
	initGlobalVariableIfNotExists(s, vardef.TiDBAnalyzeVersion, 1)
}

// For users that upgrade TiDB from a pre-4.0 version, we want to disable index merge by default.
// This helps minimize query plan regressions.
func upgradeToVer81(s sessiontypes.Session, ver int64) {
	if ver >= version81 {
		return
	}
	// Check if tidb_enable_index_merge exists in mysql.GLOBAL_VARIABLES.
	// If not, insert "tidb_enable_index_merge | off".
	initGlobalVariableIfNotExists(s, vardef.TiDBEnableIndexMerge, vardef.Off)
}

func upgradeToVer82(s sessiontypes.Session, ver int64) {
	if ver >= version82 {
		return
	}
	doReentrantDDL(s, CreateAnalyzeOptionsTable)
}

func upgradeToVer83(s sessiontypes.Session, ver int64) {
	if ver >= version83 {
		return
	}
	doReentrantDDL(s, CreateStatsHistory)
}

func upgradeToVer84(s sessiontypes.Session, ver int64) {
	if ver >= version84 {
		return
	}
	doReentrantDDL(s, CreateStatsMetaHistory)
}

func upgradeToVer85(s sessiontypes.Session, ver int64) {
	if ver >= version85 {
		return
	}
	mustExecute(s, fmt.Sprintf("UPDATE HIGH_PRIORITY mysql.bind_info SET status= '%s' WHERE status = '%s'", bindinfo.StatusEnabled, bindinfo.StatusUsing))
}

func upgradeToVer86(s sessiontypes.Session, ver int64) {
	if ver >= version86 {
		return
	}
	doReentrantDDL(s, "ALTER TABLE mysql.tables_priv MODIFY COLUMN Column_priv SET('Select','Insert','Update','References')")
}

func upgradeToVer87(s sessiontypes.Session, ver int64) {
	if ver >= version87 {
		return
	}
	doReentrantDDL(s, CreateAnalyzeJobs)
}

func upgradeToVer88(s sessiontypes.Session, ver int64) {
	if ver >= version88 {
		return
	}
	doReentrantDDL(s, "ALTER TABLE mysql.user CHANGE `Repl_slave_priv` `Repl_slave_priv` ENUM('N','Y') NOT NULL DEFAULT 'N' AFTER `Execute_priv`")
	doReentrantDDL(s, "ALTER TABLE mysql.user CHANGE `Repl_client_priv` `Repl_client_priv` ENUM('N','Y') NOT NULL DEFAULT 'N' AFTER `Repl_slave_priv`")
}

func upgradeToVer89(s sessiontypes.Session, ver int64) {
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
func importConfigOption(s sessiontypes.Session, configName, svName, valStr string) {
	message := fmt.Sprintf("%s is now configured by the system variable %s. One-time importing the value specified in tidb.toml file", configName, svName)
	logutil.BgLogger().Warn(message, zap.String("value", valStr))
	// We use insert ignore, since if its a duplicate we don't want to overwrite any user-set values.
	sql := fmt.Sprintf("INSERT IGNORE INTO  %s.%s (`VARIABLE_NAME`, `VARIABLE_VALUE`) VALUES ('%s', '%s')",
		mysql.SystemDB, mysql.GlobalVariablesTable, svName, valStr)
	mustExecute(s, sql)
}

func upgradeToVer90(s sessiontypes.Session, ver int64) {
	if ver >= version90 {
		return
	}
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

func upgradeToVer91(s sessiontypes.Session, ver int64) {
	if ver >= version91 {
		return
	}
	valStr := variable.BoolToOnOff(config.GetGlobalConfig().PreparedPlanCache.Enabled)
	importConfigOption(s, "prepared-plan-cache.enable", vardef.TiDBEnablePrepPlanCache, valStr)

	valStr = strconv.Itoa(int(config.GetGlobalConfig().PreparedPlanCache.Capacity))
	importConfigOption(s, "prepared-plan-cache.capacity", vardef.TiDBPrepPlanCacheSize, valStr)

	valStr = strconv.FormatFloat(config.GetGlobalConfig().PreparedPlanCache.MemoryGuardRatio, 'f', -1, 64)
	importConfigOption(s, "prepared-plan-cache.memory-guard-ratio", vardef.TiDBPrepPlanCacheMemoryGuardRatio, valStr)
}

func upgradeToVer93(s sessiontypes.Session, ver int64) {
	if ver >= version93 {
		return
	}
	valStr := variable.BoolToOnOff(config.GetGlobalConfig().OOMUseTmpStorage)
	importConfigOption(s, "oom-use-tmp-storage", vardef.TiDBEnableTmpStorageOnOOM, valStr)
}

func upgradeToVer94(s sessiontypes.Session, ver int64) {
	if ver >= version94 {
		return
	}
	mustExecute(s, CreateMDLView)
}

func upgradeToVer95(s sessiontypes.Session, ver int64) {
	if ver >= version95 {
		return
	}
	doReentrantDDL(s, "ALTER TABLE mysql.user ADD COLUMN IF NOT EXISTS `User_attributes` JSON")
}

func upgradeToVer97(s sessiontypes.Session, ver int64) {
	if ver >= version97 {
		return
	}
	// Check if tidb_opt_range_max_size exists in mysql.GLOBAL_VARIABLES.
	// If not, insert "tidb_opt_range_max_size | 0" since this is the old behavior before we introduce this variable.
	initGlobalVariableIfNotExists(s, vardef.TiDBOptRangeMaxSize, 0)
}

func upgradeToVer98(s sessiontypes.Session, ver int64) {
	if ver >= version98 {
		return
	}
	doReentrantDDL(s, "ALTER TABLE mysql.user ADD COLUMN IF NOT EXISTS `Token_issuer` varchar(255)")
}

func upgradeToVer99Before(s sessiontypes.Session) {
	mustExecute(s, "INSERT HIGH_PRIORITY IGNORE INTO %n.%n VALUES (%?, %?);",
		mysql.SystemDB, mysql.GlobalVariablesTable, vardef.TiDBEnableMDL, 0)
}

func upgradeToVer99After(s sessiontypes.Session) {
	sql := fmt.Sprintf("UPDATE HIGH_PRIORITY %[1]s.%[2]s SET VARIABLE_VALUE = %[4]d WHERE VARIABLE_NAME = '%[3]s'",
		mysql.SystemDB, mysql.GlobalVariablesTable, vardef.TiDBEnableMDL, 1)
	mustExecute(s, sql)
	err := kv.RunInNewTxn(kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL), s.GetStore(), true, func(_ context.Context, txn kv.Transaction) error {
		t := meta.NewMutator(txn)
		return t.SetMetadataLock(true)
	})
	terror.MustNil(err)
}

func upgradeToVer100(s sessiontypes.Session, ver int64) {
	if ver >= version100 {
		return
	}
	valStr := strconv.Itoa(int(config.GetGlobalConfig().Performance.ServerMemoryQuota))
	importConfigOption(s, "performance.server-memory-quota", vardef.TiDBServerMemoryLimit, valStr)
}

func upgradeToVer101(s sessiontypes.Session, ver int64) {
	if ver >= version101 {
		return
	}
	doReentrantDDL(s, CreatePlanReplayerStatusTable)
}

func upgradeToVer102(s sessiontypes.Session, ver int64) {
	if ver >= version102 {
		return
	}
	doReentrantDDL(s, CreatePlanReplayerTaskTable)
}

func upgradeToVer103(s sessiontypes.Session, ver int64) {
	if ver >= version103 {
		return
	}
	doReentrantDDL(s, CreateStatsTableLocked)
}

func upgradeToVer104(s sessiontypes.Session, ver int64) {
	if ver >= version104 {
		return
	}

	doReentrantDDL(s, "ALTER TABLE mysql.bind_info ADD COLUMN IF NOT EXISTS `sql_digest` varchar(64)")
	doReentrantDDL(s, "ALTER TABLE mysql.bind_info ADD COLUMN IF NOT EXISTS `plan_digest` varchar(64)")
}

// For users that upgrade TiDB from a pre-6.0 version, we want to disable tidb cost model2 by default to keep plans unchanged.
func upgradeToVer105(s sessiontypes.Session, ver int64) {
	if ver >= version105 {
		return
	}
	initGlobalVariableIfNotExists(s, vardef.TiDBCostModelVersion, "1")
}

func upgradeToVer106(s sessiontypes.Session, ver int64) {
	if ver >= version106 {
		return
	}
	doReentrantDDL(s, CreatePasswordHistory)
	doReentrantDDL(s, "Alter table mysql.user add COLUMN IF NOT EXISTS `Password_reuse_history` smallint unsigned  DEFAULT NULL AFTER `Create_Tablespace_Priv` ")
	doReentrantDDL(s, "Alter table mysql.user add COLUMN IF NOT EXISTS `Password_reuse_time` smallint unsigned DEFAULT NULL AFTER `Password_reuse_history`")
}

func upgradeToVer107(s sessiontypes.Session, ver int64) {
	if ver >= version107 {
		return
	}
	doReentrantDDL(s, "ALTER TABLE mysql.user ADD COLUMN IF NOT EXISTS `Password_expired` ENUM('N','Y') NOT NULL DEFAULT 'N'")
	doReentrantDDL(s, "ALTER TABLE mysql.user ADD COLUMN IF NOT EXISTS `Password_last_changed` TIMESTAMP DEFAULT CURRENT_TIMESTAMP()")
	doReentrantDDL(s, "ALTER TABLE mysql.user ADD COLUMN IF NOT EXISTS `Password_lifetime` SMALLINT UNSIGNED DEFAULT NULL")
}

func upgradeToVer108(s sessiontypes.Session, ver int64) {
	if ver >= version108 {
		return
	}
	doReentrantDDL(s, CreateTTLTableStatus)
}

// For users that upgrade TiDB from a 6.2-6.4 version, we want to disable tidb gc_aware_memory_track by default.
func upgradeToVer109(s sessiontypes.Session, ver int64) {
	if ver >= version109 {
		return
	}
	mustExecute(s, "REPLACE HIGH_PRIORITY INTO %n.%n VALUES (%?, %?);",
		mysql.SystemDB, mysql.GlobalVariablesTable, vardef.TiDBEnableGCAwareMemoryTrack, 0)
}

// For users that upgrade TiDB from a 5.4-6.4 version, we want to enable tidb tidb_stats_load_pseudo_timeout by default.
func upgradeToVer110(s sessiontypes.Session, ver int64) {
	if ver >= version110 {
		return
	}
	mustExecute(s, "REPLACE HIGH_PRIORITY INTO %n.%n VALUES (%?, %?);",
		mysql.SystemDB, mysql.GlobalVariablesTable, vardef.TiDBStatsLoadPseudoTimeout, 1)
}

func upgradeToVer130(s sessiontypes.Session, ver int64) {
	if ver >= version130 {
		return
	}
	doReentrantDDL(s, "ALTER TABLE mysql.stats_meta_history ADD COLUMN IF NOT EXISTS `source` varchar(40) NOT NULL after `version`;")
}

func upgradeToVer131(s sessiontypes.Session, ver int64) {
	if ver >= version131 {
		return
	}
	doReentrantDDL(s, CreateTTLTask)
	doReentrantDDL(s, CreateTTLJobHistory)
}

func upgradeToVer132(s sessiontypes.Session, ver int64) {
	if ver >= version132 {
		return
	}
	doReentrantDDL(s, CreateMDLView)
}

func upgradeToVer133(s sessiontypes.Session, ver int64) {
	if ver >= version133 {
		return
	}
	mustExecute(s, "UPDATE HIGH_PRIORITY %n.%n set VARIABLE_VALUE = %? where VARIABLE_NAME = %? and VARIABLE_VALUE = %?;",
		mysql.SystemDB, mysql.GlobalVariablesTable, vardef.DefTiDBServerMemoryLimit, vardef.TiDBServerMemoryLimit, "0")
}

func upgradeToVer134(s sessiontypes.Session, ver int64) {
	if ver >= version134 {
		return
	}
	mustExecute(s, "REPLACE HIGH_PRIORITY INTO %n.%n VALUES (%?, %?);", mysql.SystemDB, mysql.GlobalVariablesTable, vardef.ForeignKeyChecks, vardef.On)
	mustExecute(s, "REPLACE HIGH_PRIORITY INTO %n.%n VALUES (%?, %?);", mysql.SystemDB, mysql.GlobalVariablesTable, vardef.TiDBEnableForeignKey, vardef.On)
	mustExecute(s, "REPLACE HIGH_PRIORITY INTO %n.%n VALUES (%?, %?);", mysql.SystemDB, mysql.GlobalVariablesTable, vardef.TiDBEnableHistoricalStats, vardef.On)
	mustExecute(s, "REPLACE HIGH_PRIORITY INTO %n.%n VALUES (%?, %?);", mysql.SystemDB, mysql.GlobalVariablesTable, vardef.TiDBEnablePlanReplayerCapture, vardef.On)
	mustExecute(s, "UPDATE HIGH_PRIORITY %n.%n SET VARIABLE_VALUE = %? WHERE VARIABLE_NAME = %? AND VARIABLE_VALUE = %?;", mysql.SystemDB, mysql.GlobalVariablesTable, "4", vardef.TiDBStoreBatchSize, "0")
}

// For users that upgrade TiDB from a pre-7.0 version, we want to set tidb_opt_advanced_join_hint to off by default to keep plans unchanged.
func upgradeToVer135(s sessiontypes.Session, ver int64) {
	if ver >= version135 {
		return
	}
	initGlobalVariableIfNotExists(s, vardef.TiDBOptAdvancedJoinHint, false)
}

func upgradeToVer136(s sessiontypes.Session, ver int64) {
	if ver >= version136 {
		return
	}
	mustExecute(s, CreateGlobalTask)
	doReentrantDDL(s, "ALTER TABLE mysql.tidb_background_subtask DROP INDEX namespace", dbterror.ErrCantDropFieldOrKey)
	doReentrantDDL(s, "ALTER TABLE mysql.tidb_background_subtask ADD INDEX idx_task_key(task_key)", dbterror.ErrDupKeyName)
}

func upgradeToVer137(_ sessiontypes.Session, _ int64) {
	// NOOP, we don't depend on ddl to init the default group due to backward compatible issue.
}

// For users that upgrade TiDB from a version below 7.0, we want to enable tidb tidb_enable_null_aware_anti_join by default.
func upgradeToVer138(s sessiontypes.Session, ver int64) {
	if ver >= version138 {
		return
	}
	mustExecute(s, "REPLACE HIGH_PRIORITY INTO %n.%n VALUES (%?, %?);", mysql.SystemDB, mysql.GlobalVariablesTable, vardef.TiDBOptimizerEnableNAAJ, vardef.On)
}

func upgradeToVer139(sessiontypes.Session, int64) {}

func upgradeToVer140(s sessiontypes.Session, ver int64) {
	if ver >= version140 {
		return
	}
	doReentrantDDL(s, "ALTER TABLE mysql.tidb_global_task ADD COLUMN `task_key` VARCHAR(256) NOT NULL AFTER `id`", infoschema.ErrColumnExists)
	doReentrantDDL(s, "ALTER TABLE mysql.tidb_global_task ADD UNIQUE KEY task_key(task_key)", dbterror.ErrDupKeyName)
}

// upgradeToVer141 sets the value of `tidb_session_plan_cache_size` as `tidb_prepared_plan_cache_size` for compatibility,
// and update tidb_load_based_replica_read_threshold from 0 to 4.
func upgradeToVer141(s sessiontypes.Session, ver int64) {
	if ver >= version141 {
		return
	}
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

func upgradeToVer142(s sessiontypes.Session, ver int64) {
	if ver >= version142 {
		return
	}
	initGlobalVariableIfNotExists(s, vardef.TiDBEnableNonPreparedPlanCache, vardef.Off)
}

func upgradeToVer143(s sessiontypes.Session, ver int64) {
	if ver >= version143 {
		return
	}
	doReentrantDDL(s, "ALTER TABLE mysql.tidb_global_task ADD COLUMN `error` BLOB", infoschema.ErrColumnExists)
	doReentrantDDL(s, "ALTER TABLE mysql.tidb_background_subtask ADD COLUMN `error` BLOB", infoschema.ErrColumnExists)
}

func upgradeToVer144(s sessiontypes.Session, ver int64) {
	if ver >= version144 {
		return
	}

	initGlobalVariableIfNotExists(s, vardef.TiDBPlanCacheInvalidationOnFreshStats, vardef.Off)
}

func upgradeToVer146(s sessiontypes.Session, ver int64) {
	if ver >= version146 {
		return
	}
	doReentrantDDL(s, "ALTER TABLE mysql.stats_meta_history ADD INDEX idx_create_time (create_time)", dbterror.ErrDupKeyName)
	doReentrantDDL(s, "ALTER TABLE mysql.stats_history ADD INDEX idx_create_time (create_time)", dbterror.ErrDupKeyName)
}

func upgradeToVer167(s sessiontypes.Session, ver int64) {
	if ver >= version167 {
		return
	}
	doReentrantDDL(s, "ALTER TABLE mysql.tidb_background_subtask ADD COLUMN `step` INT AFTER `id`", infoschema.ErrColumnExists)
}

func upgradeToVer168(s sessiontypes.Session, ver int64) {
	if ver >= version168 {
		return
	}
	mustExecute(s, CreateImportJobs)
}

func upgradeToVer169(s sessiontypes.Session, ver int64) {
	if ver >= version169 {
		return
	}
	mustExecute(s, CreateRunawayTable)
}

func upgradeToVer170(s sessiontypes.Session, ver int64) {
	if ver >= version170 {
		return
	}
	mustExecute(s, CreateTimers)
}

func upgradeToVer171(s sessiontypes.Session, ver int64) {
	if ver >= version171 {
		return
	}
	mustExecute(s, "ALTER TABLE mysql.tidb_runaway_queries CHANGE COLUMN `tidb_server` `tidb_server` varchar(512)")
}

func upgradeToVer172(s sessiontypes.Session, ver int64) {
	if ver >= version172 {
		return
	}
	mustExecute(s, "DROP TABLE IF EXISTS mysql.tidb_runaway_quarantined_watch")
	mustExecute(s, CreateRunawayWatchTable)
	mustExecute(s, CreateDoneRunawayWatchTable)
}

func upgradeToVer173(s sessiontypes.Session, ver int64) {
	if ver >= version173 {
		return
	}
	doReentrantDDL(s, "ALTER TABLE mysql.tidb_background_subtask ADD COLUMN `summary` JSON", infoschema.ErrColumnExists)
}

func upgradeToVer174(s sessiontypes.Session, ver int64) {
	if ver >= version174 {
		return
	}
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
func upgradeToVer175(s sessiontypes.Session, ver int64) {
	if ver >= version175 {
		return
	}

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

func upgradeToVer176(s sessiontypes.Session, ver int64) {
	if ver >= version176 {
		return
	}
	mustExecute(s, CreateGlobalTaskHistory)
}

func upgradeToVer177(s sessiontypes.Session, ver int64) {
	if ver >= version177 {
		return
	}
	// ignore error when upgrading from v7.4 to higher version.
	doReentrantDDL(s, CreateDistFrameworkMeta, infoschema.ErrTableExists)
	err := s.GetSessionVars().GlobalVarsAccessor.SetGlobalSysVar(context.Background(), vardef.TiDBEnableAsyncMergeGlobalStats, vardef.Off)
	if err != nil {
		logutil.BgLogger().Fatal("upgradeToVer177 error", zap.Error(err))
	}
}

// writeDDLTableVersion writes mDDLTableVersion into mysql.tidb
func writeDDLTableVersion(s sessiontypes.Session) {
	var err error
	var ddlTableVersion meta.DDLTableVersion
	err = kv.RunInNewTxn(kv.WithInternalSourceType(context.Background(), kv.InternalTxnBootstrap), s.GetStore(), true, func(_ context.Context, txn kv.Transaction) error {
		t := meta.NewMutator(txn)
		ddlTableVersion, err = t.CheckDDLTableVersion()
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

func upgradeToVer178(s sessiontypes.Session, ver int64) {
	if ver >= version178 {
		return
	}
	writeDDLTableVersion(s)
}

func upgradeToVer179(s sessiontypes.Session, ver int64) {
	if ver >= version179 {
		return
	}
	doReentrantDDL(s, "ALTER TABLE mysql.global_variables MODIFY COLUMN `VARIABLE_VALUE` varchar(16383)")
}

func upgradeToVer190(s sessiontypes.Session, ver int64) {
	if ver >= version190 {
		return
	}
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

func upgradeToVer191(s sessiontypes.Session, ver int64) {
	if ver >= version191 {
		return
	}
	sql := fmt.Sprintf("INSERT HIGH_PRIORITY IGNORE INTO %s.%s VALUES('%s', '%s')",
		mysql.SystemDB, mysql.GlobalVariablesTable,
		vardef.TiDBTxnMode, vardef.OptimisticTxnMode)
	mustExecute(s, sql)
}

func upgradeToVer192(s sessiontypes.Session, ver int64) {
	if ver >= version192 {
		return
	}
	doReentrantDDL(s, CreateRequestUnitByGroupTable)
}

func upgradeToVer193(s sessiontypes.Session, ver int64) {
	if ver >= version193 {
		return
	}
	doReentrantDDL(s, CreateMDLView)
}

func upgradeToVer194(s sessiontypes.Session, ver int64) {
	if ver >= version194 {
		return
	}
	mustExecute(s, "DROP TABLE IF EXISTS mysql.load_data_jobs")
}

func upgradeToVer195(s sessiontypes.Session, ver int64) {
	if ver >= version195 {
		return
	}

	doReentrantDDL(s, DropMySQLIndexUsageTable)
}

func upgradeToVer196(s sessiontypes.Session, ver int64) {
	if ver >= version196 {
		return
	}

	doReentrantDDL(s, "ALTER TABLE mysql.tidb_global_task ADD COLUMN target_scope VARCHAR(256) DEFAULT '' AFTER `step`;", infoschema.ErrColumnExists)
	doReentrantDDL(s, "ALTER TABLE mysql.tidb_global_task_history ADD COLUMN target_scope VARCHAR(256) DEFAULT '' AFTER `step`;", infoschema.ErrColumnExists)
}

func upgradeToVer197(s sessiontypes.Session, ver int64) {
	if ver >= version197 {
		return
	}

	doReentrantDDL(s, CreateMDLView)
}

func upgradeToVer198(s sessiontypes.Session, ver int64) {
	if ver >= version198 {
		return
	}

	doReentrantDDL(s, "ALTER TABLE mysql.tidb_mdl_info ADD COLUMN owner_id VARCHAR(64) NOT NULL DEFAULT '';", infoschema.ErrColumnExists)
}

func upgradeToVer209(s sessiontypes.Session, ver int64) {
	if ver >= version209 {
		return
	}

	initGlobalVariableIfNotExists(s, vardef.TiDBResourceControlStrictMode, vardef.Off)
}

func upgradeToVer210(s sessiontypes.Session, ver int64) {
	if ver >= version210 {
		return
	}

	// Check if tidb_analyze_column_options exists in mysql.GLOBAL_VARIABLES.
	// If not, set tidb_analyze_column_options to ALL since this is the old behavior before we introduce this variable.
	initGlobalVariableIfNotExists(s, vardef.TiDBAnalyzeColumnOptions, ast.AllColumns.String())

	// Check if tidb_opt_projection_push_down exists in mysql.GLOBAL_VARIABLES.
	// If not, set tidb_opt_projection_push_down to Off since this is the old behavior before we introduce this variable.
	initGlobalVariableIfNotExists(s, vardef.TiDBOptProjectionPushDown, vardef.Off)
}

func upgradeToVer211(s sessiontypes.Session, ver int64) {
	if ver >= version211 {
		return
	}
	doReentrantDDL(s, "ALTER TABLE mysql.tidb_background_subtask_history ADD COLUMN `summary` JSON", infoschema.ErrColumnExists)
}

func upgradeToVer212(s sessiontypes.Session, ver int64) {
	if ver >= version212 {
		return
	}
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

func upgradeToVer213(s sessiontypes.Session, ver int64) {
	if ver >= version213 {
		return
	}

	mustExecute(s, CreatePITRIDMap)
}

func upgradeToVer214(s sessiontypes.Session, ver int64) {
	if ver >= version214 {
		return
	}

	mustExecute(s, CreateIndexAdvisorTable)
	mustExecute(s, CreateKernelOptionsTable)
}

func upgradeToVer215(s sessiontypes.Session, ver int64) {
	if ver >= version215 {
		return
	}

	initGlobalVariableIfNotExists(s, vardef.TiDBEnableINLJoinInnerMultiPattern, vardef.Off)
}

func upgradeToVer216(s sessiontypes.Session, ver int64) {
	if ver >= version216 {
		return
	}

	mustExecute(s, "UPDATE mysql.global_variables SET VARIABLE_VALUE='' WHERE VARIABLE_NAME = 'tidb_scatter_region' AND VARIABLE_VALUE = 'OFF'")
	mustExecute(s, "UPDATE mysql.global_variables SET VARIABLE_VALUE='table' WHERE VARIABLE_NAME = 'tidb_scatter_region' AND VARIABLE_VALUE = 'ON'")
}

func upgradeToVer217(s sessiontypes.Session, ver int64) {
	if ver >= version217 {
		return
	}
	// If tidb_schema_cache_size does not exist, insert a record and set the value to 0
	// Otherwise do nothing.
	mustExecute(s, "INSERT IGNORE INTO mysql.global_variables VALUES ('tidb_schema_cache_size', 0)")
}

func upgradeToVer218(_ sessiontypes.Session, ver int64) {
	if ver >= version218 {
		return
	}
	// empty, just make lint happy.
}

func upgradeToVer239(s sessiontypes.Session, ver int64) {
	if ver >= version239 {
		return
	}
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

func upgradeToVer240(s sessiontypes.Session, ver int64) {
	if ver >= version240 {
		return
	}
	doReentrantDDL(s, addAnalyzeJobsSchemaTableStateIndex, dbterror.ErrDupKeyName)
	doReentrantDDL(s, addAnalyzeJobsSchemaTablePartitionStateIndex, dbterror.ErrDupKeyName)
}

func upgradeToVer241(s sessiontypes.Session, ver int64) {
	if ver >= version241 {
		return
	}
	doReentrantDDL(s, "ALTER TABLE mysql.user ADD INDEX i_user (user)", dbterror.ErrDupKeyName)
	doReentrantDDL(s, "ALTER TABLE mysql.global_priv ADD INDEX i_user (user)", dbterror.ErrDupKeyName)
	doReentrantDDL(s, "ALTER TABLE mysql.db ADD INDEX i_user (user)", dbterror.ErrDupKeyName)
	doReentrantDDL(s, "ALTER TABLE mysql.tables_priv ADD INDEX i_user (user)", dbterror.ErrDupKeyName)
	doReentrantDDL(s, "ALTER TABLE mysql.columns_priv ADD INDEX i_user (user)", dbterror.ErrDupKeyName)
	doReentrantDDL(s, "ALTER TABLE mysql.global_grants ADD INDEX i_user (user)", dbterror.ErrDupKeyName)
	doReentrantDDL(s, "ALTER TABLE mysql.default_roles ADD INDEX i_user (user)", dbterror.ErrDupKeyName)
}

// writeClusterID writes cluster id into mysql.tidb
func writeClusterID(s sessiontypes.Session) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(internalSQLTimeout)*time.Second)
	defer cancel()

	clusterID := s.GetDomain().(*domain.Domain).GetPDClient().GetClusterID(ctx)

	mustExecute(s, `INSERT HIGH_PRIORITY INTO %n.%n VALUES (%?, %?, "TiDB Cluster ID.") ON DUPLICATE KEY UPDATE VARIABLE_VALUE= %?`,
		mysql.SystemDB,
		mysql.TiDBTable,
		tidbClusterID,
		clusterID,
		clusterID,
	)
}

func upgradeToVer242(s sessiontypes.Session, ver int64) {
	if ver >= version242 {
		return
	}
	writeClusterID(s)
	mustExecute(s, CreateTiDBWorkloadValuesTable)
}

func upgradeToVer243(s sessiontypes.Session, ver int64) {
	if ver >= version243 {
		return
	}
	doReentrantDDL(s, "ALTER TABLE mysql.tidb_global_task ADD COLUMN max_node_count INT DEFAULT 0 AFTER `modify_params`;", infoschema.ErrColumnExists)
	doReentrantDDL(s, "ALTER TABLE mysql.tidb_global_task_history ADD COLUMN max_node_count INT DEFAULT 0 AFTER `modify_params`;", infoschema.ErrColumnExists)
	doReentrantDDL(s, "ALTER TABLE mysql.tidb_global_task ADD COLUMN extra_params json AFTER max_node_count;", infoschema.ErrColumnExists)
	doReentrantDDL(s, "ALTER TABLE mysql.tidb_global_task_history ADD COLUMN extra_params json AFTER max_node_count;", infoschema.ErrColumnExists)
}

func upgradeToVer244(s sessiontypes.Session, ver int64) {
	if ver >= version244 {
		return
	}

	doReentrantDDL(s, "ALTER TABLE mysql.user ADD COLUMN IF NOT EXISTS `Max_user_connections` INT UNSIGNED NOT NULL DEFAULT 0 AFTER `Password_lifetime`")
}

func upgradeToVer245(s sessiontypes.Session, ver int64) {
	if ver >= version245 {
		return
	}
	doReentrantDDL(s, "ALTER TABLE mysql.bind_info MODIFY COLUMN original_sql LONGTEXT NOT NULL")
	doReentrantDDL(s, "ALTER TABLE mysql.bind_info MODIFY COLUMN bind_sql LONGTEXT NOT NULL")
}

func upgradeToVer246(s sessiontypes.Session, ver int64) {
	if ver >= version246 {
		return
	}

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

func upgradeToVer247(s sessiontypes.Session, ver int64) {
	if ver >= version247 {
		return
	}
	doReentrantDDL(s, "ALTER TABLE mysql.stats_meta ADD COLUMN last_stats_histograms_version bigint unsigned DEFAULT NULL", infoschema.ErrColumnExists)
}

func upgradeToVer248(s sessiontypes.Session, ver int64) {
	if ver >= version248 {
		return
	}
	doReentrantDDL(s, "ALTER TABLE mysql.tidb_pitr_id_map ADD COLUMN restore_id BIGINT NOT NULL DEFAULT 0", infoschema.ErrColumnExists)
	doReentrantDDL(s, "ALTER TABLE mysql.tidb_pitr_id_map DROP PRIMARY KEY")
	doReentrantDDL(s, "ALTER TABLE mysql.tidb_pitr_id_map ADD PRIMARY KEY(restore_id, restored_ts, upstream_cluster_id, segment_id)")
}

func upgradeToVer249(s sessiontypes.Session, ver int64) {
	if ver >= version249 {
		return
	}
	doReentrantDDL(s, CreateRestoreRegistryTable)
}

// initGlobalVariableIfNotExists initialize a global variable with specific val if it does not exist.
func initGlobalVariableIfNotExists(s sessiontypes.Session, name string, val any) {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnBootstrap)
	rs, err := s.ExecuteInternal(ctx, "SELECT VARIABLE_VALUE FROM %n.%n WHERE VARIABLE_NAME=%?;",
		mysql.SystemDB, mysql.GlobalVariablesTable, name)
	terror.MustNil(err)
	req := rs.NewChunk(nil)
	err = rs.Next(ctx, req)
	terror.MustNil(err)
	if req.NumRows() != 0 {
		return
	}

	mustExecute(s, "INSERT HIGH_PRIORITY IGNORE INTO %n.%n VALUES (%?, %?);",
		mysql.SystemDB, mysql.GlobalVariablesTable, name, val)
}

func writeOOMAction(s sessiontypes.Session) {
	comment := "oom-action is `log` by default in v3.0.x, `cancel` by default in v4.0.11+"
	mustExecute(s, `INSERT HIGH_PRIORITY INTO %n.%n VALUES (%?, %?, %?) ON DUPLICATE KEY UPDATE VARIABLE_VALUE= %?`,
		mysql.SystemDB, mysql.TiDBTable, tidbDefOOMAction, vardef.OOMActionLog, comment, vardef.OOMActionLog,
	)
}

// updateBootstrapVer updates bootstrap version variable in mysql.TiDB table.
func updateBootstrapVer(s sessiontypes.Session) {
	// Update bootstrap version.
	mustExecute(s, `INSERT HIGH_PRIORITY INTO %n.%n VALUES (%?, %?, "TiDB bootstrap version.") ON DUPLICATE KEY UPDATE VARIABLE_VALUE=%?`,
		mysql.SystemDB, mysql.TiDBTable, tidbServerVersionVar, currentBootstrapVersion, currentBootstrapVersion,
	)
}

// getBootstrapVersion gets bootstrap version from mysql.tidb table;
func getBootstrapVersion(s sessiontypes.Session) (int64, error) {
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
func doDDLWorks(s sessiontypes.Session) {
	// Create a test database.
	mustExecute(s, "CREATE DATABASE IF NOT EXISTS test")
	// Create system db.
	mustExecute(s, "CREATE DATABASE IF NOT EXISTS %n", mysql.SystemDB)
	// Create user table.
	mustExecute(s, CreateUserTable)
	// Create password history.
	mustExecute(s, CreatePasswordHistory)
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
	// NOTE: Feedback is deprecated, but we still need to create this table for compatibility.
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
	// Create mdl view.
	mustExecute(s, CreateMDLView)
	// Create plan_replayer_status table
	mustExecute(s, CreatePlanReplayerStatusTable)
	// Create plan_replayer_task table
	mustExecute(s, CreatePlanReplayerTaskTable)
	// Create stats_meta_table_locked table
	mustExecute(s, CreateStatsTableLocked)
	// Create tidb_ttl_table_status table
	mustExecute(s, CreateTTLTableStatus)
	// Create tidb_ttl_task table
	mustExecute(s, CreateTTLTask)
	// Create tidb_ttl_job_history table
	mustExecute(s, CreateTTLJobHistory)
	// Create tidb_global_task table
	mustExecute(s, CreateGlobalTask)
	// Create tidb_global_task_history table
	mustExecute(s, CreateGlobalTaskHistory)
	// Create tidb_import_jobs
	mustExecute(s, CreateImportJobs)
	// create runaway_watch
	mustExecute(s, CreateRunawayWatchTable)
	// create runaway_queries
	mustExecute(s, CreateRunawayTable)
	// create tidb_timers
	mustExecute(s, CreateTimers)
	// create runaway_watch done
	mustExecute(s, CreateDoneRunawayWatchTable)
	// create dist_framework_meta
	mustExecute(s, CreateDistFrameworkMeta)
	// create request_unit_by_group
	mustExecute(s, CreateRequestUnitByGroupTable)
	// create tidb_pitr_id_map
	mustExecute(s, CreatePITRIDMap)
	// create tidb_restore_registry
	mustExecute(s, CreateRestoreRegistryTable)
	// create `sys` schema
	mustExecute(s, CreateSysSchema)
	// create `sys.schema_unused_indexes` view
	mustExecute(s, CreateSchemaUnusedIndexesView)
	// create mysql.index_advisor_results
	mustExecute(s, CreateIndexAdvisorTable)
	// create mysql.tidb_kernel_options
	mustExecute(s, CreateKernelOptionsTable)
	// create mysql.tidb_workload_values
	mustExecute(s, CreateTiDBWorkloadValuesTable)
}

// doBootstrapSQLFile executes SQL commands in a file as the last stage of bootstrap.
// It is useful for setting the initial value of GLOBAL variables.
func doBootstrapSQLFile(s sessiontypes.Session) error {
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
func doDMLWorks(s sessiontypes.Session) {
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

func mustExecute(s sessiontypes.Session, sql string, args ...any) {
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
