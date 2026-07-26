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

//! `pkg/meta/metadef/system_tables_def.go`: the `CREATE TABLE` statements
//! for TiDB's system tables. These are verbatim transcreations of the Go
//! string constants (extracted by script for byte fidelity); the Rust name
//! is the Go identifier in SCREAMING_SNAKE_CASE.

/// Go `CreateUserTable`.
pub const CREATE_USER_TABLE: &str = r#"CREATE TABLE IF NOT EXISTS mysql.user (
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
		KEY i_user (User));"#;

/// Go `CreateGlobalPrivTable`.
pub const CREATE_GLOBAL_PRIV_TABLE: &str = r#"CREATE TABLE IF NOT EXISTS mysql.global_priv (
		Host CHAR(255) NOT NULL DEFAULT '',
		User CHAR(80) NOT NULL DEFAULT '',
		Priv LONGTEXT NOT NULL DEFAULT '',
		PRIMARY KEY (Host, User),
		KEY i_user (User))"#;

/// Go `CreateDBTable`.
pub const CREATE_DBTABLE: &str = r#"CREATE TABLE IF NOT EXISTS mysql.db (
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
		KEY i_user (User));"#;

/// Go `CreateTablesPrivTable`.
pub const CREATE_TABLES_PRIV_TABLE: &str = r#"CREATE TABLE IF NOT EXISTS mysql.tables_priv (
		Host		CHAR(255),
		DB			CHAR(64) CHARSET utf8mb4 COLLATE utf8mb4_general_ci,
		User		CHAR(32),
		Table_name	CHAR(64) CHARSET utf8mb4 COLLATE utf8mb4_general_ci,
		Grantor		CHAR(77),
		Timestamp	TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		Table_priv	SET('Select','Insert','Update','Delete','Create','Drop','Grant','Index','Alter','Create View','Show View','Trigger','References'),
		Column_priv	SET('Select','Insert','Update','References'),
		PRIMARY KEY (Host, DB, User, Table_name),
		KEY i_user (User));"#;

/// Go `CreateColumnsPrivTable`.
pub const CREATE_COLUMNS_PRIV_TABLE: &str = r#"CREATE TABLE IF NOT EXISTS mysql.columns_priv (
		Host		CHAR(255),
		DB			CHAR(64) CHARSET utf8mb4 COLLATE utf8mb4_general_ci,
		User		CHAR(32),
		Table_name	CHAR(64) CHARSET utf8mb4 COLLATE utf8mb4_general_ci,
		Column_name	CHAR(64) CHARSET utf8mb4 COLLATE utf8mb4_general_ci,
		Timestamp	TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		Column_priv	SET('Select','Insert','Update','References'),
		PRIMARY KEY (Host, DB, User, Table_name, Column_name),
		KEY i_user (User));"#;

/// Go `CreateGlobalVariablesTable`.
pub const CREATE_GLOBAL_VARIABLES_TABLE: &str = r#"CREATE TABLE IF NOT EXISTS mysql.global_variables (
		VARIABLE_NAME  VARCHAR(64) NOT NULL PRIMARY KEY,
		VARIABLE_VALUE VARCHAR(16383) DEFAULT NULL);"#;

/// Go `CreateTiDBTable`.
pub const CREATE_TI_DBTABLE: &str = r#"CREATE TABLE IF NOT EXISTS mysql.tidb (
		VARIABLE_NAME  	VARCHAR(64) NOT NULL PRIMARY KEY,
		VARIABLE_VALUE 	VARCHAR(1024) DEFAULT NULL,
		COMMENT 		VARCHAR(1024));"#;

/// Go `CreateHelpTopicTable`.
pub const CREATE_HELP_TOPIC_TABLE: &str = r#"CREATE TABLE IF NOT EXISTS mysql.help_topic (
		help_topic_id 		INT(10) UNSIGNED NOT NULL,
		name 				CHAR(64) NOT NULL,
		help_category_id 	SMALLINT(5) UNSIGNED NOT NULL,
		description 		TEXT NOT NULL,
		example 			TEXT NOT NULL,
		url 				TEXT NOT NULL,
		PRIMARY KEY (help_topic_id) clustered,
		UNIQUE KEY name (name)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8 STATS_PERSISTENT=0 COMMENT='help topics';"#;

/// Go `CreateStatsMetaTable`.
pub const CREATE_STATS_META_TABLE: &str = r#"CREATE TABLE IF NOT EXISTS mysql.stats_meta (
		version 					BIGINT(64) UNSIGNED NOT NULL,
		table_id 					BIGINT(64) NOT NULL,
		modify_count				BIGINT(64) NOT NULL DEFAULT 0,
		count 						BIGINT(64) UNSIGNED NOT NULL DEFAULT 0,
		snapshot        			BIGINT(64) UNSIGNED NOT NULL DEFAULT 0,
		last_stats_histograms_version 	BIGINT(64) UNSIGNED DEFAULT NULL,
		PRIMARY KEY (table_id) CLUSTERED,
		INDEX idx_ver(version)
	);"#;

/// Go `CreateStatsHistogramsTable`.
pub const CREATE_STATS_HISTOGRAMS_TABLE: &str = r#"CREATE TABLE IF NOT EXISTS mysql.stats_histograms (
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
		PRIMARY KEY (table_id, is_index, hist_id) CLUSTERED
	);"#;

/// Go `CreateStatsBucketsTable`.
pub const CREATE_STATS_BUCKETS_TABLE: &str = r#"CREATE TABLE IF NOT EXISTS mysql.stats_buckets (
		table_id 	BIGINT(64) NOT NULL,
		is_index 	TINYINT(2) NOT NULL,
		hist_id 	BIGINT(64) NOT NULL,
		bucket_id 	BIGINT(64) NOT NULL,
		count 		BIGINT(64) NOT NULL,
		repeats 	BIGINT(64) NOT NULL,
		upper_bound LONGBLOB NOT NULL,
		lower_bound LONGBLOB ,
		ndv         BIGINT NOT NULL DEFAULT 0,
		PRIMARY KEY (table_id, is_index, hist_id, bucket_id) CLUSTERED
	);"#;

/// Go `CreateGCDeleteRangeTable`.
pub const CREATE_GCDELETE_RANGE_TABLE: &str = r#"CREATE TABLE IF NOT EXISTS mysql.gc_delete_range (
		job_id 		BIGINT NOT NULL COMMENT "the DDL job ID",
		element_id 	BIGINT NOT NULL COMMENT "the schema element ID",
		start_key 	VARCHAR(255) NOT NULL COMMENT "encoded in hex",
		end_key 	VARCHAR(255) NOT NULL COMMENT "encoded in hex",
		ts 			BIGINT NOT NULL COMMENT "timestamp in uint64",
		UNIQUE KEY delete_range_index (job_id, element_id)
	);"#;

/// Go `CreateGCDeleteRangeDoneTable`.
pub const CREATE_GCDELETE_RANGE_DONE_TABLE: &str = r#"CREATE TABLE IF NOT EXISTS mysql.gc_delete_range_done (
		job_id 		BIGINT NOT NULL COMMENT "the DDL job ID",
		element_id 	BIGINT NOT NULL COMMENT "the schema element ID",
		start_key 	VARCHAR(255) NOT NULL COMMENT "encoded in hex",
		end_key 	VARCHAR(255) NOT NULL COMMENT "encoded in hex",
		ts 			BIGINT NOT NULL COMMENT "timestamp in uint64",
		UNIQUE KEY delete_range_done_index (job_id, element_id)
	);"#;

/// Go `CreateStatsFeedbackTable`.
pub const CREATE_STATS_FEEDBACK_TABLE: &str = r#"CREATE TABLE IF NOT EXISTS mysql.stats_feedback (
		table_id 	BIGINT(64) NOT NULL,
		is_index 	TINYINT(2) NOT NULL,
		hist_id 	BIGINT(64) NOT NULL,
		feedback 	BLOB NOT NULL,
		INDEX hist(table_id, is_index, hist_id)
	);"#;

/// Go `CreateBindInfoTable`.
pub const CREATE_BIND_INFO_TABLE: &str = r#"CREATE TABLE IF NOT EXISTS mysql.bind_info (
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
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;"#;

/// Go `CreateRoleEdgesTable`.
pub const CREATE_ROLE_EDGES_TABLE: &str = r#"CREATE TABLE IF NOT EXISTS mysql.role_edges (
		FROM_HOST 			CHAR(60) COLLATE utf8_bin NOT NULL DEFAULT '',
		FROM_USER 			CHAR(32) COLLATE utf8_bin NOT NULL DEFAULT '',
		TO_HOST 			CHAR(60) COLLATE utf8_bin NOT NULL DEFAULT '',
		TO_USER 			CHAR(32) COLLATE utf8_bin NOT NULL DEFAULT '',
		WITH_ADMIN_OPTION 	ENUM('N','Y') CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'N',
		PRIMARY KEY (FROM_HOST,FROM_USER,TO_HOST,TO_USER)
	);"#;

/// Go `CreateDefaultRolesTable`.
pub const CREATE_DEFAULT_ROLES_TABLE: &str = r#"CREATE TABLE IF NOT EXISTS mysql.default_roles (
		HOST 				CHAR(60) COLLATE utf8_bin NOT NULL DEFAULT '',
		USER 				CHAR(32) COLLATE utf8_bin NOT NULL DEFAULT '',
		DEFAULT_ROLE_HOST 	CHAR(60) COLLATE utf8_bin NOT NULL DEFAULT '%',
		DEFAULT_ROLE_USER 	CHAR(32) COLLATE utf8_bin NOT NULL DEFAULT '',
		PRIMARY KEY (HOST,USER,DEFAULT_ROLE_HOST,DEFAULT_ROLE_USER),
		KEY i_user (USER))"#;

/// Go `CreateStatsTopNTable`.
pub const CREATE_STATS_TOP_NTABLE: &str = r#"CREATE TABLE IF NOT EXISTS mysql.stats_top_n (
		table_id 	BIGINT(64) NOT NULL,
		is_index 	TINYINT(2) NOT NULL,
		hist_id 	BIGINT(64) NOT NULL,
		value 		LONGBLOB,
		count 		BIGINT(64) UNSIGNED NOT NULL,
		INDEX tbl(table_id, is_index, hist_id)
	);"#;

/// Go `CreateStatsFMSketchTable`.
pub const CREATE_STATS_FMSKETCH_TABLE: &str = r#"CREATE TABLE IF NOT EXISTS mysql.stats_fm_sketch (
		table_id 	BIGINT(64) NOT NULL,
		is_index 	TINYINT(2) NOT NULL,
		hist_id 	BIGINT(64) NOT NULL,
		value 		LONGBLOB,
		PRIMARY KEY (table_id, is_index, hist_id) CLUSTERED
	);"#;

/// Go `CreateExprPushdownBlacklistTable`.
pub const CREATE_EXPR_PUSHDOWN_BLACKLIST_TABLE: &str = r#"CREATE TABLE IF NOT EXISTS mysql.expr_pushdown_blacklist (
		name 		CHAR(100) NOT NULL,
		store_type 	CHAR(100) NOT NULL DEFAULT 'tikv,tiflash,tidb',
		reason 		VARCHAR(200)
	);"#;

/// Go `CreateOptRuleBlacklistTable`.
pub const CREATE_OPT_RULE_BLACKLIST_TABLE: &str = r#"CREATE TABLE IF NOT EXISTS mysql.opt_rule_blacklist (
		name 	CHAR(100) NOT NULL
	);"#;

/// Go `CreateStatsExtendedTable`.
pub const CREATE_STATS_EXTENDED_TABLE: &str = r#"CREATE TABLE IF NOT EXISTS mysql.stats_extended (
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
	);"#;

/// Go `CreateSchemaIndexUsageTable`.
pub const CREATE_SCHEMA_INDEX_USAGE_TABLE: &str = r#"CREATE TABLE IF NOT EXISTS mysql.schema_index_usage (
		TABLE_ID bigint(64),
		INDEX_ID bigint(21),
		QUERY_COUNT bigint(64),
		ROWS_SELECTED bigint(64),
		LAST_USED_AT timestamp,
		PRIMARY KEY(TABLE_ID, INDEX_ID)
	);"#;

/// Go `CreateGlobalGrantsTable`.
pub const CREATE_GLOBAL_GRANTS_TABLE: &str = r#"CREATE TABLE IF NOT EXISTS mysql.global_grants (
		USER char(32) NOT NULL DEFAULT '',
		HOST char(255) NOT NULL DEFAULT '',
		PRIV char(32) NOT NULL DEFAULT '',
		WITH_GRANT_OPTION enum('N','Y') NOT NULL DEFAULT 'N',
		PRIMARY KEY (USER,HOST,PRIV),
		KEY i_user (USER)
	);"#;

/// Go `CreateCapturePlanBaselinesBlacklistTable`.
pub const CREATE_CAPTURE_PLAN_BASELINES_BLACKLIST_TABLE: &str = r#"CREATE TABLE IF NOT EXISTS mysql.capture_plan_baselines_blacklist (
		id bigint(64) auto_increment,
		filter_type varchar(32) NOT NULL COMMENT "type of the filter, only db, table and frequency supported now",
		filter_value varchar(32) NOT NULL,
		key idx(filter_type),
		primary key(id)
	);"#;

/// Go `CreateColumnStatsUsageTable`.
pub const CREATE_COLUMN_STATS_USAGE_TABLE: &str = r#"CREATE TABLE IF NOT EXISTS mysql.column_stats_usage (
		table_id BIGINT(64) NOT NULL,
		column_id BIGINT(64) NOT NULL,
		last_used_at TIMESTAMP,
		last_analyzed_at TIMESTAMP,
		PRIMARY KEY (table_id, column_id) CLUSTERED
	);"#;

/// Go `CreateTableCacheMetaTable`.
pub const CREATE_TABLE_CACHE_META_TABLE: &str = r#"CREATE TABLE IF NOT EXISTS mysql.table_cache_meta (
		tid bigint(11) NOT NULL DEFAULT 0,
		lock_type enum('NONE','READ', 'INTEND', 'WRITE') NOT NULL DEFAULT 'NONE',
		lease bigint(20) NOT NULL DEFAULT 0,
		oldReadLease bigint(20) NOT NULL DEFAULT 0,
		PRIMARY KEY (tid)
	);"#;

/// Go `CreateAnalyzeOptionsTable`.
pub const CREATE_ANALYZE_OPTIONS_TABLE: &str = r#"CREATE TABLE IF NOT EXISTS mysql.analyze_options (
		table_id BIGINT(64) NOT NULL,
		sample_num BIGINT(64) NOT NULL DEFAULT 0,
		sample_rate DOUBLE NOT NULL DEFAULT -1,
		buckets BIGINT(64) NOT NULL DEFAULT 0,
		topn BIGINT(64) NOT NULL DEFAULT -1,
		column_choice enum('DEFAULT','ALL','PREDICATE','LIST') NOT NULL DEFAULT 'DEFAULT',
		column_ids TEXT(19372),
		PRIMARY KEY (table_id) CLUSTERED
	);"#;

/// Go `CreateStatsHistoryTable`.
pub const CREATE_STATS_HISTORY_TABLE: &str = r#"CREATE TABLE IF NOT EXISTS mysql.stats_history (
		table_id bigint(64) NOT NULL,
		stats_data longblob NOT NULL,
		seq_no bigint(64) NOT NULL comment 'sequence number of the gzipped data slice',
		version bigint(64) NOT NULL comment 'stats version which corresponding to stats:version in EXPLAIN',
		create_time datetime(6) NOT NULL,
		PRIMARY KEY (table_id, version, seq_no) CLUSTERED,
		KEY table_create_time (table_id, create_time, seq_no),
    	KEY idx_create_time (create_time)
	);"#;

/// Go `CreateStatsMetaHistoryTable`.
pub const CREATE_STATS_META_HISTORY_TABLE: &str = r#"CREATE TABLE IF NOT EXISTS mysql.stats_meta_history (
		table_id bigint(64) NOT NULL,
		modify_count bigint(64) NOT NULL,
		count bigint(64) NOT NULL,
		version bigint(64) NOT NULL comment 'stats version which corresponding to stats:version in EXPLAIN',
    	source varchar(40) NOT NULL,
		create_time datetime(6) NOT NULL,
		PRIMARY KEY (table_id, version) CLUSTERED,
		KEY table_create_time (table_id, create_time),
    	KEY idx_create_time (create_time)
	);"#;

/// Go `CreateAnalyzeJobsTable`.
pub const CREATE_ANALYZE_JOBS_TABLE: &str = r#"CREATE TABLE IF NOT EXISTS mysql.analyze_jobs (
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
	);"#;

/// Go `CreateAdvisoryLocksTable`.
pub const CREATE_ADVISORY_LOCKS_TABLE: &str = r#"CREATE TABLE IF NOT EXISTS mysql.advisory_locks (
		lock_name VARCHAR(64) NOT NULL PRIMARY KEY
	);"#;

/// Go `CreateTiDBMDLView`.
pub const CREATE_TI_DBMDLVIEW: &str = r#"CREATE OR REPLACE SQL SECURITY INVOKER VIEW mysql.tidb_mdl_view as (
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
	);"#;

/// Go `CreatePlanReplayerStatusTable`.
pub const CREATE_PLAN_REPLAYER_STATUS_TABLE: &str = r#"CREATE TABLE IF NOT EXISTS mysql.plan_replayer_status (
		sql_digest VARCHAR(128),
		plan_digest VARCHAR(128),
		origin_sql TEXT,
		token VARCHAR(128),
		update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
		fail_reason TEXT,
		instance VARCHAR(512) NOT NULL comment 'address of the TiDB instance executing the plan replayer job');"#;

/// Go `CreatePlanReplayerTaskTable`.
pub const CREATE_PLAN_REPLAYER_TASK_TABLE: &str = r#"CREATE TABLE IF NOT EXISTS mysql.plan_replayer_task (
		sql_digest VARCHAR(128) NOT NULL,
		plan_digest VARCHAR(128) NOT NULL,
		update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
		PRIMARY KEY (sql_digest,plan_digest));"#;

/// Go `CreateStatsTableLockedTable`.
pub const CREATE_STATS_TABLE_LOCKED_TABLE: &str = r#"CREATE TABLE IF NOT EXISTS mysql.stats_table_locked (
		table_id bigint(64) NOT NULL,
		modify_count bigint(64) NOT NULL DEFAULT 0,
		count bigint(64) NOT NULL DEFAULT 0,
		version bigint(64) UNSIGNED NOT NULL DEFAULT 0,
		PRIMARY KEY (table_id) CLUSTERED);"#;

/// Go `CreatePasswordHistoryTable`.
pub const CREATE_PASSWORD_HISTORY_TABLE: &str = r#"CREATE TABLE  IF NOT EXISTS mysql.password_history (
         Host char(255)  NOT NULL DEFAULT '',
         User char(32)  NOT NULL DEFAULT '',
         Password_timestamp timestamp(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
         Password text,
         PRIMARY KEY (Host,User,Password_timestamp )
        ) COMMENT='Password history for user accounts' "#;

/// Go `CreateTiDBTTLTableStatusTable`.
pub const CREATE_TI_DBTTLTABLE_STATUS_TABLE: &str = r#"CREATE TABLE IF NOT EXISTS mysql.tidb_ttl_table_status (
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
  		current_job_status_update_time timestamp NULL DEFAULT NULL);"#;

/// Go `CreateTiDBTTLTaskTable`.
pub const CREATE_TI_DBTTLTASK_TABLE: &str = r#"CREATE TABLE IF NOT EXISTS mysql.tidb_ttl_task (
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
		key(created_time));"#;

/// Go `CreateTiDBTTLJobHistoryTable`.
pub const CREATE_TI_DBTTLJOB_HISTORY_TABLE: &str = r#"CREATE TABLE IF NOT EXISTS mysql.tidb_ttl_job_history (
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
	);"#;

/// Go `CreateTiDBGlobalTaskTable`.
pub const CREATE_TI_DBGLOBAL_TASK_TABLE: &str = r#"CREATE TABLE IF NOT EXISTS mysql.tidb_global_task (
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
	);"#;

/// Go `CreateTiDBGlobalTaskHistoryTable`.
pub const CREATE_TI_DBGLOBAL_TASK_HISTORY_TABLE: &str = r#"CREATE TABLE IF NOT EXISTS mysql.tidb_global_task_history (
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
	);"#;

/// Go `CreateDistFrameworkMetaTable`.
pub const CREATE_DIST_FRAMEWORK_META_TABLE: &str = r#"CREATE TABLE IF NOT EXISTS mysql.dist_framework_meta (
        host VARCHAR(261) NOT NULL PRIMARY KEY,
        role VARCHAR(64),
        cpu_count int default 0,
        keyspace_id bigint(8) NOT NULL DEFAULT -1
    );"#;

/// Go `CreateTiDBRunawayQueriesTable`.
pub const CREATE_TI_DBRUNAWAY_QUERIES_TABLE: &str = r#"CREATE TABLE IF NOT EXISTS mysql.tidb_runaway_queries (
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
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;"#;

/// Go `CreateTiDBTimersTable`.
pub const CREATE_TI_DBTIMERS_TABLE: &str = r#"CREATE TABLE IF NOT EXISTS mysql.tidb_timers (
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
	)"#;

/// Go `CreateTiDBRunawayWatchTable`.
pub const CREATE_TI_DBRUNAWAY_WATCH_TABLE: &str = r#"CREATE TABLE IF NOT EXISTS mysql.tidb_runaway_watch (
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
		INDEX time_index(end_time) COMMENT "accelerate the speed when querying with active watch",
		INDEX idx_start_time(start_time) COMMENT "accelerate the speed when syncing new watch records"
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;"#;

/// Go `CreateTiDBRunawayWatchDoneTable`.
pub const CREATE_TI_DBRUNAWAY_WATCH_DONE_TABLE: &str = r#"CREATE TABLE IF NOT EXISTS mysql.tidb_runaway_watch_done (
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
		done_time TIMESTAMP(6) NOT NULL,
		INDEX idx_done_time(done_time) COMMENT "accelerate the speed when syncing done watch records"
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;"#;

/// Go `CreateRequestUnitByGroupTable`.
pub const CREATE_REQUEST_UNIT_BY_GROUP_TABLE: &str = r#"CREATE TABLE IF NOT EXISTS mysql.request_unit_by_group (
		start_time TIMESTAMP(6) NOT NULL,
		end_time TIMESTAMP(6) NOT NULL,
		resource_group VARCHAR(32) NOT null,
		total_ru bigint(64) UNSIGNED NOT NULL,
		PRIMARY KEY (start_time, end_time, resource_group),
		KEY (resource_group)
	);"#;

/// Go `CreateTiDBImportJobsTable`.
pub const CREATE_TI_DBIMPORT_JOBS_TABLE: &str = r#"CREATE TABLE IF NOT EXISTS mysql.tidb_import_jobs (
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
		KEY (status));"#;

/// Go `CreateTiDBPITRIDMapTable`.
pub const CREATE_TI_DBPITRIDMAP_TABLE: &str = r#"CREATE TABLE IF NOT EXISTS mysql.tidb_pitr_id_map (
		restore_id BIGINT NOT NULL DEFAULT 0,
		restored_ts BIGINT NOT NULL,
		upstream_cluster_id BIGINT NOT NULL,
		segment_id BIGINT NOT NULL,
		id_map BLOB(524288) NOT NULL,
		update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		PRIMARY KEY (restore_id, restored_ts, upstream_cluster_id, segment_id));"#;

/// Go `CreateTiDBRestoreRegistryTable`.
pub const CREATE_TI_DBRESTORE_REGISTRY_TABLE: &str = r#"CREATE TABLE IF NOT EXISTS mysql.tidb_restore_registry (
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
	) AUTO_INCREMENT = 1;"#;

/// Go `CreateSchemaUnusedIndexesView`.
pub const CREATE_SCHEMA_UNUSED_INDEXES_VIEW: &str = r#"CREATE OR REPLACE VIEW sys.schema_unused_indexes AS
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
			sum(last_access_time) is null;"#;

/// Go `CreateIndexAdvisorResultsTable`.
pub const CREATE_INDEX_ADVISOR_RESULTS_TABLE: &str = r#"CREATE TABLE IF NOT EXISTS mysql.index_advisor_results (
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
       unique index idx(schema_name, table_name, index_columns));"#;

/// Go `CreateTiDBKernelOptionsTable`.
pub const CREATE_TI_DBKERNEL_OPTIONS_TABLE: &str = r#"CREATE TABLE IF NOT EXISTS mysql.tidb_kernel_options (
        module varchar(128),
        name varchar(128),
        value varchar(128),
        updated_at datetime,
        status varchar(128),
        description text,
        primary key(module, name));"#;

/// Go `CreateTiDBWorkloadValuesTable`.
pub const CREATE_TI_DBWORKLOAD_VALUES_TABLE: &str = r#"CREATE TABLE IF NOT EXISTS mysql.tidb_workload_values (
		id bigint(20) NOT NULL AUTO_INCREMENT PRIMARY KEY,
		version bigint(20) NOT NULL,
		category varchar(64) NOT NULL,
		type varchar(64) NOT NULL,
		table_id bigint(20) NOT NULL,
		value json NOT NULL,
		index idx_version_category_type (version, category, type),
		index idx_table_id (table_id));"#;

/// Go `CreateTiDBMaskingPolicyTable`.
pub const CREATE_TI_DBMASKING_POLICY_TABLE: &str = r#"CREATE TABLE IF NOT EXISTS mysql.tidb_masking_policy (
		policy_id BIGINT(64) NOT NULL AUTO_INCREMENT,
		policy_name VARCHAR(64) NOT NULL,
		db_name VARCHAR(64) NOT NULL,
		table_name VARCHAR(64) NOT NULL,
		table_id BIGINT(64) NOT NULL,
		column_name VARCHAR(64) NOT NULL,
		column_id BIGINT(64) NOT NULL,
		expression TEXT NOT NULL,
		status VARCHAR(16) NOT NULL,
		masking_type VARCHAR(32) NOT NULL,
		restrict_on VARCHAR(256) NOT NULL DEFAULT 'NONE',
		created_at DATETIME(6) NOT NULL,
		updated_at DATETIME(6) NOT NULL,
		created_by VARCHAR(288) NOT NULL DEFAULT '',
		PRIMARY KEY(policy_id),
		UNIQUE KEY uk_table_policy(table_id, policy_name),
		UNIQUE KEY uk_table_column(table_id, column_id)
		);"#;

/// Go `CreateTiDBDDLJobTable`.
pub const CREATE_TI_DBDDLJOB_TABLE: &str = r#"create table mysql.tidb_ddl_job (
		job_id bigint not null,
		reorg int,
		schema_ids text(65535),
		table_ids text(65535),
		job_meta longblob,
		type int,
		processing int,
		primary key(job_id))"#;

/// Go `CreateTiDBReorgTable`.
pub const CREATE_TI_DBREORG_TABLE: &str = r#"create table mysql.tidb_ddl_reorg (
		job_id bigint not null,
		ele_id bigint,
		ele_type blob,
		start_key blob,
		end_key blob,
		physical_id bigint,
		reorg_meta longblob,
		unique key(job_id, ele_id, ele_type(20)))"#;

/// Go `CreateTiDBDDLHistoryTable`.
pub const CREATE_TI_DBDDLHISTORY_TABLE: &str = r#"create table mysql.tidb_ddl_history (
		job_id bigint not null,
		job_meta longblob,
		db_name char(64),
		table_name char(64),
		schema_ids text(65535),
		table_ids text(65535),
		create_time datetime,
		primary key(job_id))"#;

/// Go `CreateTiDBMDLTable`.
pub const CREATE_TI_DBMDLTABLE: &str = r#"create table mysql.tidb_mdl_info (
		job_id BIGINT NOT NULL PRIMARY KEY,
		version BIGINT NOT NULL,
		table_ids text(65535),
		owner_id varchar(64) NOT NULL DEFAULT ''
	);"#;

/// Go `CreateTiDBBackgroundSubtaskTable`.
pub const CREATE_TI_DBBACKGROUND_SUBTASK_TABLE: &str = r#"create table mysql.tidb_background_subtask (
		id bigint not null auto_increment primary key,
		step int,
		namespace varchar(256),
		task_key varchar(256),
		ddl_physical_tid bigint(20),
		type int,
		exec_id varchar(261),
		exec_expired timestamp,
		state varchar(64) not null,
		checkpoint longblob not null,
		concurrency int,
		create_time timestamp,
		start_time bigint,
		state_update_time bigint,
		end_time TIMESTAMP,
		meta longblob,
		ordinal int,
		error BLOB,
		summary json,
		key idx_task_key(task_key),
		key idx_exec_id(exec_id),
		unique uk_task_key_step_ordinal(task_key, step, ordinal)
	)"#;

/// Go `CreateTiDBBackgroundSubtaskHistoryTable`.
pub const CREATE_TI_DBBACKGROUND_SUBTASK_HISTORY_TABLE: &str = r#"create table mysql.tidb_background_subtask_history (
	 	id bigint not null auto_increment primary key,
		step int,
		namespace varchar(256),
		task_key varchar(256),
		ddl_physical_tid bigint(20),
		type int,
		exec_id varchar(261),
		exec_expired timestamp,
		state varchar(64) not null,
		checkpoint longblob not null,
		concurrency int,
		create_time timestamp,
		start_time bigint,
		state_update_time bigint,
		end_time TIMESTAMP,
		meta longblob,
		ordinal int,
		error BLOB,
		summary json,
		key idx_task_key(task_key),
		key idx_state_update_time(state_update_time))"#;

/// Go `CreateTiDBDDLNotifierTable`.
pub const CREATE_TI_DBDDLNOTIFIER_TABLE: &str = r#"CREATE TABLE mysql.tidb_ddl_notifier (
		ddl_job_id BIGINT,
		sub_job_id BIGINT COMMENT '-1 if the schema change does not belong to a multi-schema change DDL or a merged DDL. 0 or positive numbers representing the sub-job index of a multi-schema change DDL or a merged DDL',
		schema_change LONGBLOB COMMENT 'SchemaChangeEvent at rest',
		processed_by_flag BIGINT UNSIGNED DEFAULT 0 COMMENT 'flag to mark which subscriber has processed the event',
		PRIMARY KEY(ddl_job_id, sub_job_id))"#;
