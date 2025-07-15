// Copyright 2022 PingCAP, Inc.
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

package ddl

const (
	// JobTableSQL is the CREATE TABLE SQL of `tidb_ddl_job`.
	JobTableSQL = `create table mysql.tidb_ddl_job (
		job_id bigint not null,
		reorg int,
		schema_ids text(65535),
		table_ids text(65535),
		job_meta longblob,
		type int,
		processing int,
		primary key(job_id))`
	// ReorgTableSQL is the CREATE TABLE SQL of `tidb_ddl_reorg`.
	ReorgTableSQL = `create table mysql.tidb_ddl_reorg (
		job_id bigint not null,
		ele_id bigint,
		ele_type blob,
		start_key blob,
		end_key blob,
		physical_id bigint,
		reorg_meta longblob,
		unique key(job_id, ele_id, ele_type(20)))`
	// HistoryTableSQL is the CREATE TABLE SQL of `tidb_ddl_history`.
	HistoryTableSQL = `create table mysql.tidb_ddl_history (
		job_id bigint not null,
		job_meta longblob,
		db_name char(64),
		table_name char(64),
		schema_ids text(65535),
		table_ids text(65535),
		create_time datetime,
		primary key(job_id))`
	// MDLTableSQL is the CREATE TABLE SQL of `tidb_mdl_info`.
	MDLTableSQL = `create table mysql.tidb_mdl_info (
		job_id BIGINT NOT NULL PRIMARY KEY,
		version BIGINT NOT NULL,
		table_ids text(65535),
		owner_id varchar(64) NOT NULL DEFAULT ''
	);`
	// BackgroundSubtaskTableSQL is the CREATE TABLE SQL of `tidb_background_subtask`.
	BackgroundSubtaskTableSQL = `create table mysql.tidb_background_subtask (
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
	)`
	// BackgroundSubtaskHistoryTableSQL is the CREATE TABLE SQL of `tidb_background_subtask_history`.
	BackgroundSubtaskHistoryTableSQL = `create table mysql.tidb_background_subtask_history (
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
		key idx_state_update_time(state_update_time))`

	// NotifierTableName is `tidb_ddl_notifier`.
	NotifierTableName = "tidb_ddl_notifier"

	// NotifierTableSQL is the CREATE TABLE SQL of `tidb_ddl_notifier`.
	NotifierTableSQL = `CREATE TABLE mysql.tidb_ddl_notifier (
		ddl_job_id BIGINT,
		sub_job_id BIGINT COMMENT '-1 if the schema change does not belong to a multi-schema change DDL or a merged DDL. 0 or positive numbers representing the sub-job index of a multi-schema change DDL or a merged DDL',
		schema_change LONGBLOB COMMENT 'SchemaChangeEvent at rest',
		processed_by_flag BIGINT UNSIGNED DEFAULT 0 COMMENT 'flag to mark which subscriber has processed the event',
		PRIMARY KEY(ddl_job_id, sub_job_id))`
)
