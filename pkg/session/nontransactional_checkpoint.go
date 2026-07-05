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

package session

const createNonTransactionalDMLCheckpointTableSQL = `CREATE TABLE IF NOT EXISTS mysql.tidb_nontransactional_dml_checkpoint (
	job_id VARCHAR(128) NOT NULL,
	range_id BIGINT(64) NOT NULL,
	mode VARCHAR(16) NOT NULL,
	dml_type VARCHAR(16) NOT NULL,
	db_name VARCHAR(64) NOT NULL,
	table_id BIGINT(64) NOT NULL,
	physical_table_id BIGINT(64) NOT NULL,
	handle_kind VARCHAR(32) NOT NULL,
	range_start BLOB DEFAULT NULL,
	range_start_inclusive TINYINT(1) NOT NULL DEFAULT 0,
	range_end BLOB DEFAULT NULL,
	range_end_inclusive TINYINT(1) NOT NULL DEFAULT 0,
	checkpoint BLOB DEFAULT NULL,
	status VARCHAR(16) NOT NULL,
	retry_count BIGINT(64) UNSIGNED NOT NULL DEFAULT 0,
	error_class VARCHAR(64) DEFAULT NULL,
	error_text TEXT DEFAULT NULL,
	scanned BIGINT(64) UNSIGNED NOT NULL DEFAULT 0,
	affected BIGINT(64) UNSIGNED NOT NULL DEFAULT 0,
	created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
	finished_at TIMESTAMP NULL DEFAULT NULL,
	PRIMARY KEY(job_id, range_id),
	KEY idx_table_status(table_id, status),
	KEY idx_updated_at(updated_at)
);`
