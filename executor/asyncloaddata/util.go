// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package asyncloaddata

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/util/sqlexec"
)

const (
	// CreateLoadDataJobs is a table that LOAD DATA uses
	// TODO: move it to bootstrap.go and create it in bootstrap
	CreateLoadDataJobs = `CREATE TABLE IF NOT EXISTS mysql.load_data_jobs (
       job_id bigint(64) NOT NULL AUTO_INCREMENT,
       expected_status ENUM('running', 'paused', 'canceled') NOT NULL DEFAULT 'running',
       create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
       start_time TIMESTAMP,
       update_time TIMESTAMP,
       end_time TIMESTAMP,
       data_source TEXT NOT NULL,
       table_schema VARCHAR(64) NOT NULL,
       table_name VARCHAR(64) NOT NULL,
       import_mode VARCHAR(64) NOT NULL,
       create_user VARCHAR(32) NOT NULL,
       progress TEXT,
       result_message TEXT,
       error_message TEXT,
       PRIMARY KEY (job_id),
       KEY (created_user));`
)

func CreateLoadDataJob(
	ctx context.Context,
	conn sqlexec.SQLExecutor,
	source, db, table string,
	importMode string,
	user string,
) (int64, error) {
	const insertSQL = `INSERT INTO mysql.load_data_jobs
    	(data_source, table_schema, table_name, import_mode, create_user)
		VALUES (%?, %?, %?, %?, %?);`
	_, err := conn.ExecuteInternal(ctx, insertSQL, source, db, table, importMode, user)
	if err != nil {
		return 0, err
	}
	const lastInsertID = `SELECT LAST_INSERT_ID();`
	rs, err := conn.ExecuteInternal(ctx, lastInsertID)
	if err != nil {
		return 0, err
	}
	rows, err := sqlexec.DrainRecordSet(ctx, rs, 8)
	if err != nil {
		return 0, err
	}
	if len(rows) != 1 {
		return 0, errors.Errorf("unexpected result length: %d", len(rows))
	}
	return rows[0].GetInt64(0), nil
}
