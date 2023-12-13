// Copyright 2023 PingCAP, Inc.
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

package analyzequeue

import (
	"time"

	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/statistics/handle/util"
)

func getAverageAnalysisDuration(
	sctx sessionctx.Context,
	schema string,
	tableName string,
	partitionName string,
) (time.Duration, error) {
	var query string
	var args []interface{}
	if partitionName != "" {
		query = `
      SELECT AVG(TIMESTAMPDIFF(SECOND, start_time, end_time)) AS avg_duration
      FROM mysql.analyze_jobs
      WHERE table_schema = ?
      AND table_name = ?
      AND partition_name = ?
      AND state = 'finished' AND fail_reason IS NULL
      ORDER BY id DESC
      LIMIT 5;
    `
		args = []interface{}{schema, tableName, partitionName}
	} else {
		query = `
      SELECT AVG(TIMESTAMPDIFF(SECOND, start_time, end_time)) AS avg_duration
      FROM mysql.analyze_jobs
      WHERE table_schema = ?
      AND table_name = ?
      AND partition_name IS NULL  
      AND state = 'finished' AND fail_reason IS NULL
      ORDER BY id DESC
      LIMIT 5;
    `
		args = []interface{}{schema, tableName}
	}

	rows, _, err := util.ExecRows(sctx, query, args...)
	if err != nil {
		return 0, err
	}

	var avgDuration float64
	if len(rows) > 0 {
		avgDuration = rows[0].GetFloat64(0)
	}

	return time.Duration(avgDuration) * time.Second, nil
}

func getLastFailedAnalysisDuration(
	sctx sessionctx.Context,
	schema string,
	tableName string,
	partitionName string, // pass empty for non-partition table
) (time.Duration, error) {
	var query string
	if partitionName != "" {
		query = `
      SELECT TIMESTAMPDIFF(SECOND, start_time, CURRENT_TIMESTAMP) 
      FROM mysql.analyze_jobs
      WHERE table_schema = ?
      AND table_name = ? 
      AND partition_name = ?
      AND state = 'failed'
      ORDER BY id DESC
      LIMIT 1;
    `
	} else {
		query = `
      SELECT TIMESTAMPDIFF(SECOND, start_time, CURRENT_TIMESTAMP)
      FROM mysql.analyze_jobs
      WHERE table_schema = ?
      AND table_name = ?
      AND state = 'failed'  
      ORDER BY id DESC
      LIMIT 1;
    `
	}

	rows, _, err := util.ExecRows(sctx, query, schema, tableName, partitionName)
	if err != nil {
		return 0, err
	}

	var durationInSeconds int64
	if len(rows) > 0 {
		durationInSeconds = rows[0].GetInt64(0)
	}

	return time.Duration(durationInSeconds) * time.Second, nil
}
