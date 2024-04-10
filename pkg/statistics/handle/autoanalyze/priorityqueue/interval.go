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

package priorityqueue

import (
	"time"

	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/statistics/handle/util"
)

// NoRecord is used to indicate that there is no related record in mysql.analyze_jobs.
const NoRecord = -1

// justFailed is used to indicate that the last analysis has just failed.
const justFailed = 0

const avgDurationQueryForTable = `
	SELECT AVG(TIMESTAMPDIFF(SECOND, start_time, end_time)) AS avg_duration
	FROM (
		SELECT start_time, end_time
		FROM mysql.analyze_jobs
		WHERE table_schema = %? AND table_name = %? AND state = 'finished' AND fail_reason IS NULL AND partition_name = ''
		ORDER BY id DESC
		LIMIT 5
	) AS recent_analyses;
`

// For multiple partitions, we only need to return the average duration of the most recent 5 successful analyses
// from all partitions.
const avgDurationQueryForPartition = `
	SELECT AVG(TIMESTAMPDIFF(SECOND, start_time, end_time)) AS avg_duration
	FROM (
		SELECT start_time, end_time
		FROM mysql.analyze_jobs
		WHERE table_schema = %? AND table_name = %? AND state = 'finished' AND fail_reason IS NULL AND partition_name in (%?)
		ORDER BY id DESC
		LIMIT 5
	) AS recent_analyses;
`

const lastFailedDurationQueryForTable = `
	SELECT TIMESTAMPDIFF(SECOND, start_time, CURRENT_TIMESTAMP)
	FROM mysql.analyze_jobs
	WHERE table_schema = %? AND table_name = %? AND state = 'failed' AND partition_name = ''
	ORDER BY id DESC
	LIMIT 1;
`

// For multiple partitions, we only need to return the duration of the most recent failed analysis.
// We pick the minimum duration of all failed analyses because we want to be conservative.
const lastFailedDurationQueryForPartition = `
	SELECT
		MIN(TIMESTAMPDIFF(SECOND, aj.start_time, CURRENT_TIMESTAMP)) AS min_duration
	FROM (
		SELECT
			MAX(id) AS max_id
		FROM
			mysql.analyze_jobs
		WHERE
			table_schema = %?
			AND table_name = %?
			AND state = 'failed'
			AND partition_name IN (%?)
		GROUP BY
			partition_name
	) AS latest_failures
	JOIN mysql.analyze_jobs aj ON aj.id = latest_failures.max_id;
`

// GetAverageAnalysisDuration returns the average duration of the last 5 successful analyses for each specified partition.
// If there are no successful analyses, it returns 0.
func GetAverageAnalysisDuration(
	sctx sessionctx.Context,
	schema, tableName string,
	partitionNames ...string,
) (time.Duration, error) {
	query := ""
	params := make([]any, 0, len(partitionNames)+3)
	if len(partitionNames) == 0 {
		query = avgDurationQueryForTable
		params = append(params, schema, tableName)
	} else {
		query = avgDurationQueryForPartition
		params = append(params, schema, tableName, partitionNames)
	}

	rows, _, err := util.ExecRows(sctx, query, params...)
	if err != nil {
		return NoRecord, err
	}

	// NOTE: if there are no successful analyses, we return 0.
	if len(rows) == 0 || rows[0].IsNull(0) {
		return NoRecord, nil
	}
	avgDuration := rows[0].GetMyDecimal(0)
	duration, err := avgDuration.ToFloat64()
	if err != nil {
		return NoRecord, err
	}

	return time.Duration(duration) * time.Second, nil
}

// GetLastFailedAnalysisDuration returns the duration since the last failed analysis.
// If there is no failed analysis, it returns 0.
func GetLastFailedAnalysisDuration(
	sctx sessionctx.Context,
	schema, tableName string,
	partitionNames ...string,
) (time.Duration, error) {
	query := ""
	params := make([]any, 0, len(partitionNames)+3)
	if len(partitionNames) == 0 {
		query = lastFailedDurationQueryForTable
		params = append(params, schema, tableName)
	} else {
		query = lastFailedDurationQueryForPartition
		params = append(params, schema, tableName, partitionNames)
	}

	rows, _, err := util.ExecRows(sctx, query, params...)
	if err != nil {
		return NoRecord, err
	}

	// NOTE: if there are no failed analyses, we return 0.
	if len(rows) == 0 || rows[0].IsNull(0) {
		return NoRecord, nil
	}
	lastFailedDuration := rows[0].GetUint64(0)
	if lastFailedDuration == 0 {
		return justFailed, nil
	}

	return time.Duration(lastFailedDuration) * time.Second, nil
}
