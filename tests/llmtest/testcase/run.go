// Copyright 2025 PingCAP, Inc.
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

package testcase

import (
	"database/sql"
	"strings"

	"github.com/pingcap/tidb/tests/llmtest/logger"
	"go.uber.org/zap"
)

func executeSingleQueryInDB(db *sql.DB, query string, args ...any) ([][]string, error) {
	dbRows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer dbRows.Close()

	cols, err := dbRows.Columns()
	if err != nil {
		return nil, err
	}
	var queryResults [][]string
	for dbRows.Next() {
		data := make([]any, len(cols))
		for i := range data {
			var s sql.NullString
			data[i] = &s
		}
		err = dbRows.Scan(data...)
		if err != nil {
			return nil, err
		}

		rowStrings := make([]string, len(cols))
		for i, val := range data {
			strVal := val.(*sql.NullString)
			if strVal.Valid {
				rowStrings[i] = strVal.String
			} else {
				rowStrings[i] = "<nil>"
			}
		}
		queryResults = append(queryResults, rowStrings)
	}

	// In some cases (e.g. `SELECT COT(0)`), `dbRows.Next()` will always return false, and the error
	// can be retrieved by calling `dbRows.Err()`. This behavior can be different with TiDB, because
	// for TiDB the client may get error from `dbRows.Scan()`.
	// Not sure whether it's expected, but it looks acceptable for now.
	if dbRows.Err() != nil {
		return nil, dbRows.Err()
	}

	return queryResults, nil
}

func executeSQLsInDB(db *sql.DB, c *Case) (ret [][][]string, retErr error) {
	allQueries := strings.Split(c.SQL, ";")
	allResults := make([][][]string, 0, len(allQueries))

	for _, query := range allQueries {
		if len(query) == 0 {
			continue
		}

		queryResults, err := executeSingleQueryInDB(db, query, c.Args...)
		if err != nil {
			return nil, err
		}
		allResults = append(allResults, queryResults)
	}

	return allResults, nil
}

// RunABTest runs the A/B test on two databases.
func (m *Manager) RunABTest(db1 *sql.DB, db2 *sql.DB, recheckPassed bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Run A/B test
	for _, cases := range m.cases {
	caseLoop:
		for _, c := range cases {
			if c.Known {
				continue
			}
			if !recheckPassed && c.Pass {
				continue
			}
			logger := logger.Global.With(
				zap.String("sql", c.SQL), zap.Any("args", c.Args),
			)
			result1, err1 := executeSQLsInDB(db1, c)
			result2, err2 := executeSQLsInDB(db2, c)

			if err1 != nil || err2 != nil {
				// all of them should fail
				if !(err1 != nil && err2 != nil) {
					logger.Info("One of the result is error", zap.Error(err1), zap.Error(err2))
					c.Pass = false
				} else {
					c.Pass = true
				}

				continue
			}

			if len(result1) != len(result2) {
				logger.Info("Different result set count", zap.Any("result1", result1), zap.Any("result2", result2))
				c.Pass = false
				continue
			}

			for i := range result1 {
				if len(result1[i]) != len(result2[i]) {
					c.Pass = false
					logger.Info("Different row count", zap.Any("result1", result1[i]), zap.Any("result2", result2[i]))
					continue caseLoop
				}
				for j := range result1[i] {
					if len(result1[i][j]) != len(result2[i][j]) {
						c.Pass = false
						logger.Info("Different column length", zap.Strings("result1", result1[i][j]), zap.Strings("result2", result2[i][j]))
						continue caseLoop
					}

					for k := range result1[i][j] {
						if result1[i][j][k] != result2[i][j][k] {
							c.Pass = false
							logger.Info("Different result", zap.Strings("result1", result1[i][j]), zap.Strings("result2", result2[i][j]))
							continue caseLoop
						}
					}
				}
			}

			c.Pass = true
		}
	}
}
