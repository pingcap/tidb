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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGenSQLForAnalyzeTable(t *testing.T) {
	job := &TableAnalysisJob{
		TableSchema: "test_schema",
		TableName:   "test_table",
	}

	expectedSQL := "analyze table %n.%n"
	expectedParams := []interface{}{"test_schema", "test_table"}

	sql, params := job.genSQLForAnalyzeTable()

	assert.Equal(t, expectedSQL, sql)
	assert.Equal(t, expectedParams, params)
}

func TestGenSQLForAnalyzeIndex(t *testing.T) {
	job := &TableAnalysisJob{
		TableSchema: "test_schema",
		TableName:   "test_table",
	}

	index := "test_index"

	expectedSQL := "analyze table %n.%n index %n"
	expectedParams := []interface{}{"test_schema", "test_table", index}

	sql, params := job.genSQLForAnalyzeIndex(index)

	assert.Equal(t, expectedSQL, sql)
	assert.Equal(t, expectedParams, params)
}
