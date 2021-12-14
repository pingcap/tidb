// Copyright 2021 PingCAP, Inc.
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

package stmtstats

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/atomic"
)

func Test_statementStatsManager_register_collect(t *testing.T) {
	m := newStatementStatsManagerManager()
	stats := &StatementStats{
		data:   StatementStatsMap{},
		closed: atomic.NewBool(false),
	}
	m.register(stats)
	stats.AddExecCount("SQL-1", "", 1001, 1)
	assert.Empty(t, m.data)
	m.collect()
	assert.NotEmpty(t, m.data)
	assert.Equal(t, uint64(1), m.data[SQLPlanDigest{SQLDigest: "SQL-1"}][1001].ExecCount)
}
