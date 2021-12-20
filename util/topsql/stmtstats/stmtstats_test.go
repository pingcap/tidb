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
)

func TestKvStatementStatsItem_Merge(t *testing.T) {
	item1 := &KvStatementStatsItem{
		KvExecCount: map[string]uint64{
			"127.0.0.1:10001": 1,
			"127.0.0.1:10002": 2,
		},
	}
	item2 := &KvStatementStatsItem{
		KvExecCount: map[string]uint64{
			"127.0.0.1:10002": 2,
			"127.0.0.1:10003": 3,
		},
	}
	assert.Len(t, item1.KvExecCount, 2)
	assert.Len(t, item2.KvExecCount, 2)
	item1.Merge(item2)
	assert.Len(t, item1.KvExecCount, 3)
	assert.Len(t, item2.KvExecCount, 2)
	assert.Equal(t, uint64(1), item1.KvExecCount["127.0.0.1:10001"])
	assert.Equal(t, uint64(3), item1.KvExecCount["127.0.0.1:10003"])
	assert.Equal(t, uint64(3), item1.KvExecCount["127.0.0.1:10003"])
}

func TestStatementsStatsItem_Merge(t *testing.T) {
	item1 := &StatementStatsItem{
		ExecCount:   1,
		KvStatsItem: NewKvStatementStatsItem(),
	}
	item2 := &StatementStatsItem{
		ExecCount:   2,
		KvStatsItem: NewKvStatementStatsItem(),
	}
	item1.Merge(item2)
	assert.Equal(t, uint64(3), item1.ExecCount)
}

func TestStatementStatsMap_Merge(t *testing.T) {
	m1 := StatementStatsMap{
		SQLPlanDigest{SQLDigest: "SQL-1"}: &StatementStatsItem{
			ExecCount: 1,
			KvStatsItem: &KvStatementStatsItem{
				KvExecCount: map[string]uint64{
					"KV-1": 1,
					"KV-2": 2,
				},
			},
		},
		SQLPlanDigest{SQLDigest: "SQL-2"}: &StatementStatsItem{
			ExecCount: 1,
			KvStatsItem: &KvStatementStatsItem{
				KvExecCount: map[string]uint64{
					"KV-1": 1,
					"KV-2": 2,
				},
			},
		},
	}
	m2 := StatementStatsMap{
		SQLPlanDigest{SQLDigest: "SQL-2"}: &StatementStatsItem{
			ExecCount: 1,
			KvStatsItem: &KvStatementStatsItem{
				KvExecCount: map[string]uint64{
					"KV-1": 1,
					"KV-2": 2,
				},
			},
		},
		SQLPlanDigest{SQLDigest: "SQL-3"}: &StatementStatsItem{
			ExecCount: 1,
			KvStatsItem: &KvStatementStatsItem{
				KvExecCount: map[string]uint64{
					"KV-1": 1,
					"KV-2": 2,
				},
			},
		},
	}
	assert.Len(t, m1, 2)
	assert.Len(t, m2, 2)
	m1.Merge(m2)
	assert.Len(t, m1, 3)
	assert.Len(t, m2, 2)
	assert.Equal(t, uint64(1), m1[SQLPlanDigest{SQLDigest: "SQL-1"}].ExecCount)
	assert.Equal(t, uint64(2), m1[SQLPlanDigest{SQLDigest: "SQL-2"}].ExecCount)
	assert.Equal(t, uint64(1), m1[SQLPlanDigest{SQLDigest: "SQL-3"}].ExecCount)
	assert.Equal(t, uint64(1), m1[SQLPlanDigest{SQLDigest: "SQL-1"}].KvStatsItem.KvExecCount["KV-1"])
	assert.Equal(t, uint64(2), m1[SQLPlanDigest{SQLDigest: "SQL-1"}].KvStatsItem.KvExecCount["KV-2"])
	assert.Equal(t, uint64(2), m1[SQLPlanDigest{SQLDigest: "SQL-2"}].KvStatsItem.KvExecCount["KV-1"])
	assert.Equal(t, uint64(4), m1[SQLPlanDigest{SQLDigest: "SQL-2"}].KvStatsItem.KvExecCount["KV-2"])
	assert.Equal(t, uint64(1), m1[SQLPlanDigest{SQLDigest: "SQL-3"}].KvStatsItem.KvExecCount["KV-1"])
	assert.Equal(t, uint64(2), m1[SQLPlanDigest{SQLDigest: "SQL-3"}].KvStatsItem.KvExecCount["KV-2"])
	m1.Merge(nil)
	assert.Len(t, m1, 3)
}

func TestCreateStatementStats(t *testing.T) {
	c := newAggregator()
	globalAggregator.Store(c)
	stats := CreateStatementStats()
	assert.NotNil(t, stats)
	_, ok := c.statsSet.Load(stats)
	assert.True(t, ok)
	assert.False(t, stats.Finished())
	stats.SetFinished()
	assert.True(t, stats.Finished())
}

func TestExecCounter_AddExecCount_Take(t *testing.T) {
	globalAggregator.Store(newAggregator())
	stats := CreateStatementStats()
	m := stats.Take()
	assert.Len(t, m, 0)
	stats.AddExecCount([]byte("SQL-1"), []byte(""), 1)
	stats.AddExecCount([]byte("SQL-2"), []byte(""), 2)
	stats.AddExecCount([]byte("SQL-3"), []byte(""), 3)
	m = stats.Take()
	assert.Len(t, m, 3)
	assert.Equal(t, uint64(1), m[SQLPlanDigest{SQLDigest: "SQL-1"}].ExecCount)
	assert.Equal(t, uint64(2), m[SQLPlanDigest{SQLDigest: "SQL-2"}].ExecCount)
	assert.Equal(t, uint64(3), m[SQLPlanDigest{SQLDigest: "SQL-3"}].ExecCount)
	m = stats.Take()
	assert.Len(t, m, 0)
}
