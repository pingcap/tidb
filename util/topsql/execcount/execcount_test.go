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

package execcount

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestExecCountMap_Merge(t *testing.T) {
	m1 := ExecCountMap{
		"SQL-1": {
			10001: 1,
			10002: 2,
			10003: 3,
		},
		"SQL-2": {
			10001: 1,
			10002: 2,
			10003: 3,
		},
	}
	m2 := ExecCountMap{
		"SQL-2": {
			10001: 1,
			10002: 2,
			10003: 3,
		},
		"SQL-3": {
			10001: 1,
			10002: 2,
			10003: 3,
		},
	}
	assert.Len(t, m1, 2)
	assert.Len(t, m2, 2)
	m1.Merge(m2)
	assert.Len(t, m1, 3)
	assert.Len(t, m2, 2)
	assert.Equal(t, uint64(1), m1["SQL-1"][10001])
	assert.Equal(t, uint64(2), m1["SQL-1"][10002])
	assert.Equal(t, uint64(3), m1["SQL-1"][10003])
	assert.Equal(t, uint64(2), m1["SQL-2"][10001])
	assert.Equal(t, uint64(4), m1["SQL-2"][10002])
	assert.Equal(t, uint64(6), m1["SQL-2"][10003])
	assert.Equal(t, uint64(1), m1["SQL-3"][10001])
	assert.Equal(t, uint64(2), m1["SQL-3"][10002])
	assert.Equal(t, uint64(3), m1["SQL-3"][10003])
	m1.Merge(nil)
	assert.Len(t, m1, 3)
}

func TestKvExecCountMap_Merge(t *testing.T) {
	m1 := KvExecCountMap{
		"SQL-1": {
			"ADDR-1": {
				10001: 1,
				10002: 2,
				10003: 3,
			},
			"ADDR-2": {
				10001: 1,
				10002: 2,
				10003: 3,
			},
		},
		"SQL-2": {
			"ADDR-2": {
				10001: 1,
				10002: 2,
				10003: 3,
			},
			"ADDR-3": {
				10001: 1,
				10002: 2,
				10003: 3,
			},
		},
	}
	m2 := KvExecCountMap{
		"SQL-2": {
			"ADDR-3": {
				10001: 1,
				10002: 2,
				10003: 3,
			},
			"ADDR-4": {
				10001: 1,
				10002: 2,
				10003: 3,
			},
		},
		"SQL-3": {
			"ADDR-4": {
				10001: 1,
				10002: 2,
				10003: 3,
			},
			"ADDR-5": {
				10001: 1,
				10002: 2,
				10003: 3,
			},
		},
	}
	m1.Merge(m2)
	assert.Len(t, m1, 3)
	assert.Len(t, m1["SQL-1"], 2)
	assert.Len(t, m1["SQL-2"], 3)
	assert.Len(t, m1["SQL-3"], 2)
	assert.Equal(t, uint64(1), m1["SQL-1"]["ADDR-1"][10001])
	assert.Equal(t, uint64(2), m1["SQL-1"]["ADDR-1"][10002])
	assert.Equal(t, uint64(3), m1["SQL-1"]["ADDR-1"][10003])
	assert.Equal(t, uint64(1), m1["SQL-1"]["ADDR-2"][10001])
	assert.Equal(t, uint64(2), m1["SQL-1"]["ADDR-2"][10002])
	assert.Equal(t, uint64(3), m1["SQL-1"]["ADDR-2"][10003])
	assert.Equal(t, uint64(1), m1["SQL-2"]["ADDR-2"][10001])
	assert.Equal(t, uint64(2), m1["SQL-2"]["ADDR-2"][10002])
	assert.Equal(t, uint64(3), m1["SQL-2"]["ADDR-2"][10003])
	assert.Equal(t, uint64(2), m1["SQL-2"]["ADDR-3"][10001])
	assert.Equal(t, uint64(4), m1["SQL-2"]["ADDR-3"][10002])
	assert.Equal(t, uint64(6), m1["SQL-2"]["ADDR-3"][10003])
	assert.Equal(t, uint64(1), m1["SQL-2"]["ADDR-4"][10001])
	assert.Equal(t, uint64(2), m1["SQL-2"]["ADDR-4"][10002])
	assert.Equal(t, uint64(3), m1["SQL-2"]["ADDR-4"][10003])
	assert.Equal(t, uint64(1), m1["SQL-3"]["ADDR-4"][10001])
	assert.Equal(t, uint64(2), m1["SQL-3"]["ADDR-4"][10002])
	assert.Equal(t, uint64(3), m1["SQL-3"]["ADDR-4"][10003])
	assert.Equal(t, uint64(1), m1["SQL-3"]["ADDR-5"][10001])
	assert.Equal(t, uint64(2), m1["SQL-3"]["ADDR-5"][10002])
	assert.Equal(t, uint64(3), m1["SQL-3"]["ADDR-5"][10003])
}

func TestCreateExecCounter(t *testing.T) {
	globalExecCounterManager = nil
	counter := CreateExecCounter()
	assert.Nil(t, counter)
	globalExecCounterManager = newExecCountManager()
	counter = CreateExecCounter()
	assert.NotNil(t, counter)
	counter2Raw, ok := globalExecCounterManager.counters.Load(globalExecCounterManager.curCounterID.Load())
	assert.True(t, ok)
	counter2 := counter2Raw.(*ExecCounter)
	assert.Equal(t, counter, counter2)
	assert.False(t, counter.Closed())
	counter2.Close()
	assert.True(t, counter.Closed())
}

func TestExecCounter_Count_Take(t *testing.T) {
	globalExecCounterManager = newExecCountManager()
	counter := CreateExecCounter()
	m := counter.Take()
	assert.Len(t, m, 0)
	counter.Count("SQL-1", 1)
	counter.Count("SQL-2", 2)
	counter.Count("SQL-3", 3)
	m = counter.Take()
	assert.Len(t, m, 3)
	assert.Len(t, m["SQL-1"], 1)
	assert.Len(t, m["SQL-2"], 1)
	assert.Len(t, m["SQL-3"], 1)
	for _, v := range m["SQL-1"] {
		assert.Equal(t, uint64(1), v)
	}
	for _, v := range m["SQL-2"] {
		assert.Equal(t, uint64(2), v)
	}
	for _, v := range m["SQL-3"] {
		assert.Equal(t, uint64(3), v)
	}
	m = counter.Take()
	assert.Len(t, m, 0)
}

func TestExecCounter_CountKv_TakeKv(t *testing.T) {
	globalExecCounterManager = newExecCountManager()
	counter := CreateExecCounter()
	m := counter.TakeKv()
	assert.Len(t, m, 0)
	counter.CountKv("SQL-1", "ADDR-1", 11)
	counter.CountKv("SQL-1", "ADDR-2", 12)
	counter.CountKv("SQL-2", "ADDR-2", 22)
	counter.CountKv("SQL-2", "ADDR-3", 23)
	counter.CountKv("SQL-3", "ADDR-3", 33)
	counter.CountKv("SQL-3", "ADDR-4", 34)
	m = counter.TakeKv()
	assert.Len(t, m, 3)
	assert.Len(t, m["SQL-1"], 2)
	assert.Len(t, m["SQL-2"], 2)
	assert.Len(t, m["SQL-3"], 2)
	assert.Len(t, m["SQL-1"]["ADDR-1"], 1)
	assert.Len(t, m["SQL-1"]["ADDR-2"], 1)
	assert.Len(t, m["SQL-2"]["ADDR-2"], 1)
	assert.Len(t, m["SQL-2"]["ADDR-3"], 1)
	assert.Len(t, m["SQL-3"]["ADDR-3"], 1)
	assert.Len(t, m["SQL-3"]["ADDR-4"], 1)
	for _, v := range m["SQL-1"]["ADDR-1"] {
		assert.Equal(t, uint64(11), v)
	}
	for _, v := range m["SQL-1"]["ADDR-2"] {
		assert.Equal(t, uint64(12), v)
	}
	for _, v := range m["SQL-2"]["ADDR-2"] {
		assert.Equal(t, uint64(22), v)
	}
	for _, v := range m["SQL-2"]["ADDR-3"] {
		assert.Equal(t, uint64(23), v)
	}
	for _, v := range m["SQL-3"]["ADDR-3"] {
		assert.Equal(t, uint64(33), v)
	}
	for _, v := range m["SQL-3"]["ADDR-4"] {
		assert.Equal(t, uint64(34), v)
	}
	m = counter.TakeKv()
	assert.Len(t, m, 0)
}
