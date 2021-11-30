package topsql

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

func TestCreateExecCounter(t *testing.T) {
	globalExecCounterManager = nil
	counter := CreateExecCounter()
	assert.Nil(t, counter)
	globalExecCounterManager = newExecCountManager()
	counter = CreateExecCounter()
	assert.NotNil(t, counter)
	counter2_, ok := globalExecCounterManager.counters.Load(globalExecCounterManager.curCounterID.Load())
	assert.True(t, ok)
	counter2 := counter2_.(*ExecCounter)
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
