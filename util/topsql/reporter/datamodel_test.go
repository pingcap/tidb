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

package reporter

import (
	"bytes"
	"sort"
	"sync"
	"testing"

	topsqlstate "github.com/pingcap/tidb/util/topsql/state"
	"github.com/pingcap/tidb/util/topsql/stmtstats"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/assert"
)

func Test_tsItem_toProto(t *testing.T) {
	item := &tsItem{
		timestamp: 1,
		cpuTimeMs: 2,
		stmtStats: stmtstats.StatementStatsItem{
			ExecCount:     3,
			SumDurationNs: 50000,
			DurationCount: 2,
			KvStatsItem:   stmtstats.KvStatementStatsItem{KvExecCount: map[string]uint64{"": 4}},
		},
	}
	pb := item.toProto()
	assert.Equal(t, uint64(1), pb.TimestampSec)
	assert.Equal(t, uint32(2), pb.CpuTimeMs)
	assert.Equal(t, uint64(3), pb.StmtExecCount)
	assert.Equal(t, uint64(50000), pb.StmtDurationSumNs)
	assert.Equal(t, uint64(2), pb.StmtDurationCount)
	assert.Equal(t, uint64(4), pb.StmtKvExecCount[""])
}

func Test_tsItems_toProto(t *testing.T) {
	items := &tsItems{{}, {}, {}}
	pb := items.toProto()
	assert.Len(t, pb, 3)
}

func Test_record_Sort(t *testing.T) {
	r := record{
		tsItems: tsItems{
			{timestamp: 2},
			{timestamp: 3},
			{timestamp: 1},
		},
		tsIndex: map[uint64]int{
			2: 0,
			3: 1,
			1: 2,
		},
	}
	sort.Sort(&r)
	assert.Equal(t, uint64(1), r.tsItems[0].timestamp)
	assert.Equal(t, uint64(2), r.tsItems[1].timestamp)
	assert.Equal(t, uint64(3), r.tsItems[2].timestamp)
	assert.Equal(t, 0, r.tsIndex[1])
	assert.Equal(t, 1, r.tsIndex[2])
	assert.Equal(t, 2, r.tsIndex[3])
}

func Test_record_append(t *testing.T) {
	r := newRecord(nil, nil)
	//   TimestampList: []
	//     CPUTimeList: []
	//   ExecCountList: []

	r.appendCPUTime(1, 1)
	//   TimestampList: [1]
	//     CPUTimeList: [1]
	//   ExecCountList: [0]

	r.appendStmtStatsItem(1, stmtstats.StatementStatsItem{ExecCount: 1, SumDurationNs: 10000})
	//   TimestampList: [1]
	//     CPUTimeList: [1]
	//   ExecCountList: [1]

	r.appendCPUTime(2, 1)
	//   TimestampList: [1, 2]
	//     CPUTimeList: [1, 1]
	//   ExecCountList: [1, 0]

	r.appendCPUTime(3, 1)
	//   TimestampList: [1, 2, 3]
	//     CPUTimeList: [1, 1, 1]
	//   ExecCountList: [1, 0, 0]

	r.appendStmtStatsItem(3, stmtstats.StatementStatsItem{ExecCount: 1, SumDurationNs: 30000})
	//   TimestampList: [1, 2, 3]
	//     CPUTimeList: [1, 1, 1]
	//   ExecCountList: [1, 0, 1]

	r.appendStmtStatsItem(2, stmtstats.StatementStatsItem{ExecCount: 1, SumDurationNs: 20000})
	//   TimestampList: [1, 2, 3]
	//     CPUTimeList: [1, 1, 1]
	//   ExecCountList: [1, 1, 1]

	assert.Len(t, r.tsItems, 3)
	assert.Len(t, r.tsIndex, 3)
	assert.Equal(t, uint64(3), r.totalCPUTimeMs)
	assert.Equal(t, uint64(1), r.tsItems[0].timestamp)
	assert.Equal(t, uint64(2), r.tsItems[1].timestamp)
	assert.Equal(t, uint64(3), r.tsItems[2].timestamp)
	assert.Equal(t, uint32(1), r.tsItems[0].cpuTimeMs)
	assert.Equal(t, uint32(1), r.tsItems[1].cpuTimeMs)
	assert.Equal(t, uint32(1), r.tsItems[2].cpuTimeMs)
	assert.Equal(t, uint64(1), r.tsItems[0].stmtStats.ExecCount)
	assert.Equal(t, uint64(1), r.tsItems[1].stmtStats.ExecCount)
	assert.Equal(t, uint64(1), r.tsItems[2].stmtStats.ExecCount)
	assert.Equal(t, uint64(10000), r.tsItems[0].stmtStats.SumDurationNs)
	assert.Equal(t, uint64(20000), r.tsItems[1].stmtStats.SumDurationNs)
	assert.Equal(t, uint64(30000), r.tsItems[2].stmtStats.SumDurationNs)
}

func Test_record_toProto(t *testing.T) {
	r := record{
		sqlDigest:      []byte("SQL-1"),
		planDigest:     []byte("PLAN-1"),
		totalCPUTimeMs: 123,
		tsItems:        tsItems{{}, {}, {}},
	}
	pb := r.toProto()
	assert.Equal(t, []byte("SQL-1"), pb.SqlDigest)
	assert.Equal(t, []byte("PLAN-1"), pb.PlanDigest)
	assert.Len(t, pb.Items, 3)
}

func Test_records_Sort(t *testing.T) {
	rs := records{
		{totalCPUTimeMs: 1},
		{totalCPUTimeMs: 3},
		{totalCPUTimeMs: 2},
	}
	sort.Sort(rs)
	assert.Equal(t, uint64(3), rs[0].totalCPUTimeMs)
	assert.Equal(t, uint64(2), rs[1].totalCPUTimeMs)
	assert.Equal(t, uint64(1), rs[2].totalCPUTimeMs)
}

func Test_records_toProto(t *testing.T) {
	rs := records{{}, {}}
	pb := rs.toProto()
	assert.Len(t, pb, 2)
}

func Test_collecting_getOrCreateRecord(t *testing.T) {
	c := newCollecting()
	r1 := c.getOrCreateRecord([]byte("SQL-1"), []byte("PLAN-1"))
	assert.NotNil(t, r1)
	r2 := c.getOrCreateRecord([]byte("SQL-1"), []byte("PLAN-1"))
	assert.Equal(t, r1, r2)
}

func Test_collecting_markAsEvicted_hasEvicted(t *testing.T) {
	c := newCollecting()
	c.markAsEvicted(1, []byte("SQL-1"), []byte("PLAN-1"))
	assert.True(t, c.hasEvicted(1, []byte("SQL-1"), []byte("PLAN-1")))
	assert.False(t, c.hasEvicted(1, []byte("SQL-2"), []byte("PLAN-2")))
	assert.False(t, c.hasEvicted(2, []byte("SQL-1"), []byte("PLAN-1")))
}

func Test_collecting_appendOthers(t *testing.T) {
	c := newCollecting()
	c.appendOthersCPUTime(1, 1)
	c.appendOthersCPUTime(2, 2)
	c.appendOthersStmtStatsItem(1, stmtstats.StatementStatsItem{ExecCount: 1, SumDurationNs: 1000})
	c.appendOthersStmtStatsItem(2, stmtstats.StatementStatsItem{ExecCount: 2, SumDurationNs: 2000})
	r := c.records[keyOthers]
	assert.Len(t, r.tsItems, 2)
	assert.Len(t, r.tsIndex, 2)
	assert.Equal(t, uint64(1), r.tsItems[0].timestamp)
	assert.Equal(t, uint64(2), r.tsItems[1].timestamp)
	assert.Equal(t, uint32(1), r.tsItems[0].cpuTimeMs)
	assert.Equal(t, uint32(2), r.tsItems[1].cpuTimeMs)
	assert.Equal(t, uint64(1), r.tsItems[0].stmtStats.ExecCount)
	assert.Equal(t, uint64(2), r.tsItems[1].stmtStats.ExecCount)
	assert.Equal(t, uint64(1000), r.tsItems[0].stmtStats.SumDurationNs)
	assert.Equal(t, uint64(2000), r.tsItems[1].stmtStats.SumDurationNs)
}

func Test_collecting_getReportRecords(t *testing.T) {
	c := newCollecting()
	c.getOrCreateRecord([]byte("SQL-1"), []byte("PLAN-1")).appendCPUTime(1, 1)
	c.getOrCreateRecord([]byte("SQL-2"), []byte("PLAN-2")).appendCPUTime(1, 2)
	c.getOrCreateRecord([]byte("SQL-3"), []byte("PLAN-3")).appendCPUTime(1, 3)
	c.getOrCreateRecord([]byte(keyOthers), []byte(keyOthers)).appendCPUTime(1, 10)
	rs := c.getReportRecords()
	assert.Len(t, rs, 4)
	assert.Equal(t, uint32(10), rs[3].tsItems[0].cpuTimeMs)
	assert.Equal(t, uint64(10), rs[3].totalCPUTimeMs)
}

func Test_collecting_take(t *testing.T) {
	c1 := newCollecting()
	c1.getOrCreateRecord([]byte("SQL-1"), []byte("PLAN-1")).appendCPUTime(1, 1)
	c2 := c1.take()
	assert.Empty(t, c1.records)
	assert.Len(t, c2.records, 1)
	assert.NotEqual(t, c1.keyBuf, c2.keyBuf)
}

func Test_cpuRecords_Sort(t *testing.T) {
	rs := cpuRecords{
		{CPUTimeMs: 1},
		{CPUTimeMs: 3},
		{CPUTimeMs: 2},
	}
	sort.Sort(rs)
	assert.Equal(t, uint32(3), rs[0].CPUTimeMs)
	assert.Equal(t, uint32(2), rs[1].CPUTimeMs)
	assert.Equal(t, uint32(1), rs[2].CPUTimeMs)
}

func Test_cpuRecords_topN(t *testing.T) {
	rs := cpuRecords{
		{CPUTimeMs: 1},
		{CPUTimeMs: 3},
		{CPUTimeMs: 2},
	}
	top, evicted := rs.topN(2)
	assert.Len(t, top, 2)
	assert.Len(t, evicted, 1)
	assert.Equal(t, uint32(3), top[0].CPUTimeMs)
	assert.Equal(t, uint32(2), top[1].CPUTimeMs)
	assert.Equal(t, uint32(1), evicted[0].CPUTimeMs)
}

func Test_normalizedSQLMap_register(t *testing.T) {
	topsqlstate.GlobalState.MaxCollect.Store(2)
	m := newNormalizedSQLMap()
	m.register([]byte("SQL-1"), "SQL-1", true)
	m.register([]byte("SQL-2"), "SQL-2", false)
	m.register([]byte("SQL-3"), "SQL-3", true)
	assert.Equal(t, int64(2), m.length.Load())
	v, ok := m.data.Load().(*sync.Map).Load("SQL-1")
	meta := v.(sqlMeta)
	assert.True(t, ok)
	assert.Equal(t, "SQL-1", meta.normalizedSQL)
	assert.True(t, meta.isInternal)
	v, ok = m.data.Load().(*sync.Map).Load("SQL-2")
	meta = v.(sqlMeta)
	assert.True(t, ok)
	assert.Equal(t, "SQL-2", meta.normalizedSQL)
	assert.False(t, meta.isInternal)
	_, ok = m.data.Load().(*sync.Map).Load("SQL-3")
	assert.False(t, ok)
}

func Test_normalizedSQLMap_take(t *testing.T) {
	topsqlstate.GlobalState.MaxCollect.Store(999)
	m1 := newNormalizedSQLMap()
	m1.register([]byte("SQL-1"), "SQL-1", true)
	m1.register([]byte("SQL-2"), "SQL-2", false)
	m1.register([]byte("SQL-3"), "SQL-3", true)
	m2 := m1.take()
	assert.Equal(t, int64(0), m1.length.Load())
	assert.Equal(t, int64(3), m2.length.Load())
	data1 := m1.data.Load().(*sync.Map)
	_, ok := data1.Load("SQL-1")
	assert.False(t, ok)
	_, ok = data1.Load("SQL-2")
	assert.False(t, ok)
	_, ok = data1.Load("SQL-3")
	assert.False(t, ok)
	data2 := m2.data.Load().(*sync.Map)
	_, ok = data2.Load("SQL-1")
	assert.True(t, ok)
	_, ok = data2.Load("SQL-2")
	assert.True(t, ok)
	_, ok = data2.Load("SQL-3")
	assert.True(t, ok)
}

func Test_normalizedSQLMap_toProto(t *testing.T) {
	topsqlstate.GlobalState.MaxCollect.Store(999)
	m := newNormalizedSQLMap()
	m.register([]byte("SQL-1"), "SQL-1", true)
	m.register([]byte("SQL-2"), "SQL-2", false)
	m.register([]byte("SQL-3"), "SQL-3", true)
	pb := m.toProto()
	assert.Len(t, pb, 3)
	hash := map[string]tipb.SQLMeta{}
	for _, meta := range pb {
		hash[meta.NormalizedSql] = meta
	}
	assert.Equal(t, tipb.SQLMeta{
		SqlDigest:     []byte("SQL-1"),
		NormalizedSql: "SQL-1",
		IsInternalSql: true,
	}, hash["SQL-1"])
	assert.Equal(t, tipb.SQLMeta{
		SqlDigest:     []byte("SQL-2"),
		NormalizedSql: "SQL-2",
		IsInternalSql: false,
	}, hash["SQL-2"])
	assert.Equal(t, tipb.SQLMeta{
		SqlDigest:     []byte("SQL-3"),
		NormalizedSql: "SQL-3",
		IsInternalSql: true,
	}, hash["SQL-3"])
}

func Test_normalizedPlanMap_register(t *testing.T) {
	topsqlstate.GlobalState.MaxCollect.Store(2)
	m := newNormalizedPlanMap()
	m.register([]byte("PLAN-1"), "PLAN-1")
	m.register([]byte("PLAN-2"), "PLAN-2")
	m.register([]byte("PLAN-3"), "PLAN-3")
	assert.Equal(t, int64(2), m.length.Load())
	v, ok := m.data.Load().(*sync.Map).Load("PLAN-1")
	assert.True(t, ok)
	assert.Equal(t, "PLAN-1", v.(string))
	v, ok = m.data.Load().(*sync.Map).Load("PLAN-2")
	assert.True(t, ok)
	assert.Equal(t, "PLAN-2", v.(string))
	_, ok = m.data.Load().(*sync.Map).Load("PLAN-3")
	assert.False(t, ok)
}

func Test_normalizedPlanMap_take(t *testing.T) {
	topsqlstate.GlobalState.MaxCollect.Store(999)
	m1 := newNormalizedPlanMap()
	m1.register([]byte("PLAN-1"), "PLAN-1")
	m1.register([]byte("PLAN-2"), "PLAN-2")
	m1.register([]byte("PLAN-3"), "PLAN-3")
	m2 := m1.take()
	assert.Equal(t, int64(0), m1.length.Load())
	assert.Equal(t, int64(3), m2.length.Load())
	data1 := m1.data.Load().(*sync.Map)
	_, ok := data1.Load("PLAN-1")
	assert.False(t, ok)
	_, ok = data1.Load("PLAN-2")
	assert.False(t, ok)
	_, ok = data1.Load("PLAN-3")
	assert.False(t, ok)
	data2 := m2.data.Load().(*sync.Map)
	_, ok = data2.Load("PLAN-1")
	assert.True(t, ok)
	_, ok = data2.Load("PLAN-2")
	assert.True(t, ok)
	_, ok = data2.Load("PLAN-3")
	assert.True(t, ok)
}

func Test_normalizedPlanMap_toProto(t *testing.T) {
	topsqlstate.GlobalState.MaxCollect.Store(999)
	m := newNormalizedPlanMap()
	m.register([]byte("PLAN-1"), "PLAN-1")
	m.register([]byte("PLAN-2"), "PLAN-2")
	m.register([]byte("PLAN-3"), "PLAN-3")
	pb := m.toProto(func(s string) (string, error) { return s, nil })
	assert.Len(t, pb, 3)
	hash := map[string]tipb.PlanMeta{}
	for _, meta := range pb {
		hash[meta.NormalizedPlan] = meta
	}
	assert.Equal(t, tipb.PlanMeta{
		PlanDigest:     []byte("PLAN-1"),
		NormalizedPlan: "PLAN-1",
	}, hash["PLAN-1"])
	assert.Equal(t, tipb.PlanMeta{
		PlanDigest:     []byte("PLAN-2"),
		NormalizedPlan: "PLAN-2",
	}, hash["PLAN-2"])
	assert.Equal(t, tipb.PlanMeta{
		PlanDigest:     []byte("PLAN-3"),
		NormalizedPlan: "PLAN-3",
	}, hash["PLAN-3"])
}

func Test_encodeKey(t *testing.T) {
	buf := bytes.NewBuffer(make([]byte, 0, 64))
	key := encodeKey(buf, []byte("S"), []byte("P"))
	assert.Equal(t, "SP", key)
}
