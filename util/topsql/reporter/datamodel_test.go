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
	"github.com/stretchr/testify/require"
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
	require.Equal(t, uint64(1), pb.TimestampSec)
	require.Equal(t, uint32(2), pb.CpuTimeMs)
	require.Equal(t, uint64(3), pb.StmtExecCount)
	require.Equal(t, uint64(50000), pb.StmtDurationSumNs)
	require.Equal(t, uint64(2), pb.StmtDurationCount)
	require.Equal(t, uint64(4), pb.StmtKvExecCount[""])
}

func Test_tsItems_Sort(t *testing.T) {
	items := tsItems{}
	require.True(t, items.sorted())
	items = nil
	require.True(t, items.sorted())
	items = tsItems{
		{timestamp: 2},
		{timestamp: 3},
		{timestamp: 1},
	}
	require.False(t, items.sorted())
	sort.Sort(items)
	require.True(t, items.sorted())
	require.Equal(t, uint64(1), items[0].timestamp)
	require.Equal(t, uint64(2), items[1].timestamp)
	require.Equal(t, uint64(3), items[2].timestamp)
}

func Test_tsItems_toProto(t *testing.T) {
	items := &tsItems{{}, {}, {}}
	pb := items.toProto()
	require.Len(t, pb, 3)
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
	require.Equal(t, uint64(1), r.tsItems[0].timestamp)
	require.Equal(t, uint64(2), r.tsItems[1].timestamp)
	require.Equal(t, uint64(3), r.tsItems[2].timestamp)
	require.Equal(t, 0, r.tsIndex[1])
	require.Equal(t, 1, r.tsIndex[2])
	require.Equal(t, 2, r.tsIndex[3])
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

	require.Len(t, r.tsItems, 3)
	require.Len(t, r.tsIndex, 3)
	require.Equal(t, uint64(3), r.totalCPUTimeMs)
	require.Equal(t, uint64(1), r.tsItems[0].timestamp)
	require.Equal(t, uint64(2), r.tsItems[1].timestamp)
	require.Equal(t, uint64(3), r.tsItems[2].timestamp)
	require.Equal(t, uint32(1), r.tsItems[0].cpuTimeMs)
	require.Equal(t, uint32(1), r.tsItems[1].cpuTimeMs)
	require.Equal(t, uint32(1), r.tsItems[2].cpuTimeMs)
	require.Equal(t, uint64(1), r.tsItems[0].stmtStats.ExecCount)
	require.Equal(t, uint64(1), r.tsItems[1].stmtStats.ExecCount)
	require.Equal(t, uint64(1), r.tsItems[2].stmtStats.ExecCount)
	require.Equal(t, uint64(10000), r.tsItems[0].stmtStats.SumDurationNs)
	require.Equal(t, uint64(20000), r.tsItems[1].stmtStats.SumDurationNs)
	require.Equal(t, uint64(30000), r.tsItems[2].stmtStats.SumDurationNs)
}

func Test_record_merge(t *testing.T) {
	r1 := record{
		totalCPUTimeMs: 1 + 2 + 3,
		tsItems: tsItems{
			{timestamp: 1, cpuTimeMs: 1, stmtStats: *stmtstats.NewStatementStatsItem()},
			{timestamp: 2, cpuTimeMs: 2, stmtStats: *stmtstats.NewStatementStatsItem()},
			{timestamp: 3, cpuTimeMs: 3, stmtStats: *stmtstats.NewStatementStatsItem()},
		},
	}
	r1.rebuildTsIndex()
	r2 := record{
		totalCPUTimeMs: 6 + 5 + 4,
		tsItems: tsItems{
			{timestamp: 6, cpuTimeMs: 6, stmtStats: *stmtstats.NewStatementStatsItem()},
			{timestamp: 5, cpuTimeMs: 5, stmtStats: *stmtstats.NewStatementStatsItem()},
			{timestamp: 4, cpuTimeMs: 4, stmtStats: *stmtstats.NewStatementStatsItem()},
		},
	}
	r2.rebuildTsIndex()
	r1.merge(&r2)
	require.Equal(t, uint64(4), r2.tsItems[0].timestamp)
	require.Equal(t, uint64(5), r2.tsItems[1].timestamp)
	require.Equal(t, uint64(6), r2.tsItems[2].timestamp)
	require.Len(t, r1.tsItems, 6)
	require.Len(t, r1.tsIndex, 6)
	require.Equal(t, uint64(1), r1.tsItems[0].timestamp)
	require.Equal(t, uint64(2), r1.tsItems[1].timestamp)
	require.Equal(t, uint64(3), r1.tsItems[2].timestamp)
	require.Equal(t, uint64(4), r1.tsItems[3].timestamp)
	require.Equal(t, uint64(5), r1.tsItems[4].timestamp)
	require.Equal(t, uint64(6), r1.tsItems[5].timestamp)
	require.Equal(t, uint64(1+2+3+4+5+6), r1.totalCPUTimeMs)
}

func Test_record_rebuildTsIndex(t *testing.T) {
	r := record{tsIndex: map[uint64]int{1: 1}}
	r.rebuildTsIndex()
	require.Empty(t, r.tsIndex)
	r.tsItems = tsItems{
		{timestamp: 1, cpuTimeMs: 1},
		{timestamp: 2, cpuTimeMs: 2},
		{timestamp: 3, cpuTimeMs: 3},
	}
	r.rebuildTsIndex()
	require.Len(t, r.tsIndex, 3)
	require.Equal(t, 0, r.tsIndex[1])
	require.Equal(t, 1, r.tsIndex[2])
	require.Equal(t, 2, r.tsIndex[3])
}

func Test_record_toProto(t *testing.T) {
	r := record{
		sqlDigest:      []byte("SQL-1"),
		planDigest:     []byte("PLAN-1"),
		totalCPUTimeMs: 123,
		tsItems:        tsItems{{}, {}, {}},
	}
	pb := r.toProto()
	require.Equal(t, []byte("SQL-1"), pb.SqlDigest)
	require.Equal(t, []byte("PLAN-1"), pb.PlanDigest)
	require.Len(t, pb.Items, 3)
}

func Test_records_Sort(t *testing.T) {
	rs := records{
		{totalCPUTimeMs: 1},
		{totalCPUTimeMs: 3},
		{totalCPUTimeMs: 2},
	}
	sort.Sort(rs)
	require.Equal(t, uint64(3), rs[0].totalCPUTimeMs)
	require.Equal(t, uint64(2), rs[1].totalCPUTimeMs)
	require.Equal(t, uint64(1), rs[2].totalCPUTimeMs)
}

func Test_records_topN(t *testing.T) {
	rs := records{
		{totalCPUTimeMs: 1},
		{totalCPUTimeMs: 3},
		{totalCPUTimeMs: 2},
	}
	top, evicted := rs.topN(2)
	require.Len(t, top, 2)
	require.Len(t, evicted, 1)
	require.Equal(t, uint64(3), top[0].totalCPUTimeMs)
	require.Equal(t, uint64(2), top[1].totalCPUTimeMs)
	require.Equal(t, uint64(1), evicted[0].totalCPUTimeMs)
}

func Test_records_toProto(t *testing.T) {
	rs := records{{}, {}}
	pb := rs.toProto()
	require.Len(t, pb, 2)
}

func Test_collecting_getOrCreateRecord(t *testing.T) {
	c := newCollecting()
	r1 := c.getOrCreateRecord([]byte("SQL-1"), []byte("PLAN-1"))
	require.NotNil(t, r1)
	r2 := c.getOrCreateRecord([]byte("SQL-1"), []byte("PLAN-1"))
	require.Equal(t, r1, r2)
}

func Test_collecting_markAsEvicted_hasEvicted(t *testing.T) {
	c := newCollecting()
	c.markAsEvicted(1, []byte("SQL-1"), []byte("PLAN-1"))
	require.True(t, c.hasEvicted(1, []byte("SQL-1"), []byte("PLAN-1")))
	require.False(t, c.hasEvicted(1, []byte("SQL-2"), []byte("PLAN-2")))
	require.False(t, c.hasEvicted(2, []byte("SQL-1"), []byte("PLAN-1")))
}

func Test_collecting_appendOthers(t *testing.T) {
	c := newCollecting()
	c.appendOthersCPUTime(1, 1)
	c.appendOthersCPUTime(2, 2)
	c.appendOthersStmtStatsItem(1, stmtstats.StatementStatsItem{ExecCount: 1, SumDurationNs: 1000})
	c.appendOthersStmtStatsItem(2, stmtstats.StatementStatsItem{ExecCount: 2, SumDurationNs: 2000})
	r := c.records[keyOthers]
	require.Len(t, r.tsItems, 2)
	require.Len(t, r.tsIndex, 2)
	require.Equal(t, uint64(1), r.tsItems[0].timestamp)
	require.Equal(t, uint64(2), r.tsItems[1].timestamp)
	require.Equal(t, uint32(1), r.tsItems[0].cpuTimeMs)
	require.Equal(t, uint32(2), r.tsItems[1].cpuTimeMs)
	require.Equal(t, uint64(1), r.tsItems[0].stmtStats.ExecCount)
	require.Equal(t, uint64(2), r.tsItems[1].stmtStats.ExecCount)
	require.Equal(t, uint64(1000), r.tsItems[0].stmtStats.SumDurationNs)
	require.Equal(t, uint64(2000), r.tsItems[1].stmtStats.SumDurationNs)
}

func Test_collecting_getReportRecords(t *testing.T) {
	c := newCollecting()
	c.getOrCreateRecord([]byte("SQL-1"), []byte("PLAN-1")).appendCPUTime(1, 1)
	c.getOrCreateRecord([]byte("SQL-2"), []byte("PLAN-2")).appendCPUTime(1, 2)
	c.getOrCreateRecord([]byte("SQL-3"), []byte("PLAN-3")).appendCPUTime(1, 3)
	c.getOrCreateRecord([]byte(keyOthers), []byte(keyOthers)).appendCPUTime(1, 10)
	rs := c.getReportRecords()
	require.Len(t, rs, 4)
	require.Equal(t, uint32(10), rs[3].tsItems[0].cpuTimeMs)
	require.Equal(t, uint64(10), rs[3].totalCPUTimeMs)
}

func Test_collecting_take(t *testing.T) {
	c1 := newCollecting()
	c1.getOrCreateRecord([]byte("SQL-1"), []byte("PLAN-1")).appendCPUTime(1, 1)
	c2 := c1.take()
	require.Empty(t, c1.records)
	require.Len(t, c2.records, 1)
	require.NotEqual(t, c1.keyBuf, c2.keyBuf)
}

func Test_cpuRecords_Sort(t *testing.T) {
	rs := cpuRecords{
		{CPUTimeMs: 1},
		{CPUTimeMs: 3},
		{CPUTimeMs: 2},
	}
	sort.Sort(rs)
	require.Equal(t, uint32(3), rs[0].CPUTimeMs)
	require.Equal(t, uint32(2), rs[1].CPUTimeMs)
	require.Equal(t, uint32(1), rs[2].CPUTimeMs)
}

func Test_cpuRecords_topN(t *testing.T) {
	rs := cpuRecords{
		{CPUTimeMs: 1},
		{CPUTimeMs: 3},
		{CPUTimeMs: 2},
	}
	top, evicted := rs.topN(2)
	require.Len(t, top, 2)
	require.Len(t, evicted, 1)
	require.Equal(t, uint32(3), top[0].CPUTimeMs)
	require.Equal(t, uint32(2), top[1].CPUTimeMs)
	require.Equal(t, uint32(1), evicted[0].CPUTimeMs)
}

func Test_normalizedSQLMap_register(t *testing.T) {
	topsqlstate.GlobalState.MaxCollect.Store(2)
	m := newNormalizedSQLMap()
	m.register([]byte("SQL-1"), "SQL-1", true)
	m.register([]byte("SQL-2"), "SQL-2", false)
	m.register([]byte("SQL-3"), "SQL-3", true)
	require.Equal(t, int64(2), m.length.Load())
	v, ok := m.data.Load().(*sync.Map).Load("SQL-1")
	meta := v.(sqlMeta)
	require.True(t, ok)
	require.Equal(t, "SQL-1", meta.normalizedSQL)
	require.True(t, meta.isInternal)
	v, ok = m.data.Load().(*sync.Map).Load("SQL-2")
	meta = v.(sqlMeta)
	require.True(t, ok)
	require.Equal(t, "SQL-2", meta.normalizedSQL)
	require.False(t, meta.isInternal)
	_, ok = m.data.Load().(*sync.Map).Load("SQL-3")
	require.False(t, ok)
}

func Test_normalizedSQLMap_take(t *testing.T) {
	topsqlstate.GlobalState.MaxCollect.Store(999)
	m1 := newNormalizedSQLMap()
	m1.register([]byte("SQL-1"), "SQL-1", true)
	m1.register([]byte("SQL-2"), "SQL-2", false)
	m1.register([]byte("SQL-3"), "SQL-3", true)
	m2 := m1.take()
	require.Equal(t, int64(0), m1.length.Load())
	require.Equal(t, int64(3), m2.length.Load())
	data1 := m1.data.Load().(*sync.Map)
	_, ok := data1.Load("SQL-1")
	require.False(t, ok)
	_, ok = data1.Load("SQL-2")
	require.False(t, ok)
	_, ok = data1.Load("SQL-3")
	require.False(t, ok)
	data2 := m2.data.Load().(*sync.Map)
	_, ok = data2.Load("SQL-1")
	require.True(t, ok)
	_, ok = data2.Load("SQL-2")
	require.True(t, ok)
	_, ok = data2.Load("SQL-3")
	require.True(t, ok)
}

func Test_normalizedSQLMap_toProto(t *testing.T) {
	topsqlstate.GlobalState.MaxCollect.Store(999)
	m := newNormalizedSQLMap()
	m.register([]byte("SQL-1"), "SQL-1", true)
	m.register([]byte("SQL-2"), "SQL-2", false)
	m.register([]byte("SQL-3"), "SQL-3", true)
	pb := m.toProto()
	require.Len(t, pb, 3)
	hash := map[string]tipb.SQLMeta{}
	for _, meta := range pb {
		hash[meta.NormalizedSql] = meta
	}
	require.Equal(t, tipb.SQLMeta{
		SqlDigest:     []byte("SQL-1"),
		NormalizedSql: "SQL-1",
		IsInternalSql: true,
	}, hash["SQL-1"])
	require.Equal(t, tipb.SQLMeta{
		SqlDigest:     []byte("SQL-2"),
		NormalizedSql: "SQL-2",
		IsInternalSql: false,
	}, hash["SQL-2"])
	require.Equal(t, tipb.SQLMeta{
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
	require.Equal(t, int64(2), m.length.Load())
	v, ok := m.data.Load().(*sync.Map).Load("PLAN-1")
	require.True(t, ok)
	require.Equal(t, "PLAN-1", v.(string))
	v, ok = m.data.Load().(*sync.Map).Load("PLAN-2")
	require.True(t, ok)
	require.Equal(t, "PLAN-2", v.(string))
	_, ok = m.data.Load().(*sync.Map).Load("PLAN-3")
	require.False(t, ok)
}

func Test_normalizedPlanMap_take(t *testing.T) {
	topsqlstate.GlobalState.MaxCollect.Store(999)
	m1 := newNormalizedPlanMap()
	m1.register([]byte("PLAN-1"), "PLAN-1")
	m1.register([]byte("PLAN-2"), "PLAN-2")
	m1.register([]byte("PLAN-3"), "PLAN-3")
	m2 := m1.take()
	require.Equal(t, int64(0), m1.length.Load())
	require.Equal(t, int64(3), m2.length.Load())
	data1 := m1.data.Load().(*sync.Map)
	_, ok := data1.Load("PLAN-1")
	require.False(t, ok)
	_, ok = data1.Load("PLAN-2")
	require.False(t, ok)
	_, ok = data1.Load("PLAN-3")
	require.False(t, ok)
	data2 := m2.data.Load().(*sync.Map)
	_, ok = data2.Load("PLAN-1")
	require.True(t, ok)
	_, ok = data2.Load("PLAN-2")
	require.True(t, ok)
	_, ok = data2.Load("PLAN-3")
	require.True(t, ok)
}

func Test_normalizedPlanMap_toProto(t *testing.T) {
	topsqlstate.GlobalState.MaxCollect.Store(999)
	m := newNormalizedPlanMap()
	m.register([]byte("PLAN-1"), "PLAN-1")
	m.register([]byte("PLAN-2"), "PLAN-2")
	m.register([]byte("PLAN-3"), "PLAN-3")
	pb := m.toProto(func(s string) (string, error) { return s, nil })
	require.Len(t, pb, 3)
	hash := map[string]tipb.PlanMeta{}
	for _, meta := range pb {
		hash[meta.NormalizedPlan] = meta
	}
	require.Equal(t, tipb.PlanMeta{
		PlanDigest:     []byte("PLAN-1"),
		NormalizedPlan: "PLAN-1",
	}, hash["PLAN-1"])
	require.Equal(t, tipb.PlanMeta{
		PlanDigest:     []byte("PLAN-2"),
		NormalizedPlan: "PLAN-2",
	}, hash["PLAN-2"])
	require.Equal(t, tipb.PlanMeta{
		PlanDigest:     []byte("PLAN-3"),
		NormalizedPlan: "PLAN-3",
	}, hash["PLAN-3"])
}

func Test_encodeKey(t *testing.T) {
	buf := bytes.NewBuffer(make([]byte, 0, 64))
	key := encodeKey(buf, []byte("S"), []byte("P"))
	require.Equal(t, "SP", key)
}

func TestRemoveInValidPlanRecord(t *testing.T) {
	c1 := newCollecting()
	rs := []struct {
		sql  string
		plan string
		tss  []uint64
	}{
		{"SQL-1", "PLAN-1", []uint64{1, 2, 3, 5}},
		{"SQL-1", "PLAN-2", []uint64{1, 2, 5, 6}},

		{"SQL-2", "PLAN-1", []uint64{1, 2, 3, 5}},
		{"SQL-2", "", []uint64{1, 2, 3, 4, 6}},

		{"SQL-3", "", []uint64{2, 3, 5}},
		{"SQL-3", "PLAN-1", []uint64{1, 2, 3, 4, 6}},
	}
	for _, r := range rs {
		record := c1.getOrCreateRecord([]byte(r.sql), []byte(r.plan))
		for _, ts := range r.tss {
			record.appendCPUTime(ts, 1)
		}
	}

	c1.removeInvalidPlanRecord()

	result := []struct {
		sql  string
		plan string
		tss  []uint64
		cpus []uint32
	}{
		{"SQL-1", "PLAN-1", []uint64{1, 2, 3, 5}, []uint32{1, 1, 1, 1}},
		{"SQL-1", "PLAN-2", []uint64{1, 2, 5, 6}, []uint32{1, 1, 1, 1}},
		{"SQL-2", "PLAN-1", []uint64{1, 2, 3, 4, 5, 6}, []uint32{2, 2, 2, 1, 1, 1}},
		{"SQL-3", "PLAN-1", []uint64{1, 2, 3, 4, 5, 6}, []uint32{1, 2, 2, 1, 1, 1}},
	}
	require.Equal(t, len(result), len(c1.records))
	buf := bytes.NewBuffer(make([]byte, 0, 64))
	for _, r := range result {
		buf.Reset()
		k := encodeKey(buf, []byte(r.sql), []byte(r.plan))
		record, ok := c1.records[k]
		require.True(t, ok)
		require.Equal(t, []byte(r.sql), record.sqlDigest)
		require.Equal(t, []byte(r.plan), record.planDigest)
		require.Equal(t, len(r.tss), len(record.tsItems))
		for i, ts := range r.tss {
			require.Equal(t, ts, record.tsItems[i].timestamp)
			require.Equal(t, r.cpus[i], record.tsItems[i].cpuTimeMs)
		}
	}
}
