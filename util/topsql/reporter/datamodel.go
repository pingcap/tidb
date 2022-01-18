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
	"sync/atomic"

	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/topsql/collector"
	topsqlstate "github.com/pingcap/tidb/util/topsql/state"
	"github.com/pingcap/tidb/util/topsql/stmtstats"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/wangjohn/quickselect"
	atomic2 "go.uber.org/atomic"
	"go.uber.org/zap"
)

// Data naming and relationship in datamodel.go:
//
// tsItem:
//     - timestamp
//     - cpuTime
//     - stmtStats(execCount, durationSum, ...)
//
// tsItems: [ tsItem | tsItem | tsItem | ... ]
//
// record:
//     - tsItems: [ tsItem | tsItem | tsItem | ... ]
//     - tsIndex: { 1640500000 => 0 | 1640500001 => 1 | 1640500002 => 2 | ... }
//
// records: [ record | record | record | ... ]
//
// collecting:
//     - records: { sqlPlanDigest => record | sqlPlanDigest => record | ... }
//     - evicted: { sqlPlanDigest | sqlPlanDigest | ... }
//
// cpuRecords: [ collector.SQLCPUTimeRecord | collector.SQLCPUTimeRecord | ... ]
//
// normalizeSQLMap: { sqlDigest => normalizedSQL | sqlDigest => normalizedSQL | ... }
//
// normalizePlanMap: { planDigest => normalizedPlan | planDigest => normalizedPlan | ... }

const (
	// keyOthers is the key to store the aggregation of all records that is out of Top N.
	keyOthers = ""

	// maxTsItemsCapacity is a protection to avoid excessive memory usage caused by
	// incorrect configuration. The corresponding value defaults to 60 (60 s/min).
	maxTsItemsCapacity = 1000
)

// tsItem is a self-contained complete piece of data for a certain timestamp.
type tsItem struct {
	timestamp uint64
	cpuTimeMs uint32
	stmtStats stmtstats.StatementStatsItem
}

func zeroTsItem() tsItem {
	return tsItem{
		stmtStats: stmtstats.StatementStatsItem{
			KvStatsItem: stmtstats.KvStatementStatsItem{
				KvExecCount: map[string]uint64{},
			},
		},
	}
}

// toProto converts the tsItem to the corresponding protobuf representation.
func (i *tsItem) toProto() *tipb.TopSQLRecordItem {
	return &tipb.TopSQLRecordItem{
		TimestampSec:      i.timestamp,
		CpuTimeMs:         i.cpuTimeMs,
		StmtExecCount:     i.stmtStats.ExecCount,
		StmtKvExecCount:   i.stmtStats.KvStatsItem.KvExecCount,
		StmtDurationSumNs: i.stmtStats.SumDurationNs,
		StmtDurationCount: i.stmtStats.DurationCount,
		// Convert more indicators here.
	}
}

var _ sort.Interface = &tsItems{}

// tsItems is a sortable list of tsItem, sort by tsItem.timestamp (asc).
type tsItems []tsItem

func (ts tsItems) Len() int {
	return len(ts)
}

func (ts tsItems) Less(i, j int) bool {
	return ts[i].timestamp < ts[j].timestamp
}

func (ts tsItems) Swap(i, j int) {
	ts[i], ts[j] = ts[j], ts[i]
}

func (ts tsItems) sorted() bool {
	for n := 0; n < len(ts)-1; n++ {
		if ts[n].timestamp > ts[n+1].timestamp {
			return false
		}
	}
	return true
}

// toProto converts the tsItems to the corresponding protobuf representation.
func (ts tsItems) toProto() []*tipb.TopSQLRecordItem {
	capacity := len(ts)
	if capacity == 0 {
		return nil
	}
	items := make([]*tipb.TopSQLRecordItem, 0, capacity)
	for _, i := range ts {
		items = append(items, i.toProto())
	}
	return items
}

var _ sort.Interface = &record{}

// record represents the cumulative tsItem in current minute window.
// record do not guarantee the tsItems is sorted by timestamp when there is a time jump backward.
// record is also sortable, and the tsIndex will be updated while sorting the internal tsItems.
type record struct {
	sqlDigest      []byte
	planDigest     []byte
	totalCPUTimeMs uint64
	tsItems        tsItems

	// tsIndex is used to quickly find the corresponding tsItems index through timestamp.
	tsIndex map[uint64]int // timestamp => index of tsItems
}

func newRecord(sqlDigest, planDigest []byte) *record {
	listCap := topsqlstate.GlobalState.ReportIntervalSeconds.Load()/topsqlstate.GlobalState.PrecisionSeconds.Load() + 1
	if listCap > maxTsItemsCapacity {
		listCap = maxTsItemsCapacity
	}
	return &record{
		sqlDigest:  sqlDigest,
		planDigest: planDigest,
		tsItems:    make(tsItems, 0, listCap),
		tsIndex:    make(map[uint64]int, listCap),
	}
}

func (r *record) Len() int {
	return r.tsItems.Len()
}

func (r *record) Less(i, j int) bool {
	return r.tsItems.Less(i, j)
}

func (r *record) Swap(i, j int) {
	// before swap:
	//     timestamps: [10000, 10001, 10002]
	//        tsIndex: [10000 => 0, 10001 => 1, 10002 => 2]
	//
	// let i = 0, j = 1
	// after swap tsIndex:
	//     timestamps: [10000, 10001, 10002]
	//        tsIndex: [10000 => 1, 10001 => 0, 10002 => 2]
	//
	// after swap tsItems:
	//     timestamps: [10001, 10000, 10002]
	//        tsIndex: [10000 => 1, 10001 => 0, 10002 => 2]
	r.tsIndex[r.tsItems[i].timestamp], r.tsIndex[r.tsItems[j].timestamp] = r.tsIndex[r.tsItems[j].timestamp], r.tsIndex[r.tsItems[i].timestamp]
	r.tsItems.Swap(i, j)
}

// appendCPUTime appends a cpuTime under a certain timestamp to record.
// If the timestamp already exists in tsItems, then cpuTime will be added.
func (r *record) appendCPUTime(timestamp uint64, cpuTimeMs uint32) {
	if index, ok := r.tsIndex[timestamp]; ok {
		// For the same timestamp, we have already called appendStmtStatsItem,
		// r.tsItems already exists the corresponding timestamp, and the corresponding
		// cpuTimeMs has been set to 0 (or other values, although impossible), so we add it.
		//
		// let timestamp = 10000, cpuTimeMs = 123
		//
		// Before:
		//     tsIndex: [10000 => 0]
		//     tsItems:
		//             timestamp: [10000]
		//             cpuTimeMs: [0]
		//   stmtStats.ExecCount: [?]
		// stmtStats.KvExecCount: [map{"?": ?}]
		// stmtStats.DurationSum: [?]
		//
		// After:
		//     tsIndex: [10000 => 0]
		//     tsItems:
		//             timestamp: [10000]
		//             cpuTimeMs: [123]
		//   stmtStats.ExecCount: [?]
		// stmtStats.KvExecCount: [map{"?": ?}]
		// stmtStats.DurationSum: [?]
		//
		r.tsItems[index].cpuTimeMs += cpuTimeMs
	} else {
		// For this timestamp, we have not appended any tsItem, so append it directly.
		// Other fields in tsItem except cpuTimeMs will be initialized to 0.
		//
		// let timestamp = 10000, cpu_time = 123
		//
		// Before:
		//     tsIndex: []
		//     tsItems:
		//             timestamp: []
		//             cpuTimeMs: []
		//   stmtStats.ExecCount: []
		// stmtStats.KvExecCount: []
		// stmtStats.DurationSum: []
		//
		// After:
		//     tsIndex: [10000 => 0]
		//     tsItems:
		//             timestamp: [10000]
		//             cpuTimeMs: [123]
		//   stmtStats.ExecCount: [0]
		// stmtStats.KvExecCount: [map{}]
		// stmtStats.DurationSum: [0]
		//
		newItem := zeroTsItem()
		newItem.timestamp = timestamp
		newItem.cpuTimeMs = cpuTimeMs
		r.tsIndex[timestamp] = len(r.tsItems)
		r.tsItems = append(r.tsItems, newItem)
	}
	r.totalCPUTimeMs += uint64(cpuTimeMs)
}

// appendStmtStatsItem appends a stmtstats.StatementStatsItem under a certain timestamp to record.
// If the timestamp already exists in tsItems, then stmtstats.StatementStatsItem will be merged.
func (r *record) appendStmtStatsItem(timestamp uint64, item stmtstats.StatementStatsItem) {
	if index, ok := r.tsIndex[timestamp]; ok {
		// For the same timestamp, we have already called appendCPUTime,
		// r.tsItems already exists the corresponding timestamp, and the
		// corresponding stmtStats has been set to 0 (or other values,
		// although impossible), so we merge it.
		//
		// let timestamp = 10000, execCount = 123, kvExecCount = map{"1.1.1.1:1": 123}, durationSum = 456
		//
		// Before:
		//     tsIndex: [10000 => 0]
		//     tsItems:
		//             timestamp: [10000]
		//             cpuTimeMs: [?]
		//   stmtStats.ExecCount: [0]
		// stmtStats.KvExecCount: [map{}]
		// stmtStats.DurationSum: [0]
		//
		// After:
		//     tsIndex: [10000 => 0]
		//     tsItems:
		//             timestamp: [10000]
		//             cpuTimeMs: [?]
		//   stmtStats.ExecCount: [123]
		// stmtStats.KvExecCount: [map{"1.1.1.1:1": 123}]
		// stmtStats.DurationSum: [456]
		//
		r.tsItems[index].stmtStats.Merge(&item)
	} else {
		// For this timestamp, we have not appended any tsItem, so append it directly.
		// Other fields in tsItem except stmtStats will be initialized to 0.
		//
		// let timestamp = 10000, execCount = 123, kvExecCount = map{"1.1.1.1:1": 123}, durationSum = 456
		//
		// Before:
		//     tsIndex: []
		//     tsItems:
		//             timestamp: []
		//             cpuTimeMs: []
		//   stmtStats.ExecCount: []
		// stmtStats.KvExecCount: []
		// stmtStats.DurationSum: []
		//
		// After:
		//     tsIndex: [10000 => 0]
		//     tsItems:
		//             timestamp: [10000]
		//             cpuTimeMs: [0]
		//   stmtStats.ExecCount: [123]
		// stmtStats.KvExecCount: [map{"1.1.1.1:1": 123}]
		// stmtStats.DurationSum: [456]
		//
		newItem := zeroTsItem()
		newItem.timestamp = timestamp
		newItem.stmtStats = item
		r.tsIndex[timestamp] = len(r.tsItems)
		r.tsItems = append(r.tsItems, newItem)
	}
}

// merge other record into r.
// Attention, this function depend on r is sorted, and will sort `other` by timestamp.
func (r *record) merge(other *record) {
	if other == nil || len(other.tsItems) == 0 {
		return
	}

	if !other.tsItems.sorted() {
		sort.Sort(other) // this may never happen
	}
	if len(r.tsItems) == 0 {
		r.totalCPUTimeMs = other.totalCPUTimeMs
		r.tsItems = other.tsItems
		r.tsIndex = other.tsIndex
		return
	}
	length := len(r.tsItems) + len(other.tsItems)
	newTsItems := make(tsItems, 0, length)
	i, j := 0, 0
	for i < len(r.tsItems) && j < len(other.tsItems) {
		if r.tsItems[i].timestamp == other.tsItems[j].timestamp {
			newItem := zeroTsItem()
			newItem.timestamp = r.tsItems[i].timestamp
			newItem.cpuTimeMs = r.tsItems[i].cpuTimeMs + other.tsItems[j].cpuTimeMs
			r.tsItems[i].stmtStats.Merge(&other.tsItems[j].stmtStats)
			newItem.stmtStats = r.tsItems[i].stmtStats
			newTsItems = append(newTsItems, newItem)
			i++
			j++
		} else if r.tsItems[i].timestamp < other.tsItems[j].timestamp {
			newItem := zeroTsItem()
			newItem.timestamp = r.tsItems[i].timestamp
			newItem.cpuTimeMs = r.tsItems[i].cpuTimeMs
			newItem.stmtStats = r.tsItems[i].stmtStats
			newTsItems = append(newTsItems, newItem)
			i++
		} else {
			newItem := zeroTsItem()
			newItem.timestamp = other.tsItems[j].timestamp
			newItem.cpuTimeMs = other.tsItems[j].cpuTimeMs
			newItem.stmtStats = other.tsItems[j].stmtStats
			newTsItems = append(newTsItems, newItem)
			j++
		}
	}
	if i < len(r.tsItems) {
		newTsItems = append(newTsItems, r.tsItems[i:]...)
	}
	if j < len(other.tsItems) {
		newTsItems = append(newTsItems, other.tsItems[j:]...)
	}
	r.tsItems = newTsItems
	r.totalCPUTimeMs += other.totalCPUTimeMs
	r.rebuildTsIndex()
}

// rebuildTsIndex rebuilds the entire tsIndex based on tsItems.
func (r *record) rebuildTsIndex() {
	if len(r.tsItems) == 0 {
		r.tsIndex = map[uint64]int{}
		return
	}
	r.tsIndex = make(map[uint64]int, len(r.tsItems))
	for index, item := range r.tsItems {
		r.tsIndex[item.timestamp] = index
	}
}

// toProto converts the record to the corresponding protobuf representation.
func (r *record) toProto() tipb.TopSQLRecord {
	return tipb.TopSQLRecord{
		SqlDigest:  r.sqlDigest,
		PlanDigest: r.planDigest,
		Items:      r.tsItems.toProto(),
	}
}

var _ sort.Interface = &records{}

// records is a sortable list of record, sort by record.totalCPUTimeMs (desc).
type records []record

func (rs records) Len() int {
	return len(rs)
}

func (rs records) Less(i, j int) bool {
	// Order by totalCPUTimeMs **DESC**.
	return rs[i].totalCPUTimeMs > rs[j].totalCPUTimeMs
}

func (rs records) Swap(i, j int) {
	rs[i], rs[j] = rs[j], rs[i]
}

// topN returns the largest n records (by record.totalCPUTimeMs), other
// records are returned as evicted.
func (rs records) topN(n int) (top, evicted records) {
	if len(rs) <= n {
		return rs, nil
	}
	if err := quickselect.QuickSelect(rs, n); err != nil {
		return rs, nil
	}
	return rs[:n], rs[n:]
}

// toProto converts the records to the corresponding protobuf representation.
func (rs records) toProto() []tipb.TopSQLRecord {
	pb := make([]tipb.TopSQLRecord, 0, len(rs))
	for _, r := range rs {
		pb = append(pb, r.toProto())
	}
	return pb
}

// collecting includes the collection of data being collected by the reporter.
type collecting struct {
	records map[string]*record             // sqlPlanDigest => record
	evicted map[uint64]map[string]struct{} // { sqlPlanDigest }
	keyBuf  *bytes.Buffer
}

func newCollecting() *collecting {
	return &collecting{
		records: map[string]*record{},
		evicted: map[uint64]map[string]struct{}{},
		keyBuf:  bytes.NewBuffer(make([]byte, 0, 64)),
	}
}

// getOrCreateRecord gets the record corresponding to sqlDigest + planDigest, if it
// does not exist, it will be created.
func (c *collecting) getOrCreateRecord(sqlDigest, planDigest []byte) *record {
	key := encodeKey(c.keyBuf, sqlDigest, planDigest)
	r, ok := c.records[key]
	if !ok {
		r = newRecord(sqlDigest, planDigest)
		c.records[key] = r
	}
	return r
}

// markAsEvicted marks sqlDigest + planDigest under a certain timestamp as "evicted".
// Later, we can determine whether a certain sqlDigest + planDigest within a certain
// timestamp has been evicted.
func (c *collecting) markAsEvicted(timestamp uint64, sqlDigest, planDigest []byte) {
	if _, ok := c.evicted[timestamp]; !ok {
		c.evicted[timestamp] = map[string]struct{}{}
	}
	c.evicted[timestamp][encodeKey(c.keyBuf, sqlDigest, planDigest)] = struct{}{}
}

// hasEvicted determines whether a certain sqlDigest + planDigest has been evicted
// in a certain timestamp.
func (c *collecting) hasEvicted(timestamp uint64, sqlDigest, planDigest []byte) bool {
	if digestSet, ok := c.evicted[timestamp]; ok {
		if _, ok := digestSet[encodeKey(c.keyBuf, sqlDigest, planDigest)]; ok {
			return true
		}
	}
	return false
}

// appendOthersCPUTime appends totalCPUTimeMs to a special record named "others".
func (c *collecting) appendOthersCPUTime(timestamp uint64, totalCPUTimeMs uint32) {
	if totalCPUTimeMs == 0 {
		return
	}
	others, ok := c.records[keyOthers]
	if !ok {
		others = newRecord(nil, nil)
		c.records[keyOthers] = others
	}
	others.appendCPUTime(timestamp, totalCPUTimeMs)
}

// appendOthersStmtStatsItem appends stmtstats.StatementStatsItem to a special record named "others".
func (c *collecting) appendOthersStmtStatsItem(timestamp uint64, item stmtstats.StatementStatsItem) {
	others, ok := c.records[keyOthers]
	if !ok {
		others = newRecord(nil, nil)
		c.records[keyOthers] = others
	}
	others.appendStmtStatsItem(timestamp, item)
}

// removeInvalidPlanRecord remove "" plan if there are only 1 valid plan in the record.
func (c *collecting) removeInvalidPlanRecord() {
	sql2PlansMap := make(map[string][][]byte, len(c.records)) // sql_digest => []plan_digest
	for _, v := range c.records {
		k := string(v.sqlDigest)
		sql2PlansMap[k] = append(sql2PlansMap[k], v.planDigest)
	}
	for k, plans := range sql2PlansMap {
		if len(plans) != 2 {
			continue
		}
		if len(plans[0]) > 0 && len(plans[1]) > 0 {
			continue
		}

		sqlDigest := []byte(k)
		key0 := encodeKey(c.keyBuf, sqlDigest, plans[0])
		key1 := encodeKey(c.keyBuf, sqlDigest, plans[1])
		record0, ok0 := c.records[key0]
		record1, ok1 := c.records[key1]
		if !ok0 || !ok1 {
			continue
		}
		if len(plans[0]) != 0 {
			record0.merge(record1)
			delete(c.records, key1)
		} else {
			record1.merge(record0)
			delete(c.records, key0)
		}
	}
}

// getReportRecords returns all records, others record will be packed and appended to the end.
func (c *collecting) getReportRecords() records {
	others := c.records[keyOthers]
	delete(c.records, keyOthers)

	c.removeInvalidPlanRecord()

	rs := make(records, 0, len(c.records))
	for _, v := range c.records {
		rs = append(rs, *v)
	}
	if others != nil && others.totalCPUTimeMs > 0 {
		rs = append(rs, *others)
	}
	return rs
}

// take away all data inside collecting, put them in the returned new collecting.
func (c *collecting) take() *collecting {
	r := &collecting{
		records: c.records,
		evicted: c.evicted,
		keyBuf:  bytes.NewBuffer(make([]byte, 0, 64)),
	}
	c.records = map[string]*record{}
	c.evicted = map[uint64]map[string]struct{}{}
	return r
}

// cpuRecords is a sortable list of collector.SQLCPUTimeRecord, sort by CPUTimeMs (desc).
type cpuRecords []collector.SQLCPUTimeRecord

func (rs cpuRecords) Len() int {
	return len(rs)
}

func (rs cpuRecords) Less(i, j int) bool {
	// Order by CPUTimeMs **DESC**.
	return rs[i].CPUTimeMs > rs[j].CPUTimeMs
}

func (rs cpuRecords) Swap(i, j int) {
	rs[i], rs[j] = rs[j], rs[i]
}

// topN returns the largest n cpuRecords (by CPUTimeMs), other cpuRecords are returned as evicted.
func (rs cpuRecords) topN(n int) (top, evicted cpuRecords) {
	if len(rs) <= n {
		return rs, nil
	}
	if err := quickselect.QuickSelect(rs, n); err != nil {
		return rs, nil
	}
	return rs[:n], rs[n:]
}

// sqlMeta is the SQL meta which contains the normalized SQL string and a bool
// field which uses to distinguish internal SQL.
type sqlMeta struct {
	normalizedSQL string
	isInternal    bool
}

// normalizedSQLMap is a wrapped map used to register normalizedSQL.
type normalizedSQLMap struct {
	data   atomic.Value // *sync.Map
	length atomic2.Int64
}

func newNormalizedSQLMap() *normalizedSQLMap {
	r := &normalizedSQLMap{}
	r.data.Store(&sync.Map{})
	return r
}

// register saves the relationship between sqlDigest and normalizedSQL.
// If the internal map size exceeds the limit, the relationship will be discarded.
func (m *normalizedSQLMap) register(sqlDigest []byte, normalizedSQL string, isInternal bool) {
	if m.length.Load() >= topsqlstate.GlobalState.MaxCollect.Load() {
		ignoreExceedSQLCounter.Inc()
		return
	}
	data := m.data.Load().(*sync.Map)
	_, loaded := data.LoadOrStore(string(sqlDigest), sqlMeta{
		normalizedSQL: normalizedSQL,
		isInternal:    isInternal,
	})
	if !loaded {
		m.length.Add(1)
	}
}

// take away all data inside normalizedSQLMap, put them in the returned new normalizedSQLMap.
func (m *normalizedSQLMap) take() *normalizedSQLMap {
	data := m.data.Load().(*sync.Map)
	length := m.length.Load()
	r := &normalizedSQLMap{}
	r.data.Store(data)
	r.length.Store(length)
	m.data.Store(&sync.Map{})
	m.length.Store(0)
	return r
}

// toProto converts the normalizedSQLMap to the corresponding protobuf representation.
func (m *normalizedSQLMap) toProto() []tipb.SQLMeta {
	metas := make([]tipb.SQLMeta, 0, m.length.Load())
	m.data.Load().(*sync.Map).Range(func(k, v interface{}) bool {
		meta := v.(sqlMeta)
		metas = append(metas, tipb.SQLMeta{
			SqlDigest:     []byte(k.(string)),
			NormalizedSql: meta.normalizedSQL,
			IsInternalSql: meta.isInternal,
		})
		return true
	})
	return metas
}

// planBinaryDecodeFunc is used to decode the value when converting
// normalizedPlanMap to protobuf representation.
type planBinaryDecodeFunc func(string) (string, error)

// normalizedSQLMap is a wrapped map used to register normalizedPlan.
type normalizedPlanMap struct {
	data   atomic.Value // *sync.Map
	length atomic2.Int64
}

func newNormalizedPlanMap() *normalizedPlanMap {
	r := &normalizedPlanMap{}
	r.data.Store(&sync.Map{})
	return r
}

// register saves the relationship between planDigest and normalizedPlan.
// If the internal map size exceeds the limit, the relationship will be discarded.
func (m *normalizedPlanMap) register(planDigest []byte, normalizedPlan string) {
	if m.length.Load() >= topsqlstate.GlobalState.MaxCollect.Load() {
		ignoreExceedPlanCounter.Inc()
		return
	}
	data := m.data.Load().(*sync.Map)
	_, loaded := data.LoadOrStore(string(planDigest), normalizedPlan)
	if !loaded {
		m.length.Add(1)
	}
}

// take away all data inside normalizedPlanMap, put them in the returned new normalizedPlanMap.
func (m *normalizedPlanMap) take() *normalizedPlanMap {
	data := m.data.Load().(*sync.Map)
	length := m.length.Load()
	r := &normalizedPlanMap{}
	r.data.Store(data)
	r.length.Store(length)
	m.data.Store(&sync.Map{})
	m.length.Store(0)
	return r
}

// toProto converts the normalizedPlanMap to the corresponding protobuf representation.
func (m *normalizedPlanMap) toProto(decodePlan planBinaryDecodeFunc) []tipb.PlanMeta {
	metas := make([]tipb.PlanMeta, 0, m.length.Load())
	m.data.Load().(*sync.Map).Range(func(k, v interface{}) bool {
		planDecoded, errDecode := decodePlan(v.(string))
		if errDecode != nil {
			logutil.BgLogger().Warn("[top-sql] decode plan failed", zap.Error(errDecode))
			return true
		}
		metas = append(metas, tipb.PlanMeta{
			PlanDigest:     []byte(k.(string)),
			NormalizedPlan: planDecoded,
		})
		return true
	})
	return metas
}

func encodeKey(buf *bytes.Buffer, sqlDigest, planDigest []byte) string {
	buf.Reset()
	buf.Write(sqlDigest)
	buf.Write(planDigest)
	return buf.String()
}
