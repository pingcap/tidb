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
	"sort"

	topsqlstate "github.com/pingcap/tidb/pkg/util/topsql/state"
	"github.com/pingcap/tidb/pkg/util/topsql/stmtstats"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/wangjohn/quickselect"
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
	stmtStats stmtstats.StatementStatsItem
	timestamp uint64
	cpuTimeMs uint32
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
		TimestampSec:        i.timestamp,
		CpuTimeMs:           i.cpuTimeMs,
		StmtExecCount:       i.stmtStats.ExecCount,
		StmtKvExecCount:     i.stmtStats.KvStatsItem.KvExecCount,
		StmtDurationSumNs:   i.stmtStats.SumDurationNs,
		StmtDurationCount:   i.stmtStats.DurationCount,
		StmtNetworkInBytes:  i.stmtStats.NetworkInBytes,
		StmtNetworkOutBytes: i.stmtStats.NetworkOutBytes,
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
	for n := range len(ts) - 1 {
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
	// tsIndex is used to quickly find the corresponding tsItems index through timestamp.
	tsIndex        map[uint64]int
	sqlDigest      []byte
	planDigest     []byte
	tsItems        tsItems
	totalCPUTimeMs uint64
}

func newRecord(sqlDigest, planDigest []byte) *record {
	listCap := min(topsqlstate.GlobalState.ReportIntervalSeconds.Load()/topsqlstate.GlobalState.PrecisionSeconds.Load()+1, maxTsItemsCapacity)
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
		//             timestamp:     [10000]
		//             cpuTimeMs:     [0]
		//   stmtStats.ExecCount:     [?]
		// stmtStats.KvExecCount:     [map{"?": ?}]
		// stmtStats.DurationSum:     [?]
		// stmtStats.NetworkInBytes:  [?]
		// stmtStats.NetworkOutBytes: [?]
		//
		// After:
		//     tsIndex: [10000 => 0]
		//     tsItems:
		//             timestamp:     [10000]
		//             cpuTimeMs:     [123]
		//   stmtStats.ExecCount:     [?]
		// stmtStats.KvExecCount:     [map{"?": ?}]
		// stmtStats.DurationSum:     [?]
		// stmtStats.DurationSum:     [?]
		// stmtStats.NetworkInBytes:  [?]
		// stmtStats.NetworkOutBytes: [?]
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
		//             timestamp:     []
		//             cpuTimeMs:     []
		//   stmtStats.ExecCount:     []
		// stmtStats.KvExecCount:     []
		// stmtStats.DurationSum:     []
		// stmtStats.NetworkInBytes:  []
		// stmtStats.NetworkOutBytes: []
		//
		// After:
		//     tsIndex: [10000 => 0]
		//     tsItems:
		//             timestamp:     [10000]
		//             cpuTimeMs:     [123]
		//   stmtStats.ExecCount:     [0]
		// stmtStats.KvExecCount:     [map{}]
		// stmtStats.DurationSum:     [0]
		// stmtStats.NetworkInBytes:  [0]
		// stmtStats.NetworkOutBytes: [0]
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
		// let timestamp = 10000, execCount = 123, kvExecCount = map{"1.1.1.1:1": 123}, durationSum = 456,
		//    networkInBytes = 10, networkOutBytes = 20
		// Before:
		//     tsIndex: [10000 => 0]
		//     tsItems:
		//             timestamp:     [10000]
		//             cpuTimeMs:     [?]
		//   stmtStats.ExecCount:     [0]
		// stmtStats.KvExecCount:     [map{}]
		// stmtStats.DurationSum:     [0]
		// stmtStats.NetworkInBytes:  [0]
		// stmtStats.NetworkOutBytes: [0]
		//
		// After:
		//     tsIndex: [10000 => 0]
		//     tsItems:
		//             timestamp:     [10000]
		//             cpuTimeMs:     [?]
		//   stmtStats.ExecCount:     [123]
		// stmtStats.KvExecCount:     [map{"1.1.1.1:1": 123}]
		// stmtStats.DurationSum:     [456]
		// stmtStats.NetworkInBytes:  [10]
		// stmtStats.NetworkOutBytes: [20]
		//
		r.tsItems[index].stmtStats.Merge(&item)
	} else {
		// For this timestamp, we have not appended any tsItem, so append it directly.
		// Other fields in tsItem except stmtStats will be initialized to 0.
		//
		// let timestamp = 10000, execCount = 123, kvExecCount = map{"1.1.1.1:1": 123}, durationSum = 456
		//    networkInBytes = 10, networkOutBytes = 20
		//
		// Before:
		//     tsIndex: []
		//     tsItems:
		//             timestamp:     []
		//             cpuTimeMs:     []
		//   stmtStats.ExecCount:     []
		// stmtStats.KvExecCount:     []
		// stmtStats.DurationSum:     []
		// stmtStats.NetworkInBytes:  []
		// stmtStats.NetworkOutBytes: []
		//
		// After:
		//     tsIndex: [10000 => 0]
		//     tsItems:
		//             timestamp:     [10000]
		//             cpuTimeMs:     [0]
		//   stmtStats.ExecCount:     [123]
		// stmtStats.KvExecCount:     [map{"1.1.1.1:1": 123}]
		// stmtStats.DurationSum:     [456]
		// stmtStats.NetworkInBytes:  [10]
		// stmtStats.NetworkOutBytes: [20]
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
func (r *record) toProto(keyspaceName []byte) tipb.TopSQLRecord {
	return tipb.TopSQLRecord{
		KeyspaceName: keyspaceName,
		SqlDigest:    r.sqlDigest,
		PlanDigest:   r.planDigest,
		Items:        r.tsItems.toProto(),
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
func (rs records) toProto(keyspaceName []byte) []tipb.TopSQLRecord {
	pb := make([]tipb.TopSQLRecord, 0, len(rs))
	for _, r := range rs {
		pb = append(pb, r.toProto(keyspaceName))
	}
	return pb
}

// collecting includes the collection of data being collected by the reporter.
