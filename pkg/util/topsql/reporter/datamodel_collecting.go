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
	"sync"
	"sync/atomic"

	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/topsql/collector"
	topsqlstate "github.com/pingcap/tidb/pkg/util/topsql/state"
	"github.com/pingcap/tidb/pkg/util/topsql/stmtstats"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/wangjohn/quickselect"
	atomic2 "go.uber.org/atomic"
	reporter_metrics "github.com/pingcap/tidb/pkg/util/topsql/reporter/metrics"
	"go.uber.org/zap"
)

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
// Basically, it should be called once at the end of the collection, currently in `getReportRecords`.
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
	if others != nil {
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

// planMeta contains a binaryNormalizedPlan and a bool field isLarge to indicate
// whether that binaryNormalizedPlan is too large to decode quickly
type planMeta struct {
	binaryNormalizedPlan string
	isLarge              bool
}

// normalizedSQLMap is a wrapped map used to register normalizedSQL.
type normalizedSQLMap struct {
	data   atomic.Pointer[sync.Map]
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
		reporter_metrics.IgnoreExceedSQLCounter.Inc()
		return
	}
	data := m.data.Load()
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
	data := m.data.Load()
	length := m.length.Load()
	r := &normalizedSQLMap{}
	r.data.Store(data)
	r.length.Store(length)
	m.data.Store(&sync.Map{})
	m.length.Store(0)
	return r
}

// toProto converts the normalizedSQLMap to the corresponding protobuf representation.
func (m *normalizedSQLMap) toProto(keyspaceName []byte) []tipb.SQLMeta {
	metas := make([]tipb.SQLMeta, 0, m.length.Load())
	m.data.Load().Range(func(k, v any) bool {
		meta := v.(sqlMeta)
		metas = append(metas, tipb.SQLMeta{
			KeyspaceName:  keyspaceName,
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

// planBinaryCompressFunc is used to compress large normalized plan
// into encoded format
type planBinaryCompressFunc func([]byte) string

// normalizedSQLMap is a wrapped map used to register normalizedPlan.
type normalizedPlanMap struct {
	data   atomic.Pointer[sync.Map]
	length atomic2.Int64
}

func newNormalizedPlanMap() *normalizedPlanMap {
	r := &normalizedPlanMap{}
	r.data.Store(&sync.Map{})
	return r
}

// register saves the relationship between planDigest and normalizedPlan.
// If the internal map size exceeds the limit, the relationship will be discarded.
func (m *normalizedPlanMap) register(planDigest []byte, normalizedPlan string, isLarge bool) {
	if m.length.Load() >= topsqlstate.GlobalState.MaxCollect.Load() {
		reporter_metrics.IgnoreExceedPlanCounter.Inc()
		return
	}
	data := m.data.Load()
	_, loaded := data.LoadOrStore(string(planDigest), planMeta{
		binaryNormalizedPlan: normalizedPlan,
		isLarge:              isLarge,
	})
	if !loaded {
		m.length.Add(1)
	}
}

// take away all data inside normalizedPlanMap, put them in the returned new normalizedPlanMap.
func (m *normalizedPlanMap) take() *normalizedPlanMap {
	data := m.data.Load()
	length := m.length.Load()
	r := &normalizedPlanMap{}
	r.data.Store(data)
	r.length.Store(length)
	m.data.Store(&sync.Map{})
	m.length.Store(0)
	return r
}

// toProto converts the normalizedPlanMap to the corresponding protobuf representation.
func (m *normalizedPlanMap) toProto(keyspaceName []byte, decodePlan planBinaryDecodeFunc, compressPlan planBinaryCompressFunc) []tipb.PlanMeta {
	metas := make([]tipb.PlanMeta, 0, m.length.Load())
	m.data.Load().Range(func(k, v any) bool {
		originalMeta := v.(planMeta)
		protoMeta := tipb.PlanMeta{
			KeyspaceName: keyspaceName,
			PlanDigest:   hack.Slice(k.(string)),
		}

		var err error
		if originalMeta.isLarge {
			protoMeta.EncodedNormalizedPlan = compressPlan(hack.Slice(originalMeta.binaryNormalizedPlan))
		} else {
			protoMeta.NormalizedPlan, err = decodePlan(originalMeta.binaryNormalizedPlan)
		}
		if err != nil {
			logutil.BgLogger().Warn("decode plan failed", zap.String("category", "top-sql"), zap.Error(err))
			return true
		}

		metas = append(metas, protoMeta)
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
