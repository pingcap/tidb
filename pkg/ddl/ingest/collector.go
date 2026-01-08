// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ingest

import (
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

var tempIndexOpsCollectorInstance = newTempIndexOpsCollector()

func init() {
	prometheus.MustRegister(tempIndexOpsCollectorInstance)

	metrics.DDLCommitTempIndexWrite = tempIndexOpsCollectorInstance.commitWrites
	metrics.DDLAddOneTempIndexWrite = tempIndexOpsCollectorInstance.addWrite
	metrics.DDLRollbackTempIndexWrite = tempIndexOpsCollectorInstance.rollbackWrites
	metrics.DDLClearTempIndexWrite = tempIndexOpsCollectorInstance.clearConn
	metrics.DDLSetTempIndexScanAndMerge = tempIndexOpsCollectorInstance.addScanAndMerge
	metrics.DDLClearTempIndexOps = tempIndexOpsCollectorInstance.clearTable
}

const (
	labelSingleWrite = "single_write"
	labelDoubleWrite = "double_write"
	labelMerge       = "merge"
	labelScan        = "scan"
)

type tempIndexOpsCollector struct {
	// write tracks temp-index writes scoped by connection.
	// connID => *connWriteState
	write sync.Map
	// read tracks scan/merge counters scoped by table.
	// tableID => *scanMergeCounters
	read sync.Map

	desc *prometheus.Desc
}

type scanMergeCounters struct {
	merge atomic.Uint64
	scan  atomic.Uint64
}

type connWriteState struct {
	// tableID => *tableWriteCounters
	byTable sync.Map
}

func (s *connWriteState) getOrCreateTableCounters(tableID int64) *tableWriteCounters {
	value, _ := s.byTable.LoadOrStore(tableID, &tableWriteCounters{})
	counters, _ := value.(*tableWriteCounters)
	return counters
}

type tableWriteCounters struct {
	pendingSingleWrites atomic.Uint64
	pendingDoubleWrites atomic.Uint64

	committedSingleWrites atomic.Uint64
	committedDoubleWrites atomic.Uint64
}

func newTempIndexOpsCollector() *tempIndexOpsCollector {
	return &tempIndexOpsCollector{
		desc: prometheus.NewDesc(
			"tidb_ddl_temp_index_op_count",
			"Gauge of temp index operation count",
			[]string{"type", "table_id"}, nil,
		),
	}
}

func (c *tempIndexOpsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.desc
}

func (c *tempIndexOpsCollector) Collect(ch chan<- prometheus.Metric) {
	emit := func(typ string, tableID int64, value uint64) {
		ch <- prometheus.MustNewConstMetric(
			c.desc,
			prometheus.GaugeValue,
			float64(value),
			typ,
			strconv.FormatInt(tableID, 10),
		)
	}

	committedByTable := c.buildCommittedWritesByTable()
	for tableID, counts := range committedByTable {
		emit(labelSingleWrite, tableID, counts[0])
		emit(labelDoubleWrite, tableID, counts[1])
	}

	c.read.Range(func(key, value any) bool {
		tableID, _ := key.(int64)
		counters, _ := value.(*scanMergeCounters)
		emit(labelMerge, tableID, counters.merge.Load())
		emit(labelScan, tableID, counters.scan.Load())
		return true
	})
}

func (c *tempIndexOpsCollector) buildCommittedWritesByTable() map[int64][2]uint64 {
	committedByTable := make(map[int64][2]uint64)
	c.write.Range(func(_, value any) bool {
		state, _ := value.(*connWriteState)
		state.byTable.Range(func(key, tableValue any) bool {
			tableID, _ := key.(int64)
			counters, _ := tableValue.(*tableWriteCounters)
			counts := committedByTable[tableID]
			counts[0] += counters.committedSingleWrites.Load()
			counts[1] += counters.committedDoubleWrites.Load()
			committedByTable[tableID] = counts
			return true
		})
		return true
	})
	return committedByTable
}

func (c *tempIndexOpsCollector) addWrite(connID uint64, tableID int64, doubleWrite bool) {
	state := c.getOrCreateConnWriteState(connID)
	counters := state.getOrCreateTableCounters(tableID)
	if doubleWrite {
		counters.pendingDoubleWrites.Add(1)
		return
	}
	counters.pendingSingleWrites.Add(1)
}

func (c *tempIndexOpsCollector) commitWrites(connID uint64) {
	value, ok := c.write.Load(connID)
	if !ok {
		return
	}
	state, ok := value.(*connWriteState)
	if !ok {
		return
	}
	state.byTable.Range(func(_, tableValue any) bool {
		counters, _ := tableValue.(*tableWriteCounters)
		single := counters.pendingSingleWrites.Swap(0)
		double := counters.pendingDoubleWrites.Swap(0)
		counters.committedSingleWrites.Add(single)
		counters.committedDoubleWrites.Add(double)
		return true
	})
}

func (c *tempIndexOpsCollector) rollbackWrites(connID uint64) {
	value, ok := c.write.Load(connID)
	if !ok {
		return
	}
	state, ok := value.(*connWriteState)
	if !ok {
		return
	}
	state.byTable.Range(func(_, tableValue any) bool {
		counters, _ := tableValue.(*tableWriteCounters)
		counters.pendingSingleWrites.Store(0)
		counters.pendingDoubleWrites.Store(0)
		return true
	})
}

func (c *tempIndexOpsCollector) clearConn(connID uint64) {
	c.write.Delete(connID)
}

func (c *tempIndexOpsCollector) addScanAndMerge(tableID int64, scanCnt, mergeCnt uint64) {
	value, _ := c.read.LoadOrStore(tableID, &scanMergeCounters{})
	counters, _ := value.(*scanMergeCounters)
	counters.scan.Add(scanCnt)
	counters.merge.Add(mergeCnt)
}

func (c *tempIndexOpsCollector) clearTable(tableID int64) {
	c.read.Delete(tableID)
	c.write.Range(func(_, value any) bool {
		state, _ := value.(*connWriteState)
		state.byTable.Delete(tableID)
		return true
	})
}

func (c *tempIndexOpsCollector) getOrCreateConnWriteState(connID uint64) *connWriteState {
	value, _ := c.write.LoadOrStore(connID, &connWriteState{})
	state, _ := value.(*connWriteState)
	return state
}
