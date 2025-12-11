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
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

const (
	labelSingleWrite = "single_write"
	labelDoubleWrite = "double_write"
	labelMerged      = "merged"
	labelScanned     = "scanned"
)

// uncommittedMetric tracks uncommitted metric for a temporary index.
type uncommittedMetric struct {
	singleWrite atomic.Uint64
	doubleWrite atomic.Uint64
}

// uncommittedMetrics tracks all uncommitted metrics for a single connection.
type uncommittedMetrics struct {
	counters sync.Map // tableID => writeCounter
}

// committedMetric tracks all committed metric for a temporary index.
type committedMetric struct {
	singleWrite atomic.Uint64
	doubleWrite atomic.Uint64
	merged      atomic.Uint64
	scanned     atomic.Uint64
}

type tempIndexCollector struct {
	uncommitted sync.Map // connectionID => uncommittedMetrics
	committed   sync.Map // tableID => committedMetric

	gaugeVec *prometheus.GaugeVec

	// tableIDs in last collection
	lastActive sync.Map
}

func newTempIndexCollector() *tempIndexCollector {
	return &tempIndexCollector{
		gaugeVec: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "tidb_ddl_temp_index_op_count",
				Help: "Gauge of temp index operation count",
			},
			[]string{"type", "table_id"},
		),
	}
}

func (c *tempIndexCollector) commit(connID uint64) {
	v, ok := c.uncommitted.Load(connID)
	if !ok {
		return
	}

	//nolint:forcetypeassert
	v.(*uncommittedMetrics).counters.Range(func(key, value any) bool {
		//nolint:forcetypeassert
		tableID := key.(int64)
		//nolint:forcetypeassert
		um := value.(*uncommittedMetric)

		v, _ := c.committed.LoadOrStore(tableID, &committedMetric{})
		//nolint:forcetypeassert
		cm := v.(*committedMetric)
		cm.singleWrite.Add(um.singleWrite.Load())
		cm.doubleWrite.Add(um.doubleWrite.Load())
		cm.singleWrite.Store(0)
		cm.doubleWrite.Store(0)
		return true
	})
}

func (c *tempIndexCollector) rollback(connID uint64) {
	v, ok := c.uncommitted.Load(connID)
	if !ok {
		return
	}

	//nolint:forcetypeassert
	v.(*uncommittedMetrics).counters.Range(func(_, value any) bool {
		//nolint:forcetypeassert
		um := value.(*uncommittedMetric)
		um.singleWrite.Store(0)
		um.doubleWrite.Store(0)
		return true
	})
}
func (c *tempIndexCollector) addTempIndexEntry(connID uint64, tableID int64, doubleWrite bool) {
	v, _ := c.uncommitted.LoadOrStore(connID, &uncommittedMetrics{})
	//nolint:forcetypeassert
	w, _ := v.(*uncommittedMetrics).counters.LoadOrStore(tableID, &uncommittedMetric{})
	//nolint:forcetypeassert
	um := w.(*uncommittedMetric)
	if doubleWrite {
		um.doubleWrite.Add(1)
	} else {
		um.singleWrite.Add(1)
	}
}

func (c *tempIndexCollector) addTempIndexProcessed(tableID int64, scanned, merged uint64) {
	v, _ := c.committed.LoadOrStore(tableID, &committedMetric{})
	//nolint:forcetypeassert
	cm := v.(*committedMetric)
	cm.scanned.Add(scanned)
	cm.merged.Add(merged)
}

func (c *tempIndexCollector) removeTempIndex(tableID int64) {
	c.uncommitted.Range(func(_, value any) bool {
		//nolint:forcetypeassert
		value.(*uncommittedMetrics).counters.Delete(tableID)
		return true
	})
	c.committed.Delete(tableID)
}

var collector *tempIndexCollector

func init() {
	collector = newTempIndexCollector()
	prometheus.MustRegister(collector)

	metrics.DDLCommitTempIndexWrite = func(connID uint64) {
		collector.commit(connID)
	}

	metrics.DDLAddOneTempIndexWrite = func(connID uint64, tableID int64, doubleWrite bool) {
		collector.addTempIndexEntry(connID, tableID, doubleWrite)
	}

	metrics.DDLRollbackTempIndexWrite = func(connID uint64) {
		collector.rollback(connID)
	}

	metrics.DDLRemoveTempIndex = func(tblID int64) {
		collector.removeTempIndex(tblID)
	}

	metrics.DDLClearTempIndexWrite = func(connID uint64) {
		collector.uncommitted.Delete(connID)
	}

	metrics.DDLSetTempIndexScanAndMerge = func(tableID int64, scanCnt, mergeCnt uint64) {
		collector.addTempIndexProcessed(tableID, scanCnt, mergeCnt)
	}
}

func (c *tempIndexCollector) Describe(ch chan<- *prometheus.Desc) {
	c.gaugeVec.Describe(ch)
}

func (c *tempIndexCollector) Collect(ch chan<- prometheus.Metric) {
	currentActive := make(map[int64]bool)

	c.committed.Range(func(key, value any) bool {
		//nolint:forcetypeassert
		tableID := key.(int64)
		tableIDStr := strconv.FormatInt(tableID, 10)
		currentActive[tableID] = true

		//nolint:forcetypeassert
		tc := value.(*committedMetric)

		singleWrite := tc.singleWrite.Load()
		if singleWrite > 0 {
			c.gaugeVec.WithLabelValues(labelSingleWrite, tableIDStr).Set(float64(singleWrite))
		}

		doubleWrite := tc.doubleWrite.Load()
		if doubleWrite > 0 {
			c.gaugeVec.WithLabelValues(labelDoubleWrite, tableIDStr).Set(float64(doubleWrite))
		}

		merged := tc.merged.Load()
		if merged > 0 {
			c.gaugeVec.WithLabelValues(labelMerged, tableIDStr).Set(float64(merged))
		}

		scanned := tc.scanned.Load()
		if scanned > 0 {
			c.gaugeVec.WithLabelValues(labelScanned, tableIDStr).Set(float64(scanned))
		}

		return true
	})

	c.lastActive.Range(func(key, _ any) bool {
		//nolint:forcetypeassert
		tableID := key.(int64)
		if !currentActive[tableID] {
			tableIDStr := strconv.FormatInt(tableID, 10)
			c.gaugeVec.DeleteLabelValues(labelSingleWrite, tableIDStr)
			c.gaugeVec.DeleteLabelValues(labelDoubleWrite, tableIDStr)
			c.gaugeVec.DeleteLabelValues(labelMerged, tableIDStr)
			c.gaugeVec.DeleteLabelValues(labelScanned, tableIDStr)
			c.lastActive.Delete(tableID)
			logutil.BgLogger().Debug("clean up temp index metrics for table", zap.Int64("tableID", tableID))
		}
		return true
	})

	for tableID := range currentActive {
		c.lastActive.Store(tableID, true)
	}

	c.gaugeVec.Collect(ch)
}
