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

var coll = newCollector()

func init() {
	prometheus.MustRegister(coll)
	metrics.DDLCommitIngestIncrementalOpCount = func(connID uint64) {
		c, ok := coll.inc.Load(connID)
		if !ok {
			return
		}
		connIDCollector := c.(*connIDCollector)
		connIDCollector.tblID2Count.Range(func(key, value interface{}) bool {
			tableCollector := value.(*tableCollector)
			tableCollector.totalIncOpCnt.Add(tableCollector.incOpCnt.Load())
			tableCollector.incOpCnt.Store(0)
			return true
		})
	}
	metrics.DDLRecordIngestIncrementalOpCount = func(connID uint64, tableID int64, opCount uint64) {
		c, _ := coll.inc.LoadOrStore(connID, &connIDCollector{
			tblID2Count: sync.Map{},
		})
		tc, _ := c.(*connIDCollector).tblID2Count.LoadOrStore(tableID, &tableCollector{
			incOpCnt:      &atomic.Uint64{},
			totalIncOpCnt: &atomic.Uint64{},
		})
		tc.(*tableCollector).incOpCnt.Add(opCount)
	}
	metrics.DDLRollbackIngestIncrementalOpCount = func(connID uint64) {
		c, ok := coll.inc.Load(connID)
		if !ok {
			return
		}
		connIDCollector := c.(*connIDCollector)
		connIDCollector.tblID2Count.Range(func(key, value interface{}) bool {
			tableCollector := value.(*tableCollector)
			tableCollector.incOpCnt.Store(0)
			return true
		})
	}
	metrics.DDLResetTotalIngestIncrementalOpCount = func(tblID int64) {
		coll.inc.Range(func(key, value interface{}) bool {
			connIDCollector := value.(*connIDCollector)
			connIDCollector.tblID2Count.Delete(tblID)
			return true
		})
		coll.merged.Delete(tblID)
		coll.scanned.Delete(tblID)

	}
	metrics.DDLRecordScannedIncrementalOpCount = func(tableID int64, opCount uint64) {
		c, _ := coll.scanned.LoadOrStore(tableID, &atomic.Uint64{})
		c.(*atomic.Uint64).Add(opCount)
	}
	metrics.DDLRecordMergedIncrementalOpCount = func(tableID int64, opCount uint64) {
		c, _ := coll.merged.LoadOrStore(tableID, &atomic.Uint64{})
		c.(*atomic.Uint64).Add(opCount)
	}
}

type collector struct {
	inc     sync.Map // connectionID => connIDCollector
	merged  sync.Map // tableID => atomic.Uint64
	scanned sync.Map // tableID => atomic.Uint64

	incDesc     *prometheus.Desc
	mergedDesc  *prometheus.Desc
	scannedDesc *prometheus.Desc
}

type connIDCollector struct {
	tblID2Count sync.Map // tableID => tableCollector
}

type tableCollector struct {
	incOpCnt      *atomic.Uint64
	totalIncOpCnt *atomic.Uint64
}

func newCollector() *collector {
	return &collector{
		inc:     sync.Map{},
		merged:  sync.Map{},
		scanned: sync.Map{},
		incDesc: prometheus.NewDesc(
			"tidb_ingest_incremental_op_count",
			"Gauge of incremental operations.",
			[]string{"table_id"}, nil,
		),
		mergedDesc: prometheus.NewDesc(
			"tidb_ingest_merged_op_count",
			"Gauge of merged incremental operations.",
			[]string{"table_id"}, nil,
		),
		scannedDesc: prometheus.NewDesc(
			"tidb_ingest_scanned_op_count",
			"Gauge of scanned incremental operations.",
			[]string{"table_id"}, nil,
		),
	}
}

func (c *collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.incDesc
	ch <- c.mergedDesc
	ch <- c.scannedDesc
}

func (c *collector) Collect(ch chan<- prometheus.Metric) {
	tableIDCnt := make(map[int64]uint64)
	c.inc.Range(func(key, value interface{}) bool {
		connIDCollector := value.(*connIDCollector)
		connIDCollector.tblID2Count.Range(func(tableKey, tableValue interface{}) bool {
			tableID := tableKey.(int64)
			tableCollector := tableValue.(*tableCollector)
			if _, exists := tableIDCnt[tableID]; !exists {
				tableIDCnt[tableID] = 0
			}
			tableIDCnt[tableID] += tableCollector.totalIncOpCnt.Load()
			return true
		})
		return true
	})
	for tableID, opCount := range tableIDCnt {
		ch <- prometheus.MustNewConstMetric(
			c.incDesc,
			prometheus.GaugeValue,
			float64(opCount),
			strconv.FormatInt(tableID, 10),
		)
	}
	tableIDCnt = make(map[int64]uint64)
	c.merged.Range(func(key, value interface{}) bool {
		tableID := key.(int64)
		opCount := value.(*atomic.Uint64).Load()
		if _, exists := tableIDCnt[tableID]; !exists {
			tableIDCnt[tableID] = 0
		}
		tableIDCnt[tableID] += opCount
		return true
	})
	for tableID, opCount := range tableIDCnt {
		ch <- prometheus.MustNewConstMetric(
			c.mergedDesc,
			prometheus.GaugeValue,
			float64(opCount),
			strconv.FormatInt(tableID, 10),
		)
	}
	tableIDCnt = make(map[int64]uint64)
	c.scanned.Range(func(key, value interface{}) bool {
		tableID := key.(int64)
		opCount := value.(*atomic.Uint64).Load()
		if _, exists := tableIDCnt[tableID]; !exists {
			tableIDCnt[tableID] = 0
		}
		tableIDCnt[tableID] += opCount
		return true
	})
	for tableID, opCount := range tableIDCnt {
		ch <- prometheus.MustNewConstMetric(
			c.scannedDesc,
			prometheus.GaugeValue,
			float64(opCount),
			strconv.FormatInt(tableID, 10),
		)
	}
}
