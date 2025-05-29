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

var mergeTempCollector = newCollector()

func init() {
	prometheus.MustRegister(mergeTempCollector)
	metrics.DDLCommitIngestIncrementalOpCount = func(connID uint64) {
		c, ok := mergeTempCollector.connIDCollectors.Load(connID)
		if !ok {
			return
		}
		connIDCollector := c.(*connIDCollector)
		connIDCollector.tblIDCollectors.Range(func(key, value interface{}) bool {
			tableCollector := value.(*tableCollector)
			tableCollector.totalIncOpCnt.Add(tableCollector.incOpCnt.Load())
			tableCollector.incOpCnt.Store(0)
			return true
		})
	}
	metrics.DDLRecordIngestIncrementalOpCount = func(connID uint64, tableID int64, opCount uint64) {
		c, _ := mergeTempCollector.connIDCollectors.LoadOrStore(connID, &connIDCollector{
			tblIDCollectors: sync.Map{},
		})
		tc, _ := c.(*connIDCollector).tblIDCollectors.LoadOrStore(tableID, &tableCollector{
			incOpCnt:      &atomic.Uint64{},
			totalIncOpCnt: atomic.Uint64{},
		})
		tc.(*tableCollector).incOpCnt.Add(opCount)
	}
	metrics.DDLRollbackIngestIncrementalOpCount = func(connID uint64) {
		c, ok := mergeTempCollector.connIDCollectors.Load(connID)
		if !ok {
			return
		}
		connIDCollector := c.(*connIDCollector)
		connIDCollector.tblIDCollectors.Range(func(key, value interface{}) bool {
			tableCollector := value.(*tableCollector)
			tableCollector.incOpCnt.Store(0)
			return true
		})
	}
	metrics.DDLResetTotalIngestIncrementalOpCount = func(tblID int64) {
		mergeTempCollector.connIDCollectors.Range(func(key, value interface{}) bool {
			connIDCollector := value.(*connIDCollector)
			if tblCollector, ok := connIDCollector.tblIDCollectors.Load(tblID); ok {
				tableCollector := tblCollector.(*tableCollector)
				tableCollector.totalIncOpCnt.Store(0)
			}
			return true
		})
	}
}

type collector struct {
	connIDCollectors sync.Map // connectionID => connIDCollector
	incOpCntDesc     *prometheus.Desc
}

type connIDCollector struct {
	tblIDCollectors sync.Map // tableID => tableCollector
}

type tableCollector struct {
	incOpCnt      *atomic.Uint64
	totalIncOpCnt atomic.Uint64
}

func newCollector() *collector {
	return &collector{
		connIDCollectors: sync.Map{},
		incOpCntDesc: prometheus.NewDesc(
			"tidb_ingest_incremental_op_count",
			"Number of incremental operations.",
			[]string{"table_id"}, nil,
		),
	}
}

func (c *collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.incOpCntDesc
}

func (c *collector) Collect(ch chan<- prometheus.Metric) {
	tableIDCnt := make(map[int64]uint64)
	c.connIDCollectors.Range(func(key, value interface{}) bool {
		connIDCollector := value.(*connIDCollector)
		connIDCollector.tblIDCollectors.Range(func(tableKey, tableValue interface{}) bool {
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
			c.incOpCntDesc,
			prometheus.CounterValue,
			float64(opCount),
			strconv.FormatInt(tableID, 10),
		)
	}

}
