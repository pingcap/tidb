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
	metrics.DDLCommitTempIndexWrite = func(connID uint64) {
		c, ok := coll.write.Load(connID)
		if !ok {
			return
		}
		//nolint:forcetypeassert
		c.(*connIDCollector).tblID2Count.Range(func(_, value any) bool {
			tblColl := value.(*tableCollector)
			tblColl.totalSingleWriteCnt.Add(tblColl.singleWriteCnt.Load())
			tblColl.singleWriteCnt.Store(0)
			tblColl.totalDoubleWriteCnt.Add(tblColl.doubleWriteCnt.Load())
			tblColl.doubleWriteCnt.Store(0)
			return true
		})
	}
	metrics.DDLSetTempIndexWrite = func(connID uint64, tableID int64, opCount uint64, doubleWrite bool) {
		c, _ := coll.write.LoadOrStore(connID, &connIDCollector{
			tblID2Count: sync.Map{},
		})
		//nolint:forcetypeassert
		tc, _ := c.(*connIDCollector).tblID2Count.LoadOrStore(tableID, &tableCollector{
			singleWriteCnt:      &atomic.Uint64{},
			doubleWriteCnt:      &atomic.Uint64{},
			totalSingleWriteCnt: &atomic.Uint64{},
			totalDoubleWriteCnt: &atomic.Uint64{},
		})
		if doubleWrite {
			//nolint:forcetypeassert
			tc.(*tableCollector).doubleWriteCnt.Add(opCount)
		} else {
			//nolint:forcetypeassert
			tc.(*tableCollector).singleWriteCnt.Add(opCount)
		}
	}
	metrics.DDLRollbackTempIndexWrite = func(connID uint64) {
		c, ok := coll.write.Load(connID)
		if !ok {
			return
		}
		//nolint:forcetypeassert
		connIDColl := c.(*connIDCollector)
		connIDColl.tblID2Count.Range(func(_, value any) bool {
			//nolint:forcetypeassert
			tblColl := value.(*tableCollector)
			tblColl.singleWriteCnt.Store(0)
			tblColl.doubleWriteCnt.Store(0)
			return true
		})
	}
	metrics.DDLResetTempIndexWrite = func(tblID int64) {
		coll.write.Range(func(_, value any) bool {
			//nolint:forcetypeassert
			connIDCollector := value.(*connIDCollector)
			connIDCollector.tblID2Count.Delete(tblID)
			return true
		})
		coll.merge.Delete(tblID)
		coll.scan.Delete(tblID)
	}
	metrics.DDLSetTempIndexScan = func(tableID int64, opCount uint64) {
		c, _ := coll.scan.LoadOrStore(tableID, &atomic.Uint64{})
		//nolint:forcetypeassert
		c.(*atomic.Uint64).Add(opCount)
	}
	metrics.DDLSetTempIndexMerge = func(tableID int64, opCount uint64) {
		c, _ := coll.merge.LoadOrStore(tableID, &atomic.Uint64{})
		//nolint:forcetypeassert
		c.(*atomic.Uint64).Add(opCount)
	}
}

type collector struct {
	write sync.Map // connectionID => connIDCollector
	merge sync.Map // tableID => atomic.Uint64
	scan  sync.Map // tableID => atomic.Uint64

	singleWriteDesc *prometheus.Desc
	doubleWriteDesc *prometheus.Desc
	mergeDesc       *prometheus.Desc
	scanDesc        *prometheus.Desc
}

type connIDCollector struct {
	tblID2Count sync.Map // tableID => tableCollector
}

type tableCollector struct {
	singleWriteCnt *atomic.Uint64
	doubleWriteCnt *atomic.Uint64

	totalSingleWriteCnt *atomic.Uint64
	totalDoubleWriteCnt *atomic.Uint64
}

func newCollector() *collector {
	return &collector{
		write: sync.Map{},
		merge: sync.Map{},
		scan:  sync.Map{},
		singleWriteDesc: prometheus.NewDesc(
			"tidb_ddl_temp_index_write",
			"Gauge of temp index write times",
			[]string{"table_id"}, nil,
		),
		doubleWriteDesc: prometheus.NewDesc(
			"tidb_ddl_temp_index_double_write",
			"Gauge of temp index double write times",
			[]string{"table_id"}, nil,
		),
		mergeDesc: prometheus.NewDesc(
			"tidb_ddl_temp_index_merge",
			"Gauge of temp index merge times.",
			[]string{"table_id"}, nil,
		),
		scanDesc: prometheus.NewDesc(
			"tidb_ddl_temp_index_scan",
			"Gauge of temp index scan times.",
			[]string{"table_id"}, nil,
		),
	}
}

func (c *collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.singleWriteDesc
	ch <- c.doubleWriteDesc
	ch <- c.mergeDesc
	ch <- c.scanDesc
}

func (c *collector) Collect(ch chan<- prometheus.Metric) {
	singleMap := make(map[int64]uint64)
	doubleMap := make(map[int64]uint64)
	c.write.Range(func(_, value any) bool {
		//nolint:forcetypeassert
		connIDColl := value.(*connIDCollector)
		connIDColl.tblID2Count.Range(func(tableKey, tableValue any) bool {
			//nolint:forcetypeassert
			tableID := tableKey.(int64)
			//nolint:forcetypeassert
			tblColl := tableValue.(*tableCollector)
			if _, exists := singleMap[tableID]; !exists {
				singleMap[tableID] = 0
			}
			singleMap[tableID] += tblColl.totalSingleWriteCnt.Load()
			if _, exists := doubleMap[tableID]; !exists {
				doubleMap[tableID] = 0
			}
			doubleMap[tableID] += tblColl.totalDoubleWriteCnt.Load()
			return true
		})
		return true
	})
	for tableID, cnt := range singleMap {
		ch <- prometheus.MustNewConstMetric(
			c.singleWriteDesc,
			prometheus.GaugeValue,
			float64(cnt),
			strconv.FormatInt(tableID, 10),
		)
	}
	for tableID, cnt := range doubleMap {
		ch <- prometheus.MustNewConstMetric(
			c.doubleWriteDesc,
			prometheus.GaugeValue,
			float64(cnt),
			strconv.FormatInt(tableID, 10),
		)
	}
	mergeMap := make(map[int64]uint64)
	c.merge.Range(func(key, value any) bool {
		//nolint:forcetypeassert
		tableID := key.(int64)
		//nolint:forcetypeassert
		opCount := value.(*atomic.Uint64).Load()
		if _, exists := mergeMap[tableID]; !exists {
			mergeMap[tableID] = 0
		}
		mergeMap[tableID] += opCount
		return true
	})
	for tableID, cnt := range mergeMap {
		ch <- prometheus.MustNewConstMetric(
			c.mergeDesc,
			prometheus.GaugeValue,
			float64(cnt),
			strconv.FormatInt(tableID, 10),
		)
	}
	scanMap := make(map[int64]uint64)
	c.scan.Range(func(key, value any) bool {
		//nolint:forcetypeassert
		tableID := key.(int64)
		//nolint:forcetypeassert
		opCount := value.(*atomic.Uint64).Load()
		if _, exists := scanMap[tableID]; !exists {
			scanMap[tableID] = 0
		}
		scanMap[tableID] += opCount
		return true
	})
	for tableID, cnt := range scanMap {
		ch <- prometheus.MustNewConstMetric(
			c.scanDesc,
			prometheus.GaugeValue,
			float64(cnt),
			strconv.FormatInt(tableID, 10),
		)
	}
}
