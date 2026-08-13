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
	metrics.DDLAddOneTempIndexWrite = func(connID uint64, tableID int64, doubleWrite bool) {
		c, _ := coll.write.LoadOrStore(connID, &connIDCollector{
			tblID2Count: sync.Map{},
		})
		//nolint:forcetypeassert
		tc, _ := c.(*connIDCollector).tblID2Count.LoadOrStore(tableID, &tableCollector{})
		if doubleWrite {
			//nolint:forcetypeassert
			tc.(*tableCollector).doubleWriteCnt.Add(1)
		} else {
			//nolint:forcetypeassert
			tc.(*tableCollector).singleWriteCnt.Add(1)
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
		coll.read.Delete(tblID)
	}
	metrics.DDLClearTempIndexWrite = func(connID uint64) {
		coll.write.Delete(connID)
	}

	metrics.DDLSetTempIndexScanAndMerge = func(tableID int64, scanCnt, mergeCnt uint64) {
		c, _ := coll.read.LoadOrStore(tableID, &mergeAndScan{})
		//nolint:forcetypeassert
		c.(*mergeAndScan).scan.Add(scanCnt)
		//nolint:forcetypeassert
		c.(*mergeAndScan).merge.Add(mergeCnt)
	}
}

const (
	labelSingleWrite = "single_write"
	labelDoubleWrite = "double_write"
	labelMerge       = "merge"
	labelScan        = "scan"
)

type collector struct {
	write sync.Map // connectionID => connIDCollector
	read  sync.Map // tableID => mergeAndScan

	desc *prometheus.Desc
}

type mergeAndScan struct {
	merge atomic.Uint64
	scan  atomic.Uint64
}

type connIDCollector struct {
	tblID2Count sync.Map // tableID => tableCollector
}
type tableCollector struct {
	singleWriteCnt atomic.Uint64
	doubleWriteCnt atomic.Uint64

	totalSingleWriteCnt atomic.Uint64
	totalDoubleWriteCnt atomic.Uint64
}

func newCollector() *collector {
	return &collector{
		write: sync.Map{},
		read:  sync.Map{},
		desc: prometheus.NewDesc(
			"tidb_ddl_temp_index_op_count",
			"Gauge of temp index operation count",
			[]string{"type", "table_id"}, nil,
		),
	}
}

func (c *collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.desc
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
			singleMap[tableID] += tblColl.totalSingleWriteCnt.Load()
			doubleMap[tableID] += tblColl.totalDoubleWriteCnt.Load()
			return true
		})
		return true
	})
	for tableID, cnt := range singleMap {
		ch <- prometheus.MustNewConstMetric(
			c.desc,
			prometheus.GaugeValue,
			float64(cnt),
			labelSingleWrite,
			strconv.FormatInt(tableID, 10),
		)
	}
	for tableID, cnt := range doubleMap {
		ch <- prometheus.MustNewConstMetric(
			c.desc,
			prometheus.GaugeValue,
			float64(cnt),
			labelDoubleWrite,
			strconv.FormatInt(tableID, 10),
		)
	}
	mergeMap := make(map[int64]uint64)
	scanMap := make(map[int64]uint64)
	c.read.Range(func(key, value any) bool {
		//nolint:forcetypeassert
		tableID := key.(int64)
		//nolint:forcetypeassert
		ms := value.(*mergeAndScan)
		mergeMap[tableID] += ms.merge.Load()
		scanMap[tableID] += ms.scan.Load()
		return true
	})
	for tableID, cnt := range mergeMap {
		ch <- prometheus.MustNewConstMetric(
			c.desc,
			prometheus.GaugeValue,
			float64(cnt),
			labelMerge,
			strconv.FormatInt(tableID, 10),
		)
	}
	for tableID, cnt := range scanMap {
		ch <- prometheus.MustNewConstMetric(
			c.desc,
			prometheus.GaugeValue,
			float64(cnt),
			labelScan,
			strconv.FormatInt(tableID, 10),
		)
	}
}
