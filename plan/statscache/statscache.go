// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package statscache

import (
	"sync"
	"time"

	"github.com/ngaut/log"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/plan/statistics"
	"github.com/pingcap/tidb/store/tikv/oracle"
)

type statsInfo struct {
	tbl      *statistics.Table
	loadTime int64
}

type statsCache struct {
	m     sync.RWMutex
	cache map[int64]statsInfo
}

var statsTblCache = statsCache{cache: map[int64]statsInfo{}}

// expireDuration is 1 hour.
var expireDuration int64 = 60 * 60 * 1000

func tableCacheExpired(si statsInfo) bool {
	duration := oracle.GetPhysical(time.Now()) - si.loadTime
	if duration >= expireDuration {
		return true
	}
	return false
}

// GetStatisticsTableCache retrieves the statistics table from cache, and will refill cache if necessary.
func GetStatisticsTableCache(ctx context.Context, tblInfo *model.TableInfo) *statistics.Table {
	statsTblCache.m.RLock()
	si, ok := statsTblCache.cache[tblInfo.ID]
	statsTblCache.m.RUnlock()
	// Here we check the TableInfo because there may be some ddl changes in the duration period.
	// Also, we rely on the fact that TableInfo will not be same if and only if there are ddl changes.
	if ok && tblInfo == si.tbl.Info && !tableCacheExpired(si) {
		return si.tbl
	}
	txn := ctx.Txn()
	if txn == nil {
		err := ctx.ActivePendingTxn()
		if err != nil || ctx.Txn() == nil {
			return statistics.PseudoTable(tblInfo)
		}
		txn = ctx.Txn()
	}
	m := meta.NewMeta(txn)
	tpb, err := m.GetTableStats(tblInfo.ID)
	if err != nil {
		return statistics.PseudoTable(tblInfo)
	}
	// This table has no statistics table, we give it a pseudo one and save in cache.
	if tpb == nil {
		tbl := statistics.PseudoTable(tblInfo)
		SetStatisticsTableCache(tblInfo.ID, tbl)
		return tbl
	}
	tbl, err := statistics.TableFromPB(tblInfo, tpb)
	// Error is not nil may mean that there are some ddl changes on this table, so the origin
	// statistics can not be used any more, we give it a pseudo one and save in cache.
	if err != nil {
		log.Errorf("Error occured when convert pb table for %s", tblInfo.Name.O)
		tbl = statistics.PseudoTable(tblInfo)
		SetStatisticsTableCache(tblInfo.ID, tbl)
		return tbl
	}
	SetStatisticsTableCache(tblInfo.ID, tbl)
	return tbl
}

// SetStatisticsTableCache sets the statistics table cache.
func SetStatisticsTableCache(id int64, statsTbl *statistics.Table) {
	si := statsInfo{
		tbl:      statsTbl,
		loadTime: oracle.GetPhysical(time.Now()),
	}
	statsTblCache.m.Lock()
	statsTblCache.cache[id] = si
	statsTblCache.m.Unlock()
}
