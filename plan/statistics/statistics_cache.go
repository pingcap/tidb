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

package statistics

import (
	"sync"
	"time"

	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/store/tikv/oracle"
)

type tableCache struct {
	m     sync.RWMutex
	cache map[int64]*Table
}

var tblCache = tableCache{cache: map[int64]*Table{}}

// expireDuration is 1 hour.
var expireDuration int64 = 60 * 60 * 1000

func tableCacheExpired(tbl *Table) bool {
	duration := oracle.GetPhysical(time.Now()) - oracle.ExtractPhysical(uint64(tbl.TS))
	if duration >= expireDuration {
		return true
	}
	return false
}

// GetStatisticsTableCache retrieves the statistics table from cache.
func GetStatisticsTableCache(tblInfo *model.TableInfo) *Table {
	tblCache.m.RLock()
	statTbl, ok := tblCache.cache[tblInfo.ID]
	tblCache.m.RUnlock()
	// Here we check the TableInfo because there may be some ddl changes in the duration period.
	// Also, we rely on the fact that TableInfo will not be same if and only if there are ddl changes.
	if !ok || tblInfo != statTbl.info || tableCacheExpired(statTbl) {
		return nil
	}
	return statTbl
}

// SetStatisticsTableCache sets the statistics table cache.
func SetStatisticsTableCache(id int64, tbl *Table) {
	tblCache.m.Lock()
	tblCache.cache[id] = tbl
	tblCache.m.Unlock()
}
