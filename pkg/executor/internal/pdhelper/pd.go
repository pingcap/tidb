// Copyright 2023 PingCAP, Inc.
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

package pdhelper

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jellydator/ttlcache/v3"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/store/helper"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/sqlescape"
)

// GlobalPDHelper is the global variable for PDHelper.
var GlobalPDHelper = defaultPDHelper()
var globalPDHelperOnce sync.Once

// PDHelper is used to get some information from PD.
type PDHelper struct {
	cacheForApproximateTableCountFromStorage *ttlcache.Cache[string, float64]

	getApproximateTableCountFromStorageFunc func(ctx context.Context, sctx sessionctx.Context, tid int64, dbName, tableName, partitionName string) (float64, bool)
	wg                                      util.WaitGroupWrapper
}

func defaultPDHelper() *PDHelper {
	cache := ttlcache.New[string, float64](
		ttlcache.WithTTL[string, float64](30*time.Second),
		ttlcache.WithCapacity[string, float64](1024*1024),
	)
	return &PDHelper{
		cacheForApproximateTableCountFromStorage: cache,
		getApproximateTableCountFromStorageFunc:  getApproximateTableCountFromStorage,
	}
}

// Start is used to start the background task of PDHelper. Currently, the background task is used to clean up TTL cache.
func (p *PDHelper) Start() {
	globalPDHelperOnce.Do(func() {
		p.wg.Run(p.cacheForApproximateTableCountFromStorage.Start)
	})
}

// Stop stops the background task of PDHelper.
func (p *PDHelper) Stop() {
	p.cacheForApproximateTableCountFromStorage.Stop()
	p.wg.Wait()
}

func approximateTableCountKey(tid int64, dbName, tableName, partitionName string) string {
	return strings.Join([]string{strconv.FormatInt(tid, 10), dbName, tableName, partitionName}, "_")
}

// GetApproximateTableCountFromStorage gets the approximate count of the table.
func (p *PDHelper) GetApproximateTableCountFromStorage(
	ctx context.Context, sctx sessionctx.Context,
	tid int64, dbName, tableName, partitionName string,
) (float64, bool) {
	key := approximateTableCountKey(tid, dbName, tableName, partitionName)
	if item := p.cacheForApproximateTableCountFromStorage.Get(key); item != nil {
		return item.Value(), true
	}
	result, hasPD := p.getApproximateTableCountFromStorageFunc(ctx, sctx, tid, dbName, tableName, partitionName)
	p.cacheForApproximateTableCountFromStorage.Set(key, result, ttlcache.DefaultTTL)
	return result, hasPD
}

func getApproximateTableCountFromStorage(
	ctx context.Context, sctx sessionctx.Context,
	tid int64, dbName, tableName, partitionName string,
) (float64, bool) {
	tikvStore, ok := sctx.GetStore().(helper.Storage)
	if !ok {
		return 0, false
	}
	regionStats, err := helper.NewHelper(tikvStore).GetPDRegionStats(ctx, tid, true)
	failpoint.Inject("calcSampleRateByStorageCount", func() {
		// Force the TiDB thinking that there's PD and the count of region is small.
		err = nil
		regionStats.Count = 1
		// Set a very large approximate count.
		regionStats.StorageKeys = 1000000
	})
	if err != nil {
		return 0, false
	}
	// If this table is not small, we directly use the count from PD,
	// since for a small table, it's possible that it's data is in the same region with part of another large table.
	// Thus, we use the number of the regions of the table's table KV to decide whether the table is small.
	if regionStats.Count > 2 {
		return float64(regionStats.StorageKeys), true
	}
	// Otherwise, we use count(*) to calc it's size, since it's very small, the table data can be filled in no more than 2 regions.
	sql := new(strings.Builder)
	sqlescape.MustFormatSQL(sql, "select count(*) from %n.%n", dbName, tableName)
	if partitionName != "" {
		sqlescape.MustFormatSQL(sql, " partition(%n)", partitionName)
	}
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnStats)
	rows, _, err := sctx.GetRestrictedSQLExecutor().ExecRestrictedSQL(ctx, nil, sql.String())
	if err != nil {
		return 0, false
	}
	// If the record set is nil, there's something wrong with the execution. The COUNT(*) would always return one row.
	if len(rows) == 0 || rows[0].Len() == 0 {
		return 0, false
	}
	return float64(rows[0].GetInt64(0)), true
}
