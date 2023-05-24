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

package util

import (
	"context"
	"strings"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/helper"
	"github.com/pingcap/tidb/util/sqlexec"
)

// GetApproximateTableCountFromStorage gets the approximate count of the table.
func GetApproximateTableCountFromStorage(sctx sessionctx.Context, tid int64, dbName, tableName, partitionName string) (float64, bool) {
	tikvStore, ok := sctx.GetStore().(helper.Storage)
	if !ok {
		log.Info("fuckfuck")
		return 0, false
	}
	regionStats := &helper.PDRegionStats{}
	pdHelper := helper.NewHelper(tikvStore)
	err := pdHelper.GetPDRegionStats(tid, regionStats, true)
	failpoint.Inject("calcSampleRateByStorageCount", func() {
		// Force the TiDB thinking that there's PD and the count of region is small.
		err = nil
		regionStats.Count = 1
		// Set a very large approximate count.
		regionStats.StorageKeys = 1000000
	})
	failpoint.Inject("calcSampleRateByStorageCountForAnalyze", func() {
		// Force the TiDB thinking that there's PD and the count of region is small.
		err = nil
		regionStats.Count = 100
		// Set a very large approximate count.
		regionStats.StorageKeys = 100000000
	})
	if err != nil {
		return 0, false
	}
	// If this table is not small, we directly use the count from PD,
	// since for a small table, it's possible that it's data is in the same region with part of another large table.
	// Thus, we use the number of the regions of the table's table KV to decide whether the table is small.
	if regionStats.Count > 2 {
		log.Info("fuck ok")
		return float64(regionStats.StorageKeys), true
	}
	// Otherwise, we use count(*) to calc it's size, since it's very small, the table data can be filled in no more than 2 regions.
	sql := new(strings.Builder)
	sqlexec.MustFormatSQL(sql, "select count(*) from %n.%n", dbName, tableName)
	if partitionName != "" {
		sqlexec.MustFormatSQL(sql, " partition(%n)", partitionName)
	}
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnStats)
	rows, _, err := sctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(ctx, nil, sql.String())
	if err != nil {
		return 0, false
	}
	// If the record set is nil, there's something wrong with the execution. The COUNT(*) would always return one row.
	if len(rows) == 0 || rows[0].Len() == 0 {
		return 0, false
	}
	return float64(rows[0].GetInt64(0)), true
}
