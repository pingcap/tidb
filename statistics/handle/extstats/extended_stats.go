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

package extstats

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/statistics/handle/cache"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/mathutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"go.uber.org/zap"
)

var (
	// StatsMetaHistorySourceExtendedStats indicates stats history meta source from extended stats
	StatsMetaHistorySourceExtendedStats = "extended stats"
)

// InsertExtendedStats inserts a record into mysql.stats_extended and update version in mysql.stats_meta.
func InsertExtendedStats(sctx sessionctx.Context,
	recordHistoricalStatsMeta func(tableID int64, version uint64, source string),
	updateStatsCache func(newCache *cache.StatsCache, tables []*statistics.Table, deletedIDs []int64) (updated bool),
	currentCache *cache.StatsCache,
	statsName string, colIDs []int64, tp int, tableID int64, ifNotExists bool) (err error) {
	statsVer := uint64(0)
	defer func() {
		if err == nil && statsVer != 0 {
			recordHistoricalStatsMeta(tableID, statsVer, StatsMetaHistorySourceExtendedStats)
		}
	}()
	exec := sctx.(sqlexec.RestrictedSQLExecutor)
	slices.Sort(colIDs)
	bytes, err := json.Marshal(colIDs)
	if err != nil {
		return errors.Trace(err)
	}
	strColIDs := string(bytes)

	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnStats)

	sqlExecutor := exec.(sqlexec.SQLExecutor)
	_, err = sqlExecutor.ExecuteInternal(ctx, "begin pessimistic")
	if err != nil {
		return errors.Trace(err)
	}
	defer func() {
		err = finishTransaction(ctx, sqlExecutor, err)
	}()
	// No need to use `exec.ExecuteInternal` since we have acquired the lock.
	rows, _, err := exec.ExecRestrictedSQL(ctx, []sqlexec.OptionFuncAlias{sqlexec.ExecOptionUseCurSession}, "SELECT name, type, column_ids FROM mysql.stats_extended WHERE table_id = %? and status in (%?, %?)", tableID, statistics.ExtendedStatsInited, statistics.ExtendedStatsAnalyzed)
	if err != nil {
		return errors.Trace(err)
	}
	for _, row := range rows {
		currStatsName := row.GetString(0)
		currTp := row.GetInt64(1)
		currStrColIDs := row.GetString(2)
		if currStatsName == statsName {
			if ifNotExists {
				return nil
			}
			return errors.Errorf("extended statistics '%s' for the specified table already exists", statsName)
		}
		if tp == int(currTp) && currStrColIDs == strColIDs {
			return errors.Errorf("extended statistics '%s' with same type on same columns already exists", statsName)
		}
	}
	version, err := getStartTS(sctx)
	if err != nil {
		return errors.Trace(err)
	}
	// Bump version in `mysql.stats_meta` to trigger stats cache refresh.
	if _, err = sqlExecutor.ExecuteInternal(ctx, "UPDATE mysql.stats_meta SET version = %? WHERE table_id = %?", version, tableID); err != nil {
		return err
	}
	statsVer = version
	// Remove the existing 'deleted' records.
	if _, err = sqlExecutor.ExecuteInternal(ctx, "DELETE FROM mysql.stats_extended WHERE name = %? and table_id = %?", statsName, tableID); err != nil {
		return err
	}
	// Remove the cache item, which is necessary for cases like a cluster with 3 tidb instances, e.g, a, b and c.
	// If tidb-a executes `alter table drop stats_extended` to mark the record as 'deleted', and before this operation
	// is synchronized to other tidb instances, tidb-b executes `alter table add stats_extended`, which would delete
	// the record from the table, tidb-b should delete the cached item synchronously. While for tidb-c, it has to wait for
	// next `Update()` to remove the cached item then.
	removeExtendedStatsItem(currentCache, updateStatsCache, tableID, statsName)
	const sql = "INSERT INTO mysql.stats_extended(name, type, table_id, column_ids, version, status) VALUES (%?, %?, %?, %?, %?, %?)"
	if _, err = sqlExecutor.ExecuteInternal(ctx, sql, statsName, tp, tableID, strColIDs, version, statistics.ExtendedStatsInited); err != nil {
		return err
	}
	return
}

// MarkExtendedStatsDeleted update the status of mysql.stats_extended to be `deleted` and the version of mysql.stats_meta.
func MarkExtendedStatsDeleted(sctx sessionctx.Context,
	recordHistoricalStatsMeta func(tableID int64, version uint64, source string),
	updateStatsCache func(newCache *cache.StatsCache, tables []*statistics.Table, deletedIDs []int64) (updated bool),
	currentCache *cache.StatsCache,
	statsName string, tableID int64, ifExists bool) (err error) {
	statsVer := uint64(0)
	defer func() {
		if err == nil && statsVer != 0 {
			recordHistoricalStatsMeta(tableID, statsVer, StatsMetaHistorySourceExtendedStats)
		}
	}()
	exec := sctx.(sqlexec.RestrictedSQLExecutor)
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnStats)
	rows, _, err := exec.ExecRestrictedSQL(ctx, []sqlexec.OptionFuncAlias{sqlexec.ExecOptionUseCurSession}, "SELECT name FROM mysql.stats_extended WHERE name = %? and table_id = %? and status in (%?, %?)", statsName, tableID, statistics.ExtendedStatsInited, statistics.ExtendedStatsAnalyzed)
	if err != nil {
		return errors.Trace(err)
	}
	if len(rows) == 0 {
		if ifExists {
			return nil
		}
		return fmt.Errorf("extended statistics '%s' for the specified table does not exist", statsName)
	}
	if len(rows) > 1 {
		logutil.BgLogger().Warn("unexpected duplicate extended stats records found", zap.String("name", statsName), zap.Int64("table_id", tableID))
	}

	sqlExec := exec.(sqlexec.SQLExecutor)

	_, err = sqlExec.ExecuteInternal(ctx, "begin pessimistic")
	if err != nil {
		return errors.Trace(err)
	}
	defer func() {
		err1 := finishTransaction(ctx, sqlExec, err)
		if err == nil && err1 == nil {
			removeExtendedStatsItem(currentCache, updateStatsCache, tableID, statsName)
		}
		err = err1
	}()
	version, err := getStartTS(sctx)
	if err != nil {
		return errors.Trace(err)
	}
	if _, err = sqlExec.ExecuteInternal(ctx, "UPDATE mysql.stats_meta SET version = %? WHERE table_id = %?", version, tableID); err != nil {
		return err
	}
	statsVer = version
	if _, err = sqlExec.ExecuteInternal(ctx, "UPDATE mysql.stats_extended SET version = %?, status = %? WHERE name = %? and table_id = %?", version, statistics.ExtendedStatsDeleted, statsName, tableID); err != nil {
		return err
	}
	return nil
}

// BuildExtendedStats build extended stats for column groups if needed based on the column samples.
func BuildExtendedStats(sctx sessionctx.Context,
	tableID int64, cols []*model.ColumnInfo, collectors []*statistics.SampleCollector) (*statistics.ExtendedStatsColl, error) {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnStats)
	exec := sctx.(sqlexec.RestrictedSQLExecutor)
	const sql = "SELECT name, type, column_ids FROM mysql.stats_extended WHERE table_id = %? and status in (%?, %?)"
	rows, _, err := exec.ExecRestrictedSQL(ctx, []sqlexec.OptionFuncAlias{sqlexec.ExecOptionUseCurSession}, sql, tableID, statistics.ExtendedStatsAnalyzed, statistics.ExtendedStatsInited)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(rows) == 0 {
		return nil, nil
	}
	statsColl := statistics.NewExtendedStatsColl()
	for _, row := range rows {
		name := row.GetString(0)
		item := &statistics.ExtendedStatsItem{Tp: uint8(row.GetInt64(1))}
		colIDs := row.GetString(2)
		err := json.Unmarshal([]byte(colIDs), &item.ColIDs)
		if err != nil {
			logutil.BgLogger().Error("invalid column_ids in mysql.stats_extended, skip collecting extended stats for this row", zap.String("column_ids", colIDs), zap.Error(err))
			continue
		}
		item = fillExtendedStatsItemVals(sctx, item, cols, collectors)
		if item != nil {
			statsColl.Stats[name] = item
		}
	}
	if len(statsColl.Stats) == 0 {
		return nil, nil
	}
	return statsColl, nil
}

func fillExtendedStatsItemVals(sctx sessionctx.Context, item *statistics.ExtendedStatsItem, cols []*model.ColumnInfo, collectors []*statistics.SampleCollector) *statistics.ExtendedStatsItem {
	switch item.Tp {
	case ast.StatsTypeCardinality, ast.StatsTypeDependency:
		return nil
	case ast.StatsTypeCorrelation:
		return fillExtStatsCorrVals(sctx, item, cols, collectors)
	}
	return nil
}

func fillExtStatsCorrVals(sctx sessionctx.Context, item *statistics.ExtendedStatsItem, cols []*model.ColumnInfo, collectors []*statistics.SampleCollector) *statistics.ExtendedStatsItem {
	colOffsets := make([]int, 0, 2)
	for _, id := range item.ColIDs {
		for i, col := range cols {
			if col.ID == id {
				colOffsets = append(colOffsets, i)
				break
			}
		}
	}
	if len(colOffsets) != 2 {
		return nil
	}
	// samplesX and samplesY are in order of handle, i.e, their SampleItem.Ordinals are in order.
	samplesX := collectors[colOffsets[0]].Samples
	// We would modify Ordinal of samplesY, so we make a deep copy.
	samplesY := statistics.CopySampleItems(collectors[colOffsets[1]].Samples)
	sampleNum := mathutil.Min(len(samplesX), len(samplesY))
	if sampleNum == 1 {
		item.ScalarVals = 1
		return item
	}
	if sampleNum <= 0 {
		item.ScalarVals = 0
		return item
	}

	sc := sctx.GetSessionVars().StmtCtx

	var err error
	samplesX, err = statistics.SortSampleItems(sc, samplesX)
	if err != nil {
		return nil
	}
	samplesYInXOrder := make([]*statistics.SampleItem, 0, sampleNum)
	for i, itemX := range samplesX {
		if itemX.Ordinal >= len(samplesY) {
			continue
		}
		itemY := samplesY[itemX.Ordinal]
		itemY.Ordinal = i
		samplesYInXOrder = append(samplesYInXOrder, itemY)
	}
	samplesYInYOrder, err := statistics.SortSampleItems(sc, samplesYInXOrder)
	if err != nil {
		return nil
	}
	var corrXYSum float64
	for i := 1; i < len(samplesYInYOrder); i++ {
		corrXYSum += float64(i) * float64(samplesYInYOrder[i].Ordinal)
	}
	// X means the ordinal of the item in original sequence, Y means the oridnal of the item in the
	// sorted sequence, we know that X and Y value sets are both:
	// 0, 1, ..., sampleNum-1
	// we can simply compute sum(X) = sum(Y) =
	//    (sampleNum-1)*sampleNum / 2
	// and sum(X^2) = sum(Y^2) =
	//    (sampleNum-1)*sampleNum*(2*sampleNum-1) / 6
	// We use "Pearson correlation coefficient" to compute the order correlation of columns,
	// the formula is based on https://en.wikipedia.org/wiki/Pearson_correlation_coefficient.
	// Note that (itemsCount*corrX2Sum - corrXSum*corrXSum) would never be zero when sampleNum is larger than 1.
	itemsCount := float64(sampleNum)
	corrXSum := (itemsCount - 1) * itemsCount / 2.0
	corrX2Sum := (itemsCount - 1) * itemsCount * (2*itemsCount - 1) / 6.0
	item.ScalarVals = (itemsCount*corrXYSum - corrXSum*corrXSum) / (itemsCount*corrX2Sum - corrXSum*corrXSum)
	return item
}

// SaveExtendedStatsToStorage writes extended stats of a table into mysql.stats_extended.
func SaveExtendedStatsToStorage(sctx sessionctx.Context,
	recordHistoricalStatsMeta func(tableID int64, version uint64, source string),
	tableID int64, extStats *statistics.ExtendedStatsColl, isLoad bool) (err error) {
	statsVer := uint64(0)
	defer func() {
		if err == nil && statsVer != 0 {
			recordHistoricalStatsMeta(tableID, statsVer, StatsMetaHistorySourceExtendedStats)
		}
	}()
	if extStats == nil || len(extStats.Stats) == 0 {
		return nil
	}

	sqlExec := sctx.(sqlexec.SQLExecutor)
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnStats)

	_, err = sqlExec.ExecuteInternal(ctx, "begin pessimistic")
	if err != nil {
		return errors.Trace(err)
	}
	defer func() {
		err = finishTransaction(ctx, sqlExec, err)
	}()
	version, err := getStartTS(sctx)
	if err != nil {
		return errors.Trace(err)
	}
	for name, item := range extStats.Stats {
		bytes, err := json.Marshal(item.ColIDs)
		if err != nil {
			return errors.Trace(err)
		}
		strColIDs := string(bytes)
		var statsStr string
		switch item.Tp {
		case ast.StatsTypeCardinality, ast.StatsTypeCorrelation:
			statsStr = fmt.Sprintf("%f", item.ScalarVals)
		case ast.StatsTypeDependency:
			statsStr = item.StringVals
		}
		// If isLoad is true, it's INSERT; otherwise, it's UPDATE.
		if _, err := sqlExec.ExecuteInternal(ctx, "replace into mysql.stats_extended values (%?, %?, %?, %?, %?, %?, %?)", name, item.Tp, tableID, strColIDs, statsStr, version, statistics.ExtendedStatsAnalyzed); err != nil {
			return err
		}
	}
	if !isLoad {
		if _, err := sqlExec.ExecuteInternal(ctx, "UPDATE mysql.stats_meta SET version = %? WHERE table_id = %?", version, tableID); err != nil {
			return err
		}
		statsVer = version
	}
	return nil
}

const updateStatsCacheRetryCnt = 5

func removeExtendedStatsItem(currentCache *cache.StatsCache,
	updateStatsCache func(newCache *cache.StatsCache, tables []*statistics.Table, deletedIDs []int64) (updated bool),
	tableID int64, statsName string) {
	for retry := updateStatsCacheRetryCnt; retry > 0; retry-- {
		oldCache := currentCache
		tbl, ok := oldCache.Get(tableID)
		if !ok || tbl.ExtendedStats == nil || len(tbl.ExtendedStats.Stats) == 0 {
			return
		}
		newTbl := tbl.Copy()
		delete(newTbl.ExtendedStats.Stats, statsName)
		if updateStatsCache(oldCache, []*statistics.Table{newTbl}, nil) {
			return
		}
		if retry == 1 {
			logutil.BgLogger().Info("remove extended stats cache failed", zap.String("stats_name", statsName), zap.Int64("table_id", tableID))
		} else {
			logutil.BgLogger().Info("remove extended stats cache failed, retrying", zap.String("stats_name", statsName), zap.Int64("table_id", tableID))
		}
	}
}

// finishTransaction will execute `commit` when error is nil, otherwise `rollback`.
func finishTransaction(ctx context.Context, exec sqlexec.SQLExecutor, err error) error {
	if err == nil {
		_, err = exec.ExecuteInternal(ctx, "commit")
	} else {
		_, err1 := exec.ExecuteInternal(ctx, "rollback")
		terror.Log(errors.Trace(err1))
	}
	return errors.Trace(err)
}

func getStartTS(sctx sessionctx.Context) (uint64, error) {
	txn, err := sctx.Txn(true)
	if err != nil {
		return 0, err
	}
	return txn.StartTS(), nil
}
