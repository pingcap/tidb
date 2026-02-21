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

package storage

import (
	"strconv"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/statistics/asyncload"
	statslogutil "github.com/pingcap/tidb/pkg/statistics/handle/logutil"
	statstypes "github.com/pingcap/tidb/pkg/statistics/handle/types"
	"github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"go.uber.org/zap"
)

// loadNeededIndexHistograms loads the necessary index histograms.
// It is similar to loadNeededColumnHistograms, but for index.
func loadNeededIndexHistograms(sctx sessionctx.Context, is infoschema.InfoSchema, statsHandle statstypes.StatsHandle, idx model.TableItemID) (err error) {
	// Regardless of whether the load is successful or not, we must remove the item from the async load list.
	// The principle is to load the histogram for each index at most once in async load, as we already have a retry mechanism in the sync load.
	defer asyncload.AsyncLoadHistogramNeededItems.Delete(idx)

	tbl, ok := statsHandle.Get(idx.TableID)
	if !ok {
		// This could happen when the table is dropped after the async load is triggered.
		statslogutil.StatsSampleLogger().Info(
			"Table statistics item not found, possibly due to table being dropped",
			zap.Int64("tableID", idx.TableID),
			zap.Int64("indexID", idx.ID),
		)
		return nil
	}
	_, loadNeeded := tbl.IndexIsLoadNeeded(idx.ID)
	if !loadNeeded {
		return nil
	}
	hgMeta, statsVer, err := HistMetaFromStorageWithHighPriority(sctx, &idx, nil)
	if hgMeta == nil || err != nil {
		if hgMeta == nil {
			statslogutil.StatsLogger().Warn(
				"Histogram not found, possibly due to DDL event is not handled, please consider analyze the table",
				zap.Int64("tableID", idx.TableID),
				zap.Int64("indexID", idx.ID),
			)
		}
		return err
	}
	tblInfo, ok := statsHandle.TableInfoByID(is, idx.TableID)
	if !ok {
		// This could happen when the table is dropped after the async load is triggered.
		statslogutil.StatsSampleLogger().Info(
			"Table information not found, possibly due to table being dropped",
			zap.Int64("tableID", idx.TableID),
			zap.Int64("indexID", idx.ID),
		)
		return nil
	}
	idxInfo := tblInfo.Meta().FindIndexByID(idx.ID)
	if idxInfo == nil {
		// This could happen when the index is dropped after the async load is triggered.
		statslogutil.StatsSampleLogger().Info(
			"Index information not found, possibly due to index being dropped",
			zap.Int64("tableID", idx.TableID),
			zap.Int64("indexID", idx.ID),
		)
		return nil
	}
	hg, err := HistogramFromStorageWithPriority(sctx, idx.TableID, idx.ID, types.NewFieldType(mysql.TypeBlob), hgMeta.NDV, 1, hgMeta.LastUpdateVersion, hgMeta.NullCount, hgMeta.TotColSize, hgMeta.Correlation, kv.PriorityHigh)
	if err != nil {
		return errors.Trace(err)
	}
	cms, topN, err := CMSketchAndTopNFromStorageWithHighPriority(sctx, idx.TableID, 1, idx.ID, statsVer)
	if err != nil {
		return errors.Trace(err)
	}
	idxHist := &statistics.Index{
		Histogram:         *hg,
		CMSketch:          cms,
		TopN:              topN,
		Info:              idxInfo,
		StatsVer:          statsVer,
		PhysicalID:        idx.TableID,
		StatsLoadedStatus: statistics.NewStatsFullLoadStatus(),
	}

	tbl, ok = statsHandle.Get(idx.TableID)
	if !ok {
		// This could happen when the table is dropped after the async load is triggered.
		statslogutil.StatsSampleLogger().Info(
			"Table statistics item not found, possibly due to table being dropped",
			zap.Int64("tableID", idx.TableID),
			zap.Int64("indexID", idx.ID),
		)
		return nil
	}
	tbl = tbl.CopyAs(statistics.IndexMapWritable)
	if statistics.IsAnalyzed(idxHist.StatsVer) {
		tbl.StatsVer = int(idxHist.StatsVer)
		tbl.LastAnalyzeVersion = max(tbl.LastAnalyzeVersion, idxHist.LastUpdateVersion)
	}
	tbl.SetIdx(idx.ID, idxHist)
	statsHandle.UpdateStatsCache(statstypes.CacheUpdate{
		Updated: []*statistics.Table{tbl},
	})
	if idx.IsSyncLoadFailed {
		statslogutil.StatsLogger().Warn("Index histogram loaded asynchronously after sync load failure",
			zap.Int64("tableID", idx.TableID),
			zap.Int64("indexID", idxHist.Info.ID),
			zap.String("indexName", idxHist.Info.Name.O))
	}
	return nil
}

// StatsMetaByTableIDFromStorage gets the stats meta of a table from storage.
func StatsMetaByTableIDFromStorage(sctx sessionctx.Context, tableID int64, snapshot uint64) (version uint64, modifyCount, count int64, err error) {
	var rows []chunk.Row
	if snapshot == 0 {
		rows, _, err = util.ExecRows(sctx,
			"SELECT version, modify_count, count from mysql.stats_meta where table_id = %? order by version", tableID)
	} else {
		rows, _, err = util.ExecWithOpts(sctx,
			[]sqlexec.OptionFuncAlias{sqlexec.ExecOptionWithSnapshot(snapshot), sqlexec.ExecOptionUseCurSession},
			"SELECT version, modify_count, count from mysql.stats_meta where table_id = %? order by version", tableID)
	}
	if err != nil || len(rows) == 0 {
		return
	}
	version = rows[0].GetUint64(0)
	modifyCount = rows[0].GetInt64(1)
	count = rows[0].GetInt64(2)
	return
}

// convertBoundFromBlob reads the bound from blob. The `blob` is read from the `mysql.stats_buckets` table.
// The `convertBoundFromBlob(convertBoundToBlob(a))` should be equal to `a`.
// TODO: add a test to make sure that this assumption is correct.
func convertBoundFromBlob(ctx types.Context, blob types.Datum, tp *types.FieldType) (types.Datum, error) {
	// For `BIT` type, when converting to `BLOB`, it's formated as an integer (when it's possible). Therefore, we should try to
	// parse it as an integer first.
	if tp.GetType() == mysql.TypeBit {
		var ret types.Datum

		// The implementation of converting BIT to BLOB will try to format it as an integer first. Theoretically, it should
		// always be able to format the integer because the `BIT` length is limited to 64. Therefore, this err should never
		// happen.
		uintValue, err := strconv.ParseUint(string(blob.GetBytes()), 10, 64)
		intest.AssertNoError(err)
		if err != nil {
			// Fail to parse, return the original blob as BIT directly.
			ret.SetBinaryLiteral(types.BinaryLiteral(blob.GetBytes()))
			return ret, nil
		}

		// part of the code is copied from `(*Datum).convertToMysqlBit`.
		if tp.GetFlen() < 64 && uintValue >= 1<<(uint64(tp.GetFlen())) {
			logutil.BgLogger().Warn("bound in stats exceeds the bit length", zap.Uint64("bound", uintValue), zap.Int("flen", tp.GetFlen()))
			err = types.ErrDataTooLong.GenWithStack("Data Too Long, field len %d", tp.GetFlen())
			intest.Assert(false, "bound in stats exceeds the bit length")
			uintValue = (1 << (uint64(tp.GetFlen()))) - 1
		}
		byteSize := (tp.GetFlen() + 7) >> 3
		ret.SetMysqlBit(types.NewBinaryLiteralFromUint(uintValue, byteSize))
		return ret, errors.Trace(err)
	}
	return blob.ConvertTo(ctx, tp)
}
