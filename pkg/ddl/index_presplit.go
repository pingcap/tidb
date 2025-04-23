// Copyright 2024 PingCAP, Inc.
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

package ddl

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/exprctx"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	contextutil "github.com/pingcap/tidb/pkg/util/context"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"go.uber.org/zap"
)

func preSplitIndexRegions(
	ctx context.Context,
	sctx sessionctx.Context,
	store kv.Storage,
	tblInfo *model.TableInfo,
	allIndexInfos []*model.IndexInfo,
	reorgMeta *model.DDLReorgMeta,
	args *model.ModifyIndexArgs,
) error {
	warnHandler := contextutil.NewStaticWarnHandler(0)
	exprCtx, err := newReorgExprCtxWithReorgMeta(reorgMeta, warnHandler)
	if err != nil {
		return errors.Trace(err)
	}
	splitOnTempIdx := reorgMeta.ReorgTp == model.ReorgTypeLitMerge ||
		reorgMeta.ReorgTp == model.ReorgTypeTxnMerge
	for i, idxInfo := range allIndexInfos {
		idxArg := args.IndexArgs[i]
		splitArgs, err := evalSplitDatumFromArgs(exprCtx, tblInfo, idxInfo, idxArg)
		if err != nil {
			return errors.Trace(err)
		}
		if splitArgs == nil {
			continue
		}
		splitKeys, err := getSplitIdxKeys(sctx, tblInfo, idxInfo, splitArgs)
		if err != nil {
			return errors.Trace(err)
		}
		if splitOnTempIdx {
			for i := range splitKeys {
				tablecodec.IndexKey2TempIndexKey(splitKeys[i])
			}
		}
		failpoint.InjectCall("beforePresplitIndex", splitKeys)
		err = splitIndexRegionAndWait(ctx, sctx, store, tblInfo, idxInfo, splitKeys)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

type splitArgs struct {
	byRows [][]types.Datum

	betweenLower []types.Datum
	betweenUpper []types.Datum
	regionsCnt   int
}

func getSplitIdxKeys(
	sctx sessionctx.Context,
	tblInfo *model.TableInfo,
	idxInfo *model.IndexInfo,
	args *splitArgs,
) ([][]byte, error) {
	// Split index regions by user specified value lists.
	if len(args.byRows) > 0 {
		return getSplitIdxKeysFromValueList(sctx, tblInfo, idxInfo, args.byRows)
	}

	return getSplitIdxKeysFromBound(
		sctx, tblInfo, idxInfo, args.betweenLower, args.betweenUpper, args.regionsCnt)
}

func getSplitIdxKeysFromValueList(
	sctx sessionctx.Context,
	tblInfo *model.TableInfo,
	idxInfo *model.IndexInfo,
	byRows [][]types.Datum,
) (destKeys [][]byte, err error) {
	pi := tblInfo.GetPartitionInfo()
	if pi == nil {
		destKeys = make([][]byte, 0, len(byRows)+1)
		return getSplitIdxPhysicalKeysFromValueList(sctx, tblInfo, idxInfo, tblInfo.ID, byRows, destKeys)
	}

	if idxInfo.Global {
		destKeys = make([][]byte, 0, len(byRows)+1)
		return getSplitIdxPhysicalKeysFromValueList(sctx, tblInfo, idxInfo, tblInfo.ID, byRows, destKeys)
	}

	destKeys = make([][]byte, 0, (len(byRows)+1)*len(pi.Definitions))
	for _, p := range pi.Definitions {
		destKeys, err = getSplitIdxPhysicalKeysFromValueList(sctx, tblInfo, idxInfo, p.ID, byRows, destKeys)
		if err != nil {
			return nil, err
		}
	}
	return destKeys, nil
}

func getSplitIdxPhysicalKeysFromValueList(
	sctx sessionctx.Context,
	tblInfo *model.TableInfo,
	idxInfo *model.IndexInfo,
	physicalID int64,
	splitDatum [][]types.Datum,
	destKeys [][]byte,
) ([][]byte, error) {
	destKeys = getSplitIdxPhysicalStartAndOtherIdxKeys(tblInfo, idxInfo, physicalID, destKeys)
	index := tables.NewIndex(physicalID, tblInfo, idxInfo)
	sc := sctx.GetSessionVars().StmtCtx
	for _, v := range splitDatum {
		idxKey, _, err := index.GenIndexKey(sc.ErrCtx(), sc.TimeZone(), v, kv.IntHandle(math.MinInt64), nil)
		if err != nil {
			return nil, err
		}
		destKeys = append(destKeys, idxKey)
	}
	return destKeys, nil
}

func getSplitIdxPhysicalStartAndOtherIdxKeys(
	tblInfo *model.TableInfo,
	idxInfo *model.IndexInfo,
	physicalID int64,
	keys [][]byte,
) [][]byte {
	// 1. Split in the start key for the index if the index is not the first index.
	// For the first index, splitting the start key can produce the region [tid, tid_i_1), which is useless.
	if len(tblInfo.Indices) > 0 && tblInfo.Indices[0].ID != idxInfo.ID {
		startKey := tablecodec.EncodeTableIndexPrefix(physicalID, idxInfo.ID)
		keys = append(keys, startKey)
	}

	// 2. Split in the end key.
	endKey := tablecodec.EncodeTableIndexPrefix(physicalID, idxInfo.ID+1)
	keys = append(keys, endKey)
	return keys
}

func getSplitIdxKeysFromBound(
	sctx sessionctx.Context,
	tblInfo *model.TableInfo,
	idxInfo *model.IndexInfo,
	lower, upper []types.Datum,
	splitNum int,
) (keys [][]byte, err error) {
	pi := tblInfo.GetPartitionInfo()
	if pi == nil {
		keys = make([][]byte, 0, splitNum)
		return getSplitIdxPhysicalKeysFromBound(
			sctx, tblInfo, idxInfo, tblInfo.ID, lower, upper, splitNum, keys)
	}
	keys = make([][]byte, 0, splitNum*len(pi.Definitions))
	for _, p := range pi.Definitions {
		keys, err = getSplitIdxPhysicalKeysFromBound(
			sctx, tblInfo, idxInfo, p.ID, lower, upper, splitNum, keys)
		if err != nil {
			return nil, err
		}
	}
	return keys, nil
}

func getSplitIdxPhysicalKeysFromBound(
	sctx sessionctx.Context,
	tblInfo *model.TableInfo,
	idxInfo *model.IndexInfo,
	physicalID int64,
	lower, upper []types.Datum,
	splitNum int,
	destKeys [][]byte,
) ([][]byte, error) {
	destKeys = getSplitIdxPhysicalStartAndOtherIdxKeys(tblInfo, idxInfo, physicalID, destKeys)
	index := tables.NewIndex(physicalID, tblInfo, idxInfo)
	// Split index regions by lower, upper value and calculate the step by (upper - lower)/num.
	sc := sctx.GetSessionVars().StmtCtx
	lowerIdxKey, _, err := index.GenIndexKey(sc.ErrCtx(), sc.TimeZone(), lower, kv.IntHandle(math.MinInt64), nil)
	if err != nil {
		return nil, err
	}
	// Use math.MinInt64 as handle_id for the upper index key to avoid affecting calculate split point.
	// If use math.MaxInt64 here, test of `TestSplitIndex` will report error.
	upperIdxKey, _, err := index.GenIndexKey(sc.ErrCtx(), sc.TimeZone(), upper, kv.IntHandle(math.MinInt64), nil)
	if err != nil {
		return nil, err
	}

	if bytes.Compare(lowerIdxKey, upperIdxKey) >= 0 {
		lowerStr := datumSliceToString(lower)
		upperStr := datumSliceToString(upper)
		errMsg := fmt.Sprintf("Split index `%v` region lower value %v should less than the upper value %v",
			idxInfo.Name, lowerStr, upperStr)
		return nil, exeerrors.ErrInvalidSplitRegionRanges.GenWithStackByArgs(errMsg)
	}
	return util.GetValuesList(lowerIdxKey, upperIdxKey, splitNum, destKeys), nil
}

func datumSliceToString(ds []types.Datum) string {
	str := "("
	for i, d := range ds {
		s, err := d.ToString()
		if err != nil {
			return fmt.Sprintf("%v", ds)
		}
		if i > 0 {
			str += ","
		}
		str += s
	}
	str += ")"
	return str
}

func splitIndexRegionAndWait(
	ctx context.Context,
	sctx sessionctx.Context,
	store kv.Storage,
	tblInfo *model.TableInfo,
	idxInfo *model.IndexInfo,
	splitIdxKeys [][]byte,
) error {
	s, ok := store.(kv.SplittableStore)
	if !ok {
		return nil
	}
	start := time.Now()
	ctxWithTimeout, cancel := context.WithTimeout(ctx, sctx.GetSessionVars().GetSplitRegionTimeout())
	defer cancel()
	regionIDs, err := s.SplitRegions(ctxWithTimeout, splitIdxKeys, true, &tblInfo.ID)
	if err != nil {
		logutil.DDLLogger().Error("split table index region failed",
			zap.String("table", tblInfo.Name.L),
			zap.String("index", tblInfo.Name.L),
			zap.Error(err))
		return err
	}
	failpoint.Inject("mockSplitIndexRegionAndWaitErr", func(_ failpoint.Value) {
		failpoint.Return(context.DeadlineExceeded)
	})
	finishScatterRegions := waitScatterRegionFinish(ctxWithTimeout, sctx, start, s, regionIDs, tblInfo.Name.L, idxInfo.Name.L)
	logutil.DDLLogger().Info("split table index region finished",
		zap.String("table", tblInfo.Name.L),
		zap.String("index", idxInfo.Name.L),
		zap.Int("splitRegions", len(regionIDs)),
		zap.Int("scatterRegions", finishScatterRegions),
	)
	return nil
}

func evalSplitDatumFromArgs(
	buildCtx exprctx.BuildContext,
	tblInfo *model.TableInfo,
	idxInfo *model.IndexInfo,
	idxArg *model.IndexArg,
) (*splitArgs, error) {
	opt := idxArg.SplitOpt
	if opt == nil {
		return nil, nil
	}
	if len(opt.ValueLists) > 0 {
		indexValues := make([][]types.Datum, 0, len(opt.ValueLists))
		for i, valueList := range opt.ValueLists {
			if len(valueList) > len(idxInfo.Columns) {
				return nil, plannererrors.ErrWrongValueCountOnRow.GenWithStackByArgs(i + 1)
			}
			values, err := evalConstExprNodes(buildCtx, valueList, tblInfo, idxInfo)
			if err != nil {
				return nil, err
			}
			indexValues = append(indexValues, values)
		}
		return &splitArgs{byRows: indexValues}, nil
	}

	if len(opt.Lower) == 0 && len(opt.Upper) == 0 && opt.Num > 0 {
		lowerVals := make([]types.Datum, 0, len(idxInfo.Columns))
		upperVals := make([]types.Datum, 0, len(idxInfo.Columns))
		for i := 0; i < len(idxInfo.Columns); i++ {
			lowerVals = append(lowerVals, types.MinNotNullDatum())
			upperVals = append(upperVals, types.MaxValueDatum())
		}
		return &splitArgs{
			betweenLower: lowerVals,
			betweenUpper: upperVals,
			regionsCnt:   int(opt.Num),
		}, nil
	}

	// Split index regions by lower, upper value.
	checkLowerUpperValue := func(valuesItem []string, name string) ([]types.Datum, error) {
		if len(valuesItem) == 0 {
			return nil, errors.Errorf("Split index `%v` region %s value count should be greater than 0", idxInfo.Name, name)
		}
		if len(valuesItem) > len(idxInfo.Columns) {
			return nil, errors.Errorf("Split index `%v` region column count doesn't match value count at %v", idxInfo.Name, name)
		}
		return evalConstExprNodes(buildCtx, valuesItem, tblInfo, idxInfo)
	}
	lowerValues, err := checkLowerUpperValue(opt.Lower, "lower")
	if err != nil {
		return nil, err
	}
	upperValues, err := checkLowerUpperValue(opt.Upper, "upper")
	if err != nil {
		return nil, err
	}
	splitArgs := &splitArgs{
		betweenLower: lowerValues,
		betweenUpper: upperValues,
	}
	splitArgs.regionsCnt = int(opt.Num)
	return splitArgs, nil
}

func evalConstExprNodes(
	buildCtx exprctx.BuildContext,
	valueList []string,
	tblInfo *model.TableInfo,
	idxInfo *model.IndexInfo,
) ([]types.Datum, error) {
	values := make([]types.Datum, 0, len(valueList))
	for j, value := range valueList {
		colOffset := idxInfo.Columns[j].Offset
		col := tblInfo.Columns[colOffset]
		exp, err := expression.ParseSimpleExpr(buildCtx, value)
		if err != nil {
			return nil, err
		}
		evalCtx := buildCtx.GetEvalCtx()
		evaluatedVal, err := exp.Eval(evalCtx, chunk.Row{})
		if err != nil {
			return nil, err
		}

		d, err := evaluatedVal.ConvertTo(evalCtx.TypeCtx(), &col.FieldType)
		if err != nil {
			if !types.ErrTruncated.Equal(err) &&
				!types.ErrTruncatedWrongVal.Equal(err) &&
				!types.ErrBadNumber.Equal(err) {
				return nil, err
			}
			valStr, err1 := evaluatedVal.ToString()
			if err1 != nil {
				return nil, err
			}
			return nil, types.ErrTruncated.GenWithStack("Incorrect value: '%-.128s' for column '%.192s'", valStr, col.Name.O)
		}
		values = append(values, d)
	}
	return values, nil
}

func waitScatterRegionFinish(
	ctxWithTimeout context.Context,
	sctx sessionctx.Context,
	startTime time.Time,
	store kv.SplittableStore,
	regionIDs []uint64,
	tableName, indexName string,
) int {
	remainMillisecond := 0
	finishScatterNum := 0
	for _, regionID := range regionIDs {
		select {
		case <-ctxWithTimeout.Done():
			// Do not break here for checking remain regions scatter finished with a very short backoff time.
			// Consider this situation -  Regions 1, 2, and 3 are to be split.
			// Region 1 times out before scattering finishes, while Region 2 and Region 3 have finished scattering.
			// In this case, we should return 2 Regions, instead of 0, have finished scattering.
			remainMillisecond = 50
		default:
			remainMillisecond = int((sctx.GetSessionVars().GetSplitRegionTimeout().Seconds() - time.Since(startTime).Seconds()) * 1000)
		}

		err := store.WaitScatterRegionFinish(ctxWithTimeout, regionID, remainMillisecond)
		if err == nil {
			finishScatterNum++
		} else {
			logutil.DDLLogger().Warn("wait scatter region failed",
				zap.Uint64("regionID", regionID),
				zap.String("table", tableName),
				zap.String("index", indexName),
				zap.Error(err))
		}
	}
	return finishScatterNum
}
