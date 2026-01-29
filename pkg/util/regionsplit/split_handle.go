// Copyright 2025 PingCAP, Inc.
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

package regionsplit

import (
	"bytes"
	"fmt"
	"math"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	cutil "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/codec"
)

// MinRegionStepValue is the minimum step.
var MinRegionStepValue = int64(1000)

// SplitHandleCols is a minimal interface for region split operations.
type SplitHandleCols interface {
	// BuildHandleByDatums builds a Handle from a datum slice.
	BuildHandleByDatums(sc *stmtctx.StatementContext, row []types.Datum) (kv.Handle, error)
	// IsInt returns if the HandleCols is a single int column.
	IsInt() bool
}

// calculateIntBoundValue calculates the lower value and step for int handle split.
// This function strictly follows the logic from SplitTableRegionExec.calculateIntBoundValue.
// The splitRangeError parameter should be a terror.Error like exeerrors.ErrInvalidSplitRegionRanges.
func calculateIntBoundValue(tbInfo *model.TableInfo, lower, upper []types.Datum, num int, splitRangeError *terror.Error) (lowerValue int64, step int64, err error) {
	isUnsigned := false
	if tbInfo.PKIsHandle {
		if pkCol := tbInfo.GetPkColInfo(); pkCol != nil {
			isUnsigned = mysql.HasUnsignedFlag(pkCol.GetFlag())
		}
	}
	if isUnsigned {
		lowerRecordID := lower[0].GetUint64()
		upperRecordID := upper[0].GetUint64()
		if upperRecordID <= lowerRecordID {
			errMsg := fmt.Sprintf("lower value %v should less than the upper value %v", lowerRecordID, upperRecordID)
			return 0, 0, splitRangeError.GenWithStackByArgs(errMsg)
		}
		step = int64((upperRecordID - lowerRecordID) / uint64(num))
		lowerValue = int64(lowerRecordID)
	} else {
		lowerRecordID := lower[0].GetInt64()
		upperRecordID := upper[0].GetInt64()
		if upperRecordID <= lowerRecordID {
			errMsg := fmt.Sprintf("lower value %v should less than the upper value %v", lowerRecordID, upperRecordID)
			return 0, 0, splitRangeError.GenWithStackByArgs(errMsg)
		}
		step = int64(uint64(upperRecordID-lowerRecordID) / uint64(num))
		lowerValue = lowerRecordID
	}
	if step < MinRegionStepValue {
		errMsg := fmt.Sprintf("the region size is too small, expected at least %d, but got %d", MinRegionStepValue, step)
		return 0, 0, splitRangeError.GenWithStackByArgs(errMsg)
	}
	return lowerValue, step, nil
}

// GetSplitTableKeys generates split keys for table's record data.
// This function strictly follows the logic from SplitTableRegionExec.getSplitTablePhysicalKeysFromBound.
// The splitRangeError parameter should be a terror.Error like exeerrors.ErrInvalidSplitRegionRanges.
func GetSplitTableKeys(sc *stmtctx.StatementContext, tbInfo *model.TableInfo, handleCols SplitHandleCols,
	physicalID int64, lower, upper []types.Datum, num int, keys [][]byte, splitRangeError *terror.Error) ([][]byte, error) {
	recordPrefix := tablecodec.GenTableRecordPrefix(physicalID)
	// Split a separate region for index.
	containsIndex := len(tbInfo.Indices) > 0 && !(tbInfo.IsCommonHandle && len(tbInfo.Indices) == 1)
	if containsIndex {
		keys = append(keys, recordPrefix)
	}

	if handleCols.IsInt() {
		low, step, err := calculateIntBoundValue(tbInfo, lower, upper, num, splitRangeError)
		if err != nil {
			return keys, err
		}
		recordID := low
		for i := 1; i < num; i++ {
			recordID += step
			key := tablecodec.EncodeRecordKey(recordPrefix, kv.IntHandle(recordID))
			keys = append(keys, key)
		}
		return keys, nil
	}

	// For common handle
	lowerHandle, err := handleCols.BuildHandleByDatums(sc, lower)
	if err != nil {
		return keys, err
	}
	upperHandle, err := handleCols.BuildHandleByDatums(sc, upper)
	if err != nil {
		return keys, err
	}
	if lowerHandle.Compare(upperHandle) >= 0 {
		lowerStr := datumSliceToString(lower)
		upperStr := datumSliceToString(upper)
		errMsg := fmt.Sprintf("Split table `%v` region lower value %v should less than the upper value %v",
			tbInfo.Name.O, lowerStr, upperStr)
		return keys, splitRangeError.GenWithStackByArgs(errMsg)
	}
	low := tablecodec.EncodeRecordKey(recordPrefix, lowerHandle)
	up := tablecodec.EncodeRecordKey(recordPrefix, upperHandle)
	return cutil.GetValuesList(low, up, num, keys), nil
}

// GetSplitIdxPhysicalStartAndOtherIdxKeys generates start and end keys for an index.
// This function strictly follows the logic from SplitIndexRegionExec.getSplitIdxPhysicalStartAndOtherIdxKeys.
func GetSplitIdxPhysicalStartAndOtherIdxKeys(tbInfo *model.TableInfo, indexInfo *model.IndexInfo,
	physicalID int64, keys [][]byte) [][]byte {
	// 1. Split in the start key for the index if the index is not the first index.
	// For the first index, splitting the start key can produce the region [tid, tid_i_1), which is useless.
	if len(tbInfo.Indices) > 0 && tbInfo.Indices[0].ID != indexInfo.ID {
		startKey := tablecodec.EncodeTableIndexPrefix(physicalID, indexInfo.ID)
		keys = append(keys, startKey)
	}

	// 2. Split in the end key.
	endKey := tablecodec.EncodeTableIndexPrefix(physicalID, indexInfo.ID+1)
	keys = append(keys, endKey)
	return keys
}

// GetSplitIndexKeys generates split keys for index.
// This function strictly follows the logic from SplitIndexRegionExec.getSplitIdxPhysicalKeysFromBound.
// The splitRangeError parameter should be a terror.Error like exeerrors.ErrInvalidSplitRegionRanges.
func GetSplitIndexKeys(sc *stmtctx.StatementContext, tbInfo *model.TableInfo, indexInfo *model.IndexInfo,
	physicalID int64, lower, upper []types.Datum, num int, keys [][]byte, splitRangeError *terror.Error) ([][]byte, error) {
	newkeys := GetSplitIdxPhysicalStartAndOtherIdxKeys(tbInfo, indexInfo, physicalID, keys)

	index, err := tables.NewIndex(physicalID, tbInfo, indexInfo)
	if err != nil {
		return keys, err
	}
	// Split index regions by lower, upper value and calculate the step by (upper - lower)/num.
	lowerIdxKey, _, err := index.GenIndexKey(sc.ErrCtx(), sc.TimeZone(), lower, kv.IntHandle(math.MinInt64), nil)
	if err != nil {
		return keys, err
	}
	// Use math.MinInt64 as handle_id for the upper index key to avoid affecting calculate split point.
	// If use math.MaxInt64 here, test of `TestSplitIndex` will report error.
	upperIdxKey, _, err := index.GenIndexKey(sc.ErrCtx(), sc.TimeZone(), upper, kv.IntHandle(math.MinInt64), nil)
	if err != nil {
		return keys, err
	}

	if bytes.Compare(lowerIdxKey, upperIdxKey) >= 0 {
		lowerStr := datumSliceToString(lower)
		upperStr := datumSliceToString(upper)
		errMsg := fmt.Sprintf("Split index `%v` region lower value %v should less than the upper value %v",
			indexInfo.Name, lowerStr, upperStr)
		return keys, splitRangeError.GenWithStackByArgs(errMsg)
	}
	return cutil.GetValuesList(lowerIdxKey, upperIdxKey, num, newkeys), nil
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

type intHandleCols struct{}

func newIntHandleCols() *intHandleCols {
	return &intHandleCols{}
}

// BuildHandleByDatums implements SplitHandleCols interface.
func (*intHandleCols) BuildHandleByDatums(_ *stmtctx.StatementContext, row []types.Datum) (kv.Handle, error) {
	return kv.IntHandle(row[0].GetInt64()), nil
}

// IsInt implements SplitHandleCols interface.
func (*intHandleCols) IsInt() bool {
	return true
}

type commonHandleCols struct{}

func newCommonHandleCols() *commonHandleCols {
	return &commonHandleCols{}
}

// BuildHandleByDatums implements SplitHandleCols interface.
func (*commonHandleCols) BuildHandleByDatums(sc *stmtctx.StatementContext, row []types.Datum) (kv.Handle, error) {
	handleBytes, err := codec.EncodeKey(sc.TimeZone(), nil, row...)
	if err != nil {
		return nil, err
	}
	return kv.NewCommonHandle(handleBytes)
}

// IsInt implements SplitHandleCols interface.
func (*commonHandleCols) IsInt() bool {
	return false
}

// BuildHandleColsForSplit builds a SplitHandleCols for region split operations.
func BuildHandleColsForSplit(tbInfo *model.TableInfo) SplitHandleCols {
	if tbInfo.IsCommonHandle {
		return newCommonHandleCols()
	}
	return newIntHandleCols()
}
