// Copyright 2019 PingCAP, Inc.
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

package executor

import (
	"bytes"
	"context"
	"encoding/binary"
	"math"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// SplitIndexRegionExec represents a split index regions executor.
type SplitIndexRegionExec struct {
	baseExecutor

	tableInfo  *model.TableInfo
	indexInfo  *model.IndexInfo
	min        []types.Datum
	max        []types.Datum
	num        int
	valueLists [][]types.Datum
}

type splitableStore interface {
	SplitRegionAndScatter(splitKey kv.Key) (uint64, error)
	WaitScatterRegionFinish(regionID uint64) error
}

// Next implements the Executor Next interface.
func (e *SplitIndexRegionExec) Next(ctx context.Context, _ *chunk.RecordBatch) error {
	store := e.ctx.GetStore()
	s, ok := store.(splitableStore)
	if !ok {
		return nil
	}
	splitIdxKeys, err := e.getSplitIdxKeys()
	if err != nil {
		return err
	}
	regionIDs := make([]uint64, 0, len(splitIdxKeys))
	for _, idxKey := range splitIdxKeys {
		regionID, err := s.SplitRegionAndScatter(idxKey)
		if err != nil {
			logutil.Logger(context.Background()).Warn("split table index region failed",
				zap.String("table", e.tableInfo.Name.L),
				zap.String("index", e.indexInfo.Name.L),
				zap.Error(err))
			continue
		}
		regionIDs = append(regionIDs, regionID)

	}
	if !e.ctx.GetSessionVars().WaitTableSplitFinish {
		return nil
	}
	for _, regionID := range regionIDs {
		err := s.WaitScatterRegionFinish(regionID)
		if err != nil {
			logutil.Logger(context.Background()).Warn("wait scatter region failed",
				zap.Uint64("regionID", regionID),
				zap.String("table", e.tableInfo.Name.L),
				zap.String("index", e.indexInfo.Name.L),
				zap.Error(err))
		}
	}
	return nil
}

func (e *SplitIndexRegionExec) getSplitIdxKeys() ([][]byte, error) {
	var idxKeys [][]byte
	if e.num > 0 {
		idxKeys = make([][]byte, 0, e.num)
	} else {
		idxKeys = make([][]byte, 0, len(e.valueLists)+1)
	}
	// Split in the start of the index key.
	startIdxKey := tablecodec.EncodeTableIndexPrefix(e.tableInfo.ID, e.indexInfo.ID)
	idxKeys = append(idxKeys, startIdxKey)

	index := tables.NewIndex(e.tableInfo.ID, e.tableInfo, e.indexInfo)
	// Split index regions by user specified value lists.
	if len(e.valueLists) > 0 {
		for _, v := range e.valueLists {
			idxKey, _, err := index.GenIndexKey(e.ctx.GetSessionVars().StmtCtx, v, math.MinInt64, nil)
			if err != nil {
				return nil, err
			}
			idxKeys = append(idxKeys, idxKey)
		}
		return idxKeys, nil
	}
	// Split index regions by min, max value and calculate the step by (max - min)/num.
	minIdxKey, _, err := index.GenIndexKey(e.ctx.GetSessionVars().StmtCtx, e.min, math.MinInt64, nil)
	if err != nil {
		return nil, err
	}
	maxIdxKey, _, err := index.GenIndexKey(e.ctx.GetSessionVars().StmtCtx, e.max, math.MinInt64, nil)
	if err != nil {
		return nil, err
	}
	if bytes.Compare(minIdxKey, maxIdxKey) >= 0 {
		return nil, errors.Errorf("Split index region `%v` min value %v should less than the max value %v", e.indexInfo.Name, e.min, e.max)
	}
	return getValuesList(minIdxKey, maxIdxKey, e.num, idxKeys), nil
}

// longestCommonPrefixLen gets the longest common prefix byte length.
func longestCommonPrefixLen(s1, s2 []byte) int {
	l := len(s1)
	if len(s2) < len(s1) {
		l = len(s2)
	}
	i := 0
	for ; i < l; i++ {
		if s1[i] != s2[i] {
			break
		}
	}
	return i
}

// getStepValue gets the step of between the min and max value. step = (max-min)/num.
// convert byte slice to uint64 first.
func getStepValue(min, max []byte, num int) uint64 {
	minUint := getUint64FromBytes(min, 0)
	maxUint := getUint64FromBytes(max, 0xff)
	return (maxUint - minUint) / uint64(num)
}

// getUint64FromBytes gets a uint64 from the `bs` byte slice.
// If len(bs) < 8, then padding with `pad`.
func getUint64FromBytes(bs []byte, pad byte) uint64 {
	buf := bs
	if len(buf) < 8 {
		buf = make([]byte, 0, 8)
		buf = append(buf, bs...)
		for i := len(buf); i < 8; i++ {
			buf = append(buf, pad)
		}
	}
	return binary.BigEndian.Uint64(buf)
}

// getValuesList is used to get `num` values between min and max value.
// To Simplify the explain, suppose min and max value type is int64, and min=0, max=100, num=10,
// then calculate the step=(max-min)/num=10, then the function should return 0+10, 10+10, 20+10... all together 9 (num-1) values.
// then the function will return [10,20,30,40,50,60,70,80,90].
// The difference is the max,min value type is []byte, So I use getUint64FromBytes to convert []byte to uint64.
func getValuesList(min, max []byte, num int, valuesList [][]byte) [][]byte {
	commonPrefixIdx := longestCommonPrefixLen(min, max)
	step := getStepValue(min[commonPrefixIdx:], max[commonPrefixIdx:], num)

	startValueTemp := min[commonPrefixIdx:]
	if len(startValueTemp) > 8 {
		startValueTemp = startValueTemp[:8]
	}
	startValue := make([]byte, 0, 8)
	startValue = append(startValue, startValueTemp...)
	for i := len(startValue); i < 8; i++ {
		startValue = append(startValue, 0)
	}
	startV := binary.BigEndian.Uint64(startValue)
	// To get `num` regions, only need to split `num-1` idx keys.
	tmp := make([]byte, 8)
	for i := 0; i < num-1; i++ {
		value := make([]byte, 0, commonPrefixIdx+8)
		value = append(value, min[:commonPrefixIdx]...)
		startV += step
		binary.BigEndian.PutUint64(tmp, startV)
		value = append(value, tmp...)
		valuesList = append(valuesList, value)
	}
	return valuesList
}
