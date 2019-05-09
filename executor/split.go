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
	idxKeys := make([][]byte, 0, len(e.valueLists))
	index := tables.NewIndex(e.tableInfo.ID, e.tableInfo, e.indexInfo)
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
	idxKeys = make([][]byte, 0, e.num+1)
	return getValuesList(minIdxKey, maxIdxKey, e.num), nil
}

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

func getDiffBytesValue(startIdx int, min, max []byte) uint64 {
	l := len(min)
	if len(max) < len(min) {
		l = len(max)
	}
	if l-startIdx > 8 {
		l = startIdx + 8
	}
	diff := make([]byte, 0, 8)
	for i := startIdx; i < l; i++ {
		diff = append(diff, max[i]-min[i])
	}
	if len(max) > l {
		for i := l; i < len(max); i++ {
			diff = append(diff, max[i])
			if len(diff) >= 8 {
				break
			}
		}
	}
	if len(min) > l {
		for i := l; i < len(min); i++ {
			diff = append(diff, 0xff-min[i])
			if len(diff) >= 8 {
				break
			}
		}
	}

	for i := len(diff); i < 8; i++ {
		diff = append(diff, 0xff)
	}
	diffValue := binary.BigEndian.Uint64(diff)
	return diffValue
}

func getValuesList(min, max []byte, num int) [][]byte {
	startIdx := longestCommonPrefixLen(min, max)
	diffValue := getDiffBytesValue(startIdx, min, max)
	step := diffValue / uint64(num)

	startValueTemp := min[startIdx:]
	if len(startValueTemp) > 8 {
		startValueTemp = startValueTemp[:8]
	}
	startValue := make([]byte, 0, 8)
	startValue = append(startValue, startValueTemp...)
	for i := len(startValue); i < 8; i++ {
		startValue = append(startValue, 0)
	}
	startV := binary.BigEndian.Uint64(startValue)
	valuesList := make([][]byte, 0, num+1)
	valuesList = append(valuesList, min)
	tmp := make([]byte, 8)
	for i := 0; i < num; i++ {
		value := make([]byte, 0, startIdx+8)
		value = append(value, min[:startIdx]...)
		startV += step
		binary.BigEndian.PutUint64(tmp, startV)
		value = append(value, tmp...)
		valuesList = append(valuesList, value)
	}
	return valuesList
}
