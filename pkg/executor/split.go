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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"time"

	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	cutil "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// SplitIndexRegionExec represents a split index regions executor.
type SplitIndexRegionExec struct {
	exec.BaseExecutor

	tableInfo      *model.TableInfo
	partitionNames []ast.CIStr
	indexInfo      *model.IndexInfo
	lower          []types.Datum
	upper          []types.Datum
	num            int
	valueLists     [][]types.Datum
	splitIdxKeys   [][]byte

	done bool
	splitRegionResult
}

// nolint:structcheck
type splitRegionResult struct {
	splitRegions     int
	finishScatterNum int
}

// Open implements the Executor Open interface.
func (e *SplitIndexRegionExec) Open(context.Context) (err error) {
	e.splitIdxKeys, err = e.getSplitIdxKeys()
	return err
}

// Next implements the Executor Next interface.
func (e *SplitIndexRegionExec) Next(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	if e.done {
		return nil
	}
	e.done = true
	if err := e.splitIndexRegion(ctx); err != nil {
		return err
	}

	appendSplitRegionResultToChunk(chk, e.splitRegions, e.finishScatterNum)
	return nil
}

// checkScatterRegionFinishBackOff is the back off time that used to check if a region has finished scattering before split region timeout.
const checkScatterRegionFinishBackOff = 50

// splitIndexRegion is used to split index regions.
func (e *SplitIndexRegionExec) splitIndexRegion(ctx context.Context) error {
	store := e.Ctx().GetStore()
	s, ok := store.(kv.SplittableStore)
	if !ok {
		return nil
	}

	start := time.Now()
	ctxWithTimeout, cancel := context.WithTimeout(ctx, e.Ctx().GetSessionVars().GetSplitRegionTimeout())
	defer cancel()
	regionIDs, err := s.SplitRegions(ctxWithTimeout, e.splitIdxKeys, true, &e.tableInfo.ID)
	if err != nil {
		logutil.BgLogger().Warn("split table index region failed",
			zap.String("table", e.tableInfo.Name.L),
			zap.String("index", e.indexInfo.Name.L),
			zap.Error(err))
	}
	e.splitRegions = len(regionIDs)
	if e.splitRegions == 0 {
		return nil
	}

	if !e.Ctx().GetSessionVars().WaitSplitRegionFinish {
		return nil
	}
	e.finishScatterNum = waitScatterRegionFinish(ctxWithTimeout, e.Ctx(), start, s, regionIDs, e.tableInfo.Name.L, e.indexInfo.Name.L)
	return nil
}

func (e *SplitIndexRegionExec) getSplitIdxKeys() ([][]byte, error) {
	// Split index regions by user specified value lists.
	if len(e.valueLists) > 0 {
		return e.getSplitIdxKeysFromValueList()
	}

	return e.getSplitIdxKeysFromBound()
}

func (e *SplitIndexRegionExec) getSplitIdxKeysFromValueList() (keys [][]byte, err error) {
	pi := e.tableInfo.GetPartitionInfo()
	if pi == nil {
		keys = make([][]byte, 0, len(e.valueLists)+1)
		return e.getSplitIdxPhysicalKeysFromValueList(e.tableInfo.ID, keys)
	}

	// Split for all table partitions.
	if len(e.partitionNames) == 0 {
		keys = make([][]byte, 0, (len(e.valueLists)+1)*len(pi.Definitions))
		for _, p := range pi.Definitions {
			keys, err = e.getSplitIdxPhysicalKeysFromValueList(p.ID, keys)
			if err != nil {
				return nil, err
			}
		}
		return keys, nil
	}

	// Split for specified table partitions.
	keys = make([][]byte, 0, (len(e.valueLists)+1)*len(e.partitionNames))
	for _, name := range e.partitionNames {
		pid, err := tables.FindPartitionByName(e.tableInfo, name.L)
		if err != nil {
			return nil, err
		}
		keys, err = e.getSplitIdxPhysicalKeysFromValueList(pid, keys)
		if err != nil {
			return nil, err
		}
	}
	return keys, nil
}

func (e *SplitIndexRegionExec) getSplitIdxPhysicalKeysFromValueList(physicalID int64, keys [][]byte) ([][]byte, error) {
	keys = e.getSplitIdxPhysicalStartAndOtherIdxKeys(physicalID, keys)
	index, err := tables.NewIndex(physicalID, e.tableInfo, e.indexInfo)
	if err != nil {
		return nil, err
	}
	sc := e.Ctx().GetSessionVars().StmtCtx
	for _, v := range e.valueLists {
		idxKey, _, err := index.GenIndexKey(sc.ErrCtx(), sc.TimeZone(), v, kv.IntHandle(math.MinInt64), nil)
		if err != nil {
			return nil, err
		}
		keys = append(keys, idxKey)
	}
	return keys, nil
}

func (e *SplitIndexRegionExec) getSplitIdxPhysicalStartAndOtherIdxKeys(physicalID int64, keys [][]byte) [][]byte {
	// 1. Split in the start key for the index if the index is not the first index.
	// For the first index, splitting the start key can produce the region [tid, tid_i_1), which is useless.
	if len(e.tableInfo.Indices) > 0 && e.tableInfo.Indices[0].ID != e.indexInfo.ID {
		startKey := tablecodec.EncodeTableIndexPrefix(physicalID, e.indexInfo.ID)
		keys = append(keys, startKey)
	}

	// 2. Split in the end key.
	endKey := tablecodec.EncodeTableIndexPrefix(physicalID, e.indexInfo.ID+1)
	keys = append(keys, endKey)
	return keys
}

func (e *SplitIndexRegionExec) getSplitIdxKeysFromBound() (keys [][]byte, err error) {
	pi := e.tableInfo.GetPartitionInfo()
	if pi == nil {
		keys = make([][]byte, 0, e.num)
		return e.getSplitIdxPhysicalKeysFromBound(e.tableInfo.ID, keys)
	}

	// Split for all table partitions.
	if len(e.partitionNames) == 0 {
		keys = make([][]byte, 0, e.num*len(pi.Definitions))
		for _, p := range pi.Definitions {
			keys, err = e.getSplitIdxPhysicalKeysFromBound(p.ID, keys)
			if err != nil {
				return nil, err
			}
		}
		return keys, nil
	}

	// Split for specified table partitions.
	keys = make([][]byte, 0, e.num*len(e.partitionNames))
	for _, name := range e.partitionNames {
		pid, err := tables.FindPartitionByName(e.tableInfo, name.L)
		if err != nil {
			return nil, err
		}
		keys, err = e.getSplitIdxPhysicalKeysFromBound(pid, keys)
		if err != nil {
			return nil, err
		}
	}
	return keys, nil
}

func (e *SplitIndexRegionExec) getSplitIdxPhysicalKeysFromBound(physicalID int64, keys [][]byte) ([][]byte, error) {
	keys = e.getSplitIdxPhysicalStartAndOtherIdxKeys(physicalID, keys)
	index, err := tables.NewIndex(physicalID, e.tableInfo, e.indexInfo)
	if err != nil {
		return nil, err
	}
	// Split index regions by lower, upper value and calculate the step by (upper - lower)/num.
	sc := e.Ctx().GetSessionVars().StmtCtx
	lowerIdxKey, _, err := index.GenIndexKey(sc.ErrCtx(), sc.TimeZone(), e.lower, kv.IntHandle(math.MinInt64), nil)
	if err != nil {
		return nil, err
	}
	// Use math.MinInt64 as handle_id for the upper index key to avoid affecting calculate split point.
	// If use math.MaxInt64 here, test of `TestSplitIndex` will report error.
	upperIdxKey, _, err := index.GenIndexKey(sc.ErrCtx(), sc.TimeZone(), e.upper, kv.IntHandle(math.MinInt64), nil)
	if err != nil {
		return nil, err
	}

	if bytes.Compare(lowerIdxKey, upperIdxKey) >= 0 {
		lowerStr := datumSliceToString(e.lower)
		upperStr := datumSliceToString(e.upper)
		errMsg := fmt.Sprintf("Split index `%v` region lower value %v should less than the upper value %v",
			e.indexInfo.Name, lowerStr, upperStr)
		return nil, exeerrors.ErrInvalidSplitRegionRanges.GenWithStackByArgs(errMsg)
	}
	return cutil.GetValuesList(lowerIdxKey, upperIdxKey, e.num, keys), nil
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

