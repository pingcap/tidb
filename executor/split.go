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
	"context"
	"math"

	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// SplitIndexRegionExec represents a split index regions executor.
type SplitIndexRegionExec struct {
	baseExecutor

	table      table.Table
	indexInfo  *model.IndexInfo
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
	regionIDs := make([]uint64, 0, len(e.valueLists))
	index := tables.NewIndex(e.table.Meta().ID, e.table.Meta(), e.indexInfo)
	for _, values := range e.valueLists {
		idxKey, _, err := index.GenIndexKey(e.ctx.GetSessionVars().StmtCtx, values, math.MinInt64, nil)
		if err != nil {
			return err
		}

		regionID, err := s.SplitRegionAndScatter(idxKey)
		if err != nil {
			logutil.Logger(context.Background()).Warn("split table index region failed",
				zap.String("table", e.table.Meta().Name.L),
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
				zap.String("table", e.table.Meta().Name.L),
				zap.String("index", e.indexInfo.Name.L),
				zap.Error(err))
		}
	}
	return nil
}
