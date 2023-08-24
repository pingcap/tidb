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

package cardinality

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/planner/util/debugtrace"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/util/ranger"
)

// GetRowCountByIndexRanges estimates the row count by a slice of Range.
func GetRowCountByIndexRanges(sctx sessionctx.Context, coll *statistics.HistColl, idxID int64, indexRanges []*ranger.Range) (result float64, err error) {
	var name string
	if sctx.GetSessionVars().StmtCtx.EnableOptimizerDebugTrace {
		debugtrace.EnterContextCommon(sctx)
		debugTraceGetRowCountInput(sctx, idxID, indexRanges)
		defer func() {
			debugtrace.RecordAnyValuesWithNames(sctx, "Name", name, "Result", result)
			debugtrace.LeaveContextCommon(sctx)
		}()
	}
	sc := sctx.GetSessionVars().StmtCtx
	idx, ok := coll.Indices[idxID]
	colNames := make([]string, 0, 8)
	if ok {
		if idx.Info != nil {
			name = idx.Info.Name.O
			for _, col := range idx.Info.Columns {
				colNames = append(colNames, col.Name.O)
			}
		}
	}
	recordUsedItemStatsStatus(sctx, idx, coll.PhysicalID, idxID)
	if !ok || idx.IsInvalid(sctx, coll.Pseudo) {
		colsLen := -1
		if idx != nil && idx.Info.Unique {
			colsLen = len(idx.Info.Columns)
		}
		result, err = getPseudoRowCountByIndexRanges(sc, indexRanges, float64(coll.RealtimeCount), colsLen)
		if err == nil && sc.EnableOptimizerCETrace && ok {
			CETraceRange(sctx, coll.PhysicalID, colNames, indexRanges, "Index Stats-Pseudo", uint64(result))
		}
		return result, err
	}
	if sctx.GetSessionVars().StmtCtx.EnableOptimizerDebugTrace {
		debugtrace.RecordAnyValuesWithNames(sctx,
			"Histogram NotNull Count", idx.Histogram.NotNullCount(),
			"TopN total count", idx.TopN.TotalCount(),
			"Increase Factor", idx.GetIncreaseFactor(coll.RealtimeCount),
		)
	}
	if idx.CMSketch != nil && idx.StatsVer == statistics.Version1 {
		result, err = coll.GetIndexRowCount(sctx, idxID, indexRanges)
	} else {
		result, err = idx.GetRowCount(sctx, coll, indexRanges, coll.RealtimeCount, coll.ModifyCount)
	}
	if sc.EnableOptimizerCETrace {
		CETraceRange(sctx, coll.PhysicalID, colNames, indexRanges, "Index Stats", uint64(result))
	}
	return result, errors.Trace(err)
}
