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
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/ranger"
)

func init() {
	statistics.GetRowCountByColumnRanges = GetRowCountByColumnRanges
	statistics.GetRowCountByIntColumnRanges = GetRowCountByIntColumnRanges
	statistics.GetRowCountByIndexRanges = GetRowCountByIndexRanges
	statistics.OutOfRangeEQSelectivity = outOfRangeEQSelectivity
	statistics.GetEqualCondSelectivity = getEqualCondSelectivity
}

// GetRowCountByColumnRanges estimates the row count by a slice of Range.
func GetRowCountByColumnRanges(sctx sessionctx.Context, coll *statistics.HistColl, colID int64, colRanges []*ranger.Range) (result float64, err error) {
	var name string
	if sctx.GetSessionVars().StmtCtx.EnableOptimizerDebugTrace {
		debugtrace.EnterContextCommon(sctx)
		debugTraceGetRowCountInput(sctx, colID, colRanges)
		defer func() {
			debugtrace.RecordAnyValuesWithNames(sctx, "Name", name, "Result", result)
			debugtrace.LeaveContextCommon(sctx)
		}()
	}
	sc := sctx.GetSessionVars().StmtCtx
	c, ok := coll.Columns[colID]
	recordUsedItemStatsStatus(sctx, c, coll.PhysicalID, colID)
	if c != nil && c.Info != nil {
		name = c.Info.Name.O
	}
	if !ok || c.IsInvalid(sctx, coll.Pseudo) {
		result, err = GetPseudoRowCountByColumnRanges(sc, float64(coll.RealtimeCount), colRanges, 0)
		if err == nil && sc.EnableOptimizerCETrace && ok {
			CETraceRange(sctx, coll.PhysicalID, []string{c.Info.Name.O}, colRanges, "Column Stats-Pseudo", uint64(result))
		}
		return result, err
	}
	if sctx.GetSessionVars().StmtCtx.EnableOptimizerDebugTrace {
		debugtrace.RecordAnyValuesWithNames(sctx,
			"Histogram NotNull Count", c.Histogram.NotNullCount(),
			"TopN total count", c.TopN.TotalCount(),
			"Increase Factor", c.GetIncreaseFactor(coll.RealtimeCount),
		)
	}
	result, err = c.GetColumnRowCount(sctx, colRanges, coll.RealtimeCount, coll.ModifyCount, false)
	if sc.EnableOptimizerCETrace {
		CETraceRange(sctx, coll.PhysicalID, []string{c.Info.Name.O}, colRanges, "Column Stats", uint64(result))
	}
	return result, errors.Trace(err)
}

// GetRowCountByIntColumnRanges estimates the row count by a slice of IntColumnRange.
func GetRowCountByIntColumnRanges(sctx sessionctx.Context, coll *statistics.HistColl, colID int64, intRanges []*ranger.Range) (result float64, err error) {
	var name string
	if sctx.GetSessionVars().StmtCtx.EnableOptimizerDebugTrace {
		debugtrace.EnterContextCommon(sctx)
		debugTraceGetRowCountInput(sctx, colID, intRanges)
		defer func() {
			debugtrace.RecordAnyValuesWithNames(sctx, "Name", name, "Result", result)
			debugtrace.LeaveContextCommon(sctx)
		}()
	}
	sc := sctx.GetSessionVars().StmtCtx
	c, ok := coll.Columns[colID]
	recordUsedItemStatsStatus(sctx, c, coll.PhysicalID, colID)
	if c != nil && c.Info != nil {
		name = c.Info.Name.O
	}
	if !ok || c.IsInvalid(sctx, coll.Pseudo) {
		if len(intRanges) == 0 {
			return 0, nil
		}
		if intRanges[0].LowVal[0].Kind() == types.KindInt64 {
			result = getPseudoRowCountBySignedIntRanges(intRanges, float64(coll.RealtimeCount))
		} else {
			result = getPseudoRowCountByUnsignedIntRanges(intRanges, float64(coll.RealtimeCount))
		}
		if sc.EnableOptimizerCETrace && ok {
			CETraceRange(sctx, coll.PhysicalID, []string{c.Info.Name.O}, intRanges, "Column Stats-Pseudo", uint64(result))
		}
		return result, nil
	}
	if sctx.GetSessionVars().StmtCtx.EnableOptimizerDebugTrace {
		debugtrace.RecordAnyValuesWithNames(sctx,
			"Histogram NotNull Count", c.Histogram.NotNullCount(),
			"TopN total count", c.TopN.TotalCount(),
			"Increase Factor", c.GetIncreaseFactor(coll.RealtimeCount),
		)
	}
	result, err = c.GetColumnRowCount(sctx, intRanges, coll.RealtimeCount, coll.ModifyCount, true)
	if sc.EnableOptimizerCETrace {
		CETraceRange(sctx, coll.PhysicalID, []string{c.Info.Name.O}, intRanges, "Column Stats", uint64(result))
	}
	return result, errors.Trace(err)
}
