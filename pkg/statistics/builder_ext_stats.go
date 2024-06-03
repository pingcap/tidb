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

package statistics

import (
	"context"
	"encoding/json"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// BuildExtendedStats build extended stats for column groups if needed based on the column samples.
func BuildExtendedStats(sctx sessionctx.Context,
	tableID int64, cols []*model.ColumnInfo, collectors []*SampleCollector) (*ExtendedStatsColl, error) {
	const sql = "SELECT name, type, column_ids FROM mysql.stats_extended WHERE table_id = %? and status in (%?, %?)"

	sqlExec := sctx.GetRestrictedSQLExecutor()
	rows, _, err := sqlExec.ExecRestrictedSQL(kv.WithInternalSourceType(context.Background(), kv.InternalTxnStats), nil, sql, tableID, ExtendedStatsAnalyzed, ExtendedStatsInited)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(rows) == 0 {
		return nil, nil
	}
	statsColl := NewExtendedStatsColl()
	for _, row := range rows {
		name := row.GetString(0)
		item := &ExtendedStatsItem{Tp: uint8(row.GetInt64(1))}
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

func fillExtendedStatsItemVals(sctx sessionctx.Context, item *ExtendedStatsItem, cols []*model.ColumnInfo, collectors []*SampleCollector) *ExtendedStatsItem {
	switch item.Tp {
	case ast.StatsTypeCardinality, ast.StatsTypeDependency:
		return nil
	case ast.StatsTypeCorrelation:
		return fillExtStatsCorrVals(sctx, item, cols, collectors)
	}
	return nil
}

func fillExtStatsCorrVals(sctx sessionctx.Context, item *ExtendedStatsItem, cols []*model.ColumnInfo, collectors []*SampleCollector) *ExtendedStatsItem {
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
	samplesY := CopySampleItems(collectors[colOffsets[1]].Samples)
	sampleNum := min(len(samplesX), len(samplesY))
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
	err = sortSampleItems(sc, samplesX)
	if err != nil {
		return nil
	}
	samplesYInXOrder := make([]*SampleItem, 0, sampleNum)
	for i, itemX := range samplesX {
		if itemX.Ordinal >= len(samplesY) {
			continue
		}
		itemY := samplesY[itemX.Ordinal]
		itemY.Ordinal = i
		samplesYInXOrder = append(samplesYInXOrder, itemY)
	}
	samplesYInYOrder := make([]*SampleItem, len(samplesYInXOrder))
	copy(samplesYInYOrder, samplesYInXOrder)
	err = sortSampleItems(sc, samplesYInYOrder)
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
