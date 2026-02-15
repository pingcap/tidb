// Copyright 2026 PingCAP, Inc.
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

package globalstats

import (
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/ranger"
)

// BuildGlobalStatsFromSamples builds global-level statistics from a merged
// ReservoirRowSampleCollector. This avoids the slow and lossy partition-level
// stats merge by building TopN and Histograms directly from the merged samples,
// using the same BuildHistAndTopN code path used for per-partition stats.
func BuildGlobalStatsFromSamples(
	ctx sessionctx.Context,
	collector *statistics.ReservoirRowSampleCollector,
	tableInfo *model.TableInfo,
	opts map[ast.AnalyzeOptionType]uint64,
	colsInfo []*model.ColumnInfo,
	indexes []*model.IndexInfo,
	histIDs []int64,
	isIndex bool,
) (*GlobalStats, error) {
	if isIndex {
		return buildGlobalIndexStatsFromSamples(ctx, collector, tableInfo, opts, colsInfo, indexes, histIDs)
	}
	return buildGlobalColumnStatsFromSamples(ctx, collector, tableInfo, opts, colsInfo, indexes, histIDs)
}

func buildGlobalColumnStatsFromSamples(
	ctx sessionctx.Context,
	collector *statistics.ReservoirRowSampleCollector,
	tableInfo *model.TableInfo,
	opts map[ast.AnalyzeOptionType]uint64,
	colsInfo []*model.ColumnInfo,
	indexes []*model.IndexInfo,
	histIDs []int64,
) (*GlobalStats, error) {
	histIDSet := make(map[int64]bool, len(histIDs))
	for _, id := range histIDs {
		histIDSet[id] = true
	}

	numBuckets := int(opts[ast.AnalyzeOptNumBuckets])
	numTopN := int(opts[ast.AnalyzeOptNumTopN])

	globalStats := newGlobalStats(len(histIDs))
	globalStats.Count = collector.Base().Count

	histIdx := 0
	for i, col := range colsInfo {
		if !histIDSet[col.ID] {
			continue
		}
		if col.IsGenerated() && !col.GeneratedStored {
			histIdx++
			continue
		}
		sc := buildColumnSampleCollector(collector, i, col)
		hist, topn, err := statistics.BuildHistAndTopN(ctx, numBuckets, numTopN, col.ID, sc, &col.FieldType, true, nil, false)
		if err != nil {
			return nil, err
		}
		globalStats.Hg[histIdx] = hist
		globalStats.TopN[histIdx] = topn
		globalStats.Fms[histIdx] = collector.Base().FMSketches[i]
		histIdx++
	}

	return globalStats, nil
}

func buildGlobalIndexStatsFromSamples(
	ctx sessionctx.Context,
	collector *statistics.ReservoirRowSampleCollector,
	tableInfo *model.TableInfo,
	opts map[ast.AnalyzeOptionType]uint64,
	colsInfo []*model.ColumnInfo,
	indexes []*model.IndexInfo,
	histIDs []int64,
) (*GlobalStats, error) {
	numBuckets := int(opts[ast.AnalyzeOptNumBuckets])
	numTopN := int(opts[ast.AnalyzeOptNumTopN])
	colLen := len(colsInfo)

	globalStats := newGlobalStats(len(histIDs))
	globalStats.Count = collector.Base().Count

	for histIdx, targetID := range histIDs {
		idxOffset := -1
		for j, idx := range indexes {
			if idx.ID == targetID {
				idxOffset = j
				break
			}
		}
		if idxOffset < 0 {
			continue
		}
		idx := indexes[idxOffset]
		fmSketchIdx := colLen + idxOffset
		sc := buildIndexSampleCollector(collector, colsInfo, idx, fmSketchIdx, ctx)
		hist, topn, err := statistics.BuildHistAndTopN(ctx, numBuckets, numTopN, idx.ID, sc, types.NewFieldType(mysql.TypeBlob), false, nil, false)
		if err != nil {
			return nil, err
		}
		globalStats.Hg[histIdx] = hist
		globalStats.TopN[histIdx] = topn
		globalStats.Fms[histIdx] = collector.Base().FMSketches[fmSketchIdx]
	}

	return globalStats, nil
}

// buildColumnSampleCollector extracts a per-column SampleCollector from the
// multi-column ReservoirRowSampleCollector.
func buildColumnSampleCollector(
	collector *statistics.ReservoirRowSampleCollector,
	colOffset int,
	col *model.ColumnInfo,
) *statistics.SampleCollector {
	sampleNum := collector.Base().Samples.Len()
	sampleItems := make([]*statistics.SampleItem, 0, sampleNum)

	var collator collate.Collator
	ft := col.FieldType
	if ft.EvalType() == types.ETString && ft.GetType() != mysql.TypeEnum && ft.GetType() != mysql.TypeSet {
		collator = collate.GetCollator(ft.GetCollate())
	}

	for j, row := range collector.Base().Samples {
		if row.Columns[colOffset].IsNull() {
			continue
		}
		val := row.Columns[colOffset]
		if len(val.GetBytes()) > statistics.MaxSampleValueLength {
			continue
		}
		if collator != nil {
			val.SetBytes(collator.Key(val.GetString()))
		}
		sampleItems = append(sampleItems, &statistics.SampleItem{
			Value:   &val,
			Ordinal: j,
		})
	}

	return &statistics.SampleCollector{
		Samples:   sampleItems,
		NullCount: collector.Base().NullCount[colOffset],
		Count:     collector.Base().Count - collector.Base().NullCount[colOffset],
		FMSketch:  collector.Base().FMSketches[colOffset],
		TotalSize: collector.Base().TotalSizes[colOffset],
	}
}

// buildIndexSampleCollector extracts a per-index SampleCollector from the
// multi-column ReservoirRowSampleCollector by encoding index key columns.
func buildIndexSampleCollector(
	collector *statistics.ReservoirRowSampleCollector,
	colsInfo []*model.ColumnInfo,
	idx *model.IndexInfo,
	fmSketchIdx int,
	ctx sessionctx.Context,
) *statistics.SampleCollector {
	sampleNum := collector.Base().Samples.Len()
	sampleItems := make([]*statistics.SampleItem, 0, sampleNum)

	for _, row := range collector.Base().Samples {
		if len(idx.Columns) == 1 && row.Columns[idx.Columns[0].Offset].IsNull() {
			continue
		}
		b := make([]byte, 0, 8)
		skip := false
		for _, col := range idx.Columns {
			if len(row.Columns[col.Offset].GetBytes()) > statistics.MaxSampleValueLength {
				skip = true
				break
			}
			var err error
			if col.Length != types.UnspecifiedLength {
				var tmpDatum types.Datum
				row.Columns[col.Offset].Copy(&tmpDatum)
				ranger.CutDatumByPrefixLen(&tmpDatum, col.Length, &colsInfo[col.Offset].FieldType)
				b, err = codec.EncodeKey(ctx.GetSessionVars().StmtCtx.TimeZone(), b, tmpDatum)
			} else {
				b, err = codec.EncodeKey(ctx.GetSessionVars().StmtCtx.TimeZone(), b, row.Columns[col.Offset])
			}
			if err != nil {
				skip = true
				break
			}
		}
		if skip {
			continue
		}
		tmp := types.NewBytesDatum(b)
		sampleItems = append(sampleItems, &statistics.SampleItem{
			Value: &tmp,
		})
	}

	return &statistics.SampleCollector{
		Samples:   sampleItems,
		NullCount: collector.Base().NullCount[fmSketchIdx],
		Count:     collector.Base().Count - collector.Base().NullCount[fmSketchIdx],
		FMSketch:  collector.Base().FMSketches[fmSketchIdx],
		TotalSize: collector.Base().TotalSizes[fmSketchIdx],
	}
}
