// Copyright 2017 PingCAP, Inc.
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

package plan

import (
	"math"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/plan/statistics"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/types"
)

func getRowCountByIndexRanges(sc *variable.StatementContext, statsTbl *statistics.Table, indexRanges []*IndexRange, indexInfo *model.IndexInfo, tableInfo *model.TableInfo) (uint64, error) {
	var offset int
	for i := range tableInfo.Indices {
		if tableInfo.Indices[i].Name.L == indexInfo.Name.L {
			offset = i
			break
		}
	}
	if len(statsTbl.Indices[offset].Numbers) == 0 {
		return getPseudoRowCountByIndexRanges(sc, statsTbl, indexRanges, indexInfo)
	}
	return getRealRowCountByIndexRanges(sc, statsTbl, indexRanges, indexInfo, offset)
}

func getRowCountByRange(sc *variable.StatementContext, statsTblCount int64, statsCol *statistics.Column, l, r types.Datum) (int64, error) {
	var rowCount int64
	var err error
	if l.Kind() == types.KindNull && r.Kind() == types.KindMaxValue {
		return statsTblCount, nil
	} else if l.Kind() == types.KindMinNotNull {
		var nullCount int64
		nullCount, err = statsCol.EqualRowCount(sc, types.Datum{})
		if r.Kind() == types.KindMaxValue {
			rowCount = statsTblCount - nullCount
		} else if err == nil {
			lessCount, err1 := statsCol.LessRowCount(sc, r)
			rowCount = lessCount - nullCount
			err = err1
		}
	} else if r.Kind() == types.KindMaxValue {
		rowCount, err = statsCol.GreaterRowCount(sc, l)
	} else {
		compare, err1 := l.CompareDatum(sc, r)
		if err1 != nil {
			return 0, errors.Trace(err1)
		}
		if compare == 0 {
			rowCount, err = statsCol.EqualRowCount(sc, l)
		} else {
			rowCount, err = statsCol.BetweenRowCount(sc, l, r)
		}
	}
	if err != nil {
		return 0, errors.Trace(err)
	}
	return rowCount, nil
}

func getRealRowCountByIndexRanges(sc *variable.StatementContext, statsTbl *statistics.Table, indexRanges []*IndexRange, indexInfo *model.IndexInfo, offset int) (uint64, error) {
	totalCount := int64(0)
	for _, indexRange := range indexRanges {
		lv := indexRange.LowVal
		rv := indexRange.HighVal
		for i := len(lv); i < len(indexInfo.Columns); i++ {
			lv = append(lv, types.MinNotNullDatum())
		}
		for i := len(rv); i < len(indexInfo.Columns); i++ {
			rv = append(rv, types.MaxValueDatum())
		}
		lb, err := codec.EncodeKey(nil, lv...)
		if err != nil {
			return 0, errors.Trace(err)
		}
		rb, err := codec.EncodeKey(nil, rv...)
		if err != nil {
			return 0, errors.Trace(err)
		}
		l := types.NewBytesDatum(lb)
		r := types.NewBytesDatum(rb)
		rowCount, err := getRowCountByRange(sc, statsTbl.Count, statsTbl.Indices[offset], l, r)
		if err != nil {
			return 0, errors.Trace(err)
		}
		totalCount += rowCount
	}
	if totalCount > statsTbl.Count {
		totalCount = statsTbl.Count
	}
	return uint64(totalCount), nil
}

func getPseudoRowCountByIndexRanges(sc *variable.StatementContext, statsTbl *statistics.Table, indexRanges []*IndexRange, indexInfo *model.IndexInfo) (uint64, error) {
	totalCount := float64(0)
	for _, indexRange := range indexRanges {
		count := float64(statsTbl.Count)
		i := len(indexRange.LowVal) - 1
		l := indexRange.LowVal[i]
		r := indexRange.HighVal[i]
		offset := indexInfo.Columns[i].Offset
		rowCount, err := getRowCountByRange(sc, statsTbl.Count, statsTbl.Columns[offset], l, r)
		if err != nil {
			return 0, errors.Trace(err)
		}
		count = count / float64(statsTbl.Count) * float64(rowCount)
		// If the condition is a = 1, b = 1, c = 1, d = 1, we think every a=1, b=1, c=1 only filtrate 1/100 data,
		// so as to avoid collapsing too fast.
		for j := 0; j < i; j++ {
			count = count / float64(100)
		}
		totalCount += count
	}
	// To avoid the totalCount become too small.
	if uint64(totalCount) < 1000 {
		// We will not let the row count less than 1000 to avoid collapsing too fast in the future calculation.
		totalCount = 1000.0
	}
	if totalCount > float64(statsTbl.Count) {
		totalCount = float64(statsTbl.Count) / 3.0
	}
	return uint64(totalCount), nil
}

func getRowCountByTableRange(sc *variable.StatementContext, statsTbl *statistics.Table, ranges []TableRange, offset int) (uint64, error) {
	var rowCount uint64
	for _, rg := range ranges {
		var cnt int64
		var err error
		if rg.LowVal == math.MinInt64 && rg.HighVal == math.MaxInt64 {
			cnt = statsTbl.Count
		} else if rg.LowVal == math.MinInt64 {
			cnt, err = statsTbl.Columns[offset].LessRowCount(sc, types.NewDatum(rg.HighVal))
		} else if rg.HighVal == math.MaxInt64 {
			cnt, err = statsTbl.Columns[offset].GreaterRowCount(sc, types.NewDatum(rg.LowVal))
		} else {
			if rg.LowVal == rg.HighVal {
				cnt, err = statsTbl.Columns[offset].EqualRowCount(sc, types.NewDatum(rg.LowVal))
			} else {
				cnt, err = statsTbl.Columns[offset].BetweenRowCount(sc, types.NewDatum(rg.LowVal), types.NewDatum(rg.HighVal))
			}
		}
		if err != nil {
			return 0, errors.Trace(err)
		}
		if rg.HighVal-rg.LowVal > 0 && cnt > rg.HighVal-rg.LowVal {
			cnt = rg.HighVal - rg.LowVal
		}
		rowCount += uint64(cnt)
	}
	if rowCount > uint64(statsTbl.Count) {
		rowCount = uint64(statsTbl.Count)
	}
	return rowCount, nil
}
