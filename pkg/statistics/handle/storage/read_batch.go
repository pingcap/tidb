// Copyright 2024 PingCAP, Inc.
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

package storage

import (
	"fmt"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

type batchReadHistogramsParm struct {
	tp         *types.FieldType
	tableID    int64
	colID      int64
	distinct   int64
	isIndex    int
	ver        uint64
	nullCount  int64
	totColSize int64
	corr       float64
}

func generateBatchReadHistogramsSQLs(params []*batchReadHistogramsParm) string {
	var sql = "select hist_id, count, repeats, lower_bound, upper_bound, ndv from mysql.stats_buckets where"
	for idx, param := range params {
		sql += fmt.Sprintf(" (table_id = %d", param.tableID)
		sql += fmt.Sprintf(" and is_index = %d", param.isIndex)
		sql += fmt.Sprintf(" and hist_id = %d)", param.colID)
		if idx != 0 && idx != len(params)-1 {
			sql += " or"
		}
	}
	sql += " order by hist_id,bucket_id;"
	return sql
}

// HistogramsFromStorage reads histograms from storage.
func HistogramsFromStorage(sctx sessionctx.Context, params []*batchReadHistogramsParm) ([]*statistics.Histogram, error) {
	var mapParam = make(map[int64]*batchReadHistogramsParm, len(params))
	for _, param := range params {
		mapParam[param.colID] = param
	}
	rows, fields, err := util.ExecRows(sctx, generateBatchReadHistogramsSQLs(params))
	if err != nil {
		return nil, errors.Trace(err)
	}
	resultRow := make(map[int64][]chunk.Row, len(params))
	for _, row := range rows {
		tmpColID := row.GetInt64(0)
		resultRow[tmpColID] = append(resultRow[tmpColID], row)
	}
	result := make([]*statistics.Histogram, 0, len(params))
	for colID, rows := range resultRow {
		bucketSize := len(rows)
		p := mapParam[colID]
		hg := statistics.NewHistogram(colID, p.distinct, p.nullCount, p.ver, p.tp, bucketSize, p.totColSize)
		hg.Correlation = p.corr
		totalCount := int64(0)
		for _, row := range rows {
			count := row.GetInt64(0)
			repeats := row.GetInt64(1)
			var upperBound, lowerBound types.Datum
			if p.isIndex == 1 {
				lowerBound = row.GetDatum(2, &fields[2].Column.FieldType)
				upperBound = row.GetDatum(3, &fields[3].Column.FieldType)
			} else {
				// Invalid date values may be inserted into table under some relaxed sql mode. Those values may exist in statistics.
				// Hence, when reading statistics, we should skip invalid date check. See #39336.
				sc := stmtctx.NewStmtCtxWithTimeZone(time.UTC)
				sc.SetTypeFlags(sc.TypeFlags().WithIgnoreInvalidDateErr(true).WithIgnoreZeroInDate(true))
				d := row.GetDatum(2, &fields[2].Column.FieldType)
				// For new collation data, when storing the bounds of the histogram, we store the collate key instead of the
				// original value.
				// But there's additional conversion logic for new collation data, and the collate key might be longer than
				// the FieldType.flen.
				// If we use the original FieldType here, there might be errors like "Invalid utf8mb4 character string"
				// or "Data too long".
				// So we change it to TypeBlob to bypass those logics here.
				if p.tp.EvalType() == types.ETString && p.tp.GetType() != mysql.TypeEnum && p.tp.GetType() != mysql.TypeSet {
					p.tp = types.NewFieldType(mysql.TypeBlob)
				}
				lowerBound, err = d.ConvertTo(sc.TypeCtx(), p.tp)
				if err != nil {
					return nil, errors.Trace(err)
				}
				d = row.GetDatum(3, &fields[3].Column.FieldType)
				upperBound, err = d.ConvertTo(sc.TypeCtx(), p.tp)
				if err != nil {
					return nil, errors.Trace(err)
				}
			}
			totalCount += count
			hg.AppendBucketWithNDV(&lowerBound, &upperBound, totalCount, repeats, row.GetInt64(4))
		}
		hg.PreCalculateScalar()
		result = append(result, hg)
	}
	return result, nil
}

type BatchLoadMeta struct {
	item            *model.TableItemID
	possibleColInfo *model.ColumnInfo
}

// HistMetasFromStorage reads the meta info of the histogram from the storage.
func HistMetasFromStorage(sctx sessionctx.Context, items BatchLoadMeta) (*statistics.Histogram, *types.Datum, int64, int64, error) {
	isIndex := 0
	var tp *types.FieldType
	if item.IsIndex {
		isIndex = 1
		tp = types.NewFieldType(mysql.TypeBlob)
	} else {
		tp = &possibleColInfo.FieldType
	}
	rows, _, err := util.ExecRows(sctx,
		"select distinct_count, version, null_count, tot_col_size, stats_ver, correlation, flag, last_analyze_pos from mysql.stats_histograms where table_id = %? and hist_id = %? and is_index = %?",
		item.TableID,
		item.ID,
		isIndex,
	)
	if err != nil {
		return nil, nil, 0, 0, err
	}
	if len(rows) == 0 {
		return nil, nil, 0, 0, nil
	}
	hist := statistics.NewHistogram(item.ID, rows[0].GetInt64(0), rows[0].GetInt64(2), rows[0].GetUint64(1), tp, chunk.InitialCapacity, rows[0].GetInt64(3))
	hist.Correlation = rows[0].GetFloat64(5)
	lastPos := rows[0].GetDatum(7, types.NewFieldType(mysql.TypeBlob))
	return hist, &lastPos, rows[0].GetInt64(4), rows[0].GetInt64(6), nil
}
