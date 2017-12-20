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

package executor

import (
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
)

func (e *ShowExec) fetchShowStatsMeta(forChunk bool) error {
	do := domain.GetDomain(e.ctx)
	h := do.StatsHandle()
	dbs := do.InfoSchema().AllSchemas()
	for _, db := range dbs {
		for _, tbl := range db.Tables {
			statsTbl := h.GetTableStats(tbl.ID)
			if !statsTbl.Pseudo {
				if forChunk {
					e.result.AppendString(0, db.Name.O)
					e.result.AppendString(1, tbl.Name.O)
					e.result.AppendTime(2, e.versionToTime(statsTbl.Version))
					e.result.AppendInt64(3, statsTbl.ModifyCount)
					e.result.AppendInt64(4, statsTbl.Count)
				} else {
					row := types.MakeDatums(
						db.Name.O,
						tbl.Name.O,
						e.versionToTime(statsTbl.Version),
						statsTbl.ModifyCount,
						statsTbl.Count,
					)
					e.rows = append(e.rows, row)
				}
			}
		}
	}
	return nil
}

func (e *ShowExec) fetchShowStatsHistogram(forChunk bool) error {
	do := domain.GetDomain(e.ctx)
	h := do.StatsHandle()
	dbs := do.InfoSchema().AllSchemas()
	for _, db := range dbs {
		for _, tbl := range db.Tables {
			statsTbl := h.GetTableStats(tbl.ID)
			if !statsTbl.Pseudo {
				for _, col := range statsTbl.Columns {
					e.histogramToRow(db.Name.O, tbl.Name.O, col.Info.Name.O, 0, col.Histogram, forChunk)
				}
				for _, idx := range statsTbl.Indices {
					e.histogramToRow(db.Name.O, tbl.Name.O, idx.Info.Name.O, 1, idx.Histogram, forChunk)
				}
			}
		}
	}
	return nil
}

func (e *ShowExec) histogramToRow(dbName string, tblName string, colName string, isIndex int, hist statistics.Histogram, forChunk bool) {
	if forChunk {
		e.result.AppendString(0, dbName)
		e.result.AppendString(1, tblName)
		e.result.AppendString(2, colName)
		e.result.AppendInt64(3, int64(isIndex))
		e.result.AppendTime(4, e.versionToTime(hist.LastUpdateVersion))
		e.result.AppendInt64(5, hist.NDV)
		e.result.AppendInt64(6, hist.NullCount)
		return
	}
	e.rows = append(e.rows, types.MakeDatums(
		dbName,
		tblName,
		colName,
		isIndex,
		e.versionToTime(hist.LastUpdateVersion),
		hist.NDV,
		hist.NullCount,
	))
}

func (e *ShowExec) versionToTime(version uint64) types.Time {
	t := time.Unix(0, oracle.ExtractPhysical(version)*int64(time.Millisecond))
	return types.Time{Time: types.FromGoTime(t), Type: mysql.TypeDatetime}
}

func (e *ShowExec) fetchShowStatsBuckets(forChunk bool) error {
	do := domain.GetDomain(e.ctx)
	h := do.StatsHandle()
	dbs := do.InfoSchema().AllSchemas()
	for _, db := range dbs {
		for _, tbl := range db.Tables {
			statsTbl := h.GetTableStats(tbl.ID)
			if !statsTbl.Pseudo {
				for _, col := range statsTbl.Columns {
					err := e.bucketsToRows(db.Name.O, tbl.Name.O, col.Info.Name.O, 0, col.Histogram, forChunk)
					if err != nil {
						return errors.Trace(err)
					}
				}
				for _, idx := range statsTbl.Indices {
					err := e.bucketsToRows(db.Name.O, tbl.Name.O, idx.Info.Name.O, len(idx.Info.Columns), idx.Histogram, forChunk)
					if err != nil {
						return errors.Trace(err)
					}
				}
			}
		}
	}
	return nil
}

// bucketsToRows converts histogram buckets to rows. If the histogram is built from index, then numOfCols equals to number
// of index columns, else numOfCols is 0.
func (e *ShowExec) bucketsToRows(dbName, tblName, colName string, numOfCols int, hist statistics.Histogram, forChunk bool) error {
	isIndex := 0
	if numOfCols > 0 {
		isIndex = 1
	}
	for i, bkt := range hist.Buckets {
		lowerBoundStr, err := e.valueToString(bkt.LowerBound, numOfCols)
		if err != nil {
			return errors.Trace(err)
		}
		upperBoundStr, err := e.valueToString(bkt.UpperBound, numOfCols)
		if err != nil {
			return errors.Trace(err)
		}
		if forChunk {
			e.result.AppendString(0, dbName)
			e.result.AppendString(1, tblName)
			e.result.AppendString(2, colName)
			e.result.AppendInt64(3, int64(isIndex))
			e.result.AppendInt64(4, int64(i))
			e.result.AppendInt64(5, bkt.Count)
			e.result.AppendInt64(6, bkt.Repeats)
			e.result.AppendString(7, lowerBoundStr)
			e.result.AppendString(8, upperBoundStr)
		} else {
			e.rows = append(e.rows, types.MakeDatums(
				dbName,
				tblName,
				colName,
				isIndex,
				i,
				bkt.Count,
				bkt.Repeats,
				lowerBoundStr,
				upperBoundStr,
			))
		}
	}
	return nil
}

// valueToString converts a possible encoded value to a formatted string. If the value is encoded, then
// size equals to number of origin values, else size is 0.
func (e *ShowExec) valueToString(value types.Datum, size int) (string, error) {
	if size == 0 {
		return value.ToString()
	}
	decodedVals, err := codec.Decode(value.GetBytes(), size)
	if err != nil {
		return "", errors.Trace(err)
	}
	str, err := types.DatumsToString(decodedVals)
	if err != nil {
		return "", errors.Trace(err)
	}
	return str, nil
}
