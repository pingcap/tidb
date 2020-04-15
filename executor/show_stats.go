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

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/v4/domain"
	"github.com/pingcap/tidb/v4/statistics"
	"github.com/pingcap/tidb/v4/store/tikv/oracle"
	"github.com/pingcap/tidb/v4/types"
)

func (e *ShowExec) fetchShowStatsMeta() error {
	do := domain.GetDomain(e.ctx)
	h := do.StatsHandle()
	dbs := do.InfoSchema().AllSchemas()
	for _, db := range dbs {
		for _, tbl := range db.Tables {
			pi := tbl.GetPartitionInfo()
			if pi == nil {
				e.appendTableForStatsMeta(db.Name.O, tbl.Name.O, "", h.GetTableStats(tbl))
			} else {
				for _, def := range pi.Definitions {
					e.appendTableForStatsMeta(db.Name.O, tbl.Name.O, def.Name.O, h.GetPartitionStats(tbl, def.ID))
				}
			}
		}
	}
	return nil
}

func (e *ShowExec) appendTableForStatsMeta(dbName, tblName, partitionName string, statsTbl *statistics.Table) {
	if statsTbl.Pseudo {
		return
	}
	e.appendRow([]interface{}{
		dbName,
		tblName,
		partitionName,
		e.versionToTime(statsTbl.Version),
		statsTbl.ModifyCount,
		statsTbl.Count,
	})
}

func (e *ShowExec) fetchShowStatsHistogram() error {
	do := domain.GetDomain(e.ctx)
	h := do.StatsHandle()
	dbs := do.InfoSchema().AllSchemas()
	for _, db := range dbs {
		for _, tbl := range db.Tables {
			pi := tbl.GetPartitionInfo()
			if pi == nil {
				e.appendTableForStatsHistograms(db.Name.O, tbl.Name.O, "", h.GetTableStats(tbl))
			} else {
				for _, def := range pi.Definitions {
					e.appendTableForStatsHistograms(db.Name.O, tbl.Name.O, def.Name.O, h.GetPartitionStats(tbl, def.ID))
				}
			}
		}
	}
	return nil
}

func (e *ShowExec) appendTableForStatsHistograms(dbName, tblName, partitionName string, statsTbl *statistics.Table) {
	if statsTbl.Pseudo {
		return
	}
	for _, col := range statsTbl.Columns {
		// Pass a nil StatementContext to avoid column stats being marked as needed.
		if col.IsInvalid(nil, false) {
			continue
		}
		e.histogramToRow(dbName, tblName, partitionName, col.Info.Name.O, 0, col.Histogram, col.AvgColSize(statsTbl.Count, false))
	}
	for _, idx := range statsTbl.Indices {
		e.histogramToRow(dbName, tblName, partitionName, idx.Info.Name.O, 1, idx.Histogram, 0)
	}
}

func (e *ShowExec) histogramToRow(dbName, tblName, partitionName, colName string, isIndex int, hist statistics.Histogram, avgColSize float64) {
	e.appendRow([]interface{}{
		dbName,
		tblName,
		partitionName,
		colName,
		isIndex,
		e.versionToTime(hist.LastUpdateVersion),
		hist.NDV,
		hist.NullCount,
		avgColSize,
		hist.Correlation,
	})
}

func (e *ShowExec) versionToTime(version uint64) types.Time {
	t := time.Unix(0, oracle.ExtractPhysical(version)*int64(time.Millisecond))
	return types.NewTime(types.FromGoTime(t), mysql.TypeDatetime, 0)
}

func (e *ShowExec) fetchShowStatsBuckets() error {
	do := domain.GetDomain(e.ctx)
	h := do.StatsHandle()
	dbs := do.InfoSchema().AllSchemas()
	for _, db := range dbs {
		for _, tbl := range db.Tables {
			pi := tbl.GetPartitionInfo()
			if pi == nil {
				if err := e.appendTableForStatsBuckets(db.Name.O, tbl.Name.O, "", h.GetTableStats(tbl)); err != nil {
					return err
				}
			} else {
				for _, def := range pi.Definitions {
					if err := e.appendTableForStatsBuckets(db.Name.O, tbl.Name.O, def.Name.O, h.GetPartitionStats(tbl, def.ID)); err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}

func (e *ShowExec) appendTableForStatsBuckets(dbName, tblName, partitionName string, statsTbl *statistics.Table) error {
	if statsTbl.Pseudo {
		return nil
	}
	for _, col := range statsTbl.Columns {
		err := e.bucketsToRows(dbName, tblName, partitionName, col.Info.Name.O, 0, col.Histogram)
		if err != nil {
			return errors.Trace(err)
		}
	}
	for _, idx := range statsTbl.Indices {
		err := e.bucketsToRows(dbName, tblName, partitionName, idx.Info.Name.O, len(idx.Info.Columns), idx.Histogram)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// bucketsToRows converts histogram buckets to rows. If the histogram is built from index, then numOfCols equals to number
// of index columns, else numOfCols is 0.
func (e *ShowExec) bucketsToRows(dbName, tblName, partitionName, colName string, numOfCols int, hist statistics.Histogram) error {
	isIndex := 0
	if numOfCols > 0 {
		isIndex = 1
	}
	for i := 0; i < hist.Len(); i++ {
		lowerBoundStr, err := statistics.ValueToString(hist.GetLower(i), numOfCols)
		if err != nil {
			return errors.Trace(err)
		}
		upperBoundStr, err := statistics.ValueToString(hist.GetUpper(i), numOfCols)
		if err != nil {
			return errors.Trace(err)
		}
		e.appendRow([]interface{}{
			dbName,
			tblName,
			partitionName,
			colName,
			isIndex,
			i,
			hist.Buckets[i].Count,
			hist.Buckets[i].Repeat,
			lowerBoundStr,
			upperBoundStr,
		})
	}
	return nil
}

func (e *ShowExec) fetchShowStatsHealthy() {
	do := domain.GetDomain(e.ctx)
	h := do.StatsHandle()
	dbs := do.InfoSchema().AllSchemas()
	for _, db := range dbs {
		for _, tbl := range db.Tables {
			pi := tbl.GetPartitionInfo()
			if pi == nil {
				e.appendTableForStatsHealthy(db.Name.O, tbl.Name.O, "", h.GetTableStats(tbl))
			} else {
				for _, def := range pi.Definitions {
					e.appendTableForStatsHealthy(db.Name.O, tbl.Name.O, def.Name.O, h.GetPartitionStats(tbl, def.ID))
				}
			}
		}
	}
}

func (e *ShowExec) appendTableForStatsHealthy(dbName, tblName, partitionName string, statsTbl *statistics.Table) {
	if statsTbl.Pseudo {
		return
	}
	var healthy int64
	if statsTbl.ModifyCount < statsTbl.Count {
		healthy = int64((1.0 - float64(statsTbl.ModifyCount)/float64(statsTbl.Count)) * 100.0)
	} else if statsTbl.ModifyCount == 0 {
		healthy = 100
	}
	e.appendRow([]interface{}{
		dbName,
		tblName,
		partitionName,
		healthy,
	})
}

func (e *ShowExec) fetchShowAnalyzeStatus() {
	rows := dataForAnalyzeStatusHelper(e.baseExecutor.ctx)
	for _, row := range rows {
		for i, val := range row {
			e.result.AppendDatum(i, &val)
		}
	}
}
