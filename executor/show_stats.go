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
	"fmt"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/sqlexec"
)

func (e *ShowExec) fetchSchemaName(is infoschema.InfoSchema, physicalID, histID, isIndex int64) (dbName, tblName, partName, colOrIdxName string, colTp byte, err error) {
	table, ok := is.TableByID(physicalID)
	if !ok {
		err = errors.New(fmt.Sprintf("can not find corresponding table using physicalID %d", physicalID))
		return
	}
	tblInfo := table.Meta()
	dbInfo, ok := is.SchemaByTable(tblInfo)
	if !ok {
		err = errors.New(fmt.Sprintf("can not find corresponding database for table %s", tblInfo.Name.O))
		return
	}
	if isIndex == 1 {
		for _, idx := range tblInfo.Indices {
			if histID == idx.ID {
				colOrIdxName = idx.Name.O
				break
			}
		}
	} else if isIndex == 0 {
		for _, col := range tblInfo.Columns {
			if histID == col.ID {
				colOrIdxName = col.Name.O
				colTp = col.Tp
				break
			}
		}
	}
	tblName, dbName, partName = tblInfo.Name.O, dbInfo.Name.O, ""
	if partInfo := tblInfo.GetPartitionInfo(); partInfo != nil {
		partName = partInfo.GetNameByID(physicalID)
	}
	return
}

func (e *ShowExec) fetchShowStatsMeta() error {
	do := domain.GetDomain(e.ctx)
	h := do.StatsHandle()

	if snapshot := e.ctx.GetSessionVars().SnapshotTS; snapshot == 0 {
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
	sql := "SELECT version, table_id,  modify_count, count from mysql.stats_meta order by version"
	rows, _, err := e.ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQLWithSnapshot(nil, sql)
	if err != nil {
		return err
	}
	is := GetInfoSchema(e.ctx)
	for _, row := range rows {
		version, physicalID, modifyCount, count := row.GetUint64(0), row.GetInt64(1), row.GetInt64(2), row.GetInt64(3)
		dbName, tblName, partName, _, _, err := e.fetchSchemaName(is, physicalID, -1, -1)
		if err != nil {
			return err
		}
		e.appendRow([]interface{}{
			dbName,
			tblName,
			partName,
			e.versionToTime(version),
			modifyCount,
			count,
		})
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

	if snapshot := e.ctx.GetSessionVars().SnapshotTS; snapshot == 0 {
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
	sql := `SELECT h.table_id, h.is_index, h.hist_id, h.distinct_count, h.version, h.null_count, h.tot_col_size, h.correlation, m.count 
			FROM mysql.stats_histograms as h left join mysql.stats_meta as m 
			on h.table_id = m.table_id
			order by table_id, hist_id`
	rows, _, err := e.ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQLWithSnapshot(nil, sql)
	if err != nil {
		return err
	}
	is := GetInfoSchema(e.ctx)
	for _, row := range rows {
		physicalID := row.GetInt64(0)
		isIndex := row.GetInt64(1)
		histID := row.GetInt64(2)
		distinct := row.GetInt64(3)
		version := row.GetUint64(4)
		nullCount := row.GetInt64(5)
		totalColSize := row.GetInt64(6)
		correlation := row.GetFloat64(7)
		totalRowCnt := row.GetInt64(8)
		dbName, tblName, partName, colOrIdxName, colTp, err := e.fetchSchemaName(is, physicalID, histID, isIndex)
		if err != nil {
			return err
		}
		var avgColSize float64
		if isIndex == 1 {
			totalColSize = 0
		} else {
			avgColSize = statistics.CalculateAvgColSize(colTp, totalColSize, totalRowCnt)
		}

		e.appendRow([]interface{}{
			dbName,
			tblName,
			partName,
			colOrIdxName,
			isIndex,
			e.versionToTime(version),
			distinct,
			nullCount,
			avgColSize,
			correlation,
		})
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
		e.histogramToRow(dbName, tblName, partitionName, col.Info.Name.O, 0, col.Histogram, col.AvgColSize(statsTbl.Count))
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
	return types.Time{Time: types.FromGoTime(t), Type: mysql.TypeDatetime}
}

func (e *ShowExec) fetchShowStatsBuckets() error {
	do := domain.GetDomain(e.ctx)
	h := do.StatsHandle()

	if snapshot := e.ctx.GetSessionVars().SnapshotTS; snapshot == 0 {
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
	sql := "select table_id, is_index, hist_id, bucket_id, count, repeats, upper_bound, lower_bound from mysql.stats_buckets"
	rows, _, err := e.ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQLWithSnapshot(nil, sql)
	if err != nil {
		return err
	}
	is := GetInfoSchema(e.ctx)
	// preBucketsCnt indicates the total count of values in the buckets whose `bucket_id` < `cur_bucket_id`.
	preBucketsCnt := int64(0)
	for _, row := range rows {
		physicalID, isIndex, histID, bucketID, count, repeats, upperBound, lowerBound := row.GetInt64(0), row.GetInt64(1), row.GetInt64(2), row.GetInt64(3), row.GetInt64(4), row.GetInt64(5), row.GetString(6), row.GetString(7)
		dbName, tblName, partName, colOrIdxName, _, err := e.fetchSchemaName(is, physicalID, histID, isIndex)
		if err != nil {
			return err
		}
		e.appendRow([]interface{}{
			dbName,
			tblName,
			partName,
			colOrIdxName,
			isIndex,
			bucketID,
			count + preBucketsCnt,
			repeats,
			lowerBound,
			upperBound,
		})
		preBucketsCnt += count
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
