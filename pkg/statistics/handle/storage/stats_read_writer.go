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

package storage

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/statistics/handle/util"
)

// statsReadWriter implements the util.StatsReadWriter interface.
type statsReadWriter struct {
	statsHandler util.StatsHandle
}

// NewStatsReadWriter creates a new StatsReadWriter.
func NewStatsReadWriter(statsHandler util.StatsHandle) util.StatsReadWriter {
	return &statsReadWriter{statsHandler: statsHandler}
}

// StatsMetaCountAndModifyCount reads count and modify_count for the given table from mysql.stats_meta.
func (s *statsReadWriter) StatsMetaCountAndModifyCount(tableID int64) (count, modifyCount int64, err error) {
	err = util.CallWithSCtx(s.statsHandler.SPool(), func(sctx sessionctx.Context) error {
		count, modifyCount, _, err = StatsMetaCountAndModifyCount(sctx, tableID)
		return err
	}, util.FlagWrapTxn)
	return
}

// TableStatsFromStorage loads table stats info from storage.
func (s *statsReadWriter) TableStatsFromStorage(tableInfo *model.TableInfo, physicalID int64, loadAll bool, snapshot uint64) (statsTbl *statistics.Table, err error) {
	err = util.CallWithSCtx(s.statsHandler.SPool(), func(sctx sessionctx.Context) error {
		var ok bool
		statsTbl, ok = s.statsHandler.Get(physicalID)
		if !ok {
			statsTbl = nil
		}
		statsTbl, err = TableStatsFromStorage(sctx, snapshot, tableInfo, physicalID, loadAll, s.statsHandler.Lease(), statsTbl)
		return err
	}, util.FlagWrapTxn)
	return
}

// SaveStatsToStorage saves the stats to storage.
// If count is negative, both count and modify count would not be used and not be written to the table. Unless, corresponding
// fields in the stats_meta table will be updated.
// TODO: refactor to reduce the number of parameters
func (s *statsReadWriter) SaveStatsToStorage(tableID int64, count, modifyCount int64, isIndex int, hg *statistics.Histogram,
	cms *statistics.CMSketch, topN *statistics.TopN, statsVersion int, isAnalyzed int64, updateAnalyzeTime bool, source string) (err error) {
	var statsVer uint64
	err = util.CallWithSCtx(s.statsHandler.SPool(), func(sctx sessionctx.Context) error {
		statsVer, err = SaveStatsToStorage(sctx, tableID,
			count, modifyCount, isIndex, hg, cms, topN, statsVersion, isAnalyzed, updateAnalyzeTime)
		return err
	})
	if err == nil && statsVer != 0 {
		s.statsHandler.RecordHistoricalStatsMeta(tableID, statsVer, source)
	}
	return
}

// SaveMetaToStorage saves stats meta to the storage.
func (s *statsReadWriter) SaveMetaToStorage(tableID, count, modifyCount int64, source string) (err error) {
	var statsVer uint64
	err = util.CallWithSCtx(s.statsHandler.SPool(), func(sctx sessionctx.Context) error {
		statsVer, err = SaveMetaToStorage(sctx, tableID, count, modifyCount)
		return err
	})
	if err == nil && statsVer != 0 {
		s.statsHandler.RecordHistoricalStatsMeta(tableID, statsVer, source)
	}
	return
}

// InsertExtendedStats inserts a record into mysql.stats_extended and update version in mysql.stats_meta.
func (s *statsReadWriter) InsertExtendedStats(statsName string, colIDs []int64, tp int, tableID int64, ifNotExists bool) (err error) {
	var statsVer uint64
	err = util.CallWithSCtx(s.statsHandler.SPool(), func(sctx sessionctx.Context) error {
		statsVer, err = InsertExtendedStats(sctx, s.statsHandler, statsName, colIDs, tp, tableID, ifNotExists)
		return err
	})
	if err == nil && statsVer != 0 {
		s.statsHandler.RecordHistoricalStatsMeta(tableID, statsVer, "extended stats")
	}
	return
}

// MarkExtendedStatsDeleted update the status of mysql.stats_extended to be `deleted` and the version of mysql.stats_meta.
func (s *statsReadWriter) MarkExtendedStatsDeleted(statsName string, tableID int64, ifExists bool) (err error) {
	var statsVer uint64
	err = util.CallWithSCtx(s.statsHandler.SPool(), func(sctx sessionctx.Context) error {
		statsVer, err = MarkExtendedStatsDeleted(sctx, s.statsHandler, statsName, tableID, ifExists)
		return err
	})
	if err == nil && statsVer != 0 {
		s.statsHandler.RecordHistoricalStatsMeta(tableID, statsVer, "extended stats")
	}
	return
}

// SaveExtendedStatsToStorage writes extended stats of a table into mysql.stats_extended.
func (s *statsReadWriter) SaveExtendedStatsToStorage(tableID int64, extStats *statistics.ExtendedStatsColl, isLoad bool) (err error) {
	var statsVer uint64
	err = util.CallWithSCtx(s.statsHandler.SPool(), func(sctx sessionctx.Context) error {
		statsVer, err = SaveExtendedStatsToStorage(sctx, tableID, extStats, isLoad)
		return err
	})
	if err == nil && statsVer != 0 {
		s.statsHandler.RecordHistoricalStatsMeta(tableID, statsVer, "extended stats")
	}
	return
}

// SaveStatsFromJSON saves stats from JSON to the storage.
func (s *statsReadWriter) SaveStatsFromJSON(tableInfo *model.TableInfo, physicalID int64, jsonTblI interface{}) error {
	jsonTbl := jsonTblI.(*JSONTable)
	tbl, err := TableStatsFromJSON(tableInfo, physicalID, jsonTbl)
	if err != nil {
		return errors.Trace(err)
	}

	for _, col := range tbl.Columns {
		// loadStatsFromJSON doesn't support partition table now.
		// The table level count and modify_count would be overridden by the SaveMetaToStorage below, so we don't need
		// to care about them here.
		err = s.SaveStatsToStorage(tbl.PhysicalID, tbl.RealtimeCount, 0, 0, &col.Histogram, col.CMSketch, col.TopN, int(col.GetStatsVer()), 1, false, "load stats")
		if err != nil {
			return errors.Trace(err)
		}
	}
	for _, idx := range tbl.Indices {
		// loadStatsFromJSON doesn't support partition table now.
		// The table level count and modify_count would be overridden by the SaveMetaToStorage below, so we don't need
		// to care about them here.
		err = s.SaveStatsToStorage(tbl.PhysicalID, tbl.RealtimeCount, 0, 1, &idx.Histogram, idx.CMSketch, idx.TopN, int(idx.GetStatsVer()), 1, false, "load stats")
		if err != nil {
			return errors.Trace(err)
		}
	}
	err = s.SaveExtendedStatsToStorage(tbl.PhysicalID, tbl.ExtendedStats, true)
	if err != nil {
		return errors.Trace(err)
	}
	return s.SaveMetaToStorage(tbl.PhysicalID, tbl.RealtimeCount, tbl.ModifyCount, "load stats")
}

// LoadNeededHistograms will load histograms for those needed columns/indices.
func (s *statsReadWriter) LoadNeededHistograms() (err error) {
	err = util.CallWithSCtx(s.statsHandler.SPool(), func(sctx sessionctx.Context) error {
		loadFMSketch := config.GetGlobalConfig().Performance.EnableLoadFMSketch
		return LoadNeededHistograms(sctx, s.statsHandler, loadFMSketch)
	}, util.FlagWrapTxn)
	return err
}

// ReloadExtendedStatistics drops the cache for extended statistics and reload data from mysql.stats_extended.
func (s *statsReadWriter) ReloadExtendedStatistics() error {
	return util.CallWithSCtx(s.statsHandler.SPool(), func(sctx sessionctx.Context) error {
		tables := make([]*statistics.Table, 0, s.statsHandler.Len())
		for _, tbl := range s.statsHandler.Values() {
			t, err := ExtendedStatsFromStorage(sctx, tbl.Copy(), tbl.PhysicalID, true)
			if err != nil {
				return err
			}
			tables = append(tables, t)
		}
		s.statsHandler.UpdateStatsCache(tables, nil)
		return nil
	}, util.FlagWrapTxn)
}
