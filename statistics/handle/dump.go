// Copyright 2018 PingCAP, Inc.
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

package handle

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
)

// JSONTable is used for dumping statistics.
type JSONTable struct {
	IsHistoricalStats bool                   `json:"is_historical_stats"`
	DatabaseName      string                 `json:"database_name"`
	TableName         string                 `json:"table_name"`
	Columns           map[string]*jsonColumn `json:"columns"`
	Indices           map[string]*jsonColumn `json:"indices"`
	ExtStats          []*jsonExtendedStats   `json:"ext_stats"`
	Count             int64                  `json:"count"`
	ModifyCount       int64                  `json:"modify_count"`
	Partitions        map[string]*JSONTable  `json:"partitions"`
	Version           uint64                 `json:"version"`
}

type jsonExtendedStats struct {
	StatsName  string  `json:"stats_name"`
	ColIDs     []int64 `json:"cols"`
	Tp         uint8   `json:"type"`
	ScalarVals float64 `json:"scalar_vals"`
	StringVals string  `json:"string_vals"`
}

func dumpJSONExtendedStats(statsColl *statistics.ExtendedStatsColl) []*jsonExtendedStats {
	if statsColl == nil || len(statsColl.Stats) == 0 {
		return nil
	}
	stats := make([]*jsonExtendedStats, 0, len(statsColl.Stats))
	for name, item := range statsColl.Stats {
		js := &jsonExtendedStats{
			StatsName:  name,
			ColIDs:     item.ColIDs,
			Tp:         item.Tp,
			ScalarVals: item.ScalarVals,
			StringVals: item.StringVals,
		}
		stats = append(stats, js)
	}
	return stats
}

func extendedStatsFromJSON(statsColl []*jsonExtendedStats) *statistics.ExtendedStatsColl {
	if len(statsColl) == 0 {
		return nil
	}
	stats := statistics.NewExtendedStatsColl()
	for _, js := range statsColl {
		item := &statistics.ExtendedStatsItem{
			ColIDs:     js.ColIDs,
			Tp:         js.Tp,
			ScalarVals: js.ScalarVals,
			StringVals: js.StringVals,
		}
		stats.Stats[js.StatsName] = item
	}
	return stats
}

type jsonColumn struct {
	Histogram         *tipb.Histogram `json:"histogram"`
	CMSketch          *tipb.CMSketch  `json:"cm_sketch"`
	FMSketch          *tipb.FMSketch  `json:"fm_sketch"`
	NullCount         int64           `json:"null_count"`
	TotColSize        int64           `json:"tot_col_size"`
	LastUpdateVersion uint64          `json:"last_update_version"`
	Correlation       float64         `json:"correlation"`
	// StatsVer is a pointer here since the old version json file would not contain version information.
	StatsVer *int64 `json:"stats_ver"`
}

func dumpJSONCol(hist *statistics.Histogram, CMSketch *statistics.CMSketch, topn *statistics.TopN, FMSketch *statistics.FMSketch, statsVer *int64) *jsonColumn {
	jsonCol := &jsonColumn{
		Histogram:         statistics.HistogramToProto(hist),
		NullCount:         hist.NullCount,
		TotColSize:        hist.TotColSize,
		LastUpdateVersion: hist.LastUpdateVersion,
		Correlation:       hist.Correlation,
		StatsVer:          statsVer,
	}
	if CMSketch != nil || topn != nil {
		jsonCol.CMSketch = statistics.CMSketchToProto(CMSketch, topn)
	}
	if FMSketch != nil {
		jsonCol.FMSketch = statistics.FMSketchToProto(FMSketch)
	}
	return jsonCol
}

// DumpStatsToJSON dumps statistic to json.
func (h *Handle) DumpStatsToJSON(dbName string, tableInfo *model.TableInfo,
	historyStatsExec sqlexec.RestrictedSQLExecutor, dumpPartitionStats bool) (*JSONTable, error) {
	var snapshot uint64
	if historyStatsExec != nil {
		sctx := historyStatsExec.(sessionctx.Context)
		snapshot = sctx.GetSessionVars().SnapshotTS
	}
	return h.DumpStatsToJSONBySnapshot(dbName, tableInfo, snapshot, dumpPartitionStats)
}

var (
	dumpHistoricalStatsSuccessCounter = metrics.HistoricalStatsCounter.WithLabelValues("dump", "success")
	dumpHistoricalStatsFailedCounter  = metrics.HistoricalStatsCounter.WithLabelValues("dump", "fail")
)

// DumpHistoricalStatsBySnapshot dumped json tables from mysql.stats_meta_history and mysql.stats_history
func (h *Handle) DumpHistoricalStatsBySnapshot(dbName string, tableInfo *model.TableInfo, snapshot uint64) (jt *JSONTable, err error) {
	historicalStatsEnabled, err := h.CheckHistoricalStatsEnable()
	if err != nil {
		return nil, errors.Errorf("check %v failed: %v", variable.TiDBEnableHistoricalStats, err)
	}
	if !historicalStatsEnabled {
		return nil, errors.Errorf("%v should be enabled", variable.TiDBEnableHistoricalStats)
	}

	defer func() {
		if err == nil {
			dumpHistoricalStatsSuccessCounter.Inc()
		} else {
			dumpHistoricalStatsFailedCounter.Inc()
		}
	}()
	pi := tableInfo.GetPartitionInfo()
	if pi == nil {
		return h.getTableHistoricalStatsToJSONWithFallback(dbName, tableInfo, tableInfo.ID, snapshot)
	}
	jsonTbl := &JSONTable{
		DatabaseName: dbName,
		TableName:    tableInfo.Name.L,
		Partitions:   make(map[string]*JSONTable, len(pi.Definitions)),
	}
	for _, def := range pi.Definitions {
		tbl, err := h.getTableHistoricalStatsToJSONWithFallback(dbName, tableInfo, def.ID, snapshot)
		if err != nil {
			return nil, errors.Trace(err)
		}
		jsonTbl.Partitions[def.Name.L] = tbl
	}
	tbl, err := h.getTableHistoricalStatsToJSONWithFallback(dbName, tableInfo, tableInfo.ID, snapshot)
	if err != nil {
		return nil, err
	}
	// dump its global-stats if existed
	if tbl != nil {
		jsonTbl.Partitions["global"] = tbl
	}
	return jsonTbl, nil
}

// DumpStatsToJSONBySnapshot dumps statistic to json.
func (h *Handle) DumpStatsToJSONBySnapshot(dbName string, tableInfo *model.TableInfo, snapshot uint64, dumpPartitionStats bool) (*JSONTable, error) {
	h.mu.Lock()
	isDynamicMode := variable.PartitionPruneMode(h.mu.ctx.GetSessionVars().PartitionPruneMode.Load()) == variable.Dynamic
	h.mu.Unlock()
	pi := tableInfo.GetPartitionInfo()
	if pi == nil {
		return h.tableStatsToJSON(dbName, tableInfo, tableInfo.ID, snapshot)
	}
	jsonTbl := &JSONTable{
		DatabaseName: dbName,
		TableName:    tableInfo.Name.L,
		Partitions:   make(map[string]*JSONTable, len(pi.Definitions)),
	}
	// dump partition stats only if in static mode or enable dumpPartitionStats flag in dynamic mode
	if !isDynamicMode || dumpPartitionStats {
		for _, def := range pi.Definitions {
			tbl, err := h.tableStatsToJSON(dbName, tableInfo, def.ID, snapshot)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if tbl == nil {
				continue
			}
			jsonTbl.Partitions[def.Name.L] = tbl
		}
	}
	// dump its global-stats if existed
	tbl, err := h.tableStatsToJSON(dbName, tableInfo, tableInfo.ID, snapshot)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if tbl != nil {
		jsonTbl.Partitions["global"] = tbl
	}
	return jsonTbl, nil
}

// GenJSONTableFromStats generate jsonTable from tableInfo and stats
func GenJSONTableFromStats(dbName string, tableInfo *model.TableInfo, tbl *statistics.Table) (*JSONTable, error) {
	jsonTbl := &JSONTable{
		DatabaseName: dbName,
		TableName:    tableInfo.Name.L,
		Columns:      make(map[string]*jsonColumn, len(tbl.Columns)),
		Indices:      make(map[string]*jsonColumn, len(tbl.Indices)),
		Count:        tbl.Count,
		ModifyCount:  tbl.ModifyCount,
		Version:      tbl.Version,
	}
	for _, col := range tbl.Columns {
		sc := &stmtctx.StatementContext{TimeZone: time.UTC}
		hist, err := col.ConvertTo(sc, types.NewFieldType(mysql.TypeBlob))
		if err != nil {
			return nil, errors.Trace(err)
		}
		jsonTbl.Columns[col.Info.Name.L] = dumpJSONCol(hist, col.CMSketch, col.TopN, col.FMSketch, &col.StatsVer)
	}

	for _, idx := range tbl.Indices {
		jsonTbl.Indices[idx.Info.Name.L] = dumpJSONCol(&idx.Histogram, idx.CMSketch, idx.TopN, nil, &idx.StatsVer)
	}
	jsonTbl.ExtStats = dumpJSONExtendedStats(tbl.ExtendedStats)
	return jsonTbl, nil
}

// getTableHistoricalStatsToJSONWithFallback try to get table historical stats, if not exit, directly fallback to the latest stats
func (h *Handle) getTableHistoricalStatsToJSONWithFallback(dbName string, tableInfo *model.TableInfo, physicalID int64, snapshot uint64) (*JSONTable, error) {
	jt, exist, err := h.tableHistoricalStatsToJSON(physicalID, snapshot)
	if err != nil {
		return nil, err
	}
	if !exist {
		return h.tableStatsToJSON(dbName, tableInfo, physicalID, 0)
	}
	return jt, nil
}

func (h *Handle) tableHistoricalStatsToJSON(physicalID int64, snapshot uint64) (*JSONTable, bool, error) {
	reader, err := h.getGlobalStatsReader(0)
	if err != nil {
		return nil, false, err
	}
	defer func() {
		err1 := h.releaseGlobalStatsReader(reader)
		if err == nil && err1 != nil {
			err = err1
		}
	}()

	// get meta version
	rows, _, err := reader.Read("select distinct version from mysql.stats_meta_history where table_id = %? and version <= %? order by version desc limit 1", physicalID, snapshot)
	if err != nil {
		return nil, false, errors.AddStack(err)
	}
	if len(rows) < 1 {
		logutil.BgLogger().Warn("failed to get records of stats_meta_history",
			zap.Int64("table-id", physicalID),
			zap.Uint64("snapshotTS", snapshot))
		return nil, false, nil
	}
	statsMetaVersion := rows[0].GetInt64(0)
	// get stats meta
	rows, _, err = reader.Read("select modify_count, count from mysql.stats_meta_history where table_id = %? and version = %?", physicalID, statsMetaVersion)
	if err != nil {
		return nil, false, errors.AddStack(err)
	}
	modifyCount, count := rows[0].GetInt64(0), rows[0].GetInt64(1)

	// get stats version
	rows, _, err = reader.Read("select distinct version from mysql.stats_history where table_id = %? and version <= %? order by version desc limit 1", physicalID, snapshot)
	if err != nil {
		return nil, false, errors.AddStack(err)
	}
	if len(rows) < 1 {
		logutil.BgLogger().Warn("failed to get record of stats_history",
			zap.Int64("table-id", physicalID),
			zap.Uint64("snapshotTS", snapshot))
		return nil, false, nil
	}
	statsVersion := rows[0].GetInt64(0)

	// get stats
	rows, _, err = reader.Read("select stats_data from mysql.stats_history where table_id = %? and version = %? order by seq_no", physicalID, statsVersion)
	if err != nil {
		return nil, false, errors.AddStack(err)
	}
	blocks := make([][]byte, 0)
	for _, row := range rows {
		blocks = append(blocks, row.GetBytes(0))
	}
	jsonTbl, err := BlocksToJSONTable(blocks)
	if err != nil {
		return nil, false, errors.AddStack(err)
	}
	jsonTbl.Count = count
	jsonTbl.ModifyCount = modifyCount
	jsonTbl.IsHistoricalStats = true
	return jsonTbl, true, nil
}

func (h *Handle) tableStatsToJSON(dbName string, tableInfo *model.TableInfo, physicalID int64, snapshot uint64) (*JSONTable, error) {
	tbl, err := h.TableStatsFromStorage(tableInfo, physicalID, true, snapshot)
	if err != nil || tbl == nil {
		return nil, err
	}
	tbl.Version, tbl.ModifyCount, tbl.Count, err = h.statsMetaByTableIDFromStorage(physicalID, snapshot)
	if err != nil {
		return nil, err
	}
	jsonTbl, err := GenJSONTableFromStats(dbName, tableInfo, tbl)
	if err != nil {
		return nil, err
	}
	return jsonTbl, nil
}

// LoadStatsFromJSON will load statistic from JSONTable, and save it to the storage.
func (h *Handle) LoadStatsFromJSON(is infoschema.InfoSchema, jsonTbl *JSONTable) error {
	table, err := is.TableByName(model.NewCIStr(jsonTbl.DatabaseName), model.NewCIStr(jsonTbl.TableName))
	if err != nil {
		return errors.Trace(err)
	}
	tableInfo := table.Meta()
	pi := tableInfo.GetPartitionInfo()
	if pi == nil || jsonTbl.Partitions == nil {
		err := h.loadStatsFromJSON(tableInfo, tableInfo.ID, jsonTbl)
		if err != nil {
			return errors.Trace(err)
		}
	} else {
		for _, def := range pi.Definitions {
			tbl := jsonTbl.Partitions[def.Name.L]
			if tbl == nil {
				continue
			}
			err := h.loadStatsFromJSON(tableInfo, def.ID, tbl)
			if err != nil {
				return errors.Trace(err)
			}
		}
		// load global-stats if existed
		if globalStats, ok := jsonTbl.Partitions["global"]; ok {
			if err := h.loadStatsFromJSON(tableInfo, tableInfo.ID, globalStats); err != nil {
				return errors.Trace(err)
			}
		}
	}
	return errors.Trace(h.Update(is))
}

func (h *Handle) loadStatsFromJSON(tableInfo *model.TableInfo, physicalID int64, jsonTbl *JSONTable) error {
	tbl, err := TableStatsFromJSON(tableInfo, physicalID, jsonTbl)
	if err != nil {
		return errors.Trace(err)
	}

	for _, col := range tbl.Columns {
		// loadStatsFromJSON doesn't support partition table now.
		// The table level Count and Modify_count would be overridden by the SaveMetaToStorage below, so we don't need
		// to care about them here.
		err = h.SaveStatsToStorage(tbl.PhysicalID, tbl.Count, 0, 0, &col.Histogram, col.CMSketch, col.TopN, int(col.StatsVer), 1, false, StatsMetaHistorySourceLoadStats)
		if err != nil {
			return errors.Trace(err)
		}
	}
	for _, idx := range tbl.Indices {
		// loadStatsFromJSON doesn't support partition table now.
		// The table level Count and Modify_count would be overridden by the SaveMetaToStorage below, so we don't need
		// to care about them here.
		err = h.SaveStatsToStorage(tbl.PhysicalID, tbl.Count, 0, 1, &idx.Histogram, idx.CMSketch, idx.TopN, int(idx.StatsVer), 1, false, StatsMetaHistorySourceLoadStats)
		if err != nil {
			return errors.Trace(err)
		}
	}
	err = h.SaveExtendedStatsToStorage(tbl.PhysicalID, tbl.ExtendedStats, true)
	if err != nil {
		return errors.Trace(err)
	}
	return h.SaveMetaToStorage(tbl.PhysicalID, tbl.Count, tbl.ModifyCount, StatsMetaHistorySourceLoadStats)
}

// TableStatsFromJSON loads statistic from JSONTable and return the Table of statistic.
func TableStatsFromJSON(tableInfo *model.TableInfo, physicalID int64, jsonTbl *JSONTable) (*statistics.Table, error) {
	newHistColl := statistics.HistColl{
		PhysicalID:     physicalID,
		HavePhysicalID: true,
		Count:          jsonTbl.Count,
		ModifyCount:    jsonTbl.ModifyCount,
		Columns:        make(map[int64]*statistics.Column, len(jsonTbl.Columns)),
		Indices:        make(map[int64]*statistics.Index, len(jsonTbl.Indices)),
	}
	tbl := &statistics.Table{
		HistColl: newHistColl,
	}
	for id, jsonIdx := range jsonTbl.Indices {
		for _, idxInfo := range tableInfo.Indices {
			if idxInfo.Name.L != id {
				continue
			}
			hist := statistics.HistogramFromProto(jsonIdx.Histogram)
			hist.ID, hist.NullCount, hist.LastUpdateVersion, hist.Correlation = idxInfo.ID, jsonIdx.NullCount, jsonIdx.LastUpdateVersion, jsonIdx.Correlation
			cm, topN := statistics.CMSketchAndTopNFromProto(jsonIdx.CMSketch)
			// If the statistics is loaded from a JSON without stats version,
			// we set it to 1.
			statsVer := int64(statistics.Version1)
			if jsonIdx.StatsVer != nil {
				statsVer = *jsonIdx.StatsVer
			}
			idx := &statistics.Index{
				Histogram:         *hist,
				CMSketch:          cm,
				TopN:              topN,
				Info:              idxInfo,
				StatsVer:          statsVer,
				PhysicalID:        physicalID,
				StatsLoadedStatus: statistics.NewStatsFullLoadStatus(),
			}
			tbl.Indices[idx.ID] = idx
		}
	}

	for id, jsonCol := range jsonTbl.Columns {
		for _, colInfo := range tableInfo.Columns {
			if colInfo.Name.L != id {
				continue
			}
			hist := statistics.HistogramFromProto(jsonCol.Histogram)
			sc := &stmtctx.StatementContext{TimeZone: time.UTC}
			tmpFT := colInfo.FieldType
			// For new collation data, when storing the bounds of the histogram, we store the collate key instead of the
			// original value.
			// But there's additional conversion logic for new collation data, and the collate key might be longer than
			// the FieldType.flen.
			// If we use the original FieldType here, there might be errors like "Invalid utf8mb4 character string"
			// or "Data too long".
			// So we change it to TypeBlob to bypass those logics here.
			if colInfo.FieldType.EvalType() == types.ETString && colInfo.FieldType.GetType() != mysql.TypeEnum && colInfo.FieldType.GetType() != mysql.TypeSet {
				tmpFT = *types.NewFieldType(mysql.TypeBlob)
			}
			hist, err := hist.ConvertTo(sc, &tmpFT)
			if err != nil {
				return nil, errors.Trace(err)
			}
			cm, topN := statistics.CMSketchAndTopNFromProto(jsonCol.CMSketch)
			fms := statistics.FMSketchFromProto(jsonCol.FMSketch)
			hist.ID, hist.NullCount, hist.LastUpdateVersion, hist.TotColSize, hist.Correlation = colInfo.ID, jsonCol.NullCount, jsonCol.LastUpdateVersion, jsonCol.TotColSize, jsonCol.Correlation
			// If the statistics is loaded from a JSON without stats version,
			// we set it to 1.
			statsVer := int64(statistics.Version1)
			if jsonCol.StatsVer != nil {
				statsVer = *jsonCol.StatsVer
			}
			col := &statistics.Column{
				PhysicalID:        physicalID,
				Histogram:         *hist,
				CMSketch:          cm,
				TopN:              topN,
				FMSketch:          fms,
				Info:              colInfo,
				IsHandle:          tableInfo.PKIsHandle && mysql.HasPriKeyFlag(colInfo.GetFlag()),
				StatsVer:          statsVer,
				StatsLoadedStatus: statistics.NewStatsFullLoadStatus(),
			}
			col.Count = int64(col.TotalRowCount())
			tbl.Columns[col.ID] = col
		}
	}
	tbl.ExtendedStats = extendedStatsFromJSON(jsonTbl.ExtStats)
	return tbl, nil
}

// JSONTableToBlocks convert JSONTable to json, then compresses it to blocks by gzip.
func JSONTableToBlocks(jsTable *JSONTable, blockSize int) ([][]byte, error) {
	data, err := json.Marshal(jsTable)
	if err != nil {
		return nil, errors.Trace(err)
	}
	var gzippedData bytes.Buffer
	gzipWriter := gzip.NewWriter(&gzippedData)
	if _, err := gzipWriter.Write(data); err != nil {
		return nil, errors.Trace(err)
	}
	if err := gzipWriter.Close(); err != nil {
		return nil, errors.Trace(err)
	}
	blocksNum := gzippedData.Len() / blockSize
	if gzippedData.Len()%blockSize != 0 {
		blocksNum = blocksNum + 1
	}
	blocks := make([][]byte, blocksNum)
	for i := 0; i < blocksNum-1; i++ {
		blocks[i] = gzippedData.Bytes()[blockSize*i : blockSize*(i+1)]
	}
	blocks[blocksNum-1] = gzippedData.Bytes()[blockSize*(blocksNum-1):]
	return blocks, nil
}

// BlocksToJSONTable convert gzip-compressed blocks to JSONTable
func BlocksToJSONTable(blocks [][]byte) (*JSONTable, error) {
	if len(blocks) == 0 {
		return nil, errors.New("Block empty error")
	}
	data := blocks[0]
	for i := 1; i < len(blocks); i++ {
		data = append(data, blocks[i]...)
	}
	gzippedData := bytes.NewReader(data)
	gzipReader, err := gzip.NewReader(gzippedData)
	if err != nil {
		return nil, err
	}
	if err := gzipReader.Close(); err != nil {
		return nil, err
	}
	jsonStr, err := io.ReadAll(gzipReader)
	if err != nil {
		return nil, errors.Trace(err)
	}
	jsonTbl := JSONTable{}
	err = json.Unmarshal(jsonStr, &jsonTbl)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &jsonTbl, nil
}
