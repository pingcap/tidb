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
	"bytes"
	"encoding/json"
	"io"

	"github.com/klauspost/compress/gzip"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/types"
	compressutil "github.com/pingcap/tidb/pkg/util/compress"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/memory"
	"go.uber.org/zap"
)

func dumpJSONExtendedStats(statsColl *statistics.ExtendedStatsColl) []*util.JSONExtendedStats {
	if statsColl == nil || len(statsColl.Stats) == 0 {
		return nil
	}
	stats := make([]*util.JSONExtendedStats, 0, len(statsColl.Stats))
	for name, item := range statsColl.Stats {
		js := &util.JSONExtendedStats{
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

func extendedStatsFromJSON(statsColl []*util.JSONExtendedStats) *statistics.ExtendedStatsColl {
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

func dumpJSONCol(hist *statistics.Histogram, cmsketch *statistics.CMSketch, topn *statistics.TopN, fmsketch *statistics.FMSketch, statsVer *int64) *util.JSONColumn {
	jsonCol := &util.JSONColumn{
		Histogram:         statistics.HistogramToProto(hist),
		NullCount:         hist.NullCount,
		TotColSize:        hist.TotColSize,
		LastUpdateVersion: hist.LastUpdateVersion,
		Correlation:       hist.Correlation,
		StatsVer:          statsVer,
	}
	if cmsketch != nil || topn != nil {
		jsonCol.CMSketch = statistics.CMSketchToProto(cmsketch, topn)
	}
	if fmsketch != nil {
		jsonCol.FMSketch = statistics.FMSketchToProto(fmsketch)
	}
	return jsonCol
}

// GenJSONTableFromStats generate jsonTable from tableInfo and stats
func GenJSONTableFromStats(sctx sessionctx.Context, dbName string, tableInfo *model.TableInfo, tbl *statistics.Table) (*util.JSONTable, error) {
	tracker := memory.NewTracker(memory.LabelForAnalyzeMemory, -1)
	tracker.AttachTo(sctx.GetSessionVars().MemTracker)
	defer tracker.Detach()
	jsonTbl := &util.JSONTable{
		DatabaseName: dbName,
		TableName:    tableInfo.Name.L,
		Columns:      make(map[string]*util.JSONColumn, len(tbl.Columns)),
		Indices:      make(map[string]*util.JSONColumn, len(tbl.Indices)),
		Count:        tbl.RealtimeCount,
		ModifyCount:  tbl.ModifyCount,
		Version:      tbl.Version,
	}
	for _, col := range tbl.Columns {
		hist, err := col.ConvertTo(statistics.UTCWithAllowInvalidDateCtx, types.NewFieldType(mysql.TypeBlob))
		if err != nil {
			return nil, errors.Trace(err)
		}
		proto := dumpJSONCol(hist, col.CMSketch, col.TopN, col.FMSketch, &col.StatsVer)
		tracker.Consume(proto.TotalMemoryUsage())
		if err := sctx.GetSessionVars().SQLKiller.HandleSignal(); err != nil {
			return nil, err
		}
		jsonTbl.Columns[col.Info.Name.L] = proto
		col.FMSketch.DestroyAndPutToPool()
		hist.DestroyAndPutToPool()
	}
	for _, idx := range tbl.Indices {
		proto := dumpJSONCol(&idx.Histogram, idx.CMSketch, idx.TopN, nil, &idx.StatsVer)
		tracker.Consume(proto.TotalMemoryUsage())
		if err := sctx.GetSessionVars().SQLKiller.HandleSignal(); err != nil {
			return nil, err
		}
		jsonTbl.Indices[idx.Info.Name.L] = proto
	}
	jsonTbl.ExtStats = dumpJSONExtendedStats(tbl.ExtendedStats)
	return jsonTbl, nil
}

// TableStatsFromJSON loads statistic from JSONTable and return the Table of statistic.
func TableStatsFromJSON(tableInfo *model.TableInfo, physicalID int64, jsonTbl *util.JSONTable) (*statistics.Table, error) {
	newHistColl := statistics.HistColl{
		PhysicalID:     physicalID,
		HavePhysicalID: true,
		RealtimeCount:  jsonTbl.Count,
		ModifyCount:    jsonTbl.ModifyCount,
		Columns:        make(map[int64]*statistics.Column, len(jsonTbl.Columns)),
		Indices:        make(map[int64]*statistics.Index, len(jsonTbl.Indices)),
	}
	tbl := &statistics.Table{
		HistColl:              newHistColl,
		ColAndIdxExistenceMap: statistics.NewColAndIndexExistenceMap(len(tableInfo.Columns), len(tableInfo.Indices)),
	}
	for id, jsonIdx := range jsonTbl.Indices {
		for _, idxInfo := range tableInfo.Indices {
			if idxInfo.Name.L != id {
				continue
			}
			hist := statistics.HistogramFromProto(jsonIdx.Histogram)
			hist.ID, hist.NullCount, hist.LastUpdateVersion, hist.Correlation = idxInfo.ID, jsonIdx.NullCount, jsonIdx.LastUpdateVersion, jsonIdx.Correlation
			cm, topN := statistics.CMSketchAndTopNFromProto(jsonIdx.CMSketch)
			statsVer := int64(statistics.Version0)
			if jsonIdx.StatsVer != nil {
				statsVer = *jsonIdx.StatsVer
			} else if jsonIdx.Histogram.Ndv > 0 || jsonIdx.NullCount > 0 {
				// If the statistics are collected without setting stats version(which happens in v4.0 and earlier versions),
				// we set it to 1.
				statsVer = int64(statistics.Version1)
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
			// All the objects in the table shares the same stats version.
			if statsVer != statistics.Version0 {
				tbl.StatsVer = int(statsVer)
			}
			tbl.Indices[idx.ID] = idx
			tbl.ColAndIdxExistenceMap.InsertIndex(idxInfo.ID, idxInfo, true)
		}
	}

	for id, jsonCol := range jsonTbl.Columns {
		for _, colInfo := range tableInfo.Columns {
			if colInfo.Name.L != id {
				continue
			}
			hist := statistics.HistogramFromProto(jsonCol.Histogram)
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
			hist, err := hist.ConvertTo(statistics.UTCWithAllowInvalidDateCtx, &tmpFT)
			if err != nil {
				return nil, errors.Trace(err)
			}
			cm, topN := statistics.CMSketchAndTopNFromProto(jsonCol.CMSketch)
			fms := statistics.FMSketchFromProto(jsonCol.FMSketch)
			hist.ID, hist.NullCount, hist.LastUpdateVersion, hist.TotColSize, hist.Correlation = colInfo.ID, jsonCol.NullCount, jsonCol.LastUpdateVersion, jsonCol.TotColSize, jsonCol.Correlation
			statsVer := int64(statistics.Version0)
			if jsonCol.StatsVer != nil {
				statsVer = *jsonCol.StatsVer
			} else if jsonCol.Histogram.Ndv > 0 || jsonCol.NullCount > 0 {
				// If the statistics are collected without setting stats version(which happens in v4.0 and earlier versions),
				// we set it to 1.
				statsVer = int64(statistics.Version1)
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
			// All the objects in the table shares the same stats version.
			if statsVer != statistics.Version0 {
				tbl.StatsVer = int(statsVer)
			}
			tbl.Columns[col.ID] = col
			tbl.ColAndIdxExistenceMap.InsertCol(colInfo.ID, colInfo, true)
		}
	}
	tbl.ExtendedStats = extendedStatsFromJSON(jsonTbl.ExtStats)
	return tbl, nil
}

// JSONTableToBlocks convert JSONTable to json, then compresses it to blocks by gzip.
func JSONTableToBlocks(jsTable *util.JSONTable, blockSize int) ([][]byte, error) {
	data, err := json.Marshal(jsTable)
	if err != nil {
		return nil, errors.Trace(err)
	}
	var gzippedData bytes.Buffer
	gzipWriter := compressutil.GzipWriterPool.Get().(*gzip.Writer)
	defer compressutil.GzipWriterPool.Put(gzipWriter)
	gzipWriter.Reset(&gzippedData)
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
func BlocksToJSONTable(blocks [][]byte) (*util.JSONTable, error) {
	if len(blocks) == 0 {
		return nil, errors.New("Block empty error")
	}
	data := blocks[0]
	for i := 1; i < len(blocks); i++ {
		data = append(data, blocks[i]...)
	}
	gzippedData := bytes.NewReader(data)
	gzipReader := compressutil.GzipReaderPool.Get().(*gzip.Reader)
	if err := gzipReader.Reset(gzippedData); err != nil {
		compressutil.GzipReaderPool.Put(gzipReader)
		return nil, err
	}
	defer func() {
		compressutil.GzipReaderPool.Put(gzipReader)
	}()
	if err := gzipReader.Close(); err != nil {
		return nil, err
	}
	jsonStr, err := io.ReadAll(gzipReader)
	if err != nil {
		return nil, errors.Trace(err)
	}
	jsonTbl := util.JSONTable{}
	err = json.Unmarshal(jsonStr, &jsonTbl)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &jsonTbl, nil
}

// TableHistoricalStatsToJSON converts the historical stats of a table to JSONTable.
func TableHistoricalStatsToJSON(sctx sessionctx.Context, physicalID int64, snapshot uint64) (jt *util.JSONTable, exist bool, err error) {
	// get meta version
	rows, _, err := util.ExecRows(sctx, "select distinct version from mysql.stats_meta_history where table_id = %? and version <= %? order by version desc limit 1", physicalID, snapshot)
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
	rows, _, err = util.ExecRows(sctx, "select modify_count, count from mysql.stats_meta_history where table_id = %? and version = %?", physicalID, statsMetaVersion)
	if err != nil {
		return nil, false, errors.AddStack(err)
	}
	modifyCount, count := rows[0].GetInt64(0), rows[0].GetInt64(1)

	// get stats version
	rows, _, err = util.ExecRows(sctx, "select distinct version from mysql.stats_history where table_id = %? and version <= %? order by version desc limit 1", physicalID, snapshot)
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
	rows, _, err = util.ExecRows(sctx, "select stats_data from mysql.stats_history where table_id = %? and version = %? order by seq_no", physicalID, statsVersion)
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
