// Copyright 2022 PingCAP, Inc.
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

package domain

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/statistics/handle"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

var (
	generateHistoricalStatsSuccessCounter = metrics.HistoricalStatsCounter.WithLabelValues("generate", "success")
	generateHistoricalStatsFailedCounter  = metrics.HistoricalStatsCounter.WithLabelValues("generate", "fail")
)

// HistoricalStatsWorker indicates for dump historical stats
type HistoricalStatsWorker struct {
	tblCH chan int64
	sctx  sessionctx.Context
}

// SendTblToDumpHistoricalStats send tableID to worker to dump historical stats
func (w *HistoricalStatsWorker) SendTblToDumpHistoricalStats(tableID int64) {
	send := enableDumpHistoricalStats.Load()
	failpoint.Inject("sendHistoricalStats", func(val failpoint.Value) {
		if val.(bool) {
			send = true
		}
	})
	if !send {
		return
	}
	select {
	case w.tblCH <- tableID:
		return
	default:
		logutil.BgLogger().Warn("discard dump historical stats task", zap.Int64("table-id", tableID))
	}
}

// DumpHistoricalStats dump stats by given tableID
func (w *HistoricalStatsWorker) DumpHistoricalStats(tableID int64, statsHandle *handle.Handle) error {
	historicalStatsEnabled, err := statsHandle.CheckHistoricalStatsEnable()
	if err != nil {
		return errors.Errorf("check tidb_enable_historical_stats failed: %v", err)
	}
	if !historicalStatsEnabled {
		return nil
	}
	sctx := w.sctx
	is := GetDomain(sctx).InfoSchema()
	isPartition := false
	var tblInfo *model.TableInfo
	tbl, existed := is.TableByID(tableID)
	if !existed {
		tbl, db, p := is.FindTableByPartitionID(tableID)
		if tbl != nil && db != nil && p != nil {
			isPartition = true
			tblInfo = tbl.Meta()
		} else {
			return errors.Errorf("cannot get table by id %d", tableID)
		}
	} else {
		tblInfo = tbl.Meta()
	}
	dbInfo, existed := is.SchemaByTable(tblInfo)
	if !existed {
		return errors.Errorf("cannot get DBInfo by TableID %d", tableID)
	}
	if _, err := statsHandle.RecordHistoricalStatsToStorage(dbInfo.Name.O, tblInfo, tableID, isPartition); err != nil {
		generateHistoricalStatsFailedCounter.Inc()
		return errors.Errorf("record table %s.%s's historical stats failed, err:%v", dbInfo.Name.O, tblInfo.Name.O, err)
	}
	generateHistoricalStatsSuccessCounter.Inc()
	return nil
}

// GetOneHistoricalStatsTable gets one tableID from channel, only used for test
func (w *HistoricalStatsWorker) GetOneHistoricalStatsTable() int64 {
	select {
	case tblID := <-w.tblCH:
		return tblID
	default:
		return -1
	}
}
