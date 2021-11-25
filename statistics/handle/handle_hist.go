// Copyright 2021 PingCAP, Inc.
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
	"runtime"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/pingcap/tidb/util/timeutil"
	"go.uber.org/zap"
)

type NeededColumnsCh struct {
	ColumnsCh        chan *NeededColumnTask
	TimeoutColumnsCh chan *NeededColumnTask
}

// NeededColumnTask represents one needed column with expire time.
type NeededColumnTask struct {
	TableColumnID model.TableColumnID
	ToTimeout     time.Time
	Wg            *sync.WaitGroup
}

// AppendNeededColumn appends needed column to ch, if exists, do not append the duplicated one.
func (h *Handle) AppendNeededColumn(c model.TableColumnID, wg *sync.WaitGroup, timeout time.Duration) {
	toTimout := time.Now().Local().Add(timeout)
	colTask := &NeededColumnTask{TableColumnID: c, ToTimeout: toTimout, Wg: wg}
	h.HistogramNeeded.ColumnsCh <- colTask
}

var ErrExit = errors.New("Stop loading since domain is closed.")

// SubLoadWorker loads hist data for each column
func (h *Handle) SubLoadWorker(ctx sessionctx.Context, exit chan struct{}) error {
	reader, err0 := h.getStatsReader(0, ctx.(sqlexec.RestrictedSQLExecutor))
	if err0 != nil {
		return err0
	}
	defer func() {
		err1 := h.releaseStatsReader(reader, ctx.(sqlexec.RestrictedSQLExecutor))
		if err1 != nil && err0 == nil {
			logutil.BgLogger().Error("Fail to release stats loader: ", zap.Error(err1))
		}
	}()
	batched := 0
	for {
		batched += 1
		err := h.handleOneTask(reader, exit)
		if err != nil {
			switch err {
			case ErrExit:
				return nil
			default:
				time.Sleep(500 * time.Millisecond)
				continue
			}
		}
		if batched >= 100 {
			// refresh statsReader after a while for latest stats
			err = h.releaseStatsReader(reader, ctx.(sqlexec.RestrictedSQLExecutor))
			if err != nil {
				logutil.BgLogger().Error("Fail to release stats loader: ", zap.Error(err))
			}
			reader, err = h.getStatsReader(0, ctx.(sqlexec.RestrictedSQLExecutor))
			if err != nil {
				// TODO will begin/commit fail?
				logutil.BgLogger().Error("Fail to new stats loader: ", zap.Error(err))
			}
			batched = 0
		}
	}
}

// handleOneTask handles one column task.
func (h *Handle) handleOneTask(reader *statsReader, exit chan struct{}) error {
	defer func() {
		if r := recover(); r != nil {
			buf := make([]byte, 4096)
			stackSize := runtime.Stack(buf, false)
			buf = buf[:stackSize]
			logutil.BgLogger().Error("stats loading panicked", zap.String("stack", string(buf)))
			metrics.PanicCounter.WithLabelValues(metrics.LabelStatsLoadWorker).Inc()
		}
	}()
	task, err0 := h.drainColTask(exit)
	if err0 != nil && task == nil {
		logutil.BgLogger().Fatal("Fail to drain task for stats loading.")
		return err0
	}
	col := task.TableColumnID
	oldCache := h.statsCache.Load().(statsCache)
	tbl, ok := oldCache.tables[col.TableID]
	if !ok {
		task.Wg.Done()
		return nil
	}
	c, ok := tbl.Columns[col.ColumnID]
	if !ok || c.Len() > 0 {
		task.Wg.Done()
		return nil
	}
	hist, err := h.readStatsForOne(col, c, reader)
	if err != nil {
		h.HistogramNeeded.ColumnsCh <- task
		return err
	}
	if hist != nil && h.updateCachedColumn(col, hist) {
		task.Wg.Done()
	}
	return nil
}

// readStatsForOne reads hist for one column, TODO load data via kv-get asynchronously
func (h *Handle) readStatsForOne(col model.TableColumnID, c *statistics.Column, reader *statsReader) (*statistics.Column, error) {
	hg, err := h.histogramFromStorage(reader, col.TableID, c.ID, &c.Info.FieldType, c.Histogram.NDV, 0, c.LastUpdateVersion, c.NullCount, c.TotColSize, c.Correlation)
	if err != nil {
		return nil, errors.Trace(err)
	}
	cms, topN, err := h.cmSketchAndTopNFromStorage(reader, col.TableID, 0, col.ColumnID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	fms, err := h.fmSketchFromStorage(reader, col.TableID, 0, col.ColumnID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	rows, _, err := reader.read("select stats_ver from mysql.stats_histograms where is_index = 0 and table_id = %? and hist_id = %?", col.TableID, col.ColumnID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(rows) == 0 {
		logutil.BgLogger().Error("fail to get stats version for this histogram", zap.Int64("table_id", col.TableID), zap.Int64("hist_id", col.ColumnID))
	}
	colHist := &statistics.Column{
		PhysicalID: col.TableID,
		Histogram:  *hg,
		Info:       c.Info,
		CMSketch:   cms,
		TopN:       topN,
		FMSketch:   fms,
		IsHandle:   c.IsHandle,
		StatsVer:   rows[0].GetInt64(0),
	}
	// Column.Count is calculated by Column.TotalRowCount(). Hence, we don't set Column.Count when initializing colHist.
	colHist.Count = int64(colHist.TotalRowCount())
	return colHist, nil
}

// drainColTask will hang until a column task can return.
func (h *Handle) drainColTask(exit chan struct{}) (*NeededColumnTask, error) {
	timeout := time.Nanosecond * 100
	to := timeutil.NewGoodTimer(timeout)
	for {
		to.Reset(timeout)
		select { // select ColumnsCh firstly since the priority
		case task, ok := <-h.HistogramNeeded.ColumnsCh:
			if !ok {
				return nil, errors.New("drainColTask: cannot read from a closed ColumnsCh, maybe the chan is closed.")
			}
			if time.Now().After(task.ToTimeout) {
				h.HistogramNeeded.TimeoutColumnsCh <- task
				continue
			}
			return task, nil
		case <-to.C():
			to.SetRead()
			select { // select TimeoutColumnsCh if there's no task from ColumnsCh currently
			case task, ok := <-h.HistogramNeeded.TimeoutColumnsCh:
				if !ok {
					return nil, errors.New("drainColTask: cannot read from a closed TimeoutColumnsCh, maybe the chan is closed.")
				}
				return task, nil
			case <-to.C():
				to.SetRead()
				continue
			}
		case <-exit:
			return nil, ErrExit
		case <-exit:
			return nil, ErrExit
		}
	}
}

// updateCachedColumn updates the column hist to global statsCache.
func (h *Handle) updateCachedColumn(col model.TableColumnID, colHist *statistics.Column) (updated bool) {
	h.statsCache.Lock()
	defer h.statsCache.Unlock()
	// Reload the latest stats cache, otherwise the `updateStatsCache` may fail with high probability, because functions
	// like `GetPartitionStats` called in `fmSketchFromStorage` would have modified the stats cache already.
	oldCache := h.statsCache.Load().(statsCache)
	tbl, ok := oldCache.tables[col.TableID]
	if !ok {
		return true
	}
	c, ok := tbl.Columns[col.ColumnID]
	if !ok || c.Len() > 0 {
		return true
	}
	tbl = tbl.Copy()
	tbl.Columns[c.ID] = colHist
	return h.updateStatsCache(oldCache.update([]*statistics.Table{tbl}, nil, oldCache.version))
}
