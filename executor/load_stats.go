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
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"encoding/json"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/util/chunk"
	"golang.org/x/net/context"
)

var _ Executor = &LoadStatsExec{}

// LoadStatsExec represents a load statistic executor.
type LoadStatsExec struct {
	baseExecutor
	info *LoadStatsInfo
}

// LoadStatsInfo saves the information of loading statistic operation.
type LoadStatsInfo struct {
	Path string
	Ctx  sessionctx.Context
}

// loadStatsVarKeyType is a dummy type to avoid naming collision in context.
type loadStatsVarKeyType int

// String defines a Stringer function for debugging and pretty printing.
func (k loadStatsVarKeyType) String() string {
	return "load_stats_var"
}

// LoadStatsVarKey is a variable key for load statistic.
const LoadStatsVarKey loadStatsVarKeyType = 0

// Next implements the Executor Next interface.
func (e *LoadStatsExec) Next(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	if len(e.info.Path) == 0 {
		return errors.New("Load Stats: file path is empty")
	}
	val := e.ctx.Value(LoadStatsVarKey)
	if val != nil {
		e.ctx.SetValue(LoadStatsVarKey, nil)
		return errors.New("Load Stats: previous load stats option isn't closed normally")
	}
	e.ctx.SetValue(LoadStatsVarKey, e.info)
	return nil
}

// Close implements the Executor Close interface.
func (e *LoadStatsExec) Close() error {
	return nil
}

// Open implements the Executor Open interface.
func (e *LoadStatsExec) Open(ctx context.Context) error {
	return nil
}

// Update updates the stats of the corresponding table according to the data.
func (e *LoadStatsInfo) Update(data []byte) error {
	jsonTbl := &statistics.JSONTable{}
	if err := json.Unmarshal(data, jsonTbl); err != nil {
		return errors.Trace(err)
	}

	do := domain.GetDomain(e.Ctx)
	is := do.InfoSchema()

	tableInfo, err := is.TableByName(model.NewCIStr(jsonTbl.DatabaseName), model.NewCIStr(jsonTbl.TableName))
	if err != nil {
		return errors.Trace(err)
	}

	h := do.StatsHandle()
	if h == nil {
		return errors.New("Load Stats: handle is nil")
	}

	tbl, err := h.LoadStatsFromJSON(tableInfo.Meta(), jsonTbl)
	if err != nil {
		return errors.Trace(err)
	}

	if h.Lease > 0 {
		hists := make([]*statistics.Histogram, 0, len(tbl.Columns))
		cms := make([]*statistics.CMSketch, 0, len(tbl.Columns))
		for _, col := range tbl.Columns {
			hists = append(hists, &col.Histogram)
			cms = append(cms, col.CMSketch)
		}
		h.AnalyzeResultCh() <- &statistics.AnalyzeResult{
			TableID: tbl.TableID,
			Hist:    hists,
			Cms:     cms,
			Count:   tbl.Count,
			IsIndex: 0,
			Err:     nil}

		hists = make([]*statistics.Histogram, 0, len(tbl.Indices))
		cms = make([]*statistics.CMSketch, 0, len(tbl.Indices))
		for _, idx := range tbl.Indices {
			hists = append(hists, &idx.Histogram)
			cms = append(cms, idx.CMSketch)
		}
		h.AnalyzeResultCh() <- &statistics.AnalyzeResult{
			TableID: tbl.TableID,
			Hist:    hists,
			Cms:     cms,
			Count:   tbl.Count,
			IsIndex: 1,
			Err:     nil}

		return nil
	}
	for _, col := range tbl.Columns {
		err = statistics.SaveStatsToStorage(e.Ctx, tbl.TableID, tbl.Count, 0, &col.Histogram, col.CMSketch)
		if err != nil {
			return errors.Trace(err)
		}
	}
	for _, idx := range tbl.Indices {
		err = statistics.SaveStatsToStorage(e.Ctx, tbl.TableID, tbl.Count, 1, &idx.Histogram, idx.CMSketch)
		if err != nil {
			return errors.Trace(err)
		}
	}
	err = h.Update(GetInfoSchema(e.Ctx))
	return errors.Trace(err)
}
