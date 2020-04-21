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

package perfschema

import (
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/profile"
)

const (
	tableNameGlobalStatus                    = "global_status"
	tableNameSessionStatus                   = "session_status"
	tableNameSetupActors                     = "setup_actors"
	tableNameSetupObjects                    = "setup_objects"
	tableNameSetupInstruments                = "setup_instruments"
	tableNameSetupConsumers                  = "setup_consumers"
	tableNameEventsStatementsCurrent         = "events_statements_current"
	tableNameEventsStatementsHistory         = "events_statements_history"
	tableNameEventsStatementsHistoryLong     = "events_statements_history_long"
	tableNamePreparedStatementsInstances     = "prepared_statements_instances"
	tableNameEventsTransactionsCurrent       = "events_transactions_current"
	tableNameEventsTransactionsHistory       = "events_transactions_history"
	tableNameEventsTransactionsHistoryLong   = "events_transactions_history_long"
	tableNameEventsStagesCurrent             = "events_stages_current"
	tableNameEventsStagesHistory             = "events_stages_history"
	tableNameEventsStagesHistoryLong         = "events_stages_history_long"
	tableNameEventsStatementsSummaryByDigest = "events_statements_summary_by_digest"
	tableNameTiDBProfileCPU                  = "tidb_profile_cpu"
	tableNameTiDBProfileMemory               = "tidb_profile_memory"
	tableNameTiDBProfileMutex                = "tidb_profile_mutex"
	tableNameTiDBProfileAllocs               = "tidb_profile_allocs"
	tableNameTiDBProfileBlock                = "tidb_profile_block"
	tableNameTiDBProfileGoroutines           = "tidb_profile_goroutines"
	tableNameTiKVProfileCPU                  = "tikv_profile_cpu"
	tableNamePDProfileCPU                    = "pd_profile_cpu"
	tableNamePDProfileMemory                 = "pd_profile_memory"
	tableNamePDProfileMutex                  = "pd_profile_mutex"
	tableNamePDProfileAllocs                 = "pd_profile_allocs"
	tableNamePDProfileBlock                  = "pd_profile_block"
	tableNamePDProfileGoroutines             = "pd_profile_goroutines"
)

var tableIDMap = map[string]int64{
	tableNameGlobalStatus:                    autoid.PerformanceSchemaDBID + 1,
	tableNameSessionStatus:                   autoid.PerformanceSchemaDBID + 2,
	tableNameSetupActors:                     autoid.PerformanceSchemaDBID + 3,
	tableNameSetupObjects:                    autoid.PerformanceSchemaDBID + 4,
	tableNameSetupInstruments:                autoid.PerformanceSchemaDBID + 5,
	tableNameSetupConsumers:                  autoid.PerformanceSchemaDBID + 6,
	tableNameEventsStatementsCurrent:         autoid.PerformanceSchemaDBID + 7,
	tableNameEventsStatementsHistory:         autoid.PerformanceSchemaDBID + 8,
	tableNameEventsStatementsHistoryLong:     autoid.PerformanceSchemaDBID + 9,
	tableNamePreparedStatementsInstances:     autoid.PerformanceSchemaDBID + 10,
	tableNameEventsTransactionsCurrent:       autoid.PerformanceSchemaDBID + 11,
	tableNameEventsTransactionsHistory:       autoid.PerformanceSchemaDBID + 12,
	tableNameEventsTransactionsHistoryLong:   autoid.PerformanceSchemaDBID + 13,
	tableNameEventsStagesCurrent:             autoid.PerformanceSchemaDBID + 14,
	tableNameEventsStagesHistory:             autoid.PerformanceSchemaDBID + 15,
	tableNameEventsStagesHistoryLong:         autoid.PerformanceSchemaDBID + 16,
	tableNameEventsStatementsSummaryByDigest: autoid.PerformanceSchemaDBID + 17,
	tableNameTiDBProfileCPU:                  autoid.PerformanceSchemaDBID + 18,
	tableNameTiDBProfileMemory:               autoid.PerformanceSchemaDBID + 19,
	tableNameTiDBProfileMutex:                autoid.PerformanceSchemaDBID + 20,
	tableNameTiDBProfileAllocs:               autoid.PerformanceSchemaDBID + 21,
	tableNameTiDBProfileBlock:                autoid.PerformanceSchemaDBID + 22,
	tableNameTiDBProfileGoroutines:           autoid.PerformanceSchemaDBID + 23,
	tableNameTiKVProfileCPU:                  autoid.PerformanceSchemaDBID + 24,
	tableNamePDProfileCPU:                    autoid.PerformanceSchemaDBID + 25,
	tableNamePDProfileMemory:                 autoid.PerformanceSchemaDBID + 26,
	tableNamePDProfileMutex:                  autoid.PerformanceSchemaDBID + 27,
	tableNamePDProfileAllocs:                 autoid.PerformanceSchemaDBID + 28,
	tableNamePDProfileBlock:                  autoid.PerformanceSchemaDBID + 29,
	tableNamePDProfileGoroutines:             autoid.PerformanceSchemaDBID + 30,
}

// perfSchemaTable stands for the fake table all its data is in the memory.
type perfSchemaTable struct {
	infoschema.VirtualTable
	meta *model.TableInfo
	cols []*table.Column
	tp   table.Type
}

var pluginTable = make(map[string]func(autoid.Allocators, *model.TableInfo) (table.Table, error))

// IsPredefinedTable judges whether this table is predefined.
func IsPredefinedTable(tableName string) bool {
	_, ok := tableIDMap[strings.ToLower(tableName)]
	return ok
}

// RegisterTable registers a new table into TiDB.
func RegisterTable(tableName, sql string,
	tableFromMeta func(autoid.Allocators, *model.TableInfo) (table.Table, error)) {
	perfSchemaTables = append(perfSchemaTables, sql)
	pluginTable[tableName] = tableFromMeta
}

func tableFromMeta(allocs autoid.Allocators, meta *model.TableInfo) (table.Table, error) {
	if f, ok := pluginTable[meta.Name.L]; ok {
		ret, err := f(allocs, meta)
		return ret, err
	}
	return createPerfSchemaTable(meta), nil
}

// createPerfSchemaTable creates all perfSchemaTables
func createPerfSchemaTable(meta *model.TableInfo) *perfSchemaTable {
	columns := make([]*table.Column, 0, len(meta.Columns))
	for _, colInfo := range meta.Columns {
		col := table.ToColumn(colInfo)
		columns = append(columns, col)
	}
	tp := table.VirtualTable
	t := &perfSchemaTable{
		meta: meta,
		cols: columns,
		tp:   tp,
	}
	return t
}

// Cols implements table.Table Type interface.
func (vt *perfSchemaTable) Cols() []*table.Column {
	return vt.cols
}

// VisibleCols implements table.Table VisibleCols interface.
func (vt *perfSchemaTable) VisibleCols() []*table.Column {
	return vt.cols
}

// HiddenCols implements table.Table HiddenCols interface.
func (vt *perfSchemaTable) HiddenCols() []*table.Column {
	return nil
}

// WritableCols implements table.Table Type interface.
func (vt *perfSchemaTable) WritableCols() []*table.Column {
	return vt.cols
}

// DeletableCols implements table DeletableCols interface.
func (vt *perfSchemaTable) DeletableCols() []*table.Column {
	return vt.cols
}

// GetID implements table.Table GetID interface.
func (vt *perfSchemaTable) GetPhysicalID() int64 {
	return vt.meta.ID
}

// Meta implements table.Table Type interface.
func (vt *perfSchemaTable) Meta() *model.TableInfo {
	return vt.meta
}

// Type implements table.Table Type interface.
func (vt *perfSchemaTable) Type() table.Type {
	return vt.tp
}

func (vt *perfSchemaTable) getRows(ctx sessionctx.Context, cols []*table.Column) (fullRows [][]types.Datum, err error) {
	switch vt.meta.Name.O {
	case tableNameTiDBProfileCPU:
		fullRows, err = (&profile.Collector{}).ProfileGraph("cpu")
	case tableNameTiDBProfileMemory:
		fullRows, err = (&profile.Collector{}).ProfileGraph("heap")
	case tableNameTiDBProfileMutex:
		fullRows, err = (&profile.Collector{}).ProfileGraph("mutex")
	case tableNameTiDBProfileAllocs:
		fullRows, err = (&profile.Collector{}).ProfileGraph("allocs")
	case tableNameTiDBProfileBlock:
		fullRows, err = (&profile.Collector{}).ProfileGraph("block")
	case tableNameTiDBProfileGoroutines:
		fullRows, err = (&profile.Collector{}).ProfileGraph("goroutine")
	case tableNameTiKVProfileCPU:
		interval := fmt.Sprintf("%d", profile.CPUProfileInterval/time.Second)
		fullRows, err = dataForRemoteProfile(ctx, "tikv", "/debug/pprof/profile?seconds="+interval, false)
	case tableNamePDProfileCPU:
		interval := fmt.Sprintf("%d", profile.CPUProfileInterval/time.Second)
		fullRows, err = dataForRemoteProfile(ctx, "pd", "/pd/api/v1/debug/pprof/profile?seconds="+interval, false)
	case tableNamePDProfileMemory:
		fullRows, err = dataForRemoteProfile(ctx, "pd", "/pd/api/v1/debug/pprof/heap", false)
	case tableNamePDProfileMutex:
		fullRows, err = dataForRemoteProfile(ctx, "pd", "/pd/api/v1/debug/pprof/mutex", false)
	case tableNamePDProfileAllocs:
		fullRows, err = dataForRemoteProfile(ctx, "pd", "/pd/api/v1/debug/pprof/allocs", false)
	case tableNamePDProfileBlock:
		fullRows, err = dataForRemoteProfile(ctx, "pd", "/pd/api/v1/debug/pprof/block", false)
	case tableNamePDProfileGoroutines:
		fullRows, err = dataForRemoteProfile(ctx, "pd", "/pd/api/v1/debug/pprof/goroutine?debug=2", true)
	}
	if err != nil {
		return
	}
	if len(cols) == len(vt.cols) {
		return
	}
	rows := make([][]types.Datum, len(fullRows))
	for i, fullRow := range fullRows {
		row := make([]types.Datum, len(cols))
		for j, col := range cols {
			row[j] = fullRow[col.Offset]
		}
		rows[i] = row
	}
	return rows, nil
}

// IterRecords implements table.Table IterRecords interface.
func (vt *perfSchemaTable) IterRecords(ctx sessionctx.Context, startKey kv.Key, cols []*table.Column,
	fn table.RecordIterFunc) error {
	if len(startKey) != 0 {
		return table.ErrUnsupportedOp
	}
	rows, err := vt.getRows(ctx, cols)
	if err != nil {
		return err
	}
	for i, row := range rows {
		more, err := fn(int64(i), row, cols)
		if err != nil {
			return err
		}
		if !more {
			break
		}
	}
	return nil
}

func dataForRemoteProfile(ctx sessionctx.Context, nodeType, uri string, isGoroutine bool) ([][]types.Datum, error) {
	var (
		servers []infoschema.ServerInfo
		err     error
	)
	switch nodeType {
	case "tikv":
		servers, err = infoschema.GetTiKVServerInfo(ctx)
	case "pd":
		servers, err = infoschema.GetPDServerInfo(ctx)
	default:
		return nil, errors.Errorf("%s does not support profile remote component", nodeType)
	}
	failpoint.Inject("mockRemoteNodeStatusAddress", func(val failpoint.Value) {
		// The cluster topology is injected by `failpoint` expression and
		// there is no extra checks for it. (let the test fail if the expression invalid)
		if s := val.(string); len(s) > 0 {
			servers = servers[:0]
			for _, server := range strings.Split(s, ";") {
				parts := strings.Split(server, ",")
				if parts[0] != nodeType {
					continue
				}
				servers = append(servers, infoschema.ServerInfo{
					ServerType: parts[0],
					Address:    parts[1],
					StatusAddr: parts[2],
				})
			}
			// erase error
			err = nil
		}
	})
	if err != nil {
		return nil, errors.Trace(err)
	}

	type result struct {
		addr string
		rows [][]types.Datum
		err  error
	}

	wg := sync.WaitGroup{}
	ch := make(chan result, len(servers))
	for _, server := range servers {
		statusAddr := server.StatusAddr
		if len(statusAddr) == 0 {
			ctx.GetSessionVars().StmtCtx.AppendWarning(errors.Errorf("TiKV node %s does not contain status address", server.Address))
			continue
		}

		wg.Add(1)
		go func(address string) {
			util.WithRecovery(func() {
				defer wg.Done()
				url := fmt.Sprintf("%s://%s%s", util.InternalHTTPSchema(), statusAddr, uri)
				req, err := http.NewRequest(http.MethodGet, url, nil)
				if err != nil {
					ch <- result{err: errors.Trace(err)}
					return
				}
				// Forbidden PD follower proxy
				req.Header.Add("PD-Allow-follower-handle", "true")
				// TiKV output svg format in default
				req.Header.Add("Content-Type", "application/protobuf")
				resp, err := util.InternalHTTPClient().Do(req)
				if err != nil {
					ch <- result{err: errors.Trace(err)}
					return
				}
				defer func() {
					terror.Log(resp.Body.Close())
				}()
				if resp.StatusCode != http.StatusOK {
					ch <- result{err: errors.Errorf("request %s failed: %s", url, resp.Status)}
					return
				}
				collector := profile.Collector{}
				var rows [][]types.Datum
				if isGoroutine {
					rows, err = collector.ParseGoroutines(resp.Body)
				} else {
					rows, err = collector.ProfileReaderToDatums(resp.Body)
				}
				if err != nil {
					ch <- result{err: errors.Trace(err)}
					return
				}
				ch <- result{addr: address, rows: rows}
			}, nil)
		}(statusAddr)
	}

	wg.Wait()
	close(ch)

	// Keep the original order to make the result more stable
	var results []result
	for result := range ch {
		if result.err != nil {
			ctx.GetSessionVars().StmtCtx.AppendWarning(result.err)
			continue
		}
		results = append(results, result)
	}
	sort.Slice(results, func(i, j int) bool { return results[i].addr < results[j].addr })
	var finalRows [][]types.Datum
	for _, result := range results {
		addr := types.NewStringDatum(result.addr)
		for _, row := range result.rows {
			// Insert the node address in front of rows
			finalRows = append(finalRows, append([]types.Datum{addr}, row...))
		}
	}
	return finalRows, nil
}
