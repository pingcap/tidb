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
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/profile"
	"github.com/pingcap/tidb/util/stmtsummary"
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
}

// perfSchemaTable stands for the fake table all its data is in the memory.
type perfSchemaTable struct {
	infoschema.VirtualTable
	meta *model.TableInfo
	cols []*table.Column
}

var pluginTable = make(map[string]func(autoid.Allocator, *model.TableInfo) (table.Table, error))

// RegisterTable registers a new table into TiDB.
func RegisterTable(tableName, sql string,
	tableFromMeta func(autoid.Allocator, *model.TableInfo) (table.Table, error)) {
	perfSchemaTables = append(perfSchemaTables, sql)
	pluginTable[tableName] = tableFromMeta
}

func tableFromMeta(alloc autoid.Allocator, meta *model.TableInfo) (table.Table, error) {
	if f, ok := pluginTable[meta.Name.L]; ok {
		ret, err := f(alloc, meta)
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
	t := &perfSchemaTable{
		meta: meta,
		cols: columns,
	}
	return t
}

// Cols implements table.Table Type interface.
func (vt *perfSchemaTable) Cols() []*table.Column {
	return vt.cols
}

// WritableCols implements table.Table Type interface.
func (vt *perfSchemaTable) WritableCols() []*table.Column {
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

func (vt *perfSchemaTable) getRows(ctx sessionctx.Context, cols []*table.Column) (fullRows [][]types.Datum, err error) {
	switch vt.meta.Name.O {
	case tableNameEventsStatementsSummaryByDigest:
		fullRows = stmtsummary.StmtSummaryByDigestMap.ToDatum()
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
		fullRows, err = (&profile.Collector{}).Goroutines()
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
