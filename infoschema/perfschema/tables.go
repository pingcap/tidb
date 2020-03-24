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
	"strings"

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
	tableNameEventsStatementsSummaryByDigest        = "events_statements_summary_by_digest"
	tableNameEventsStatementsSummaryByDigestHistory = "events_statements_summary_by_digest_history"
	tableNameTiDBProfileCPU                         = "tidb_profile_cpu"
	tableNameTiDBProfileMemory                      = "tidb_profile_memory"
	tableNameTiDBProfileMutex                       = "tidb_profile_mutex"
	tableNameTiDBProfileAllocs                      = "tidb_profile_allocs"
	tableNameTiDBProfileBlock                       = "tidb_profile_block"
	tableNameTiDBProfileGoroutines                  = "tidb_profile_goroutines"
)

var tableList = []string{
	tableNameEventsStatementsSummaryByDigest,
	tableNameTiDBProfileCPU,
	tableNameTiDBProfileMemory,
	tableNameTiDBProfileMutex,
	tableNameTiDBProfileAllocs,
	tableNameTiDBProfileBlock,
	tableNameTiDBProfileGoroutines,
	tableNameEventsStatementsSummaryByDigestHistory,
}

// perfSchemaTable stands for the fake table all its data is in the memory.
type perfSchemaTable struct {
	infoschema.VirtualTable
	meta *model.TableInfo
	cols []*table.Column
}

var pluginTable = make(map[string]func(autoid.Allocators, *model.TableInfo) (table.Table, error))

// IsPredefinedTable judges whether this table is predefined.
func IsPredefinedTable(tableName string) bool {
	for _, name := range tableList {
		if strings.EqualFold(tableName, name) {
			return true
		}
	}
	return false
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
		fullRows = stmtsummary.StmtSummaryByDigestMap.ToCurrentDatum()
	case tableNameEventsStatementsSummaryByDigestHistory:
		fullRows = stmtsummary.StmtSummaryByDigestMap.ToHistoryDatum()
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
