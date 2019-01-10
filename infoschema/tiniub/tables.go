// Copyright 2019 PingCAP, Inc.
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

package tiniub

import (
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/infoschema/perfschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
)

// Init should be called before perfschema.Init()
func Init() {
	perfschema.RegisterTable("slow_query", tableSlowQuery, tableFromMeta)
}

type slowQueryTable struct {
	infoschema.VirtualTable
	meta *model.TableInfo
	cols []*table.Column
}

const tableSlowQuery = "CREATE TABLE if not exists slow_query (" +
	"`SQL` VARCHAR(4096)," +
	"`START` TIMESTAMP (6)," +
	"`DURATION` TIME (6)," +
	"DETAILS VARCHAR(256)," +
	"SUCC TINYINT," +
	"CONN_ID BIGINT," +
	"TRANSACTION_TS BIGINT," +
	"USER VARCHAR(32) NOT NULL," +
	"DB VARCHAR(64) NOT NULL," +
	"TABLE_IDS VARCHAR(256)," +
	"INDEX_IDS VARCHAR(256)," +
	"INTERNAL TINYINT);"

// tableFromMeta creates the slow query table.
func tableFromMeta(alloc autoid.Allocator, meta *model.TableInfo) (table.Table, error) {
	columns := make([]*table.Column, 0, len(meta.Columns))
	for _, colInfo := range meta.Columns {
		col := table.ToColumn(colInfo)
		columns = append(columns, col)
	}
	t := &slowQueryTable{
		meta: meta,
		cols: columns,
	}
	return t, nil
}

// IterRecords rewrites the IterRecords method of slowQueryTable.
func (s slowQueryTable) IterRecords(ctx sessionctx.Context, startKey kv.Key, cols []*table.Column, fn table.RecordIterFunc) error {
	dom := domain.GetDomain(ctx)
	result := dom.ShowSlowQuery(&ast.ShowSlow{Tp: 42})

	for i, item := range result {
		row := make([]types.Datum, 0, len(cols))
		for _, col := range cols {
			switch col.Name.L {
			case "sql":
				row = append(row, types.NewDatum(item.SQL))
			case "start":
				ts := types.NewTimeDatum(types.Time{types.FromGoTime(item.Start), mysql.TypeTimestamp, types.MaxFsp})
				row = append(row, ts)
			case "duration":
				row = append(row, types.NewDurationDatum(types.Duration{item.Duration, types.MaxFsp}))
			case "details":
				row = append(row, types.NewDatum(item.Detail.String()))
			case "succ":
				row = append(row, types.NewDatum(item.Succ))
			case "conn_id":
				row = append(row, types.NewDatum(item.ConnID))
			case "transaction_ts":
				row = append(row, types.NewDatum(item.TxnTS))
			case "user":
				row = append(row, types.NewDatum(item.User))
			case "db":
				row = append(row, types.NewDatum(item.DB))
			case "table_ids":
				row = append(row, types.NewDatum(item.TableIDs))
			case "index_ids":
				row = append(row, types.NewDatum(item.IndexIDs))
			case "internal":
				row = append(row, types.NewDatum(item.Internal))
			}
		}

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

// Cols implements table.Table Type interface.
func (vt *slowQueryTable) Cols() []*table.Column {
	return vt.cols
}

// WritableCols implements table.Table Type interface.
func (vt *slowQueryTable) WritableCols() []*table.Column {
	return vt.cols
}

// GetID implements table.Table GetID interface.
func (vt *slowQueryTable) GetPhysicalID() int64 {
	return vt.meta.ID
}

// Meta implements table.Table Type interface.
func (vt *slowQueryTable) Meta() *model.TableInfo {
	return vt.meta
}
