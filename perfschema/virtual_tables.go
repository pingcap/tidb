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

	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/types"
	log "github.com/sirupsen/logrus"
)

// session/global status decided by scope.
type statusDataSource struct {
	meta        *model.TableInfo
	cols        []*table.Column
	globalScope bool
}

// GetRows implements the interface of VirtualDataSource.
func (ds *statusDataSource) GetRows(ctx context.Context) (fullRows [][]types.Datum,
	err error) {
	sessionVars := ctx.GetSessionVars()
	statusVars, err := variable.GetStatusVars(sessionVars)
	if err != nil {
		return nil, errors.Trace(err)
	}

	rows := [][]types.Datum{}
	for status, v := range statusVars {
		if ds.globalScope && v.Scope == variable.ScopeSession {
			continue
		}

		switch v.Value.(type) {
		case []interface{}, nil:
			v.Value = fmt.Sprintf("%v", v.Value)
		}
		value, err := types.ToString(v.Value)
		if err != nil {
			return nil, errors.Trace(err)
		}
		row := types.MakeDatums(status, value)
		rows = append(rows, row)
	}

	return rows, nil
}

// Meta implements the interface of VirtualDataSource.
func (ds *statusDataSource) Meta() *model.TableInfo {
	return ds.meta
}

// Cols implements the interface of VirtualDataSource.
func (ds *statusDataSource) Cols() []*table.Column {
	return ds.cols
}

// CreateVirtualDataSource is only used for test.
func CreateVirtualDataSource(tableName string, meta *model.TableInfo) (tables.VirtualDataSource, error) {
	columns := make([]*table.Column, 0, len(meta.Columns))
	for _, colInfo := range meta.Columns {
		col := table.ToColumn(colInfo)
		columns = append(columns, col)
	}

	switch tableName {
	case TableSessionStatus:
		return &statusDataSource{meta: meta, cols: columns, globalScope: false}, nil
	case TableGlobalStatus:
		return &statusDataSource{meta: meta, cols: columns, globalScope: true}, nil
	default:
		return nil, errors.New("can't find table named by " + tableName)
	}
}

func createVirtualTable(meta *model.TableInfo, tableName string) table.Table {
	dataSource, err := CreateVirtualDataSource(tableName, meta)
	if err != nil {
		log.Fatal(err.Error())
	}
	return tables.CreateVirtualTable(dataSource)
}
