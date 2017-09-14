// Copyright 2016 PingCAP, Inc.
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
//
package perfschema

import (
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/util/types"
)

type TableSessionStatusHandle struct {
}

func (h *TableSessionStatusHandle) GetRows(ctx context.Context,
	cols []*table.Column) (fullRows [][]types.Datum, err error) {
	sessionVars := ctx.GetSessionVars()
	statusVars, err := variable.GetStatusVars(sessionVars)
	if err != nil {
		return nil, errors.Trace(err)
	}

	rows := [][]types.Datum{}
	for status, v := range statusVars {
		// @TODO should to check scope here?
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

//global status,
type TableGlobalStatusHandle struct {
}

func (h *TableGlobalStatusHandle) GetRows(ctx context.Context,
	cols []*table.Column) (fullRows [][]types.Datum, err error) {
	sessionVars := ctx.GetSessionVars()
	statusVars, err := variable.GetStatusVars(sessionVars)
	if err != nil {
		return nil, errors.Trace(err)
	}

	rows := [][]types.Datum{}
	for status, v := range statusVars {
		if v.Scope == variable.ScopeSession {
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

func createSysVarHandle(handleType string) tables.SysVarHandle {
	switch handleType {
	case TableSessionStatus:
		return &TableSessionStatusHandle{}
	case TableGlobalStatus:
		return &TableGlobalStatusHandle{}
	default:
		log.Fatal("unexpected system variables handler type")
	}

	return nil
}

func createSysVarTable(meta *model.TableInfo, handleType string) table.Table {
	handle := createSysVarHandle(handleType)
	if handle == nil {
		log.Fatal("unexpected system variables handler type")
	}
	return tables.CreateSysVarTable(handle, meta)
}
