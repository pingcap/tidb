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

package executor

import (
	"fmt"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/dashbase"
)

// DashbaseInsertExec represents a DashbaseInsert executor.
type DashbaseInsertExec struct {
	baseExecutor

	TableInfo       *model.TableInfo
	HiColumns       []*model.ColumnInfo
	LoColumns       []*dashbase.Column
	Hi2LoConverters []dashbase.Hi2LoConverter
	Values          []*expression.Constant
	ctx             context.Context
	finished        bool
}

// Next implements Execution Next interface.Insert
func (e *DashbaseInsertExec) Next() (*Row, error) {
	if e.finished {
		return nil, nil
	}

	row := make(map[string]interface{})
	for i, value := range e.Values {
		castedValue, err := table.CastValue(e.ctx, value.Value, e.HiColumns[i])
		if err != nil {
			return nil, errors.Trace(err)
		}
		jsonValue := e.Hi2LoConverters[i](castedValue)
		row[e.LoColumns[i].Name] = jsonValue
	}

	client := dashbase.FirehoseClient{
		Host: e.TableInfo.DashbaseConnection.FirehoseHostname,
		Port: e.TableInfo.DashbaseConnection.FirehosePort,
	}

	// Pass all columns two the second parameter.
	result, err := client.InsertOne(row, e.TableInfo.DashbaseColumns)
	if err != nil {
		return nil, errors.Trace(err)
	}

	fmt.Printf("======== Dashbase Response ========\n")
	fmt.Printf("%v\n", result)

	e.finished = true
	return nil, nil
}
