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
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/util/dashbase"
)

// DashbaseSelectExec represents a DashbaseSelect executor.
type DashbaseSelectExec struct {
	baseExecutor

	TableInfo *model.TableInfo
	SQL       string

	fetched bool
	rows    []*Row
	cursor  int
}

// Next implements Execution Next interface.
func (e *DashbaseSelectExec) Next() (*Row, error) {
	if e.rows == nil {
		err := e.fetchAll()
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	if e.cursor >= len(e.rows) {
		return nil, nil
	}
	row := e.rows[e.cursor]
	e.cursor++
	return row, nil
}

func (e *DashbaseSelectExec) fetchAll() error {
	client := dashbase.ApiClient{
		Host: e.TableInfo.DashbaseConnection.ProxyHostname,
		Port: e.TableInfo.DashbaseConnection.ProxyPort,
	}
	result, err := client.Query(e.SQL)
	if err != nil {
		return errors.Trace(err)
	}

	fmt.Println("Dashbase Fetch All Result ===============")
	fmt.Println(result)

	return nil
}
