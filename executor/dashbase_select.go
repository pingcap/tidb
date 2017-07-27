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
	"encoding/json"
	"fmt"

	"strings"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/util/dashbase"
	"github.com/pingcap/tidb/util/types"
)

// DashbaseSelectExec represents a DashbaseSelect executor.
type DashbaseSelectExec struct {
	baseExecutor

	TableInfo       *model.TableInfo
	SrcColumns      []*dashbase.Column
	Lo2HiConverters []dashbase.Lo2HiConverter
	SQL             string

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

	for _, hit := range result.Hits {
		// recordString is json serialized record
		recordString := hit.Payload.Stored
		// recordRaw is the deserialized record
		recordRaw := make(map[string]interface{})
		// record is the key-upper-cased record
		record := make(map[string]interface{})
		err := json.Unmarshal([]byte(recordString), &recordRaw)
		if err != nil {
			return errors.Trace(fmt.Errorf("Failed to deserialize Dashbase record %s", recordString))
		}
		for key, value := range recordRaw {
			record[strings.ToLower(key)] = value
		}
		datums := make([]types.Datum, len(e.SrcColumns))
		for i, column := range e.SrcColumns {
			raw, ok := record[column.Name]
			if !ok {
				datums[i] = types.NewDatum(nil)
			} else {
				data := e.Lo2HiConverters[i](raw)
				datums[i] = types.NewDatum(data)
			}
		}
		e.rows = append(e.rows, &Row{Data: datums})
	}
	return nil
}
