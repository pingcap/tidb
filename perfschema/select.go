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

package perfschema

import (
	"strings"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/field"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/util/format"
	"github.com/syndtr/goleveldb/leveldb"
)

// perfSchemaPlan handles performance_schema query, simulates the behavior of
// MySQL.
type perfSchemaPlan struct {
	tableName string
	schema    *perfSchema
	rows      []*plan.Row
	cursor    int
}

var _ = (*perfSchemaPlan)(nil)

// NewPerfSchemaPlan returns new PerfSchemaPlan instance, and checks if the
// given table name is valid.
func (ps *perfSchema) NewPerfSchemaPlan(tableName string) (plan.Plan, error) {
	for _, t := range PerfSchemaTables {
		if strings.EqualFold(t, tableName) {
			isp := &perfSchemaPlan{
				tableName: strings.ToUpper(tableName),
				schema:    ps,
			}

			var v interface{} = isp
			return v.(plan.Plan), nil
		}
	}

	return nil, errors.Errorf("table PERFORMANCE_SCHEMA.%s does not exist", tableName)
}

// Explain implements plan.Plan Explain interface.
func (isp *perfSchemaPlan) Explain(w format.Formatter) {}

func (isp *perfSchemaPlan) fetchAll(tableName string) ([]*plan.Row, error) {
	for _, t := range PerfSchemaTables {
		if strings.EqualFold(t, tableName) {
			return fetchStore(isp.schema.stores[tableName], isp.schema.tables[tableName].Columns)
		}
	}

	return nil, nil
}

func fetchStore(store *leveldb.DB, cols []*model.ColumnInfo) ([]*plan.Row, error) {
	var rows []*plan.Row

	iter := store.NewIterator(nil, nil)
	defer iter.Release()

	for iter.Next() {
		value := iter.Value()
		record, err := decodeValue(value, cols)
		if err != nil {
			return nil, errors.Trace(err)
		}
		rows = append(rows, &plan.Row{Data: record})
	}

	err := iter.Error()
	if err != nil {
		return nil, errors.Trace(err)
	}
	return rows, nil
}

// Filter implements plan.Plan Filter interface.
func (isp *perfSchemaPlan) Filter(ctx context.Context, expr expression.Expression) (p plan.Plan, filtered bool, err error) {
	return isp, false, nil
}

// GetFields implements plan.Plan GetFields interface, simulates MySQL's output.
func (isp *perfSchemaPlan) GetFields() (rfs []*field.ResultField) {
	return isp.schema.fields[isp.tableName]
}

// Next implements plan.Plan Next interface.
func (isp *perfSchemaPlan) Next(ctx context.Context) (row *plan.Row, err error) {
	if isp.rows == nil {
		isp.rows, err = isp.fetchAll(isp.tableName)
	}
	if isp.cursor == len(isp.rows) {
		return
	}
	row = isp.rows[isp.cursor]
	isp.cursor++
	return
}

// Close implements plan.Plan Close interface.
func (isp *perfSchemaPlan) Close() error {
	isp.rows = nil
	isp.cursor = 0
	return nil
}
