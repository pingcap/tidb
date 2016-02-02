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
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/util/codec"
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
	switch strings.ToUpper(tableName) {
	case TableSetupActors:
	case TableSetupObjects:
	case TableSetupInstruments:
	case TableSetupConsumers:
	case TableSetupTimers:
	case TableStmtsCurrent:
	case TableStmtsHistory:
	case TableStmtsHistoryLong:
	case TablePreparedStmtsInstances:
	case TableTransCurrent:
	case TableTransHistory:
	case TableTransHistoryLong:
	case TableStagesCurrent:
	case TableStagesHistory:
	case TableStagesHistoryLong:
	default:
		return nil, errors.Errorf("table PERFORMANCE_SCHEMA.%s does not exist", tableName)
	}

	isp := &perfSchemaPlan{
		tableName: strings.ToUpper(tableName),
		schema:    ps,
	}

	var v interface{} = isp
	return v.(plan.Plan), nil
}

// Explain implements plan.Plan Explain interface.
func (isp *perfSchemaPlan) Explain(w format.Formatter) {}

func (isp *perfSchemaPlan) fetchAll(tableName string) ([]*plan.Row, error) {
	switch strings.ToUpper(tableName) {
	case TableSetupActors:
		return fetchStore(isp.schema.setupActorsStore)
	case TableSetupObjects:
		return fetchStore(isp.schema.setupObjectsStore)
	case TableSetupInstruments:
		return fetchStore(isp.schema.setupInstrumentsStore)
	case TableSetupConsumers:
		return fetchStore(isp.schema.setupConsumersStore)
	case TableSetupTimers:
		return fetchStore(isp.schema.setupTimersStore)
	case TableStmtsCurrent:
		return fetchStore(isp.schema.stmtsCurrentStore)
	case TableStmtsHistory:
		return fetchStore(isp.schema.stmtsHistoryStore)
	case TableStmtsHistoryLong:
		return fetchStore(isp.schema.stmtsHistoryLongStore)
	case TablePreparedStmtsInstances:
		return fetchStore(isp.schema.preparedStmtsInstancesStore)
	case TableTransCurrent:
		return fetchStore(isp.schema.transCurrentStore)
	case TableTransHistory:
		return fetchStore(isp.schema.transHistoryStore)
	case TableTransHistoryLong:
		return fetchStore(isp.schema.transHistoryLongStore)
	case TableStagesCurrent:
		return fetchStore(isp.schema.stagesCurrentStore)
	case TableStagesHistory:
		return fetchStore(isp.schema.stagesHistoryStore)
	case TableStagesHistoryLong:
		return fetchStore(isp.schema.stagesHistoryLongStore)
	}
	return nil, nil
}

func fetchStore(store *leveldb.DB) (rows []*plan.Row, err error) {
	iter := store.NewIterator(nil, nil)
	for iter.Next() {
		value := iter.Value()
		record, err := codec.Decode(value)
		if err != nil {
			iter.Release()
			return nil, errors.Trace(err)
		}
		rows = append(rows, &plan.Row{Data: record})
	}
	iter.Release()
	err = iter.Error()
	if err != nil {
		return nil, errors.Trace(err)
	}
	return
}

// Filter implements plan.Plan Filter interface.
func (isp *perfSchemaPlan) Filter(ctx context.Context, expr expression.Expression) (p plan.Plan, filtered bool, err error) {
	return isp, false, nil
}

// GetFields implements plan.Plan GetFields interface, simulates MySQL's output.
func (isp *perfSchemaPlan) GetFields() (rfs []*field.ResultField) {
	switch strings.ToUpper(isp.tableName) {
	case TableSetupActors:
		return isp.getTableFields(TableSetupActors, ColumnSetupActors)
	case TableSetupObjects:
		return isp.getTableFields(TableSetupObjects, ColumnSetupObjects)
	case TableSetupInstruments:
		return isp.getTableFields(TableSetupInstruments, ColumnSetupInstruments)
	case TableSetupConsumers:
		return isp.getTableFields(TableSetupConsumers, ColumnSetupConsumers)
	case TableSetupTimers:
		return isp.getTableFields(TableSetupTimers, ColumnSetupTimers)
	case TableStmtsCurrent:
		return isp.getTableFields(TableStmtsCurrent, ColumnStmtsCurrent)
	case TableStmtsHistory:
		return isp.getTableFields(TableStmtsHistory, ColumnStmtsHistory)
	case TableStmtsHistoryLong:
		return isp.getTableFields(TableStmtsHistoryLong, ColumnStmtsHistoryLong)
	case TablePreparedStmtsInstances:
		return isp.getTableFields(TablePreparedStmtsInstances, ColumnPreparedStmtsInstances)
	case TableTransCurrent:
		return isp.getTableFields(TableTransCurrent, ColumnTransCurrent)
	case TableTransHistory:
		return isp.getTableFields(TableTransHistory, ColumnTransHistory)
	case TableTransHistoryLong:
		return isp.getTableFields(TableTransHistoryLong, ColumnTransHistoryLong)
	case TableStagesCurrent:
		return isp.getTableFields(TableStagesCurrent, ColumnStagesCurrent)
	case TableStagesHistory:
		return isp.getTableFields(TableStagesHistory, ColumnStagesHistory)
	case TableStagesHistoryLong:
		return isp.getTableFields(TableStagesHistoryLong, ColumnStagesHistoryLong)
	}
	return nil
}

func (isp *perfSchemaPlan) getTableFields(tableName string, columnNames []string) (rfs []*field.ResultField) {
	for i := 0; i < len(columnNames); i++ {
		rfs = append(rfs, isp.schema.fields[columnName{tableName, columnNames[i]}])
	}
	return rfs
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
