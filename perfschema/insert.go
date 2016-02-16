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
	"sync/atomic"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/codec"
	"github.com/syndtr/goleveldb/leveldb"
)

// InsertValues is the rest part of insert/replace into statement.
type InsertValues struct {
	ColNames   []string
	Lists      [][]expression.Expression
	TableIdent table.Ident
	Setlist    []*expression.Assignment
}

// Simulate the behavior of an INSERT statement.
func (ps *perfSchema) ExecInsert(insertVals *InsertValues) error {
	name := strings.ToUpper(insertVals.TableIdent.Name.O)
	t := ps.getTable(name)
	if t == nil {
		return errors.Errorf("INSERT INTO %s: operation not permitted", insertVals.TableIdent)
	}

	cols, err := ps.getColumns(insertVals, t.Columns)
	if err != nil {
		return errors.Trace(err)
	}

	rows, err := ps.getRows(insertVals, t, cols)
	if err != nil {
		return errors.Trace(err)
	}

	return ps.addRecords(name, rows)
}

func (ps *perfSchema) addRecords(tbName string, rows [][]interface{}) error {
	var store *leveldb.DB
	var lastLsn uint64

	// Same as MySQL, we only support INSERT operations for setup_actors & setup_objects.
	switch tbName {
	case TableSetupActors:
		store = ps.stores[TableSetupActors]
		lastLsn = atomic.AddUint64(ps.lsns[TableSetupActors], uint64(len(rows)))
	case TableSetupObjects:
		store = ps.stores[TableSetupObjects]
		lastLsn = atomic.AddUint64(ps.lsns[TableSetupObjects], uint64(len(rows)))
	default:
		return errors.Errorf("INSERT INTO %s.%s: operation not permitted", Name, tbName)
	}

	batch := pool.Get().(*leveldb.Batch)
	defer func() {
		batch.Reset()
		pool.Put(batch)
	}()

	for i, row := range rows {
		lsn := lastLsn - uint64(len(rows)) + uint64(i)
		rawKey := []interface{}{uint64(lsn)}
		key, err := codec.EncodeKey(nil, rawKey...)
		if err != nil {
			return errors.Trace(err)
		}
		val, err := codec.EncodeValue(nil, row...)
		if err != nil {
			return errors.Trace(err)
		}
		batch.Put(key, val)
	}

	err := store.Write(batch, nil)
	return errors.Trace(err)
}

func checkValueCount(insertVals *InsertValues, insertValueCount, valueCount, num int, cols []*model.ColumnInfo) error {
	if insertValueCount != valueCount {
		// "insert into t values (), ()" is valid.
		// "insert into t values (), (1)" is not valid.
		// "insert into t values (1), ()" is not valid.
		// "insert into t values (1,2), (1)" is not valid.
		// So the value count must be same for all insert list.
		return errors.Errorf("Column count doesn't match value count at row %d", num+1)
	}
	if valueCount == 0 && len(insertVals.ColNames) > 0 {
		// "insert into t (c1) values ()" is not valid.
		return errors.Errorf("INSERT INTO %s: expected %d value(s), have %d", insertVals.TableIdent, len(insertVals.ColNames), 0)
	} else if valueCount > 0 && valueCount != len(cols) {
		return errors.Errorf("INSERT INTO %s: expected %d value(s), have %d", insertVals.TableIdent, len(cols), valueCount)
	}

	return nil
}

func fillRowData(t *model.TableInfo, cols []*model.ColumnInfo, vals []interface{}) ([]interface{}, error) {
	row := make([]interface{}, len(t.Columns))
	marked := make(map[int]struct{}, len(vals))
	for i, v := range vals {
		offset := cols[i].Offset
		row[offset] = v
		marked[offset] = struct{}{}
	}
	err := initDefaultValues(t, row, marked)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if err = castValues(row, cols); err != nil {
		return nil, errors.Trace(err)
	}
	if err = checkNotNulls(t.Columns, row); err != nil {
		return nil, errors.Trace(err)
	}
	return row, nil
}

func fillValueList(insertVals *InsertValues) error {
	if len(insertVals.Setlist) > 0 {
		if len(insertVals.Lists) > 0 {
			return errors.Errorf("INSERT INTO %s: set type should not use values", insertVals.TableIdent)
		}

		var l []expression.Expression
		for _, v := range insertVals.Setlist {
			l = append(l, v.Expr)
		}
		insertVals.Lists = append(insertVals.Lists, l)
	}

	return nil
}

func getColumnDefaultValues(cols []*model.ColumnInfo) (map[interface{}]interface{}, error) {
	defaultValMap := map[interface{}]interface{}{}
	for _, col := range cols {
		if value, ok, err := getColDefaultValue(col); ok {
			if err != nil {
				return nil, errors.Trace(err)
			}

			defaultValMap[col.Name.L] = value
		}
	}

	return defaultValMap, nil
}

// There are three types of insert statements:
// 1 insert ... values(...)  --> name type column
// 2 insert ... set x=y...   --> set type column
// 3 insert ... (select ..)  --> name type column
// See: https://dev.mysql.com/doc/refman/5.7/en/insert.html
func (ps *perfSchema) getColumns(insertVals *InsertValues, tableCols []*model.ColumnInfo) ([]*model.ColumnInfo, error) {
	var cols []*model.ColumnInfo
	var err error

	if len(insertVals.Setlist) > 0 {
		// Process `set` type column.
		columns := make([]string, 0, len(insertVals.Setlist))
		for _, v := range insertVals.Setlist {
			columns = append(columns, v.ColName)
		}

		cols, err = findCols(tableCols, columns)
		if err != nil {
			return nil, errors.Errorf("INSERT INTO %s: %s", insertVals.TableIdent, err)
		}

		if len(cols) == 0 {
			return nil, errors.Errorf("INSERT INTO %s: empty column", insertVals.TableIdent)
		}
	} else {
		// Process `name` type column.
		cols, err = findCols(tableCols, insertVals.ColNames)
		if err != nil {
			return nil, errors.Errorf("INSERT INTO %s: %s", insertVals.TableIdent, err)
		}

		// If cols are empty, use all columns instead.
		if len(cols) == 0 {
			cols = tableCols
		}
	}

	return cols, nil
}

func (ps *perfSchema) getRows(insertVals *InsertValues, t *model.TableInfo, cols []*model.ColumnInfo) (rows [][]interface{}, err error) {
	// process `insert|replace ... set x=y...`
	if err = fillValueList(insertVals); err != nil {
		return nil, errors.Trace(err)
	}

	evalMap, err := getColumnDefaultValues(cols)
	if err != nil {
		return nil, errors.Trace(err)
	}

	rows = make([][]interface{}, len(insertVals.Lists))
	for i, list := range insertVals.Lists {
		if err = checkValueCount(insertVals, len(insertVals.Lists[0]), len(list), i, cols); err != nil {
			return nil, errors.Trace(err)
		}

		vals := make([]interface{}, len(list))
		for j, expr := range list {
			// For "insert into t values (default)" Default Eval.
			evalMap[expression.ExprEvalDefaultName] = cols[j].Name.O

			vals[j], err = expr.Eval(nil, evalMap)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}

		rows[i], err = fillRowData(t, cols, vals)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return
}

func (ps *perfSchema) getTable(tbName string) *model.TableInfo {
	// Same as MySQL, we only support INSERT operations for setup_actors & setup_objects.
	switch tbName {
	case TableSetupActors:
	case TableSetupObjects:
	default:
		return nil
	}
	return ps.tables[tbName]
}

func initDefaultValues(t *model.TableInfo, row []interface{}, marked map[int]struct{}) error {
	var defaultValueCols []*model.ColumnInfo
	for i, c := range t.Columns {
		if row[i] != nil {
			// Column value is not nil, continue.
			continue
		}
		// If the nil value is evaluated in insert list, we will use nil.
		if _, ok := marked[i]; ok {
			continue
		}

		var value interface{}
		value, _, err := getColDefaultValue(c)
		if err != nil {
			return errors.Trace(err)
		}
		row[i] = value
		defaultValueCols = append(defaultValueCols, c)
	}

	if err := castValues(row, defaultValueCols); err != nil {
		return errors.Trace(err)
	}
	return nil
}
