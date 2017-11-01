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
	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/types"
)

// HashSemiJoinExec implements the hash join algorithm for semi join.
type HashSemiJoinExec struct {
	hashTable    map[string][]Row
	smallHashKey []*expression.Column
	bigHashKey   []*expression.Column
	smallExec    Executor
	bigExec      Executor
	prepared     bool
	ctx          context.Context
	smallFilter  expression.CNFExprs
	bigFilter    expression.CNFExprs
	otherFilter  expression.CNFExprs
	schema       *expression.Schema
	resultRows   []Row
	// auxMode is a mode that the result row always returns with an extra column which stores a boolean
	// or NULL value to indicate if this row is matched.
	auxMode           bool
	smallTableHasNull bool
	// anti is true, semi join only output the unmatched row.
	anti bool
}

// Close implements the Executor Close interface.
func (e *HashSemiJoinExec) Close() error {
	e.hashTable = nil
	e.resultRows = nil
	return e.bigExec.Close()
}

// Open implements the Executor Open interface.
func (e *HashSemiJoinExec) Open() error {
	e.prepared = false
	e.smallTableHasNull = false
	e.hashTable = make(map[string][]Row)
	e.resultRows = make([]Row, 1)
	return errors.Trace(e.bigExec.Open())
}

// Schema implements the Executor Schema interface.
func (e *HashSemiJoinExec) Schema() *expression.Schema {
	return e.schema
}

// prepare runs the first time when 'Next' is called and it reads all data from the small table and stores
// them in a hash table.
func (e *HashSemiJoinExec) prepare() error {
	err := e.smallExec.Open()
	if err != nil {
		return errors.Trace(err)
	}
	defer terror.Call(e.smallExec.Close)
	e.hashTable = make(map[string][]Row)
	e.resultRows = make([]Row, 1)
	e.prepared = true
	for {
		row, err := e.smallExec.Next()
		if err != nil {
			return errors.Trace(err)
		}
		if row == nil {
			return nil
		}

		matched, err := expression.EvalBool(e.smallFilter, row, e.ctx)
		if err != nil {
			return errors.Trace(err)
		}
		if !matched {
			continue
		}
		hasNull, hashcode, err := getJoinKey(e.smallHashKey, row, make([]types.Datum, len(e.smallHashKey)), nil)
		if err != nil {
			return errors.Trace(err)
		}
		if hasNull {
			e.smallTableHasNull = true
			continue
		}
		if rows, ok := e.hashTable[string(hashcode)]; !ok {
			e.hashTable[string(hashcode)] = []Row{row}
		} else {
			e.hashTable[string(hashcode)] = append(rows, row)
		}
	}
}

func (e *HashSemiJoinExec) rowIsMatched(bigRow Row) (matched bool, hasNull bool, err error) {
	hasNull, hashcode, err := getJoinKey(e.bigHashKey, bigRow, make([]types.Datum, len(e.smallHashKey)), nil)
	if err != nil {
		return false, false, errors.Trace(err)
	}
	if hasNull {
		return false, true, nil
	}
	rows, ok := e.hashTable[string(hashcode)]
	if !ok {
		return
	}
	// match eq condition
	for _, smallRow := range rows {
		matchedRow := makeJoinRow(bigRow, smallRow)
		matched, err = expression.EvalBool(e.otherFilter, matchedRow, e.ctx)
		if err != nil {
			return false, false, errors.Trace(err)
		}
		if matched {
			return
		}
	}
	return
}

func (e *HashSemiJoinExec) fetchBigRow() (Row, bool, error) {
	for {
		bigRow, err := e.bigExec.Next()
		if err != nil {
			return nil, false, errors.Trace(err)
		}
		if bigRow == nil {
			return nil, false, errors.Trace(e.bigExec.Close())
		}

		matched, err := expression.EvalBool(e.bigFilter, bigRow, e.ctx)
		if err != nil {
			return nil, false, errors.Trace(err)
		}
		if matched {
			return bigRow, true, nil
		} else if e.auxMode {
			return bigRow, false, nil
		}
	}
}

func (e *HashSemiJoinExec) doJoin(bigRow Row, match bool) ([]Row, error) {
	if e.auxMode && !match {
		bigRow = append(bigRow, types.NewDatum(false))
		e.resultRows[0] = bigRow
		return e.resultRows, nil
	}
	matched, isNull, err := e.rowIsMatched(bigRow)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if !matched && e.smallTableHasNull {
		isNull = true
	}
	if e.anti && !isNull {
		matched = !matched
	}
	// For the auxMode subquery, we return the row with a Datum indicating if it's a match,
	// For the non-auxMode subquery, we return the matching row only.
	if e.auxMode {
		if isNull {
			bigRow = append(bigRow, types.NewDatum(nil))
		} else {
			bigRow = append(bigRow, types.NewDatum(matched))
		}
		matched = true
	}
	if matched {
		e.resultRows[0] = bigRow
		return e.resultRows, nil
	}
	return nil, nil
}

// Next implements the Executor Next interface.
func (e *HashSemiJoinExec) Next() (Row, error) {
	if !e.prepared {
		if err := e.prepare(); err != nil {
			return nil, errors.Trace(err)
		}
	}

	for {
		bigRow, match, err := e.fetchBigRow()
		if bigRow == nil || err != nil {
			return bigRow, errors.Trace(err)
		}
		resultRows, err := e.doJoin(bigRow, match)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if len(resultRows) > 0 {
			return resultRows[0], nil
		}
	}
}
