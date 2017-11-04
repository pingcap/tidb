// Copyright 2015 PingCAP, Inc.
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
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/util/types"
)

// Row represents a result set row, it may be returned from a table, a join, or a projection.
//
// The following cases will need store the handle information:
//
// If the top plan is update or delete, then every executor will need the handle.
// If there is an union scan, then the below scan plan must store the handle.
// If there is sort need in the double read, then the table scan of the double read must store the handle.
// If there is a select for update. then we need to store the handle until the lock plan. But if there is aggregation, the handle info can be removed.
// Otherwise the executor's returned rows don't need to store the handle information.
type Row = types.DatumRow

// Executor executes a query.
type Executor interface {
	Next() (Row, error)
	Close() error
	Open() error
	Schema() *expression.Schema
}

type baseExecutor struct {
	children []Executor
	ctx      context.Context
	schema   *expression.Schema
}

// Open implements the Executor Open interface.
func (e *baseExecutor) Open() error {
	for _, child := range e.children {
		err := child.Open()
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// Close implements the Executor Close interface.
func (e *baseExecutor) Close() error {
	for _, child := range e.children {
		err := child.Close()
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// Schema implements the Executor Schema interface.
func (e *baseExecutor) Schema() *expression.Schema {
	if e.schema == nil {
		return expression.NewSchema()
	}
	return e.schema
}

func newBaseExecutor(schema *expression.Schema, ctx context.Context, children ...Executor) baseExecutor {
	return baseExecutor{
		children: children,
		ctx:      ctx,
		schema:   schema,
	}
}

func init() {
	// While doing optimization in the plan package, we need to execute uncorrelated subquery,
	// but the plan package cannot import the executor package because of the dependency cycle.
	// So we assign a function implemented in the executor package to the plan package to avoid the dependency cycle.
	plan.EvalSubquery = func(p plan.PhysicalPlan, is infoschema.InfoSchema, ctx context.Context) (rows [][]types.Datum, err error) {
		err = ctx.ActivePendingTxn()
		if err != nil {
			return rows, errors.Trace(err)
		}
		e := &executorBuilder{is: is, ctx: ctx}
		exec := e.build(p)
		if e.err != nil {
			return rows, errors.Trace(err)
		}
		err = exec.Open()
		if err != nil {
			return rows, errors.Trace(err)
		}
		for {
			row, err := exec.Next()
			if err != nil {
				return rows, errors.Trace(err)
			}
			if row == nil {
				return rows, errors.Trace(exec.Close())
			}
			rows = append(rows, row)
		}
	}
}
