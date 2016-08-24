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

package executor

import (
	"encoding/json"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/util/types"
)

var ExplainTestMode = false

// ExplainExec represents an explain executor.
// See https://dev.mysql.com/doc/refman/5.7/en/explain-output.html
type ExplainExec struct {
	StmtPlan  plan.Plan
	schema    expression.Schema
	evaluated bool
}

// Schema implements Executor Schema interface.
func (e *ExplainExec) Schema() expression.Schema {
	return e.schema
}

// Fields implements Executor Fields interface.
func (e *ExplainExec) Fields() []*ast.ResultField {
	return nil
}

// Next implements Execution Next interface.
func (e *ExplainExec) Next() (*Row, error) {
	if e.evaluated {
		return nil, nil
	}
	e.evaluated = true
	var explain []byte
	var err error
	if ExplainTestMode {
		explain, err = json.Marshal(e.StmtPlan)
	} else {
		explain, err = json.MarshalIndent(e.StmtPlan, "", "    ")
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	row := &Row{
		Data: types.MakeDatums("EXPLAIN", string(explain)),
	}
	return row, nil
}

// Close implements Executor Close interface.
func (e *ExplainExec) Close() error {
	return nil
}
