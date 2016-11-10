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
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/util/types"
)

// ExplainExec represents an explain executor.
// See https://dev.mysql.com/doc/refman/5.7/en/explain-output.html
type ExplainExec struct {
	StmtPlan  plan.Plan
	schema    expression.Schema
	evaluated bool
}

// Schema implements the Executor Schema interface.
func (e *ExplainExec) Schema() expression.Schema {
	return e.schema
}

// Next implements Execution Next interface.
func (e *ExplainExec) Next() (*Row, error) {
	if e.evaluated {
		return nil, nil
	}
	e.evaluated = true
	explain, err := json.MarshalIndent(e.StmtPlan, "", "    ")
	if err != nil {
		return nil, errors.Trace(err)
	}
	row := &Row{
		Data: types.MakeDatums("EXPLAIN", string(explain)),
	}
	return row, nil
}

// Close implements the Executor Close interface.
func (e *ExplainExec) Close() error {
	return nil
}
