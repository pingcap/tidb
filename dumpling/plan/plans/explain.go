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

package plans

import (
	"bytes"

	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/field"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/stmt"
	"github.com/pingcap/tidb/util/format"
)

var _ plan.Plan = (*ExplainDefaultPlan)(nil)

// ExplainDefaultPlan executes the explain statement, and provides debug
// infomations.
type ExplainDefaultPlan struct {
	S stmt.Statement
}

// Do returns explain result lines.
func (r *ExplainDefaultPlan) Do(ctx context.Context, f plan.RowIterFunc) error {
	var buf bytes.Buffer
	switch x := r.S.(type) {
	default:
		w := format.IndentFormatter(&buf, "│   ")
		x.Explain(ctx, w)
	}

	a := bytes.Split(buf.Bytes(), []byte{'\n'})
	for _, v := range a[:len(a)-1] {
		if more, err := f(nil, []interface{}{string(v)}); !more || err != nil {
			return err
		}
	}
	return nil
}

// Explain implements the plan.Plan Explain interface.
func (r *ExplainDefaultPlan) Explain(w format.Formatter) {
	// Do nothing
}

// GetFields implements the plan.Plan GetFields interface.
func (r *ExplainDefaultPlan) GetFields() []*field.ResultField {
	return []*field.ResultField{&field.ResultField{Name: ""}}
}

// Filter implements the plan.Plan Filter interface.
func (r *ExplainDefaultPlan) Filter(ctx context.Context, expr expression.Expression) (plan.Plan, bool, error) {
	return r, false, nil
}
