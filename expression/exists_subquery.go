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

package expression

import (
	"fmt"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
)

// ExistsSubQuery is the expression for "exists (select ...)".
// https://dev.mysql.com/doc/refman/5.7/en/exists-and-not-exists-subqueries.html
type ExistsSubQuery struct {
	// Sel is the sub query.
	Sel SubQuery
}

// Clone implements the Expression Clone interface.
func (es *ExistsSubQuery) Clone() Expression {
	sel := es.Sel.Clone()
	return &ExistsSubQuery{Sel: sel.(SubQuery)}
}

// IsStatic implements the Expression IsStatic interface.
func (es *ExistsSubQuery) IsStatic() bool {
	return es.Sel.IsStatic()
}

// String implements the Expression String interface.
func (es *ExistsSubQuery) String() string {
	return fmt.Sprintf("EXISTS %s", es.Sel)
}

// Eval implements the Expression Eval interface.
func (es *ExistsSubQuery) Eval(ctx context.Context, args map[interface{}]interface{}) (interface{}, error) {
	if !es.Sel.UseOuterQuery() && es.Sel.Value() != nil {
		return es.Sel.Value().(bool), nil
	}

	rows, err := es.Sel.EvalRows(ctx, args, 1)
	if err != nil {
		return nil, errors.Trace(err)
	}

	es.Sel.SetValue(len(rows) > 0)
	return es.Sel.Value(), nil
}

// Accept implements Expression Accept interface.
func (es *ExistsSubQuery) Accept(v Visitor) (Expression, error) {
	return v.VisitExistsSubQuery(es)
}

// NewExistsSubQuery creates a ExistsSubQuery object.
func NewExistsSubQuery(sel SubQuery) *ExistsSubQuery {
	return &ExistsSubQuery{Sel: sel}
}
