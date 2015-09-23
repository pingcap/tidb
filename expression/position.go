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

var (
	_ Expression = (*Position)(nil)
)

// Position is the expression for order by and group by position.
// MySQL use position expression started from 1, it looks a little confused inner.
// maybe later we will use 0 at first.
type Position struct {
	// N is the position, started from 1 now.
	N int
	// Name is the corresponding field name if we want better format and explain instead of position.
	Name string
}

// Clone implements the Expression Clone interface.
func (p *Position) Clone() Expression {
	newP := *p
	return &newP
}

// IsStatic implements the Expression IsStatic interface, always returns false.
func (p *Position) IsStatic() bool {
	return false
}

// String implements the Expression String interface.
func (p *Position) String() string {
	if len(p.Name) > 0 {
		return p.Name
	}
	return fmt.Sprintf("%d", p.N)
}

// Eval implements the Expression Eval interface.
func (p *Position) Eval(ctx context.Context, args map[interface{}]interface{}) (v interface{}, err error) {
	f, ok := args[ExprEvalPositionFunc]
	if !ok {
		return nil, errors.Errorf("no eval position function")
	}
	got, ok := f.(func(int) (interface{}, error))
	if !ok {
		return nil, errors.Errorf("invalid eval position function format")
	}

	return got(p.N)
}

// Accept implements Expression Accept interface.
func (p *Position) Accept(v Visitor) (Expression, error) {
	return v.VisitPosition(p)
}
