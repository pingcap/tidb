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
	"github.com/pingcap/tidb/context"
)

// DateAdd is for time date_add function.
// See: https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_date-add
type DateAdd struct {
	Unit     string
	Date     Expression
	Interval Expression
}

// Clone implements the Expression Clone interface.
func (e *DateAdd) Clone() Expression {
	n := *e
	return &n
}

// Eval implements the Expression Eval interface.
func (e *DateAdd) Eval(ctx context.Context, args map[interface{}]interface{}) (interface{}, error) {
	// TODO
	return nil, nil
}

// IsStatic implements the Expression IsStatic interface.
func (e *DateAdd) IsStatic() bool {
	return e.Date.IsStatic() && e.Interval.IsStatic()
}

// String implements the Expression String interface.
func (e *DateAdd) String() string {
	// TODO
	return ""
}

// Accept implements the Visitor Accept interface.
func (e *DateAdd) Accept(v Visitor) (Expression, error) {
	return v.VisitDateAdd(e)
}
