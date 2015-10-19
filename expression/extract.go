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
	"strings"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/types"
)

// Extract is for time extract function.
// See: https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_extract
type Extract struct {
	Unit string
	Date Expression
}

// Clone implements the Expression Clone interface.
func (e *Extract) Clone() Expression {
	n := *e
	return &n
}

// Eval implements the Expression Eval interface.
func (e *Extract) Eval(ctx context.Context, args map[interface{}]interface{}) (interface{}, error) {
	v, err := e.Date.Eval(ctx, args)
	if v == nil || err != nil {
		return nil, errors.Trace(err)
	}

	f := types.NewFieldType(mysql.TypeDatetime)
	f.Decimal = mysql.MaxFsp

	v, err = types.Convert(v, f)
	if v == nil || err != nil {
		return nil, errors.Trace(err)
	}

	t, ok := v.(mysql.Time)
	if !ok {
		return nil, errors.Errorf("need time type, but got %T", v)
	}

	n, err1 := mysql.ExtractTimeNum(e.Unit, t)
	if err1 != nil {
		return nil, errors.Trace(err1)
	}

	return n, nil
}

// IsStatic implements the Expression IsStatic interface.
func (e *Extract) IsStatic() bool {
	return e.Date.IsStatic()
}

// String implements the Expression String interface.
func (e *Extract) String() string {
	return fmt.Sprintf("EXTRACT(%s FROM %s)", strings.ToUpper(e.Unit), e.Date)
}

// Accept implements the Visitor Accept interface.
func (e *Extract) Accept(v Visitor) (Expression, error) {
	return v.VisitExtract(e)
}
