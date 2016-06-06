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

package evaluator

import (
	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/util/types"
)

// See https://dev.mysql.com/doc/refman/5.7/en/control-flow-functions.html#function_if
func builtinIf(args []types.Datum, _ context.Context) (d types.Datum, err error) {
	// if(expr1, expr2, expr3)
	// if expr1 is true, return expr2, otherwise, return expr3
	v1 := args[0]
	v2 := args[1]
	v3 := args[2]

	if v1.Kind() == types.KindNull {
		return v3, nil
	}

	b, err := v1.ToBool()
	if err != nil {
		d := types.Datum{}
		return d, errors.Trace(err)
	}

	// TODO: check return type, must be numeric or string
	if b == 1 {
		return v2, nil
	}

	return v3, nil
}

// See https://dev.mysql.com/doc/refman/5.7/en/control-flow-functions.html#function_ifnull
func builtinIfNull(args []types.Datum, _ context.Context) (d types.Datum, err error) {
	// ifnull(expr1, expr2)
	// if expr1 is not null, return expr1, otherwise, return expr2
	v1 := args[0]
	v2 := args[1]

	if v1.Kind() != types.KindNull {
		return v1, nil
	}

	return v2, nil
}

// See https://dev.mysql.com/doc/refman/5.7/en/control-flow-functions.html#function_nullif
func builtinNullIf(args []types.Datum, _ context.Context) (d types.Datum, err error) {
	// nullif(expr1, expr2)
	// returns null if expr1 = expr2 is true, otherwise returns expr1
	v1 := args[0]
	v2 := args[1]

	if v1.Kind() == types.KindNull || v2.Kind() == types.KindNull {
		return v1, nil
	}

	if n, err1 := v1.CompareDatum(v2); err1 != nil || n == 0 {
		d := types.Datum{}
		return d, errors.Trace(err1)
	}

	return v1, nil
}
