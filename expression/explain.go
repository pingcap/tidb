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

package expression

import (
	"bytes"
	"fmt"

	"github.com/pingcap/tidb/types"
)

// ExplainInfo implements the Expression interface.
func (expr *ScalarFunction) ExplainInfo() string {
	buffer := bytes.NewBufferString(fmt.Sprintf("%s(", expr.FuncName.L))
	for i, arg := range expr.GetArgs() {
		buffer.WriteString(arg.ExplainInfo())
		if i+1 < len(expr.GetArgs()) {
			buffer.WriteString(", ")
		}
	}
	buffer.WriteString(")")
	return buffer.String()
}

// ExplainInfo implements the Expression interface.
func (expr *Column) ExplainInfo() string {
	return expr.String()
}

// ExplainInfo implements the Expression interface.
func (expr *Constant) ExplainInfo() string {
	dt, err := expr.Eval(nil)
	if err != nil {
		if expr.Value.Kind() == types.KindNull {
			return "null"
		}
		return "not recognized const vanue"
	}
	valStr, err := dt.ToString()
	if err != nil {
		if expr.Value.Kind() == types.KindNull {
			return "null"
		}
		return "not recognized const vanue"
	}
	return valStr
}

// ExplainExpressionList generates explain information for a list of expressions.
func ExplainExpressionList(exprs []Expression) []byte {
	buffer := bytes.NewBufferString("")
	for i, expr := range exprs {
		buffer.WriteString(expr.ExplainInfo())
		if i+1 < len(exprs) {
			buffer.WriteString(", ")
		}
	}
	return buffer.Bytes()
}

// ExplainColumnList generates explain information for a list of columns.
func ExplainColumnList(cols []*Column) []byte {
	buffer := bytes.NewBufferString("")
	for i, col := range cols {
		buffer.WriteString(col.ExplainInfo())
		if i+1 < len(cols) {
			buffer.WriteString(", ")
		}
	}
	return buffer.Bytes()
}
