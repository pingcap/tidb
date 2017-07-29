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
)

// ExplainInfo implements the Expression interface.
func (expr *ScalarFunction) ExplainInfo() string {
	buffer := bytes.NewBufferString(fmt.Sprintf("%s(", expr.FuncName.L))
	for i, arg := range expr.GetArgs() {
		buffer.WriteString(fmt.Sprintf("%s", arg.ExplainInfo()))
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
	valStr, err := expr.Value.ToString()
	if err != nil {
		valStr = "not recognized const value"
	}
	return valStr
}

// ExplainAggFunc generates explain information for a aggregation function.
func ExplainAggFunc(agg AggregationFunction) string {
	buffer := bytes.NewBufferString(fmt.Sprintf("%s(", agg.GetName()))
	if agg.IsDistinct() {
		buffer.WriteString("distinct ")
	}
	for i, arg := range agg.GetArgs() {
		buffer.WriteString(arg.ExplainInfo())
		if i+1 < len(agg.GetArgs()) {
			buffer.WriteString(", ")
		}
	}
	buffer.WriteString(")")
	return buffer.String()
}
