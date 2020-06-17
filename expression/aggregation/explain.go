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

package aggregation

import (
	"bytes"
	"fmt"

	"github.com/pingcap/parser/ast"
)

// ExplainAggFunc generates explain information for a aggregation function.
func ExplainAggFunc(agg *AggFuncDesc) string {
	var buffer bytes.Buffer
	fmt.Fprintf(&buffer, "%s(", agg.Name)
	if agg.HasDistinct {
		buffer.WriteString("distinct ")
	}
	for i, arg := range agg.Args {
		if agg.Name == ast.AggFuncGroupConcat && i == len(agg.Args)-1 {
			if len(agg.OrderByItems) > 0 {
				buffer.WriteString(" order by ")
				for i, item := range agg.OrderByItems {
					if item.Desc {
						fmt.Fprintf(&buffer, "%s desc", item.Expr.ExplainInfo())
					} else {
						fmt.Fprintf(&buffer, "%s", item.Expr.ExplainInfo())
					}

					if i+1 < len(agg.OrderByItems) {
						buffer.WriteString(", ")
					}
				}
			}
			buffer.WriteString(" separator ")
		} else if i != 0 {
			buffer.WriteString(", ")
		}
		buffer.WriteString(arg.ExplainInfo())
	}
	buffer.WriteString(")")
	return buffer.String()
}
