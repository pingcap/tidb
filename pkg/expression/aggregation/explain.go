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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package aggregation

import (
	"bytes"
	"fmt"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
)

// ExplainAggFunc generates explain information for a aggregation function.
func ExplainAggFunc(ctx expression.EvalContext, agg *AggFuncDesc, normalized bool) string {
	var buffer bytes.Buffer
	showMode := false
	failpoint.Inject("show-agg-mode", func(v failpoint.Value) {
		if v.(bool) {
			showMode = true
		}
	})
	if showMode {
		fmt.Fprintf(&buffer, "%s(%s,", agg.Name, agg.Mode.ToString())
	} else {
		fmt.Fprintf(&buffer, "%s(", agg.Name)
	}

	if agg.HasDistinct {
		buffer.WriteString("distinct ")
	}
	for i, arg := range agg.Args {
		if agg.Name == ast.AggFuncGroupConcat && i == len(agg.Args)-1 {
			if len(agg.OrderByItems) > 0 {
				buffer.WriteString(" order by ")
				for i, item := range agg.OrderByItems {
					if item.Desc {
						if normalized {
							fmt.Fprintf(&buffer, "%s desc", item.Expr.ExplainNormalizedInfo())
						} else {
							fmt.Fprintf(&buffer, "%s desc", item.Expr.ExplainInfo(ctx))
						}
					} else {
						if normalized {
							fmt.Fprintf(&buffer, "%s", item.Expr.ExplainNormalizedInfo())
						} else {
							fmt.Fprintf(&buffer, "%s", item.Expr.ExplainInfo(ctx))
						}
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
		if normalized {
			buffer.WriteString(arg.ExplainNormalizedInfo())
		} else {
			buffer.WriteString(arg.ExplainInfo(ctx))
		}
	}
	buffer.WriteString(")")
	return buffer.String()
}
