// Copyright 2024 PingCAP, Inc.
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

package util

import (
	"bytes"
	"fmt"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/property"
)

// ExplainByItems generates explain information for ByItems.
func ExplainByItems(ctx expression.EvalContext, buffer *bytes.Buffer, byItems []*ByItems) *bytes.Buffer {
	for i, item := range byItems {
		if item.Desc {
			fmt.Fprintf(buffer, "%s:desc", item.Expr.ExplainInfo(ctx))
		} else {
			fmt.Fprintf(buffer, "%s", item.Expr.ExplainInfo(ctx))
		}

		if i+1 < len(byItems) {
			buffer.WriteString(", ")
		}
	}
	return buffer
}

// ExplainPartitionBy produce text for p.PartitionBy. Common for window functions and TopN.
func ExplainPartitionBy(ctx expression.EvalContext, buffer *bytes.Buffer,
	partitionBy []property.SortItem, normalized bool) *bytes.Buffer {
	if len(partitionBy) > 0 {
		buffer.WriteString("partition by ")
		for i, item := range partitionBy {
			fmt.Fprintf(buffer, "%s", item.Col.ColumnExplainInfo(ctx, normalized))
			if i+1 < len(partitionBy) {
				buffer.WriteString(", ")
			}
		}
	}
	return buffer
}
