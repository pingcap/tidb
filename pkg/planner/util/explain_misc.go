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
func ExplainPartitionBy(ctx expression.EvalContext, buffer *bytes.Buffer, partitionBy []property.SortItem, normalized bool) *bytes.Buffer {
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
