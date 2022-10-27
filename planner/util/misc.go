package util

import (
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/util/ranger"
)

// CloneExprs uses Expression.Clone to clone a slice of Expression.
func CloneExprs(exprs []expression.Expression) []expression.Expression {
	cloned := make([]expression.Expression, 0, len(exprs))
	for _, e := range exprs {
		cloned = append(cloned, e.Clone())
	}
	return cloned
}

// CloneCols uses (*Column).Clone to clone a slice of *Column.
func CloneCols(cols []*expression.Column) []*expression.Column {
	cloned := make([]*expression.Column, 0, len(cols))
	for _, c := range cols {
		cloned = append(cloned, c.Clone().(*expression.Column))
	}
	return cloned
}

// CloneColInfos uses (*ColumnInfo).Clone to clone a slice of *ColumnInfo.
func CloneColInfos(cols []*model.ColumnInfo) []*model.ColumnInfo {
	cloned := make([]*model.ColumnInfo, 0, len(cols))
	for _, c := range cols {
		cloned = append(cloned, c.Clone())
	}
	return cloned
}

// CloneRanges uses (*Range).Clone to clone a slice of *Range.
func CloneRanges(ranges []*ranger.Range) []*ranger.Range {
	cloned := make([]*ranger.Range, 0, len(ranges))
	for _, r := range ranges {
		cloned = append(cloned, r.Clone())
	}
	return cloned
}
