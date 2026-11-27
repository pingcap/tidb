// Copyright 2018 PingCAP, Inc.
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

package expression

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
)

// ParseSimpleExprWithTableInfo now is only used by some external repo like tiflow to make sure they are not break.
// Deprecated: please use ParseSimpleExpr instead.
func ParseSimpleExprWithTableInfo(ctx BuildContext, exprStr string, tableInfo *model.TableInfo) (Expression, error) {
	return ParseSimpleExpr(ctx, exprStr, WithTableInfo("", tableInfo))
}

// ParseSimpleExpr parses simple expression string to Expression.
// The expression string must only reference the column in table Info.
func ParseSimpleExpr(ctx BuildContext, exprStr string, opts ...BuildOption) (Expression, error) {
	if exprStr == "" {
		intest.Assert(false)
		// This should never happen. Return a clear error message in case we have some unexpected bugs.
		return nil, errors.New("expression should not be an empty string")
	}
	exprStr = "select " + exprStr
	var stmts []ast.StmtNode
	var err error
	var warns []error
	if p, ok := ctx.(sqlexec.SQLParser); ok {
		stmts, warns, err = p.ParseSQL(context.Background(), exprStr)
	} else {
		stmts, warns, err = parser.New().ParseSQL(exprStr)
	}
	for _, warn := range warns {
		ctx.GetEvalCtx().AppendWarning(util.SyntaxWarn(warn))
	}

	if err != nil {
		return nil, errors.Trace(err)
	}
	expr := stmts[0].(*ast.SelectStmt).Fields.Fields[0].Expr
	return BuildSimpleExpr(ctx, expr, opts...)
}

// FindFieldName finds the column name from NameSlice.
func FindFieldName(names types.NameSlice, astCol *ast.ColumnName) (int, error) {
	return findFieldName(names, astCol, nil, nil)
}

// findFieldName implements FindFieldName. When next is nil it scans all names.
// Otherwise it visits only the positions in names whose ColName.L equals
// astCol.Name.L, in ascending order, starting at first[astCol.Name.L] and
// following next. Positions are visited in the same order as the full scan and
// the skipped positions are exactly those the full scan skips on the column
// name, so both modes return the same result.
func findFieldName(names types.NameSlice, astCol *ast.ColumnName, first map[string]int, next []int) (int, error) {
	dbName, tblName, colName := astCol.Schema.L, astCol.Table.L, astCol.Name.L
	idx := -1

	i, ok := 0, true
	if next != nil {
		i, ok = first[colName]
	}
	for ok && i < len(names) {
		name := names[i]
		cur := i
		if next != nil {
			i = next[i]
			ok = i >= 0
		} else {
			i++
		}
		// Early continue if not usable or column name doesn't match
		if name.NotExplicitUsable || name.ColName.L != colName {
			continue
		}

		// Check database and table name match
		if (dbName == "" || dbName == name.DBName.L) &&
			(tblName == "" || tblName == name.TblName.L) {
			if idx != -1 {
				// Do not allow multiple non-redundant columns with the same name.
				if names[idx].Redundant || name.Redundant {
					if !name.Redundant {
						idx = cur
					}
					continue
				}
				return -1, errNonUniq.GenWithStackByArgs(astCol.String(), "field list")
			}
			idx = cur
		}
	}

	return idx, nil
}

// FieldNameIndex indexes a NameSlice by lower-case column name, so that
// resolving many column references against a wide NameSlice (for example the
// thousands of columns produced by expanding SELECT * on a wide table) does not
// scan the whole slice for every reference.
type FieldNameIndex struct {
	names types.NameSlice
	// first maps a lower-case column name to its first position in names.
	first map[string]int
	// next[i] is the next position after i with the same column name, or -1.
	next []int
}

// NewFieldNameIndex builds a FieldNameIndex over names. The index is only valid
// while names is not modified; use Covers to check that it still applies.
func NewFieldNameIndex(names types.NameSlice) *FieldNameIndex {
	first := make(map[string]int, len(names))
	next := make([]int, len(names))
	// Walk backwards so that first ends up holding the lowest position of each
	// name and next links the positions in ascending order.
	for i := len(names) - 1; i >= 0; i-- {
		colName := names[i].ColName.L
		if j, ok := first[colName]; ok {
			next[i] = j
		} else {
			next[i] = -1
		}
		first[colName] = i
	}
	return &FieldNameIndex{names: names, first: first, next: next}
}

// Covers reports whether names is the same slice (same backing array and
// length) that the index was built from.
func (idx *FieldNameIndex) Covers(names types.NameSlice) bool {
	return idx != nil && len(names) == len(idx.names) && len(names) > 0 && &names[0] == &idx.names[0]
}

// FindFieldName returns the same result as FindFieldName on the indexed NameSlice.
func (idx *FieldNameIndex) FindFieldName(astCol *ast.ColumnName) (int, error) {
	return findFieldName(idx.names, astCol, idx.first, idx.next)
}

// FindFieldNameIdxByColName finds the index of corresponding name in the given slice. -1 for not found.
func FindFieldNameIdxByColName(names []*types.FieldName, colName string) int {
	for i, name := range names {
		if name.ColName.L == colName {
			return i
		}
	}
	return -1
}
