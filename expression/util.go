package expression

import "github.com/pingcap/tidb/ast"

// ExtractColumns extracts all columns from an expression.
func ExtractColumns(expr Expression) (cols []*Column) {
	switch v := expr.(type) {
	case *Column:
		return []*Column{v}
	case *ScalarFunction:
		for _, arg := range v.Args {
			cols = append(cols, ExtractColumns(arg)...)
		}
	}
	return
}

// ColumnSubstitute substitutes the columns in filter to expressions in select fields.
// e.g. select * from (select b as a from t) k where a < 10 => select * from (select b as a from t where b < 10) k.
func ColumnSubstitute(expr Expression, schema Schema, newExprs []Expression) Expression {
	switch v := expr.(type) {
	case *Column:
		id := schema.GetIndex(v)
		if id == -1 {
			return v
		}
		return newExprs[id].Clone()
	case *ScalarFunction:
		if v.FuncName.L == ast.Cast {
			newFunc := v.Clone().(*ScalarFunction)
			newFunc.Args[0] = ColumnSubstitute(newFunc.Args[0], schema, newExprs)
			return newFunc
		}
		newArgs := make([]Expression, 0, len(v.Args))
		for _, arg := range v.Args {
			newArgs = append(newArgs, ColumnSubstitute(arg, schema, newExprs))
		}
		fun, _ := NewFunction(v.FuncName.L, v.RetType, newArgs...)
		return fun
	}
	return expr
}
