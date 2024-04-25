package core

import (
	"encoding/json"
	"github.com/pingcap/tidb/pkg/parser/ast"
)

type TableName struct {
	Schema string
	Table  string
}

type ColumnName struct {
	TableName
	Column string
}

type visitor struct {
	TableNames  map[TableName]bool
	TableAlias  map[TableName]string
	ColumnNames map[ColumnName]bool
}

func (v *visitor) Enter(in ast.Node) (ast.Node, bool) {
	if v.TableNames == nil {
		v.TableNames = make(map[TableName]bool)
	}
	if v.TableAlias == nil {
		v.TableAlias = make(map[TableName]string)
	}
	if v.ColumnNames == nil {
		v.ColumnNames = make(map[ColumnName]bool)
	}

	switch x := in.(type) {
	case *ast.TableName:
		v.TableNames[TableName{x.Schema.O, x.Name.O}] = true
	case *ast.TableSource:
		if x.AsName.L != "" {
			tbl, ok := x.Source.(*ast.TableName)
			if ok {
				v.TableAlias[TableName{tbl.Schema.O, tbl.Name.O}] = x.AsName.O
			} else {
				panic("TODO")
			}
		}
	case *ast.ColumnName:
		v.ColumnNames[ColumnName{TableName{x.Table.O, x.Name.O}, x.Name.O}] = true
	}
	return in, false
}

func (v *visitor) Leave(in ast.Node) (ast.Node, bool) {
	return in, true
}

type JSONOutput struct {
	TableNames []string `json:"table_names"`
	ColumnName []string `json:"column_names"`
}

func prepareForUnity(stmt ast.StmtNode) string {
	v := &visitor{}
	stmt.Accept(v)

	var j = JSONOutput{TableNames: []string{}, ColumnName: []string{}}
	for t := range v.TableNames {
		if alias, ok := v.TableAlias[t]; ok {
			t.Table = alias
		}
		j.TableNames = append(j.TableNames, t.Table)
	}
	for c := range v.ColumnNames {
		j.ColumnName = append(j.ColumnName, c.Column)
	}

	jsonData, err := json.Marshal(j)
	must(err)
	return string(jsonData)
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
