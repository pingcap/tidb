package core

import (
	"fmt"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"reflect"
	"testing"
)

func TestAsName(t *testing.T) {
	p := parser.New()
	stmt, err := p.ParseOneStmt("select * from t as t1", "", "")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(reflect.TypeOf(stmt.(*ast.SelectStmt).From.TableRefs.Left.(*ast.TableSource).Source))
}
