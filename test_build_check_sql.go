package main

import (
	"fmt"

	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
)

func main() {
	// Test buildCheckSQLFromModifyColumn function
	fmt.Println("Testing buildCheckSQLFromModifyColumn function...")

	// Create a test table info
	tableInfo := &model.TableInfo{
		Name: ast.NewCIStr("test_table"),
	}

	// Test case 1: BIGINT to INT (should generate check SQL)
	oldCol1 := &model.ColumnInfo{
		Name:      ast.NewCIStr("col1"),
		FieldType: types.FieldType{},
	}
	oldCol1.SetType(mysql.TypeLonglong) // BIGINT

	changingCol1 := &model.ColumnInfo{
		Name:      ast.NewCIStr("col1"),
		FieldType: types.FieldType{},
	}
	changingCol1.SetType(mysql.TypeLong) // INT

	sql1 := ddl.BuildCheckSQLFromModifyColumn(tableInfo, oldCol1, changingCol1)
	fmt.Printf("Test 1 (BIGINT to INT): %s\n", sql1)

	// Test case 2: INT to BIGINT (should not generate check SQL)
	oldCol2 := &model.ColumnInfo{
		Name:      ast.NewCIStr("col2"),
		FieldType: types.FieldType{},
	}
	oldCol2.SetType(mysql.TypeLong) // INT

	changingCol2 := &model.ColumnInfo{
		Name:      ast.NewCIStr("col2"),
		FieldType: types.FieldType{},
	}
	changingCol2.SetType(mysql.TypeLonglong) // BIGINT

	sql2 := ddl.BuildCheckSQLFromModifyColumn(tableInfo, oldCol2, changingCol2)
	fmt.Printf("Test 2 (INT to BIGINT): %s\n", sql2)

	// Test case 3: INT to INT UNSIGNED (should generate check SQL)
	oldCol3 := &model.ColumnInfo{
		Name:      ast.NewCIStr("col3"),
		FieldType: types.FieldType{},
	}
	oldCol3.SetType(mysql.TypeLong) // INT

	changingCol3 := &model.ColumnInfo{
		Name:      ast.NewCIStr("col3"),
		FieldType: types.FieldType{},
	}
	changingCol3.SetType(mysql.TypeLong) // INT
	changingCol3.AddFlag(mysql.UnsignedFlag)

	sql3 := ddl.BuildCheckSQLFromModifyColumn(tableInfo, oldCol3, changingCol3)
	fmt.Printf("Test 3 (INT to INT UNSIGNED): %s\n", sql3)

	fmt.Println("Testing completed!")
}
