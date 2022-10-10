// Copyright 2022 PingCAP, Inc.
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

package mutasql

import (
	"bytes"
	"fmt"

	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/format"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/tools/go-sqlancer/pkg/connection"
	"github.com/pingcap/tidb/tools/go-sqlancer/pkg/generator"
	"github.com/pingcap/tidb/tools/go-sqlancer/pkg/types"
	"go.uber.org/zap"
)

// Pool is a list of TestPool
type Pool = []TestCase

// TestCase is test cases
type TestCase struct {
	D             []*Dataset
	Q             ast.Node
	Mutable       bool // TODO: useful?
	BeforeInsert  []ast.Node
	AfterInsert   []ast.Node // i.e. before checking SQL
	CleanUp       []ast.Node // such as `ROLLBACK` or `COMMIT`
	Result        []connection.QueryItems
	IsResultReady bool // Result contains value through execution
	// will NOT be executed as some SQLs before were failed
}

const indentLevel1 = "  "
const indentLevel2 = "    "
const indentLevel3 = "      "
const indentLevel4 = "        "

func (t *TestCase) String() string {
	str := "TestCase struct:\n"
	str += indentLevel1 + "Query:\n"
	str += indentLevel2 + restoreWithPanic(t.Q) + "\n"
	str += indentLevel1 + "Before Insert:\n"
	for _, n := range t.BeforeInsert {
		str += indentLevel2 + restoreWithPanic(n) + "\n"
	}
	str += indentLevel1 + "After Insert:\n"
	for _, n := range t.AfterInsert {
		str += indentLevel2 + restoreWithPanic(n) + "\n"
	}
	str += indentLevel1 + "Clean Up:\n"
	for _, n := range t.CleanUp {
		str += indentLevel2 + restoreWithPanic(n) + "\n"
	}
	str += indentLevel1 + "Dataset:\n"
	for _, d := range t.D {
		str += d.String()
	}
	return str
}

// Clone dataset also
// but do not clone Result
func (t *TestCase) Clone() TestCase {
	newTestCase := TestCase{
		D:             make([]*Dataset, 0),
		Q:             cloneNode(t.Q),
		Mutable:       t.Mutable,
		Result:        make([]connection.QueryItems, 0),
		IsResultReady: false,
	}
	for _, i := range t.D {
		d := i.Clone()
		newTestCase.D = append(newTestCase.D, &d)
	}
	for _, i := range t.BeforeInsert {
		newTestCase.BeforeInsert = append(newTestCase.BeforeInsert, cloneNode(i))
	}
	for _, i := range t.AfterInsert {
		newTestCase.AfterInsert = append(newTestCase.AfterInsert, cloneNode(i))
	}
	for _, i := range t.CleanUp {
		newTestCase.CleanUp = append(newTestCase.CleanUp, cloneNode(i))
	}
	return newTestCase
}

// ReplaceTableName is to replace the table name
func (t *TestCase) ReplaceTableName(tableMap map[string]string) {
	t.Q = replaceTableNameInNode(t.Q, tableMap)
	afterInsert := make([]ast.Node, 0, len(t.AfterInsert))
	for _, n := range t.AfterInsert {
		afterInsert = append(afterInsert, replaceTableNameInNode(n, tableMap))
	}
	t.AfterInsert = afterInsert
	beforeInsert := make([]ast.Node, 0, len(t.BeforeInsert))
	for _, n := range t.BeforeInsert {
		beforeInsert = append(beforeInsert, replaceTableNameInNode(n, tableMap))
	}
	t.BeforeInsert = beforeInsert

	for _, d := range t.D {
		d.ReplaceTableName(tableMap)
	}
}

func restoreWithPanic(n ast.Node) string {
	out := new(bytes.Buffer)
	err := n.Restore(format.NewRestoreCtx(format.RestoreStringDoubleQuotes, out))
	if err != nil {
		panic(zap.Error(err)) // should never get error
	}
	return out.String()
}

func cloneNode(n ast.Node) ast.Node {
	p := parser.New()
	sql := restoreWithPanic(n)
	stmtNodes, _, _ := p.Parse(sql, "", "")

	return stmtNodes[0]
}

// Visitor is a ast visitor
type Visitor struct {
	tableMap map[string]string
}

// Enter is to enter ast
func (v *Visitor) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	switch tp := in.(type) {
	case *ast.ColumnNameExpr:
		if name, ok := v.tableMap[tp.Name.Table.L]; ok {
			tp.Name.Table = model.NewCIStr(name)
		}
	case *ast.TableNameExpr:
		if name, ok := v.tableMap[tp.Name.Name.L]; ok {
			tp.Name.Name = model.NewCIStr(name)
		}
	case *ast.TableName:
		if name, ok := v.tableMap[tp.Name.L]; ok {
			tp.Name = model.NewCIStr(name)
		}
	case *ast.ColumnName:
		if name, ok := v.tableMap[tp.Table.L]; ok {
			tp.Table = model.NewCIStr(name)
		}
	}
	return in, false
}

// Leave is to leave ast
func (*Visitor) Leave(in ast.Node) (out ast.Node, ok bool) {
	return in, true
}

func replaceTableNameInNode(n ast.Node, tableMap map[string]string) ast.Node {
	v := Visitor{tableMap}
	n.Accept(&v)
	return n
}

// GetAllTables is to get all tables.
func (t *TestCase) GetAllTables() []types.Table {
	tables := make([]types.Table, 0)
	tableExist := make(map[string]bool) // for remove duplicated
	for _, d := range t.D {
		if _, ok := tableExist[d.Table.Name.String()]; !ok {
			tables = append(tables, d.Table)
			tableExist[d.Table.Name.String()] = true
		}
	}
	return tables
}

// Dataset is a dataset to test
type Dataset struct {
	Before []ast.Node // sql exec before insertion
	After  []ast.Node // sql exec after insertion
	Rows   map[string][]*connection.QueryItem
	Table  types.Table
}

// MakeQueryItem returned QueryItem.ValType is not used
func (*Dataset) MakeQueryItem(val interface{}, _ string) *connection.QueryItem {
	qi := new(connection.QueryItem)
	if val == nil {
		qi.Null = true
		return qi
	}

	qi.ValString = fmt.Sprintf("%v", val)
	return qi
}

func (d *Dataset) String() string {
	str := indentLevel2 + "Table: " + d.Table.Name.String() + "\n"
	str += indentLevel3 + "Columns:\n"
	for _, c := range d.Table.Columns {
		str += indentLevel4 + c.Name.String() + "(" + c.Type + ")\n"
	}
	str += indentLevel3 + "Rows:\n"
	if len(d.Rows) != 0 {
		firstCol := "" // get first column name
		for firstCol = range d.Rows {
			break
		}
		dataLen := len(d.Rows[firstCol])
		for i := 0; i < dataLen; i++ {
			str += indentLevel4
			for _, col := range d.Rows {
				str += col[i].StringWithoutType() + " "
			}
			str += "\n"
		}
	}
	str += indentLevel3 + "Before:\n"
	for _, n := range d.Before {
		str += indentLevel4 + restoreWithPanic(n) + "\n"
	}
	str += indentLevel3 + "After:\n"
	for _, n := range d.After {
		str += indentLevel4 + restoreWithPanic(n) + "\n"
	}
	return str
}

// ReplaceTableName is to replace the table name
func (d *Dataset) ReplaceTableName(tableMap map[string]string) {
	if tableName, ok := tableMap[d.Table.Name.String()]; ok {
		d.Table.Name = types.CIStr(tableName)
		columns := make(types.Columns, 0)
		for _, i := range d.Table.Columns {
			i.Table = types.CIStr(tableName)
			columns = append(columns, i)
		}
		d.Table.Columns = columns
	}
	nodes := make([]ast.Node, 0)
	for _, node := range d.Before {
		nodes = append(nodes, replaceTableNameInNode(node, tableMap))
	}
	d.Before = nodes
	nodes = make([]ast.Node, 0)
	for _, node := range d.After {
		nodes = append(nodes, replaceTableNameInNode(node, tableMap))
	}
	d.After = nodes
}

// Clone is to copy
func (d *Dataset) Clone() Dataset {
	newDataset := Dataset{}
	newDataset.Rows = make(map[string][]*connection.QueryItem)
	for _, i := range d.Before {
		newDataset.Before = append(newDataset.Before, cloneNode(i))
	}
	for _, i := range d.After {
		newDataset.After = append(newDataset.After, cloneNode(i))
	}
	for k, i := range d.Rows {
		var items []*connection.QueryItem
		for _, j := range i {
			item := *j
			// generally ValType is not actual type
			// j.ValType is a pointer and should never be changed
			// if anyone wants to change ValType, copy and make a new pointer
			items = append(items, &item)
		}
		newDataset.Rows[k] = items
	}
	newDataset.Table = d.Table
	return newDataset
}

// Mutation is a interface to generate tests
type Mutation interface {
	Condition(*TestCase) bool
	Mutate(*TestCase, *generator.Generator) ([]*TestCase, error)
}
