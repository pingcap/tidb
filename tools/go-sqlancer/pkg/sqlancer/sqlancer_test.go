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

package sqlancer

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/pingcap/tidb/tools/go-sqlancer/pkg/connection"
	"github.com/pingcap/tidb/tools/go-sqlancer/pkg/types"
	"github.com/pingcap/tidb/tools/go-sqlancer/pkg/util"
	"github.com/stretchr/testify/assert"
)

func helperMakeTable() *types.Table {
	t := new(types.Table)
	t.Name = types.CIStr(fmt.Sprintf("t%d", util.Rd(100)))
	t.Columns = make([]types.Column, 0)
	for i := util.Rd(5) + 2; i > 0; i-- {
		tp := "int"
		switch util.Rd(4) {
		case 0:
			tp = "varchar"
		case 1:
			tp = "text"
		}
		t.Columns = append(t.Columns, types.Column{
			Name:   types.CIStr(fmt.Sprintf("c%d", util.Rd(100))),
			Type:   tp,
			Length: int(util.RdRange(9, 20)),
		})
	}
	return t
}

func TestDeleteStmt(t *testing.T) {
	p, _ := NewSQLancer(NewConfig())
	p.Tables = []types.Table{*helperMakeTable(), *helperMakeTable()}
	s, err := p.DeleteStmt(p.Tables, p.Tables[0])
	fmt.Println(s)
	assert.NoError(t, err)
}

func TestUpdateStmt(t *testing.T) {
	p, _ := NewSQLancer(NewConfig())
	p.Tables = []types.Table{*helperMakeTable(), *helperMakeTable()}
	s, err := p.UpdateStmt(p.Tables, p.Tables[0])
	fmt.Println(s)
	assert.NoError(t, err)
}

func TestNoRecNormal(t *testing.T) {
	p, _ := NewSQLancer(NewConfig())
	p.Tables = []types.Table{*helperMakeTable(), *helperMakeTable()}
	s, sql, _, err := p.GenSelectStmt()
	fmt.Println(s, sql)
	assert.NoError(t, err)
}

func TestNoRecNoOpt(t *testing.T) {
	p, _ := NewSQLancer(NewConfig())
	p.Tables = []types.Table{*helperMakeTable(), *helperMakeTable()}
	s, sql1, _, err := p.GenSelectStmt()
	fmt.Println(s, sql1)
	assert.NoError(t, err)
	s2, sql2, _, err := p.GenSelectStmt()
	fmt.Println(s2, sql2)
	assert.NoError(t, err)
}

func TestCheckResultSet(t *testing.T) {
	// we cannot set type name out of package
	intValType := &sql.ColumnType{}
	// floatValType := &sql.ColumnType{}
	stringValType := &sql.ColumnType{}

	resultSet1 := [][]connection.QueryItems{
		{
			{
				{
					Null:      false,
					ValType:   intValType,
					ValString: "33",
				}, {
					Null:      true,
					ValType:   intValType,
					ValString: "22",
				}, {
					Null:      false,
					ValType:   stringValType,
					ValString: "-10",
				},
			},
		}, {
			{
				{
					Null:      false,
					ValType:   intValType,
					ValString: "33",
				}, {
					Null:      true,
					ValType:   intValType,
					ValString: "22",
				}, {
					Null:      false,
					ValType:   stringValType,
					ValString: "-10",
				},
			},
		}, {
			{
				{
					Null:      false,
					ValType:   intValType,
					ValString: "33",
				}, {
					Null:      true,
					ValType:   intValType,
					ValString: "22",
				}, {
					Null:      false,
					ValType:   stringValType,
					ValString: "-10",
				},
			},
		},
	}

	assert.True(t, checkResultSet(resultSet1, true))
	resultSet1[2][0] = append(resultSet1[2][0], &connection.QueryItem{
		Null:      false,
		ValType:   stringValType,
		ValString: "ZZZZ",
	})
	assert.False(t, checkResultSet(resultSet1, true))
	resultSet1[2][0] = resultSet1[2][0][:len(resultSet1[2][0])-1]
	assert.True(t, checkResultSet(resultSet1, true))

	resultSet1[1] = append(resultSet1[1], resultSet1[2][0])
	assert.False(t, checkResultSet(resultSet1, true))
}
