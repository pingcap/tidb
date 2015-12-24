// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package plans_test

import (
	"fmt"
	"reflect"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/column"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/field"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/plan/plans"
	"github.com/pingcap/tidb/rset/rsets"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/util/types"
)

type testFromSuit struct {
	txn  kv.Transaction
	cols []*column.Col
	tbl  table.Table
	vars map[string]interface{}
}

type simpleAllocator struct {
	id int64
}

func (s *simpleAllocator) Alloc(tableID int64) (int64, error) {
	s.id++
	return s.id, nil
}

// implement Context interface
func (p *testFromSuit) GetTxn(forceNew bool) (kv.Transaction, error) { return p.txn, nil }

func (p *testFromSuit) FinishTxn(rollback bool) error { return nil }

// SetValue saves a value associated with this context for key
func (p *testFromSuit) SetValue(key fmt.Stringer, value interface{}) {
	p.vars[key.String()] = value
}

// Value returns the value associated with this context for key
func (p *testFromSuit) Value(key fmt.Stringer) interface{} {
	return p.vars[key.String()]
}

// ClearValue clears the value associated with this context for key
func (p *testFromSuit) ClearValue(key fmt.Stringer) {}

var _ = Suite(&testFromSuit{})

func (p *testFromSuit) SetUpSuite(c *C) {
	store, err := tidb.NewStore(tidb.EngineGoLevelDBMemory)
	c.Assert(err, IsNil)
	p.vars = map[string]interface{}{}
	p.txn, _ = store.Begin()
	tbInfo := &model.TableInfo{
		ID:    1,
		Name:  model.NewCIStr("t"),
		State: model.StatePublic,
		Columns: []*model.ColumnInfo{
			{
				ID:           0,
				Name:         model.NewCIStr("id"),
				Offset:       0,
				DefaultValue: 0,
				FieldType:    *types.NewFieldType(mysql.TypeLonglong),
				State:        model.StatePublic,
			},
			{
				ID:           1,
				Name:         model.NewCIStr("name"),
				Offset:       1,
				DefaultValue: nil,
				FieldType:    *types.NewFieldType(mysql.TypeVarchar),
				State:        model.StatePublic,
			},
		},
	}
	p.tbl, err = tables.TableFromMeta(&simpleAllocator{}, tbInfo)
	c.Assert(err, IsNil)
	variable.BindSessionVars(p)

	var i int64
	for i = 0; i < 10; i++ {
		_, err = p.tbl.AddRecord(p, []interface{}{i * 10, "hello"})
		c.Assert(err, IsNil)
	}
}

func (p *testFromSuit) TestTableNilPlan(c *C) {
	nilPlan := &plans.TableNilPlan{
		T: p.tbl,
	}
	var ids []int64
	rset := rsets.Recordset{
		Plan: nilPlan,
		Ctx:  p,
	}
	var id int64
	rset.Do(func(data []interface{}) (bool, error) {
		id++
		ids = append(ids, id)
		return true, nil
	})
	c.Assert(reflect.DeepEqual(ids, []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}), Equals, true)
}

func (p *testFromSuit) TestTableDefaultPlan(c *C) {
	pln := &plans.TableDefaultPlan{
		T: p.tbl,
		Fields: []*field.ResultField{
			field.ColToResultField(p.tbl.Cols()[0], "t"),
			field.ColToResultField(p.tbl.Cols()[1], "t"),
		},
	}

	ret := map[int64][]byte{}
	rset := rsets.Recordset{Ctx: p, Plan: pln}
	rset.Do(func(data []interface{}) (bool, error) {
		ret[data[0].(int64)] = data[1].([]byte)
		return true, nil
	})
	excepted := map[int64][]byte{}
	for i := 0; i < 10; i++ {
		excepted[int64(i*10)] = []byte("hello")
	}
	//c.Assert(reflect.DeepEqual(ret, excepted), Equals, true)
	c.Assert(ret, DeepEquals, excepted)

	// expr: id > 0
	expr := &expression.BinaryOperation{
		Op: opcode.GE,
		L: &expression.Ident{
			CIStr: model.NewCIStr("id"),
		},
		R: &expression.Value{
			Val: 5,
		},
	}
	_, filtered, err := pln.FilterForUpdateAndDelete(p, expr)
	c.Assert(err, IsNil)
	c.Assert(filtered, IsFalse)
	// with no index

	idxCol := &column.IndexedCol{
		IndexInfo: model.IndexInfo{
			Name:  model.NewCIStr("id"),
			Table: model.NewCIStr("t"),
			Columns: []*model.IndexColumn{
				{
					Name:   model.NewCIStr("id"),
					Offset: 0,
					Length: 0,
				},
			},
			Unique:  false,
			Primary: false,
			State:   model.StatePublic,
		},
	}

	idxCol.X = kv.NewKVIndex("i", "id", 0, false)

	p.tbl.AddIndex(idxCol)

	expr4 := &expression.Ident{
		CIStr: model.NewCIStr("id"),
	}
	_, filtered, err = pln.FilterForUpdateAndDelete(p, expr4)
	c.Assert(err, IsNil)
	// with no index
	c.Assert(filtered, IsTrue)

	expr5 := &expression.IsNull{
		Expr: &expression.Ident{
			CIStr: model.NewCIStr("id"),
		},
		Not: true,
	}
	_, filtered, err = pln.FilterForUpdateAndDelete(p, expr5)
	c.Assert(err, IsNil)
	// with no index
	c.Assert(filtered, IsTrue)
}

func (p *testFromSuit) TestTableIndexPlan(c *C) {
}

func (p *testFromSuit) TearDownSuite(c *C) {
	p.txn.Commit()
}
