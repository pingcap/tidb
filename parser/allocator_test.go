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

package parser

import (
	random "math/rand"
	"runtime"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/types"
)

var _ = Suite(&testAllocator{})

type testAllocator struct {
}

func (t *testAllocator) TestSimple(c *C) {
	defer testleak.AfterTest(c)()
	ac := newAllocator()

	var table = []struct {
		alloc func(*allocator) interface{}
		zero  interface{}
		use   func(interface{})
	}{
		{
			func(ac *allocator) interface{} { return ac.allocFieldType() },
			&types.FieldType{},
			func(v interface{}) { r := v.(*types.FieldType); r.Tp = 7 },
		},
		{
			func(ac *allocator) interface{} { return ac.allocValueExpr() },
			&ast.ValueExpr{},
			func(v interface{}) { r := v.(*ast.ValueExpr); r.SetType(ac.newFieldType(23)) },
		},
		{
			func(ac *allocator) interface{} { return ac.allocInsertStmt() },
			&ast.InsertStmt{},
			func(v interface{}) { r := v.(*ast.InsertStmt); r.Priority = 7 },
		},
		{
			func(ac *allocator) interface{} { return ac.allocSelectStmt() },
			&ast.SelectStmt{},
			func(v interface{}) { r := v.(*ast.SelectStmt); r.LockTp = 42 },
		},
		{
			func(ac *allocator) interface{} { return ac.allocJoin() },
			&ast.Join{},
			func(v interface{}) { r := v.(*ast.Join); r.Tp = 54 },
		},
		{
			func(ac *allocator) interface{} { return ac.allocTableName() },
			&ast.TableName{},
			func(v interface{}) { r := v.(*ast.TableName); r.Name = model.NewCIStr("hello") },
		},
		{
			func(ac *allocator) interface{} { return ac.allocTableSource() },
			&ast.TableSource{},
			func(v interface{}) { r := v.(*ast.TableSource); r.AsName = model.NewCIStr("world") },
		},
		{
			func(ac *allocator) interface{} { return ac.allocTableRefsClause() },
			&ast.TableRefsClause{},
			func(v interface{}) { r := v.(*ast.TableRefsClause); r.TableRefs = ac.allocJoin() },
		},
	}

	for i := 0; i < 10000; i++ {
		r := random.Intn(len(table))
		if r == 0 {
			ac.reset()
		}

		data := table[r].alloc(ac)
		c.Assert(data, DeepEquals, table[r].zero)
		table[r].use(data)
	}
}

func (t *testAllocator) TestSafeAfterGC(c *C) {
	defer testleak.AfterTest(c)()
	ac := newAllocator()

	ts := &ast.TableSource{}
	ref := make([]*ast.TableRefsClause, 0, 10000)
	for i := 0; i < 10000; i++ {
		v := ac.allocTableRefsClause()
		v.TableRefs = &ast.Join{
			Left: ts,
		}
		ref = append(ref, v)
	}
	runtime.GC()

	for _, v := range ref {
		c.Assert(v.TableRefs.Left, NotNil)
	}
}
