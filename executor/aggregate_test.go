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

package executor

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/expression"
)

var _ = Suite(&testAggFuncSuite{})

type testAggFuncSuite struct {
}

func (s *testAggFuncSuite) SetUpSuite(c *C) {
}

func (s *testAggFuncSuite) TearDownSuite(c *C) {
}

type MockExec struct {
	fields    []*ast.ResultField
	Rows      []*Row
	curRowIdx int
}

func (m *MockExec) Schema() expression.Schema {
	return nil
}

func (m *MockExec) Fields() []*ast.ResultField {
	return m.fields
}

func (m *MockExec) Next() (*Row, error) {
	if m.curRowIdx >= len(m.Rows) {
		return nil, nil
	}
	r := m.Rows[m.curRowIdx]
	m.curRowIdx++
	if len(m.fields) > 0 {
		for i, d := range r.Data {
			m.fields[i].Expr.SetValue(d.GetValue())
		}
	}
	return r, nil
}

func (m *MockExec) Close() error {
	m.curRowIdx = 0
	return nil
}
