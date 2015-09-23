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

package expression

import (
	"fmt"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/kv"
)

func TestT(t *testing.T) {
	TestingT(t)
}

type mockExpr struct {
	isStatic bool
	val      interface{}
	err      error
}

func (m mockExpr) IsStatic() bool {
	return m.isStatic
}

func (m mockExpr) Eval(ctx context.Context, args map[interface{}]interface{}) (interface{}, error) {
	return m.val, m.err
}

func (m mockExpr) String() string {
	return "mock expression"
}

func (m mockExpr) Clone() Expression {
	nm := m
	return nm
}

func (m mockExpr) Accept(v Visitor) (Expression, error) {
	return m, nil
}

type mockCtx struct {
	vars map[fmt.Stringer]interface{}
}

func newMockCtx() *mockCtx {
	return &mockCtx{vars: make(map[fmt.Stringer]interface{})}
}

func (c *mockCtx) GetTxn(forceNew bool) (kv.Transaction, error) { return nil, nil }

func (c *mockCtx) FinishTxn(rollback bool) error { return nil }

func (c *mockCtx) SetValue(key fmt.Stringer, value interface{}) {
	c.vars[key] = value
}

func (c *mockCtx) Value(key fmt.Stringer) interface{} {
	return c.vars[key]
}

func (c *mockCtx) ClearValue(key fmt.Stringer) {
	delete(c.vars, key)
}
