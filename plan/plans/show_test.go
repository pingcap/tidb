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

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/expression/expressions"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/plan/plans"
	"github.com/pingcap/tidb/rset/rsets"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/stmt"
)

type testShowSuit struct {
	txn  kv.Transaction
	vars map[string]interface{}
}

// implement Context interface
func (p *testShowSuit) GetTxn(forceNew bool) (kv.Transaction, error) { return p.txn, nil }

func (p *testShowSuit) FinishTxn(rollback bool) error { return nil }

// SetValue saves a value associated with this context for key
func (p *testShowSuit) SetValue(key fmt.Stringer, value interface{}) {
	p.vars[key.String()] = value
}

// Value returns the value associated with this context for key
func (p *testShowSuit) Value(key fmt.Stringer) interface{} {
	return p.vars[key.String()]
}

// ClearValue clears the value associated with this context for key
func (p *testShowSuit) ClearValue(key fmt.Stringer) {}

var _ = Suite(&testShowSuit{})

func (p *testShowSuit) SetUpSuite(c *C) {
	var err error
	store, err := tidb.NewStore(tidb.EngineGoLevelDBMemory)
	c.Assert(err, IsNil)
	p.vars = map[string]interface{}{}
	p.txn, _ = store.Begin()
	variable.BindSessionVars(p)
}

func (p *testShowSuit) TestShowVariables(c *C) {
	pln := &plans.ShowPlan{
		Target:      stmt.ShowVariables,
		GlobalScope: true,
		Pattern: &expressions.PatternLike{
			Pattern: &expressions.Value{
				Val: "character_set_results",
			},
		},
	}
	fls := pln.GetFields()
	c.Assert(fls, HasLen, 2)
	c.Assert(fls[0].Name, Equals, "Variable_name")
	c.Assert(fls[1].Name, Equals, "Value")

	sessionVars := variable.GetSessionVars(p)
	ret := map[string]string{}
	rset := rsets.Recordset{
		Ctx:  p,
		Plan: pln,
	}
	rset.Do(func(data []interface{}) (bool, error) {
		ret[data[0].(string)] = data[1].(string)
		return true, nil
	})
	c.Assert(ret, HasLen, 1)
	v, ok := ret["character_set_results"]
	c.Assert(ok, IsTrue)
	c.Assert(v, Equals, "latin1")
	// Set session variable to utf8
	sessionVars.Systems["character_set_results"] = "utf8"
	pln.Close()
	rset.Do(func(data []interface{}) (bool, error) {
		ret[data[0].(string)] = data[1].(string)
		return true, nil
	})
	c.Assert(ret, HasLen, 1)
	v, ok = ret["character_set_results"]
	c.Assert(ok, IsTrue)
	// Show global varibale get latin1
	c.Assert(v, Equals, "latin1")

	pln.GlobalScope = false
	pln.Close()
	rset.Do(func(data []interface{}) (bool, error) {
		ret[data[0].(string)] = data[1].(string)
		return true, nil
	})
	c.Assert(ret, HasLen, 1)
	v, ok = ret["character_set_results"]
	c.Assert(ok, IsTrue)
	// Show session varibale get utf8
	c.Assert(v, Equals, "utf8")
	pln.Close()
	pln.Pattern = nil
	pln.Where = &expressions.BinaryOperation{
		L:  &expressions.Ident{CIStr: model.NewCIStr("Variable_name")},
		R:  expressions.Value{Val: "autocommit"},
		Op: opcode.EQ,
	}

	ret = map[string]string{}
	sessionVars.Systems["autocommit"] = "on"
	rset.Do(func(data []interface{}) (bool, error) {
		ret[data[0].(string)] = data[1].(string)
		return true, nil
	})

	c.Assert(ret, HasLen, 1)
	v, ok = ret["autocommit"]
	c.Assert(ok, IsTrue)
	c.Assert(v, Equals, "on")
}

func (p *testShowSuit) TearDownSuite(c *C) {
	p.txn.Commit()
}
