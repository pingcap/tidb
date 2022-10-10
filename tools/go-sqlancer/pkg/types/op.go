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

package types

import (
	"fmt"
	"math/rand"

	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/opcode"
	parser_driver "github.com/pingcap/tidb/types/parser_driver"
)

type evaluator = func(...parser_driver.ValueExpr) (parser_driver.ValueExpr, error)

// TypedExprNodeGen is generated TypedExprNode
type TypedExprNodeGen = func(uint64) (ast.ExprNode, parser_driver.ValueExpr, error)

// TypedExprNodeGenSel is a function
type TypedExprNodeGenSel = func(nodeGen TypedExprNodeGen, this OpFuncEval, retType uint64) (ast.ExprNode, parser_driver.ValueExpr, error)
type retTypeGen = func(...uint64) (retType uint64, warnings bool, err error)

// OpFuncEval is interface
type OpFuncEval interface {
	GetMinArgs() int
	SetMinArgs(int)
	GetMaxArgs() int
	SetMaxArgs(int)

	GetName() string
	SetName(string)

	MakeArgTable(bool)
	GetArgTable() argTable

	GetPossibleReturnType() uint64
	// IsValidParam(...uint64) (uint64, error)
	Eval(...parser_driver.ValueExpr) (parser_driver.ValueExpr, error)
	// for generate node
	Node(TypedExprNodeGen, uint64) (ast.ExprNode, parser_driver.ValueExpr, error)
}

// OpFuncIndex is to return type as key; f => str|int : [TypeStr]:['f':f], [TypeInt]:['f':f]
type OpFuncIndex map[uint64]map[string]OpFuncEval

// RandOpFn is to rand
func (idx *OpFuncIndex) RandOpFn(tp uint64) (OpFuncEval, error) {
	m, ok := (*idx)[tp]
	if !ok || len(m) == 0 {
		return nil, fmt.Errorf("no operations or functions return type: %d", tp)
	}
	keys := make([]string, 0)
	for k := range m {
		keys = append(keys, k)
	}
	return m[keys[rand.Intn(len(keys))]], nil
}

// OpFuncMap is a map of operation func
type OpFuncMap map[string]OpFuncEval

// Eval is to return ValueExpr
func (m *OpFuncMap) Eval(name string, vals ...parser_driver.ValueExpr) (parser_driver.ValueExpr, error) {
	var e parser_driver.ValueExpr
	ev, ok := (*m)[name]
	if !ok {
		return e, fmt.Errorf("no such function or op with opcode: %s", name)
	}
	return ev.Eval(vals...)
}

// Add a new OpFuncEval
func (m *OpFuncMap) Add(o OpFuncEval) {
	(*m)[o.GetName()] = o
}

// Find OpFuncEval by name
func (m *OpFuncMap) Find(name string) OpFuncEval {
	return (*m)[name]
}

// BaseOpFunc is base operator function
type BaseOpFunc struct {
	// min and max; -1 indicates infinite
	minArgs    int
	maxArgs    int
	name       string
	evaluator  evaluator
	nodeGen    TypedExprNodeGenSel
	retTypeGen retTypeGen
	argTable   argTable
}

// GetMinArgs is to get min args
func (o *BaseOpFunc) GetMinArgs() int {
	return o.minArgs
}

// SetMinArgs set a min args
func (o *BaseOpFunc) SetMinArgs(m int) {
	o.minArgs = m
}

// GetMaxArgs is max
func (o *BaseOpFunc) GetMaxArgs() int {
	return o.maxArgs
}

// SetMaxArgs is set
func (o *BaseOpFunc) SetMaxArgs(m int) {
	o.maxArgs = m
}

// GetName is to get the name
func (o *BaseOpFunc) GetName() string {
	return o.name
}

// SetName is to set name
func (o *BaseOpFunc) SetName(n string) {
	o.name = n
}

// SetEvalFn is to set eval fn
func (o *BaseOpFunc) SetEvalFn(fn evaluator) {
	o.evaluator = fn
}

// Eval is eval
func (o *BaseOpFunc) Eval(vals ...parser_driver.ValueExpr) (parser_driver.ValueExpr, error) {
	return o.evaluator(vals...)
}

// SetNodeFn set TypedExprNodeGenSel into nodeGen
func (o *BaseOpFunc) SetNodeFn(fn TypedExprNodeGenSel) {
	o.nodeGen = fn
}

// Node is to generate
func (o *BaseOpFunc) Node(exprNodeGen TypedExprNodeGen, ret uint64) (ast.ExprNode, parser_driver.ValueExpr, error) {
	// ? we can make o as a param
	// ? so that we can downcast o to Op/Fn/CastFn to call their owned methods
	return o.nodeGen(exprNodeGen, o, ret)
}

// SetIsValidParam is add function to retTypeGen
func (o *BaseOpFunc) SetIsValidParam(fn retTypeGen) {
	o.retTypeGen = fn
}

// MakeArgTable is to make arg table
func (o *BaseOpFunc) MakeArgTable(ignoreWarn bool) {
	o.argTable = NewArgTable(o.maxArgs)
	if o.maxArgs == 0 {
		// such as NOW()
		ret, warn, err := o.retTypeGen()
		if err != nil || warn {
			panic(fmt.Sprintf("call IsValidParam failed, err: %+v warn: %v", err, warn))
		}
		for ret != 0 {
			i := ret &^ (ret - 1)
			o.argTable.Insert(i)
			ret = ret & (ret - 1)
		}
	} else {
		stack := make([][]uint64, 0)
		for _, i := range supportArgs {
			stack = append(stack, []uint64{i})
		}
		for len(stack) > 0 {
			cur := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			if len(cur) == o.maxArgs {
				ret, warn, err := o.retTypeGen(cur...)
				if ret != 0 && err == nil {
					if ignoreWarn || !warn {
						for ret != 0 {
							i := ret &^ (ret - 1)
							o.argTable.Insert(i, cur...)
							ret = ret & (ret - 1)
						}
					}
				}
			} else {
				for _, i := range supportArgs {
					newCur := make([]uint64, len(cur))
					copy(newCur, cur)
					//nolint: makezero
					stack = append(stack, append(newCur, i))
				}
			}
		}
	}
}

// GetPossibleReturnType is return returnType
func (o *BaseOpFunc) GetPossibleReturnType() (returnType uint64) {
	for _, i := range supportArgs {
		args := make([]*uint64, 0)
		for j := 0; j < o.maxArgs; j++ {
			args = append(args, nil)
		}
		tmp := i
		res, err := o.argTable.Filter(args, &tmp)
		if err == nil && len(res) != 0 {
			returnType |= i
		}
	}
	return
}

// GetArgTable is to
func (o *BaseOpFunc) GetArgTable() argTable {
	return o.argTable
}

// Op is a operator
type Op struct {
	BaseOpFunc

	opcode opcode.Op
}

// GetOpcode is to get the opcode
func (o *Op) GetOpcode() opcode.Op {
	return o.opcode
}

// SetOpcode is to Set Opcode
func (o *Op) SetOpcode(code opcode.Op) {
	o.opcode = code
	o.name = code.String()
}

// NewOp creates a new Op
func NewOp(code opcode.Op, min, max int, fn evaluator, vp retTypeGen, gn TypedExprNodeGenSel) *Op {
	var o Op
	o.SetOpcode(code)
	o.SetMinArgs(min)
	o.SetMaxArgs(max)
	o.SetEvalFn(fn)
	o.SetIsValidParam(vp)
	o.SetNodeFn(gn)
	// TODO: give a context
	o.MakeArgTable(true)
	return &o
}

// Fn is BaseOpFunc
type Fn struct {
	BaseOpFunc
}

// NewFn is to create a new Fn
func NewFn(name string, min, max int, fn evaluator, vp retTypeGen, gn TypedExprNodeGenSel) *Fn {
	var f Fn
	f.SetName(name)
	f.SetMaxArgs(max)
	f.SetMinArgs(min)
	f.SetEvalFn(fn)
	f.SetIsValidParam(vp)
	f.SetNodeFn(gn)
	// TODO: give a context
	f.MakeArgTable(true)
	return &f
}
