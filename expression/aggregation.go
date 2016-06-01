// Copyright 2016 PingCAP, Inc.
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
	"github.com/pingcap/tidb/ast"
)

// AggregationFunction stands for aggregate functions.
type AggregationFunction interface {
	// GetArgs stands for getting all arguments.
	GetArgs() []Expression
}

// NewAggrFunction creates a new AggregationFunction.
func NewAggrFunction(funcType string, funcArgs []Expression) AggregationFunction {
	switch funcType {
	case ast.AggFuncSum:
		return &sumFunction{aggrFunction: aggrFunction{Args: funcArgs, resultMapper: make(aggrCtxMapper, 0)}}
	case ast.AggFuncCount:
		return &countFunction{aggrFunction: aggrFunction{Args: funcArgs, resultMapper: make(aggrCtxMapper, 0)}}
	case ast.AggFuncAvg:
		return &avgFunction{aggrFunction: aggrFunction{Args: funcArgs, resultMapper: make(aggrCtxMapper, 0)}}
	case ast.AggFuncGroupConcat:
		return &concatFunction{aggrFunction: aggrFunction{Args: funcArgs, resultMapper: make(aggrCtxMapper, 0)}}
	case ast.AggFuncMax:
		return &maxMinFunction{aggrFunction: aggrFunction{Args: funcArgs, resultMapper: make(aggrCtxMapper, 0)}, isMax: true}
	case ast.AggFuncMin:
		return &maxMinFunction{aggrFunction: aggrFunction{Args: funcArgs, resultMapper: make(aggrCtxMapper, 0)}, isMax: false}
	case ast.AggFuncFirstRow:
		return &firstRowFunction{aggrFunction: aggrFunction{Args: funcArgs, resultMapper: make(aggrCtxMapper, 0)}}
	}
	return nil
}

type aggrCtxMapper map[string]*ast.AggEvaluateContext

type aggrFunction struct {
	Args         []Expression
	resultMapper aggrCtxMapper
}

// GetArgs implements AggregationFunction interface.
func (af *aggrFunction) GetArgs() []Expression {
	return af.Args
}

type sumFunction struct {
	aggrFunction
}

type countFunction struct {
	aggrFunction
}

type avgFunction struct {
	aggrFunction
}

type concatFunction struct {
	aggrFunction
}

type maxMinFunction struct {
	aggrFunction
	isMax bool
}

type firstRowFunction struct {
	aggrFunction
}
