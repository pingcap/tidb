// Copyright 2017 PingCAP, Inc.
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
	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/util/types"
)

type joinBuilder struct {
	context       context.Context
	leftChild     Executor
	rightChild    Executor
	eqConditions  []*expression.ScalarFunction
	leftFilter    []expression.Expression
	rightFilter   []expression.Expression
	otherFilter   []expression.Expression
	schema        *expression.Schema
	joinType      plan.JoinType
	defaultValues []types.Datum
	isAntiMode    bool
}

func NewJoinBuilder(ctx context.Context, lhs, rhs Executor, joinType plan.JoinType,
	equalConds []*expression.ScalarFunction,
	lhsFilter, rhsFilter, otherFilter []expression.Expression,
	schema *expression.Schema, defaultValues []types.Datum,
	isAntiMode bool) *joinBuilder {
	return &joinBuilder{
		context:       ctx,
		leftChild:     lhs,
		rightChild:    rhs,
		eqConditions:  equalConds,
		leftFilter:    lhsFilter,
		rightFilter:   rhsFilter,
		otherFilter:   otherFilter,
		schema:        schema,
		joinType:      joinType,
		defaultValues: defaultValues,
		isAntiMode:    isAntiMode,
	}
}

func (b *joinBuilder) BuildMergeJoin(assumeSortedDesc bool) (*MergeJoinExec, error) {
	var leftJoinKeys, rightJoinKeys []*expression.Column
	for _, eqCond := range b.eqConditions {
		if len(eqCond.GetArgs()) != 2 {
			return nil, errors.Annotate(ErrBuildExecutor, "invalid join key for equal condition")
		}
		lKey, ok := eqCond.GetArgs()[0].(*expression.Column)
		if !ok {
			return nil, errors.Annotate(ErrBuildExecutor, "left side of join key must be column for merge join")
		}
		rKey, ok := eqCond.GetArgs()[1].(*expression.Column)
		if !ok {
			return nil, errors.Annotate(ErrBuildExecutor, "right side of join key must be column for merge join")
		}
		leftJoinKeys = append(leftJoinKeys, lKey)
		rightJoinKeys = append(rightJoinKeys, rKey)
	}
	leftRowBlock := &rowBlockIterator{
		ctx:      b.context,
		reader:   b.leftChild,
		filter:   b.leftFilter,
		joinKeys: leftJoinKeys,
	}

	rightRowBlock := &rowBlockIterator{
		ctx:      b.context,
		reader:   b.rightChild,
		filter:   b.rightFilter,
		joinKeys: rightJoinKeys,
	}

	exec := &MergeJoinExec{
		ctx:           b.context,
		leftJoinKeys:  leftJoinKeys,
		rightJoinKeys: rightJoinKeys,
		leftRowBlock:  leftRowBlock,
		rightRowBlock: rightRowBlock,
		otherFilter:   b.otherFilter,
		schema:        b.schema,
		desc:          assumeSortedDesc,
		joinType:      b.joinType,
		isAntiMode:    b.isAntiMode,
	}

	switch b.joinType {
	case plan.LeftOuterJoin:
		exec.leftRowBlock.filter = nil
		exec.leftFilter = b.leftFilter
		exec.preserveLeft = true
		exec.defaultRightRow = b.defaultValues
	case plan.RightOuterJoin:
		exec.leftRowBlock = rightRowBlock
		exec.rightRowBlock = leftRowBlock
		exec.leftRowBlock.filter = nil
		exec.leftFilter = b.leftFilter
		exec.preserveLeft = true
		exec.defaultRightRow = b.defaultValues
		exec.flipSide = true
		exec.leftJoinKeys = rightJoinKeys
		exec.rightJoinKeys = leftJoinKeys
	case plan.InnerJoin:
	case plan.SemiJoin:
	case plan.LeftOuterSemiJoin:
	default:
		return nil, errors.Annotate(ErrBuildExecutor, "unknown join type")
	}
	return exec, nil
}
