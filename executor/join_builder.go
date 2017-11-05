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
	"github.com/pingcap/tidb/types"
)

// joinBuilder builds a join Executor.
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
}

func (b *joinBuilder) BuildMergeJoin() (*MergeJoinExec, error) {
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
		ctx:             b.context,
		outerKeys:       leftJoinKeys,
		innerKeys:       rightJoinKeys,
		outerIter:       leftRowBlock,
		innerIter:       rightRowBlock,
		schema:          b.schema,
		resultGenerator: newJoinResultGenerator(b.context, b.joinType, b.defaultValues, b.otherFilter),
	}

	if b.joinType == plan.RightOuterJoin {
		exec.outerKeys, exec.innerKeys = exec.innerKeys, exec.outerKeys
		exec.outerIter, exec.innerIter = exec.innerIter, exec.outerIter
	}
	if b.joinType != plan.InnerJoin {
		exec.outerIter.filter = nil
		exec.outerFilter = b.leftFilter
	}

	return exec, nil
}
