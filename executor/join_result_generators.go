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

var (
	_ joinResultGenerator = &semiJoinResultGenerator{}
	_ joinResultGenerator = &antiSemiJoinResultGenerator{}
	_ joinResultGenerator = &leftOuterSemiJoinResultGenerator{}
	_ joinResultGenerator = &antiLeftOuterSemiJoinResultGenerator{}
	_ joinResultGenerator = &leftOuterJoinResultGenerator{}
	_ joinResultGenerator = &rightOuterJoinResultGenerator{}
	_ joinResultGenerator = &innerJoinResultGenerator{}
)

// joinResultGenerator is used to generate join results according the join type, see every implementor for detailed information.
type joinResultGenerator interface {
	// emitMatchedInners should be called when key in outer row is equal to key in every inner row.
	emitMatchedInners(outer Row, inners []Row, resultBuffer []Row) ([]Row, error)
	// emitUnMatchedOuter should be called when outer row is not matched to any inner row.
	emitUnMatchedOuter(outer Row, resultBuffer []Row) []Row
	// emitUnMatchedOuters should be called when outer row is not matched to any inner row.
	emitUnMatchedOuters(outers []Row, resultBuffer []Row) []Row
}

func newJoinResultGenerator(ctx context.Context, joinType plan.JoinType, defaultInner Row, filter []expression.Expression) joinResultGenerator {
	switch joinType {
	case plan.SemiJoin:
		return &semiJoinResultGenerator{}
	case plan.AntiSemiJoin:
		return &antiSemiJoinResultGenerator{}
	case plan.LeftOuterSemiJoin:
		return &leftOuterSemiJoinResultGenerator{}
	case plan.AntiLeftOuterSemiJoin:
		return &antiLeftOuterSemiJoinResultGenerator{}
	case plan.LeftOuterJoin:
		return &leftOuterJoinResultGenerator{
			ctx:          ctx,
			defaultInner: defaultInner,
			filter:       filter,
		}
	case plan.RightOuterJoin:
		return &rightOuterJoinResultGenerator{
			ctx:          ctx,
			defaultInner: defaultInner,
			filter:       filter,
		}
	case plan.InnerJoin:
		return &innerJoinResultGenerator{
			ctx:    ctx,
			filter: filter,
		}
	}
	panic("unsupported join type in func newJoinResultGenerator()")
}

type semiJoinResultGenerator struct{}

// emitMatchedInners implements joinResultGenerator interface.
func (outputer *semiJoinResultGenerator) emitMatchedInners(outer Row, _ []Row, resultBuffer []Row) ([]Row, error) {
	return append(resultBuffer, outer), nil
}

// emitUnMatchedOuter implements joinResultGenerator interface.
func (outputer *semiJoinResultGenerator) emitUnMatchedOuter(outer Row, resultBuffer []Row) []Row {
	return resultBuffer
}

// emitUnMatchedOuters implements joinResultGenerator interface.
func (outputer *semiJoinResultGenerator) emitUnMatchedOuters(outers []Row, resultBuffer []Row) []Row {
	return resultBuffer
}

type antiSemiJoinResultGenerator struct{}

// emitMatchedInners implements joinResultGenerator interface.
func (outputer *antiSemiJoinResultGenerator) emitMatchedInners(_ Row, _ []Row, resultBuffer []Row) ([]Row, error) {
	return resultBuffer, nil
}

// emitUnMatchedOuter implements joinResultGenerator interface.
func (outputer *antiSemiJoinResultGenerator) emitUnMatchedOuter(outer Row, resultBuffer []Row) []Row {
	return append(resultBuffer, outer)
}

// emitUnMatchedOuters implements joinResultGenerator interface.
func (outputer *antiSemiJoinResultGenerator) emitUnMatchedOuters(outers []Row, resultBuffer []Row) []Row {
	for _, outer := range outers {
		resultBuffer = append(resultBuffer, outer)
	}
	return resultBuffer
}

type leftOuterSemiJoinResultGenerator struct{}

// emitMatchedInners implements joinResultGenerator interface.
func (outputer *leftOuterSemiJoinResultGenerator) emitMatchedInners(outer Row, _ []Row, resultBuffer []Row) ([]Row, error) {
	joinedRow := append(outer, types.NewDatum(true))
	return append(resultBuffer, joinedRow), nil
}

// emitUnMatchedOuter implements joinResultGenerator interface.
func (outputer *leftOuterSemiJoinResultGenerator) emitUnMatchedOuter(outer Row, resultBuffer []Row) []Row {
	outer = append(outer, types.NewDatum(false))
	return append(resultBuffer, outer)
}

// emitUnMatchedOuters implements joinResultGenerator interface.
func (outputer *leftOuterSemiJoinResultGenerator) emitUnMatchedOuters(outers []Row, resultBuffer []Row) []Row {
	for _, outer := range outers {
		resultBuffer = append(resultBuffer, append(outer, types.NewDatum(false)))
	}
	return resultBuffer
}

type antiLeftOuterSemiJoinResultGenerator struct{}

// emitMatchedInners implements joinResultGenerator interface.
func (outputer *antiLeftOuterSemiJoinResultGenerator) emitMatchedInners(outer Row, _ []Row, resultBuffer []Row) ([]Row, error) {
	joinedRow := append(outer, types.NewDatum(false))
	return append(resultBuffer, joinedRow), nil
}

// emitUnMatchedOuter implements joinResultGenerator interface.
func (outputer *antiLeftOuterSemiJoinResultGenerator) emitUnMatchedOuter(outer Row, resultBuffer []Row) []Row {
	outer = append(outer, types.NewDatum(true))
	return append(resultBuffer, outer)
}

// emitUnMatchedOuters implements joinResultGenerator interface.
func (outputer *antiLeftOuterSemiJoinResultGenerator) emitUnMatchedOuters(outers []Row, resultBuffer []Row) []Row {
	for _, outer := range outers {
		resultBuffer = append(resultBuffer, append(outer, types.NewDatum(true)))
	}
	return resultBuffer
}

type leftOuterJoinResultGenerator struct {
	ctx          context.Context
	defaultInner Row
	filter       []expression.Expression
}

// emitMatchedInners implements joinResultGenerator interface.
func (outputer *leftOuterJoinResultGenerator) emitMatchedInners(outer Row, inners []Row, resultBuffer []Row) ([]Row, error) {
	for _, inner := range inners {
		joinedRow := makeJoinRow(outer, inner)
		if outputer.filter != nil {
			matched, err := expression.EvalBool(outputer.filter, joinedRow, outputer.ctx)
			if err != nil {
				return resultBuffer, errors.Trace(err)
			}
			if !matched {
				continue
			}
		}
		resultBuffer = append(resultBuffer, joinedRow)
	}
	return resultBuffer, nil
}

// emitUnMatchedOuter implements joinResultGenerator interface.
func (outputer *leftOuterJoinResultGenerator) emitUnMatchedOuter(outer Row, resultBuffer []Row) []Row {
	return append(resultBuffer, makeJoinRow(outer, outputer.defaultInner))
}

// emitUnMatchedOuters implements joinResultGenerator interface.
func (outputer *leftOuterJoinResultGenerator) emitUnMatchedOuters(outers []Row, resultBuffer []Row) []Row {
	for _, outer := range outers {
		resultBuffer = append(resultBuffer, makeJoinRow(outer, outputer.defaultInner))
	}
	return resultBuffer
}

type rightOuterJoinResultGenerator struct {
	ctx          context.Context
	defaultInner Row
	filter       []expression.Expression
}

// emitMatchedInners implements joinResultGenerator interface.
func (outputer *rightOuterJoinResultGenerator) emitMatchedInners(outer Row, inners []Row, resultBuffer []Row) ([]Row, error) {
	for _, inner := range inners {
		joinedRow := makeJoinRow(inner, outer)
		if outputer.filter != nil {
			matched, err := expression.EvalBool(outputer.filter, joinedRow, outputer.ctx)
			if err != nil {
				return resultBuffer, errors.Trace(err)
			}
			if !matched {
				continue
			}
		}
		resultBuffer = append(resultBuffer, joinedRow)
	}
	return resultBuffer, nil
}

// emitUnMatchedOuter implements joinResultGenerator interface.
func (outputer *rightOuterJoinResultGenerator) emitUnMatchedOuter(outer Row, resultBuffer []Row) []Row {
	return append(resultBuffer, makeJoinRow(outputer.defaultInner, outer))
}

// emitUnMatchedOuters implements joinResultGenerator interface.
func (outputer *rightOuterJoinResultGenerator) emitUnMatchedOuters(outers []Row, resultBuffer []Row) []Row {
	for _, outer := range outers {
		resultBuffer = append(resultBuffer, makeJoinRow(outputer.defaultInner, outer))
	}
	return resultBuffer
}

type innerJoinResultGenerator struct {
	ctx    context.Context
	filter []expression.Expression
}

// emitMatchedInners implements joinResultGenerator interface.
func (outputer *innerJoinResultGenerator) emitMatchedInners(outer Row, inners []Row, resultBuffer []Row) ([]Row, error) {
	for _, inner := range inners {
		joinedRow := makeJoinRow(outer, inner)
		if outputer.filter != nil {
			matched, err := expression.EvalBool(outputer.filter, joinedRow, outputer.ctx)
			if err != nil {
				return resultBuffer, errors.Trace(err)
			}
			if !matched {
				continue
			}
		}
		resultBuffer = append(resultBuffer, joinedRow)
	}
	return resultBuffer, nil
}

// emitUnMatchedOuter implements joinResultGenerator interface.
func (outputer *innerJoinResultGenerator) emitUnMatchedOuter(_ Row, resultBuffer []Row) []Row {
	return resultBuffer
}

// emitUnMatchedOuters implements joinResultGenerator interface.
func (outputer *innerJoinResultGenerator) emitUnMatchedOuters(_ []Row, resultBuffer []Row) []Row {
	return resultBuffer
}
