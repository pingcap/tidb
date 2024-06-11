// Copyright 2023 PingCAP, Ins.
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

package core

import (
	"context"
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/baseimpl"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
)

// ScalarSubqueryEvalCtx store the plan for the subquery, used by ScalarSubQueryExpr.
type ScalarSubqueryEvalCtx struct {
	baseimpl.Plan

	// The context for evaluating the subquery.
	scalarSubQuery base.PhysicalPlan
	ctx            context.Context
	is             infoschema.InfoSchema
	evalErr        error
	evaled         bool

	outputColIDs []int64
	colsData     []types.Datum
}

func (ssctx *ScalarSubqueryEvalCtx) getColVal(colID int64) (*types.Datum, error) {
	err := ssctx.selfEval()
	if err != nil {
		return nil, err
	}
	for i, id := range ssctx.outputColIDs {
		if id == colID {
			return &ssctx.colsData[i], nil
		}
	}
	return nil, errors.Errorf("Could not found the ScalarSubQueryExpr#%d in the ScalarSubquery_%d", colID, ssctx.ID())
}

func (ssctx *ScalarSubqueryEvalCtx) selfEval() error {
	if ssctx.evaled {
		return ssctx.evalErr
	}
	ssctx.evaled = true
	row, err := EvalSubqueryFirstRow(ssctx.ctx, ssctx.scalarSubQuery, ssctx.is, ssctx.SCtx())
	if err != nil {
		ssctx.evalErr = err
		return err
	}
	ssctx.colsData = row
	return nil
}

// ScalarSubQueryExpr is a expression placeholder for the non-correlated scalar subqueries which can be evaluated during optimizing phase.
// TODO: The methods related with evaluate the function will be revised in next step.
type ScalarSubQueryExpr struct {
	scalarSubqueryColID int64

	// The context for evaluating the subquery.
	evalCtx *ScalarSubqueryEvalCtx
	evalErr error
	evaled  bool

	hashcode []byte

	expression.Constant
}

// Traverse implements the TraverseDown interface.
func (s *ScalarSubQueryExpr) Traverse(_ expression.TraverseAction) expression.Expression {
	return s
}

func (s *ScalarSubQueryExpr) selfEvaluate() error {
	colVal, err := s.evalCtx.getColVal(s.scalarSubqueryColID)
	if err != nil {
		s.evalErr = err
		s.Constant = *expression.NewNull()
		return err
	}
	s.Constant.Value = *colVal
	s.evaled = true
	return nil
}

// Eval implements the Expression interface.
func (s *ScalarSubQueryExpr) Eval(_ expression.EvalContext, _ chunk.Row) (types.Datum, error) {
	if s.evaled {
		return s.Value, nil
	}
	if s.evalErr != nil {
		return s.Value, s.evalErr
	}
	err := s.selfEvaluate()
	return s.Value, err
}

// EvalInt returns the int64 representation of expression.
func (*ScalarSubQueryExpr) EvalInt(_ expression.EvalContext, _ chunk.Row) (val int64, isNull bool, err error) {
	return 0, false, errors.Errorf("Evaluation methods is not implemented for ScalarSubQueryExpr")
}

// EvalReal returns the float64 representation of expression.
func (*ScalarSubQueryExpr) EvalReal(_ expression.EvalContext, _ chunk.Row) (val float64, isNull bool, err error) {
	return 0, false, errors.Errorf("Evaluation methods is not implemented for ScalarSubQueryExpr")
}

// EvalString returns the string representation of expression.
func (*ScalarSubQueryExpr) EvalString(_ expression.EvalContext, _ chunk.Row) (val string, isNull bool, err error) {
	return "", false, errors.Errorf("Evaluation methods is not implemented for ScalarSubQueryExpr")
}

// EvalDecimal returns the decimal representation of expression.
func (*ScalarSubQueryExpr) EvalDecimal(_ expression.EvalContext, _ chunk.Row) (val *types.MyDecimal, isNull bool, err error) {
	return nil, false, errors.Errorf("Evaluation methods is not implemented for ScalarSubQueryExpr")
}

// EvalTime returns the DATE/DATETIME/TIMESTAMP representation of expression.
func (*ScalarSubQueryExpr) EvalTime(_ expression.EvalContext, _ chunk.Row) (val types.Time, isNull bool, err error) {
	return types.ZeroTime, false, errors.Errorf("Evaluation methods is not implemented for ScalarSubQueryExpr")
}

// EvalDuration returns the duration representation of expression.
func (*ScalarSubQueryExpr) EvalDuration(_ expression.EvalContext, _ chunk.Row) (val types.Duration, isNull bool, err error) {
	return types.ZeroDuration, false, errors.Errorf("Evaluation methods is not implemented for ScalarSubQueryExpr")
}

// EvalJSON returns the JSON representation of expression.
func (*ScalarSubQueryExpr) EvalJSON(_ expression.EvalContext, _ chunk.Row) (val types.BinaryJSON, isNull bool, err error) {
	return types.BinaryJSON{}, false, errors.Errorf("Evaluation methods is not implemented for ScalarSubQueryExpr")
}

// GetType implements the Expression interface.
func (s *ScalarSubQueryExpr) GetType(_ expression.EvalContext) *types.FieldType {
	return s.RetType
}

// Clone copies an expression totally.
func (s *ScalarSubQueryExpr) Clone() expression.Expression {
	ret := *s
	ret.RetType = s.RetType.Clone()
	return &ret
}

// Equal implements the Expression interface.
func (s *ScalarSubQueryExpr) Equal(_ expression.EvalContext, e expression.Expression) bool {
	anotherS, ok := e.(*ScalarSubQueryExpr)
	if !ok {
		return false
	}
	if s.scalarSubqueryColID == anotherS.scalarSubqueryColID {
		return true
	}
	return false
}

// IsCorrelated implements the Expression interface.
func (*ScalarSubQueryExpr) IsCorrelated() bool {
	return false
}

// ConstLevel returns the const level for the expression
func (*ScalarSubQueryExpr) ConstLevel() expression.ConstLevel {
	return expression.ConstNone
}

// Decorrelate implements the Expression interface.
func (s *ScalarSubQueryExpr) Decorrelate(*expression.Schema) expression.Expression {
	return s
}

// resolveIndices implements the Expression interface.
func (*ScalarSubQueryExpr) resolveIndices(*expression.Schema) error {
	return nil
}

// ResolveIndices implements the Expression interface.
func (s *ScalarSubQueryExpr) ResolveIndices(_ *expression.Schema) (expression.Expression, error) {
	return s, nil
}

// ResolveIndicesByVirtualExpr implements the Expression interface.
func (s *ScalarSubQueryExpr) ResolveIndicesByVirtualExpr(_ expression.EvalContext, _ *expression.Schema) (expression.Expression, bool) {
	return s, false
}

// resolveIndicesByVirtualExpr implements the Expression interface.
func (*ScalarSubQueryExpr) resolveIndicesByVirtualExpr(_ expression.EvalContext, _ *expression.Schema) bool {
	return false
}

// RemapColumn implements the Expression interface.
func (s *ScalarSubQueryExpr) RemapColumn(_ map[int64]*expression.Column) (expression.Expression, error) {
	return s, nil
}

// ExplainInfo implements the Expression interface.
func (s *ScalarSubQueryExpr) ExplainInfo(expression.EvalContext) string {
	return s.String()
}

// ExplainNormalizedInfo implements the Expression interface.
func (s *ScalarSubQueryExpr) ExplainNormalizedInfo() string {
	return s.String()
}

// HashCode implements the Expression interface.
func (s *ScalarSubQueryExpr) HashCode() []byte {
	if len(s.hashcode) != 0 {
		return s.hashcode
	}
	s.hashcode = make([]byte, 0, 9)
	s.hashcode = append(s.hashcode, expression.ScalarSubQFlag)
	s.hashcode = codec.EncodeInt(s.hashcode, s.scalarSubqueryColID)
	return s.hashcode
}

// CanonicalHashCode implements the Expression interface.
func (s *ScalarSubQueryExpr) CanonicalHashCode() []byte {
	return s.HashCode()
}

// MemoryUsage implements the Expression interface.
func (s *ScalarSubQueryExpr) MemoryUsage() int64 {
	ret := int64(0)
	if s.evaled {
		ret += s.Constant.MemoryUsage()
	}
	return ret
}

// String implements the Stringer interface.
func (s *ScalarSubQueryExpr) String() string {
	builder := &strings.Builder{}
	fmt.Fprintf(builder, "ScalarQueryCol#%d", s.scalarSubqueryColID)
	return builder.String()
}

// MarshalJSON implements the goJSON.Marshaler interface.
func (s *ScalarSubQueryExpr) MarshalJSON() ([]byte, error) {
	if s.evalErr != nil {
		return nil, s.evalErr
	}
	if s.evaled {
		return s.Constant.MarshalJSON()
	}
	err := s.selfEvaluate()
	if err != nil {
		return nil, err
	}
	return s.Constant.MarshalJSON()
}

// VecEvalInt evaluates this expression in a vectorized manner.
func (*ScalarSubQueryExpr) VecEvalInt(_ expression.EvalContext, _ *chunk.Chunk, _ *chunk.Column) error {
	return errors.Errorf("ScalarSubQueryExpr doesn't implement the vec eval yet")
}

// VecEvalReal evaluates this expression in a vectorized manner.
func (*ScalarSubQueryExpr) VecEvalReal(_ expression.EvalContext, _ *chunk.Chunk, _ *chunk.Column) error {
	return errors.Errorf("ScalarSubQueryExpr doesn't implement the vec eval yet")
}

// VecEvalString evaluates this expression in a vectorized manner.
func (*ScalarSubQueryExpr) VecEvalString(_ expression.EvalContext, _ *chunk.Chunk, _ *chunk.Column) error {
	return errors.Errorf("ScalarSubQueryExpr doesn't implement the vec eval yet")
}

// VecEvalDecimal evaluates this expression in a vectorized manner.
func (*ScalarSubQueryExpr) VecEvalDecimal(_ expression.EvalContext, _ *chunk.Chunk, _ *chunk.Column) error {
	return errors.Errorf("ScalarSubQueryExpr doesn't implement the vec eval yet")
}

// VecEvalTime evaluates this expression in a vectorized manner.
func (*ScalarSubQueryExpr) VecEvalTime(_ expression.EvalContext, _ *chunk.Chunk, _ *chunk.Column) error {
	return errors.Errorf("ScalarSubQueryExpr doesn't implement the vec eval yet")
}

// VecEvalDuration evaluates this expression in a vectorized manner.
func (*ScalarSubQueryExpr) VecEvalDuration(_ expression.EvalContext, _ *chunk.Chunk, _ *chunk.Column) error {
	return errors.Errorf("ScalarSubQueryExpr doesn't implement the vec eval yet")
}

// VecEvalJSON evaluates this expression in a vectorized manner.
func (*ScalarSubQueryExpr) VecEvalJSON(_ expression.EvalContext, _ *chunk.Chunk, _ *chunk.Column) error {
	return errors.Errorf("ScalarSubQueryExpr doesn't implement the vec eval yet")
}

// Vectorized returns whether the expression can be vectorized.
func (*ScalarSubQueryExpr) Vectorized() bool {
	return true
}

// Schema implements the Plan interface.
func (*ScalarSubqueryEvalCtx) Schema() *expression.Schema {
	return nil
}

// ExplainInfo implements the Plan interface.
func (ssctx *ScalarSubqueryEvalCtx) ExplainInfo() string {
	builder := &strings.Builder{}
	fmt.Fprintf(builder, "Output: ")
	for i, id := range ssctx.outputColIDs {
		fmt.Fprintf(builder, "ScalarQueryCol#%d", id)
		if i+1 != len(ssctx.outputColIDs) {
			fmt.Fprintf(builder, ",")
		}
	}
	return builder.String()
}
