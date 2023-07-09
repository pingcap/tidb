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
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
)

// ScalarSubQueryExpr is a expression placeholder for the non-correlated scalar subqueries which can be evaluated during optimizing phase.
type ScalarSubQueryExpr struct {
	basePlan

	scalarSubQueryID int64
	ScalarSubQuery   PhysicalPlan

	// The context for evaluating the subquery.
	ctx     context.Context
	is      infoschema.InfoSchema
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
	row, err := EvalSubqueryFirstRow(s.ctx, s.ScalarSubQuery, s.is, s.ScalarSubQuery.SCtx())
	if err != nil {
		s.evalErr = err
		s.Constant = *expression.NewNull()
		return err
	}
	if len(row) > 1 {
		s.evalErr = errors.Errorf("ScalarSubQuery doesn't support multiple return values at a time")
		return s.evalErr
	}
	s.Constant.Value = row[0]
	s.evaled = true
	return nil
}

// Eval implements the Expression interface.
func (s *ScalarSubQueryExpr) Eval(_ chunk.Row) (types.Datum, error) {
	if s.evaled {
		return s.Value, nil
	}
	if s.evalErr != nil {
		return s.Value, s.evalErr
	}
	err := s.selfEvaluate()
	if err != nil {
		return s.Value, err
	}
	return s.Value, nil
}

// EvalInt returns the int64 representation of expression.
func (*ScalarSubQueryExpr) EvalInt(_ sessionctx.Context, _ chunk.Row) (val int64, isNull bool, err error) {
	return 0, false, errors.Errorf("Evaluation methods is not implemented for ScalarSubQueryExpr")
}

// EvalReal returns the float64 representation of expression.
func (*ScalarSubQueryExpr) EvalReal(_ sessionctx.Context, _ chunk.Row) (val float64, isNull bool, err error) {
	return 0, false, errors.Errorf("Evaluation methods is not implemented for ScalarSubQueryExpr")
}

// EvalString returns the string representation of expression.
func (*ScalarSubQueryExpr) EvalString(_ sessionctx.Context, _ chunk.Row) (val string, isNull bool, err error) {
	return "", false, errors.Errorf("Evaluation methods is not implemented for ScalarSubQueryExpr")
}

// EvalDecimal returns the decimal representation of expression.
func (*ScalarSubQueryExpr) EvalDecimal(_ sessionctx.Context, _ chunk.Row) (val *types.MyDecimal, isNull bool, err error) {
	return nil, false, errors.Errorf("Evaluation methods is not implemented for ScalarSubQueryExpr")
}

// EvalTime returns the DATE/DATETIME/TIMESTAMP representation of expression.
func (*ScalarSubQueryExpr) EvalTime(_ sessionctx.Context, _ chunk.Row) (val types.Time, isNull bool, err error) {
	return types.ZeroTime, false, errors.Errorf("Evaluation methods is not implemented for ScalarSubQueryExpr")
}

// EvalDuration returns the duration representation of expression.
func (*ScalarSubQueryExpr) EvalDuration(_ sessionctx.Context, _ chunk.Row) (val types.Duration, isNull bool, err error) {
	return types.ZeroDuration, false, errors.Errorf("Evaluation methods is not implemented for ScalarSubQueryExpr")
}

// EvalJSON returns the JSON representation of expression.
func (*ScalarSubQueryExpr) EvalJSON(_ sessionctx.Context, _ chunk.Row) (val types.BinaryJSON, isNull bool, err error) {
	return types.BinaryJSON{}, false, errors.Errorf("Evaluation methods is not implemented for ScalarSubQueryExpr")
}

// GetType implements the Expression interface.
func (s *ScalarSubQueryExpr) GetType() *types.FieldType {
	return s.RetType
}

// Clone copies an expression totally.
func (s *ScalarSubQueryExpr) Clone() expression.Expression {
	ret := *s
	ret.RetType = s.RetType.Clone()
	return &ret
}

// Equal implements the Expression interface.
func (s *ScalarSubQueryExpr) Equal(_ sessionctx.Context, e expression.Expression) bool {
	anotherS, ok := e.(*ScalarSubQueryExpr)
	if !ok {
		return false
	}
	if s.ScalarSubQuery == anotherS.ScalarSubQuery {
		return true
	}
	return false
}

// IsCorrelated implements the Expression interface.
func (*ScalarSubQueryExpr) IsCorrelated() bool {
	return false
}

// ConstItem implements the Expression interface.
func (*ScalarSubQueryExpr) ConstItem(_ *stmtctx.StatementContext) bool {
	return true
}

// Decorrelate implements the Expression interface.
func (s *ScalarSubQueryExpr) Decorrelate(_ *expression.Schema) expression.Expression {
	return s
}

// resolveIndices implements the Expression interface.
func (*ScalarSubQueryExpr) resolveIndices(_ *expression.Schema) error {
	return nil
}

// ResolveIndices implements the Expression interface.
func (s *ScalarSubQueryExpr) ResolveIndices(_ *expression.Schema) (expression.Expression, error) {
	return s, nil
}

// ResolveIndicesByVirtualExpr implements the Expression interface.
func (s *ScalarSubQueryExpr) ResolveIndicesByVirtualExpr(_ *expression.Schema) (expression.Expression, bool) {
	return s, false
}

// resolveIndicesByVirtualExpr implements the Expression interface.
func (*ScalarSubQueryExpr) resolveIndicesByVirtualExpr(_ *expression.Schema) bool {
	return false
}

// RemapColumn implements the Expression interface.
func (s *ScalarSubQueryExpr) RemapColumn(_ map[int64]*expression.Column) (expression.Expression, error) {
	return s, nil
}

// ExplainInfo implements the Expression interface.
func (s *ScalarSubQueryExpr) ExplainInfo() string {
	return s.String()
}

// ExplainNormalizedInfo implements the Expression interface.
func (s *ScalarSubQueryExpr) ExplainNormalizedInfo() string {
	return s.String()
}

// HashCode implements the Expression interface.
func (s *ScalarSubQueryExpr) HashCode(_ *stmtctx.StatementContext) []byte {
	if len(s.hashcode) != 0 {
		return s.hashcode
	}
	s.hashcode = make([]byte, 0, 9)
	s.hashcode = append(s.hashcode, expression.ScalarSubQFlag)
	s.hashcode = codec.EncodeInt(s.hashcode, s.scalarSubQueryID)
	return s.hashcode
}

// MemoryUsage implements the Expression interface.
func (s *ScalarSubQueryExpr) MemoryUsage() int64 {
	ret := int64(0)
	if s.evaled {
		ret += s.Constant.MemoryUsage()
	}
	ret += s.ScalarSubQuery.MemoryUsage()
	return ret
}

// String implements the Stringer interface.
func (s *ScalarSubQueryExpr) String() string {
	builder := &strings.Builder{}
	fmt.Fprintf(builder, "ScalarQuery#%d", s.scalarSubQueryID)
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

// ReverseEval evaluates the only one column value with given function result.
func (s *ScalarSubQueryExpr) ReverseEval(_ *stmtctx.StatementContext, _ types.Datum, _ types.RoundingType) (val types.Datum, err error) {
	if s.evalErr != nil {
		return s.Value, s.evalErr
	}
	if s.evaled {
		return s.Value, nil
	}
	err = s.selfEvaluate()
	if err != nil {
		return s.Value, err
	}
	return s.Value, nil
}

// SupportReverseEval implements the Expression interface.
func (*ScalarSubQueryExpr) SupportReverseEval() bool {
	return true
}

// VecEvalInt evaluates this expression in a vectorized manner.
func (*ScalarSubQueryExpr) VecEvalInt(_ sessionctx.Context, _ *chunk.Chunk, _ *chunk.Column) error {
	return errors.Errorf("ScalarSubQueryExpr doesn't implement the vec eval yet")
}

// VecEvalReal evaluates this expression in a vectorized manner.
func (*ScalarSubQueryExpr) VecEvalReal(_ sessionctx.Context, _ *chunk.Chunk, _ *chunk.Column) error {
	return errors.Errorf("ScalarSubQueryExpr doesn't implement the vec eval yet")
}

// VecEvalString evaluates this expression in a vectorized manner.
func (*ScalarSubQueryExpr) VecEvalString(_ sessionctx.Context, _ *chunk.Chunk, _ *chunk.Column) error {
	return errors.Errorf("ScalarSubQueryExpr doesn't implement the vec eval yet")
}

// VecEvalDecimal evaluates this expression in a vectorized manner.
func (*ScalarSubQueryExpr) VecEvalDecimal(_ sessionctx.Context, _ *chunk.Chunk, _ *chunk.Column) error {
	return errors.Errorf("ScalarSubQueryExpr doesn't implement the vec eval yet")
}

// VecEvalTime evaluates this expression in a vectorized manner.
func (*ScalarSubQueryExpr) VecEvalTime(_ sessionctx.Context, _ *chunk.Chunk, _ *chunk.Column) error {
	return errors.Errorf("ScalarSubQueryExpr doesn't implement the vec eval yet")
}

// VecEvalDuration evaluates this expression in a vectorized manner.
func (*ScalarSubQueryExpr) VecEvalDuration(_ sessionctx.Context, _ *chunk.Chunk, _ *chunk.Column) error {
	return errors.Errorf("ScalarSubQueryExpr doesn't implement the vec eval yet")
}

// VecEvalJSON evaluates this expression in a vectorized manner.
func (*ScalarSubQueryExpr) VecEvalJSON(_ sessionctx.Context, _ *chunk.Chunk, _ *chunk.Column) error {
	return errors.Errorf("ScalarSubQueryExpr doesn't implement the vec eval yet")
}

// Vectorized returns whether the expression can be vectorized.
func (*ScalarSubQueryExpr) Vectorized() bool {
	return true
}

// Schema implements the Plan interface.
func (*ScalarSubQueryExpr) Schema() *expression.Schema {
	return nil
}
