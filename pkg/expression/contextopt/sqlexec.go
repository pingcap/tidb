// Copyright 2024 PingCAP, Inc.
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

package contextopt

import (
	"context"

	exprctx "github.com/pingcap/tidb/pkg/expression/context"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
)

// SQLExecutor provides a subset of methods in RestrictedSQLExecutor.
var _ SQLExecutor = sqlexec.RestrictedSQLExecutor(nil)

// SQLExecutor is the interface for SQL executing in expression.
// We do not `sqlexec.SQLExecutor` to limit expression to use specified methods only.
type SQLExecutor interface {
	ExecRestrictedSQL(
		ctx context.Context,
		opts []sqlexec.OptionFuncAlias,
		sql string,
		args ...any,
	) ([]chunk.Row, []*ast.ResultField, error)
}

// SQLExecutorPropProvider provides the SQLExecutor
type SQLExecutorPropProvider func() (SQLExecutor, error)

// Desc returns the description for the property key.
func (SQLExecutorPropProvider) Desc() *exprctx.OptionalEvalPropDesc {
	return exprctx.OptPropSQLExecutor.Desc()
}

// SQLExecutorPropReader is used by expression to get sql executor
type SQLExecutorPropReader struct{}

// RequiredOptionalEvalProps implements the RequireOptionalEvalProps interface.
func (SQLExecutorPropReader) RequiredOptionalEvalProps() exprctx.OptionalEvalPropKeySet {
	return exprctx.OptPropSQLExecutor.AsPropKeySet()
}

// GetSQLExecutor returns a SQLExecutor.
func (SQLExecutorPropReader) GetSQLExecutor(ctx exprctx.EvalContext) (SQLExecutor, error) {
	p, err := getPropProvider[SQLExecutorPropProvider](ctx, exprctx.OptPropSQLExecutor)
	if err != nil {
		return nil, err
	}
	return p()
}
