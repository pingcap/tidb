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

package expropt

import "github.com/pingcap/tidb/pkg/expression/exprctx"

var _ exprctx.OptionalEvalPropProvider = StmtCleanupPropProvider(nil)
var _ StmtCleanupProvider = StmtCleanupPropProvider(nil)
var _ RequireOptionalEvalProps = StmtCleanupPropReader{}

// StmtCleanupProvider registers statement-scoped cleanup callbacks.
type StmtCleanupProvider interface {
	RegisterCleanup(func())
}

// StmtCleanupPropProvider provides the statement cleanup hook registrar.
type StmtCleanupPropProvider func(func())

// Desc implements the OptionalEvalPropProvider interface.
func (StmtCleanupPropProvider) Desc() *exprctx.OptionalEvalPropDesc {
	return exprctx.OptPropStmtCleanup.Desc()
}

// RegisterCleanup registers a cleanup callback to be executed when a statement ends.
func (p StmtCleanupPropProvider) RegisterCleanup(cleanup func()) {
	p(cleanup)
}

// StmtCleanupPropReader is used by expression to register statement cleanup callbacks.
type StmtCleanupPropReader struct{}

// RequiredOptionalEvalProps implements the RequireOptionalEvalProps interface.
func (StmtCleanupPropReader) RequiredOptionalEvalProps() exprctx.OptionalEvalPropKeySet {
	return exprctx.OptPropStmtCleanup.AsPropKeySet()
}

// RegisterCleanup registers a cleanup callback through EvalContext optional property.
func (StmtCleanupPropReader) RegisterCleanup(ctx exprctx.EvalContext, cleanup func()) error {
	p, err := getPropProvider[StmtCleanupPropProvider](ctx, exprctx.OptPropStmtCleanup)
	if err != nil {
		return err
	}
	p.RegisterCleanup(cleanup)
	return nil
}
