// Copyright 2026 PingCAP, Inc.
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

import (
	"context"

	"github.com/pingcap/tidb/pkg/expression/exprctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/intest"
)

var _ exprctx.OptionalEvalPropProvider = SessionContextPropProvider(nil)
var _ RequireOptionalEvalProps = SessionContextPropReader{}

// SessionContext provides the session APIs needed to evaluate EMBED_TEXT.
// Keeping this interface narrow prevents this low-level package from depending
// on the full sessionctx.Context and creating a dependency cycle.
type SessionContext interface {
	GetTraceCtx() context.Context
	GetSessionVars() *variable.SessionVars
	GetDomain() any
}

// SessionContextPropProvider provides the current session context.
type SessionContextPropProvider func() SessionContext

// NewSessionContextPropProvider creates a provider for the current session context.
func NewSessionContextPropProvider(sctx SessionContext) SessionContextPropProvider {
	intest.AssertNotNil(sctx)
	return func() SessionContext {
		return sctx
	}
}

// Desc implements the OptionalEvalPropProvider interface.
func (SessionContextPropProvider) Desc() *exprctx.OptionalEvalPropDesc {
	return exprctx.OptPropSessionContext.Desc()
}

// SessionContextPropReader is used by expressions to read the current session context.
type SessionContextPropReader struct{}

// RequiredOptionalEvalProps implements the RequireOptionalEvalProps interface.
func (SessionContextPropReader) RequiredOptionalEvalProps() exprctx.OptionalEvalPropKeySet {
	return exprctx.OptPropSessionContext.AsPropKeySet()
}

// GetSessionContext returns the current session context.
func (SessionContextPropReader) GetSessionContext(ctx exprctx.EvalContext) (SessionContext, error) {
	p, err := getPropProvider[SessionContextPropProvider](ctx, exprctx.OptPropSessionContext)
	if err != nil {
		return nil, err
	}
	return p(), nil
}
