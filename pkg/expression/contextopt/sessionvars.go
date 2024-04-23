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
	"github.com/pingcap/tidb/pkg/expression/context"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/intest"
)

var _ context.OptionalEvalPropProvider = &SessionVarsPropProvider{}
var _ RequireOptionalEvalProps = SessionVarsPropReader{}

// SessionVarsPropProvider is a provider to get the session variables
type SessionVarsPropProvider struct {
	vars variable.SessionVarsProvider
}

// NewSessionVarsProvider returns a new SessionVarsPropProvider
func NewSessionVarsProvider(provider variable.SessionVarsProvider) *SessionVarsPropProvider {
	intest.AssertNotNil(provider)
	return &SessionVarsPropProvider{vars: provider}
}

// Desc implements the OptionalEvalPropProvider interface.
func (p *SessionVarsPropProvider) Desc() *context.OptionalEvalPropDesc {
	return context.OptPropSessionVars.Desc()
}

// SessionVarsPropReader is used by expression to read property context.OptPropSessionVars
type SessionVarsPropReader struct{}

// RequiredOptionalEvalProps implements the RequireOptionalEvalProps interface.
func (SessionVarsPropReader) RequiredOptionalEvalProps() context.OptionalEvalPropKeySet {
	return context.OptPropSessionVars.AsPropKeySet()
}

// GetSessionVars returns the session vars from the context
func (SessionVarsPropReader) GetSessionVars(ctx context.EvalContext) (*variable.SessionVars, error) {
	p, err := getPropProvider[*SessionVarsPropProvider](ctx, context.OptPropSessionVars)
	if err != nil {
		return nil, err
	}

	if intest.InTest {
		context.AssertLocationWithSessionVars(ctx.Location(), p.vars.GetSessionVars())
	}

	return p.vars.GetSessionVars(), nil
}
