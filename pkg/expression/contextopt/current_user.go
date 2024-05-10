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
	"github.com/pingcap/tidb/pkg/parser/auth"
)

var _ context.OptionalEvalPropProvider = CurrentUserPropProvider(nil)
var _ RequireOptionalEvalProps = CurrentUserPropReader{}

// CurrentUserPropProvider is a provider to get the current user
type CurrentUserPropProvider func() (*auth.UserIdentity, []*auth.RoleIdentity)

// Desc returns the description for the property key.
func (p CurrentUserPropProvider) Desc() *context.OptionalEvalPropDesc {
	return context.OptPropCurrentUser.Desc()
}

// CurrentUserPropReader is used by expression to read property context.OptPropCurrentUser
type CurrentUserPropReader struct{}

// RequiredOptionalEvalProps implements the RequireOptionalEvalProps interface.
func (r CurrentUserPropReader) RequiredOptionalEvalProps() context.OptionalEvalPropKeySet {
	return context.OptPropCurrentUser.AsPropKeySet()
}

// CurrentUser returns the current user
func (r CurrentUserPropReader) CurrentUser(ctx context.EvalContext) (*auth.UserIdentity, error) {
	p, err := r.getProvider(ctx)
	if err != nil {
		return nil, err
	}
	user, _ := p()
	return user, nil
}

// ActiveRoles returns the active roles
func (r CurrentUserPropReader) ActiveRoles(ctx context.EvalContext) ([]*auth.RoleIdentity, error) {
	p, err := r.getProvider(ctx)
	if err != nil {
		return nil, err
	}
	_, roles := p()
	return roles, nil
}

func (r CurrentUserPropReader) getProvider(ctx context.EvalContext) (CurrentUserPropProvider, error) {
	return getPropProvider[CurrentUserPropProvider](ctx, context.OptPropCurrentUser)
}
