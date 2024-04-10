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
)

var _ context.OptionalEvalPropProvider = DDLOwnerInfoProvider(nil)
var _ RequireOptionalEvalProps = DDLOwnerPropReader{}

// DDLOwnerInfoProvider is used to provide the DDL owner information.
// It is a function to return whether the current node is ddl owner
type DDLOwnerInfoProvider func() bool

// Desc returns the description for the property key.
func (p DDLOwnerInfoProvider) Desc() *context.OptionalEvalPropDesc {
	return context.OptPropDDLOwnerInfo.Desc()
}

// DDLOwnerPropReader is used by expression to get the DDL owner information.
type DDLOwnerPropReader struct{}

// RequiredOptionalEvalProps implements the RequireOptionalEvalProps interface.
func (r DDLOwnerPropReader) RequiredOptionalEvalProps() context.OptionalEvalPropKeySet {
	return context.OptPropDDLOwnerInfo.AsPropKeySet()
}

// IsDDLOwner returns whether the current node is DDL owner
func (r DDLOwnerPropReader) IsDDLOwner(ctx context.EvalContext) (bool, error) {
	p, err := getPropProvider[DDLOwnerInfoProvider](ctx, context.OptPropDDLOwnerInfo)
	if err != nil {
		return false, err
	}
	return p(), nil
}
