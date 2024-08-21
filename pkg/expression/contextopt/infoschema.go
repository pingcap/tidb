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
	infoschema "github.com/pingcap/tidb/pkg/infoschema/context"
)

// InfoSchemaPropProvider is the function to provide information schema.
type InfoSchemaPropProvider func(isDomain bool) infoschema.MetaOnlyInfoSchema

// Desc returns the description for the property key.
func (InfoSchemaPropProvider) Desc() *context.OptionalEvalPropDesc {
	return context.OptPropInfoSchema.Desc()
}

// InfoSchemaPropReader is used to get the information schema.
type InfoSchemaPropReader struct{}

// RequiredOptionalEvalProps implements the RequireOptionalEvalProps interface.
func (InfoSchemaPropReader) RequiredOptionalEvalProps() context.OptionalEvalPropKeySet {
	return context.OptPropInfoSchema.AsPropKeySet()
}

// GetSessionInfoSchema returns session information schema.
func (InfoSchemaPropReader) GetSessionInfoSchema(ctx context.EvalContext) (infoschema.MetaOnlyInfoSchema, error) {
	p, err := getPropProvider[InfoSchemaPropProvider](ctx, context.OptPropInfoSchema)
	if err != nil {
		return nil, err
	}
	return p(false), nil
}

// GetDomainInfoSchema return domain information schema.
func (InfoSchemaPropReader) GetDomainInfoSchema(ctx context.EvalContext) (infoschema.MetaOnlyInfoSchema, error) {
	p, err := getPropProvider[InfoSchemaPropProvider](ctx, context.OptPropInfoSchema)
	if err != nil {
		return nil, err
	}
	return p(true), nil
}
