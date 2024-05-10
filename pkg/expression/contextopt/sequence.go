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

// SequenceOperator is the interface for to operate sequence.
type SequenceOperator interface {
	// GetSequenceID gets the sequence id.
	GetSequenceID() int64
	// GetSequenceNextVal gets the next value of the sequence.
	GetSequenceNextVal() (int64, error)
	// SetSequenceVal sets the sequence value.
	// The returned bool indicates the newVal is already under the base.
	SetSequenceVal(newVal int64) (int64, bool, error)
}

var _ context.OptionalEvalPropProvider = SequenceOperatorProvider(nil)

// SequenceOperatorProvider is the function to provide SequenceOperator.
type SequenceOperatorProvider func(db, name string) (SequenceOperator, error)

// Desc returns the description for the property key.
func (SequenceOperatorProvider) Desc() *context.OptionalEvalPropDesc {
	return context.OptPropSequenceOperator.Desc()
}

// SequenceOperatorPropReader is used by expression to get SequenceOperator.
type SequenceOperatorPropReader struct{}

// RequiredOptionalEvalProps implements the RequireOptionalEvalProps interface.
func (SequenceOperatorPropReader) RequiredOptionalEvalProps() context.OptionalEvalPropKeySet {
	return context.OptPropSequenceOperator.AsPropKeySet()
}

// GetSequenceOperator returns a SequenceOperator.
func (SequenceOperatorPropReader) GetSequenceOperator(ctx context.EvalContext, db, name string) (SequenceOperator, error) {
	p, err := getPropProvider[SequenceOperatorProvider](ctx, context.OptPropSequenceOperator)
	if err != nil {
		return nil, err
	}
	return p(db, name)
}
