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
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression/context"
	"github.com/pingcap/tidb/pkg/util/intest"
)

// RequireOptionalEvalProps is the interface for the function that requires optional evaluation properties or not.
type RequireOptionalEvalProps interface {
	// RequiredOptionalEvalProps returns the optional properties that this function requires.
	// If the returned `OptionalEvalPropKeySet` is empty,
	// it means this function does not require any optional properties.
	RequiredOptionalEvalProps() context.OptionalEvalPropKeySet
}

// OptionalEvalPropProviders contains some evaluation property providers in EvalContext.
type OptionalEvalPropProviders [context.OptPropsCnt]context.OptionalEvalPropProvider

// Contains checks whether the provider by key exists.
func (o *OptionalEvalPropProviders) Contains(key context.OptionalEvalPropKey) bool {
	return o[key] != nil
}

// Get gets the provider by key.
func (o *OptionalEvalPropProviders) Get(key context.OptionalEvalPropKey) (context.OptionalEvalPropProvider, bool) {
	if key < 0 || int(key) >= context.OptPropsCnt {
		return nil, false
	}

	if val := o[key]; val != nil {
		intest.Assert(key == val.Desc().Key())
		return val, true
	}
	return nil, false
}

// Add adds an optional property
func (o *OptionalEvalPropProviders) Add(val context.OptionalEvalPropProvider) {
	intest.AssertFunc(func() bool {
		intest.AssertNotNil(val)
		switch val.Desc().Key() {
		case context.OptPropCurrentUser:
			_, ok := val.(CurrentUserPropProvider)
			intest.Assert(ok)
		case context.OptPropSessionVars:
			_, ok := val.(*SessionVarsPropProvider)
			intest.Assert(ok)
		case context.OptPropInfoSchema:
			_, ok := val.(InfoSchemaPropProvider)
			intest.Assert(ok)
		case context.OptPropKVStore:
			_, ok := val.(KVStorePropProvider)
			intest.Assert(ok)
		case context.OptPropSQLExecutor:
			_, ok := val.(SQLExecutorPropProvider)
			intest.Assert(ok)
		case context.OptPropAdvisoryLock:
			_, ok := val.(*AdvisoryLockPropProvider)
			intest.Assert(ok)
		case context.OptPropDDLOwnerInfo:
			_, ok := val.(DDLOwnerInfoProvider)
			intest.Assert(ok)
		case context.OptPropSequenceOperator:
			_, ok := val.(SequenceOperatorProvider)
			intest.Assert(ok)
		default:
			intest.Assert(false)
		}
		return true
	})
	o[val.Desc().Key()] = val
}

// PropKeySet returns the set for optional evaluation properties in EvalContext.
func (o *OptionalEvalPropProviders) PropKeySet() (set context.OptionalEvalPropKeySet) {
	for _, p := range o {
		if p != nil {
			set = set.Add(p.Desc().Key())
		}
	}
	return
}

func getPropProvider[T context.OptionalEvalPropProvider](ctx context.EvalContext, key context.OptionalEvalPropKey) (p T, _ error) {
	intest.AssertFunc(func() bool {
		var stub T
		intest.Assert(stub.Desc().Key() == key)
		return true
	})

	val, ok := ctx.GetOptionalPropProvider(key)
	if !ok {
		return p, errors.Errorf("optional property: '%s' not exists in EvalContext", key)
	}

	p, ok = val.(T)
	if !ok {
		intest.Assert(false)
		return p, errors.Errorf("cannot cast OptionalEvalPropProvider to %T for key '%s'", p, key)
	}

	return p, nil
}
