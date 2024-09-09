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
	"github.com/pingcap/tidb/pkg/kv"
)

var _ context.OptionalEvalPropProvider = KVStorePropProvider(nil)

// KVStorePropProvider is used to provide kv.Storage for context
type KVStorePropProvider func() kv.Storage

// Desc returns the description for the property key.
func (KVStorePropProvider) Desc() *context.OptionalEvalPropDesc {
	return context.OptPropKVStore.Desc()
}

// KVStorePropReader is used by expression to get kv.Storage.
type KVStorePropReader struct{}

// RequiredOptionalEvalProps implements the RequireOptionalEvalProps interface.
func (KVStorePropReader) RequiredOptionalEvalProps() context.OptionalEvalPropKeySet {
	return context.OptPropKVStore.AsPropKeySet()
}

// GetKVStore returns a SequenceOperator.
func (KVStorePropReader) GetKVStore(ctx context.EvalContext) (kv.Storage, error) {
	p, err := getPropProvider[KVStorePropProvider](ctx, context.OptPropKVStore)
	if err != nil {
		return nil, err
	}
	return p(), nil
}
