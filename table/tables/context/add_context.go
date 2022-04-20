// Copyright 2021 PingCAP, Inc.
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

package context

import (
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
)

// CommonAddRecordCtx is used in `AddRecord` to avoid memory malloc for some temp slices.
// This is useful in lightning parse Row data to key-values pairs. This can gain upto 5%  performance
// improvement in lightning's local mode.
type CommonAddRecordCtx struct {
	ColIDs []int64
	Row    []types.Datum
}

// NewCommonAddRecordCtx create a context used for `AddRecord`
func NewCommonAddRecordCtx(size int) *CommonAddRecordCtx {
	return &CommonAddRecordCtx{
		ColIDs: make([]int64, 0, size),
		Row:    make([]types.Datum, 0, size),
	}
}

// commonAddRecordKey is used as key in `sessionctx.Context.Value(key)`
type commonAddRecordKey struct{}

// String implement `stringer.String` for CommonAddRecordKey
func (c commonAddRecordKey) String() string {
	return "_common_add_record_context_key"
}

// AddRecordCtxKey is key in `sessionctx.Context` for CommonAddRecordCtx
var AddRecordCtxKey = commonAddRecordKey{}

// SetAddRecordCtx set a CommonAddRecordCtx to session context
func SetAddRecordCtx(ctx sessionctx.Context, r *CommonAddRecordCtx) {
	ctx.SetValue(AddRecordCtxKey, r)
}
