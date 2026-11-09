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

package tables

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
)

func pkdbGetWriteOnlyColumnInsertValue(ctx context.Context, sctx table.MutateContext, tbl *TableCommon, col *table.Column) (types.Datum, error) {
	if !mysql.HasAutoIncrementFlag(col.GetFlag()) {
		return table.GetColOriginDefaultValue(sctx.GetExprCtx(), col.ToInfo())
	}

	// For online DDL, a newly-added AUTO_INCREMENT column is not public yet, so the executor won't fill the
	// value. We need to allocate a value here to avoid writing duplicated `0`s, otherwise adding a unique
	// key/PK on this column in the same multi-schema change might fail.
	increment, offset := int64(1), int64(1)
	if sessCtx, ok := sctx.(sessionctx.Context); ok {
		vars := sessCtx.GetSessionVars()
		inc := vars.AutoIncrementIncrement
		off := vars.AutoIncrementOffset
		if off > inc {
			off = 1
		}
		increment, offset = int64(inc), int64(off)
	}
	alloc := tbl.Allocators(sctx).Get(autoid.AutoIncrementType)
	if alloc == nil {
		return types.Datum{}, errors.New("auto_increment allocator not found")
	}
	// Alloc() returns a range (min, max], the allocated value is max for n=1.
	_, newID, err := alloc.Alloc(ctx, 1, increment, offset)
	if err != nil {
		return types.Datum{}, err
	}
	var value types.Datum
	value.SetAutoID(newID, col.GetFlag())
	value, err = table.CastColumnValue(sctx.GetExprCtx(), value, col.ToInfo(), false, false)
	if err == nil && value.GetInt64() < newID {
		// Auto ID is out of range, avoid truncation causing duplicates.
		return types.Datum{}, autoid.ErrAutoincReadFailed
	}
	if err != nil {
		return types.Datum{}, err
	}
	return value, nil
}
