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

package infoschema

import (
	"context"

	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/meta/model"
)

func pkdbMaybeAdjustAllocsAfterAutoIncrementEnabled(
	requirement autoid.Requirement,
	dbInfo *model.DBInfo,
	tblInfo *model.TableInfo,
	allocs autoid.Allocators,
	tblVer autoid.AllocOption,
) autoid.Allocators {
	// Modify column / multi-schema change can add/enable AUTO_INCREMENT via ADD/MODIFY COLUMN.
	// If we keep allocators across schema versions, their cached ranges must be rebased to avoid
	// allocating values below the new AUTO_INCREMENT base.
	if tblInfo.GetAutoIncrementColInfo() == nil {
		return allocs
	}

	idCacheOpt := autoid.CustomAutoIncCacheOption(tblInfo.AutoIDCache)

	// If AUTO_INCREMENT routes to RowIDAllocType (AUTO_ID_CACHE != 1), the RowID allocator's unsignedness
	// must match the AUTO_INCREMENT column. We might be reusing a signed RowID allocator created before
	// AUTO_INCREMENT was enabled (when IsAutoIncColUnsigned() was false).
	if !tblInfo.SepAutoInc() && tblInfo.IsAutoIncColUnsigned() {
		allocs = allocs.Filter(func(a autoid.Allocator) bool {
			return a.GetType() != autoid.RowIDAllocType
		})
	}

	if allocs.Get(autoid.RowIDAllocType) == nil {
		// Ensure rowid allocator exists for Get(AUTO_INCREMENT) mapping and for _tidb_rowid allocation.
		newAlloc := autoid.NewAllocator(requirement, dbInfo.ID, tblInfo.ID, tblInfo.IsAutoIncColUnsigned(), autoid.RowIDAllocType, tblVer, idCacheOpt)
		allocs = allocs.Append(newAlloc)
	}
	if tblInfo.SepAutoInc() && allocs.Get(autoid.AutoIncrementType) == nil {
		// AUTO_ID_CACHE=1 uses a dedicated AUTO_INCREMENT allocator.
		newAlloc := autoid.NewAllocator(requirement, dbInfo.ID, tblInfo.ID, tblInfo.IsAutoIncColUnsigned(), autoid.AutoIncrementType, tblVer, idCacheOpt)
		allocs = allocs.Append(newAlloc)
	}

	var needRebase bool
	if tblInfo.IsAutoIncColUnsigned() {
		needRebase = uint64(tblInfo.AutoIncID) > 1
	} else {
		needRebase = tblInfo.AutoIncID > 1
	}
	if needRebase {
		if alloc := allocs.Get(autoid.AutoIncrementType); alloc != nil {
			// Best-effort: Rebase() won't touch KV if the new base is already within the cached range.
			_ = alloc.Rebase(context.Background(), tblInfo.AutoIncID-1, false)
		}
	}
	return allocs
}
