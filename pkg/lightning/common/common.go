// Copyright 2023 PingCAP, Inc.
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

package common

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/parser/model"
)

const (
	// IndexEngineID is the engine ID for index engine.
	IndexEngineID = -1
)

// DefaultImportantVariables is used in ObtainImportantVariables to retrieve the system
// variables from downstream which may affect KV encode result. The values record the default
// values if missing.
var DefaultImportantVariables = map[string]string{
	"max_allowed_packet":      "67108864",
	"div_precision_increment": "4",
	"time_zone":               "SYSTEM",
	"lc_time_names":           "en_US",
	"default_week_format":     "0",
	"block_encryption_mode":   "aes-128-ecb",
	"group_concat_max_len":    "1024",
	"tidb_backoff_weight":     "6",
}

// DefaultImportVariablesTiDB is used in ObtainImportantVariables to retrieve the system
// variables from downstream in local/importer backend. The values record the default
// values if missing.
var DefaultImportVariablesTiDB = map[string]string{
	"tidb_row_format_version": "1",
}

// AllocGlobalAutoID allocs N consecutive autoIDs from TiDB.
func AllocGlobalAutoID(ctx context.Context, n int64, r autoid.Requirement, dbID int64,
	tblInfo *model.TableInfo) (autoIDBase, autoIDMax int64, err error) {
	allocators, err := GetGlobalAutoIDAlloc(r, dbID, tblInfo)
	if err != nil {
		return 0, 0, err
	}
	// there might be 2 allocators when tblInfo.SepAutoInc is true, and in this case
	// RowIDAllocType will be the last one.
	// we return the value of last Alloc as autoIDBase and autoIDMax, i.e. the value
	// either comes from RowIDAllocType or AutoRandomType.
	for _, alloc := range allocators {
		autoIDBase, autoIDMax, err = alloc.Alloc(ctx, uint64(n), 1, 1)
		if err != nil {
			return 0, 0, err
		}
	}
	return
}

// RebaseGlobalAutoID rebase the autoID base to newBase.
func RebaseGlobalAutoID(ctx context.Context, newBase int64, r autoid.Requirement, dbID int64,
	tblInfo *model.TableInfo) error {
	allocators, err := GetGlobalAutoIDAlloc(r, dbID, tblInfo)
	if err != nil {
		return err
	}
	for _, alloc := range allocators {
		err = alloc.Rebase(ctx, newBase, false)
		if err != nil {
			return err
		}
	}
	return nil
}

// RebaseTableAllocators rebase the allocators of a table.
// This function only rebase a table allocator when its new base is given in
// `bases` param, else it will be skipped.
// base is the max id that have been used by the table, the next usable id will
// be base + 1, see Allocator.Alloc.
func RebaseTableAllocators(ctx context.Context, bases map[autoid.AllocatorType]int64, r autoid.Requirement, dbID int64,
	tblInfo *model.TableInfo) error {
	allocators, err := GetGlobalAutoIDAlloc(r, dbID, tblInfo)
	if err != nil {
		return err
	}
	for _, alloc := range allocators {
		base, ok := bases[alloc.GetType()]
		if !ok {
			continue
		}
		err = alloc.Rebase(ctx, base, false)
		if err != nil {
			return err
		}
	}
	return nil
}

// GetGlobalAutoIDAlloc returns the autoID allocators for a table.
// export it for testing.
func GetGlobalAutoIDAlloc(r autoid.Requirement, dbID int64, tblInfo *model.TableInfo) ([]autoid.Allocator, error) {
	if r == nil || r.Store() == nil {
		return nil, errors.New("internal error: kv store should not be nil")
	}
	if dbID == 0 {
		return nil, errors.New("internal error: dbID should not be 0")
	}

	// We don't need autoid cache here because we allocate all IDs at once.
	// The argument for CustomAutoIncCacheOption is the cache step. Step 1 means no cache,
	// but step 1 will enable an experimental feature, so we use step 2 here.
	//
	// See https://github.com/pingcap/tidb/issues/38442 for more details.
	noCache := autoid.CustomAutoIncCacheOption(2)
	tblVer := autoid.AllocOptionTableInfoVersion(tblInfo.Version)

	hasRowID := TableHasAutoRowID(tblInfo)
	hasAutoIncID := tblInfo.GetAutoIncrementColInfo() != nil
	hasAutoRandID := tblInfo.ContainsAutoRandomBits()

	// TiDB version <= 6.4.0 has some limitations for auto ID.
	// 1. Auto increment ID and auto row ID are using the same RowID allocator.
	//    See https://github.com/pingcap/tidb/issues/982.
	// 2. Auto random column must be a clustered primary key. That is to say,
	//    there is no implicit row ID for tables with auto random column.
	// 3. There is at most one auto column in a table.
	// Therefore, we assume there is only one auto column in a table and use RowID allocator if possible.
	//
	// Since TiDB 6.5.0, row ID and auto ID are using different allocators when tblInfo.SepAutoInc is true
	switch {
	case hasRowID || hasAutoIncID:
		allocators := make([]autoid.Allocator, 0, 2)
		if tblInfo.SepAutoInc() && hasAutoIncID {
			allocators = append(allocators, autoid.NewAllocator(r, dbID, tblInfo.ID, tblInfo.IsAutoIncColUnsigned(),
				autoid.AutoIncrementType, noCache, tblVer))
		}
		// this allocator is NOT used when SepAutoInc=true and auto increment column is clustered.
		allocators = append(allocators, autoid.NewAllocator(r, dbID, tblInfo.ID, tblInfo.IsAutoIncColUnsigned(),
			autoid.RowIDAllocType, noCache, tblVer))
		return allocators, nil
	case hasAutoRandID:
		return []autoid.Allocator{autoid.NewAllocator(r, dbID, tblInfo.ID, tblInfo.IsAutoRandomBitColUnsigned(),
			autoid.AutoRandomType, noCache, tblVer)}, nil
	default:
		return nil, errors.Errorf("internal error: table %s has no auto ID", tblInfo.Name)
	}
}
