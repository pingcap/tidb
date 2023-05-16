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
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/parser/model"
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
}

// DefaultImportVariablesTiDB is used in ObtainImportantVariables to retrieve the system
// variables from downstream in local/importer backend. The values record the default
// values if missing.
var DefaultImportVariablesTiDB = map[string]string{
	"tidb_row_format_version": "1",
}

// AllocGlobalAutoID allocs N consecutive autoIDs from TiDB.
func AllocGlobalAutoID(ctx context.Context, n int64, store kv.Storage, dbID int64,
	tblInfo *model.TableInfo) (autoIDBase, autoIDMax int64, err error) {
	alloc, err := getGlobalAutoIDAlloc(store, dbID, tblInfo)
	if err != nil {
		return 0, 0, err
	}
	return alloc.Alloc(ctx, uint64(n), 1, 1)
}

// RebaseGlobalAutoID rebase the autoID base to newBase.
func RebaseGlobalAutoID(ctx context.Context, newBase int64, store kv.Storage, dbID int64,
	tblInfo *model.TableInfo) error {
	alloc, err := getGlobalAutoIDAlloc(store, dbID, tblInfo)
	if err != nil {
		return err
	}
	return alloc.Rebase(ctx, newBase, false)
}

func getGlobalAutoIDAlloc(store kv.Storage, dbID int64, tblInfo *model.TableInfo) (autoid.Allocator, error) {
	if store == nil {
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

	// Current TiDB has some limitations for auto ID.
	// 1. Auto increment ID and auto row ID are using the same RowID allocator.
	//    See https://github.com/pingcap/tidb/issues/982.
	// 2. Auto random column must be a clustered primary key. That is to say,
	//    there is no implicit row ID for tables with auto random column.
	// 3. There is at most one auto column in a table.
	// Therefore, we assume there is only one auto column in a table and use RowID allocator if possible.
	switch {
	case hasRowID || hasAutoIncID:
		return autoid.NewAllocator(store, dbID, tblInfo.ID, tblInfo.IsAutoIncColUnsigned(),
			autoid.RowIDAllocType, noCache, tblVer), nil
	case hasAutoRandID:
		return autoid.NewAllocator(store, dbID, tblInfo.ID, tblInfo.IsAutoRandomBitColUnsigned(),
			autoid.AutoRandomType, noCache, tblVer), nil
	default:
		return nil, errors.Errorf("internal error: table %s has no auto ID", tblInfo.Name)
	}
}
