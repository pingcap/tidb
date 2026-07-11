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

package infoschema

import (
	"errors"
	"testing"

	"github.com/ngaut/pools"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/stretchr/testify/require"
)

// GetCache is exported for testing.
func (is *infoschemaV2) HasCache(tableID int64, schemaVersion int64) bool {
	key := tableCacheKey{tableID, schemaVersion}
	_, found := is.tableCache.Get(key)
	return found
}

func TestInfoSchemaAddDel(t *testing.T) {
	is := newInfoSchema(nil, nil)
	is.addSchema(&schemaTables{
		dbInfo: &model.DBInfo{ID: 1, Name: ast.NewCIStr("test")},
	})
	require.Contains(t, is.schemaMap, "test")
	require.Contains(t, is.schemaID2Name, int64(1))
	is.delSchema(&model.DBInfo{ID: 1, Name: ast.NewCIStr("test")})
	require.Empty(t, is.schemaMap)
	require.Empty(t, is.schemaID2Name)
}

func TestLoadMaskingPoliciesRetryOnGenericError(t *testing.T) {
	is := newInfoSchema(nil, nil)
	calls := 0
	is.factory = func() (pools.Resource, error) {
		calls++
		return nil, errors.New("temporary load failure")
	}

	is.loadMaskingPoliciesIfNeeded()
	require.False(t, is.maskingPoliciesLoaded)
	require.Equal(t, 1, calls)

	is.loadMaskingPoliciesIfNeeded()
	require.False(t, is.maskingPoliciesLoaded)
	require.Equal(t, 2, calls)
}

func TestLoadMaskingPoliciesNoRetryWhenTableNotReady(t *testing.T) {
	is := newInfoSchema(nil, nil)
	calls := 0
	is.factory = func() (pools.Resource, error) {
		calls++
		return nil, ErrTableNotExists.GenWithStackByArgs("mysql", "tidb_masking_policy")
	}

	is.loadMaskingPoliciesIfNeeded()
	require.True(t, is.maskingPoliciesLoaded)
	require.Equal(t, 1, calls)

	is.loadMaskingPoliciesIfNeeded()
	require.Equal(t, 1, calls)
}

func TestInitWithOldInfoSchemaCopiesMaskingLoadedState(t *testing.T) {
	oldIS := newInfoSchema(nil, nil)
	oldIS.maskingPoliciesLoaded = true
	oldIS.maskingPolicyTableColumnMap[42] = map[int64]*model.MaskingPolicyInfo{
		7: {
			ID:       1,
			Name:     ast.NewCIStr("p"),
			TableID:  42,
			ColumnID: 7,
		},
	}

	builder := NewBuilder(nil, 0, nil, NewData(), false)
	require.NoError(t, builder.InitWithOldInfoSchema(oldIS))

	newIS := builder.infoSchema
	require.True(t, newIS.maskingPoliciesLoaded)
	policy, ok := newIS.maskingPolicyTableColumnMap[42][7]
	require.True(t, ok)
	require.Equal(t, int64(1), policy.ID)
}

func TestInitWithOldInfoSchemaDoesNotInheritMaskingLoadChannel(t *testing.T) {
	oldIS := newInfoSchema(nil, nil)
	oldIS.maskingPoliciesLoaded = false
	oldIS.maskingPoliciesLoadCh = make(chan struct{})
	oldIS.maskingPolicyTableColumnMap[42] = map[int64]*model.MaskingPolicyInfo{
		7: {
			ID:       1,
			Name:     ast.NewCIStr("p"),
			TableID:  42,
			ColumnID: 7,
		},
	}

	builder := NewBuilder(nil, 0, nil, NewData(), false)
	require.NoError(t, builder.InitWithOldInfoSchema(oldIS))

	newIS := builder.infoSchema
	require.Nil(t, newIS.maskingPoliciesLoadCh)
	require.False(t, newIS.maskingPoliciesLoaded)
	policy, ok := newIS.maskingPolicyTableColumnMap[42][7]
	require.True(t, ok)
	require.Equal(t, int64(1), policy.ID)
}

func TestMaskingPolicyByNameAmbiguous(t *testing.T) {
	is := newInfoSchema(nil, nil)
	is.maskingPoliciesLoaded = true
	is.maskingPolicyTableColumnMap[101] = map[int64]*model.MaskingPolicyInfo{
		11: {
			ID:       1,
			Name:     ast.NewCIStr("dup"),
			TableID:  101,
			ColumnID: 11,
		},
	}
	is.maskingPolicyTableColumnMap[202] = map[int64]*model.MaskingPolicyInfo{
		22: {
			ID:       2,
			Name:     ast.NewCIStr("dup"),
			TableID:  202,
			ColumnID: 22,
		},
	}

	policy, ok := is.MaskingPolicyByName(ast.NewCIStr("dup"))
	require.False(t, ok)
	require.Nil(t, policy)
}

func TestNormalizeMaskingPolicyTableIDs(t *testing.T) {
	ids, hasFilter := normalizeMaskingPolicyTableIDs(nil)
	require.False(t, hasFilter)
	require.Nil(t, ids)

	ids, hasFilter = normalizeMaskingPolicyTableIDs([]int64{0, -1, 42, 42, 7})
	require.True(t, hasFilter)
	require.Equal(t, []int64{7, 42}, ids)

	ids, hasFilter = normalizeMaskingPolicyTableIDs([]int64{0, -1})
	require.True(t, hasFilter)
	require.Empty(t, ids)
}
