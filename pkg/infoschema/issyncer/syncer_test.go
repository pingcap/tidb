// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package issyncer

import (
	"testing"

	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/metadef"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/stretchr/testify/require"
)

func TestSyncerSkipMDLCheck(t *testing.T) {
	syncer := New(nil, nil, 0, nil, nil, nil)
	require.False(t, syncer.skipMDLCheck(map[int64]struct{}{}, nil))
	require.False(t, syncer.skipMDLCheck(map[int64]struct{}{123: {}}, nil))
	require.False(t, syncer.skipMDLCheck(map[int64]struct{}{123: {}, 456: {}}, nil))
	require.False(t, syncer.skipMDLCheck(map[int64]struct{}{metadef.ReservedGlobalIDUpperBound: {}}, nil))
	require.False(t, syncer.skipMDLCheck(map[int64]struct{}{123: {}, metadef.ReservedGlobalIDUpperBound: {}}, nil))

	cache := infoschema.NewCache(nil, 1)
	db := &model.DBInfo{ID: 2, Name: ast.NewCIStr("mysql"), State: model.StatePublic}
	db.Deprecated.Tables = []*model.TableInfo{
		{ID: 9999, DBID: db.ID, Name: ast.NewCIStr("stats_meta"), State: model.StatePublic},
		{ID: 10000, DBID: db.ID, Name: ast.NewCIStr("custom_table"), State: model.StatePublic},
	}
	builder := infoschema.NewBuilder(nil, 0, nil, infoschema.NewData(), false)
	require.NoError(t, builder.InitWithDBInfos([]*model.DBInfo{db}, nil, nil, nil, 1))
	cache.Insert(builder.Build(0), 0)
	syncer = NewCrossKSSyncer(testStoreWithKS{}, cache, 0, nil, nil, "ks1")
	for _, isLegacyNextGen := range []bool{false, true} {
		syncer.loader.legacyNextGen.Store(&isLegacyNextGen)
		require.True(t, syncer.skipMDLCheck(map[int64]struct{}{}, cache.GetLatest()))
		for _, tc := range []struct {
			id                     int64
			skipNative, skipLegacy bool
		}{
			{0, true, true},
			{123, true, true},
			{9999, true, false},
			{10000, true, true},
			{metadef.GlobalPrivTableID, false, true},
		} {
			id, wantSkip := tc.id, tc.skipNative
			if isLegacyNextGen {
				wantSkip = tc.skipLegacy
			}
			if kerneltype.IsClassic() {
				wantSkip = true
			}
			require.Equal(t, wantSkip, syncer.skipMDLCheck(map[int64]struct{}{id: {}}, cache.GetLatest()), "isLegacyNextGen=%t id=%d", isLegacyNextGen, id)
			require.Equal(t, wantSkip, syncer.skipMDLCheck(map[int64]struct{}{123: {}, id: {}}, cache.GetLatest()))
		}
	}
}
