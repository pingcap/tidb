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

	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/metadef"
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
	cache.Insert(infoschema.MockInfoSchema(nil), 0)
	syncer = NewCrossKSSyncer(testStoreWithKS{}, cache, 0, nil, nil, "ks1")
	require.True(t, syncer.skipMDLCheck(map[int64]struct{}{}, cache.GetLatest()))
	require.True(t, syncer.skipMDLCheck(map[int64]struct{}{123: {}}, cache.GetLatest()))
	require.True(t, syncer.skipMDLCheck(map[int64]struct{}{123: {}, 456: {}}, cache.GetLatest()))
	require.True(t, syncer.skipMDLCheck(map[int64]struct{}{metadef.ReservedGlobalIDUpperBound: {}}, cache.GetLatest()))
	require.True(t, syncer.skipMDLCheck(map[int64]struct{}{123: {}, metadef.ReservedGlobalIDUpperBound: {}}, cache.GetLatest()))
	// MockInfoSchema loads mysql.stats_meta with legacy ID 9999 and mysql ID 2.
	require.False(t, syncer.skipMDLCheck(map[int64]struct{}{9999: {}}, cache.GetLatest()))
	require.False(t, syncer.skipMDLCheck(map[int64]struct{}{123: {}, 9999: {}}, cache.GetLatest()))
	require.True(t, syncer.skipMDLCheck(map[int64]struct{}{2: {}}, cache.GetLatest()))
}
