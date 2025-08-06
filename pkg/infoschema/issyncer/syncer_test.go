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

	"github.com/pingcap/tidb/pkg/meta/metadef"
	"github.com/stretchr/testify/require"
)

func TestSyncerSkipMDLCheck(t *testing.T) {
	syncer := New(nil, nil, 0, nil, nil)
	require.False(t, syncer.skipMDLCheck(map[int64]struct{}{}))
	require.False(t, syncer.skipMDLCheck(map[int64]struct{}{123: {}}))
	require.False(t, syncer.skipMDLCheck(map[int64]struct{}{123: {}, 456: {}}))
	require.False(t, syncer.skipMDLCheck(map[int64]struct{}{metadef.ReservedGlobalIDUpperBound: {}}))
	require.False(t, syncer.skipMDLCheck(map[int64]struct{}{123: {}, metadef.ReservedGlobalIDUpperBound: {}}))

	syncer = NewCrossKSSyncer(testStoreWithKS{}, nil, 0, nil, nil, "ks1")
	require.True(t, syncer.skipMDLCheck(map[int64]struct{}{}))
	require.True(t, syncer.skipMDLCheck(map[int64]struct{}{123: {}}))
	require.True(t, syncer.skipMDLCheck(map[int64]struct{}{123: {}, 456: {}}))
	require.False(t, syncer.skipMDLCheck(map[int64]struct{}{metadef.ReservedGlobalIDUpperBound: {}}))
	require.False(t, syncer.skipMDLCheck(map[int64]struct{}{123: {}, metadef.ReservedGlobalIDUpperBound: {}}))
}
