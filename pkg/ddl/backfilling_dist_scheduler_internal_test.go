// Copyright 2026 PingCAP, Inc.
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

package ddl

import (
	"testing"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend/external"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/tici"
	"github.com/stretchr/testify/require"
)

type noPDStore struct {
	kv.Storage
}

func TestGetStorageWithPDAndCodec(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})

	storeWithPDAndCodec, err := getStorageWithPDAndCodec(store)
	require.NoError(t, err)
	require.NotNil(t, storeWithPDAndCodec)

	_, err = getStorageWithPDAndCodec(noPDStore{Storage: store})
	require.ErrorContains(t, err, "does not implement storageWithPDAndCodec")
}

func TestBuildTiCIPreSplitImportShardsRequestUsesUniqueFileCounts(t *testing.T) {
	backfillMeta := &BackfillTaskMeta{
		Job:    model.Job{ID: 1, TableID: 2},
		EleIDs: []int64{10},
	}
	reportGroups := []*tici.PreSplitImportShardMeta{
		{
			EleId:         10,
			StartKey:      []byte("a"),
			EndKey:        []byte("b"),
			TotalKvSize:   12,
			TotalKvCnt:    3,
			DataFileCount: 2,
			StatFileCount: 2,
		},
		{
			EleId:         10,
			StartKey:      []byte("b"),
			EndKey:        []byte("c"),
			TotalKvSize:   8,
			TotalKvCnt:    2,
			DataFileCount: 2,
			StatFileCount: 2,
		},
	}
	kvMetaGroups := []*external.SortedKVMeta{
		{
			MultipleFilesStats: []external.MultipleFilesStat{{
				Filenames: [][2]string{
					{"data/1", "stat/1"},
					{"data/shared", "stat/shared"},
				},
			}},
		},
		{
			MultipleFilesStats: []external.MultipleFilesStat{{
				Filenames: [][2]string{
					{"data/shared", "stat/shared"},
					{"data/2", "stat/2"},
				},
			}},
		},
	}

	req, err := buildTiCIPreSplitImportShardsRequest(backfillMeta, reportGroups, kvMetaGroups)
	require.NoError(t, err)
	require.EqualValues(t, 20, req.TotalKvSize)
	require.EqualValues(t, 5, req.TotalKvCnt)
	require.EqualValues(t, 3, req.DataFileCount)
	require.EqualValues(t, 3, req.StatFileCount)
}
