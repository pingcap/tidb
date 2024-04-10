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

package metautil

import (
	"context"
	"fmt"
	"testing"

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/encryptionpb"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/statistics/handle/types"
	"github.com/pingcap/tidb/pkg/statistics/handle/util"
	tidbutil "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func newJsonColumn(magic int64) *util.JSONColumn {
	statsVer := magic
	return &util.JSONColumn{
		Histogram: &tipb.Histogram{
			Ndv: magic,
			Buckets: []*tipb.Bucket{
				{
					Count:      magic,
					LowerBound: []byte(fmt.Sprintf("%d", magic)),
					UpperBound: []byte(fmt.Sprintf("%d", magic)),
					Repeats:    magic,
				},
			},
		},
		CMSketch: &tipb.CMSketch{
			DefaultValue: uint64(magic),
		},
		FMSketch: &tipb.FMSketch{
			Mask: uint64(magic),
		},
		StatsVer:  &statsVer,
		NullCount: magic,
	}
}

func TestStatsWriter(t *testing.T) {
	ctx := context.Background()

	testCases := []encryptTest{
		{
			method: encryptionpb.EncryptionMethod_PLAINTEXT,
		},
		{
			method:   encryptionpb.EncryptionMethod_AES128_CTR,
			rightKey: "0123456789012345",
			wrongKey: "012345678901234",
		},
		{
			method:   encryptionpb.EncryptionMethod_AES192_CTR,
			rightKey: "012345678901234567890123",
			wrongKey: "0123456789012345678901234",
		},
		{
			method:   encryptionpb.EncryptionMethod_AES256_CTR,
			rightKey: "01234567890123456789012345678901",
			wrongKey: "01234567890123456789012345678902",
		},
	}
	fakeJsonTables := map[int64]*util.JSONTable{
		1: {
			Columns:      map[string]*util.JSONColumn{"test": newJsonColumn(1)},
			Indices:      map[string]*util.JSONColumn{"test": newJsonColumn(2)},
			DatabaseName: "test-schema",
			TableName:    "test-table",
			ExtStats: []*util.JSONExtendedStats{
				{
					StatsName:  "test",
					StringVals: "test",
					ColIDs:     []int64{1, 2, 3},
				},
			},
			Count: 1,
		},
		2: {
			Columns:      map[string]*util.JSONColumn{"test": newJsonColumn(3)},
			Indices:      map[string]*util.JSONColumn{"test": newJsonColumn(4)},
			DatabaseName: "test-schema",
			TableName:    "test-table-1",
			ExtStats: []*util.JSONExtendedStats{
				{
					StatsName:  "test",
					StringVals: "test",
					ColIDs:     []int64{3, 4, 5},
				},
			},
			Count: 2,
		},
	}

	rewriteIDs := map[int64]int64{1: 10, 2: 20}
	rerewriteIDs := map[int64]int64{10: 1, 20: 2}

	base := t.TempDir()
	stg, err := storage.NewLocalStorage(base)
	require.NoError(t, err)
	for _, v := range testCases {
		cipher := backuppb.CipherInfo{
			CipherType: v.method,
			CipherKey:  []byte(v.rightKey),
		}
		statsWriter := newStatsWriter(stg, &cipher)

		// set the maxStatsJsonTableSize less enough
		maxStatsJsonTableSize = 1
		inlineSize = 1
		err := statsWriter.BackupStats(ctx, fakeJsonTables[1], 1)
		require.NoError(t, err)

		// set the maxStatsJsonTableSize back
		maxStatsJsonTableSize = 32 * 1024 * 1024
		inlineSize = 8 * 1024
		err = statsWriter.BackupStats(ctx, fakeJsonTables[2], 2)
		require.NoError(t, err)
		statsFileIndexes, err := statsWriter.BackupStatsDone(ctx)
		require.NoError(t, err)

		controlWorker := tidbutil.NewWorkerPool(2, "test")
		eg, ectx := errgroup.WithContext(ctx)
		taskCh := make(chan *types.PartitionStatisticLoadTask)
		controlWorker.ApplyOnErrorGroup(eg, func() error {
			return downloadStats(ectx, stg, &cipher, statsFileIndexes, rewriteIDs, taskCh)
		})
		controlWorker.ApplyOnErrorGroup(eg, func() error {
			for task := range taskCh {
				expectedJsonTable := fakeJsonTables[rerewriteIDs[task.PhysicalID]]
				require.Equal(t, expectedJsonTable, task.JSONTable)
			}
			return nil
		})
		err = eg.Wait()
		require.NoError(t, err)
	}
}
