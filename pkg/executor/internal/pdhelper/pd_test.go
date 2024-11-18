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

package pdhelper

import (
	"context"
	"testing"
	"time"

	"github.com/jellydator/ttlcache/v3"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/stretchr/testify/require"
)

var globalMockClient mockClient

type mockClient struct {
	missCnt int
}

func (m *mockClient) getMissCnt() int {
	return m.missCnt
}

func (m *mockClient) getFakeApproximateTableCountFromStorage(_ context.Context, _ sessionctx.Context, _ int64, _, _, _ string) (float64, bool) {
	m.missCnt++
	return 1.0, true
}

func TestTTLCache(t *testing.T) {
	cache := ttlcache.New[string, float64](
		ttlcache.WithTTL[string, float64](100*time.Millisecond),
		ttlcache.WithCapacity[string, float64](2),
	)
	helper := &PDHelper{
		cacheForApproximateTableCountFromStorage: cache,
		getApproximateTableCountFromStorageFunc:  globalMockClient.getFakeApproximateTableCountFromStorage,
	}
	ctx := context.Background()
	helper.GetApproximateTableCountFromStorage(ctx, nil, 1, "db", "table", "partition") // Miss
	require.Equal(t, 1, globalMockClient.getMissCnt())
	helper.GetApproximateTableCountFromStorage(ctx, nil, 1, "db", "table", "partition") // Hit
	require.Equal(t, 1, globalMockClient.getMissCnt())
	helper.GetApproximateTableCountFromStorage(ctx, nil, 2, "db1", "table1", "partition") // Miss
	require.Equal(t, 2, globalMockClient.getMissCnt())
	helper.GetApproximateTableCountFromStorage(ctx, nil, 3, "db2", "table2", "partition") // Miss
	helper.GetApproximateTableCountFromStorage(ctx, nil, 1, "db", "table", "partition")   // Miss
	require.Equal(t, 4, globalMockClient.getMissCnt())
	helper.GetApproximateTableCountFromStorage(ctx, nil, 3, "db2", "table2", "partition") // Hit
	require.Equal(t, 4, globalMockClient.getMissCnt())
	time.Sleep(200 * time.Millisecond)
	// All is miss.
	helper.GetApproximateTableCountFromStorage(ctx, nil, 1, "db", "table", "partition")
	helper.GetApproximateTableCountFromStorage(ctx, nil, 2, "db1", "table1", "partition")
	helper.GetApproximateTableCountFromStorage(ctx, nil, 3, "db2", "table2", "partition")
	require.Equal(t, 7, globalMockClient.getMissCnt())
}
