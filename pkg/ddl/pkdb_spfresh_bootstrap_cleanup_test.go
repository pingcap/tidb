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

package ddl

import (
	"context"
	"net/http"
	"sync"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/stretchr/testify/require"
)

func TestSPFreshBootstrapCleanupUsesDurableEndpointsAndRetries(t *testing.T) {
	record := &meta.SPFreshBootstrapJob{JobID: 10, Finished: true, Targets: []meta.SPFreshBootstrapTarget{
		{TableID: 20, IndexIDs: []int64{1, 2}, ClusterID: 7, KeyspaceID: ^uint32(0), WorkerEndpoints: []string{"http://one", "http://two"}},
		{TableID: 21, IndexIDs: []int64{1}, ClusterID: 7, KeyspaceID: ^uint32(0), WorkerEndpoints: []string{"http://three"}},
	}}
	var mu sync.Mutex
	calls := make(map[string][]int64)
	fail := true
	teardown := func(_ context.Context, _ *http.Client, endpoint string, clusterID uint64, keyspaceID uint32, tableID, indexID int64) error {
		require.Equal(t, uint64(7), clusterID)
		require.Equal(t, ^uint32(0), keyspaceID)
		mu.Lock()
		defer mu.Unlock()
		calls[endpoint] = append(calls[endpoint], tableID*100+indexID)
		if endpoint == "http://one" && fail {
			return errors.New("lost teardown response")
		}
		return nil
	}
	require.ErrorContains(t, cleanupSPFreshBootstrap(context.Background(), record, teardown), "lost teardown response")
	require.Equal(t, []int64{2001, 2002}, calls["http://two"])
	fail = false
	clear(calls)
	require.NoError(t, cleanupSPFreshBootstrap(context.Background(), record, teardown))
	require.Equal(t, []int64{2001, 2002}, calls["http://one"])
	require.Equal(t, []int64{2001, 2002}, calls["http://two"])
	require.Equal(t, []int64{2101}, calls["http://three"])
	gcJob := spfreshBootstrapCleanupGCJob(record)
	require.Equal(t, record.JobID, gcJob.ID)
	args, err := model.GetFinishedDropSchemaArgs(gcJob)
	require.NoError(t, err)
	require.Equal(t, []int64{20, 21}, args.AllDroppedTableIDs)
	record.Succeeded = true
	require.ErrorContains(t, cleanupSPFreshBootstrap(context.Background(), record, teardown), "successful")
	record.Succeeded, record.Finished = false, false
	require.ErrorContains(t, cleanupSPFreshBootstrap(context.Background(), record, teardown), "active")
}
