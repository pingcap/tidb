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

package preparesnap_test

import (
	"context"
	"slices"
	"testing"

	"github.com/pingcap/kvproto/pkg/metapb"
	. "github.com/pingcap/tidb/br/pkg/backup/prepare_snap"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/opt"
)

type prepareStorePDClient struct {
	pd.Client
	stores []*metapb.Store
}

func (c *prepareStorePDClient) GetAllStores(
	context.Context,
	...opt.GetStoreOption,
) ([]*metapb.Store, error) {
	return slices.Clone(c.stores), nil
}

func (c *prepareStorePDClient) GetClusterID(context.Context) uint64 {
	return 1
}

// TestCliEnvGetAllLiveStoresSkipsReplicator verifies EBS snapshot preparation target discovery.
// It exposes TiKV, TiFlash, and replicator stores through PD and expects only TiKV to be returned.
func TestCliEnvGetAllLiveStoresSkipsReplicator(t *testing.T) {
	pdClient := &prepareStorePDClient{stores: []*metapb.Store{
		{Id: 1},
		{Id: 2, Labels: []*metapb.StoreLabel{{Key: "engine", Value: "tiflash"}}},
		{Id: 3, Labels: []*metapb.StoreLabel{{Key: "engine", Value: "replicator"}}},
	}}
	regionCache := tikv.NewRegionCache(pdClient)
	t.Cleanup(regionCache.Close)

	stores, err := (CliEnv{Cache: regionCache}).GetAllLiveStores(context.Background())
	require.NoError(t, err)
	require.Len(t, stores, 1)
	require.Equal(t, uint64(1), stores[0].GetId())
}
