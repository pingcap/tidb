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

package operator

import (
	"context"
	"slices"
	"testing"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/opt"
)

type forceFlushStorePDClient struct {
	pd.Client
	stores []*metapb.Store
}

func (c *forceFlushStorePDClient) GetAllStores(
	context.Context,
	...opt.GetStoreOption,
) ([]*metapb.Store, error) {
	return slices.Clone(c.stores), nil
}

// TestGetAllTiKVsSkipsReplicator verifies force-flush target discovery.
// It lists TiKV, TiFlash, and replicator stores and expects only TiKV to be selected for FlushNow.
func TestGetAllTiKVsSkipsReplicator(t *testing.T) {
	pdClient := &forceFlushStorePDClient{stores: []*metapb.Store{
		{Id: 1},
		{Id: 2, Labels: []*metapb.StoreLabel{{Key: "engine", Value: "tiflash"}}},
		{Id: 3, Labels: []*metapb.StoreLabel{{Key: "engine", Value: "replicator"}}},
	}}

	stores, err := getAllTiKVs(context.Background(), pdClient)
	require.NoError(t, err)
	require.Len(t, stores, 1)
	require.Equal(t, uint64(1), stores[0].GetId())
}
