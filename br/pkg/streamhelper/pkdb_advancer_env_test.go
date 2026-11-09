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

package streamhelper_test

import (
	"context"
	"slices"
	"testing"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/br/pkg/streamhelper"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/opt"
)

type advancerStorePDClient struct {
	pd.Client
	stores []*metapb.Store
}

func (c *advancerStorePDClient) GetAllStores(
	context.Context,
	...opt.GetStoreOption,
) ([]*metapb.Store, error) {
	return slices.Clone(c.stores), nil
}

// TestPDRegionScannerStoresSkipsReplicator verifies flush-subscriber topology discovery.
// It maps TiKV, TiFlash, and replicator PD stores and expects a subscription target only for TiKV.
func TestPDRegionScannerStoresSkipsReplicator(t *testing.T) {
	pdClient := &advancerStorePDClient{stores: []*metapb.Store{
		{Id: 1, StartTimestamp: 10},
		{Id: 2, StartTimestamp: 20, Labels: []*metapb.StoreLabel{{Key: "engine", Value: "tiflash"}}},
		{Id: 3, StartTimestamp: 30, Labels: []*metapb.StoreLabel{{Key: "engine", Value: "replicator"}}},
	}}

	stores, err := (streamhelper.PDRegionScanner{Client: pdClient}).Stores(context.Background())
	require.NoError(t, err)
	require.Equal(t, []streamhelper.Store{{ID: 1, BootAt: 10}}, stores)
}
