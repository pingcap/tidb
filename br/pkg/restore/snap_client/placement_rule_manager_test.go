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

package snapclient_test

import (
	"context"
	"testing"

	"github.com/pingcap/failpoint"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/br/pkg/metautil"
	snapclient "github.com/pingcap/tidb/br/pkg/restore/snap_client"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	restoreutils "github.com/pingcap/tidb/br/pkg/restore/utils"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/client/clients/router"
)

func generateTables() []*snapclient.CreatedTable {
	return []*snapclient.CreatedTable{
		{
			Table: &model.TableInfo{
				ID: 1,
			},
			OldTable: &metautil.Table{
				DB:   &model.DBInfo{Name: ast.NewCIStr("test")},
				Info: &model.TableInfo{Name: ast.NewCIStr("t1")},
			},
		},
		{
			Table: &model.TableInfo{
				ID: 100,
			},
			OldTable: &metautil.Table{
				DB:   &model.DBInfo{Name: ast.NewCIStr("test")},
				Info: &model.TableInfo{Name: ast.NewCIStr("t100")},
			},
		},
	}
}

func TestContextManagerOffline(t *testing.T) {
	ctx := context.Background()
	placementRuleManager, err := snapclient.NewPlacementRuleManager(ctx, nil, nil, nil, false)
	require.NoError(t, err)
	tables := generateTables()
	err = placementRuleManager.SetPlacementRule(ctx, tables)
	require.NoError(t, err)
	err = placementRuleManager.ResetPlacementRules(ctx)
	require.NoError(t, err)
}

func TestContextManagerOnlineNoStores(t *testing.T) {
	ctx := context.Background()
	stores := []*metapb.Store{
		{
			Id:    1,
			State: metapb.StoreState_Up,
			Labels: []*metapb.StoreLabel{
				{
					Key:   "engine",
					Value: "tiflash",
				},
			},
		},
		{
			Id:    2,
			State: metapb.StoreState_Offline,
			Labels: []*metapb.StoreLabel{
				{
					Key:   "engine",
					Value: "tikv",
				},
			},
		},
	}

	pdClient := split.NewFakePDClient(stores, false, nil)
	pdHTTPCli := split.NewFakePDHTTPClient()
	placementRuleManager, err := snapclient.NewPlacementRuleManager(ctx, pdClient, pdHTTPCli, nil, true)
	require.NoError(t, err)
	tables := generateTables()
	err = placementRuleManager.SetPlacementRule(ctx, tables)
	require.NoError(t, err)
	err = placementRuleManager.ResetPlacementRules(ctx)
	require.NoError(t, err)
}

func generateRegions() []*router.Region {
	return []*router.Region{
		{
			Meta: &metapb.Region{
				Id:       0,
				StartKey: []byte(""),
				EndKey:   codec.EncodeBytes([]byte{}, tablecodec.EncodeTablePrefix(0)),
				Peers:    []*metapb.Peer{{StoreId: 1}, {StoreId: 2}},
			},
			Leader: &metapb.Peer{StoreId: 1},
		},
		{
			Meta: &metapb.Region{
				Id:       1,
				StartKey: codec.EncodeBytes([]byte{}, tablecodec.EncodeTablePrefix(0)),
				EndKey:   codec.EncodeBytes([]byte{}, tablecodec.EncodeTablePrefix(1)),
				Peers:    []*metapb.Peer{{StoreId: 1}, {StoreId: 2}},
			},
			Leader: &metapb.Peer{StoreId: 1},
		},
		{
			Meta: &metapb.Region{
				Id:       2,
				StartKey: codec.EncodeBytes([]byte{}, tablecodec.EncodeTablePrefix(1)),
				EndKey:   codec.EncodeBytes([]byte{}, tablecodec.EncodeTablePrefix(2)),
				Peers:    []*metapb.Peer{{StoreId: 1}, {StoreId: 2}},
			},
			Leader: &metapb.Peer{StoreId: 1},
		},
		{
			Meta: &metapb.Region{
				Id:       3,
				StartKey: codec.EncodeBytes([]byte{}, tablecodec.EncodeTablePrefix(2)),
				EndKey:   codec.EncodeBytes([]byte{}, tablecodec.EncodeTablePrefix(200)),
				Peers:    []*metapb.Peer{{StoreId: 1}, {StoreId: 2}},
			},
			Leader: &metapb.Peer{StoreId: 1},
		},
		{
			Meta: &metapb.Region{
				Id:       4,
				StartKey: codec.EncodeBytes([]byte{}, tablecodec.EncodeTablePrefix(200)),
				EndKey:   []byte(""),
				Peers:    []*metapb.Peer{{StoreId: 1}, {StoreId: 2}},
			},
			Leader: &metapb.Peer{StoreId: 1},
		},
	}
}

func generateFiles() ([]*backuppb.File, *restoreutils.RewriteRules) {
	files := make([]*backuppb.File, 0, 10)
	for i := 0; i < 10; i += 1 {
		files = append(files, &backuppb.File{
			StartKey: tablecodec.EncodeTablePrefix(100),
			EndKey:   tablecodec.EncodeTablePrefix(100),
		})
	}
	return files, &restoreutils.RewriteRules{
		Data: []*import_sstpb.RewriteRule{
			{
				OldKeyPrefix: tablecodec.EncodeTablePrefix(100),
				NewKeyPrefix: tablecodec.EncodeTablePrefix(1),
			},
		},
	}
}

func generateStores() []*metapb.Store {
	return []*metapb.Store{
		{
			Id:    1,
			State: metapb.StoreState_Up,
			Labels: []*metapb.StoreLabel{
				{
					Key:   "engine",
					Value: "tiflash",
				},
			},
		},
		{
			Id:    2,
			State: metapb.StoreState_Up,
			Labels: []*metapb.StoreLabel{
				{
					Key:   "engine",
					Value: "tikv",
				},
				{
					Key:   snapclient.RestoreLabelKey,
					Value: snapclient.RestoreLabelValue,
				},
			},
		},
		{
			Id:    3,
			State: metapb.StoreState_Offline,
			Labels: []*metapb.StoreLabel{
				{
					Key:   "engine",
					Value: "tikv",
				},
			},
		},
		{
			Id:    4,
			State: metapb.StoreState_Offline,
			Labels: []*metapb.StoreLabel{
				{
					Key:   "engine",
					Value: "tikv",
				},
				{
					Key:   snapclient.RestoreLabelKey,
					Value: snapclient.RestoreLabelValue,
				},
			},
		},
	}
}

func TestContextManagerOnlineLeave(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/br/pkg/restore/snap_client/wait-placement-schedule-quicker-ticker", "return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/br/pkg/restore/snap_client/wait-placement-schedule-quicker-ticker"))
	}()
	ctx := context.Background()
	stores := generateStores()
	regions := generateRegions()

	pdClient := split.NewFakePDClient(stores, false, nil)
	pdClient.SetRegions(regions)
	pdHTTPCli := split.NewFakePDHTTPClient()
	placementRuleManager, err := snapclient.NewPlacementRuleManager(ctx, pdClient, pdHTTPCli, nil, true)
	require.NoError(t, err)
	tables := generateTables()
	err = placementRuleManager.SetPlacementRule(ctx, tables)
	require.NoError(t, err)
	err = placementRuleManager.ResetPlacementRules(ctx)
	require.NoError(t, err)
}
