// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.
package data_test

import (
	"context"
	"testing"

	"github.com/pingcap/kvproto/pkg/metapb"
	recovpb "github.com/pingcap/kvproto/pkg/recoverdatapb"
	"github.com/pingcap/tidb/br/pkg/conn"
	gluemock "github.com/pingcap/tidb/br/pkg/gluetidb/mock"
	"github.com/pingcap/tidb/br/pkg/pdutil"
	"github.com/pingcap/tidb/br/pkg/restore/data"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/testutils"
	pd "github.com/tikv/pd/client"
)

const (
	numOnlineStore = 3
	// test max allocate id
	maxAllocateId = 0x176f
)

type testData struct {
	ctx    context.Context
	cancel context.CancelFunc

	mockPDClient pd.Client
	mockRecovery data.Recovery
}

func newRegionMeta(
	RegionId uint64,
	PeerId uint64,
	LastLogTerm uint64,
	LastIndex uint64,
	CommitIndex uint64,
	Version uint64,
	Tombstone bool,
	StartKey []byte,
	EndKey []byte,
) *recovpb.RegionMeta {
	return &recovpb.RegionMeta{
		RegionId:    RegionId,
		PeerId:      PeerId,
		LastLogTerm: LastLogTerm,
		LastIndex:   LastIndex,
		CommitIndex: CommitIndex,
		Version:     Version,
		Tombstone:   Tombstone,
		StartKey:    StartKey,
		EndKey:      EndKey,
	}
}

func (t *testData) generateRegionMeta() {
	storeMeta0 := data.NewStoreMeta(1)
	storeMeta0.RegionMetas = append(storeMeta0.RegionMetas, newRegionMeta(11, 24, 8, 5, 4, 1, false, []byte(""), []byte("b")))
	storeMeta0.RegionMetas = append(storeMeta0.RegionMetas, newRegionMeta(12, 34, 5, 6, 5, 1, false, []byte("b"), []byte("c")))
	storeMeta0.RegionMetas = append(storeMeta0.RegionMetas, newRegionMeta(13, 44, 1200, 7, 6, 1, false, []byte("c"), []byte("")))
	t.mockRecovery.StoreMetas[0] = storeMeta0

	storeMeta1 := data.NewStoreMeta(2)
	storeMeta1.RegionMetas = append(storeMeta1.RegionMetas, newRegionMeta(11, 25, 7, 6, 4, 1, false, []byte(""), []byte("b")))
	storeMeta1.RegionMetas = append(storeMeta1.RegionMetas, newRegionMeta(12, 35, 5, 6, 5, 1, false, []byte("b"), []byte("c")))
	storeMeta1.RegionMetas = append(storeMeta1.RegionMetas, newRegionMeta(13, 45, 1200, 6, 6, 1, false, []byte("c"), []byte("")))
	t.mockRecovery.StoreMetas[1] = storeMeta1

	storeMeta2 := data.NewStoreMeta(3)
	storeMeta2.RegionMetas = append(storeMeta2.RegionMetas, newRegionMeta(11, 26, 7, 5, 4, 1, false, []byte(""), []byte("b")))
	storeMeta2.RegionMetas = append(storeMeta2.RegionMetas, newRegionMeta(12, 36, 5, 6, 6, 1, false, []byte("b"), []byte("c")))
	storeMeta2.RegionMetas = append(storeMeta2.RegionMetas, newRegionMeta(13, maxAllocateId, 1200, 6, 6, 1, false, []byte("c"), []byte("")))
	t.mockRecovery.StoreMetas[2] = storeMeta2
}

func (t *testData) cleanUp() {
	t.mockPDClient.Close()
}

func createStores() []*metapb.Store {
	return []*metapb.Store{
		{Id: 1, Labels: []*metapb.StoreLabel{{Key: "engine", Value: "tikv"}}},
		{Id: 2, Labels: []*metapb.StoreLabel{{Key: "else", Value: "tikv"}, {Key: "engine", Value: "tiflash"}}},
		{Id: 3, Labels: []*metapb.StoreLabel{{Key: "else", Value: "tiflash"}, {Key: "engine", Value: "tikv"}}},
	}
}

func createDataSuite(t *testing.T) *testData {
	tikvClient, _, pdClient, err := testutils.NewMockTiKV("", nil)
	require.NoError(t, err)
	mockGlue := &gluemock.MockGlue{}
	ctx, cancel := context.WithCancel(context.Background())
	mockMgr := &conn.Mgr{PdController: &pdutil.PdController{}}
	mockMgr.SetPDClient(pdClient)

	fakeProgress := mockGlue.StartProgress(ctx, "Restore Data", int64(numOnlineStore*3), false)

	var recovery = data.NewRecovery(createStores(), mockMgr, fakeProgress, 64)
	tikvClient.Close()
	return &testData{
		ctx:          ctx,
		cancel:       cancel,
		mockPDClient: pdClient,
		mockRecovery: recovery,
	}
}

func TestGetTotalRegions(t *testing.T) {
	testData := createDataSuite(t)
	testData.generateRegionMeta()
	totalRegion := testData.mockRecovery.GetTotalRegions()
	require.Equal(t, totalRegion, 3)
	testData.cleanUp()
}

func TestMakeRecoveryPlan(t *testing.T) {
	testData := createDataSuite(t)
	testData.generateRegionMeta()
	err := testData.mockRecovery.MakeRecoveryPlan()
	require.NoError(t, err)
	require.Equal(t, testData.mockRecovery.MaxAllocID, uint64(maxAllocateId))
	require.Equal(t, len(testData.mockRecovery.RecoveryPlan), 2)
	testData.cleanUp()
}
