// Copyright 2021 PingCAP, Inc. Licensed under Apache-2.0.

package restore_test

import (
	"context"
	"encoding/hex"
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/br/pkg/restore"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/store/pdtypes"
	"github.com/pingcap/tidb/util/codec"
	"github.com/stretchr/testify/require"
)

func assertDecode(t *testing.T, key []byte) []byte {
	if len(key) == 0 {
		return []byte{}
	}
	_, decoded, err := codec.DecodeBytes(key, nil)
	require.NoError(t, err)
	return decoded
}

func assertRegions(t *testing.T, regions []*restore.RegionInfo, keys ...string) {
	require.Equal(t, len(regions)+1, len(keys))
	last := keys[0]
	for i, r := range regions {
		start := assertDecode(t, r.Region.StartKey)
		end := assertDecode(t, r.Region.EndKey)

		require.Equal(t, start, []byte(last), "not match for region: %+v", *r)
		last = keys[i+1]
		require.Equal(t, end, []byte(last), "not match for region: %+v", *r)
	}
}

func TestScanSuccess(t *testing.T) {
	// region: [, aay), [aay, bba), [bba, bbh), [bbh, cca), [cca, )
	cli := initTestClient()
	rs := utils.InitialRetryState(1, 0, 0)
	ctx := context.Background()

	// make exclusive to inclusive.
	ctl := restore.OverRegionsInRange([]byte("aa"), []byte("aay"), cli, &rs)
	collectedRegions := []*restore.RegionInfo{}
	ctl.Run(ctx, func(ctx context.Context, r *restore.RegionInfo) restore.RPCResult {
		collectedRegions = append(collectedRegions, r)
		return restore.RPCResultOK()
	})
	assertRegions(t, collectedRegions, "", "aay", "bba")

	ctl = restore.OverRegionsInRange([]byte("aaz"), []byte("bb"), cli, &rs)
	collectedRegions = []*restore.RegionInfo{}
	ctl.Run(ctx, func(ctx context.Context, r *restore.RegionInfo) restore.RPCResult {
		collectedRegions = append(collectedRegions, r)
		return restore.RPCResultOK()
	})
	assertRegions(t, collectedRegions, "aay", "bba", "bbh", "cca")

	ctl = restore.OverRegionsInRange([]byte("aa"), []byte("cc"), cli, &rs)
	collectedRegions = []*restore.RegionInfo{}
	ctl.Run(ctx, func(ctx context.Context, r *restore.RegionInfo) restore.RPCResult {
		collectedRegions = append(collectedRegions, r)
		return restore.RPCResultOK()
	})
	assertRegions(t, collectedRegions, "", "aay", "bba", "bbh", "cca", "")

	ctl = restore.OverRegionsInRange([]byte("aa"), []byte(""), cli, &rs)
	collectedRegions = []*restore.RegionInfo{}
	ctl.Run(ctx, func(ctx context.Context, r *restore.RegionInfo) restore.RPCResult {
		collectedRegions = append(collectedRegions, r)
		return restore.RPCResultOK()
	})
	assertRegions(t, collectedRegions, "", "aay", "bba", "bbh", "cca", "")
}

func TestNotLeader(t *testing.T) {
	// region: [, aay), [aay, bba), [bba, bbh), [bbh, cca), [cca, )
	cli := initTestClient()
	rs := utils.InitialRetryState(1, 0, 0)
	ctl := restore.OverRegionsInRange([]byte(""), []byte(""), cli, &rs)
	ctx := context.Background()

	notLeader := errorpb.Error{
		NotLeader: &errorpb.NotLeader{
			Leader: &metapb.Peer{
				Id: 42,
			},
		},
	}
	// record the regions we didn't touch.
	meetRegions := []*restore.RegionInfo{}
	// record all regions we meet with id == 2.
	idEqualsTo2Regions := []*restore.RegionInfo{}
	err := ctl.Run(ctx, func(ctx context.Context, r *restore.RegionInfo) restore.RPCResult {
		if r.Region.Id == 2 {
			idEqualsTo2Regions = append(idEqualsTo2Regions, r)
		}
		if r.Region.Id == 2 && (r.Leader == nil || r.Leader.Id != 42) {
			return restore.RPCResult{
				StoreError: &notLeader,
			}
		}
		meetRegions = append(meetRegions, r)
		return restore.RPCResultOK()
	})

	require.NoError(t, err)
	require.Len(t, idEqualsTo2Regions, 2)
	if idEqualsTo2Regions[1].Leader != nil {
		require.NotEqual(t, 42, idEqualsTo2Regions[0].Leader.Id)
	}
	require.EqualValues(t, 42, idEqualsTo2Regions[1].Leader.Id)
	assertRegions(t, meetRegions, "", "aay", "bba", "bbh", "cca", "")
}

func printRegion(name string, infos []*restore.RegionInfo) {
	fmt.Printf(">>>>> %s <<<<<\n", name)
	for _, info := range infos {
		fmt.Printf("[%d] %s ~ %s\n", info.Region.Id, hex.EncodeToString(info.Region.StartKey), hex.EncodeToString(info.Region.EndKey))
	}
	fmt.Printf("<<<<< %s >>>>>\n", name)
}

func printPDRegion(name string, infos []*pdtypes.Region) {
	fmt.Printf(">>>>> %s <<<<<\n", name)
	for _, info := range infos {
		fmt.Printf("[%d] %s ~ %s\n", info.Meta.Id, hex.EncodeToString(info.Meta.StartKey), hex.EncodeToString(info.Meta.EndKey))
	}
	fmt.Printf("<<<<< %s >>>>>\n", name)
}

func TestEpochNotMatch(t *testing.T) {
	// region: [, aay), [aay, bba), [bba, bbh), [bbh, cca), [cca, )
	cli := initTestClient()
	rs := utils.InitialRetryState(2, 0, 0)
	ctl := restore.OverRegionsInRange([]byte(""), []byte(""), cli, &rs)
	ctx := context.Background()

	printPDRegion("cli", cli.regionsInfo.Regions)
	regions, err := restore.PaginateScanRegion(ctx, cli, []byte("aaz"), []byte("bbb"), 2)
	require.NoError(t, err)
	require.Len(t, regions, 2)
	left, right := regions[0], regions[1]
	info := restore.RegionInfo{
		Region: &metapb.Region{
			StartKey: left.Region.StartKey,
			EndKey:   right.Region.EndKey,
			Id:       42,
			Peers: []*metapb.Peer{
				{Id: 43},
			},
		},
		Leader: &metapb.Peer{Id: 43},
	}
	newRegion := pdtypes.NewRegionInfo(info.Region, info.Leader)
	mergeRegion := func() {
		cli.regionsInfo.SetRegion(newRegion)
		cli.regions[42] = &info
	}
	epochNotMatch := &import_sstpb.Error{
		Message: "Epoch not match",
		StoreError: &errorpb.Error{
			EpochNotMatch: &errorpb.EpochNotMatch{
				CurrentRegions: []*metapb.Region{info.Region},
			},
		}}
	firstRunRegions := []*restore.RegionInfo{}
	secondRunRegions := []*restore.RegionInfo{}
	isSecondRun := false
	err = ctl.Run(ctx, func(ctx context.Context, r *restore.RegionInfo) restore.RPCResult {
		if !isSecondRun && r.Region.Id == left.Region.Id {
			mergeRegion()
			isSecondRun = true
			return restore.RPCResultFromPBError(epochNotMatch)
		}
		if isSecondRun {
			secondRunRegions = append(secondRunRegions, r)
		} else {
			firstRunRegions = append(firstRunRegions, r)
		}
		return restore.RPCResultOK()
	})
	printRegion("first", firstRunRegions)
	printRegion("second", secondRunRegions)
	printPDRegion("cli", cli.regionsInfo.Regions)
	assertRegions(t, firstRunRegions, "", "aay")
	assertRegions(t, secondRunRegions, "", "aay", "bbh", "cca", "")
	require.NoError(t, err)
}

func TestRegionSplit(t *testing.T) {
	// region: [, aay), [aay, bba), [bba, bbh), [bbh, cca), [cca, )
	cli := initTestClient()
	rs := utils.InitialRetryState(2, 0, 0)
	ctl := restore.OverRegionsInRange([]byte(""), []byte(""), cli, &rs)
	ctx := context.Background()

	printPDRegion("cli", cli.regionsInfo.Regions)
	regions, err := restore.PaginateScanRegion(ctx, cli, []byte("aaz"), []byte("aazz"), 1)
	require.NoError(t, err)
	require.Len(t, regions, 1)
	target := regions[0]

	newRegions := []*restore.RegionInfo{
		{
			Region: &metapb.Region{
				Id:       42,
				StartKey: target.Region.StartKey,
				EndKey:   codec.EncodeBytes(nil, []byte("aayy")),
			},
			Leader: &metapb.Peer{
				Id: 43,
			},
		},
		{
			Region: &metapb.Region{
				Id:       44,
				StartKey: codec.EncodeBytes(nil, []byte("aayy")),
				EndKey:   target.Region.EndKey,
			},
			Leader: &metapb.Peer{
				Id: 45,
			},
		},
	}
	splitRegion := func() {
		for _, r := range newRegions {
			newRegion := pdtypes.NewRegionInfo(r.Region, r.Leader)
			cli.regionsInfo.SetRegion(newRegion)
			cli.regions[r.Region.Id] = r
		}
	}
	epochNotMatch := &import_sstpb.Error{
		Message: "Epoch not match",
		StoreError: &errorpb.Error{
			EpochNotMatch: &errorpb.EpochNotMatch{
				CurrentRegions: []*metapb.Region{
					newRegions[0].Region,
					newRegions[1].Region,
				},
			},
		}}
	firstRunRegions := []*restore.RegionInfo{}
	secondRunRegions := []*restore.RegionInfo{}
	isSecondRun := false
	err = ctl.Run(ctx, func(ctx context.Context, r *restore.RegionInfo) restore.RPCResult {
		if !isSecondRun && r.Region.Id == target.Region.Id {
			splitRegion()
			isSecondRun = true
			return restore.RPCResultFromPBError(epochNotMatch)
		}
		if isSecondRun {
			secondRunRegions = append(secondRunRegions, r)
		} else {
			firstRunRegions = append(firstRunRegions, r)
		}
		return restore.RPCResultOK()
	})
	printRegion("first", firstRunRegions)
	printRegion("second", secondRunRegions)
	printPDRegion("cli", cli.regionsInfo.Regions)
	assertRegions(t, firstRunRegions, "", "aay")
	assertRegions(t, secondRunRegions, "", "aay", "aayy", "bba", "bbh", "cca", "")
	require.NoError(t, err)
}

func TestRetryBackoff(t *testing.T) {
	// region: [, aay), [aay, bba), [bba, bbh), [bbh, cca), [cca, )
	cli := initTestClient()
	rs := utils.InitialRetryState(2, time.Millisecond, 10*time.Millisecond)
	ctl := restore.OverRegionsInRange([]byte(""), []byte(""), cli, &rs)
	ctx := context.Background()

	printPDRegion("cli", cli.regionsInfo.Regions)
	regions, err := restore.PaginateScanRegion(ctx, cli, []byte("aaz"), []byte("bbb"), 2)
	require.NoError(t, err)
	require.Len(t, regions, 2)
	left := regions[0]

	epochNotLeader := &import_sstpb.Error{
		Message: "leader not found",
		StoreError: &errorpb.Error{
			NotLeader: &errorpb.NotLeader{
				RegionId: 2,
			},
		}}
	isSecondRun := false
	err = ctl.Run(ctx, func(ctx context.Context, r *restore.RegionInfo) restore.RPCResult {
		if !isSecondRun && r.Region.Id == left.Region.Id {
			isSecondRun = true
			return restore.RPCResultFromPBError(epochNotLeader)
		}
		return restore.RPCResultOK()
	})
	printPDRegion("cli", cli.regionsInfo.Regions)
	require.Equal(t, 1, rs.Attempt())
	// we retried leader not found error. so the next backoff should be 2 * initical backoff.
	require.Equal(t, 2*time.Millisecond, rs.ExponentialBackoff())
	require.NoError(t, err)
}
