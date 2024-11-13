// Copyright 2021 PingCAP, Inc. Licensed under Apache-2.0.

package restore

import (
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/store/pdtypes"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func assertDecode(t *testing.T, key []byte) []byte {
	if len(key) == 0 {
		return []byte{}
	}
	_, decoded, err := codec.DecodeBytes(key, nil)
	require.NoError(t, err)
	return decoded
}

func assertRegions(t *testing.T, regions []*split.RegionInfo, keys ...string) {
	require.Equal(t, len(regions)+1, len(keys), "%+v\nvs\n%+v", regions, keys)
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
	cli := initTestClient(false)
	rs := utils.InitialRetryState(1, 0, 0)
	ctx := context.Background()

	// make exclusive to inclusive.
	ctl := OverRegionsInRange([]byte("aa"), []byte("aay"), cli, &rs)
	collectedRegions := []*split.RegionInfo{}
	ctl.Run(ctx, func(ctx context.Context, r *split.RegionInfo) RPCResult {
		collectedRegions = append(collectedRegions, r)
		return RPCResultOK()
	})
	assertRegions(t, collectedRegions, "", "aay", "bba")

	ctl = OverRegionsInRange([]byte("aaz"), []byte("bb"), cli, &rs)
	collectedRegions = []*split.RegionInfo{}
	ctl.Run(ctx, func(ctx context.Context, r *split.RegionInfo) RPCResult {
		collectedRegions = append(collectedRegions, r)
		return RPCResultOK()
	})
	assertRegions(t, collectedRegions, "aay", "bba", "bbh", "cca")

	ctl = OverRegionsInRange([]byte("aa"), []byte("cc"), cli, &rs)
	collectedRegions = []*split.RegionInfo{}
	ctl.Run(ctx, func(ctx context.Context, r *split.RegionInfo) RPCResult {
		collectedRegions = append(collectedRegions, r)
		return RPCResultOK()
	})
	assertRegions(t, collectedRegions, "", "aay", "bba", "bbh", "cca", "")

	ctl = OverRegionsInRange([]byte("aa"), []byte(""), cli, &rs)
	collectedRegions = []*split.RegionInfo{}
	ctl.Run(ctx, func(ctx context.Context, r *split.RegionInfo) RPCResult {
		collectedRegions = append(collectedRegions, r)
		return RPCResultOK()
	})
	assertRegions(t, collectedRegions, "", "aay", "bba", "bbh", "cca", "")
}

func TestNotLeader(t *testing.T) {
	// region: [, aay), [aay, bba), [bba, bbh), [bbh, cca), [cca, )
	cli := initTestClient(false)
	rs := utils.InitialRetryState(1, 0, 0)
	ctl := OverRegionsInRange([]byte(""), []byte(""), cli, &rs)
	ctx := context.Background()

	notLeader := errorpb.Error{
		NotLeader: &errorpb.NotLeader{
			Leader: &metapb.Peer{
				Id: 42,
			},
		},
	}
	// record the regions we didn't touch.
	meetRegions := []*split.RegionInfo{}
	// record all regions we meet with id == 2.
	idEqualsTo2Regions := []*split.RegionInfo{}
	err := ctl.Run(ctx, func(ctx context.Context, r *split.RegionInfo) RPCResult {
		if r.Region.Id == 2 {
			idEqualsTo2Regions = append(idEqualsTo2Regions, r)
		}
		if r.Region.Id == 2 && (r.Leader == nil || r.Leader.Id != 42) {
			return RPCResult{
				StoreError: &notLeader,
			}
		}
		meetRegions = append(meetRegions, r)
		return RPCResultOK()
	})

	require.NoError(t, err)
	require.Len(t, idEqualsTo2Regions, 2)
	if idEqualsTo2Regions[1].Leader != nil {
		require.NotEqual(t, 42, idEqualsTo2Regions[0].Leader.Id)
	}
	require.EqualValues(t, 42, idEqualsTo2Regions[1].Leader.Id)
	assertRegions(t, meetRegions, "", "aay", "bba", "bbh", "cca", "")
}

func TestServerIsBusy(t *testing.T) {
	// region: [, aay), [aay, bba), [bba, bbh), [bbh, cca), [cca, )
	cli := initTestClient(false)
	rs := utils.InitialRetryState(2, 0, 0)
	ctl := OverRegionsInRange([]byte(""), []byte(""), cli, &rs)
	ctx := context.Background()

	serverIsBusy := errorpb.Error{
		Message: "server is busy",
		ServerIsBusy: &errorpb.ServerIsBusy{
			Reason: "memory is out",
		},
	}
	// record the regions we didn't touch.
	meetRegions := []*split.RegionInfo{}
	// record all regions we meet with id == 2.
	idEqualsTo2Regions := []*split.RegionInfo{}
	theFirstRun := true
	err := ctl.Run(ctx, func(ctx context.Context, r *split.RegionInfo) RPCResult {
		if theFirstRun && r.Region.Id == 2 {
			idEqualsTo2Regions = append(idEqualsTo2Regions, r)
			theFirstRun = false
			return RPCResult{
				StoreError: &serverIsBusy,
			}
		}
		meetRegions = append(meetRegions, r)
		return RPCResultOK()
	})

	require.NoError(t, err)
	assertRegions(t, idEqualsTo2Regions, "aay", "bba")
	assertRegions(t, meetRegions, "", "aay", "bba", "bbh", "cca", "")
	require.Equal(t, rs.Attempt(), 1)
}

func TestServerIsBusyWithMemoryIsLimited(t *testing.T) {
	_ = failpoint.Enable("github.com/pingcap/tidb/br/pkg/restore/hint-memory-is-limited", "return(true)")
	defer func() {
		_ = failpoint.Disable("github.com/pingcap/tidb/br/pkg/restore/hint-memory-is-limited")
	}()

	// region: [, aay), [aay, bba), [bba, bbh), [bbh, cca), [cca, )
	cli := initTestClient(false)
	rs := utils.InitialRetryState(2, 0, 0)
	ctl := OverRegionsInRange([]byte(""), []byte(""), cli, &rs)
	ctx := context.Background()

	serverIsBusy := errorpb.Error{
		Message: "memory is limited",
		ServerIsBusy: &errorpb.ServerIsBusy{
			Reason: "",
		},
	}
	// record the regions we didn't touch.
	meetRegions := []*split.RegionInfo{}
	// record all regions we meet with id == 2.
	idEqualsTo2Regions := []*split.RegionInfo{}
	theFirstRun := true
	err := ctl.Run(ctx, func(ctx context.Context, r *split.RegionInfo) RPCResult {
		if theFirstRun && r.Region.Id == 2 {
			idEqualsTo2Regions = append(idEqualsTo2Regions, r)
			theFirstRun = false
			return RPCResult{
				StoreError: &serverIsBusy,
			}
		}
		meetRegions = append(meetRegions, r)
		return RPCResultOK()
	})

	require.NoError(t, err)
	assertRegions(t, idEqualsTo2Regions, "aay", "bba")
	assertRegions(t, meetRegions, "", "aay", "bba", "bbh", "cca", "")
	require.Equal(t, rs.Attempt(), 2)
}

func printRegion(name string, infos []*split.RegionInfo) {
	fmt.Printf(">>>>> %s <<<<<\n", name)
	for _, info := range infos {
		fmt.Printf("[%04d] %s ~ %s\n", info.Region.Id, hex.EncodeToString(info.Region.StartKey), hex.EncodeToString(info.Region.EndKey))
	}
	fmt.Printf("<<<<< %s >>>>>\n", name)
}

func printPDRegion(name string, infos []*pdtypes.Region) {
	fmt.Printf(">>>>> %s <<<<<\n", name)
	for _, info := range infos {
		fmt.Printf("[%04d] %s ~ %s\n", info.Meta.Id, hex.EncodeToString(info.Meta.StartKey), hex.EncodeToString(info.Meta.EndKey))
	}
	fmt.Printf("<<<<< %s >>>>>\n", name)
}

func TestEpochNotMatch(t *testing.T) {
	// region: [, aay), [aay, bba), [bba, bbh), [bbh, cca), [cca, )
	cli := initTestClient(false)
	rs := utils.InitialRetryState(2, 0, 0)
	ctl := OverRegionsInRange([]byte(""), []byte(""), cli, &rs)
	ctx := context.Background()

	printPDRegion("cli", cli.regionsInfo.Regions)
	regions, err := split.PaginateScanRegion(ctx, cli, []byte("aaz"), []byte("bbb"), 2)
	require.NoError(t, err)
	require.Len(t, regions, 2)
	left, right := regions[0], regions[1]
	info := split.RegionInfo{
		Region: &metapb.Region{
			StartKey: left.Region.StartKey,
			EndKey:   right.Region.EndKey,
			Id:       42,
			Peers: []*metapb.Peer{
				{Id: 43},
			},
		},
		Leader: &metapb.Peer{Id: 43, StoreId: 1},
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
	firstRunRegions := []*split.RegionInfo{}
	secondRunRegions := []*split.RegionInfo{}
	isSecondRun := false
	err = ctl.Run(ctx, func(ctx context.Context, r *split.RegionInfo) RPCResult {
		if !isSecondRun && r.Region.Id == left.Region.Id {
			mergeRegion()
			isSecondRun = true
			return RPCResultFromPBError(epochNotMatch)
		}
		if isSecondRun {
			secondRunRegions = append(secondRunRegions, r)
		} else {
			firstRunRegions = append(firstRunRegions, r)
		}
		return RPCResultOK()
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
	cli := initTestClient(false)
	rs := utils.InitialRetryState(2, 0, 0)
	ctl := OverRegionsInRange([]byte(""), []byte(""), cli, &rs)
	ctx := context.Background()

	printPDRegion("cli", cli.regionsInfo.Regions)
	regions, err := split.PaginateScanRegion(ctx, cli, []byte("aaz"), []byte("aazz"), 1)
	require.NoError(t, err)
	require.Len(t, regions, 1)
	target := regions[0]

	newRegions := []*split.RegionInfo{
		{
			Region: &metapb.Region{
				Id:       42,
				StartKey: target.Region.StartKey,
				EndKey:   codec.EncodeBytes(nil, []byte("aayy")),
			},
			Leader: &metapb.Peer{
				Id:      43,
				StoreId: 1,
			},
		},
		{
			Region: &metapb.Region{
				Id:       44,
				StartKey: codec.EncodeBytes(nil, []byte("aayy")),
				EndKey:   target.Region.EndKey,
			},
			Leader: &metapb.Peer{
				Id:      45,
				StoreId: 1,
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
	firstRunRegions := []*split.RegionInfo{}
	secondRunRegions := []*split.RegionInfo{}
	isSecondRun := false
	err = ctl.Run(ctx, func(ctx context.Context, r *split.RegionInfo) RPCResult {
		if !isSecondRun && r.Region.Id == target.Region.Id {
			splitRegion()
			isSecondRun = true
			return RPCResultFromPBError(epochNotMatch)
		}
		if isSecondRun {
			secondRunRegions = append(secondRunRegions, r)
		} else {
			firstRunRegions = append(firstRunRegions, r)
		}
		return RPCResultOK()
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
	cli := initTestClient(false)
	rs := utils.InitialRetryState(2, time.Millisecond, 10*time.Millisecond)
	ctl := OverRegionsInRange([]byte(""), []byte(""), cli, &rs)
	ctx := context.Background()

	printPDRegion("cli", cli.regionsInfo.Regions)
	regions, err := split.PaginateScanRegion(ctx, cli, []byte("aaz"), []byte("bbb"), 2)
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
	err = ctl.Run(ctx, func(ctx context.Context, r *split.RegionInfo) RPCResult {
		if !isSecondRun && r.Region.Id == left.Region.Id {
			isSecondRun = true
			return RPCResultFromPBError(epochNotLeader)
		}
		return RPCResultOK()
	})
	printPDRegion("cli", cli.regionsInfo.Regions)
	require.Equal(t, 1, rs.Attempt())
	// we retried leader not found error. so the next backoff should be 2 * initical backoff.
	require.Equal(t, 2*time.Millisecond, rs.ExponentialBackoff())
	require.NoError(t, err)
}

func TestWrappedError(t *testing.T) {
	result := RPCResultFromError(errors.Trace(status.Error(codes.Unavailable, "the server is slacking. ><=·>")))
	require.Equal(t, result.StrategyForRetry(), StrategyFromThisRegion)
	result = RPCResultFromError(errors.Trace(status.Error(codes.Unknown, "the server said something hard to understand")))
	require.Equal(t, result.StrategyForRetry(), StrategyGiveUp)
}

func envInt(name string, def int) int {
	lit := os.Getenv(name)
	r, err := strconv.Atoi(lit)
	if err != nil {
		return def
	}
	return r
}

func TestPaginateScanLeader(t *testing.T) {
	// region: [, aay), [aay, bba), [bba, bbh), [bbh, cca), [cca, )
	cli := initTestClient(false)
	rs := utils.InitialRetryState(2, time.Millisecond, 10*time.Millisecond)
	ctl := OverRegionsInRange([]byte("aa"), []byte("aaz"), cli, &rs)
	ctx := context.Background()

	cli.InjectErr = true
	cli.InjectTimes = int32(envInt("PAGINATE_SCAN_LEADER_FAILURE_COUNT", 2))
	collectedRegions := []*split.RegionInfo{}
	ctl.Run(ctx, func(ctx context.Context, r *split.RegionInfo) RPCResult {
		collectedRegions = append(collectedRegions, r)
		return RPCResultOK()
	})
	assertRegions(t, collectedRegions, "", "aay", "bba")
}

func TestImportKVFiles(t *testing.T) {
	var (
		importer            = FileImporter{}
		ctx                 = context.Background()
		shiftStartTS uint64 = 100
		startTS      uint64 = 200
		restoreTS    uint64 = 300
	)

	err := importer.ImportKVFiles(
		ctx,
		[]*LogDataFileInfo{
			{
				DataFileInfo: &backuppb.DataFileInfo{
					Path: "log3",
				},
			},
			{
				DataFileInfo: &backuppb.DataFileInfo{
					Path: "log1",
				},
			},
		},
		nil,
		shiftStartTS,
		startTS,
		restoreTS,
		false,
	)
	require.True(t, berrors.ErrInvalidArgument.Equal(err))
}

func TestFilterFilesByRegion(t *testing.T) {
	files := []*LogDataFileInfo{
		{
			DataFileInfo: &backuppb.DataFileInfo{
				Path: "log3",
			},
		},
		{
			DataFileInfo: &backuppb.DataFileInfo{
				Path: "log1",
			},
		},
	}
	ranges := []kv.KeyRange{
		{
			StartKey: []byte("1111"),
			EndKey:   []byte("2222"),
		}, {
			StartKey: []byte("3333"),
			EndKey:   []byte("4444"),
		},
	}

	testCases := []struct {
		r        split.RegionInfo
		subfiles []*LogDataFileInfo
		err      error
	}{
		{
			r: split.RegionInfo{
				Region: &metapb.Region{
					StartKey: []byte("0000"),
					EndKey:   []byte("1110"),
				},
			},
			subfiles: []*LogDataFileInfo{},
			err:      nil,
		},
		{
			r: split.RegionInfo{
				Region: &metapb.Region{
					StartKey: []byte("0000"),
					EndKey:   []byte("1111"),
				},
			},
			subfiles: []*LogDataFileInfo{
				files[0],
			},
			err: nil,
		},
		{
			r: split.RegionInfo{
				Region: &metapb.Region{
					StartKey: []byte("0000"),
					EndKey:   []byte("2222"),
				},
			},
			subfiles: []*LogDataFileInfo{
				files[0],
			},
			err: nil,
		},
		{
			r: split.RegionInfo{
				Region: &metapb.Region{
					StartKey: []byte("2222"),
					EndKey:   []byte("3332"),
				},
			},
			subfiles: []*LogDataFileInfo{
				files[0],
			},
			err: nil,
		},
		{
			r: split.RegionInfo{
				Region: &metapb.Region{
					StartKey: []byte("2223"),
					EndKey:   []byte("3332"),
				},
			},
			subfiles: []*LogDataFileInfo{},
			err:      nil,
		},
		{
			r: split.RegionInfo{
				Region: &metapb.Region{
					StartKey: []byte("3332"),
					EndKey:   []byte("3333"),
				},
			},
			subfiles: []*LogDataFileInfo{
				files[1],
			},
			err: nil,
		},
		{
			r: split.RegionInfo{
				Region: &metapb.Region{
					StartKey: []byte("4444"),
					EndKey:   []byte("5555"),
				},
			},
			subfiles: []*LogDataFileInfo{
				files[1],
			},
			err: nil,
		},
		{
			r: split.RegionInfo{
				Region: &metapb.Region{
					StartKey: []byte("4444"),
					EndKey:   nil,
				},
			},
			subfiles: []*LogDataFileInfo{
				files[1],
			},
			err: nil,
		},
		{
			r: split.RegionInfo{
				Region: &metapb.Region{
					StartKey: []byte("0000"),
					EndKey:   nil,
				},
			},
			subfiles: files,
			err:      nil,
		},
	}

	for _, c := range testCases {
		subfile, err := FilterFilesByRegion(files, ranges, &c.r)
		require.Equal(t, err, c.err)
		require.Equal(t, subfile, c.subfiles)
	}
}

func TestRetryRecognizeErrCode(t *testing.T) {
	waitTime := 1 * time.Millisecond
	maxWaitTime := 16 * time.Millisecond
	ctx := context.Background()
	inner := 0
	outer := 0
	utils.WithRetry(ctx, func() error {
		e := utils.WithRetry(ctx, func() error {
			inner++
			e := status.Error(codes.Unavailable, "the connection to TiKV has been cut by a neko, meow :3")
			if e != nil {
				return errors.Trace(e)
			}
			return nil
		}, utils.NewBackoffer(10, waitTime, maxWaitTime, utils.NewErrorContext("download sst", 3)))
		outer++
		return errors.Trace(e)
	}, utils.NewBackoffer(10, waitTime, maxWaitTime, utils.NewErrorContext("import sst", 3)))
	require.Equal(t, 10, outer)
	require.Equal(t, 100, inner)
}
