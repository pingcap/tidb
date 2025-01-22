// Copyright 2021 PingCAP, Inc. Licensed under Apache-2.0.

package logclient_test

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
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	logclient "github.com/pingcap/tidb/br/pkg/restore/log_client"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	"github.com/pingcap/tidb/br/pkg/utils"
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

// region: [, aay), [aay, bba), [bba, bbh), [bbh, cca), [cca, )
func initTestClient(isRawKv bool) *split.TestClient {
	keys := []string{"", "aay", "bba", "bbh", "cca", ""}
	stores := make(map[uint64]*metapb.Store)
	stores[1] = &metapb.Store{
		Id: 1,
	}
	peers := make([]*metapb.Peer, 1)
	peers[0] = &metapb.Peer{
		Id:      1,
		StoreId: 1,
	}
	regions := make(map[uint64]*split.RegionInfo)
	for i := 1; i < len(keys); i++ {
		startKey := []byte(keys[i-1])
		if len(startKey) != 0 {
			startKey = codec.EncodeBytesExt([]byte{}, startKey, isRawKv)
		}
		endKey := []byte(keys[i])
		if len(endKey) != 0 {
			endKey = codec.EncodeBytesExt([]byte{}, endKey, isRawKv)
		}
		regions[uint64(i)] = &split.RegionInfo{
			Leader: &metapb.Peer{
				Id:      uint64(i),
				StoreId: 1,
			},
			Region: &metapb.Region{
				Id:       uint64(i),
				Peers:    peers,
				StartKey: startKey,
				EndKey:   endKey,
			},
		}
	}
	return split.NewTestClient(stores, regions, 6)
}

func TestScanSuccess(t *testing.T) {
	// region: [, aay), [aay, bba), [bba, bbh), [bbh, cca), [cca, )
	cli := initTestClient(false)
	rs := utils.InitialRetryState(1, 0, 0)
	ctx := context.Background()

	// make exclusive to inclusive.
	ctl := logclient.CreateRangeController([]byte("aa"), []byte("aay"), cli, &rs)
	collectedRegions := []*split.RegionInfo{}
	ctl.ApplyFuncToRange(ctx, func(ctx context.Context, r *split.RegionInfo) logclient.RPCResult {
		collectedRegions = append(collectedRegions, r)
		return logclient.RPCResultOK()
	})
	assertRegions(t, collectedRegions, "", "aay", "bba")

	ctl = logclient.CreateRangeController([]byte("aaz"), []byte("bb"), cli, &rs)
	collectedRegions = []*split.RegionInfo{}
	ctl.ApplyFuncToRange(ctx, func(ctx context.Context, r *split.RegionInfo) logclient.RPCResult {
		collectedRegions = append(collectedRegions, r)
		return logclient.RPCResultOK()
	})
	assertRegions(t, collectedRegions, "aay", "bba", "bbh", "cca")

	ctl = logclient.CreateRangeController([]byte("aa"), []byte("cc"), cli, &rs)
	collectedRegions = []*split.RegionInfo{}
	ctl.ApplyFuncToRange(ctx, func(ctx context.Context, r *split.RegionInfo) logclient.RPCResult {
		collectedRegions = append(collectedRegions, r)
		return logclient.RPCResultOK()
	})
	assertRegions(t, collectedRegions, "", "aay", "bba", "bbh", "cca", "")

	ctl = logclient.CreateRangeController([]byte("aa"), []byte(""), cli, &rs)
	collectedRegions = []*split.RegionInfo{}
	ctl.ApplyFuncToRange(ctx, func(ctx context.Context, r *split.RegionInfo) logclient.RPCResult {
		collectedRegions = append(collectedRegions, r)
		return logclient.RPCResultOK()
	})
	assertRegions(t, collectedRegions, "", "aay", "bba", "bbh", "cca", "")
}

func TestNotLeader(t *testing.T) {
	// region: [, aay), [aay, bba), [bba, bbh), [bbh, cca), [cca, )
	cli := initTestClient(false)
	rs := utils.InitialRetryState(1, 0, 0)
	ctl := logclient.CreateRangeController([]byte(""), []byte(""), cli, &rs)
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
	err := ctl.ApplyFuncToRange(ctx, func(ctx context.Context, r *split.RegionInfo) logclient.RPCResult {
		if r.Region.Id == 2 {
			idEqualsTo2Regions = append(idEqualsTo2Regions, r)
		}
		if r.Region.Id == 2 && (r.Leader == nil || r.Leader.Id != 42) {
			return logclient.RPCResult{
				StoreError: &notLeader,
			}
		}
		meetRegions = append(meetRegions, r)
		return logclient.RPCResultOK()
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
	ctl := logclient.CreateRangeController([]byte(""), []byte(""), cli, &rs)
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
	err := ctl.ApplyFuncToRange(ctx, func(ctx context.Context, r *split.RegionInfo) logclient.RPCResult {
		if theFirstRun && r.Region.Id == 2 {
			idEqualsTo2Regions = append(idEqualsTo2Regions, r)
			theFirstRun = false
			return logclient.RPCResult{
				StoreError: &serverIsBusy,
			}
		}
		meetRegions = append(meetRegions, r)
		return logclient.RPCResultOK()
	})

	require.NoError(t, err)
	assertRegions(t, idEqualsTo2Regions, "aay", "bba")
	assertRegions(t, meetRegions, "", "aay", "bba", "bbh", "cca", "")
	require.Equal(t, rs.RemainingAttempts(), 1)
}

func TestServerIsBusyWithMemoryIsLimited(t *testing.T) {
	_ = failpoint.Enable("github.com/pingcap/tidb/br/pkg/restore/log_client/hint-memory-is-limited", "return(true)")
	defer func() {
		_ = failpoint.Disable("github.com/pingcap/tidb/br/pkg/restore/log_client/hint-memory-is-limited")
	}()

	// region: [, aay), [aay, bba), [bba, bbh), [bbh, cca), [cca, )
	cli := initTestClient(false)
	rs := utils.InitialRetryState(2, 0, 0)
	ctl := logclient.CreateRangeController([]byte(""), []byte(""), cli, &rs)
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
	err := ctl.ApplyFuncToRange(ctx, func(ctx context.Context, r *split.RegionInfo) logclient.RPCResult {
		if theFirstRun && r.Region.Id == 2 {
			idEqualsTo2Regions = append(idEqualsTo2Regions, r)
			theFirstRun = false
			return logclient.RPCResult{
				StoreError: &serverIsBusy,
			}
		}
		meetRegions = append(meetRegions, r)
		return logclient.RPCResultOK()
	})

	require.NoError(t, err)
	assertRegions(t, idEqualsTo2Regions, "aay", "bba")
	assertRegions(t, meetRegions, "", "aay", "bba", "bbh", "cca", "")
	require.Equal(t, rs.RemainingAttempts(), 2)
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
	ctl := logclient.CreateRangeController([]byte(""), []byte(""), cli, &rs)
	ctx := context.Background()

	printPDRegion("cli", cli.RegionsInfo.Regions)
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
		cli.RegionsInfo.SetRegion(newRegion)
		cli.Regions[42] = &info
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
	err = ctl.ApplyFuncToRange(ctx, func(ctx context.Context, r *split.RegionInfo) logclient.RPCResult {
		if !isSecondRun && r.Region.Id == left.Region.Id {
			mergeRegion()
			isSecondRun = true
			return logclient.RPCResultFromPBError(epochNotMatch)
		}
		if isSecondRun {
			secondRunRegions = append(secondRunRegions, r)
		} else {
			firstRunRegions = append(firstRunRegions, r)
		}
		return logclient.RPCResultOK()
	})
	printRegion("first", firstRunRegions)
	printRegion("second", secondRunRegions)
	printPDRegion("cli", cli.RegionsInfo.Regions)
	assertRegions(t, firstRunRegions, "", "aay")
	assertRegions(t, secondRunRegions, "", "aay", "bbh", "cca", "")
	require.NoError(t, err)
}

func TestRegionSplit(t *testing.T) {
	// region: [, aay), [aay, bba), [bba, bbh), [bbh, cca), [cca, )
	cli := initTestClient(false)
	rs := utils.InitialRetryState(2, 0, 0)
	ctl := logclient.CreateRangeController([]byte(""), []byte(""), cli, &rs)
	ctx := context.Background()

	printPDRegion("cli", cli.RegionsInfo.Regions)
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
			cli.RegionsInfo.SetRegion(newRegion)
			cli.Regions[r.Region.Id] = r
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
	err = ctl.ApplyFuncToRange(ctx, func(ctx context.Context, r *split.RegionInfo) logclient.RPCResult {
		if !isSecondRun && r.Region.Id == target.Region.Id {
			splitRegion()
			isSecondRun = true
			return logclient.RPCResultFromPBError(epochNotMatch)
		}
		if isSecondRun {
			secondRunRegions = append(secondRunRegions, r)
		} else {
			firstRunRegions = append(firstRunRegions, r)
		}
		return logclient.RPCResultOK()
	})
	printRegion("first", firstRunRegions)
	printRegion("second", secondRunRegions)
	printPDRegion("cli", cli.RegionsInfo.Regions)
	assertRegions(t, firstRunRegions, "", "aay")
	assertRegions(t, secondRunRegions, "", "aay", "aayy", "bba", "bbh", "cca", "")
	require.NoError(t, err)
}

func TestRetryBackoff(t *testing.T) {
	// region: [, aay), [aay, bba), [bba, bbh), [bbh, cca), [cca, )
	cli := initTestClient(false)
	rs := utils.InitialRetryState(2, time.Millisecond, 10*time.Millisecond)
	ctl := logclient.CreateRangeController([]byte(""), []byte(""), cli, &rs)
	ctx := context.Background()

	printPDRegion("cli", cli.RegionsInfo.Regions)
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
	err = ctl.ApplyFuncToRange(ctx, func(ctx context.Context, r *split.RegionInfo) logclient.RPCResult {
		if !isSecondRun && r.Region.Id == left.Region.Id {
			isSecondRun = true
			return logclient.RPCResultFromPBError(epochNotLeader)
		}
		return logclient.RPCResultOK()
	})
	printPDRegion("cli", cli.RegionsInfo.Regions)
	require.Equal(t, 1, rs.RemainingAttempts())
	// we retried leader not found error. so the next backoff should be 2 * initical backoff.
	require.Equal(t, 2*time.Millisecond, rs.ExponentialBackoff())
	require.NoError(t, err)
}

func TestWrappedError(t *testing.T) {
	result := logclient.RPCResultFromError(errors.Trace(status.Error(codes.Unavailable, "the server is slacking. ><=·>")))
	require.Equal(t, result.StrategyForRetry(), logclient.StrategyFromThisRegion)
	result = logclient.RPCResultFromError(errors.Trace(status.Error(codes.Unknown, "the server said something hard to understand")))
	require.Equal(t, result.StrategyForRetry(), logclient.StrategyGiveUp)
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
	ctl := logclient.CreateRangeController([]byte("aa"), []byte("aaz"), cli, &rs)
	ctx := context.Background()

	cli.InjectErr = true
	cli.InjectTimes = int32(envInt("PAGINATE_SCAN_LEADER_FAILURE_COUNT", 2))
	collectedRegions := []*split.RegionInfo{}
	ctl.ApplyFuncToRange(ctx, func(ctx context.Context, r *split.RegionInfo) logclient.RPCResult {
		collectedRegions = append(collectedRegions, r)
		return logclient.RPCResultOK()
	})
	assertRegions(t, collectedRegions, "", "aay", "bba")
}

func TestRetryRecognizeErrCode(t *testing.T) {
	waitTime := 1 * time.Millisecond
	maxWaitTime := 16 * time.Millisecond
	ctx := context.Background()
	inner := 0
	outer := 0
	_ = utils.WithRetry(ctx, func() error {
		e := utils.WithRetry(ctx, func() error {
			inner++
			e := status.Error(codes.Unavailable, "the connection to TiKV has been cut by a neko, meow :3")
			if e != nil {
				return errors.Trace(e)
			}
			return nil
		}, utils.NewBackoffRetryAllErrorStrategy(10, waitTime, maxWaitTime))
		outer++
		return errors.Trace(e)
	}, utils.NewBackoffRetryAllErrorStrategy(10, waitTime, maxWaitTime))
	require.Equal(t, 10, outer)
	require.Equal(t, 100, inner)
}
