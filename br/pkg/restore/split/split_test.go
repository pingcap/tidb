// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.
package split_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
	"google.golang.org/grpc"
)

type nilMetaPDClient struct {
	pd.Client
}

func (c *nilMetaPDClient) GetRegionByID(
	ctx context.Context,
	regionID uint64,
	opts ...pd.GetRegionOption,
) (*pd.Region, error) {
	return &pd.Region{}, nil
}

func TestGetRegionByIDWithNilMeta(t *testing.T) {
	client := split.NewSplitClient(&nilMetaPDClient{}, nil, false)

	region, err := client.GetRegionByID(context.Background(), 1)
	require.NoError(t, err)
	require.Nil(t, region)
}

type splitRetryPDClient struct {
	pd.Client
	address string
	region  *pd.Region
}

func (c *splitRetryPDClient) GetStore(context.Context, uint64) (*metapb.Store, error) {
	return &metapb.Store{Id: 1, Address: c.address}, nil
}

func (c *splitRetryPDClient) GetRegionByID(context.Context, uint64, ...pd.GetRegionOption) (*pd.Region, error) {
	return c.region, nil
}

type notLeaderSplitServer struct {
	tikvpb.UnimplementedTikvServer
}

func (*notLeaderSplitServer) SplitRegion(context.Context, *kvrpcpb.SplitRegionRequest) (*kvrpcpb.SplitRegionResponse, error) {
	return &kvrpcpb.SplitRegionResponse{RegionError: &errorpb.Error{
		NotLeader: &errorpb.NotLeader{RegionId: 1},
	}}, nil
}

func TestSplitRegionMissingLeader(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	tikvpb.RegisterTikvServer(server, &notLeaderSplitServer{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = server.Serve(listener)
	}()
	t.Cleanup(func() {
		server.Stop()
		<-done
	})
	meta := &metapb.Region{Id: 1, RegionEpoch: &metapb.RegionEpoch{Version: 1},
		Peers: []*metapb.Peer{{Id: 1, StoreId: 1}}}
	for _, tc := range []struct {
		name   string
		region *pd.Region
		want   error
	}{
		{"missing region", nil, berrors.ErrKVEpochNotMatch},
		{"missing metadata", &pd.Region{}, berrors.ErrKVEpochNotMatch},
		{"missing leader", &pd.Region{Meta: meta}, berrors.ErrPDLeaderNotFound},
		{"changed epoch", &pd.Region{Meta: &metapb.Region{Id: 1,
			RegionEpoch: &metapb.RegionEpoch{Version: 2}}}, berrors.ErrKVEpochNotMatch},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := split.NewSplitClient(&splitRetryPDClient{address: listener.Addr().String(), region: tc.region}, nil, false)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, err := client.BatchSplitRegions(ctx, &split.RegionInfo{Region: meta, Leader: meta.Peers[0]}, [][]byte{[]byte("m")})
			require.ErrorContains(t, err, tc.want.Error())
		})
	}
}

func TestScanRegionBackOfferWithSuccess(t *testing.T) {
	var counter int
	bo := split.NewWaitRegionOnlineBackoffer()

	err := utils.WithRetry(context.Background(), func() error {
		defer func() {
			counter++
		}()

		if counter == 3 {
			return nil
		}
		return berrors.ErrPDBatchScanRegion
	}, bo)
	require.NoError(t, err)
	require.Equal(t, counter, 4)
}

func TestScanRegionBackOfferWithFail(t *testing.T) {
	_ = failpoint.Enable("github.com/pingcap/tidb/br/pkg/restore/split/hint-scan-region-backoff", "return(true)")
	defer func() {
		_ = failpoint.Disable("github.com/pingcap/tidb/br/pkg/restore/split/hint-scan-region-backoff")
	}()

	var counter int
	bo := split.NewWaitRegionOnlineBackoffer()

	err := utils.WithRetry(context.Background(), func() error {
		defer func() {
			counter++
		}()
		return berrors.ErrPDBatchScanRegion
	}, bo)
	require.Error(t, err)
	require.Equal(t, counter, split.WaitRegionOnlineAttemptTimes)
}

func TestScanRegionBackOfferWithStopRetry(t *testing.T) {
	_ = failpoint.Enable("github.com/pingcap/tidb/br/pkg/restore/split/hint-scan-region-backoff", "return(true)")
	defer func() {
		_ = failpoint.Disable("github.com/pingcap/tidb/br/pkg/restore/split/hint-scan-region-backoff")
	}()

	var counter int
	bo := split.NewWaitRegionOnlineBackoffer()

	err := utils.WithRetry(context.Background(), func() error {
		defer func() {
			counter++
		}()

		if counter < 5 {
			return berrors.ErrPDBatchScanRegion
		}
		return berrors.ErrKVUnknown
	}, bo)
	require.Error(t, err)
	require.Equal(t, counter, 6)
}
