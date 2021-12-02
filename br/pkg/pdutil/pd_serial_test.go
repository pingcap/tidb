// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package pdutil

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/coreos/go-semver/semver"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/util/codec"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/typeutil"
	"github.com/tikv/pd/server/api"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/statistics"
)

func TestScheduler(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	scheduler := "balance-leader-scheduler"
	mock := func(context.Context, string, string, *http.Client, string, io.Reader) ([]byte, error) {
		return nil, errors.New("failed")
	}
	schedulerPauseCh := make(chan struct{})
	pdController := &PdController{addrs: []string{"", ""}, schedulerPauseCh: schedulerPauseCh}
	// As pdController.Client is nil, (*pdController).Close() can not be called directly.
	defer close(schedulerPauseCh)

	_, err := pdController.pauseSchedulersAndConfigWith(ctx, []string{scheduler}, nil, mock)
	require.EqualError(t, err, "failed")

	go func() {
		<-schedulerPauseCh
	}()
	err = pdController.resumeSchedulerWith(ctx, []string{scheduler}, mock)
	require.NoError(t, err)

	cfg := map[string]interface{}{
		"max-merge-region-keys":       0,
		"max-snapshot":                1,
		"enable-location-replacement": false,
		"max-pending-peer-count":      uint64(16),
	}
	_, err = pdController.pauseSchedulersAndConfigWith(ctx, []string{}, cfg, mock)
	require.Error(t, err)
	require.Regexp(t, "^failed to update PD", err.Error())
	go func() {
		<-schedulerPauseCh
	}()
	err = pdController.resumeSchedulerWith(ctx, []string{scheduler}, mock)
	require.NoError(t, err)

	_, err = pdController.listSchedulersWith(ctx, mock)
	require.EqualError(t, err, "failed")

	mock = func(context.Context, string, string, *http.Client, string, io.Reader) ([]byte, error) {
		return []byte(`["` + scheduler + `"]`), nil
	}

	_, err = pdController.pauseSchedulersAndConfigWith(ctx, []string{scheduler}, cfg, mock)
	require.NoError(t, err)

	// pauseSchedulersAndConfigWith will wait on chan schedulerPauseCh
	err = pdController.resumeSchedulerWith(ctx, []string{scheduler}, mock)
	require.NoError(t, err)

	schedulers, err := pdController.listSchedulersWith(ctx, mock)
	require.NoError(t, err)
	require.Len(t, schedulers, 1)
	require.Equal(t, scheduler, schedulers[0])
}

func TestGetClusterVersion(t *testing.T) {
	pdController := &PdController{addrs: []string{"", ""}} // two endpoints
	counter := 0
	mock := func(context.Context, string, string, *http.Client, string, io.Reader) ([]byte, error) {
		counter++
		if counter <= 1 {
			return nil, errors.New("mock error")
		}
		return []byte(`test`), nil
	}

	ctx := context.Background()
	respString, err := pdController.getClusterVersionWith(ctx, mock)
	require.NoError(t, err)
	require.Equal(t, "test", respString)

	mock = func(context.Context, string, string, *http.Client, string, io.Reader) ([]byte, error) {
		return nil, errors.New("mock error")
	}
	_, err = pdController.getClusterVersionWith(ctx, mock)
	require.Error(t, err)
}

func TestRegionCount(t *testing.T) {
	regions := core.NewRegionsInfo()
	regions.SetRegion(core.NewRegionInfo(&metapb.Region{
		Id:          1,
		StartKey:    codec.EncodeBytes(nil, []byte{1, 1}),
		EndKey:      codec.EncodeBytes(nil, []byte{1, 3}),
		RegionEpoch: &metapb.RegionEpoch{},
	}, nil))
	regions.SetRegion(core.NewRegionInfo(&metapb.Region{
		Id:          2,
		StartKey:    codec.EncodeBytes(nil, []byte{1, 3}),
		EndKey:      codec.EncodeBytes(nil, []byte{1, 5}),
		RegionEpoch: &metapb.RegionEpoch{},
	}, nil))
	regions.SetRegion(core.NewRegionInfo(&metapb.Region{
		Id:          3,
		StartKey:    codec.EncodeBytes(nil, []byte{2, 3}),
		EndKey:      codec.EncodeBytes(nil, []byte{3, 4}),
		RegionEpoch: &metapb.RegionEpoch{},
	}, nil))
	require.Equal(t, 3, regions.Len())

	mock := func(
		_ context.Context, addr string, prefix string, _ *http.Client, _ string, _ io.Reader,
	) ([]byte, error) {
		query := fmt.Sprintf("%s/%s", addr, prefix)
		u, e := url.Parse(query)
		require.NoError(t, e, query)
		start := u.Query().Get("start_key")
		end := u.Query().Get("end_key")
		t.Log(hex.EncodeToString([]byte(start)))
		t.Log(hex.EncodeToString([]byte(end)))
		scanRegions := regions.ScanRange([]byte(start), []byte(end), 0)
		stats := statistics.RegionStats{Count: len(scanRegions)}
		ret, err := json.Marshal(stats)
		require.NoError(t, err)
		return ret, nil
	}

	pdController := &PdController{addrs: []string{"http://mock"}}
	ctx := context.Background()
	resp, err := pdController.getRegionCountWith(ctx, mock, []byte{}, []byte{})
	require.NoError(t, err)
	require.Equal(t, 3, resp)

	resp, err = pdController.getRegionCountWith(ctx, mock, []byte{0}, []byte{0xff})
	require.NoError(t, err)
	require.Equal(t, 3, resp)

	resp, err = pdController.getRegionCountWith(ctx, mock, []byte{1, 2}, []byte{1, 4})
	require.NoError(t, err)
	require.Equal(t, 2, resp)
}

func TestPDVersion(t *testing.T) {
	v := []byte("\"v4.1.0-alpha1\"\n")
	r := parseVersion(v)
	expectV := semver.New("4.1.0-alpha1")
	require.Equal(t, expectV.Major, r.Major)
	require.Equal(t, expectV.Minor, r.Minor)
	require.Equal(t, expectV.PreRelease, r.PreRelease)
}

func TestPDRequestRetry(t *testing.T) {
	ctx := context.Background()

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/br/pkg/pdutil/FastRetry", "return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/br/pkg/pdutil/FastRetry"))
	}()

	count := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count++
		if count <= pdRequestRetryTime-1 {
			w.WriteHeader(http.StatusGatewayTimeout)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	cli := http.DefaultClient
	taddr := ts.URL
	_, reqErr := pdRequest(ctx, taddr, "", cli, http.MethodGet, nil)
	require.NoError(t, reqErr)
	ts.Close()
	count = 0
	ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count++
		if count <= pdRequestRetryTime+1 {
			w.WriteHeader(http.StatusGatewayTimeout)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()
	taddr = ts.URL
	_, reqErr = pdRequest(ctx, taddr, "", cli, http.MethodGet, nil)
	require.Error(t, reqErr)
}

func TestStoreInfo(t *testing.T) {
	storeInfo := api.StoreInfo{
		Status: &api.StoreStatus{
			Capacity:  typeutil.ByteSize(1024),
			Available: typeutil.ByteSize(1024),
		},
		Store: &api.MetaStore{
			StateName: "Tombstone",
		},
	}
	mock := func(
		_ context.Context, addr string, prefix string, _ *http.Client, _ string, _ io.Reader,
	) ([]byte, error) {
		query := fmt.Sprintf("%s/%s", addr, prefix)
		require.Equal(t, "http://mock/pd/api/v1/store/1", query)
		ret, err := json.Marshal(storeInfo)
		require.NoError(t, err)
		return ret, nil
	}

	pdController := &PdController{addrs: []string{"http://mock"}}
	ctx := context.Background()
	resp, err := pdController.getStoreInfoWith(ctx, mock, 1)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.NotNil(t, resp.Status)
	require.Equal(t, "Tombstone", resp.Store.StateName)
	require.Equal(t, uint64(1024), uint64(resp.Status.Available))
}
