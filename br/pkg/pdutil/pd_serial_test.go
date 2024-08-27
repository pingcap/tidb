// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package pdutil

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/coreos/go-semver/semver"
	perrors "github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/store/mockstore/unistore"
	"github.com/stretchr/testify/require"
	pdhttp "github.com/tikv/pd/client/http"
)

type mockPDHTTPClient struct {
	pdhttp.Client
	err error
}

func (c *mockPDHTTPClient) SetSchedulerDelay(context.Context, string, int64) error {
	return c.err
}

func (c *mockPDHTTPClient) SetConfig(context.Context, map[string]any, ...float64) error {
	return c.err
}

func (c *mockPDHTTPClient) ResetTS(context.Context, uint64, bool) error {
	return c.err
}

func TestScheduler(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	scheduler := "balance-leader-scheduler"
	mock := &mockPDHTTPClient{err: errors.New("failed")}
	schedulerPauseCh := make(chan struct{})
	pdController := &PdController{pdHTTPCli: mock, schedulerPauseCh: schedulerPauseCh}
	// As pdController.Client is nil, (*pdController).Close() can not be called directly.
	defer close(schedulerPauseCh)

	_, err := pdController.pauseSchedulersAndConfigWith(ctx, []string{scheduler}, nil)
	require.EqualError(t, err, "failed")

	go func() {
		<-schedulerPauseCh
	}()
	err = pdController.resumeSchedulerWith(ctx, []string{scheduler})
	require.NoError(t, err)

	cfg := map[string]any{
		"max-merge-region-keys":       0,
		"max-snapshot":                1,
		"enable-location-replacement": false,
		"max-pending-peer-count":      uint64(16),
	}
	_, err = pdController.pauseSchedulersAndConfigWith(ctx, []string{}, cfg)
	require.Error(t, err)
	require.Regexp(t, "^failed to update PD", err.Error())
	go func() {
		<-schedulerPauseCh
	}()
	err = pdController.resumeSchedulerWith(ctx, []string{scheduler})
	require.NoError(t, err)

	mock.err = nil

	_, err = pdController.pauseSchedulersAndConfigWith(ctx, []string{scheduler}, cfg)
	require.NoError(t, err)

	// pauseSchedulersAndConfigWith will wait on chan schedulerPauseCh
	err = pdController.resumeSchedulerWith(ctx, []string{scheduler})
	require.NoError(t, err)
}

func TestPDVersion(t *testing.T) {
	v := "\"v4.1.0-alpha1\"\n"
	r := parseVersion(v)
	expectV := semver.New("4.1.0-alpha1")
	require.Equal(t, expectV.Major, r.Major)
	require.Equal(t, expectV.Minor, r.Minor)
	require.Equal(t, expectV.PreRelease, r.PreRelease)
}

func TestPDResetTSCompatibility(t *testing.T) {
	ctx := context.Background()
	cli := &mockPDHTTPClient{
		err: perrors.Errorf("request pd http api failed with status: '%s'", http.StatusText(http.StatusForbidden)),
	}

	pd := PdController{pdHTTPCli: cli}
	reqErr := pd.ResetTS(ctx, 123)
	require.NoError(t, reqErr)

	cli.err = nil
	reqErr = pd.ResetTS(ctx, 123)
	require.NoError(t, reqErr)
}

func TestPauseSchedulersByKeyRange(t *testing.T) {
	const ttl = time.Second

	labelExpires := make(map[string]time.Time)

	var (
		mu      sync.Mutex
		deleted bool
	)

	httpSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if deleted {
			return
		}
		switch r.Method {
		case http.MethodPatch:
			var patch pdhttp.LabelRulePatch
			err := json.NewDecoder(r.Body).Decode(&patch)
			require.NoError(t, err)
			require.Len(t, patch.SetRules, 0)
			require.Len(t, patch.DeleteRules, 1)
			delete(labelExpires, patch.DeleteRules[0])
			deleted = true
		case http.MethodPost:
			var labelRule LabelRule
			err := json.NewDecoder(r.Body).Decode(&labelRule)
			require.NoError(t, err)
			require.Len(t, labelRule.Labels, 1)
			regionLabel := labelRule.Labels[0]
			require.Equal(t, "schedule", regionLabel.Key)
			require.Equal(t, "deny", regionLabel.Value)
			reqTTL, err := time.ParseDuration(regionLabel.TTL)
			require.NoError(t, err)
			if reqTTL == 0 {
				delete(labelExpires, labelRule.ID)
			} else {
				require.Equal(t, ttl, reqTTL)
				if expire, ok := labelExpires[labelRule.ID]; ok {
					require.True(t, expire.After(time.Now()), "should not expire before now")
				}
				labelExpires[labelRule.ID] = time.Now().Add(ttl)
			}
		}
	}))
	defer httpSrv.Close()

	pdHTTPCli := pdhttp.NewClientWithServiceDiscovery("test", unistore.NewMockPDServiceDiscovery([]string{httpSrv.URL}))
	defer pdHTTPCli.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done, err := pauseSchedulerByKeyRangeWithTTL(ctx, pdHTTPCli, []byte{0, 0, 0, 0}, []byte{0xff, 0xff, 0xff, 0xff}, ttl)
	require.NoError(t, err)
	time.Sleep(ttl * 3)
	cancel()
	<-done
	require.Len(t, labelExpires, 0)
}
