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

package stream_test

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/br/pkg/stream"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/opt"
)

type statusStorePDClient struct {
	pd.Client
	stores []*metapb.Store
}

func (c *statusStorePDClient) GetAllStores(
	context.Context,
	...opt.GetStoreOption,
) ([]*metapb.Store, error) {
	return slices.Clone(c.stores), nil
}

type statusPDInfoProvider struct {
	pdClient pd.Client
}

func (p statusPDInfoProvider) GetPDClient() pd.Client {
	return p.pdClient
}

func (statusPDInfoProvider) GetTLSConfig() *tls.Config {
	return nil
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

// TestMaybeQPSSkipsReplicator verifies stream-status metrics target discovery.
// It exposes TiKV, TiFlash, and replicator stores, returning the log-backup counter only for TiKV.
// It expects TiKV and TiFlash to keep being sampled while no request reaches the replicator.
func TestMaybeQPSSkipsReplicator(t *testing.T) {
	pdClient := &statusStorePDClient{stores: []*metapb.Store{
		{Id: 1, State: metapb.StoreState_Up, StatusAddress: "tikv.test:20180"},
		{Id: 2, State: metapb.StoreState_Up, StatusAddress: "tiflash.test:20292", Labels: []*metapb.StoreLabel{{Key: "engine", Value: "tiflash"}}},
		{Id: 3, State: metapb.StoreState_Up, StatusAddress: "replicator.test:20180", Labels: []*metapb.StoreLabel{{Key: "engine", Value: "replicator"}}},
	}}
	var mu sync.Mutex
	requests := make(map[string]int)
	client := &http.Client{Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		mu.Lock()
		requests[req.URL.Host]++
		count := requests[req.URL.Host]
		mu.Unlock()
		body := ""
		if req.URL.Host == "tikv.test:20180" {
			body = fmt.Sprintf("tikv_log_backup_handle_kv_batch_sum %d", count*10)
		} else if req.URL.Host != "tiflash.test:20292" {
			return nil, fmt.Errorf("unexpected metrics request to %s", req.URL.Host)
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(body)),
			Header:     make(http.Header),
			Request:    req,
		}, nil
	})}

	qps, err := stream.MaybeQPS(context.Background(), statusPDInfoProvider{pdClient: pdClient}, client)
	require.NoError(t, err)
	require.Positive(t, qps)
	require.Equal(t, map[string]int{"tiflash.test:20292": 2, "tikv.test:20180": 2}, requests)
}
