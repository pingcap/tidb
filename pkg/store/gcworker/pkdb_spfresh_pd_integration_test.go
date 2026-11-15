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

package gcworker

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Region routing stays in MockTiKV; both service-pin requests and global GC
// uploads go to the real PD process. Physical GC consumes that uploaded floor.
type processPinPD struct {
	pd.Client
	service func(context.Context, string, int64, uint64) (uint64, error)
	upload  func(context.Context, uint64) (uint64, error)
}

func (p *processPinPD) UpdateServiceGCSafePoint(ctx context.Context, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
	return p.service(ctx, serviceID, ttl, safePoint)
}

func (p *processPinPD) UpdateGCSafePoint(ctx context.Context, safePoint uint64) (uint64, error) {
	return p.upload(ctx, safePoint)
}

// TestSPFreshPersistentPinPDProcessRestart is opt-in because it runs a real
// PD binary and embedded etcd. All ports/data belong exclusively to this test.
func TestSPFreshPersistentPinPDProcessRestart(t *testing.T) {
	binary := os.Getenv("SPFRESH_TEST_PD_BINARY")
	if binary == "" {
		t.Skip("set SPFRESH_TEST_PD_BINARY to a local same-revision pd-server")
	}
	original := config.GetGlobalConfig().KeyspaceName
	config.UpdateGlobal(func(conf *config.Config) { conf.KeyspaceName = "" })
	t.Cleanup(func() { config.UpdateGlobal(func(conf *config.Config) { conf.KeyspaceName = original }) })
	oldInterval := gcSafePointCacheInterval
	gcSafePointCacheInterval = 0
	t.Cleanup(func() { gcSafePointCacheInterval = oldInterval })
	s := createGCWorkerSuiteWithStoreType(t, mockstore.MockTiKV, config.DefSchemaLease)
	probe := s.createGCProbe(t, "spfresh-real-pd-gc-probe")
	candidate := s.mustAllocTs(t)
	port := func() string {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		address := listener.Addr().String()
		require.NoError(t, listener.Close())
		return "http://" + address
	}
	endpoint, peer := port(), port()
	dir := t.TempDir()
	var process *exec.Cmd
	var connection *grpc.ClientConn
	var client pdpb.PDClient
	var header *pdpb.RequestHeader
	stop := func() {
		if connection != nil {
			require.NoError(t, connection.Close())
			connection = nil
		}
		if process != nil {
			require.NoError(t, process.Process.Signal(syscall.SIGTERM))
			require.NoError(t, process.Wait())
			process = nil
		}
	}
	t.Cleanup(stop)
	start := func() {
		process = exec.Command(binary, "--name=spfresh-gc-test", "--data-dir="+filepath.Join(dir, "data"),
			"--client-urls="+endpoint, "--advertise-client-urls="+endpoint, "--peer-urls="+peer, "--advertise-peer-urls="+peer,
			"--initial-cluster=spfresh-gc-test="+peer, "--log-file="+filepath.Join(dir, "pd.log"))
		require.NoError(t, process.Start())
		var err error
		connection, err = grpc.NewClient(strings.TrimPrefix(endpoint, "http://"), grpc.WithTransportCredentials(insecure.NewCredentials()))
		require.NoError(t, err)
		client = pdpb.NewPDClient(connection)
		require.Eventually(t, func() bool {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			result, err := client.GetMembers(ctx, &pdpb.GetMembersRequest{})
			if err != nil || result.GetLeader().GetName() != "spfresh-gc-test" {
				return false
			}
			header = &pdpb.RequestHeader{ClusterId: result.GetHeader().GetClusterId()}
			return true
		}, 30*time.Second, 100*time.Millisecond)
	}
	service := func(ctx context.Context, id string, ttl int64, floor uint64) (uint64, error) {
		result, err := client.UpdateServiceGCSafePoint(ctx, &pdpb.UpdateServiceGCSafePointRequest{Header: header, ServiceId: []byte(id), TTL: ttl, SafePoint: floor})
		if err != nil {
			return 0, err
		}
		if result.GetHeader().GetError() != nil {
			return 0, fmt.Errorf("PD service pin: %s", result.GetHeader().GetError())
		}
		return result.GetMinSafePoint(), nil
	}
	call := func(id string, ttl int64, floor uint64) uint64 {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		result, err := service(ctx, id, ttl, floor)
		require.NoError(t, err)
		return result
	}
	start()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	bootstrap, err := client.Bootstrap(ctx, &pdpb.BootstrapRequest{Header: header, Store: &metapb.Store{Id: 1, Address: "unused-test-store"}, Region: &metapb.Region{Id: 2, Peers: []*metapb.Peer{{Id: 3, StoreId: 1}}, RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1}}})
	require.NoError(t, err)
	require.Nil(t, bootstrap.GetHeader().GetError())
	const pin = "spfresh-checkpoint-api-v1-t2-i3-g7"
	require.LessOrEqual(t, call(pin, math.MaxInt64, probe.v1Ts), probe.v1Ts)
	require.Equal(t, probe.v1Ts, call("gc_worker", math.MaxInt64, candidate))
	var uploaded uint64
	gcPD := &processPinPD{Client: s.pdClient, service: service, upload: func(ctx context.Context, floor uint64) (uint64, error) {
		result, err := client.UpdateGCSafePoint(ctx, &pdpb.UpdateGCSafePointRequest{Header: header, SafePoint: floor})
		if err != nil {
			return 0, err
		}
		if result.GetHeader().GetError() != nil {
			return 0, fmt.Errorf("PD GC upload: %s", result.GetHeader().GetError())
		}
		uploaded = result.GetNewSafePoint()
		return uploaded, nil
	}}
	s.gcWorker.pdClient = gcPD
	runGC := func(floor uint64, versions int) {
		require.NoError(t, RunDistributedGCJob(gcContext(), s.gcWorker.regionLockResolver, s.tikvStore, gcPD, candidate, "spfresh-real-pd", 1))
		require.Equal(t, floor, uploaded)
		require.Equal(t, floor, s.loadEtcdSafePoint(t))
		require.NoError(t, s.gcWorker.doGC(gcContext(), uploaded, 1))
		require.Equal(t, versions, physicalVersions(t, s.tikvStore, []byte(probe.key)))
	}
	inspect := func() {
		etcd, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 5 * time.Second})
		require.NoError(t, err)
		defer etcd.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		records, err := etcd.Get(ctx, fmt.Sprintf("/pd/%d/gc/safe_point/service/", header.ClusterId), clientv3.WithPrefix())
		require.NoError(t, err)
		found := false
		for _, record := range records.Kvs {
			if !strings.HasSuffix(string(record.Key), "/"+pin) {
				continue
			}
			var value struct {
				ExpiredAt int64  `json:"expired_at"`
				SafePoint uint64 `json:"safe_point"`
			}
			require.NoError(t, json.Unmarshal(record.Value, &value))
			require.Equal(t, int64(math.MaxInt64), value.ExpiredAt)
			require.Equal(t, probe.v1Ts, value.SafePoint)
			found = true
		}
		require.True(t, found, "pin must exist in actual persistent etcd storage")
	}
	inspect()
	runGC(probe.v1Ts, 2) // GC starts while every SPFresh worker remains offline.
	stop()
	unavailable, cancelUnavailable := context.WithTimeout(context.Background(), time.Second)
	err = RunDistributedGCJob(unavailable, s.gcWorker.regionLockResolver, s.tikvStore, gcPD, candidate, "spfresh-real-pd", 1)
	cancelUnavailable()
	require.Error(t, err)
	require.Equal(t, probe.v1Ts, s.loadEtcdSafePoint(t))
	require.Equal(t, 2, physicalVersions(t, s.tikvStore, []byte(probe.key)))
	start() // Same data, fresh process/client; no worker renewal exists.
	inspect()
	require.Equal(t, probe.v1Ts, call("gc_worker", math.MaxInt64, candidate))
	runGC(probe.v1Ts, 2)
	require.Equal(t, candidate, call(pin, -1, 0))
	runGC(candidate, 1)
}
