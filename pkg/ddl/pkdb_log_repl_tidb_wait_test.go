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

package ddl

import (
	"bytes"
	"context"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/client/constants"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/tests/v3/integration"
	"google.golang.org/grpc"
)

func TestJobStateCheckAndWaitOverwrite(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("integration.NewClusterV3 creates path with colon which is not allowed on Windows")
	}

	integration.BeforeTestExternal(t)
	cluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer cluster.Terminate(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	etcdCli := cluster.RandClient()

	job := newBlockingWaitJobForTest()
	state := &jobState{job: job}

	var wg util.WaitGroupWrapper
	state.checkAndWait(ctx, []byte{1}, &wg, etcdCli, nil)
	select {
	case <-job.ver1Started:
	case <-time.After(5 * time.Second):
		t.Fatal("first job run did not start in time")
	}

	// ver2 will also cancel the pending ver1
	state.checkAndWait(ctx, []byte{2}, &wg, etcdCli, nil)
	require.Eventually(t, func() bool {
		resp, err := etcdCli.Get(ctx, job.respKey())
		require.NoError(t, err)
		return len(resp.Kvs) == 1 && bytes.Equal(resp.Kvs[0].Value, []byte{2})
	}, 5*time.Second, 50*time.Millisecond)

	wg.Wait()
	require.ErrorIs(t, job.lastErr, context.Canceled)

	resp, err := etcdCli.Get(ctx, job.respKey())
	require.NoError(t, err)
	require.Len(t, resp.Kvs, 1)
	require.Equal(t, []byte{2}, resp.Kvs[0].Value)
}

func TestTiDBWaitJobManagerInitWatchIgnoresUnexpectedKey(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("integration.NewClusterV3 creates path with colon which is not allowed on Windows")
	}

	integration.BeforeTestExternal(t)
	cluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer cluster.Terminate(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	etcdCli := cluster.RandClient()

	_, err := etcdCli.Put(ctx, constants.PkdbTiDBWaitReqPrefix+"unexpected", "payload")
	require.NoError(t, err)

	manager := newTiDBWaitJobMgrWithJobs(ctx, nil, etcdCli)
	require.NotNil(t, manager.jobMap)
	wg := sync.WaitGroup{}
	wg.Go(manager.watchLoop)
	_, err = etcdCli.Put(ctx, constants.PkdbTiDBWaitReqPrefix+"unexpected_2", "payload")
	require.NoError(t, err)
	// wait for etcd watch processing it
	time.Sleep(time.Second)
	cancel()
	wg.Wait()
	manager.close()
}

func TestInitJobMapLoadsFinishedVersionFromEtcd(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("integration.NewClusterV3 creates path with colon which is not allowed on Windows")
	}

	integration.BeforeTestExternal(t)
	cluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer cluster.Terminate(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	etcdCli := cluster.RandClient()

	resp := &pdpb.ReplicaWaitDataSync{Version: 42}
	respBytes, err := resp.Marshal()
	require.NoError(t, err)

	_, err = etcdCli.Put(ctx, constants.PkdbWaitDataSyncResp, string(respBytes))
	require.NoError(t, err)

	jobMap := initJobMap(ctx, etcdCli, &waitDataSync{}, &waitRegionsInit{})
	require.NotNil(t, jobMap)
	require.Equal(t, uint64(42), jobMap[constants.PkdbWaitDataSyncReq].lastFinishedVer)
	require.Zero(t, jobMap[constants.PkdbWaitAllRegionInitReq].lastFinishedVer)
}

func TestInitJobMapStopsRetryOnContextDone(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("integration.NewClusterV3 creates path with colon which is not allowed on Windows")
	}

	integration.BeforeTestExternal(t)
	cluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	etcdCli := cluster.RandClient()
	cluster.Terminate(t)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	start := time.Now()
	jobMap := initJobMap(ctx, etcdCli, &waitDataSync{})
	require.NotNil(t, jobMap)
	require.Contains(t, jobMap, constants.PkdbWaitDataSyncReq)
	require.Less(t, time.Since(start), 3*time.Second)
}

func mustPutWaitJobReq(t *testing.T, cli *clientv3.Client, key string, value []byte) {
	t.Helper()
	_, err := cli.Put(context.Background(), key, string(value))
	require.NoError(t, err)
}

func mustWaitJobResp(t *testing.T, cli *clientv3.Client, key string, expected []byte) {
	t.Helper()
	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		resp, err := cli.Get(ctx, key)
		require.NoError(t, err)
		return len(resp.Kvs) == 1 && bytes.Equal(resp.Kvs[0].Value, expected)
	}, 5*time.Second, 50*time.Millisecond)
}

func TestTiDBWaitJobManagerWatchLoopCompactedBeforeStart(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("integration.NewClusterV3 creates path with colon which is not allowed on Windows")
	}
	if !intest.InTest {
		t.Skip("requires --tags=intest")
	}

	integration.BeforeTestExternal(t)
	cluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer cluster.Terminate(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	etcdCli := cluster.RandClient()

	oldRetryInterval := tidbWaitJobWatchRetryInterval
	tidbWaitJobWatchRetryInterval = 10 * time.Millisecond
	defer func() { tidbWaitJobWatchRetryInterval = oldRetryInterval }()

	oldAfterInitWatch := tidbWaitJobAfterInitWatch
	oldAfterCreateWatch := tidbWaitJobAfterCreateWatch
	defer func() {
		tidbWaitJobAfterInitWatch = oldAfterInitWatch
		tidbWaitJobAfterCreateWatch = oldAfterCreateWatch
	}()

	enterAfterInitWatchHook := make(chan struct{})
	leaveAfterInitWatchHook := make(chan struct{})

	var pauseOnce sync.Once
	tidbWaitJobAfterInitWatch = func() {
		pauseOnce.Do(func() {
			close(enterAfterInitWatchHook)
			<-leaveAfterInitWatchHook
		})
	}

	var createWatchCnt atomic.Int32
	tidbWaitJobAfterCreateWatch = func() {
		createWatchCnt.Add(1)
	}

	job := newBlockingWaitJobForTest()
	historyJob := newTestWaitJob("history_job_before_start")
	manager := newTiDBWaitJobMgrWithJobs(ctx, nil, etcdCli, historyJob, job)
	doneCh := make(chan struct{})
	go func() {
		manager.watchLoop()
		close(doneCh)
	}()
	defer func() {
		cancel()
		<-doneCh
		manager.close()
	}()

	<-enterAfterInitWatchHook
	for i := 0; i < 32; i++ {
		mustPutWaitJobReq(t, etcdCli, historyJob.reqKey(), []byte{1})
	}
	getResp, err := etcdCli.Get(ctx, constants.PkdbTiDBWaitReqPrefix, clientv3.WithPrefix())
	require.NoError(t, err)
	_, err = etcdCli.Compact(ctx, getResp.Header.Revision)
	require.NoError(t, err)
	mustPutWaitJobReq(t, etcdCli, job.reqKey(), []byte{2})
	close(leaveAfterInitWatchHook)

	mustWaitJobResp(t, etcdCli, job.respKey(), []byte{2})
	time.Sleep(100 * time.Millisecond)
	// 1 for the initial watch, 1 for the retry after compaction
	require.EqualValues(t, 2, createWatchCnt.Load())
}

func TestTiDBWaitJobManagerWatchLoopCompactedDuringRunning(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("integration.NewClusterV3 creates path with colon which is not allowed on Windows")
	}
	if !intest.InTest {
		t.Skip("requires --tags=intest")
	}

	integration.BeforeTestExternal(t)
	cluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1, UseBridge: true})
	defer cluster.Terminate(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	etcdCli := cluster.RandClient()

	// We need a client that bypasses the gRPC bridge. The bridge is used to break
	// the watch connection so it will resume from a compacted revision, but we
	// still need to keep writing keys + compacting while the bridge is paused.
	bridgeURL := cluster.Members[0].GRPCURL()
	require.True(t, strings.HasSuffix(bridgeURL, "0"), "unexpected GRPCURL format: %s", bridgeURL)
	directURL := strings.TrimSuffix(bridgeURL, "0")
	directCli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{directURL},
		DialTimeout: 5 * time.Second,
		DialOptions: []grpc.DialOption{grpc.WithBlock()},
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, directCli.Close()) }()

	oldRetryInterval := tidbWaitJobWatchRetryInterval
	tidbWaitJobWatchRetryInterval = 10 * time.Millisecond
	defer func() { tidbWaitJobWatchRetryInterval = oldRetryInterval }()

	oldAfterCreateWatch := tidbWaitJobAfterCreateWatch
	defer func() { tidbWaitJobAfterCreateWatch = oldAfterCreateWatch }()
	var createWatchCnt atomic.Int32
	firstWatchCreated := make(chan struct{})
	var firstWatchOnce sync.Once
	tidbWaitJobAfterCreateWatch = func() {
		createWatchCnt.Add(1)
		firstWatchOnce.Do(func() { close(firstWatchCreated) })
	}

	historyJob := newTestWaitJob("job_during_running")
	manager := newTiDBWaitJobMgrWithJobs(ctx, nil, etcdCli, historyJob)
	doneCh := make(chan struct{})
	go func() {
		manager.watchLoop()
		close(doneCh)
	}()
	defer func() {
		cancel()
		<-doneCh
		manager.close()
	}()

	select {
	case <-firstWatchCreated:
	case <-time.After(5 * time.Second):
		t.Fatal("tidbWaitJobManager did not create etcd watch in time")
	}

	// Make sure manager is running and watching.
	mustPutWaitJobReq(t, directCli, historyJob.reqKey(), []byte{1})
	mustWaitJobResp(t, directCli, historyJob.respKey(), []byte{1})

	// Break the watch connection and prevent it from reconnecting. While the
	// connection is down, advance revisions and compact. When the watch resumes,
	// it will try to start from a compacted revision (ErrCompacted).
	cluster.Members[0].Bridge().PauseConnections()
	cluster.Members[0].Bridge().DropConnections()

	for i := 0; i < 64; i++ {
		mustPutWaitJobReq(t, directCli, historyJob.reqKey(), []byte{10})
	}
	getResp, err := directCli.Get(ctx, constants.PkdbTiDBWaitReqPrefix, clientv3.WithPrefix())
	require.NoError(t, err)
	_, err = directCli.Compact(ctx, getResp.Header.Revision)
	require.NoError(t, err)

	// This update happens after compaction. The manager must recover from the
	// compaction error and still process new requests; otherwise it will keep
	// recreating the watch on a compacted revision forever.
	mustPutWaitJobReq(t, directCli, historyJob.reqKey(), []byte{2})

	cluster.Members[0].Bridge().UnpauseConnections()
	mustWaitJobResp(t, directCli, historyJob.respKey(), []byte{2})

	time.Sleep(100 * time.Millisecond)
	// 1 for the initial watch, 1 for the retry after compaction. If the compaction
	// error isn't handled, watchLoop will keep creating watches forever.
	require.Less(t, createWatchCnt.Load(), int32(10))
}

func TestTiDBWaitJobManagerWatchLoopCompactedResyncsRequests(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("integration.NewClusterV3 creates path with colon which is not allowed on Windows")
	}
	if !intest.InTest {
		t.Skip("requires --tags=intest")
	}

	integration.BeforeTestExternal(t)
	cluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1, UseBridge: true})
	defer cluster.Terminate(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	etcdCli := cluster.RandClient()

	// We need a client that bypasses the gRPC bridge. The bridge is used to break
	// the watch connection so it will resume from a compacted revision, but we
	// still need to keep writing keys + compacting while the bridge is paused.
	bridgeURL := cluster.Members[0].GRPCURL()
	require.True(t, strings.HasSuffix(bridgeURL, "0"), "unexpected GRPCURL format: %s", bridgeURL)
	directURL := strings.TrimSuffix(bridgeURL, "0")
	directCli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{directURL},
		DialTimeout: 5 * time.Second,
		DialOptions: []grpc.DialOption{grpc.WithBlock()},
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, directCli.Close()) }()

	oldRetryInterval := tidbWaitJobWatchRetryInterval
	tidbWaitJobWatchRetryInterval = 10 * time.Millisecond
	defer func() { tidbWaitJobWatchRetryInterval = oldRetryInterval }()

	oldAfterCreateWatch := tidbWaitJobAfterCreateWatch
	defer func() { tidbWaitJobAfterCreateWatch = oldAfterCreateWatch }()
	firstWatchCreated := make(chan struct{})
	var firstWatchOnce sync.Once
	tidbWaitJobAfterCreateWatch = func() {
		firstWatchOnce.Do(func() { close(firstWatchCreated) })
	}

	targetJob := newTestWaitJob("job_compacted_without_event")
	otherJob := newTestWaitJob("job_compaction_advancer")
	manager := newTiDBWaitJobMgrWithJobs(ctx, nil, etcdCli, targetJob, otherJob)
	doneCh := make(chan struct{})
	go func() {
		manager.watchLoop()
		close(doneCh)
	}()
	defer func() {
		cancel()
		<-doneCh
		manager.close()
	}()

	select {
	case <-firstWatchCreated:
	case <-time.After(5 * time.Second):
		t.Fatal("tidbWaitJobManager did not create etcd watch in time")
	}

	// Make sure manager is running and watching.
	mustPutWaitJobReq(t, directCli, targetJob.reqKey(), []byte{1})
	mustWaitJobResp(t, directCli, targetJob.respKey(), []byte{1})

	// Break the watch connection and prevent it from reconnecting. While the
	// connection is down, write a new request and compact it away. When the watch
	// resumes, the "PUT" event for the request is no longer available, so the
	// manager must resync by reading the latest value from etcd.
	cluster.Members[0].Bridge().PauseConnections()
	cluster.Members[0].Bridge().DropConnections()

	mustPutWaitJobReq(t, directCli, targetJob.reqKey(), []byte{2})
	for i := 0; i < 64; i++ {
		mustPutWaitJobReq(t, directCli, otherJob.reqKey(), []byte{1})
	}
	getResp, err := directCli.Get(ctx, constants.PkdbTiDBWaitReqPrefix, clientv3.WithPrefix())
	require.NoError(t, err)
	_, err = directCli.Compact(ctx, getResp.Header.Revision)
	require.NoError(t, err)

	cluster.Members[0].Bridge().UnpauseConnections()
	mustWaitJobResp(t, directCli, targetJob.respKey(), []byte{2})
}

type testWaitJob struct {
	name string
}

func newTestWaitJob(name string) *testWaitJob {
	return &testWaitJob{name: name}
}

func (j *testWaitJob) reqKey() string {
	return constants.PkdbTiDBWaitReqPrefix + j.name
}

func (j *testWaitJob) respKey() string {
	return constants.PkdbTiDBWaitRespPrefix + j.name
}

func (*testWaitJob) getVersion(bs []byte) uint64 {
	if len(bs) == 0 {
		return 0
	}
	return uint64(bs[0])
}

func (j *testWaitJob) checkAndWait(
	_ context.Context,
	newJobBytes []byte,
	_ uint64,
	_ kv.Storage,
	_ *clientv3.Client,
) (uint64, error) {
	return j.getVersion(newJobBytes), nil
}

type blockingWaitJobForTest struct {
	startedOnce sync.Once
	ver1Started chan struct{}
	lastErr     error
}

func newBlockingWaitJobForTest() *blockingWaitJobForTest {
	return &blockingWaitJobForTest{
		ver1Started: make(chan struct{}),
	}
}

func (*blockingWaitJobForTest) reqKey() string {
	return constants.PkdbTiDBWaitReqPrefix + "blocking_test"
}

func (*blockingWaitJobForTest) respKey() string {
	return constants.PkdbTiDBWaitRespPrefix + "blocking_test"
}

func (*blockingWaitJobForTest) getVersion(bs []byte) uint64 {
	if len(bs) == 0 {
		return 0
	}
	return uint64(bs[0])
}

func (j *blockingWaitJobForTest) checkAndWait(
	ctx context.Context,
	newJobBytes []byte,
	_ uint64,
	_ kv.Storage,
	_ *clientv3.Client,
) (uint64, error) {
	ver := j.getVersion(newJobBytes)
	if ver == 1 {
		j.startedOnce.Do(func() {
			close(j.ver1Started)
		})
		<-ctx.Done()
		j.lastErr = ctx.Err()
		return ver, ctx.Err()
	}
	return ver, nil
}
