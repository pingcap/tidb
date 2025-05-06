// Copyright 2019 PingCAP, Inc.
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

package owner_test

import (
	"context"
	"fmt"
	"net/url"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/owner"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.etcd.io/etcd/server/v3/embed"
	"go.etcd.io/etcd/tests/v3/integration"
	"golang.org/x/exp/rand"
)

type testInfo struct {
	cluster *integration.ClusterV3
	client  *clientv3.Client
}

func newTestInfo(t *testing.T) *testInfo {
	cluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	return &testInfo{
		cluster: cluster,
		client:  cluster.Client(0),
	}
}

func (ti *testInfo) Close(t *testing.T) {
	ti.cluster.Terminate(t)
}

type listener struct {
	val atomic.Bool
}

func (l *listener) OnBecomeOwner() {
	l.val.Store(true)
}
func (l *listener) OnRetireOwner() {
	l.val.Store(false)
}

func TestForceToBeOwner(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("integration.NewClusterV3 will create file contains a colon which is not allowed on Windows")
	}
	integration.BeforeTestExternal(t)

	tInfo := newTestInfo(t)
	client := tInfo.client
	defer tInfo.Close(t)

	// put a key with same prefix to mock another node
	ctx := context.Background()
	testKey := "/owner/key/a"
	_, err := client.Put(ctx, testKey, "a")
	require.NoError(t, err)
	resp, err := client.Get(ctx, testKey)
	require.NoError(t, err)
	require.Len(t, resp.Kvs, 1)

	bak := owner.WaitTimeOnForceOwner
	t.Cleanup(func() {
		owner.WaitTimeOnForceOwner = bak
	})
	owner.WaitTimeOnForceOwner = time.Millisecond
	ownerMgr := owner.NewOwnerManager(ctx, client, "ddl", "1", "/owner/key")
	defer ownerMgr.Close()
	lis := &listener{}
	ownerMgr.SetListener(lis)
	require.NoError(t, ownerMgr.ForceToBeOwner(ctx))
	// key of other node is deleted
	resp, err = client.Get(ctx, testKey)
	require.NoError(t, err)
	require.Empty(t, resp.Kvs)
	require.NoError(t, ownerMgr.CampaignOwner())
	isOwner := checkOwner(ownerMgr, true)
	require.True(t, isOwner)
	require.True(t, lis.val.Load())
}

func TestSingle(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("integration.NewClusterV3 will create file contains a colon which is not allowed on Windows")
	}
	integration.BeforeTestExternal(t)

	tInfo := newTestInfo(t)
	client := tInfo.client
	defer tInfo.Close(t)

	t.Run("retry on session closed before election", func(t *testing.T) {
		ownerMgr := owner.NewOwnerManager(context.Background(), client, "ddl", "1", "/owner/key")
		defer ownerMgr.Close()
		var counter atomic.Int32
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/owner/beforeElectionCampaign",
			func(se *concurrency.Session) {
				if counter.Add(1) <= 1 {
					require.NoError(t, se.Close())
				}
			},
		)
		require.NoError(t, ownerMgr.CampaignOwner())
		isOwner := checkOwner(ownerMgr, true)
		require.True(t, isOwner)
		require.EqualValues(t, 2, counter.Load())
	})

	t.Run("retry on lease revoked before election", func(t *testing.T) {
		ownerMgr := owner.NewOwnerManager(context.Background(), client, "ddl", "1", "/owner/key")
		defer ownerMgr.Close()
		var counter atomic.Int32
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/owner/beforeElectionCampaign",
			func(se *concurrency.Session) {
				if counter.Add(1) <= 2 {
					_, err := client.Revoke(context.Background(), se.Lease())
					require.NoError(t, err)
				}
			},
		)
		require.NoError(t, ownerMgr.CampaignOwner())
		isOwner := checkOwner(ownerMgr, true)
		require.True(t, isOwner)
		require.EqualValues(t, 3, counter.Load())
	})

	ownerMgr := owner.NewOwnerManager(context.Background(), client, "ddl", "1", "/owner/key")
	lis := &listener{}
	ownerMgr.SetListener(lis)
	require.NoError(t, ownerMgr.CampaignOwner())
	isOwner := checkOwner(ownerMgr, true)
	require.True(t, isOwner)
	require.True(t, lis.val.Load())

	// test for newSession failed
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	ownerMgr2 := owner.NewOwnerManager(ctx, client, "ddl", "2", "/owner/key")
	defer ownerMgr2.Close()
	cancel()

	err := ownerMgr2.CampaignOwner()
	comment := fmt.Sprintf("campaigned result don't match, err %v", err)
	require.True(t, terror.ErrorEqual(err, context.Canceled) || terror.ErrorEqual(err, context.DeadlineExceeded), comment)

	isOwner = checkOwner(ownerMgr, true)
	require.True(t, isOwner)

	// The test is used to exit campaign loop.
	ownerMgr.Close()
	isOwner = checkOwner(ownerMgr, false)
	require.False(t, isOwner)
	require.False(t, lis.val.Load())

	time.Sleep(200 * time.Millisecond)

	// err is ok to be not nil since we canceled the manager.
	ownerID, _ := ownerMgr2.GetOwnerID(ctx)
	require.Equal(t, "", ownerID)
	op, _ := owner.GetOwnerOpValue(ctx, client, "/owner/key")
	require.Equal(t, op, owner.OpNone)
}

func TestSetAndGetOwnerOpValue(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("integration.NewClusterV3 will create file contains a colon which is not allowed on Windows")
	}
	integration.BeforeTestExternal(t)

	tInfo := newTestInfo(t)
	defer tInfo.Close(t)

	ownerMgr := owner.NewOwnerManager(context.Background(), tInfo.client, "ddl", "1", "/owner/key")
	defer ownerMgr.Close()
	require.NoError(t, ownerMgr.CampaignOwner())
	isOwner := checkOwner(ownerMgr, true)
	require.True(t, isOwner)

	// test set/get owner info
	ownerID, err := ownerMgr.GetOwnerID(context.Background())
	require.NoError(t, err)
	require.Equal(t, ownerMgr.ID(), ownerID)
	op, err := owner.GetOwnerOpValue(context.Background(), tInfo.client, "/owner/key")
	require.NoError(t, err)
	require.Equal(t, op, owner.OpNone)
	require.False(t, op.IsSyncedUpgradingState())
	err = ownerMgr.SetOwnerOpValue(context.Background(), owner.OpSyncUpgradingState)
	require.NoError(t, err)
	op, err = owner.GetOwnerOpValue(context.Background(), tInfo.client, "/owner/key")
	require.NoError(t, err)
	require.Equal(t, op, owner.OpSyncUpgradingState)
	require.True(t, op.IsSyncedUpgradingState())
	// update the same as the original value
	err = ownerMgr.SetOwnerOpValue(context.Background(), owner.OpSyncUpgradingState)
	require.NoError(t, err)
	op, err = owner.GetOwnerOpValue(context.Background(), tInfo.client, "/owner/key")
	require.NoError(t, err)
	require.Equal(t, op, owner.OpSyncUpgradingState)
	require.True(t, op.IsSyncedUpgradingState())
	// test del owner key when SetOwnerOpValue
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/owner/MockDelOwnerKey", `return("delOwnerKeyAndNotOwner")`))
	err = ownerMgr.SetOwnerOpValue(context.Background(), owner.OpNone)
	require.ErrorContains(t, err, "put owner key failed, cmp is false")
	op, err = owner.GetOwnerOpValue(context.Background(), tInfo.client, "/owner/key")
	require.NotNil(t, err)
	require.Equal(t, concurrency.ErrElectionNoLeader.Error(), err.Error())
	require.Equal(t, op, owner.OpNone)
	require.False(t, op.IsSyncedUpgradingState())
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/owner/MockDelOwnerKey"))

	// Let ddl run for the owner again.
	require.NoError(t, ownerMgr.CampaignOwner())
	isOwner = checkOwner(ownerMgr, true)
	require.True(t, isOwner)
	// Mock the manager become not owner because the owner is deleted(like TTL is timeout).
	// And then the manager campaigns the owner again, and become the owner.
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/owner/MockDelOwnerKey", `return("onlyDelOwnerKey")`))
	err = ownerMgr.SetOwnerOpValue(context.Background(), owner.OpSyncUpgradingState)
	require.ErrorContains(t, err, "put owner key failed, cmp is false")
	isOwner = checkOwner(ownerMgr, true)
	require.True(t, isOwner)
	op, err = owner.GetOwnerOpValue(context.Background(), tInfo.client, "/owner/key")
	require.NoError(t, err)
	require.Equal(t, op, owner.OpNone)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/owner/MockDelOwnerKey"))
}

// TestGetOwnerOpValueBeforeSet tests get owner opValue before set this value when the etcdClient is nil.
func TestGetOwnerOpValueBeforeSet(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("integration.NewClusterV3 will create file contains a colon which is not allowed on Windows")
	}
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/owner/MockNotSetOwnerOp", `return(true)`))

	ownerMgr := owner.NewMockManager(context.Background(), "1", nil, "/owner/key")
	defer ownerMgr.Close()
	require.NoError(t, ownerMgr.CampaignOwner())
	isOwner := checkOwner(ownerMgr, true)
	require.True(t, isOwner)

	// test set/get owner info
	ownerID, err := ownerMgr.GetOwnerID(context.Background())
	require.NoError(t, err)
	require.Equal(t, ownerMgr.ID(), ownerID)
	op, err := owner.GetOwnerOpValue(context.Background(), nil, "/owner/key")
	require.NoError(t, err)
	require.Equal(t, op, owner.OpNone)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/owner/MockNotSetOwnerOp"))
	err = ownerMgr.SetOwnerOpValue(context.Background(), owner.OpSyncUpgradingState)
	require.NoError(t, err)
	op, err = owner.GetOwnerOpValue(context.Background(), nil, "/owner/key")
	require.NoError(t, err)
	require.Equal(t, op, owner.OpSyncUpgradingState)
}

func TestCluster(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("integration.NewClusterV3 will create file contains a colon which is not allowed on Windows")
	}
	integration.BeforeTestExternal(t)

	originalTTL := owner.ManagerSessionTTL
	owner.ManagerSessionTTL = 3
	defer func() {
		owner.ManagerSessionTTL = originalTTL
	}()

	tInfo := newTestInfo(t)
	defer tInfo.Close(t)
	ownerMgr := owner.NewOwnerManager(context.Background(), tInfo.client, "ddl", "1", "/owner/key")
	require.NoError(t, ownerMgr.CampaignOwner())

	isOwner := checkOwner(ownerMgr, true)
	require.True(t, isOwner)

	ownerMgr2 := owner.NewOwnerManager(context.Background(), tInfo.client, "ddl", "2", "/owner/key")
	require.NoError(t, ownerMgr2.CampaignOwner())

	isOwner = checkOwner(ownerMgr2, false)
	require.False(t, isOwner)

	// Delete the leader key, the d1 become the owner.
	err := deleteLeader(tInfo.client, "/owner/key")
	require.NoError(t, err)

	isOwner = checkOwner(ownerMgr, false)
	require.False(t, isOwner)

	ownerMgr.Close()
	// d3 (not owner) stop
	ownerMgr3 := owner.NewOwnerManager(context.Background(), tInfo.client, "ddl", "3", "/owner/key")
	require.NoError(t, ownerMgr3.CampaignOwner())

	isOwner = checkOwner(ownerMgr3, false)
	require.False(t, isOwner)

	ownerMgr3.Close()
	// Cancel the owner context, there is no owner.
	ownerMgr2.Close()

	_, _, err = owner.GetOwnerKeyInfo(context.Background(), tInfo.client, "/owner/key", "useless id")
	require.Truef(t, terror.ErrorEqual(err, concurrency.ErrElectionNoLeader), "get owner info result don't match, err %v", err)
	op, err := owner.GetOwnerOpValue(context.Background(), tInfo.client, "/owner/key")
	require.Truef(t, terror.ErrorEqual(err, concurrency.ErrElectionNoLeader), "get owner info result don't match, err %v", err)
	require.Equal(t, op, owner.OpNone)
}

func TestWatchOwner(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("integration.NewClusterV3 will create file contains a colon which is not allowed on Windows")
	}
	integration.BeforeTestExternal(t)

	tInfo := newTestInfo(t)
	client := tInfo.client
	defer tInfo.Close(t)
	ownerMgr := owner.NewOwnerManager(context.Background(), client, "ddl", "1", "/owner/key")
	defer ownerMgr.Close()
	lis := &listener{}
	ownerMgr.SetListener(lis)
	require.NoError(t, ownerMgr.CampaignOwner())
	isOwner := checkOwner(ownerMgr, true)
	require.True(t, isOwner)

	// get the owner id.
	ctx := context.Background()
	id, err := ownerMgr.GetOwnerID(ctx)
	require.NoError(t, err)

	// create etcd session.
	session, err := concurrency.NewSession(client)
	require.NoError(t, err)

	// test the GetOwnerKeyInfo()
	ownerKey, currRevision, err := owner.GetOwnerKeyInfo(ctx, client, "/owner/key", id)
	require.NoError(t, err)

	// watch the ownerKey.
	ctx2, cancel2 := context.WithTimeout(ctx, time.Millisecond*300)
	defer cancel2()
	watchDone := make(chan bool)
	watched := false
	go func() {
		watchErr := owner.WatchOwnerForTest(ctx, ownerMgr, session, ownerKey, currRevision)
		require.NoError(t, watchErr)
		watchDone <- true
	}()

	select {
	case watched = <-watchDone:
	case <-ctx2.Done():
	}
	require.False(t, watched)

	// delete the owner, and can watch the DELETE event.
	err = deleteLeader(client, "/owner/key")
	require.NoError(t, err)
	watched = <-watchDone
	require.True(t, watched)

	// the ownerKey has been deleted, watch ownerKey again, it can be watched.
	go func() {
		watchErr := owner.WatchOwnerForTest(ctx, ownerMgr, session, ownerKey, currRevision)
		require.NoError(t, watchErr)
		watchDone <- true
	}()

	watched = <-watchDone
	require.True(t, watched)
}

func TestWatchOwnerAfterDeleteOwnerKey(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("integration.NewClusterV3 will create file contains a colon which is not allowed on Windows")
	}
	integration.BeforeTestExternal(t)

	tInfo := newTestInfo(t)
	client := tInfo.client
	defer tInfo.Close(t)
	ownerMgr := owner.NewOwnerManager(context.Background(), client, "ddl", "1", "/owner/key")
	defer ownerMgr.Close()
	lis := &listener{}
	ownerMgr.SetListener(lis)
	require.NoError(t, ownerMgr.CampaignOwner())
	isOwner := checkOwner(ownerMgr, true)
	require.True(t, isOwner)

	// get the owner id.
	ctx := context.Background()
	id, err := ownerMgr.GetOwnerID(ctx)
	require.NoError(t, err)
	session, err := concurrency.NewSession(client)
	require.NoError(t, err)

	// get the ownkey informations.
	ownerKey, currRevision, err := owner.GetOwnerKeyInfo(ctx, client, "/owner/key", id)
	require.NoError(t, err)

	// delete the ownerkey
	err = deleteLeader(client, "/owner/key")
	require.NoError(t, err)

	// watch the ownerKey with the current revisoin.
	watchDone := make(chan bool)
	go func() {
		watchErr := owner.WatchOwnerForTest(ctx, ownerMgr, session, ownerKey, currRevision)
		require.NoError(t, watchErr)
		watchDone <- true
	}()
	<-watchDone
}

func checkOwner(ownerMgr owner.Manager, fbVal bool) (isOwner bool) {
	// The longest to wait for 30 seconds to
	// make sure that campaigning owners is completed.
	for range 6000 {
		time.Sleep(5 * time.Millisecond)
		isOwner = ownerMgr.IsOwner()
		if isOwner == fbVal {
			break
		}
	}
	return
}

func deleteLeader(cli *clientv3.Client, prefixKey string) error {
	session, err := concurrency.NewSession(cli)
	if err != nil {
		return errors.Trace(err)
	}
	defer func() {
		_ = session.Close()
	}()
	election := concurrency.NewElection(session, prefixKey)
	resp, err := election.Leader(context.Background())
	if err != nil {
		return errors.Trace(err)
	}
	_, err = cli.Delete(context.Background(), string(resp.Kvs[0].Key))
	return errors.Trace(err)
}

func TestImmediatelyCancel(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("integration.NewClusterV3 will create file contains a colon which is not allowed on Windows")
	}
	integration.BeforeTestExternal(t)

	tInfo := newTestInfo(t)
	defer tInfo.Close(t)
	ownerMgr := owner.NewOwnerManager(context.Background(), tInfo.client, "ddl", "1", "/owner/key")
	defer ownerMgr.Close()
	for range 10 {
		err := ownerMgr.CampaignOwner()
		require.NoError(t, err)
		ownerMgr.CampaignCancel()
	}
}

func TestAcquireDistributedLock(t *testing.T) {
	const addrFmt = "http://127.0.0.1:%d"
	cfg := embed.NewConfig()
	cfg.Dir = t.TempDir()
	// rand port in [20000, 60000)
	randPort := int(rand.Int31n(40000)) + 20000
	clientAddr := fmt.Sprintf(addrFmt, randPort)
	lcurl, _ := url.Parse(clientAddr)
	cfg.ListenClientUrls, cfg.AdvertiseClientUrls = []url.URL{*lcurl}, []url.URL{*lcurl}
	lpurl, _ := url.Parse(fmt.Sprintf(addrFmt, randPort+1))
	cfg.ListenPeerUrls, cfg.AdvertisePeerUrls = []url.URL{*lpurl}, []url.URL{*lpurl}
	cfg.InitialCluster = "default=" + lpurl.String()
	cfg.Logger = "zap"
	embedEtcd, err := embed.StartEtcd(cfg)
	require.NoError(t, err)
	<-embedEtcd.Server.ReadyNotify()
	t.Cleanup(func() {
		embedEtcd.Close()
	})
	makeEtcdCli := func(t *testing.T) (cli *clientv3.Client) {
		cli, err := clientv3.New(clientv3.Config{
			Endpoints: []string{lcurl.String()},
		})
		require.NoError(t, err)
		t.Cleanup(func() {
			cli.Close()
		})
		return cli
	}
	t.Run("acquire distributed lock with same client", func(t *testing.T) {
		cli := makeEtcdCli(t)
		getLock := make(chan struct{})
		ctx := context.Background()

		release1, err := owner.AcquireDistributedLock(ctx, cli, "test-lock", 10)
		require.NoError(t, err)
		var wg util.WaitGroupWrapper
		wg.Run(func() {
			// Acquire another distributed lock will be blocked.
			release2, err := owner.AcquireDistributedLock(ctx, cli, "test-lock", 10)
			require.NoError(t, err)
			getLock <- struct{}{}
			release2()
		})
		timer := time.NewTimer(300 * time.Millisecond)
		select {
		case <-getLock:
			require.FailNow(t, "acquired same lock unexpectedly")
		case <-timer.C:
			release1()
			<-getLock
		}
		wg.Wait()

		release1, err = owner.AcquireDistributedLock(ctx, cli, "test-lock/1", 10)
		require.NoError(t, err)
		release2, err := owner.AcquireDistributedLock(ctx, cli, "test-lock/2", 10)
		require.NoError(t, err)
		release1()
		release2()
	})

	t.Run("acquire distributed lock with different clients", func(t *testing.T) {
		cli1 := makeEtcdCli(t)
		cli2 := makeEtcdCli(t)

		getLock := make(chan struct{})
		ctx := context.Background()

		release1, err := owner.AcquireDistributedLock(ctx, cli1, "test-lock", 10)
		require.NoError(t, err)
		var wg util.WaitGroupWrapper
		wg.Run(func() {
			// Acquire another distributed lock will be blocked.
			release2, err := owner.AcquireDistributedLock(ctx, cli2, "test-lock", 10)
			require.NoError(t, err)
			getLock <- struct{}{}
			release2()
		})
		timer := time.NewTimer(300 * time.Millisecond)
		select {
		case <-getLock:
			require.FailNow(t, "acquired same lock unexpectedly")
		case <-timer.C:
			release1()
			<-getLock
		}
		wg.Wait()
	})

	t.Run("acquire distributed lock until timeout", func(t *testing.T) {
		cli1 := makeEtcdCli(t)
		cli2 := makeEtcdCli(t)
		ctx := context.Background()

		_, err := owner.AcquireDistributedLock(ctx, cli1, "test-lock", 1)
		require.NoError(t, err)
		cli1.Close() // Note that release() is not invoked.

		release2, err := owner.AcquireDistributedLock(ctx, cli2, "test-lock", 10)
		require.NoError(t, err)
		release2()
	})
}

func TestListenersWrapper(t *testing.T) {
	lis1 := &listener{}
	lis2 := &listener{}
	wrapper := owner.NewListenersWrapper(lis1, lis2)

	// Test OnBecomeOwner
	wrapper.OnBecomeOwner()
	require.True(t, lis1.val.Load())
	require.True(t, lis2.val.Load())

	// Test OnRetireOwner
	wrapper.OnRetireOwner()
	require.False(t, lis1.val.Load())
	require.False(t, lis2.val.Load())
}
