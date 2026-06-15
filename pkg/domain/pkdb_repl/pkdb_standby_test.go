package pkdbrepl

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/client/constants"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/tests/v3/integration"
)

func checkChanClosed(t *testing.T, ch chan struct{}) {
	select {
	case <-ch:
		// expected
	case <-time.After(500 * time.Millisecond):
		t.Fatal("channel not closed in time")
	}
}

func checkChanNotClosed(t *testing.T, ch chan struct{}) {
	select {
	case <-ch:
		t.Fatal("channel should not be closed yet")
	case <-time.After(500 * time.Millisecond):
		// expected
	}
}

func resetStandbyStateForTest() {
	watchRevision = 0
	disableStandbyModeInternal()
}

func mustPutStandbyEtcdValue(t *testing.T, cli *clientv3.Client, value string) {
	ctx := context.Background()
	_, err := cli.Put(ctx, constants.PkdbEtcdStandbyModeKey, value)
	require.NoError(t, err)
}

func TestStandbyBlockingCh(t *testing.T) {
	t.Run("init at non-standby mode", func(t *testing.T) {
		integration.BeforeTestExternal(t)
		cluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
		defer cluster.Terminate(t)
		etcdCli := cluster.RandClient()
		resetStandbyStateForTest()
		defer resetStandbyStateForTest()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		InitStandby(ctx, etcdCli)
		go WatchStandby(ctx, etcdCli)

		require.False(t, IsStandbyMode())

		mustPutStandbyEtcdValue(t, etcdCli, "1")
		require.Eventually(t, func() bool {
			return IsStandbyMode()
		}, 5*time.Second, 100*time.Millisecond)

		// a caller blocks on the channel
		doneCh := make(chan struct{})
		go func() {
			CheckStandbyBlocking(context.Background())
			close(doneCh)
		}()

		checkChanNotClosed(t, doneCh)
		mustPutStandbyEtcdValue(t, etcdCli, "0")
		checkChanClosed(t, doneCh)
	})

	t.Run("init at standby mode", func(t *testing.T) {
		integration.BeforeTestExternal(t)
		cluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
		defer cluster.Terminate(t)
		etcdCli := cluster.RandClient()
		resetStandbyStateForTest()
		defer resetStandbyStateForTest()

		ctx := context.Background()
		mustPutStandbyEtcdValue(t, etcdCli, "1")
		InitStandby(ctx, etcdCli)
		// InitStandby should init standby mode before it returns
		require.True(t, IsStandbyMode())
		doneCh := make(chan struct{})
		go func() {
			CheckStandbyBlocking(context.Background())
			close(doneCh)
		}()
		checkChanNotClosed(t, doneCh)

		go WatchStandby(ctx, etcdCli)

		mustPutStandbyEtcdValue(t, etcdCli, "0")
		checkChanClosed(t, doneCh)
	})
}

func TestStandbyBlockingContextCanceled(t *testing.T) {
	t.Cleanup(resetStandbyStateForTest)
	resetStandbyStateForTest()

	enableStandbyMode()

	ctx, cancel := context.WithCancel(context.Background())
	doneCh := make(chan struct{})
	go func() {
		CheckStandbyBlocking(ctx)
		close(doneCh)
	}()

	checkChanNotClosed(t, doneCh)
	cancel()
	checkChanClosed(t, doneCh)
}

func TestWatchStandbyCompactedBeforeStart(t *testing.T) {
	integration.BeforeTestExternal(t)
	cluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer cluster.Terminate(t)
	etcdCli := cluster.RandClient()
	resetStandbyStateForTest()
	defer resetStandbyStateForTest()

	oldRetryInterval := watchStandbyRetryInterval
	watchStandbyRetryInterval = 10 * time.Millisecond
	defer func() { watchStandbyRetryInterval = oldRetryInterval }()

	ctx := context.Background()
	putResp, err := etcdCli.Put(ctx, constants.PkdbEtcdStandbyModeKey, "1")
	require.NoError(t, err)
	staleRev := putResp.Header.Revision

	InitStandby(ctx, etcdCli)
	require.True(t, IsStandbyMode())

	// Bump revisions so that compaction will make the watch revision stale.
	for i := 0; i < 32; i++ {
		mustPutStandbyEtcdValue(t, etcdCli, "0")
	}
	getResp, err := etcdCli.Get(ctx, constants.PkdbEtcdStandbyModeKey)
	require.NoError(t, err)
	compactRev := getResp.Header.Revision
	require.Greater(t, compactRev, staleRev)
	_, err = etcdCli.Compact(ctx, compactRev)
	require.NoError(t, err)

	watchRevision = staleRev

	watchCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	doneCh := make(chan struct{})
	go func() {
		WatchStandby(watchCtx, etcdCli)
		close(doneCh)
	}()

	require.Eventually(t, func() bool {
		return watchRevision >= compactRev
	}, 2*time.Second, 50*time.Millisecond)

	// wait the "0" is processed
	require.Eventually(t, func() bool {
		return !IsStandbyMode()
	}, 2*time.Second, 50*time.Millisecond)

	cancel()
	<-doneCh
	require.False(t, IsStandbyMode())
}

func TestWatchStandbyCompactedDuringRunning(t *testing.T) {
	if !intest.InTest {
		t.Skip("requires --tags=intest")
	}

	integration.BeforeTestExternal(t)
	cluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer cluster.Terminate(t)
	etcdCli := cluster.RandClient()
	resetStandbyStateForTest()
	defer resetStandbyStateForTest()

	oldRetryInterval := watchStandbyRetryInterval
	watchStandbyRetryInterval = 10 * time.Millisecond
	defer func() { watchStandbyRetryInterval = oldRetryInterval }()

	ctx := context.Background()
	putResp, err := etcdCli.Put(ctx, constants.PkdbEtcdStandbyModeKey, "1")
	require.NoError(t, err)
	staleRev := putResp.Header.Revision
	InitStandby(ctx, etcdCli)
	require.True(t, IsStandbyMode())

	// Create enough history to keep the watcher behind.
	for i := 0; i < 1024; i++ {
		mustPutStandbyEtcdValue(t, etcdCli, "1")
	}
	// the latest version is "0", should not miss it
	mustPutStandbyEtcdValue(t, etcdCli, "0")
	getResp, err := etcdCli.Get(ctx, constants.PkdbEtcdStandbyModeKey)
	require.NoError(t, err)
	compactRev := getResp.Header.Revision
	require.Greater(t, compactRev, staleRev)

	// Start the watch from a stale revision so it needs to catch up, then compact after the watch is running.
	watchRevision = staleRev

	createdCh := make(chan struct{})
	allowProceedCh := make(chan struct{})
	var once sync.Once
	watchStandbyAfterCreateWatch = func() {
		once.Do(func() {
			close(createdCh)
			<-allowProceedCh
		})
	}
	defer func() { watchStandbyAfterCreateWatch = nil }()

	watchCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	doneCh := make(chan struct{})
	go func() {
		WatchStandby(watchCtx, etcdCli)
		close(doneCh)
	}()

	<-createdCh
	_, err = etcdCli.Compact(ctx, compactRev)
	require.NoError(t, err)
	close(allowProceedCh)

	require.Eventually(t, func() bool {
		return watchRevision >= compactRev
	}, 2*time.Second, 50*time.Millisecond)

	// The watcher should not miss the latest version after it renews from compaction.
	require.Eventually(t, func() bool {
		return !IsStandbyMode()
	}, 2*time.Second, 50*time.Millisecond)

	cancel()
	<-doneCh
}

func TestWatchStandbyCompactedAfterSwitchEvent(t *testing.T) {
	integration.BeforeTestExternal(t)
	cluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer cluster.Terminate(t)
	etcdCli := cluster.RandClient()
	resetStandbyStateForTest()
	defer resetStandbyStateForTest()

	oldRetryInterval := watchStandbyRetryInterval
	watchStandbyRetryInterval = 10 * time.Millisecond
	defer func() { watchStandbyRetryInterval = oldRetryInterval }()

	ctx := context.Background()
	mustPutStandbyEtcdValue(t, etcdCli, "1")
	InitStandby(ctx, etcdCli)
	require.True(t, IsStandbyMode())

	// Switch to "0" once, then compact to a revision after that PUT so the event is not watchable anymore.
	putResp, err := etcdCli.Put(ctx, constants.PkdbEtcdStandbyModeKey, "0")
	require.NoError(t, err)
	switchRev := putResp.Header.Revision

	dummyKey := "/pkdbrepl/watch_standby_dummy"
	for i := 0; i < 32; i++ {
		_, err := etcdCli.Put(ctx, dummyKey, strconv.Itoa(i))
		require.NoError(t, err)
	}
	getResp, err := etcdCli.Get(ctx, dummyKey)
	require.NoError(t, err)
	compactRev := getResp.Header.Revision
	require.Greater(t, compactRev, switchRev)
	_, err = etcdCli.Compact(ctx, compactRev)
	require.NoError(t, err)

	watchRevision = switchRev - 1

	watchCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	doneCh := make(chan struct{})
	go func() {
		WatchStandby(watchCtx, etcdCli)
		close(doneCh)
	}()

	// WatchStandby should observe the snapshot value even if the switch event is compacted away.
	require.Eventually(t, func() bool {
		return !IsStandbyMode()
	}, 2*time.Second, 10*time.Millisecond)

	cancel()
	<-doneCh
}

func TestDupEnableDisable(t *testing.T) {
	t.Cleanup(disableStandbyMode)
	// just check won't panic
	enableStandbyMode()
	enableStandbyMode()
	disableStandbyMode()
	disableStandbyMode()
	enableStandbyMode()
	disableStandbyMode()
	enableStandbyMode()
	disableStandbyMode()
	disableStandbyMode()
	enableStandbyMode()
	enableStandbyMode()
}

func TestDupEnableLeaveBlocker(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		enableStandbyMode()
		done := make(chan struct{})
		go func() {
			CheckStandbyBlocking(context.Background())
			close(done)
		}()
		synctest.Wait()
		enableStandbyMode()
		disableStandbyMode()
		<-done
	})
}
