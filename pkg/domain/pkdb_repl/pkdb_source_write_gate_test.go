package pkdbrepl

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/tests/v3/integration"
)

func resetSourceWriteGateForTest() {
	sourceWriteGateWatchRevision = 0
	disableSourceWriteGateInternal()
}

func mustPutSourceWriteGateValue(t *testing.T, cli *clientv3.Client, value string) {
	ctx := context.Background()
	_, err := cli.Put(ctx, pkdbSourceWriteGateKey, value)
	require.NoError(t, err)
}

func TestSourceWriteGateBlockingCh(t *testing.T) {
	t.Run("init at disabled source write gate", func(t *testing.T) {
		integration.BeforeTestExternal(t)
		cluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
		defer cluster.Terminate(t)
		etcdCli := cluster.RandClient()
		resetSourceWriteGateForTest()
		defer resetSourceWriteGateForTest()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		InitSourceWriteGate(ctx, etcdCli)
		go WatchSourceWriteGate(ctx, etcdCli)

		require.False(t, IsSourceWriteGateBlocked())

		mustPutSourceWriteGateValue(t, etcdCli, "1")
		require.Eventually(t, func() bool {
			return IsSourceWriteGateBlocked()
		}, 5*time.Second, 100*time.Millisecond)

		doneCh := make(chan struct{})
		go func() {
			CheckSourceWriteGateBlocking(context.Background())
			close(doneCh)
		}()

		checkChanNotClosed(t, doneCh)
		mustPutSourceWriteGateValue(t, etcdCli, "0")
		checkChanClosed(t, doneCh)
	})

	t.Run("init at enabled source write gate", func(t *testing.T) {
		integration.BeforeTestExternal(t)
		cluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
		defer cluster.Terminate(t)
		etcdCli := cluster.RandClient()
		resetSourceWriteGateForTest()
		defer resetSourceWriteGateForTest()

		ctx := context.Background()
		mustPutSourceWriteGateValue(t, etcdCli, "1")
		InitSourceWriteGate(ctx, etcdCli)
		require.True(t, IsSourceWriteGateBlocked())

		doneCh := make(chan struct{})
		go func() {
			CheckSourceWriteGateBlocking(context.Background())
			close(doneCh)
		}()
		checkChanNotClosed(t, doneCh)

		go WatchSourceWriteGate(ctx, etcdCli)

		mustPutSourceWriteGateValue(t, etcdCli, "0")
		checkChanClosed(t, doneCh)
	})
}

func TestSourceWriteGateContextCanceled(t *testing.T) {
	t.Cleanup(resetSourceWriteGateForTest)
	resetSourceWriteGateForTest()

	enableSourceWriteGate()

	ctx, cancel := context.WithCancel(context.Background())
	doneCh := make(chan struct{})
	go func() {
		CheckSourceWriteGateBlocking(ctx)
		close(doneCh)
	}()

	checkChanNotClosed(t, doneCh)
	cancel()
	checkChanClosed(t, doneCh)
}
