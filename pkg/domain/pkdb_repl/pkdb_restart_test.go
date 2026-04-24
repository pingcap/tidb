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

package pkdbrepl

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/client/constants"
	"go.etcd.io/etcd/tests/v3/integration"
)

type testRestartDomain struct {
	closeOnce sync.Once
	closedCh  chan struct{}
}

func (d *testRestartDomain) Close() {
	d.closeOnce.Do(func() { close(d.closedCh) })
}

func (*testRestartDomain) InitDistTaskLoop() error { return nil }

func TestWatchRestartCompactedMissedPut(t *testing.T) {
	if !intest.InTest {
		t.Skip("requires --tags=intest")
	}

	integration.BeforeTestExternal(t)
	cluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer cluster.Terminate(t)
	etcdCli := cluster.RandClient()

	_, err := etcdCli.Delete(context.Background(), constants.PkdbRestartTiDBKey)
	require.NoError(t, err)

	oldRetryInterval := watchRestartRetryInterval
	watchRestartRetryInterval = 10 * time.Millisecond
	defer func() { watchRestartRetryInterval = oldRetryInterval }()

	initCh := make(chan struct{})
	proceedCh := make(chan struct{})
	var proceedOnce sync.Once
	closeProceedCh := func() { proceedOnce.Do(func() { close(proceedCh) }) }

	oldHook := watchRestartAfterInitWatch
	watchRestartAfterInitWatch = func() {
		close(initCh)
		<-proceedCh
	}
	defer func() {
		watchRestartAfterInitWatch = oldHook
		closeProceedCh()
	}()

	domain := &testRestartDomain{closedCh: make(chan struct{})}
	stopCh := make(chan struct{})
	defer close(stopCh)

	doneCh := make(chan struct{})
	go func() {
		WatchRestart(etcdCli, stopCh, domain)
		close(doneCh)
	}()

	require.Eventually(t, func() bool {
		select {
		case <-initCh:
			return true
		default:
			return false
		}
	}, 2*time.Second, 10*time.Millisecond)

	putResp, err := etcdCli.Put(context.Background(), constants.PkdbRestartTiDBKey, "1")
	require.NoError(t, err)
	putRev := putResp.Header.Revision

	// Bump global revisions using a different key, then compact beyond the restart PUT revision.
	dummyKey := "/pkdbrepl/watch_restart_dummy"
	for i := 0; i < 32; i++ {
		_, err := etcdCli.Put(context.Background(), dummyKey, fmt.Sprintf("%d", i))
		require.NoError(t, err)
	}
	getResp, err := etcdCli.Get(context.Background(), dummyKey)
	require.NoError(t, err)
	compactRev := getResp.Header.Revision
	require.Greater(t, compactRev, putRev)
	_, err = etcdCli.Compact(context.Background(), compactRev)
	require.NoError(t, err)

	// Let WatchRestart create its watch after the event is compacted away.
	closeProceedCh()

	// restartProcess() should still call domain.Close() in test builds.
	require.Eventually(t, func() bool {
		select {
		case <-domain.closedCh:
			return true
		default:
			return false
		}
	}, 2*time.Second, 10*time.Millisecond)

	require.Eventually(t, func() bool {
		select {
		case <-doneCh:
			return true
		default:
			return false
		}
	}, 2*time.Second, 10*time.Millisecond)
}

func TestRestartHoldWaitsForAllHolders(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		HoldRestart()
		HoldRestart()
		t.Cleanup(func() {
			ReleaseRestart()
			ReleaseRestart()
		})

		waitDone := make(chan struct{})
		go func() {
			waitRestartHold(nil)
			close(waitDone)
		}()

		synctest.Wait()
		select {
		case <-waitDone:
			t.Fatal("restart hold released before all holders finished")
		default:
		}

		ReleaseRestart()

		synctest.Wait()
		select {
		case <-waitDone:
			t.Fatal("restart hold released after only one holder finished")
		default:
		}

		ReleaseRestart()
		synctest.Wait()
		<-waitDone
	})
}

func TestWaitRestartHoldCanBeCanceled(t *testing.T) {
	HoldRestart()
	defer ReleaseRestart()

	stopCh := make(chan struct{})
	waitDone := make(chan bool, 1)
	go func() {
		waitDone <- waitRestartHold(stopCh)
	}()

	close(stopCh)

	require.False(t, <-waitDone)
}
