package pkdbrepl

import (
	"context"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/tikv/pd/client/constants"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

var (
	// watchRestartRetryInterval is used when the restart watch is interrupted and needs
	// to be renewed. It can be overridden in tests to speed up retry loops.
	watchRestartRetryInterval = 5 * time.Second
	// watchRestartAfterInitWatch is a test hook called after reading the initial snapshot,
	// but before creating the etcd watch. It's guarded by `intest.InTest`, so it won't run
	// in normal builds.
	watchRestartAfterInitWatch func()
	// watchRestartAfterCreateWatch is a test hook called after creating/renewing the etcd
	// watch. It's guarded by `intest.InTest`, so it won't run in normal builds.
	watchRestartAfterCreateWatch func()
)

var restartHoldState struct {
	mu      sync.Mutex
	ch      chan struct{}
	holders int
}

// HoldRestart sets a barrier that blocks restartProcess until ReleaseRestart is called.
func HoldRestart() {
	restartHoldState.mu.Lock()
	defer restartHoldState.mu.Unlock()

	if restartHoldState.holders == 0 {
		restartHoldState.ch = make(chan struct{})
	}
	restartHoldState.holders++
}

// ReleaseRestart releases the restart barrier, allowing a pending restart to proceed.
func ReleaseRestart() {
	failpoint.Inject("beforeReleaseRestart", func() {})

	restartHoldState.mu.Lock()
	defer restartHoldState.mu.Unlock()

	if restartHoldState.holders == 0 {
		return
	}
	restartHoldState.holders--
	if restartHoldState.holders == 0 {
		close(restartHoldState.ch)
		restartHoldState.ch = nil
	}
}

func waitRestartHold(stopCh <-chan struct{}) bool {
	restartHoldState.mu.Lock()
	ch := restartHoldState.ch
	restartHoldState.mu.Unlock()
	if ch == nil {
		return true
	}
	if stopCh == nil {
		<-ch
		return true
	}
	select {
	case <-stopCh:
		return false
	case <-ch:
		return true
	}
}

func restart() error {
	executablePath, err := os.Executable()
	if err != nil {
		return err
	}
	args := os.Args
	env := os.Environ()

	return syscall.Exec(executablePath, args, env)
}

func restartProcess(domain domainCloser) {
	logutil.BgLogger().Info("will restart process")
	// need to unblock CheckStandbyBlocking, so we can close ownerMgr properly
	disableStandbyMode()

	if domain != nil {
		domain.Close()
		// nil is only possible in tests
		if CloseDDLOwnerMgr != nil {
			CloseDDLOwnerMgr()
		}
		logutil.BgLogger().Info("domain closed before restart")
	}
	if intest.InTest {
		// don't want to restart in go unit test
		return
	}
	if err := restart(); err != nil {
		logutil.BgLogger().Fatal("failed to restart process", zap.Error(err))
	}
}

// CloseDDLOwnerMgr is set by pkg/ddl/owner_mgr.go.
var CloseDDLOwnerMgr func()

type restartKeySnapshot struct {
	// key modification revision
	modRevision int64
	// etcd global response revision when reading the key
	revision int64
}

func getRestartKeySnapshot(ctx context.Context, etcdCli *clientv3.Client) (restartKeySnapshot, error) {
	childCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	resp, err := etcdCli.Get(childCtx, constants.PkdbRestartTiDBKey)
	if err != nil {
		return restartKeySnapshot{}, err
	}

	snap := restartKeySnapshot{revision: resp.Header.Revision}
	if len(resp.Kvs) > 0 {
		snap.modRevision = resp.Kvs[0].ModRevision
	}
	return snap, nil
}

func getRestartKeySnapshotWithRetry(
	ctx context.Context,
	etcdCli *clientv3.Client,
	stopCh <-chan struct{},
) (snap restartKeySnapshot, shouldStop bool) {
	for {
		select {
		case <-stopCh:
			shouldStop = true
			return
		default:
		}
		var err error
		snap, err = getRestartKeySnapshot(ctx, etcdCli)
		if err == nil {
			return
		}
		logutil.BgLogger().Warn("failed to get restart key from etcd, will retry", zap.Error(err))
		sleep(stopCh, watchRestartRetryInterval)
	}
}

func createRestartWatch(ctx context.Context, etcdCli *clientv3.Client, nextRev int64) clientv3.WatchChan {
	watchCh := etcdCli.Watch(ctx, constants.PkdbRestartTiDBKey, clientv3.WithRev(nextRev))
	if intest.InTest && watchRestartAfterCreateWatch != nil {
		watchRestartAfterCreateWatch()
	}
	return watchCh
}

// WatchRestart watches the restart key in etcd and restarts the process when the key changes.
func WatchRestart(etcdCli *clientv3.Client, stopCh <-chan struct{}, domain domainCloser) {
	if etcdCli == nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Read a snapshot first to establish a baseline (including the global revision) so
	// we can detect missed restart requests after watch renewals.
	snapshot, shouldStop := getRestartKeySnapshotWithRetry(ctx, etcdCli, stopCh)
	if shouldStop {
		return
	}
	nextRev := snapshot.revision + 1

	if intest.InTest && watchRestartAfterInitWatch != nil {
		watchRestartAfterInitWatch()
	}

	watchCh := createRestartWatch(ctx, etcdCli, nextRev)
	for {
		select {
		case <-stopCh:
			return
		case watchResp, ok := <-watchCh:
			if !ok || watchResp.Canceled {
				logutil.BgLogger().Info("restart watch interrupted, will renew it", zap.Error(watchResp.Err()))
				sleep(stopCh, watchRestartRetryInterval)

				// Before renewing the watch, re-read the key snapshot. If the key changed while we
				// were not watching (e.g. disconnected and the event is compacted), restart anyway.
				newSnapshot, shouldStop := getRestartKeySnapshotWithRetry(ctx, etcdCli, stopCh)
				if shouldStop {
					return
				}
				if snapshot.modRevision != newSnapshot.modRevision {
					if !waitRestartHold(stopCh) {
						return
					}
					go restartProcess(domain)
					return
				}

				snapshot = newSnapshot
				nextRev = snapshot.revision + 1
				watchCh = createRestartWatch(ctx, etcdCli, nextRev)
				continue
			}
			nextRev = watchResp.Header.Revision + 1
			if len(watchResp.Events) == 0 {
				continue
			}
			// will call Domain.Close() inside restartProcess, but this goroutine is also
			// created by Domain, so we need to start another goroutine and return after
			// Domain.Close() is blocked
			if !waitRestartHold(stopCh) {
				return
			}
			go restartProcess(domain)
			return
		}
	}
}
