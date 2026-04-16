package pkdbrepl

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/tikv/pd/client/constants"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

var standbyBlockingCh atomic.Pointer[chan struct{}]

// CheckStandbyBlocking should be called when caller has a loop that always
// failed because TiKV is in standby mode. This function will block the caller
// until standby mode is disabled.
//
// The ctx is used to allow callers to exit gracefully (e.g. during shutdown)
// even if standby mode stays enabled.
func CheckStandbyBlocking(ctx context.Context) {
	ch := standbyBlockingCh.Load()
	if ch != nil {
		if ctx == nil {
			ctx = context.Background()
		}
		select {
		case <-*ch:
		case <-ctx.Done():
		}
	}
}

// IsStandbyMode returns whether the cluster is in standby mode.
func IsStandbyMode() bool {
	return standbyBlockingCh.Load() != nil
}

var (
	// StandbyInfoSchemaReloadTickCh will be sent after standby mode is enabled, to
	// trigger eager tick for infoschema reload.
	StandbyInfoSchemaReloadTickCh = make(chan struct{}, 1)
	// StandbyUnsafeDestroyTickCh will be sent after standby mode is enabled, to
	// trigger unsafe destroy range check.
	StandbyUnsafeDestroyTickCh = make(chan struct{}, 1)
	// StandbyUserReloadTickCh will be sent after standby mode is enabled, to trigger
	// user reload.
	StandbyUserReloadTickCh = make(chan struct{}, 1)
	standbyTickWg           sync.WaitGroup
	standbyTickDone         chan struct{}
)

// enableStandbyMode should not be concurrently called with disableStandbyMode.
func enableStandbyMode() {
	ch := make(chan struct{})
	ok := standbyBlockingCh.CompareAndSwap(nil, &ch)
	if !ok {
		// already enabled
		return
	}

	standbyTickWg.Add(1)
	standbyTickDone = make(chan struct{})
	go func() {
		defer standbyTickWg.Done()
		infoSchemaTicker := time.NewTicker(time.Second)
		defer infoSchemaTicker.Stop()
		unsafeDestroyTicker := time.NewTicker(50 * time.Second)
		defer unsafeDestroyTicker.Stop()
		userReloadTicker := time.NewTicker(10 * time.Second)

		for {
			select {
			case <-standbyTickDone:
				return
			case <-infoSchemaTicker.C:
				select {
				case StandbyInfoSchemaReloadTickCh <- struct{}{}:
				default:
				}
			case <-unsafeDestroyTicker.C:
				select {
				case StandbyUnsafeDestroyTickCh <- struct{}{}:
				default:
				}
			case <-userReloadTicker.C:
				select {
				case StandbyUserReloadTickCh <- struct{}{}:
				default:
				}
			}
		}
	}()
	logutil.BgLogger().Info("standby mode is enabled")
}

// disableStandbyMode should not be concurrently called with enableStandbyMode.
func disableStandbyMode() {
	disableStandbyModeInternal()
}

// disableStandbyModeInternal disables standby mode without logging. Only used
// when tidb-server is closing to avoid noisy logs and unblock goroutines.
func disableStandbyModeInternal() {
	old := standbyBlockingCh.Swap(nil)
	if old == nil {
		// already disabled
		return
	}

	close(*old)
	close(standbyTickDone)
	standbyTickWg.Wait()
	logutil.BgLogger().Info("standby mode is disabled")
}

// InitStandby will initialize standby mode and update a global variable for
// later usage of WatchStandby.
func InitStandby(ctx context.Context, etcdCli *clientv3.Client) {
	if etcdCli == nil {
		return
	}

	var (
		resp *clientv3.GetResponse
		err  error
	)
	for {
		resp, err = etcdCli.Get(ctx, constants.PkdbEtcdStandbyModeKey)
		if err == nil {
			break
		}
		logutil.BgLogger().Warn("failed to get standby mode from etcd", zap.Error(err))
		sleep(ctx.Done(), time.Second)
		if ctx.Err() != nil {
			return
		}
	}

	if len(resp.Kvs) > 0 && string(resp.Kvs[0].Value) == "1" {
		enableStandbyMode()
	}
	watchRevision = resp.Header.Revision
}

var watchRevision int64

var (
	// watchStandbyRetryInterval is used when standby watch is interrupted and needs
	// to be renewed. It can be overridden in tests to speed up retry loops.
	watchStandbyRetryInterval = 5 * time.Second
	// watchStandbyAfterCreateWatch is a test hook called after creating/renewing the
	// etcd watch. It's guarded by `intest.InTest`, so it won't run in normal builds.
	watchStandbyAfterCreateWatch func()
)

type domainCloser interface {
	Close()
	// InitDistTaskLoop is not used. Just add some domain methods to this interface
	// to avoid the implementation of domainCloser is not Domain.
	InitDistTaskLoop() error
}

// WatchStandby should be called after InitStandby.
func WatchStandby(ctx context.Context, etcdCli *clientv3.Client, domain domainCloser) {
	defer disableStandbyModeInternal()

	if etcdCli == nil {
		return
	}

	watchCh := etcdCli.Watch(ctx, constants.PkdbEtcdStandbyModeKey, clientv3.WithRev(watchRevision+1))
	if intest.InTest && watchStandbyAfterCreateWatch != nil {
		watchStandbyAfterCreateWatch()
	}
	for {
		select {
		case <-ctx.Done():
			return
		case watchResp, ok := <-watchCh:
			if !ok || watchResp.Canceled {
				logutil.BgLogger().Info("standby mode watch chan interrupted, will renew it")
				sleep(ctx.Done(), watchStandbyRetryInterval)
				for {
					shouldReturn, err := syncStandbyStateFromEtcd(ctx, etcdCli, domain)
					if err == nil {
						if shouldReturn {
							return
						}
						break
					}
					logutil.BgLogger().Warn("failed to get standby mode from etcd, will retry", zap.Error(err))
					sleep(ctx.Done(), watchStandbyRetryInterval)
					if ctx.Err() != nil {
						return
					}
				}
				watchCh = etcdCli.Watch(ctx, constants.PkdbEtcdStandbyModeKey, clientv3.WithRev(watchRevision+1))
				if intest.InTest && watchStandbyAfterCreateWatch != nil {
					watchStandbyAfterCreateWatch()
				}
				continue
			}
			if len(watchResp.Events) == 0 {
				continue
			}

			watchRevision = watchResp.Header.Revision
			lastEvent := watchResp.Events[len(watchResp.Events)-1]
			if handleLatestStandbyEvent(lastEvent, domain) {
				return
			}
		}
	}
}

func syncStandbyStateFromEtcd(ctx context.Context, etcdCli *clientv3.Client, domain domainCloser) (shouldReturn bool, err error) {
	childCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	resp, err := etcdCli.Get(childCtx, constants.PkdbEtcdStandbyModeKey)
	cancel()
	if err != nil {
		return false, err
	}

	watchRevision = resp.Header.Revision

	if len(resp.Kvs) == 0 {
		shouldReturn = handleLatestStandbyEvent(&clientv3.Event{Type: mvccpb.DELETE}, domain)
		return shouldReturn, nil
	}

	shouldReturn = handleLatestStandbyEvent(&clientv3.Event{Type: mvccpb.PUT, Kv: resp.Kvs[0]}, domain)
	return shouldReturn, nil
}

func handleLatestStandbyEvent(event *clientv3.Event, domain domainCloser) (shouldReturn bool) {
	if event.Type == mvccpb.PUT && string(event.Kv.Value) == "1" {
		if standbyBlockingCh.Load() == nil {
			enableStandbyMode()
		}
		return false
	}

	if standbyBlockingCh.Load() != nil {
		// will call Domain.Close() inside restartProcess, but this goroutine is also
		// created by Domain, so we need to start another goroutine and return after
		// Domain.Close() is blocked
		go restartProcess(domain)
		return true
	}
	return false
}

func sleep(exitCh <-chan struct{}, d time.Duration) {
	select {
	case <-exitCh:
	case <-time.After(d):
	}
}
