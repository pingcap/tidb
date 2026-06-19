package pkdbrepl

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// Must match github.com/tikv/pd/client/constants.PkdbSourceWriteGateKey.
const pkdbSourceWriteGateKey = "/tidb/pkdb/log_repl/source_write_gate"

var (
	sourceWriteGateBlockingCh         atomic.Pointer[chan struct{}]
	sourceWriteGateWatchRevision      int64
	sourceWriteGateWatchRetryInterval = 5 * time.Second
	sourceWriteGateAfterCreateWatch   func()
)

// CheckSourceWriteGateBlocking blocks user writes while PD is recovering
// source-side log-replication learners.
func CheckSourceWriteGateBlocking(ctx context.Context) {
	ch := sourceWriteGateBlockingCh.Load()
	if ch == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-*ch:
	case <-ctx.Done():
	}
}

// IsSourceWriteGateBlocked returns whether source user writes are currently
// gated by PD.
func IsSourceWriteGateBlocked() bool {
	return sourceWriteGateBlockingCh.Load() != nil
}

// SetSourceWriteGateForTest toggles the in-memory source write gate for tests.
func SetSourceWriteGateForTest(enabled bool) {
	if enabled {
		enableSourceWriteGate()
		return
	}
	disableSourceWriteGateInternal()
}

func enableSourceWriteGate() {
	ch := make(chan struct{})
	if sourceWriteGateBlockingCh.CompareAndSwap(nil, &ch) {
		logutil.BgLogger().Info("source write gate is enabled")
	}
}

func disableSourceWriteGate() {
	disableSourceWriteGateInternal()
}

func disableSourceWriteGateInternal() {
	old := sourceWriteGateBlockingCh.Swap(nil)
	if old == nil {
		return
	}
	close(*old)
	logutil.BgLogger().Info("source write gate is disabled")
}

// InitSourceWriteGate initializes the source write gate from etcd before TiDB
// starts serving user traffic.
func InitSourceWriteGate(ctx context.Context, etcdCli *clientv3.Client) {
	if etcdCli == nil {
		return
	}

	for {
		if err := syncSourceWriteGateStateFromEtcd(ctx, etcdCli); err == nil {
			return
		} else {
			logutil.BgLogger().Warn("failed to get source write gate from etcd", zap.Error(err))
		}
		sleep(ctx.Done(), time.Second)
		if ctx.Err() != nil {
			return
		}
	}
}

// WatchSourceWriteGate watches PD's source write gate.
func WatchSourceWriteGate(ctx context.Context, etcdCli *clientv3.Client) {
	defer disableSourceWriteGateInternal()

	if etcdCli == nil {
		return
	}

	watchCh := etcdCli.Watch(ctx, pkdbSourceWriteGateKey, clientv3.WithRev(sourceWriteGateWatchRevision+1))
	if intest.InTest && sourceWriteGateAfterCreateWatch != nil {
		sourceWriteGateAfterCreateWatch()
	}
	for {
		select {
		case <-ctx.Done():
			return
		case watchResp, ok := <-watchCh:
			if !ok || watchResp.Canceled {
				logutil.BgLogger().Info("source write gate watch chan interrupted, will renew it")
				sleep(ctx.Done(), sourceWriteGateWatchRetryInterval)
				for {
					if err := syncSourceWriteGateStateFromEtcd(ctx, etcdCli); err == nil {
						break
					} else {
						logutil.BgLogger().Warn("failed to get source write gate from etcd, will retry", zap.Error(err))
					}
					sleep(ctx.Done(), sourceWriteGateWatchRetryInterval)
					if ctx.Err() != nil {
						return
					}
				}
				watchCh = etcdCli.Watch(ctx, pkdbSourceWriteGateKey, clientv3.WithRev(sourceWriteGateWatchRevision+1))
				if intest.InTest && sourceWriteGateAfterCreateWatch != nil {
					sourceWriteGateAfterCreateWatch()
				}
				continue
			}
			if len(watchResp.Events) == 0 {
				continue
			}

			sourceWriteGateWatchRevision = watchResp.Header.Revision
			lastEvent := watchResp.Events[len(watchResp.Events)-1]
			handleLatestSourceWriteGateEvent(lastEvent)
		}
	}
}

func syncSourceWriteGateStateFromEtcd(ctx context.Context, etcdCli *clientv3.Client) error {
	childCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	resp, err := etcdCli.Get(childCtx, pkdbSourceWriteGateKey)
	cancel()
	if err != nil {
		return err
	}

	sourceWriteGateWatchRevision = resp.Header.Revision
	if len(resp.Kvs) == 0 {
		handleLatestSourceWriteGateEvent(&clientv3.Event{Type: mvccpb.DELETE})
		return nil
	}

	handleLatestSourceWriteGateEvent(&clientv3.Event{Type: mvccpb.PUT, Kv: resp.Kvs[0]})
	return nil
}

func handleLatestSourceWriteGateEvent(event *clientv3.Event) {
	if event.Type == mvccpb.PUT && string(event.Kv.Value) == "1" {
		enableSourceWriteGate()
		return
	}
	disableSourceWriteGate()
}
