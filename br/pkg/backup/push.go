// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package backup

import (
	"context"
	"fmt"
	"sync"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/redact"
	"github.com/pingcap/tidb/br/pkg/rtree"
	"github.com/pingcap/tidb/br/pkg/utils"
	"go.uber.org/zap"
)

// pushDown wraps a backup task.
type pushDown struct {
	mgr    ClientMgr
	respCh chan responseAndStore
	errCh  chan error
}

type responseAndStore struct {
	Resp  *backuppb.BackupResponse
	Store *metapb.Store
}

func (r responseAndStore) GetResponse() *backuppb.BackupResponse {
	return r.Resp
}

func (r responseAndStore) GetStore() *metapb.Store {
	return r.Store
}

// newPushDown creates a push down backup.
func newPushDown(mgr ClientMgr, capacity int) *pushDown {
	return &pushDown{
		mgr:    mgr,
		respCh: make(chan responseAndStore, capacity),
		errCh:  make(chan error, capacity),
	}
}

// FullBackup make a full backup of a tikv cluster.
func (push *pushDown) pushBackup(
	ctx context.Context,
	req backuppb.BackupRequest,
	stores []*metapb.Store,
	progressCallBack func(ProgressUnit),
) (rtree.RangeTree, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("pushDown.pushBackup", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

	// Push down backup tasks to all tikv instances.
	res := rtree.NewRangeTree()
	failpoint.Inject("noop-backup", func(_ failpoint.Value) {
		logutil.CL(ctx).Warn("skipping normal backup, jump to fine-grained backup, meow :3", logutil.Key("start-key", req.StartKey), logutil.Key("end-key", req.EndKey))
		failpoint.Return(res, nil)
	})

	wg := new(sync.WaitGroup)
	for _, s := range stores {
		store := s
		storeID := s.GetId()
		lctx := logutil.ContextWithField(ctx, zap.Uint64("store-id", storeID))
		if s.GetState() != metapb.StoreState_Up {
			logutil.CL(lctx).Warn("skip store", zap.Stringer("State", s.GetState()))
			continue
		}
		client, err := push.mgr.GetBackupClient(lctx, storeID)
		if err != nil {
			// BR should be able to backup even some of stores disconnected.
			// The regions managed by this store can be retried at fine-grained backup then.
			logutil.CL(lctx).Warn("fail to connect store, skipping", zap.Error(err))
			return res, nil
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := SendBackup(
				lctx, storeID, client, req,
				func(resp *backuppb.BackupResponse) error {
					// Forward all responses (including error).
					push.respCh <- responseAndStore{
						Resp:  resp,
						Store: store,
					}
					return nil
				},
				func() (backuppb.BackupClient, error) {
					logutil.CL(lctx).Warn("reset the connection in push")
					return push.mgr.ResetBackupClient(lctx, storeID)
				})
			// Disconnected stores can be ignored.
			if err != nil {
				push.errCh <- err
				return
			}
		}()
	}

	go func() {
		wg.Wait()
		// TODO: test concurrent receive response and close channel.
		close(push.respCh)
	}()

	regionErrorIngestedOnce := false
	for {
		select {
		case respAndStore, ok := <-push.respCh:
			resp := respAndStore.GetResponse()
			store := respAndStore.GetStore()
			if !ok {
				// Finished.
				return res, nil
			}
			failpoint.Inject("backup-storage-error", func(val failpoint.Value) {
				msg := val.(string)
				logutil.CL(ctx).Debug("failpoint backup-storage-error injected.", zap.String("msg", msg))
				resp.Error = &backuppb.Error{
					Msg: msg,
				}
			})
			failpoint.Inject("tikv-rw-error", func(val failpoint.Value) {
				msg := val.(string)
				logutil.CL(ctx).Debug("failpoint tikv-rw-error injected.", zap.String("msg", msg))
				resp.Error = &backuppb.Error{
					Msg: msg,
				}
			})
			failpoint.Inject("tikv-region-error", func(val failpoint.Value) {
				if !regionErrorIngestedOnce {
					msg := val.(string)
					logutil.CL(ctx).Debug("failpoint tikv-regionh-error injected.", zap.String("msg", msg))
					resp.Error = &backuppb.Error{
						// Msg: msg,
						Detail: &backuppb.Error_RegionError{
							RegionError: &errorpb.Error{
								Message: msg,
							},
						},
					}
				}
				regionErrorIngestedOnce = true
			})
			if resp.GetError() == nil {
				// None error means range has been backuped successfully.
				res.Put(
					resp.GetStartKey(), resp.GetEndKey(), resp.GetFiles())

				// Update progress
				progressCallBack(RegionUnit)
			} else {
				errPb := resp.GetError()
				switch v := errPb.Detail.(type) {
				case *backuppb.Error_KvError:
					logutil.CL(ctx).Warn("backup occur kv error", zap.Reflect("error", v))

				case *backuppb.Error_RegionError:
					logutil.CL(ctx).Warn("backup occur region error", zap.Reflect("error", v))

				case *backuppb.Error_ClusterIdError:
					logutil.CL(ctx).Error("backup occur cluster ID error", zap.Reflect("error", v))
					return res, errors.Annotatef(berrors.ErrKVClusterIDMismatch, "%v", errPb)
				default:
					if utils.MessageIsRetryableStorageError(errPb.GetMsg()) {
						logutil.CL(ctx).Warn("backup occur storage error", zap.String("error", errPb.GetMsg()))
						continue
					}
					var errMsg string
					if utils.MessageIsNotFoundStorageError(errPb.GetMsg()) {
						errMsg = fmt.Sprintf("File or directory not found on TiKV Node (store id: %v; Address: %s). "+
							"work around:please ensure br and tikv nodes share a same storage and the user of br and tikv has same uid.",
							store.GetId(), redact.String(store.GetAddress()))
						logutil.CL(ctx).Error("", zap.String("error", berrors.ErrKVStorage.Error()+": "+errMsg))
					}
					if utils.MessageIsPermissionDeniedStorageError(errPb.GetMsg()) {
						errMsg = fmt.Sprintf("I/O permission denied error occurs on TiKV Node(store id: %v; Address: %s). "+
							"work around:please ensure tikv has permission to read from & write to the storage.",
							store.GetId(), redact.String(store.GetAddress()))
						logutil.CL(ctx).Error("", zap.String("error", berrors.ErrKVStorage.Error()+": "+errMsg))
					}

					if len(errMsg) <= 0 {
						errMsg = errPb.Msg
					}
					return res, errors.Annotatef(berrors.ErrKVStorage, "error happen in store %v at %s: %s %s",
						store.GetId(),
						redact.String(store.GetAddress()),
						req.StorageBackend.String(),
						errMsg,
					)
				}
			}
		case err := <-push.errCh:
			if !berrors.Is(err, berrors.ErrFailedToConnect) {
				return res, errors.Annotatef(err, "failed to backup range [%s, %s)", redact.Key(req.StartKey), redact.Key(req.EndKey))
			}
			logutil.CL(ctx).Warn("skipping disconnected stores", logutil.ShortError(err))
			return res, nil
		}
	}
}
