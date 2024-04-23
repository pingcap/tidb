// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package backup

import (
	"context"
	"sync"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/br/pkg/checkpoint"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/rtree"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/pkg/util/redact"
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
	pr *rtree.ProgressRange,
	stores []*metapb.Store,
	checkpointRunner *checkpoint.CheckpointRunner[checkpoint.BackupKeyType, checkpoint.BackupValueType],
	progressCallBack func(ProgressUnit),
) error {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("pushDown.pushBackup", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

	// Push down backup tasks to all tikv instances.
	failpoint.Inject("noop-backup", func(_ failpoint.Value) {
		logutil.CL(ctx).Warn("skipping normal backup, jump to fine-grained backup, meow :3", logutil.Key("start-key", req.StartKey), logutil.Key("end-key", req.EndKey))
		failpoint.Return(nil)
	})

	wg := new(sync.WaitGroup)
	errContext := utils.NewErrorContext("pushBackup", 10)
	for _, s := range stores {
		store := s
		storeID := s.GetId()
		lctx := logutil.ContextWithField(ctx, zap.Uint64("store-id", storeID))
		if err := utils.CheckStoreLiveness(s); err != nil {
			logutil.CL(lctx).Warn("skip store", logutil.ShortError(err))
			continue
		}
		client, err := push.mgr.GetBackupClient(lctx, storeID)
		if err != nil {
			// BR should be able to backup even some of stores disconnected.
			// The regions managed by this store can be retried at fine-grained backup then.
			logutil.CL(lctx).Warn("fail to connect store, skipping", zap.Error(err))
			continue
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

	for {
		select {
		case respAndStore, ok := <-push.respCh:
			resp := respAndStore.GetResponse()
			store := respAndStore.GetStore()
			if !ok {
				// Finished.
				return nil
			}
			failpoint.Inject("backup-timeout-error", func(val failpoint.Value) {
				msg := val.(string)
				logutil.CL(ctx).Info("failpoint backup-timeout-error injected.", zap.String("msg", msg))
				resp.Error = &backuppb.Error{
					Msg: msg,
				}
			})
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
				msg := val.(string)
				logutil.CL(ctx).Debug("failpoint tikv-region-error injected.", zap.String("msg", msg))
				resp.Error = &backuppb.Error{
					// Msg: msg,
					Detail: &backuppb.Error_RegionError{
						RegionError: &errorpb.Error{
							Message: msg,
						},
					},
				}
			})
			if resp.GetError() == nil {
				// None error means range has been backuped successfully.
				if checkpointRunner != nil {
					if err := checkpoint.AppendForBackup(
						ctx,
						checkpointRunner,
						pr.GroupKey,
						resp.StartKey,
						resp.EndKey,
						resp.Files,
					); err != nil {
						// the error is only from flush operator
						return errors.Annotate(err, "failed to flush checkpoint")
					}
				}
				pr.Res.Put(
					resp.GetStartKey(), resp.GetEndKey(), resp.GetFiles())

				// Update progress
				progressCallBack(RegionUnit)
			} else {
				errPb := resp.GetError()
				res := errContext.HandleIgnorableError(errPb, store.GetId())
				switch res.Strategy {
				case utils.GiveUpStrategy:
					errMsg := res.Reason
					if len(errMsg) <= 0 {
						errMsg = errPb.Msg
					}
					return errors.Annotatef(berrors.ErrKVStorage, "error happen in store %v at %s: %s",
						store.GetId(),
						redact.Value(store.GetAddress()),
						errMsg,
					)
				default:
					// other type just continue for next response
					// and finally handle the range in fineGrainedBackup
					continue
				}
			}
		case err := <-push.errCh:
			if !berrors.Is(err, berrors.ErrFailedToConnect) {
				return errors.Annotatef(err, "failed to backup range [%s, %s)", redact.Key(req.StartKey), redact.Key(req.EndKey))
			}
			logutil.CL(ctx).Warn("skipping disconnected stores", logutil.ShortError(err))
			return nil
		}
	}
}
