// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package backup

import (
	"context"
	"io"
	"os"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/rtree"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/br/pkg/utils/storewatch"
	tidbutil "github.com/pingcap/tidb/pkg/util"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type BackupRetryPolicy struct {
	One uint64
	All bool
}

type BackupSender interface {
	SendAsync(
		ctx context.Context,
		round uint64,
		storeID uint64,
		request backuppb.BackupRequest,
		concurrency uint,
		cli backuppb.BackupClient,
		respCh chan *ResponseAndStore,
		StateNotifier chan BackupRetryPolicy)
}

type ResponseAndStore struct {
	Resp    *backuppb.BackupResponse
	StoreID uint64
}

func (r ResponseAndStore) GetResponse() *backuppb.BackupResponse {
	return r.Resp
}

func (r ResponseAndStore) GetStoreID() uint64 {
	return r.StoreID
}

// timeoutRecv cancel the context if `Refresh()` is not called within the specified time `timeout`.
type timeoutRecv struct {
	storeID   uint64
	wg        sync.WaitGroup
	parentCtx context.Context
	cancel    context.CancelCauseFunc

	refresh chan struct{}
}

// Refresh the timeout ticker
func (trecv *timeoutRecv) Refresh() {
	select {
	case <-trecv.parentCtx.Done():
	case trecv.refresh <- struct{}{}:
	}
}

// Stop the timeout ticker
func (trecv *timeoutRecv) Stop() {
	close(trecv.refresh)
	trecv.wg.Wait()
	trecv.cancel(nil)
}

var TimeoutOneResponse = time.Hour

func (trecv *timeoutRecv) loop(timeout time.Duration) {
	defer trecv.wg.Done()
	ticker := time.NewTicker(timeout)
	defer ticker.Stop()
	for {
		ticker.Reset(timeout)
		select {
		case <-trecv.parentCtx.Done():
			return
		case _, ok := <-trecv.refresh:
			if !ok {
				return
			}
		case <-ticker.C:
			log.Warn("wait backup response timeout, cancel the backup",
				zap.Duration("timeout", timeout), zap.Uint64("storeID", trecv.storeID))
			trecv.cancel(errors.Errorf("receive a backup response timeout"))
		}
	}
}

func StartTimeoutRecv(ctx context.Context, timeout time.Duration, storeID uint64) (context.Context, *timeoutRecv) {
	cctx, cancel := context.WithCancelCause(ctx)
	trecv := &timeoutRecv{
		storeID:   storeID,
		parentCtx: ctx,
		cancel:    cancel,
		refresh:   make(chan struct{}),
	}
	trecv.wg.Add(1)
	go trecv.loop(timeout)
	return cctx, trecv
}

func doSendBackup(
	ctx context.Context,
	client backuppb.BackupClient,
	req backuppb.BackupRequest,
	respFn func(*backuppb.BackupResponse) error,
) error {
	failpoint.Inject("hint-backup-start", func(v failpoint.Value) {
		logutil.CL(ctx).Info("failpoint hint-backup-start injected, " +
			"process will notify the shell.")
		if sigFile, ok := v.(string); ok {
			file, err := os.Create(sigFile)
			if err != nil {
				log.Warn("failed to create file for notifying, skipping notify", zap.Error(err))
			}
			if file != nil {
				file.Close()
			}
		}
		time.Sleep(3 * time.Second)
	})
	bCli, err := client.Backup(ctx, &req)
	failpoint.Inject("reset-retryable-error", func(val failpoint.Value) {
		switch val.(string) {
		case "Unavailable":
			{
				logutil.CL(ctx).Debug("failpoint reset-retryable-error unavailable injected.")
				err = status.Error(codes.Unavailable, "Unavailable error")
			}
		case "Internal":
			{
				logutil.CL(ctx).Debug("failpoint reset-retryable-error internal injected.")
				err = status.Error(codes.Internal, "Internal error")
			}
		}
	})
	failpoint.Inject("reset-not-retryable-error", func(val failpoint.Value) {
		if val.(bool) {
			logutil.CL(ctx).Debug("failpoint reset-not-retryable-error injected.")
			err = status.Error(codes.Unknown, "Your server was haunted hence doesn't work, meow :3")
		}
	})
	if err != nil {
		return err
	}
	defer func() {
		_ = bCli.CloseSend()
	}()

	for {
		resp, err := bCli.Recv()
		if err != nil {
			if errors.Cause(err) == io.EOF { // nolint:errorlint
				logutil.CL(ctx).Debug("backup streaming finish",
					logutil.Key("backup-start-key", req.GetStartKey()),
					logutil.Key("backup-end-key", req.GetEndKey()))
				return nil
			}
			return err
		}
		// TODO: handle errors in the resp.
		logutil.CL(ctx).Debug("range backed up",
			logutil.Key("small-range-start-key", resp.GetStartKey()),
			logutil.Key("small-range-end-key", resp.GetEndKey()),
			zap.Int("api-version", int(resp.ApiVersion)))
		err = respFn(resp)
		if err != nil {
			return errors.Trace(err)
		}
	}
}

func startBackup(
	pctx context.Context,
	storeID uint64,
	backupReq backuppb.BackupRequest,
	backupCli backuppb.BackupClient,
	concurrency uint,
	respCh chan *ResponseAndStore,
) error {
	// this goroutine handle the response from a single store
	select {
	case <-pctx.Done():
		return pctx.Err()
	default:
		// Send backup request to the store.
		// handle the backup response or internal error here.
		// handle the store error(reboot or network partition) outside.
		reqs := SplitBackupReqRanges(backupReq, concurrency)
		logutil.CL(pctx).Info("starting backup to the corresponding store", zap.Uint64("storeID", storeID),
			zap.Int("requestCount", len(reqs)), zap.Uint("concurrency", concurrency))

		// Backup might be stuck on GRPC `waitonHeader`, so start a timeout ticker to
		// terminate the backup if it does not receive any new response for a long time.
		ctx, timerecv := StartTimeoutRecv(pctx, TimeoutOneResponse, storeID)
		defer timerecv.Stop()

		pool := tidbutil.NewWorkerPool(concurrency, "store_backup")
		eg, ectx := errgroup.WithContext(ctx)
		for i, req := range reqs {
			bkReq := req
			reqIndex := i
			pool.ApplyOnErrorGroup(eg, func() error {
				retry := -1
				return utils.WithRetry(ectx, func() error {
					retry += 1
					if retry > 1 {
						logutil.CL(ectx).Info("retry backup to store", zap.Uint64("storeID", storeID),
							zap.Int("retry", retry), zap.Int("reqIndex", reqIndex))
					}
					return doSendBackup(ectx, backupCli, bkReq, func(resp *backuppb.BackupResponse) error {
						// Forward all responses (including error).
						failpoint.Inject("backup-timeout-error", func(val failpoint.Value) {
							msg := val.(string)
							logutil.CL(ectx).Info("failpoint backup-timeout-error injected.", zap.String("msg", msg))
							resp.Error = &backuppb.Error{
								Msg: msg,
							}
						})
						failpoint.Inject("backup-storage-error", func(val failpoint.Value) {
							msg := val.(string)
							logutil.CL(ectx).Debug("failpoint backup-storage-error injected.", zap.String("msg", msg))
							resp.Error = &backuppb.Error{
								Msg: msg,
							}
						})
						failpoint.Inject("tikv-rw-error", func(val failpoint.Value) {
							msg := val.(string)
							logutil.CL(ectx).Debug("failpoint tikv-rw-error injected.", zap.String("msg", msg))
							resp.Error = &backuppb.Error{
								Msg: msg,
							}
						})
						failpoint.Inject("tikv-region-error", func(val failpoint.Value) {
							msg := val.(string)
							logutil.CL(ectx).Debug("failpoint tikv-region-error injected.", zap.String("msg", msg))
							resp.Error = &backuppb.Error{
								// Msg: msg,
								Detail: &backuppb.Error_RegionError{
									RegionError: &errorpb.Error{
										Message: msg,
									},
								},
							}
						})
						select {
						case <-ectx.Done():
							return ectx.Err()
						case respCh <- &ResponseAndStore{
							Resp:    resp,
							StoreID: storeID,
						}:
							// reset timeout when receive a response
							timerecv.Refresh()
						}
						return nil
					})
				}, utils.NewBackupSSTBackoffer())
			})
		}
		return eg.Wait()
	}
}

func getBackupRanges(ranges []rtree.Range) []*kvrpcpb.KeyRange {
	requestRanges := make([]*kvrpcpb.KeyRange, 0, len(ranges))
	for _, r := range ranges {
		requestRanges = append(requestRanges, &kvrpcpb.KeyRange{
			StartKey: r.StartKey,
			EndKey:   r.EndKey,
		})
	}
	return requestRanges
}

func ObserveStoreChangesAsync(ctx context.Context, stateNotifier chan BackupRetryPolicy, pdCli pd.Client) {
	go func() {
		sendAll := false
		newJoinStoresMap := make(map[uint64]struct{})
		cb := storewatch.MakeCallback(storewatch.WithOnReboot(func(s *metapb.Store) {
			sendAll = true
		}), storewatch.WithOnDisconnect(func(s *metapb.Store) {
			sendAll = true
		}), storewatch.WithOnNewStoreRegistered(func(s *metapb.Store) {
			// only backup for this store
			newJoinStoresMap[s.Id] = struct{}{}
		}))

		notifyFn := func(ctx context.Context, sendPolicy BackupRetryPolicy) {
			select {
			case <-ctx.Done():
			case stateNotifier <- sendPolicy:
			}
		}

		watcher := storewatch.New(pdCli, cb)
		// make a first step, and make the state correct for next 30s check
		err := watcher.Step(ctx)
		if err != nil {
			logutil.CL(ctx).Warn("failed to watch store changes at beginning, ignore it", zap.Error(err))
		}
		tickInterval := 30 * time.Second
		failpoint.Inject("backup-store-change-tick", func(val failpoint.Value) {
			if val.(bool) {
				tickInterval = 100 * time.Millisecond
			}
			logutil.CL(ctx).Info("failpoint backup-store-change-tick injected.", zap.Duration("interval", tickInterval))
		})
		tick := time.NewTicker(tickInterval)
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				// reset the state
				sendAll = false
				clear(newJoinStoresMap)
				logutil.CL(ctx).Info("check store changes every 30s")
				err := watcher.Step(ctx)
				if err != nil {
					logutil.CL(ctx).Warn("failed to watch store changes, ignore it", zap.Error(err))
				}
				if sendAll {
					logutil.CL(ctx).Info("detect some store(s) restarted or disconnected, notify with all stores")
					notifyFn(ctx, BackupRetryPolicy{All: true})
				} else if len(newJoinStoresMap) > 0 {
					for storeID := range newJoinStoresMap {
						logutil.CL(ctx).Info("detect a new registered store, notify with this store", zap.Uint64("storeID", storeID))
						notifyFn(ctx, BackupRetryPolicy{One: storeID})
					}
				}
			}
		}
	}()
}

func SplitBackupReqRanges(req backuppb.BackupRequest, count uint) []backuppb.BackupRequest {
	rangeCount := len(req.SubRanges)
	if rangeCount == 0 {
		return []backuppb.BackupRequest{req}
	}
	splitRequests := make([]backuppb.BackupRequest, 0, count)
	if count <= 1 {
		// 0/1 means no need to split, just send one batch request
		return []backuppb.BackupRequest{req}
	}
	splitStep := rangeCount / int(count)
	if splitStep == 0 {
		// splitStep should be at least 1
		// if count >= rangeCount, means no batch, split them all
		splitStep = 1
	}
	subRanges := req.SubRanges
	for i := 0; i < rangeCount; i += splitStep {
		splitReq := req
		splitReq.SubRanges = subRanges[i:min(i+splitStep, rangeCount)]
		splitRequests = append(splitRequests, splitReq)
	}
	return splitRequests
}
