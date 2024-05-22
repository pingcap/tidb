// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package backup

import (
	"context"
	"io"
	"os"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/rtree"
	"github.com/pingcap/tidb/br/pkg/utils"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

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

func startStoreBackup(
	ctx context.Context,
	storeID uint64,
	backupReq backuppb.BackupRequest,
	backupCli backuppb.BackupClient,
	respCh chan *ResponseAndStore,
) error {
	// this goroutine handle the response from a single store
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		retry := -1
		return utils.WithRetry(ctx, func() error {
			retry += 1
			logutil.CL(ctx).Info("try backup", zap.Uint64("storeID", storeID), zap.Int("retry time", retry))
			// Send backup request to the store.
			// handle the backup response or internal error here.
			// handle the store error(reboot or network partition) outside.
			return doSendBackup(ctx, backupCli, backupReq, func(resp *backuppb.BackupResponse) error {
				// Forward all responses (including error).
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
				select {
				case <-ctx.Done():
					return ctx.Err()
				case respCh <- &ResponseAndStore{
					Resp:    resp,
					StoreID: storeID,
				}:
				}
				return nil
			})
		}, utils.NewBackupSSTBackoffer())
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
