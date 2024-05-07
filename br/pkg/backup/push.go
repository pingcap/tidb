// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package backup

import (
	"context"

	"github.com/pingcap/failpoint"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/tidb/br/pkg/logutil"
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
	Resp    *backuppb.BackupResponse
	StoreID uint64
}

func (r responseAndStore) GetResponse() *backuppb.BackupResponse {
	return r.Resp
}

func (r responseAndStore) GetStoreID() uint64 {
	return r.StoreID
}

func startStoreBackup(
	ctx context.Context,
	storeID uint64,
	backupReq backuppb.BackupRequest,
	backupCli backuppb.BackupClient,
	respCh chan *responseAndStore,
) error {
	// this goroutine handle the response from a single store
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		retry := -1
		return utils.WithRetry(ctx, func() error {
			retry += 1
			logutil.CL(ctx).Info("try backup", zap.Int("retry time", retry))
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
				case respCh <- &responseAndStore{
					Resp:    resp,
					StoreID: storeID,
				}:
				}
				return nil
			})
		}, utils.NewBackupSSTBackoffer())
	}
}
