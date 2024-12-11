// Copyright 2021 PingCAP, Inc. Licensed under Apache-2.0.

package checksum

import (
	"context"
	"time"

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/summary"
	"go.uber.org/zap"
)

// FastChecksum checks whether the "local" checksum matches the checksum from TiKV.
func FastChecksum(
	ctx context.Context,
	backupMeta *backuppb.BackupMeta,
	storage storage.ExternalStorage,
	cipher *backuppb.CipherInfo,
) error {
	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		summary.CollectDuration("backup fast checksum", elapsed)
	}()

	ch := make(chan *metautil.Table)
	errCh := make(chan error)
	go func() {
		reader := metautil.NewMetaReader(backupMeta, storage, cipher)
		if _, err := reader.ReadSchemasFiles(ctx, ch, metautil.SkipStats); err != nil {
			errCh <- errors.Trace(err)
		}
		close(ch)
	}()

	for {
		var tbl *metautil.Table
		var ok bool
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case tbl, ok = <-ch:
			if !ok {
				close(errCh)
				return nil
			}
		}

		if tbl.Info == nil {
			// empty database
			continue
		}
		expectedChecksumStats := tbl.CalculateChecksumStatsOnFiles()
		if expectedChecksumStats.Crc64Xor != tbl.Crc64Xor ||
			expectedChecksumStats.TotalBytes != tbl.TotalBytes ||
			expectedChecksumStats.TotalKvs != tbl.TotalKvs {
			log.Error("checksum mismatch",
				zap.Stringer("db", tbl.DB.Name),
				zap.Stringer("table", tbl.Info.Name),
				zap.Uint64("origin tidb crc64", tbl.Crc64Xor),
				zap.Uint64("calculated crc64", expectedChecksumStats.Crc64Xor),
				zap.Uint64("origin tidb total kvs", tbl.TotalKvs),
				zap.Uint64("calculated total kvs", expectedChecksumStats.TotalKvs),
				zap.Uint64("origin tidb total bytes", tbl.TotalBytes),
				zap.Uint64("calculated total bytes", expectedChecksumStats.TotalBytes))
			// TODO enhance error
			return errors.Trace(berrors.ErrBackupChecksumMismatch)
		}
		log.Info("checksum success",
			zap.Stringer("db", tbl.DB.Name), zap.Stringer("table", tbl.Info.Name))
	}
}
