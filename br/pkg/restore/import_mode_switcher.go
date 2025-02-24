// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package restore

import (
	"context"

	_ "github.com/go-sql-driver/mysql" // mysql driver
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/conn"
	"github.com/pingcap/tidb/br/pkg/pdutil"
	"github.com/pingcap/tidb/pkg/util/importswitch"
	"go.uber.org/zap"
)

// RestorePreWork switches to import mode and removes pd schedulers if needed
// TODO make this function returns a restore post work.
func RestorePreWork(
	ctx context.Context,
	mgr *conn.Mgr,
	switcher *importswitch.ImportModeSwitcher,
	isOnline bool,
	switchToImport bool,
) (pdutil.UndoFunc, *pdutil.ClusterConfig, error) {
	if isOnline {
		return pdutil.Nop, nil, nil
	}

	if switchToImport {
		// Switch TiKV cluster to import mode (adjust rocksdb configuration).
		err := switcher.GoSwitchToImportMode(ctx)
		if err != nil {
			return pdutil.Nop, nil, err
		}
	}

	return mgr.RemoveSchedulersWithConfig(ctx)
}

// RestorePostWork executes some post work after restore.
// TODO: aggregate all lifetime manage methods into batcher's context manager field.
func RestorePostWork(
	ctx context.Context,
	switcher *importswitch.ImportModeSwitcher,
	restoreSchedulers pdutil.UndoFunc,
	isOnline bool,
) {
	if isOnline {
		return
	}

	if ctx.Err() != nil {
		log.Warn("context canceled, try shutdown")
		ctx = context.Background()
	}

	if err := switcher.SwitchToNormalMode(ctx); err != nil {
		log.Warn("fail to switch to normal mode", zap.Error(err))
	}
	if err := restoreSchedulers(ctx); err != nil {
		log.Warn("failed to restore PD schedulers", zap.Error(err))
	}
}
