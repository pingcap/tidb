//go:build intest

package domain

import (
	"context"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/session/syssession"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
)

// RunMaterializedViewSchedulerOnceForTest runs one iteration of the materialized view scheduler.
// It is only available in intest builds to keep the scheduler testable without timing flakes.
func (do *Domain) RunMaterializedViewSchedulerOnceForTest() error {
	if do == nil || do.advancedSysSessionPool == nil {
		return nil
	}
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnAdmin)
	return do.advancedSysSessionPool.WithSession(func(se *syssession.Session) error {
		if _, err := sqlexec.ExecSQL(ctx, se, "SET @@session."+vardef.TiDBEnableMaterializedView+" = 1"); err != nil {
			return err
		}
		return do.mvSchedulerRunOnce(ctx, se)
	})
}
