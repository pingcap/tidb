//go:build intest

package domain

import (
	"context"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/session/syssession"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
)

// RunMaterializedViewDemoSchedulerOnceForTest runs one iteration of the MV demo scheduler.
// It is only available in intest builds to keep the demo scheduler testable without timing flakes.
func (do *Domain) RunMaterializedViewDemoSchedulerOnceForTest() error {
	if do == nil || do.advancedSysSessionPool == nil {
		return nil
	}
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnAdmin)
	return do.advancedSysSessionPool.WithSession(func(se *syssession.Session) error {
		if _, err := sqlexec.ExecSQL(ctx, se, "SET @@session."+vardef.TiDBEnableMaterializedViewDemo+" = 1"); err != nil {
			return err
		}
		return do.mvDemoSchedulerRunOnce(ctx, se)
	})
}
