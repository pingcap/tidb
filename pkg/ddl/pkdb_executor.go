package ddl

import (
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/util/dbterror"
)

func getTrackedTable(t table.Table, tbTracker *TableTracker) table.Table {
	if tbTracker == nil {
		// Single ALTER paths, including single ADD FOREIGN KEY auto-index creation, do not use a tracker.
		return t
	}
	return tables.MockTableFromMeta(tbTracker.TableInfo())
}

func handleTrackerMissingDropObject(ctx sessionctx.Context, ifExists bool, name any, errFmt string) error {
	err := dbterror.ErrCantDropFieldOrKey.GenWithStack(errFmt, name)
	if ifExists {
		ctx.GetSessionVars().StmtCtx.AppendNote(err)
		return nil
	}
	return err
}
