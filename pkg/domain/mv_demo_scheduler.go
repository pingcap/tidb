package domain

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/session/syssession"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"go.uber.org/zap"
)

const (
	mvDemoSchedulerTickInterval = 5 * time.Second
	mvDemoSchedulerBatchSize    = 16
)

func (do *Domain) StartMaterializedViewDemoScheduler() {
	if do == nil || do.wg == nil || do.advancedSysSessionPool == nil {
		return
	}
	if !do.mvDemoSchedulerStarted.CompareAndSwap(false, true) {
		return
	}
	do.wg.Run(func() {
		do.mvDemoSchedulerLoop(do.ctx)
	}, "mvDemoSchedulerLoop")
}

func (do *Domain) mvDemoSchedulerLoop(ctx context.Context) {
	ticker := time.NewTicker(mvDemoSchedulerTickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		// Ensure only one node runs this background demo loop.
		if do.ddl == nil || do.ddl.OwnerManager() == nil || !do.ddl.OwnerManager().IsOwner() {
			continue
		}

		taskCtx := kv.WithInternalSourceType(ctx, kv.InternalTxnAdmin)
		if err := do.advancedSysSessionPool.WithSession(func(se *syssession.Session) error {
			// The refresh/purge SQL uses `_tidb_commit_ts`, which is gated by this session variable.
			if _, err := sqlexec.ExecSQL(taskCtx, se,
				"SET @@session."+vardef.TiDBEnableMaterializedViewDemo+" = 1",
			); err != nil {
				return err
			}
			return do.mvDemoSchedulerRunOnce(taskCtx, se)
		}); err != nil && ctx.Err() == nil {
			logutil.BgLogger().Warn("mv demo scheduler tick failed", zap.Error(err))
		}
	}
}

func (do *Domain) mvDemoSchedulerRunOnce(ctx context.Context, se *syssession.Session) error {
	if err := do.mvDemoRefreshDueMVs(ctx, se); err != nil {
		return err
	}
	if err := do.mvDemoPurgeLogs(ctx, se); err != nil {
		return err
	}
	return nil
}

func (do *Domain) mvDemoRefreshDueMVs(ctx context.Context, se *syssession.Session) error {
	rows, err := sqlexec.ExecSQL(ctx, se,
		`SELECT mv_id
		   FROM mysql.mv_refresh_info
		  WHERE next_run_time IS NULL OR next_run_time <= NOW()
		  ORDER BY next_run_time
		  LIMIT %?`,
		mvDemoSchedulerBatchSize,
	)
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		return nil
	}

	is := do.InfoSchema()
	for _, r := range rows {
		mvID := r.GetInt64(0)
		if mvID <= 0 {
			continue
		}

		schema, name, err := mvDemoLookupTableNameByID(is, mvID)
		if err != nil {
			logutil.BgLogger().Warn("mv demo scheduler: failed to resolve mv name by id", zap.Int64("mv_id", mvID), zap.Error(err))
			_, _ = sqlexec.ExecSQL(ctx, se, "DELETE FROM mysql.mv_refresh_info WHERE mv_id = %?", mvID)
			continue
		}

		sql := "ADMIN REFRESH MATERIALIZED VIEW " + mvDemoQuoteFullName(schema, name)
		if _, err := sqlexec.ExecSQL(ctx, se, sql); err != nil {
			errMsg := mvDemoTruncateError(err)
			logutil.BgLogger().Warn("mv demo scheduler: refresh failed",
				zap.Int64("mv_id", mvID),
				zap.String("mv", schema.O+"."+name.O),
				zap.Error(err),
			)
			_, _ = sqlexec.ExecSQL(ctx, se,
				`UPDATE mysql.mv_refresh_info
				    SET last_refresh_result = 'FAILED',
				        last_refresh_time = NOW(),
				        next_run_time = DATE_ADD(NOW(), INTERVAL refresh_interval_seconds SECOND),
				        last_error = %?
				  WHERE mv_id = %?`,
				errMsg, mvID,
			)
		}
	}

	return nil
}

func (do *Domain) mvDemoPurgeLogs(ctx context.Context, se *syssession.Session) error {
	rows, err := sqlexec.ExecSQL(ctx, se,
		`SELECT log_table_id, MIN(last_refresh_tso) AS purge_tso
		   FROM mysql.mv_refresh_info
		  WHERE last_refresh_tso > 0
		  GROUP BY log_table_id`,
	)
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		return nil
	}

	is := do.InfoSchema()
	for _, r := range rows {
		logTableID := r.GetInt64(0)
		purgeTSO := r.GetUint64(1)
		if logTableID <= 0 || purgeTSO == 0 {
			continue
		}

		schema, name, err := mvDemoLookupTableNameByID(is, logTableID)
		if err != nil {
			logutil.BgLogger().Warn("mv demo scheduler: failed to resolve log table name by id", zap.Int64("log_table_id", logTableID), zap.Error(err))
			continue
		}

		sql := "DELETE FROM " + mvDemoQuoteFullName(schema, name) + " WHERE _tidb_commit_ts < %?"
		if _, err := sqlexec.ExecSQL(ctx, se, sql, purgeTSO); err != nil {
			logutil.BgLogger().Warn("mv demo scheduler: purge failed",
				zap.Int64("log_table_id", logTableID),
				zap.String("log_table", schema.O+"."+name.O),
				zap.Uint64("purge_tso", purgeTSO),
				zap.Error(err),
			)
		}
	}
	return nil
}

func mvDemoLookupTableNameByID(is infoschema.InfoSchema, tableID int64) (schema ast.CIStr, table ast.CIStr, err error) {
	tbl, ok := is.TableByID(context.Background(), tableID)
	if !ok || tbl.Meta() == nil {
		return ast.CIStr{}, ast.CIStr{}, errors.Errorf("table %d not found in infoschema", tableID)
	}
	dbInfo, ok := infoschema.SchemaByTable(is, tbl.Meta())
	if !ok || dbInfo == nil {
		return ast.CIStr{}, ast.CIStr{}, errors.Errorf("schema for table %d not found in infoschema", tableID)
	}
	return dbInfo.Name, tbl.Meta().Name, nil
}

func mvDemoQuoteFullName(schema, table ast.CIStr) string {
	escape := func(s string) string {
		return strings.ReplaceAll(s, "`", "``")
	}
	if schema.L == "" {
		return fmt.Sprintf("`%s`", escape(table.O))
	}
	return fmt.Sprintf("`%s`.`%s`", escape(schema.O), escape(table.O))
}

func mvDemoTruncateError(err error) string {
	const maxLen = 4096
	if err == nil {
		return ""
	}
	msg := err.Error()
	if len(msg) <= maxLen {
		return msg
	}
	return msg[:maxLen]
}
