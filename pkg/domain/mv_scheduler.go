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
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"go.uber.org/zap"
)

const (
	mvSchedulerTickInterval = 5 * time.Second
	mvSchedulerBatchSize    = 16

	// Purge deletes are not indexable by commit_ts (pseudo column), so always batch to avoid huge transactions.
	mvSchedulerPurgeBatchSize  = 2048
	mvSchedulerPurgeMaxBatches = 64
)

func (do *Domain) StartMaterializedViewScheduler() {
	if do == nil || do.wg == nil || do.advancedSysSessionPool == nil {
		return
	}
	if !do.mvSchedulerStarted.CompareAndSwap(false, true) {
		return
	}
	do.wg.Run(func() {
		do.mvSchedulerLoop(do.ctx)
	}, "mvSchedulerLoop")
}

func (do *Domain) mvSchedulerLoop(ctx context.Context) {
	ticker := time.NewTicker(mvSchedulerTickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		// Ensure only one node runs this background loop.
		if do.ddl == nil || do.ddl.OwnerManager() == nil || !do.ddl.OwnerManager().IsOwner() {
			continue
		}

		taskCtx := kv.WithInternalSourceType(ctx, kv.InternalTxnAdmin)
		if err := do.advancedSysSessionPool.WithSession(func(se *syssession.Session) error {
			// The refresh/purge SQL uses `_tidb_commit_ts`, which is gated by this session variable.
			if _, err := sqlexec.ExecSQL(taskCtx, se,
				"SET @@session."+vardef.TiDBEnableMaterializedView+" = 1",
			); err != nil {
				return err
			}
			return do.mvSchedulerRunOnce(taskCtx, se)
		}); err != nil && ctx.Err() == nil {
			logutil.BgLogger().Warn("materialized view scheduler tick failed", zap.Error(err))
		}
	}
}

func (do *Domain) mvSchedulerRunOnce(ctx context.Context, se *syssession.Session) error {
	if err := do.mvRefreshDueMVs(ctx, se); err != nil {
		return err
	}
	if err := do.mvPurgeLogs(ctx, se); err != nil {
		return err
	}
	return nil
}

func (do *Domain) mvRefreshDueMVs(ctx context.Context, se *syssession.Session) error {
	rows, err := sqlexec.ExecSQL(ctx, se,
		`SELECT mv_id
		   FROM mysql.mv_refresh_info
		  WHERE next_run_time IS NULL OR next_run_time <= NOW()
		  ORDER BY next_run_time
		  LIMIT %?`,
		mvSchedulerBatchSize,
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

		schema, name, err := mvLookupTableNameByID(is, mvID)
		if err != nil {
			logutil.BgLogger().Warn("materialized view scheduler: failed to resolve mv name by id", zap.Int64("mv_id", mvID), zap.Error(err))
			_, _ = sqlexec.ExecSQL(ctx, se, "DELETE FROM mysql.mv_refresh_info WHERE mv_id = %?", mvID)
			continue
		}

		sql := "ADMIN REFRESH MATERIALIZED VIEW " + mvQuoteFullName(schema, name)
		if _, err := sqlexec.ExecSQL(ctx, se, sql); err != nil {
			errMsg := mvTruncateError(err)
			logutil.BgLogger().Warn("materialized view scheduler: refresh failed",
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

func (do *Domain) mvPurgeLogs(ctx context.Context, se *syssession.Session) error {
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

		schema, name, err := mvLookupTableNameByID(is, logTableID)
		if err != nil {
			logutil.BgLogger().Warn("materialized view scheduler: failed to resolve log table name by id", zap.Int64("log_table_id", logTableID), zap.Error(err))
			continue
		}

		fullName := mvQuoteFullName(schema, name)
		sql := fmt.Sprintf("DELETE FROM %s WHERE _tidb_commit_ts < %%? LIMIT %d", fullName, mvSchedulerPurgeBatchSize)
		var totalAffected uint64
		for i := 0; i < mvSchedulerPurgeMaxBatches; i++ {
			if _, err := sqlexec.ExecSQL(ctx, se, sql, purgeTSO); err != nil {
				logutil.BgLogger().Warn("materialized view scheduler: purge failed",
					zap.Int64("log_table_id", logTableID),
					zap.String("log_table", schema.O+"."+name.O),
					zap.Uint64("purge_tso", purgeTSO),
					zap.Error(err),
				)
				break
			}

			var affected uint64
			_ = se.WithSessionContext(func(sctx sessionctx.Context) error {
				affected = sctx.GetSessionVars().StmtCtx.AffectedRows()
				return nil
			})
			totalAffected += affected
			if affected == 0 {
				break
			}
		}
		if totalAffected > 0 {
			logutil.BgLogger().Info("materialized view scheduler: purged log rows",
				zap.Int64("log_table_id", logTableID),
				zap.String("log_table", schema.O+"."+name.O),
				zap.Uint64("purge_tso", purgeTSO),
				zap.Uint64("rows", totalAffected),
			)
		}
	}
	return nil
}

func mvLookupTableNameByID(is infoschema.InfoSchema, tableID int64) (schema ast.CIStr, table ast.CIStr, err error) {
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

func mvQuoteFullName(schema, table ast.CIStr) string {
	escape := func(s string) string {
		return strings.ReplaceAll(s, "`", "``")
	}
	if schema.L == "" {
		return fmt.Sprintf("`%s`", escape(table.O))
	}
	return fmt.Sprintf("`%s`.`%s`", escape(schema.O), escape(table.O))
}

func mvTruncateError(err error) string {
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
