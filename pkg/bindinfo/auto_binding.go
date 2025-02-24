package bindinfo

import (
	"fmt"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

func (h *globalBindingHandle) AutoRecordBindings() (err error) {
	// TODO: implement h.getStmtStats to get data by using in-memory function call instead of SQL to improve performance.
	stmts, err := h.getStmtStatsTemp(0)
	if err != nil {
		return err
	}

	logutil.BgLogger().Info("RecordInactiveBindings", zap.Int("rows", len(stmts)))

	for _, stmt := range stmts {
		parser4binding := parser.New()
		originNode, err := parser4binding.ParseOneStmt(stmt.QuerySampleText, stmt.Charset, stmt.Collation)
		if err != nil {
			// TODO:
		}
		bindSQL := GenerateBindingSQL(originNode, stmt.PlanHint, stmt.SchemaName)
		var hintNode ast.StmtNode
		hintNode, err = parser4binding.ParseOneStmt(bindSQL, stmt.Charset, stmt.Collation)
		if err != nil {
			// TODO:
		}
		restoredSQL := RestoreDBForBinding(originNode, stmt.SchemaName)
		bindSQL = RestoreDBForBinding(hintNode, stmt.SchemaName)
		originalSQL, sqlDigest := parser.NormalizeDigestForBinding(restoredSQL)

		// TODO: calculate binding digest
		bindingDigest := stmt.PlanDigest

		// TODO: improve the write performance
		stmtInsert := fmt.Sprintf(`insert ignore into mysql.bind_info
    (binding_digest, original_sql, bind_sql, default_db, status, create_time, update_time, charset, collation, source, sql_digest, plan_digest) values
    ('%s', '%s', '%s', '%s', 'disabled', NOW(), NOW(), '%s', '%s', 'auto', '%s', '%s')`,
			bindingDigest, originalSQL, bindSQL, stmt.SchemaName, stmt.Charset, stmt.Collation, sqlDigest, stmt.PlanDigest)
		err = h.callWithSCtx(true, func(sctx sessionctx.Context) error {
			_, err = exec(sctx, stmtInsert)
			return err
		})
	}
	return nil
}

type AutoBindingInfo struct {
	*Binding

	// Info from StmtStats
	Plan                 string
	AvgLatency           float64
	ExecTimes            int64
	AvgScanRows          float64
	AvgReturnedRows      float64
	LatencyPerReturnRow  float64
	ScanRowsPerReturnRow float64
}

func (h *globalBindingHandle) AutoBindingsForSQL(digest string) ([]*AutoBindingInfo, error) {
	whereCond := fmt.Sprintf("where sql_digest='%s'", digest)
	bindings, err := h.readBindingsFromStorage(whereCond)
	if err != nil {
		return nil, err
	}

	var autoBindings []*AutoBindingInfo
	for _, binding := range bindings {
		stmt, err := h.getStmtStatsByPlanDigestInCluster(binding.PlanDigest)
		if err != nil {
			logutil.BgLogger().Error("getStmtStatsByDigestInCluster", zap.String("digest", digest), zap.Error(err))
		}
		autoBinding := &AutoBindingInfo{Binding: binding}
		if stmt != nil && stmt.ExecCount > 0 {
			autoBinding.Plan = stmt.Plan
			autoBinding.ExecTimes = stmt.ExecCount
			autoBinding.AvgLatency = float64(stmt.TotalTime) / float64(stmt.ExecCount)
			autoBinding.AvgScanRows = float64(stmt.ProcessedKeys) / float64(stmt.ExecCount)
			autoBinding.AvgReturnedRows = float64(stmt.ResultRows) / float64(stmt.ExecCount)
			autoBinding.LatencyPerReturnRow = autoBinding.AvgLatency / autoBinding.AvgReturnedRows
			autoBinding.ScanRowsPerReturnRow = autoBinding.AvgScanRows / autoBinding.AvgReturnedRows
		}
		autoBindings = append(autoBindings, autoBinding)
	}

	return autoBindings, nil
}

// TODO: expose statement_summary.go:stmtSummaryStats and use it directly? Is it thread-safe?
type StmtStats struct {
	// meta info
	Digest          string
	QuerySampleText string
	Charset         string
	Collation       string
	PlanHint        string
	PlanDigest      string
	SchemaName      string

	// exec info
	Plan          string
	ResultRows    int64
	ExecCount     int64
	ProcessedKeys int64
	TotalTime     int64
}

func (h *globalBindingHandle) getStmtStats(execCountThreshold int) ([]*StmtStats, error) {
	// TODO: to implement.
	return nil, nil
}

func (h *globalBindingHandle) getStmtStatsTemp(execCountThreshold int) (stmts []*StmtStats, err error) {
	stmtQuery := fmt.Sprintf(`
				select digest, query_sample_text, charset,
				collation, plan_hint, plan_digest, schema_name,
				result_rows, exec_count, processed_keys, total_time, plan
				from information_schema.tidb_statements_stats
				where stmt_type in ('Select', 'Insert', 'Update', 'Delete') and
				plan_hint != "" and
				exec_count > %v`, execCountThreshold)
	var rows []chunk.Row
	err = h.callWithSCtx(false, func(sctx sessionctx.Context) error {
		rows, _, err = execRows(sctx, stmtQuery)
		return err
	})
	if err != nil {
		return
	}

	logutil.BgLogger().Error("RecordInactiveBindings", zap.Int("rows", len(rows)))

	stmts = make([]*StmtStats, 0, len(rows))
	for _, row := range rows {
		stmts = append(stmts, &StmtStats{
			Digest:          row.GetString(0),
			QuerySampleText: row.GetString(1),
			Charset:         row.GetString(2),
			Collation:       row.GetString(3),
			PlanHint:        row.GetString(4),
			PlanDigest:      row.GetString(5),
			SchemaName:      row.GetString(6),
			ResultRows:      row.GetInt64(7),
			ExecCount:       row.GetInt64(8),
			ProcessedKeys:   row.GetInt64(9),
			TotalTime:       row.GetInt64(10),
			Plan:            row.GetString(11),
		})
	}
	return
}

func (h *globalBindingHandle) getStmtStatsByPlanDigestInCluster(planDigest string) (stmt *StmtStats, err error) {
	if planDigest == "" {
		return nil, nil
	}
	stmtQuery := fmt.Sprintf(`
				select digest, query_sample_text, charset,
				collation, plan_hint, plan_digest, schema_name,
				result_rows, exec_count, processed_keys, total_time, plan
				from information_schema.tidb_statements_stats
				where plan_digest = '%v'`, planDigest)
	var rows []chunk.Row
	err = h.callWithSCtx(false, func(sctx sessionctx.Context) error {
		rows, _, err = execRows(sctx, stmtQuery)
		return err
	})
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	if len(rows) > 1 {
		// TODO: accumulate them
	}

	row := rows[0]
	stmt = &StmtStats{
		Digest:          row.GetString(0),
		QuerySampleText: row.GetString(1),
		Charset:         row.GetString(2),
		Collation:       row.GetString(3),
		PlanHint:        row.GetString(4),
		PlanDigest:      row.GetString(5),
		SchemaName:      row.GetString(6),
		ResultRows:      row.GetInt64(7),
		ExecCount:       row.GetInt64(8),
		ProcessedKeys:   row.GetInt64(9),
		TotalTime:       row.GetInt64(10),
		Plan:            row.GetString(11),
	}
	return
}
