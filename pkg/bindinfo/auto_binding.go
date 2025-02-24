package bindinfo

import (
	"fmt"
	"time"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

func (h *globalBindingHandle) AutoRecordBindings(beginTime time.Time) (err error) {
	// TODO: implement h.getStmtStats to get data by using in-memory function call instead of SQL to improve performance.
	stmts, err := h.getStmtStatsTemp(2, beginTime)
	if err != nil {
		return err
	}

	logutil.BgLogger().Error("RecordInactiveBindings", zap.Int("rows", len(stmts)))

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

func (h *globalBindingHandle) AutoBindingsForSQL(stmtOrDigest string) ([]*AutoBindingInfo, error) {
	normStmt, isStmt := h.normalizeSQLForBinding(stmtOrDigest)

	var whereCond string
	if isStmt {
		whereCond = fmt.Sprintf("where original_sql='%s'", normStmt)
	} else {
		whereCond = fmt.Sprintf("where sql_digest='%s'", normStmt)
	}

	bindings, err := h.readBindingsFromStorage(whereCond)
	if err != nil {
		return nil, err
	}

	var autoBindings []*AutoBindingInfo
	for _, binding := range bindings {
		//execInfo, err := h.getBindingExecInfo(binding.BindSQL)
		autoBindings = append(autoBindings, &AutoBindingInfo{
			Binding: binding,
		})
	}

	return autoBindings, nil
}

func (h *globalBindingHandle) normalizeSQLForBinding(SQLOrDigest string) (normStmt string, isStmt bool) {
	p := parser.New()
	stmt, err := p.ParseOneStmt(SQLOrDigest, "", "")
	if err != nil {
		return "", false
	}
	normStmt, _ = NormalizeStmtForBinding(stmt, "test", false)
	return normStmt, true
}

// TODO: expose statement_summary.go:stmtSummaryStats and use it directly? Is it thread-safe?
type StmtStats struct {
	Digest          string
	QuerySampleText string
	Charset         string
	Collation       string
	PlanHint        string
	PlanDigest      string
	SchemaName      string
}

func (h *globalBindingHandle) getStmtStats(execCountThreshold int, beginTime time.Time) ([]*StmtStats, error) {
	return nil, nil
}

func (h *globalBindingHandle) getStmtStatsTemp(execCountThreshold int, beginTime time.Time) (stmts []*StmtStats, err error) {
	stmtQuery := fmt.Sprintf(`
				select digest, query_sample_text, charset,
				collation, plan_hint, plan_digest, schema_name
				from information_schema.tidb_statement_stats
				where stmt_type in ('Select', 'Insert', 'Update', 'Delete') and
				plan_hint != "" and
				exec_count > %v and summary_begin_time >= '%s'`,
		execCountThreshold,
		beginTime.Add(-time.Minute*30).Format("2006-01-02 15:04:05"))
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
		})
	}
	return
}

func (h *globalBindingHandle) getStmtStatsByDigest(digest string) ([]*StmtStats, error) {
	return nil, nil
}
