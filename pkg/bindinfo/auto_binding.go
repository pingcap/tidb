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

func (h *globalBindingHandle) AutoRecordBindings(since time.Time) (err error) {
	// TODO: improve the scan performance
	stmtQuery := fmt.Sprintf(`select digest, query_sample_text, charset, collation, plan_hint, plan_digest, schema_name
				from information_schema.statements_summary
				where stmt_type='Select' and exec_count > 0 and plan_hint != "" and summary_begin_time >= '%s'`,
		since.Add(-time.Minute*30).Format("2006-01-02 15:04:05"))
	var rows []chunk.Row
	err = h.callWithSCtx(false, func(sctx sessionctx.Context) error {
		rows, _, err = execRows(sctx, stmtQuery)
		return err
	})
	if err != nil {
		return err
	}

	logutil.BgLogger().Error("RecordInactiveBindings", zap.Int("rows", len(rows)))

	for _, row := range rows {
		query := row.GetString(1)
		charset := row.GetString(2)
		collation := row.GetString(3)
		hint := row.GetString(4)
		planDigest := row.GetString(5)
		schema := row.GetString(6)

		parser4binding := parser.New()
		originNode, err := parser4binding.ParseOneStmt(query, charset, collation)
		if err != nil {
			// TODO:
		}
		bindSQL := GenerateBindingSQL(originNode, hint, schema)
		var hintNode ast.StmtNode
		hintNode, err = parser4binding.ParseOneStmt(bindSQL, charset, collation)
		if err != nil {
			// TODO:
		}
		restoredSQL := RestoreDBForBinding(originNode, schema)
		bindSQL = RestoreDBForBinding(hintNode, schema)
		originalSQL, sqlDigest := parser.NormalizeDigestForBinding(restoredSQL)

		// TODO: calculate binding digest
		bindingDigest := planDigest

		// TODO: improve the write performance
		stmtInsert := fmt.Sprintf(`insert ignore into mysql.bind_info
    (binding_digest, original_sql, bind_sql, default_db, status, create_time, update_time, charset, collation, source, sql_digest, plan_digest) values
    ('%s', '%s', '%s', '%s', 'disabled', NOW(), NOW(), '%s', '%s', 'auto', '%s', '%s')`,
			bindingDigest, originalSQL, bindSQL, schema, charset, collation, sqlDigest, planDigest)
		err = h.callWithSCtx(true, func(sctx sessionctx.Context) error {
			_, err = exec(sctx, stmtInsert)
			return err
		})
	}
	return nil
}

type BindingExecInfo struct {
	Plan                 string
	AvgLatency           float64
	ExecTimes            int64
	AvgScanRows          float64
	AvgReturnedRows      float64
	LatencyPerReturnRow  float64
	ScanRowsPerReturnRow float64
}

type AutoBindingInfo struct {
	*Binding
	*BindingExecInfo
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
