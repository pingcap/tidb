package bindinfo

import (
	"fmt"
	"time"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/chunk"
	utilparser "github.com/pingcap/tidb/pkg/util/parser"
)

func (h *globalBindingHandle) RecordInactiveBindings(since time.Time) (err error) {
	// TODO: improve the scan performance
	stmtQuery := fmt.Sprintf(`select digest, query_sample_text, charset, collation, plan_hint, plan_digest, schema_name
				from information_schema.statements_summary
				where stmt_type='Select' and exec_count > 0 and plan_hint != "" and summary_begin_time >= '%s'`, since.Format("2006-01-02 15:04:05"))
	var rows []chunk.Row
	err = h.callWithSCtx(false, func(sctx sessionctx.Context) error {
		rows, _, err = execRows(sctx, stmtQuery)
		return err
	})
	if err != nil {
		return err
	}

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
		restoredSQL := utilparser.RestoreWithDefaultDB(originNode, schema, query)
		bindSQL = utilparser.RestoreWithDefaultDB(hintNode, schema, hintNode.Text())
		db := utilparser.GetDefaultDB(originNode, schema)
		originalSQL, sqlDigest := parser.NormalizeDigestForBinding(restoredSQL)

		// TODO: improve the write performance
		stmtInsert := fmt.Sprintf("insert ignore into mysql.bind_info values ('%s', '%s', '%s', 'disabled', NOW(), NOW(), '%s', '%s', 'auto', '%s', '%s')",
			originalSQL, bindSQL, db, charset, collation, sqlDigest, planDigest)
		err = h.callWithSCtx(true, func(sctx sessionctx.Context) error {
			_, err = exec(sctx, stmtInsert)
			return err
		})
	}
	return nil
}
